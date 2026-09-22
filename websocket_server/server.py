# SPDX-License-Identifier: MIT

import math
import os
import threading
import time
import uuid
from dataclasses import dataclass, field

from arduino.app_utils import brick
from websockets.exceptions import ConnectionClosed
from websockets.sync.server import serve


@dataclass
class _ServerRun:
    """一次启动独享的状态；旧线程只能更新自己的记录，不能改写后续启动。"""

    state: str = "starting"
    stop_event: threading.Event = field(default_factory=threading.Event)
    startup_event: threading.Event = field(default_factory=threading.Event)
    thread: threading.Thread | None = None
    server: object = None
    error: Exception | None = None
    cleanup_error: Exception | None = None


@brick
class WebSocketServer:
    """与上层消息格式无关的 WebSocket 服务端 Brick。"""

    def __init__(
        self,
        host=None,
        port=None,
        path=None,
        max_message_bytes=None,
        max_clients=None,
        ping_interval_s=None,
        ping_timeout_s=None,
        start_timeout_s=5.0,
        stop_timeout_s=3.0,
    ):
        """
        @description         : 创建 WebSocket 服务并读取 App Lab 注入的配置变量
        @param host          : 容器内监听地址，None 时读取环境变量
        @param port          : WebSocket 监听端口，None 时读取环境变量
        @param path          : 接受的 WebSocket 请求路径，None 时读取环境变量
        @param max_message_bytes : 单条文本或二进制消息的最大字节数
        @param max_clients   : 允许同时连接的最大客户端数
        @param ping_interval_s : WebSocket 协议 ping 帧发送间隔秒数
        @param ping_timeout_s : WebSocket 协议 pong 帧等待秒数
        @param start_timeout_s : start 等待监听就绪的总秒数，必须为有限正数
        @param stop_timeout_s : stop 等待清理的总秒数，必须为有限非负数
        @return              : 无返回值
        """
        self._host = host if host is not None else os.getenv(
            "WEBSOCKET_SERVER_HOST",
            "0.0.0.0",
        )
        self._port = self._read_int(
            port,
            "WEBSOCKET_SERVER_PORT",
            8765,
            1,
            65535,
        )
        self._path = path if path is not None else os.getenv(
            "WEBSOCKET_SERVER_PATH",
            "/ws",
        )
        self._max_message_bytes = self._read_int(
            max_message_bytes,
            "WEBSOCKET_SERVER_MAX_MESSAGE_BYTES",
            16 * 1024,
            1,
            16 * 1024 * 1024,
        )
        self._max_clients = self._read_int(
            max_clients,
            "WEBSOCKET_SERVER_MAX_CLIENTS",
            4,
            1,
            1024,
        )
        self._ping_interval_s = self._read_float(
            ping_interval_s,
            "WEBSOCKET_SERVER_PING_INTERVAL_S",
            10.0,
            0.1,
            3600.0,
        )
        self._ping_timeout_s = self._read_float(
            ping_timeout_s,
            "WEBSOCKET_SERVER_PING_TIMEOUT_S",
            10.0,
            0.1,
            3600.0,
        )

        if not isinstance(self._host, str) or not self._host.strip():
            raise ValueError("WEBSOCKET_SERVER_HOST must be a non-empty string")
        if (
            not isinstance(self._path, str)
            or not self._path.startswith("/")
            or "?" in self._path
            or "#" in self._path
        ):
            raise ValueError(
                "WEBSOCKET_SERVER_PATH must start with '/' and contain no query or fragment"
            )

        self._state_lock = threading.RLock()
        self._start_timeout_s = self._validate_timeout(start_timeout_s, allow_zero=False)
        self._stop_timeout_s = self._validate_timeout(stop_timeout_s, allow_zero=True)
        self._run = None
        self._callback_context = threading.local()
        self._clients = {}
        self._connect_callback = None
        self._message_callback = None
        self._disconnect_callback = None

    def start(self):
        """
        @description         : 等待后台监听就绪，失败抛出异常，超时取消本次启动
        @param               : 无参数；等待预算由构造参数 start_timeout_s 指定
        @return              : 监听就绪返回 True，启动失败或取消抛出 RuntimeError，超时抛出 TimeoutError
        """
        timeout_s = self._start_timeout_s
        deadline = time.monotonic() + timeout_s
        with self._state_lock:
            run = self._run
            if run is not None and run.cleanup_error is not None:
                raise RuntimeError("previous WebSocket cleanup failed; restart the process") from run.cleanup_error
            if run is not None and run.thread is not None and run.thread.is_alive():
                if run.stop_event.is_set():
                    raise RuntimeError("previous WebSocket run is still cleaning up")
                # 并发 start() 等待同一次启动，不再创建监听线程。
            else:
                run = _ServerRun()
                self._run = run
                run.thread = threading.Thread(
                    target=self._run_server,
                    args=(run,),
                    name="websocket-server-brick",
                    daemon=True,
                )
                try:
                    run.thread.start()
                except Exception as exc:
                    run.error = exc
                    run.state = "failed"
                    run.stop_event.set()
                    run.startup_event.set()
                    raise RuntimeError(f"WebSocket thread start failed: {exc}") from exc

        ready = run.startup_event.wait(timeout=max(0.0, deadline - time.monotonic()))
        with self._state_lock:
            if ready and run.state == "running" and not run.stop_event.is_set():
                return True
            if not ready and not run.stop_event.is_set():
                run.error = TimeoutError(f"WebSocket startup timed out after {timeout_s:g} seconds")
                run.state = "failed"
                run.stop_event.set()
                run.startup_event.set()
            error = run.error
        if isinstance(error, TimeoutError):
            raise error
        raise RuntimeError(f"WebSocket startup failed: {error or 'startup cancelled by stop()'}") from error

    def stop(self):
        """
        @description         : 按构造参数 stop_timeout_s 的总预算请求停止并等待清理
        @param               : 无参数；App Lab 要求生命周期方法不能带额外参数
        @return              : 清理完成返回 True，仍在清理或清理失败返回 False
        """
        return self._stop(self._stop_timeout_s)

    def request_stop(self):
        """
        @description         : 只请求停止，不等待；适用于回调或 App 退出时的兜底清理
        @param               : 无参数
        @return              : 已清理完成返回 True，否则返回 False
        """
        return self._stop(0.0)

    def _stop(self, timeout_s):
        """
        @description         : 请求取消或停机，并在统一时间预算内等待全部后台清理
        @param timeout_s     : 本次调用等待清理的总秒数，0 表示只请求停止
        @return              : 全部清理完成返回 True，超时、回调内请求或清理失败返回 False
        """
        timeout_s = self._validate_timeout(timeout_s, allow_zero=True)
        deadline = time.monotonic() + timeout_s
        with self._state_lock:
            run = self._run
            if run is None:
                return True
            run.stop_event.set()
            run.startup_event.set()
            thread = run.thread
            if thread is not None and thread.is_alive() and run.error is None:
                run.state = "stopping"
        # 回调属于 shutdown() 要等待的连接线程，不能反过来等自己的退出。
        if getattr(self._callback_context, "run", None) is run:
            return False
        if thread is not None and thread.ident is not None and thread is not threading.current_thread():
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
        complete = not (thread and thread.is_alive()) and run.cleanup_error is None
        if not complete and timeout_s > 0:
            print("[websocket_server] stop incomplete; cleanup is pending or failed; restart refused", flush=True)
        return complete

    def require_running(self):
        """
        @description         : 在 App 主循环检查服务是否可用，把被生命周期管理器捕获的启动失败传播给 App
        @param               : 无参数
        @return              : 正常运行返回 None，否则抛出带具体原因的 RuntimeError
        """
        with self._state_lock:
            run = self._run
            if run is not None and run.state == "running" and not run.stop_event.is_set():
                return
            error = (run.error or run.cleanup_error) if run is not None else None
            state = run.state if run is not None else "stopped"
        raise RuntimeError(f"WebSocket service unavailable (state={state}): {error or 'not listening'}") from error

    def on_connect(self, callback):
        """
        @description         : 注册客户端成功接入后的回调函数
        @param callback      : 接收 client_info 字典的可调用对象，None 表示取消回调
        @return              : 当前 WebSocketServer 实例
        """
        if callback is not None and not callable(callback):
            raise TypeError("connect callback must be callable or None")
        with self._state_lock:
            self._connect_callback = callback
        return self

    def on_message(self, callback):
        """
        @description         : 注册收到未解析文本帧或二进制帧时的回调函数
        @param callback      : 接收 client_id 和 str 或 bytes payload 的可调用对象
        @return              : 当前 WebSocketServer 实例
        """
        if callback is not None and not callable(callback):
            raise TypeError("message callback must be callable or None")
        with self._state_lock:
            self._message_callback = callback
        return self

    def on_disconnect(self, callback):
        """
        @description         : 注册客户端断开后的回调函数
        @param callback      : 接收 client_info、关闭状态码和原因的可调用对象
        @return              : 当前 WebSocketServer 实例
        """
        if callback is not None and not callable(callback):
            raise TypeError("disconnect callback must be callable or None")
        with self._state_lock:
            self._disconnect_callback = callback
        return self

    def send(self, client_id, payload):
        """
        @description         : 向指定客户端发送一条未解析文本帧或二进制帧
        @param client_id     : 连接建立时由 Brick 分配的客户端标识
        @param payload       : str 文本消息或 bytes 二进制消息
        @return              : 发送成功返回 True，客户端不存在或已断开返回 False
        """
        self._validate_payload(payload)
        with self._state_lock:
            session = self._clients.get(client_id)
        if session is None:
            return False

        try:
            with session["send_lock"]:
                session["socket"].send(payload)
            return True
        except (ConnectionClosed, OSError, RuntimeError):
            return False

    def broadcast(self, payload):
        """
        @description         : 将同一条未解析消息发送给当前所有客户端
        @param payload       : str 文本消息或 bytes 二进制消息
        @return              : 成功发送的客户端数量
        """
        self._validate_payload(payload)
        with self._state_lock:
            client_ids = list(self._clients)
        return sum(1 for client_id in client_ids if self.send(client_id, payload))

    def disconnect(self, client_id, code=1000, reason=""):
        """
        @description         : 主动关闭指定客户端的 WebSocket 连接
        @param client_id     : 连接建立时由 Brick 分配的客户端标识
        @param code          : 合法的 WebSocket 关闭状态码
        @param reason        : UTF-8 关闭原因字符串
        @return              : 找到客户端并发起关闭返回 True，否则返回 False
        """
        with self._state_lock:
            session = self._clients.get(client_id)
        if session is None:
            return False
        session["socket"].close(code, reason)
        return True

    def get_clients(self):
        """
        @description         : 获取不包含底层 socket 的客户端信息快照
        @param               : 无参数
        @return              : 按 client_id 排序的客户端信息字典列表
        """
        with self._state_lock:
            clients = [self._client_info(session) for session in self._clients.values()]
        return sorted(clients, key=lambda client: client["client_id"])

    def get_status(self):
        """
        @description         : 获取监听配置、当前连接数和最近服务错误的状态快照
        @param               : 无参数
        @return              : 服务状态字典
        """
        with self._state_lock:
            run = self._run
            running = bool(run and run.thread and run.thread.is_alive())
            return {
                "host": self._host,
                "port": self._port,
                "path": self._path,
                "state": run.state if run else "stopped",
                "running": running,
                "listening": bool(run and run.state == "running" and not run.stop_event.is_set()),
                "cleanup_pending": bool(running and run.stop_event.is_set()),
                "client_count": len(self._clients),
                "max_clients": self._max_clients,
                "max_message_bytes": self._max_message_bytes,
                "server_error": f"{type(run.error).__name__}: {run.error}" if run and run.error else None,
                "cleanup_error": str(run.cleanup_error) if run and run.cleanup_error else None,
            }

    @staticmethod
    def _validate_timeout(value, *, allow_zero):
        """
        @description         : 校验生命周期等待预算，拒绝无穷大、NaN 和布尔值
        @param value         : 秒数
        @param allow_zero    : 是否允许零秒非阻塞请求
        @return              : 有限浮点秒数
        """
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("timeout_s must be a finite number")
        value = float(value)
        if not math.isfinite(value) or value < 0 or (value == 0 and not allow_zero):
            raise ValueError("timeout_s must be finite and positive (stop also accepts zero)")
        return value

    @staticmethod
    def _read_int(explicit_value, environment_name, default_value, minimum, maximum):
        """
        @description         : 从显式参数或环境变量读取受范围约束的整数
        @param explicit_value : 调用方显式提供的值
        @param environment_name : 环境变量名称
        @param default_value : 默认值
        @param minimum       : 最小允许值
        @param maximum       : 最大允许值
        @return              : 校验后的整数
        """
        value = explicit_value
        if value is None:
            value = os.getenv(environment_name, str(default_value))
        parsed = int(value)
        if parsed < minimum or parsed > maximum:
            raise ValueError(
                f"{environment_name} must be between {minimum} and {maximum}"
            )
        return parsed

    @staticmethod
    def _read_float(explicit_value, environment_name, default_value, minimum, maximum):
        """
        @description         : 从显式参数或环境变量读取受范围约束的浮点数
        @param explicit_value : 调用方显式提供的值
        @param environment_name : 环境变量名称
        @param default_value : 默认值
        @param minimum       : 最小允许值
        @param maximum       : 最大允许值
        @return              : 校验后的浮点数
        """
        value = explicit_value
        if value is None:
            value = os.getenv(environment_name, str(default_value))
        parsed = float(value)
        if not math.isfinite(parsed) or parsed < minimum or parsed > maximum:
            raise ValueError(
                f"{environment_name} must be between {minimum} and {maximum}"
            )
        return parsed

    def _run_server(self, run):
        """
        @description         : 绑定端口并在专用线程中运行 WebSocket 服务
        @param run           : 本次启动独享的状态记录
        @return              : 无返回值
        """
        server = None
        shutdown_thread = None
        entered_serve = False
        try:
            if run.stop_event.is_set():
                return
            server = serve(
                lambda websocket: self._handle_connection(websocket, run),
                self._host,
                self._port,
                compression=None,
                ping_interval=self._ping_interval_s,
                ping_timeout=self._ping_timeout_s,
                open_timeout=2.0,
                close_timeout=2.0,
                max_size=self._max_message_bytes,
                max_queue=16,
            )
            with self._state_lock:
                run.server = server
            # websockets 17.1 的 shutdown() 会等所有连接回调返回，必须放到独立清理线程。
            shutdown_thread = threading.Thread(
                target=self._shutdown_server,
                args=(run, server),
                name="websocket-server-cleanup",
                daemon=True,
            )
            shutdown_thread.start()
            with self._state_lock:
                if run.stop_event.is_set():
                    # serve() 内部绑定可能延迟；取消后不能发布 running 或处理业务连接。
                    server.socket.close()
                else:
                    run.state = "running"
                run.startup_event.set()
            entered_serve = True
            server.serve_forever()
            if not run.stop_event.is_set():
                raise RuntimeError("WebSocket accept loop exited unexpectedly")
        except Exception as exc:
            # Windows 的 select 可能在另一个线程关闭监听 socket 时报告 OSError。
            expected_close = entered_serve and run.stop_event.is_set() and isinstance(exc, OSError)
            if not expected_close:
                with self._state_lock:
                    if run.error is None:
                        run.error = exc
                    run.state = "failed"
                    run.stop_event.set()
                    run.startup_event.set()
                print(
                    f"[websocket_server] server failed: {type(exc).__name__}: {exc}",
                    flush=True,
                )
        finally:
            run.stop_event.set()
            run.startup_event.set()
            if shutdown_thread is not None and shutdown_thread.ident is not None:
                # 只有后台工作线程可以无限等待；公开 stop() 始终使用自己的总预算。
                shutdown_thread.join()
            elif server is not None:
                try:
                    server.socket.close()
                    # shutdown() 会等 serve_forever() 的退出事件；没运行过时也需走退出路径。
                    if not entered_serve:
                        server.serve_forever()
                    server.shutdown()
                except Exception as exc:
                    run.cleanup_error = exc
            with self._state_lock:
                run.server = None
                run.state = "failed" if run.error or run.cleanup_error else "stopped"

    def _shutdown_server(self, run, server):
        """
        @description         : 收到停止请求后关闭监听和所有连接，等待库内连接处理线程退出
        @param run           : 本次启动独享的状态记录
        @param server        : 本次 websockets 服务对象
        @return              : 无返回值，清理异常保留在状态中并禁止新一代启动
        """
        run.stop_event.wait()
        try:
            server.shutdown(reason="App Lab application is stopping")
        except Exception as exc:
            with self._state_lock:
                run.cleanup_error = exc
            print(f"[websocket_server] cleanup failed: {type(exc).__name__}: {exc}", flush=True)

    def _handle_connection(self, websocket, run):
        """
        @description         : 校验路径、登记客户端并转交原始 WebSocket 消息
        @param websocket     : websockets 同步服务端连接对象
        @param run           : 接受此连接的服务启动记录
        @return              : 无返回值
        """
        request_path = websocket.request.path.split("?", 1)[0]
        if request_path != self._path:
            websocket.close(1008, f"expected path {self._path}")
            return

        client_id = uuid.uuid4().hex
        session = {
            "client_id": client_id,
            "remote_address": self._format_remote_address(websocket.remote_address),
            "path": request_path,
            "connected_monotonic_s": time.monotonic(),
            "socket": websocket,
            "send_lock": threading.Lock(),
        }

        with self._state_lock:
            stopping = run.stop_event.is_set() or run is not self._run
            if stopping or len(self._clients) >= self._max_clients:
                accepted = False
            else:
                self._clients[client_id] = session
                connect_callback = self._connect_callback
                accepted = True

        if not accepted:
            websocket.close(1001 if stopping else 1013, "server stopping" if stopping else "maximum client count reached")
            return

        client_info = self._client_info(session)
        self._callback_context.run = run
        try:
            if not self._invoke_callback(
                "connect",
                connect_callback,
                client_info,
            ):
                websocket.close(1011, "connect callback failed")
                return

            for payload in websocket:
                with self._state_lock:
                    if run.stop_event.is_set():
                        break
                    message_callback = self._message_callback
                if not self._invoke_callback(
                    "message",
                    message_callback,
                    client_id,
                    payload,
                ):
                    websocket.close(1011, "message callback failed")
                    break
        except ConnectionClosed:
            pass
        except Exception as exc:
            print(
                f"[websocket_server] client {client_id} failed: "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )
            try:
                websocket.close(1011, "WebSocket handler failed")
            except Exception:
                pass
        finally:
            with self._state_lock:
                current = self._clients.get(client_id)
                if current is session:
                    self._clients.pop(client_id, None)
                disconnect_callback = self._disconnect_callback
            close_code = getattr(websocket, "close_code", None)
            close_reason = getattr(websocket, "close_reason", "") or ""
            self._invoke_callback(
                "disconnect",
                disconnect_callback,
                client_info,
                close_code,
                close_reason,
            )
            self._callback_context.run = None

    def _validate_payload(self, payload):
        """
        @description         : 校验待发送消息类型和 UTF-8 编码后的实际字节数
        @param payload       : str 文本消息或 bytes 二进制消息
        @return              : 消息字节数
        """
        if isinstance(payload, str):
            payload_size = len(payload.encode("utf-8"))
        elif isinstance(payload, bytes):
            payload_size = len(payload)
        else:
            raise TypeError("payload must be str or bytes")
        if payload_size > self._max_message_bytes:
            raise ValueError(
                f"payload exceeds {self._max_message_bytes} configured bytes"
            )
        return payload_size

    @staticmethod
    def _format_remote_address(remote_address):
        """
        @description         : 将 websockets 返回的远端地址转换成稳定的日志字符串
        @param remote_address : 远端地址元组、字符串或 None
        @return              : 远端地址字符串
        """
        if isinstance(remote_address, tuple):
            return ":".join(str(item) for item in remote_address)
        return str(remote_address) if remote_address is not None else "unknown"

    @staticmethod
    def _client_info(session):
        """
        @description         : 从内部会话生成不暴露 socket 和锁的客户端信息
        @param session       : 内部客户端会话字典
        @return              : 可安全交给上层使用的客户端信息字典
        """
        return {
            "client_id": session["client_id"],
            "remote_address": session["remote_address"],
            "path": session["path"],
            "connected_monotonic_s": session["connected_monotonic_s"],
        }

    @staticmethod
    def _invoke_callback(callback_name, callback, *arguments):
        """
        @description         : 隔离上层回调异常，避免单个回调破坏服务线程
        @param callback_name : 用于日志定位的回调名称
        @param callback      : 待调用函数，None 表示无需处理
        @param arguments     : 传递给回调的位置参数
        @return              : 回调成功或不存在返回 True，抛出异常返回 False
        """
        if callback is None:
            return True
        try:
            callback(*arguments)
            return True
        except Exception as exc:
            print(
                f"[websocket_server] {callback_name} callback failed: "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )
            return False

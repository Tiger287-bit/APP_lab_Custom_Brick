# WebSocket Server Brick

`websocket_server` 为 Arduino App Lab 提供 WebSocket 服务端，支持多个客户端连接、文本和二进制消息收发、广播及连接状态查询。消息内容由 App 的回调函数处理。

支持开发板：UNO Q、VENTUNO Q。Python 依赖：`websockets==17.1`。

## 接入 App

将整个 `websocket_server` 文件夹放入 App 的 `bricks/` 目录：

```text
bricks/websocket_server/
├── __init__.py
├── server.py
├── brick_config.yaml
├── requirements.txt
└── README.md
```

在 `app.yaml` 的 `bricks` 列表中加入：

```yaml
bricks:
  - websocket_server:
      variables:
        WEBSOCKET_SERVER_HOST: "0.0.0.0"
        WEBSOCKET_SERVER_PORT: "8765"
        WEBSOCKET_SERVER_PATH: "/ws"
```

App CLI 启动 App 时会安装依赖，并根据 `brick_config.yaml` 暴露端口。默认连接地址为 `ws://<开发板IP>:8765/ws`。

## 最小使用示例

将以下代码放入 App 的 `python/main.py`。示例会把收到的消息原样发回同一个客户端。

```python
import time

from arduino.app_utils import App
from websocket_server import WebSocketServer

server = WebSocketServer()
server.on_message(server.send)


def loop():
    """
    @description         : 检查服务状态并让出 CPU；服务异常时让 App 退出
    @param               : 无参数
    @return              : 无返回值；服务异常时抛出 RuntimeError
    """
    server.require_running()
    time.sleep(0.05)


try:
    App.run(user_loop=loop)
finally:
    server.request_stop()
```

`App.run()` 自动调用 Brick 的 `start()` 和 `stop()`。保留主循环中的 `require_running()`，让监听失败能够传递给 App；`finally` 中的 `request_stop()` 用于退出清理。

## 配置

连接配置的读取顺序为：构造参数 → 环境变量 → 默认值。App Lab 会将 `app.yaml` 中的 `variables` 注入环境变量。

| 构造参数 | 环境变量 | 默认值 | 用途 |
| --- | --- | --- | --- |
| `host` | `WEBSOCKET_SERVER_HOST` | `"0.0.0.0"` | 容器内监听地址 |
| `port` | `WEBSOCKET_SERVER_PORT` | `8765` | 监听端口 |
| `path` | `WEBSOCKET_SERVER_PATH` | `"/ws"` | 接受连接的请求路径 |
| `max_message_bytes` | `WEBSOCKET_SERVER_MAX_MESSAGE_BYTES` | `16384` | 单条消息上限，单位为字节 |
| `max_clients` | `WEBSOCKET_SERVER_MAX_CLIENTS` | `4` | 同时连接的客户端数量上限 |
| `ping_interval_s` | `WEBSOCKET_SERVER_PING_INTERVAL_S` | `10` | 连接保活 ping 的发送间隔，单位为秒 |
| `ping_timeout_s` | `WEBSOCKET_SERVER_PING_TIMEOUT_S` | `10` | 等待 pong 应答的时间，单位为秒 |

启停等待时间通过构造参数设置：

```python
server = WebSocketServer(start_timeout_s=5.0, stop_timeout_s=3.0)
```

`start_timeout_s` 必须为有限正数；`stop_timeout_s` 为有限非负数，设为 `0` 表示只请求停止。`start()` 和 `stop()` 本身不接受参数。

修改端口时，同步更新 `brick_config.yaml` 中的 `ports` 与监听配置，保证容器外能访问。

## Python API

### 消息和连接

| 方法 | 说明 |
| --- | --- |
| `on_connect(callback)` | 注册连接回调：`callback(client_info)` |
| `on_message(callback)` | 注册消息回调：`callback(client_id, payload)` |
| `on_disconnect(callback)` | 注册断开回调：`callback(client_info, code, reason)` |
| `send(client_id, payload)` | 向指定客户端发送消息；成功返回 `True`，连接不存在或发送失败返回 `False` |
| `broadcast(payload)` | 向所有当前客户端发送消息，返回成功发送的客户端数量 |
| `disconnect(client_id, code=1000, reason="")` | 主动关闭连接；找到连接并发起关闭返回 `True`，连接不存在返回 `False` |
| `get_clients()` | 返回当前客户端信息列表 |

三个回调注册方法返回当前实例，传入 `None` 可取消回调。`payload` 使用 `str` 表示文本消息，使用 `bytes` 表示二进制消息；文本大小按 UTF-8 编码后的字节数计算。发送类型错误抛出 `TypeError`，超过大小上限抛出 `ValueError`。

`client_info` 包含 `client_id`、`remote_address`、`path` 和 `connected_monotonic_s`。每次连接分配新的 `client_id`；`connected_monotonic_s` 是用于计算连接时长的单调时钟值。

回调运行在连接线程中，应快速返回；耗时任务可交给队列和专用线程。`send()`、`broadcast()`、`disconnect()` 是同步操作。

### 启停和状态

| 方法 | 说明 |
| --- | --- |
| `start()` | 监听就绪返回 `True`；失败或取消抛出 `RuntimeError`，超时抛出 `TimeoutError` 并请求取消启动 |
| `stop()` | 请求停止并等待清理；完成返回 `True`，仍在清理或清理失败返回 `False` |
| `request_stop()` | 请求停止，不等待；已经清理完成返回 `True`，否则返回 `False` |
| `require_running()` | 服务正常返回 `None`，否则抛出带原因的 `RuntimeError` |
| `get_status()` | 返回配置、客户端数量、运行状态和错误信息 |

重复 `start()` 复用同一次启动；重复 `stop()` 复用同一次清理。清理完成后可以重新启动，清理失败时应重启 App 进程。

停止等待默认最多 3 秒，这是一次调用的总预算。在连接回调中调用 `stop()` 会仅请求停止并返回 `False`。阻塞的回调或系统调用可能继续清理，清理完成前禁止重新启动。

`get_status()` 中的主要状态字段：

| 字段 | 含义 |
| --- | --- |
| `state` | `starting` 启动中、`running` 运行中、`stopping` 停止中、`stopped` 已停止、`failed` 失败 |
| `listening` | 监听已就绪且没有停止请求，用于判断服务是否可用 |
| `running` | 后台工作线程仍存在，启动中或清理中也可能为 `True` |
| `cleanup_pending` | 已请求停止，工作线程尚未退出，资源可能仍未释放 |
| `client_count` | 当前接入的客户端数量 |
| `server_error` | 启动或监听失败原因，正常时为 `None` |
| `cleanup_error` | 清理失败原因，正常时为 `None` |

## 连接约定

- 请求路径需匹配配置的 `path`，查询参数不参与比较；路径错误时以关闭码 `1008` 断开。
- 达到客户端数量上限后，新连接以关闭码 `1013` 断开。
- 连接或消息回调抛出异常时，记录错误并以关闭码 `1011` 关闭该连接。
- 当前提供 `ws://` 明文连接，适用于可信网络；需要 `wss://` 时可由反向代理提供 TLS。

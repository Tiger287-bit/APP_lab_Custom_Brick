# APP Lab Custom Brick

用于 Arduino App Lab 的自定义 Brick 工程。

## 可用 Brick

| Brick | 功能 | 文档 |
| --- | --- | --- |
| `websocket_server` | WebSocket 服务端，支持多客户端连接、文本和二进制消息收发、广播及连接状态查询 | [使用说明](websocket_server/README.md) |

## 使用方式

将所需的 Brick 文件夹复制到 App 的 `bricks/` 目录，并按照对应文档在 `app.yaml` 中添加配置。

WebSocket Server 的接入步骤、示例代码和 API 说明见 [websocket_server/README.md](websocket_server/README.md)。

# ClipFlow 公网轮巡工单中继

该目录是独立部署的公网边界服务。它只保存角色会话、最小工单投影、幂等命令和临时照片；步骤顺序、倒计时、确认、回退、Excel 生成、飞书写入和最终完成判断仍由内网 ClipFlow 后端负责。

## 启动

建议使用 Python 3.11+、FastAPI、Uvicorn、Pydantic 2 和 Pillow，持久化仅使用 Python 标准库 `sqlite3`。
当前 SQLite 版本按单实例/单 Uvicorn worker 部署；需要多实例时先改为共享数据库和对象存储。

PowerShell 示例：

```powershell
$env:PUBLIC_POLLING_RELAY_PUBLIC_URL = 'https://workorder.example.com'
$env:PUBLIC_POLLING_RELAY_DB = 'D:\ClipFlowRelay\relay.sqlite3'
$env:PUBLIC_POLLING_RELAY_UPLOAD_ROOT = 'D:\ClipFlowRelay\uploads'
python -m bin.public_polling_relay --host 127.0.0.1 --port 18767
```

当前 ClipFlow 程序只需公网地址：

```powershell
$env:CLIPFLOW_POLLING_RELAY_ENABLED = '1'
$env:CLIPFLOW_POLLING_RELAY_URL = 'https://workorder.example.com'
```

仓库当前仍固定使用局域网工单；上述环境变量不会自行启用公网连接。正式切换时，先将 `server.py` 中的 `POLLING_WORK_ORDER_PUBLIC_RELAY_ENABLED` 改为 `True`，再配置公网地址并重启。

`CLIPFLOW_POLLING_RELAY_CONNECTOR_ID` 可选，作为本机安装标识前缀；每次进程启动会再生成唯一实例 ID，防止旧进程和新进程共用 fencing 租约。仅本机联调 HTTP 时可设置 `CLIPFLOW_POLLING_RELAY_ALLOW_INSECURE_HTTP=1`，生产禁止使用。

生产环境使用一个 HTTPS 公网地址即可，页面和内部 API 都由同一服务提供。当前程序不需要站点 ID、共享密钥、客户端证书或私钥。由于内部 API 不再识别调用方，反向代理必须将 `/api/v1/internal/*` 限制为当前程序出口 IP 可访问。外部只开放 443，不要把原 ClipFlow `18766` 端口暴露到公网。只有本地纯 HTTP 测试时才设置：

```powershell
$env:PUBLIC_POLLING_RELAY_SECURE_COOKIE = '0'
```

上线前必须确认：

1. 公网 SQLite 和照片目录使用持久化磁盘，不放在容器临时层；
2. 公网服务器和 ClipFlow 电脑都已同步 NTP，时间误差小于 60 秒；
3. 反向代理已限制 `/api/v1/internal/*` 只允许当前程序的出口 IP；
4. 反代允许至少 10MiB 请求体，读超时不小于 75 秒，且不对内部接口做 301/307 跳转；
5. 先启动公网服务，再带上述 `CLIPFLOW_*` 环境变量重启当前程序。

如果当前程序未带 `CLIPFLOW_POLLING_RELAY_ENABLED=1` 启动，新工单仍会走原本地页面；后续再开启不会迁移已创建的本地工单。

## 公网角色链接

内网 connector 为每个角色生成独立的高强度 secret，只向注册接口发送其 SHA-256。公网链接使用 fragment，避免 secret 进入 HTTP、代理日志和 Referer：

```text
https://relay.example.com/polling-work-order#link_id=<link_id>&secret=<raw_secret>
```

页面用 `POST /api/v1/link-sessions/exchange` 交换成 `wo_session` Cookie。Cookie 为 `HttpOnly`、`SameSite=Strict`，生产默认 `Secure`；写请求还必须带交换结果中的 `X-CSRF-Token`。

本服务没有 OAuth，持有某条角色链接即代表该角色。两条角色链接和会话完全隔离；目标记录 ID、飞书 token、open_id、本地路径不会进入公网投影。
同一浏览器配置文件只有一个 `wo_session` Cookie；联调操作人和审核人时，请使用两个设备、两个浏览器或独立无痕窗口。

## 公网 API

所有 JSON 成功响应为：

```json
{"ok": true, "data": {}}
```

错误响应为：

```json
{"ok": false, "error": "可读错误", "error_code": "stable_error_code"}
```

主要接口：

- `POST /api/v1/link-sessions/exchange`
- `POST /api/v1/link-sessions/logout`
- `GET /api/v1/work-orders/session?after_revision=&wait_seconds=`
- `POST /api/v1/work-orders/commands`
- `GET /api/v1/work-orders/commands/{command_id}`
- `POST /api/v1/work-orders/uploads`
- `PUT /api/v1/work-orders/uploads/{upload_id}/content`
- `POST /api/v1/work-orders/uploads/{upload_id}/complete`
- `GET /api/v1/work-orders/photos/{photo_id}`

命令类型固定为：

- `activate`
- `release`
- `confirm`
- `rollback`
- `retry_attachment`

命令请求必须带 `Idempotency-Key` 和 `expected_version`。同一工单、角色和幂等键的相同请求返回原结果；同键不同正文返回 `409 idempotency_conflict`。内网权威租约超过 45 秒后，公网业务写入返回 `503 authority_offline`。

照片采用初始化、上传内容、完成三步；`complete` 会幂等生成内部 `attach_photo` 命令并返回 `command_id`，浏览器不能自行构造该命令。公网 `relay_ready` 不表示步骤已有照片；只有内网 ACK 后才变为 `authority_attached`。单张最多 8MiB，每步最多 5 张、每组最多 100 张且总量不超过 200MiB；仅接收 JPEG、PNG、WebP，并同时校验声明大小、实际大小、SHA-256、图片解码、40MP 像素和 12000px 边长。未完成的上传预留 15 分钟后释放，已完成公网暂存但尚未被内网确认的照片保留 30 分钟；已被内网确认的照片保留到工单撤销。

## 当前程序 API

内网 connector 只需主动出站 HTTPS，不需要公网回调或 NAT 端口映射。

租约接口无需身份头。获取租约后，其他内部请求只需额外携带 `X-Relay-Fencing-Token`。它用于拒绝旧进程写入，不是身份凭据。

内部接口：

- `POST /api/v1/internal/authority/lease`
- `PUT /api/v1/internal/groups/{public_group_id}`
- `GET /api/v1/internal/commands/lease?limit=&wait_seconds=`
- `GET /api/v1/internal/uploads/{upload_id}/content`
- `POST /api/v1/internal/commands/{command_id}/ack`
- `PUT /api/v1/internal/groups/{public_group_id}/projection`
- `POST /api/v1/internal/groups/{public_group_id}/cancel`

命令租取在同一公网工单内串行，不同工单可并行。租约带单调递增的 `authority_epoch` 和 fencing token，防止两台内网实例同时写入。ACK 和投影只接受当前 epoch，并拒绝投影 revision 或 authority version 回退。

注册正文中的 `links` 必须同时包含 `operator`、`reviewer`：

```json
{
  "protocol_version": 1,
  "registration_version": 1,
  "state": "active",
  "authority_version": 1,
  "projection_revision": 1,
  "projection": {},
  "links": {
    "operator": {"link_id": "...", "secret_sha256": "...", "generation": 1, "assigned_name": "..."},
    "reviewer": {"link_id": "...", "secret_sha256": "...", "generation": 1, "assigned_name": "..."}
  }
}
```

注册响应包含由 `PUBLIC_POLLING_RELAY_PUBLIC_URL` 生成的固定 `entry_url`，connector 用本地保存的 raw secret 拼接 fragment，不依赖请求 Host 头。

## 权威完成状态

公网 `steps_completed` 只是只读投影，不能放行结束通告。只有内网完成以下动作后，才应投影 `state=completed`、`completion.remote_verified=true`：

1. 全部步骤和本地照片校验完成；
2. Excel 生成成功；
3. 写入飞书目标表“工单附件”；
4. 点读目标记录并确认精确 file token；
5. 内网本地组状态落盘成功。

目标不存在、通告删除或外部结束时调用 cancel 接口，会撤销链接、会话、待处理命令和临时照片。

## 测试

```powershell
python -m unittest bin.public_polling_relay.test_relay -v
python -m py_compile bin/public_polling_relay/*.py
```

测试覆盖权威租约 fencing、角色链接隔离、命令幂等、照片上传/下载/ACK、投影单调性和取消失效。

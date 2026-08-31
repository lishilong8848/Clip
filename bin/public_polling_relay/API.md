# 轮巡公网工单中继 API v1

本文档只规定公网程序必须实现的路由、存储、校验和响应行为。公网程序只保存能力链接会话、最小工单投影、短命令和临时照片，不自行判定工单完成。

## 0. 唯一对接约定

公网端最终只交付一个无路径、无查询参数的 HTTPS 根地址，例如 `https://workorder.example.com`。不得要求对接方分别配置接口地址、回调地址或页面地址。

所有路由均固定为本文档所列的根相对路径：健康检查是 `/api/v1/health`，角色入口是 `/polling-work-order`，公开 API 使用 `/api/v1/...`，内部 API 使用 `/api/v1/internal/...`。注册响应中的 `entry_url` 必须固定等于 `<公网根地址>/polling-work-order`。公网端不得要求对接方开放入站端口。

## 1. 部署配置

公网服务必填：

```text
PUBLIC_POLLING_RELAY_PUBLIC_URL=https://workorder.example.com
PUBLIC_POLLING_RELAY_DB=<SQLite路径>
PUBLIC_POLLING_RELAY_UPLOAD_ROOT=<临时照片目录>
```

Uvicorn 建议只监听回环地址，由一个 HTTPS 公网地址同时提供手机页面、公开 API 和内部 API。公网程序不得要求站点 ID、共享密钥、客户端证书或私钥。由于内部 API 没有调用方凭据，生产反向代理必须限制 `/api/v1/internal/*` 的来源 IP。

## 2. 通用约定

- JSON 成功：`{"ok":true,"data":{...}}`
- JSON 失败：`{"ok":false,"error":"可读信息","error_code":"stable_code"}`
- 时间字段是 Unix 秒。
- 版本冲突返回 409；上游权威租约过期返回 503。
- 公网页面不做 OAuth。角色来自能力链接会话，请求正文不能指定或切换角色。
- `GET /api/v1/health` 必须返回 `service=public_polling_relay` 和 `protocol_version=1`。

## 3. 能力链接与公网会话

公网数据库只保存链接 secret 的 SHA-256，不保存明文。页面同时支持：

```text
/polling-work-order#link_id=<link_id>&secret=<secret>
/polling-work-order#link=<link_id>.<secret>
```

页面路由：

- `GET /polling-work-order`：只显示工单总览；
- `GET /polling-work-order/steps?run_index=1`：只显示所选工单的上一步、当前步和下一步。

### `POST /api/v1/link-sessions/exchange`

```json
{
  "link_id": "lnk_...",
  "secret": "raw-secret",
  "device_nonce": "可选、预留的设备标识"
}
```

成功后设置 `wo_session` Cookie（`Secure + HttpOnly + SameSite=Strict`），并返回：

```json
{
  "csrf_token": "...",
  "expires_at": 1788000000.0,
  "snapshot": {}
}
```

Cookie 滑动续期 12 小时。写请求必须同时携带 `X-CSRF-Token`。

`device_nonce` 当前只是向后兼容的预留输入，不绑定设备或人员，不能作为实际操作人身份证明。

### `GET /api/v1/work-orders/session`

查询参数：

- `after_revision`：已知投影版本；
- `wait_seconds`：0–30 秒长轮询。

版本无变化返回 204。有变化返回当前角色投影，核心字段为：

```json
{
  "role": "operator",
  "role_label": "操作人",
  "assigned_name": "张三",
  "state": "active",
  "authority_online": true,
  "authority_version": 18,
  "projection_revision": 24,
  "title": "E楼二次泵轮巡",
  "sop_name": "二次泵SOP",
  "current_run_index": 1,
  "can_release_selection": false,
  "can_rollback_previous": false,
  "work_orders": [],
  "steps": [],
  "pending_commands": []
}
```

`work_orders` 每项必须保留：

```json
{
  "run_index": 1,
  "from_unit": "1#",
  "to_unit": "2#",
  "label": "1#→2#",
  "step_count": 8,
  "completed_steps": 2,
  "state": "available",
  "selectable": true
}
```

`steps` 只传当前页所需的上一条、当前条和下一条，每项必须保留：

```json
{
  "step_key": "1:3",
  "global_index": 2,
  "run_index": 1,
  "run_count": 2,
  "run_label": "1#→2#",
  "step_index": 3,
  "step_count": 8,
  "content": "执行操作内容",
  "operator_required": true,
  "reviewer_required": true,
  "time_limit_seconds": 60,
  "position": "current",
  "operator_confirmed": true,
  "reviewer_confirmed": false,
  "timer_started": true,
  "confirm_available_at": 1788000060.0,
  "remaining_seconds": 17,
  "photos": [
    {
      "photo_id": "<upload_id>",
      "name": "photo.jpg",
      "content_type": "image/jpeg",
      "size": 1823344,
      "sha256": "<64位hex>",
      "status": "authority_attached",
      "preview_url": "/api/v1/work-orders/photos/<upload_id>"
    }
  ]
}
```

操作人确认后，审核人仍可在自己确认前拍照。审核人只有在操作人所需确认已完成、当前步骤已有至少一张权威照片且倒计时归零后才能确认。

### `POST /api/v1/link-sessions/logout`

需 Cookie、CSRF；撤销当前会话。

## 4. 公网命令

### `POST /api/v1/work-orders/commands`

请求头：

```text
X-CSRF-Token: ...
Idempotency-Key: <12–128位 [A-Za-z0-9_-]>
```

支持的命令：

```json
{"type":"activate","expected_version":18,"run_index":1}
{"type":"release","expected_version":19,"run_index":1}
{"type":"confirm","expected_version":20,"step_key":"1:2"}
{"type":"rollback","expected_version":21,"step_key":"1:3"}
{"type":"retry_attachment","expected_version":22}
```

角色由 Cookie 决定。相同工单、角色和幂等键的相同请求返回原命令，即使权威版本已增加；同键不同正文返回 `409 idempotency_conflict`。

新命令返回 202；相同幂等请求已进入终态时返回 200：

```json
{
  "command_id": "<32位hex>",
  "type": "confirm",
  "status": "pending",
  "expires_at": 1788000060.0,
  "created_at": 1788000000.0,
  "updated_at": 1788000000.0,
  "result": {},
  "error_code": "",
  "error": ""
}
```

未在 60 秒内被内部接口领取的命令变为 `expired`。已领取命令以执行租约为准，不会在执行中被原始 60 秒 TTL 截断。

### `GET /api/v1/work-orders/commands/{command_id}`

只能查看当前工单和当前角色创建的命令。状态：`pending | leased | succeeded | rejected | expired | retryable_error`。

## 5. 步骤照片

### `POST /api/v1/work-orders/uploads`

```json
{
  "step_key": "1:2",
  "expected_version": 20,
  "file_name": "photo.jpg",
  "content_type": "image/jpeg",
  "size": 1823344,
  "sha256": "<64位小写hex>"
}
```

返回 201：`upload_id`、`status`、`expires_at`、`content_url`、`complete_url`。同会话、步骤、版本和 SHA-256 重试会返回原 `upload_id`。未完成预留 15 分钟后释放。
完成公网暂存后，若 30 分钟内仍未被内部 ACK 确认则清理；`authority_attached` 照片保留到工单撤销。

限制：JPEG/PNG/WebP，单张 8MiB，每步 5 张，每组 100 张，总原图 200MiB，解码后 40MP，任一边 12000px。

### `PUT /api/v1/work-orders/uploads/{upload_id}/content`

请求头：`Content-Type`、`X-Content-SHA256`、`X-CSRF-Token`。正文是原始图片字节。公网端流式写入临时文件，校验大小、摘要、文件签名和解码尺寸后原子替换。

### `POST /api/v1/work-orders/uploads/{upload_id}/complete`

需 Cookie 和 CSRF。该接口自动幂等生成内部 `attach_photo` 命令，浏览器不能通过通用命令接口自行构造。新命令返回 202，已存在的终态命令返回 200：

```json
{
  "upload_id": "...",
  "status": "relay_ready",
  "command_id": "...",
  "command_status": "pending"
}
```

公网程序只能在收到成功的照片命令 ACK 及新投影后，将照片转为 `authority_attached`并显示。

### `GET /api/v1/work-orders/photos/{photo_id}`

任一有效的本工单角色会话可查看。响应使用 `Cache-Control: private, no-store`。

## 6. 内部请求

租约接口不需要身份头。获取租约后，其他内部接口必须携带当前 `X-Relay-Fencing-Token`。该 token 用于防止旧进程和新进程同时写入，不是调用方身份凭据。

## 7. 公网程序必须实现的内部接口

### `POST /api/v1/internal/authority/lease`

该接口不带 fencing token。

```json
{"instance_id":"<每次进程启动唯一>","previous_epoch":7}
```

返回：`instance_id`、`authority_epoch`、`fencing_token`、`expires_at`。租约默认 45 秒。同一且仍在线的 `instance_id` 续约必须复用原 `authority_epoch` 和 `fencing_token`；只有租约过期或换成新实例时才递增 epoch 并更换 token。另一实例在当前租约有效时获取会返回 409。

### `PUT /api/v1/internal/groups/{public_group_id}`

```json
{
  "protocol_version": 1,
  "registration_version": 1,
  "state": "active",
  "authority_version": 1,
  "projection_revision": 1,
  "projection": {"operator": {}, "reviewer": {}},
  "links": {
    "operator": {
      "link_id": "lnk_...",
      "secret_sha256": "<64位hex>",
      "generation": 1,
      "assigned_name": "张三"
    },
    "reviewer": {
      "link_id": "lnk_...",
      "secret_sha256": "<64位hex>",
      "generation": 1,
      "assigned_name": "李四"
    }
  }
}
```

`protocol_version` 必须为 1，否则拒绝注册。相同 `registration_version` 必须完全同正文；不同正文返回 409。响应同样返回 `protocol_version=1` 和固定 `entry_url`，不依赖 Host 请求头。

### `GET /api/v1/internal/commands/lease`

查询参数：`limit=1..100`、`wait_seconds=0..25`。响应 `data` 必须是 `{"commands": [...], "authority_epoch": 1, "lease_expires_at": 1788000000.0}`。同一公网工单一次只租出一条命令，不同工单可同批返回。每条命令至少包含：

```json
{
  "command_id": "<32位hex>",
  "public_group_id": "g_...",
  "role": "operator",
  "kind": "confirm",
  "type": "confirm",
  "expected_version": 20,
  "payload": {"step_key": "1:2"},
  "status": "leased",
  "lease_epoch": 1,
  "lease_until": 1788000600.0
}
```

命令执行租约默认 10 分钟，以允许 Excel 生成和飞书附件重试。照片命令还必须在顶层 `upload` 和 `payload.upload` 中提供同一份元数据：

```json
{
  "kind": "attach_photo",
  "role": "reviewer",
  "expected_version": 20,
  "payload": {
    "step_key": "1:2",
    "upload_id": "...",
    "upload": {
      "upload_id": "...",
      "file_name": "photo.jpg",
      "content_type": "image/jpeg",
      "size": 1823344,
      "sha256": "..."
    }
  }
}
```

### `GET /api/v1/internal/uploads/{upload_id}/content`

公网程序必须流式返回临时原图，并返回 `X-Content-SHA256`、`X-Public-Group-Id`、`X-Step-Key`、大小和 MIME 校验所需的精确数据。

### `POST /api/v1/internal/commands/{command_id}/ack`

```json
{
  "outcome": "succeeded",
  "authority_version": 21,
  "projection_revision": 25,
  "result": {},
  "error_code": "",
  "error": "",
  "projection": {"operator": {}, "reviewer": {}}
}
```

`outcome` 取值：`succeeded | conflict | rejected | retryable_error`。成功 ACK 必须带最新投影。服务端同时校验 authority epoch、当前进程 `instance_id`和命令执行租约。同一 `command_id` 和完全相同的 ACK 必须幂等返回原结果；只有同一命令提交不同 ACK 时才返回 409。

### `PUT /api/v1/internal/groups/{public_group_id}/projection`

```json
{
  "state": "active",
  "authority_version": 21,
  "projection_revision": 26,
  "projection": {"operator": {}, "reviewer": {}}
}
```

`projection_revision` 必须单调增加。同一 revision 和完全相同的投影是幂等成功；同 revision 不同内容才返回 409。公网服务会用已确认的临时上传恢复照片 `photo_id/preview_url`，后续确认投影不会丢失缩略图。

### `POST /api/v1/internal/groups/{public_group_id}/cancel`

```json
{"reason":"target_deleted"}
```

立即撤销两条能力链接、所有 Cookie 会话、待处理命令和临时照片。重复取消可安全重试。

## 8. 公网程序状态流程

1. 注册接口幂等保存工单 ID、两个角色链接哈希和初始投影。
2. 注册成功后，角色链接可交换为独立 Cookie 会话。
3. 只有权威租约有效时才允许新建操作命令；同一工单的命令串行租取，不同工单可并行。
4. 照片只有在收到成功 ACK 及新投影后才标记为 `authority_attached`。
5. `completed`、附件状态和步骤进度只接受内部投影，公网程序不自行推导或提前放行。
6. 取消接口必须立即撤销两条能力链接、Cookie 会话、待处理命令和临时照片，重复取消保持幂等。

## 9. 不可依赖的公网数据

- 公网程序不得用页面显示状态反向推导业务已完成；
- 未收到成功 ACK 的暂存照片不得计为有效步骤照片；
- 公网页面和 API 不得直接写飞书、生成 Excel 或修改权威工单步骤。

## 10. 兼容性验收

如果公网程序不直接使用仓库内的参考实现，交付前至少要对照：

- `bin/public_polling_relay/frontend.py`：总览页、步骤页和按钮启用规则；
- `bin/public_polling_relay/test_relay.py`：fencing、幂等、照片、投影和取消合同；
- `bin/test_polling_work_order_relay.py`：公网端到端契约。

正式验收必须使用两个独立浏览器会话走完：开始通告 → 两条角色链接 → 选择工单 → 倒计时 → 多照片 → 操作人/审核人确认 → 审核回退 → Excel 上传与精确 token 回读 → 允许结束 → 两条链接失效。


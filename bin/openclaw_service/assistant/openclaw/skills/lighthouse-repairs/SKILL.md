---
name: lighthouse-repairs
description: 维修单、关联事件、检修通告、CMDB设备、维修跟进与设备资料查询，以及维修单/跟进的填写准备。用于问未完成维修项目、维修进度、跟进条数或填写维修记录。
---

# 灯塔维修单与跟进

> 本说明不是实时数据，也不授予权限。工具返回名为准，OpenClaw 展示可能带 `lighthouse_` 前缀。

## 适用场景
维修项目数量/明细/状态、维修跟进条数、设备台账（CMDB）、关联事件与检修通告、费用备件；办理维修单或跟进填写。

## 查询口径
- 维修单以**自身 `record_id` 计数**，不按关联事件去重。
- 跟进条数是逐条跟进记录，不等于维修项目数；维修进度用已注册的只读接口读原记录（如 `GET /api/repair-management/records`、`/status` 或 `GET /api/repair-management/followups`），先查明 record_id，不用标题猜 ID。
- 泛问“检修工作”把待开始计划、进行中检修通告、未完成维修单分别说明，不相加为独立工作总数。
- 查询失败、未取得或无权限不能当作零条；分页应完整读取。

## discover 原生 API
- `discover(keyword="维修", group="维修单与跟进")` 找真实接口与 schema（列表、详情、跟进、候选、预填、绑定、写操作），不要猜路径。源码可验证的只读入口有 `GET /api/repair-management/records`、`GET /api/repair-management/records/{record_id}`、`GET /api/repair-management/status`、`GET /api/repair-management/followups`、`GET /api/repair-management/event-candidates`、`GET /api/repair-management/event-prefill`、`GET /api/repair-management/repair-candidates`、`GET /api/repair-management/cmdb-candidates`、`GET /api/repair-management/ledger-candidates`。
- **`POST /api/repair-management/prefill` 是写侧预填接口（创建/更新维修时由 prepare_business 编排调用），不是只读 GET**；纯查询或表单前置读取用上文 GET 候选/预填/记录详情，不要误用或声明为 GET。
- 查询、跟进均用 `query(operation={api_id, params, path_params})`，api_id 从 discover 返回中选取，不把带 `…` 的占位当 id。

## 记录 ID 与字段
- 维修项目/事件/检修通告/CMDB 各自独立 record_id；跟进先确定 `summary_record_id`，再用 `GET /api/repair-management/followups` 查项目 followups（返回实际 fields 和设备选项，`relation_mode=record_id`）。
- **CMDB 设备关联**按“本楼 + 空白楼栋”与“设备台账关联 ID”分别以不同跟进累计去重；不同去重口径不能混在一起计数。
- 撰写表单用 `prepare_business` 按原接口可编辑字段生成，不拿另一维修项目的设备选项；`options_source` 可选 repair_events/repair_notices/repair_projects/repair_devices 供搜索选择。

## 事件/检修重新绑定
- 重新绑定事件或检修时，**按新选择覆盖旧关系**，由原业务接口同步关联字段；不得遗留旧关系或自行拼接。
- 未收到原接口的保存成功确认前，不得声称“关联已保存/已成功”。

## 办理顺序/确认/超时
- 先 query 对应记录列表或单条详情，再 `prepare_business` 准备写操作；用户确认后由原接口执行，绝不直接写删多维记录。
- 修改原跟进先读原记录；仅当当前 schema 声明版本字段时才保留版本，**不规定所有维修保存都必有版本号**，以当前 schema 为准。
- 超时不代表成功或失败，不自动重发、不自动补建，不冒充已保存。

## 权限与禁止边界
- “维修自动补建”仅限某转检修事件 ID 尚无维修单的场景；不按标题或关联事件强制合并真实维修单，不自动创建替代维修单。
- 查询范围由登录权限限定，不得扩大楼栋。

## 调用例子
查询：`discover(keyword="维修")` → `query(operation={api_id:"GET /api/repair-management/records", params:{scope:"D"}})` 读列表 → 按返回的 `record_id` 用 `GET /api/repair-management/records/{record_id}` 读详情，或 `GET /api/repair-management/followups` 查跟进（api_id 以 discover 返回为准）。写操作：先 discover 找真实写接口 → 读取必要数据（记录详情/候选/event-prefill）→ `prepare_business` 依据 discover 返回的真实 schema 补齐表单 → 用户确认后由原接口执行 → 返回 `job_id`/`operation_id` 只能说是已受理，须按原任务状态查成功后方可称成功；超时只查询原任务，不自动重发新任务。
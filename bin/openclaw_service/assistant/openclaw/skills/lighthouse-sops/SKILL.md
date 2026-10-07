---
name: lighthouse-sops
description: SOP 模板与工单的配置及只读查询。用于问 SOP 模板、工单状态、步骤进度、延时提醒。工单逐步执行、回退、照片上传、激活仍在原工单入口办理。
---

# 灯塔 SOP 与工单

> 本说明不是实时数据，也不授予权限。工具返回名为准，OpenClaw 展示可能带 `lighthouse_` 前缀。

## 适用场景
SOP 模板的配置（查/建/改/删、延时提醒、循环配置）与工单状态/步骤只读查询；工单执行仍走原工单入口。

## 查询口径
- **SOP 是模板，工单是执行实例，步骤不是工单**；查询时区分模板/工单/步骤。
- 未结束工单数不等于步骤数；`completed_steps` 表示人员确认完成的步骤，不代表封面/附件已上传或通告可结束。
- 工单查询只用 `GET /api/assistant/work-orders`（`/api/polling-work-orders` 已被助手目录排除，不推荐、不调用原生工单执行端点）：按 scope、search、state（all/pending/active/upload_pending/completed/cancelled/stopped）或 work_type 查询；选定 `group_id` 读完整步骤，page 从 1 起、page_size ≤ 40。

## SOP 模板配置（CRUD）
- 已注册原接口：`GET/POST /api/polling-sops`、`PUT/DELETE /api/polling-sops/{sop_id}`、`POST /api/polling-sops/{sop_id}/attachments`。schema 以 discover 返回为准；没有单条 GET 详情接口。
- 创建/编辑用 `prepare_business` 原表单；修改前先用 `GET /api/polling-sops` 按楼栋、适用类型读取 items，从中选择真实 sop_id 及其完整 steps 配置。删除也经原接口确认后执行。
- **延时提醒是每个步骤元素内的 `delay_reminder_minutes`**：整数，`0` 关闭，`1–1440` 开启（分钟），不在 SOP 顶层；“完成后计时提醒”表示从该步骤完成后开始计时并提醒。
- **循环配置 `repeat_rules` 也位于每个 steps 元素内**（`from_step_id`、`to_step_id`、`count`），每步最多 10 条 rule、每条 `count` 为 1–10；循环从所在步骤之后追加该区间步骤，范围不超过该步及其之前，展开后单次工单步骤最多 100 步（源码 `POLLING_WORK_ORDER_MAX_EXPANDED_STEPS=100`）。按原配置展开即可，**不得自行推测重复次数、不改变工单执行协议**。
- 轮巡/工单执行、回退、照片上传、激活等仍由原工单入口办理，助手只读查询与模板配置。

## 记录 ID 与字段
- 用接口返回的 `group_id`/`sop_id`/步骤 index 定位，不猜 ID；`delay_status` 是原延时判断。
- `photo_count`、`photo_required`、`operator/reviewer` 姓名与工号可供只读展示；不索要签名图片或令牌。

## 办理顺序/确认/超时
- 查询：先列表后详情，带分页读完；超时不代表步骤完成，不自动确认。
- 配置：先读原 SOP，再 `prepare_business`；用户确认后由原接口执行，超时不代表已保存，不自动重发。

## 权限与禁止边界
- 不索取工单角色令牌、执行链接或照片 ID；需给原工单页面入口时只给登录权限内的入口，**不暴露角色令牌或免登录执行链接**。
- 查询范围由登录权限限定，不得扩大楼栋。

## 调用例子
查询：`discover(keyword="工单")` → `query(operation={api_id:"GET /api/assistant/work-orders", params:{state:"active", page:1, page_size:20}})` 读 `group_id` → 仍用该查询接口的 group_id 参数读取步骤与延时。配置 SOP：先通过 `GET /api/polling-sops` 的列表读取原 SOP 完整表单（保留 name、work_type、scope、version、steps 等原值），再 `prepare_business` 依据 discover 返回的真实 schema 生成编辑表单；`delay_reminder_minutes` 与 `repeat_rules` 只写入对应 steps 元素内，不放 SOP 顶层；用户确认后由原接口执行。schema 未完整取得时先补读必要数据；超时只查询原任务，不自动重发。

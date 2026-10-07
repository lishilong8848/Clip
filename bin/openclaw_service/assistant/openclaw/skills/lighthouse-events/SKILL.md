---
name: lighthouse-events
description: 事件（事件通告）查询与转检修。用于问今日发生事件、未闭环事件、事件明细或把事件标记转检修。事件新增、更新、结束不在助手办理。
---

# 灯塔事件通告

> 本说明不是实时数据，也不授予权限。工具返回名为准，OpenClaw 展示可能带 `lighthouse_` 前缀。

## 适用场景
事件“发生数量/明细/状态”；把某条事件标记转检修。事件默认指事件通告，不等于全部工作。

## 查询口径
- 今日发生事件按北京时间的事件发生时间筛选（`date_field=occurrence_time`），**含已结束记录**，不用当前待办数量替代。
- 未闭环事件才按当前状态；泛问“检修/工作”时按事件、检修计划、维修单各自分别说明，不相加为独立工作总数。
- 查询失败、未取得或无权限不能当作零。

## discover 原生 API
- 用 `discover(keyword="事件", group="事件")` 找真实接口。只读场景可用月度/列表接口 `query(operation={api_id:"GET /api/events/monthly", params:{date_field:"occurrence_time", scope:"D"}})`（带真实分页）；api_id 以 discover 返回为准。
- 标记转检修是原功能，先 `discover` 转检修接口（`POST /api/events/transfer-repair`）的真实 schema。

## 记录 ID 与字段
- 查询返回的是事件记录（含事件简述、发生/结束时间、状态等）；事件用它自己的 record_id。
- 转检修场景先 `query` **该月月度事件**确认目标事件，不用标题猜 ID；平台提供楼栋、月份和事件选择，不索要 record_id。

## 办理顺序/确认/超时
- **事件新增、更新、结束属 Qt 专用链路，助手不接入**，不能调用 workbench-actions 或其它通用动作替代，更不能伪装成维保通告。
- 助手仅可：查询事件 + 准备“标记转检修”。转检修只标记事件流向，不代表已创建维修单。
- 用户确认后由原接口执行；超时不代表已成功，不自动重发。

## 权限与禁止边界
- 查询范围由登录权限限定，补充“只看D楼”后不得扩大。
- 事件不通过通知绑定入口（notice-identity/bind）办理。

## 调用例子
`discover(keyword="事件", group="事件")` → `query(operation={api_id:"GET /api/events/monthly", params:{date_field:"occurrence_time", scope:"D", month}})` 确认目标事件（月份参数以 discover 返回为准）→ 明确转检修时 `prepare_business` 依据 discover 返回的真实 schema 补齐事件选择等字段（`POST /api/events/transfer-repair`）→ 用户确认后由原接口执行 → 接口返回只代表**已受理**，须按原任务/原记录状态确认成功后才报成功，不自动重发。
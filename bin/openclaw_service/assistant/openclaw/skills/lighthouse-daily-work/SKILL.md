---
name: lighthouse-daily-work
description: 楼栋日常任务、工作报告、晨会、交接链接与事项状态。用于问每日任务、晨会生成/发送、工作报告或交接信息。
---

# 灯塔日常工作

> 本说明不是实时数据，也不授予权限。工具返回名为准，OpenClaw 展示可能带 `lighthouse_` 前缀。

## 适用场景
日常任务清单、晨会表格生成/发送、工作报告、值班交接链接、事项状态。

## 查询口径
- 日常任务/报告/晨会**不是事件通告**，按相应日期及状态分别说明。
- “今日待办”用 `pending_work` 等通用口径；晨会只生成当天且需 H 楼权限。
- 未结束任务包含早于今天开始的事项；未读取、无权限不能当作零。

## discover 原生 API
- `discover(keyword="日常"/"晨会"/"日报"/"任务", group="日常工作")` 找真实接口与 schema。
- 生成前先读当天 preview（如 morning-meeting 的 preview 接口）可预填天气温度；路径以 discover 返回为准。
- 查询用 `query(operation={api_id, params})`，api_id 从 discover 返回中选取，不把带 `…` 的占位当 id。

## 记录 ID 与字段
- 报告/晨会按日期与楼栋定位，不猜 ID；晨会天气温度缺失不能猜成 0。
- 发送每日工作汇总时收件人由平台按姓名查找多选，不编造 open_id。
- 交接链接只读展示原入口，不采集密码或验证码。

## 办理顺序/确认/超时
- 晨会生成：先 `query` 当天 preview → `prepare_business` 展示日期、天气和干湿球温度表单 → 用户确认后由原接口生成。
- 发送汇总：`prepare_business` 准备原发送接口，选定收件人后执行。
- 生成/发送接口返回任务标识只代表**已受理**，须按原生成/发送任务状态查到最终成功后才能说“已生成/已发送”；**部分失败如实报告成功/失败数量**；超时只查询原任务，不自动重发。

## 权限与禁止边界
- 晨会生成需 H 楼权限，仅当天；交接/权限设置只给原页面（`/?admin=handover`、`/?admin=permissions`），不在模型填写。
- 查询范围由登录权限限定，不得扩大楼栋。

## 调用例子
`discover(keyword="晨会")` → `query(operation={api_id:"GET /api/daily-tasks/morning-meeting/preview", params:{date: 今天}})` 读 preview → `prepare_business` 依据 discover 返回的真实 schema 补齐日期、天气、干湿球温度等字段（如 `POST /api/daily-tasks/morning-meeting/generate`）→ 用户确认后由原接口执行 → 接口返回仅代表**已受理**，须按原生成任务状态查到最终成功后才说“已生成/已发送”；超时只查原任务，不自动重发。
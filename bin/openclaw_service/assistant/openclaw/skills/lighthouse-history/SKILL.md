---
name: lighthouse-history
description: 通告历史与归档会话的只读查询：已结束通告、历史操作、历史对话检索。用于问历史通告/历史操作或回忆以前交流。当前待办用 pending_work。
---

# 灯塔通告历史

> 本说明不是实时数据，也不授予权限。工具返回名为准，OpenClaw 展示可能带 `lighthouse_` 前缀。

## 适用场景
查询已结束通告及历史操作；用 `search_history` 回忆本账号压缩后的历史对话；当前活动事项状态以当前缓存为准。

## 查询口径
- 历史记录与当前未完成工作分开：**历史查询不用当前未完成数据代替**，反之亦然。
- 活动事项/当前状态必须重新查询，历史摘要不是当前事实。
- `search_history` 返回的是过往问答片段，非实时业务证据。
- 查询失败、未取得或无权限不能当作零条。

## discover 原生 API / 专用 tool
- 历史通告用 `discover(keyword="通告历史"/"history", group="通告历史")` 找真实接口与 schema，路径以 discover 返回为准；查询用 `query(operation={api_id, params})` 带分页/时间。
- 档案对话用 `search_history(keyword)`：简短关键词（≤100 字符）检索本账号已归档对话，返回该账号允许的 question/answer/at/scopes/status。

## 记录 ID 与字段
- 历史接口返回的记录用其自身 ID/时间定位，不猜 ID；`search_history` 返回 `total` 与本页 items，不代表完整业务数量。
- 历史操作区分“已发送/已结束”与“当前待办”，不混计。

## 办理顺序/确认/超时
- 先明确用户要“历史”还是“当前”：历史走历史接口，当前走 `pending_work` 或对应模块。
- 检索不到/超时时说明未知，不认为没有历史。
- 不回放历史中的写入操作；只读查询，不在历史上下文上自动重发。

## 权限与禁止边界
- 历史范围沿用本账号权限与楼栋限制，不得扩大；不泄露他人会话。
- `search_history` 只在本账号允许的文档内查找。

## 调用例子
查询：`discover(keyword="通告历史", group="通告历史")` → `query(operation={api_id:"GET /api/history-summary", params:{scope:"D", page:1, page_size:20}})` 分页读已结束历史记录（实际接口与参数以 discover 返回为准，不把中文占位当 api_id）；回忆对话时 `search_history(keyword="上周通告")`。
---
name: lighthouse-notices
description: 维保、检修、变更、轮巡、调整、上下电等非事件通告的查询、解析、绑定与发送准备。用于问待发计划、未结束通告、已发送次数、绑定目标或办理通告开始/更新/结束。
---

# 灯塔通告（非事件工作）

> 本说明不是实时数据，也不授予权限。工具返回名为准，OpenClaw 展示可能带 `lighthouse_` 前缀。

## 适用场景
维保/检修/变更/轮巡/调整/上下电通告的计划、待开始、未结束、已发送次数；办理开始、更新、结束；绑定源表或目标记录。事件通告见 `lighthouse-events`。
工作类型含：`maintenance` 维保、`repair` 检修、`change` 变更、`power` 上/下电、`polling` 轮巡、`adjust` 调整（以 `WORK_TYPE_LABELS` 为准，约束标题与类型映射）。

## 查询口径
- 未发=待开始计划；未结束=已开始的进行中通告；各自独立计数，不可相加成独立工作总数。
- 已发送条数与发送次数分开：条数按同一目标记录去重，另附成功发送次数。
- 普通“今日待办”用 `pending_work`；只有明确“今天发送/发布”才按发送时间过滤。
- 查询失败、未取得或无权限不能当作零。

## discover 原生 API
- 用 `discover(keyword="通告"/"维保"/"检修"/"变更"/"轮巡"/"调整"/"上下电", group="通告与事件"或"工作台")` 找到真实提交与查询接口及 schema，不要猜路径。
- 查询计划与未结束通告可 `discover` 后 `query(operation={api_id, params})` 对应 workbench/ongoing 列表接口，带真实分页。
- 有通告原文先 `parse_notice(text)` 调原解析器，再读 source 候选匹配。

## 记录 ID 与字段
- **开始时若尚无 `target_record_id`，由原流程创建一次并在成功后取回该 ID**；更新、结束复用上次成功返回的同一 `target_record_id`（外加 `active_item_id`），不创建替代记录，也不要求开始预先有 ID。
- 更新/结束前先用 `discover` 查询原未结束/进行中通告并绑定该 record_id。
- **计划关联与目标多维不是同一个 ID**：计划绑定时用计划源记录（对应 `source_record_id`），进行中改绑目标用 `target_record_id`+`active_item_id`；不要混用，具体字段以 discover 返回为准。
- 独立通告保留各自字段，不擅自套用其它通告类型。

## 办理顺序/确认/超时
- 删除整条通告：先查询真实目标，再准备 `POST /api/ongoing-items/delete`，沿用网页原删除接口、权限和二次确认，同步删除多维目标及网页/Qt条目。`POST /api/notice-undo/{undo_id}/apply` 只撤销一步操作并保留通告；不得用“撤销开始”代替“删除通告”。
- 发送、更新、结束仅可 `prepare_business` 准备原接口操作（如 `POST /api/workbench-actions` 的 `command_format=notice_command`），用户确认后由原业务流程执行，绝不直接写删多维记录。
- **发送接口返回 202 只表示任务已受理/排队**，不代表已发送成功；保存返回的任务标识，按原任务状态查到最终成功后才能说“办理成功”。多维写入与群消息发送分别说明；未配置转发群时原流程可不发送群消息，不能因此把已成功写入的通告判为失败。超时只查询原任务，不自动重发、不新增替代记录。
- 检修(repair/overhaul)通告与维修单联动时，**机柜上/下电等联动是独立同步/异步结果**：须按原任务或原记录单独核对；联动失败独立报告，不影响已成功的通告本身（通告成功只以原发送任务最终状态为准）。
- 先读原记录再准备表单，SOP 人员、计划关联、现场截图等原生要求不能绕过。

## 权限与禁止边界
- 查询范围由登录权限限定，不得扩大楼栋。
- 附件/封面等原业务要求不得用助手里手填 ID 代替；非事件通告不经过事件 Qt 链路。

## 调用例子
查询：`discover(keyword="通告", group="通告与事件")` → `query(operation={api_id:"GET /api/workbench", params:{scope:"D"}})` 读未结束列表，或按 discover 返回的 ongoing 列表接口分页读取。发送：先读原计划/来源数据（必要时 `parse_notice` 原文）→ `prepare_business` 依据 discover 返回的真实 schema 生成正式发送表单（action=start/update/end）→ 用户确认后由原接口执行。**start 若尚无 `target_record_id`，由原流程创建并在成功后取回**；update/end 复用成功返回的同一 ID 和 `active_item_id`。**接口返回 202 只算已受理，须查原 job 最终状态成功后才说“已发送成功”**；超时只查询原任务，不自动重发。

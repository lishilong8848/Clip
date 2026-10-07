---
name: lighthouse-cabinet-power
description: 机柜上下电：平面图、库存状态、待办批次、批量识别、逐柜证明、回退、文本回填、截图关联与汇总导出。用于问机柜状态、待办或办理上下电确认。
---

# 灯塔机柜上下电

> 本说明不是实时数据，也不授予权限。工具返回名为准，OpenClaw 展示可能带 `lighthouse_` 前缀。基线以平面图为准。

## 适用场景
机柜/包间上下电状态、操作历史、待办批次确认/回退/恢复、新建粘贴文本或识别批次、截图关联、文本回填、单楼本地导出与五楼月度归档。

## 查询口径
- 区分机柜存量状态（正式/测试/未上电，含来源）、上下电操作次数、待办批次状态及批次内机柜数。
- **“未确认”不只看批次 active**：以接口 `counts` 与行状态（confirmable/rollbackable/restorable 等）为准，批次内可含部分失败或已回退；不能仅凭 active 判定全部未确认或全部已确认。
- 普通今日待办用 `pending_work`；机柜详情或批次用 `query(operation={...})` 调对应接口并带真实分页。
- 查询失败、未取得或无权限不能当作零条。

## discover 原生 API
- `discover(keyword="机柜"/"上下电"/"待办", group="机柜上下电")` 找真实接口与 schema。常见操作：新建批次 `POST /api/cabinet-power/batches`、`POST /api/cabinet-power/batches/{batch_id}/text-apply`、`PATCH /api/cabinet-power/batches/{batch_id}`、截图 `apply`/`correct`、回退/恢复、`GET /api/cabinet-power/operations`、导出等；具体路径以 discover 返回为准。
- 写操作前先 `query(operation={api_id:"GET /api/cabinet-power/batches/{batch_id}", path_params:{batch_id: 真实id}})` 读取当前批次（含 version、confirmable/rollbackable/restorable 与每行状态）。版本号由平台自动带入，不向用户索要。

## 操作类型/失败原因
- 每柜当前可执行动作取决于**原接口返回的当前状态**（已上电/未上电/状态不明），不自动猜测或套用固定转换；表单按该状态给出可选动作。
- 业务结果选“失败”时必须填写 `failure_reason`。接口校验失败、回退失败或网络超时不是业务结果“失败”，不要据此改写机柜结果。
- 版本冲突后让用户重新核对原批次，不自动覆盖或重发。

## 汇总与导出
- 平面图/布局基线保留；模板宏与上下电时间统计保留。
- **“只保留机柜上电汇总表”仅指删除邮件/阿里统计的双汇总**：不删除平面图、上电时间统计、下电时间统计及模板宏，也不新增其它统计口径。
- 单楼导出仅本地；五楼层月度归档按“一条年月记录”覆盖，不重复另建多条；不存在每月阿里统计。

## 办理顺序/确认
- 确认/回退/恢复：先读完整批次，再 `prepare_business` 由表单给出整批（`body.all=true`）或机柜多选，用户确认后原接口执行。
- 截图只关联本批已有机柜，不新增机柜；PDF/图片识别可新建批次；文本回填只补现存批次，不创建新批次。

## 权限与禁止边界
- 查询范围由登录权限限定；基线以平面图为准，不猜漏识别机柜。

## 调用例子
`discover(keyword="机柜", group="机柜上下电")` → `query(operation={api_id:"GET /api/cabinet-power/batches/{batch_id}", path_params:{batch_id}})` 读详情确认目标 → `prepare_business` 根据 `POST /api/cabinet-power/batches/{batch_id}/confirm` 的真实 schema 准备确认表单；更正字段才使用 PATCH，不能以 PATCH 代替正式确认 → 用户确认后由原接口执行 → 按原批次/原任务状态确认完成后才报成功，部分失败如实报告成功/失败数量，超时只查原任务。

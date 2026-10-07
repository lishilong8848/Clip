---
name: lighthouse-water
description: 各楼水耗记录、水表读数、图片、查询修改与汇总。用于问水耗数量/明细、办理水耗录入或修改、查询楼栋用水情况。
---

# 灯塔水耗管理

> 本说明不是实时数据，也不授予权限。工具返回名为准，OpenClaw 展示可能带 `lighthouse_` 前缀。

## 适用场景
水耗记录查询/汇总、水表与频次/班次、日期读数、图片核对；管理员新增水耗，普通账号编辑已录记录。

## 查询口径
- 耗水量需**单位与日期**，缺少口径先追问，不自行假定今天。
- 已录入水耗不是未完成任务；区分记录数、录入数与用水量（含单位）。
- 按楼栋查询，`scope_mode=single` 时逐楼检查分页与截断，某楼失败不作零。

## discover 原生 API
- `discover(keyword="水耗"/"水表"/"用水", group="容量与水耗")` 找真实接口与 schema。
- 录入先 `query(operation={api_id, params})` 该楼 bootstrap 读选项；修改还须读原记录完整详情（路径以 discover 返回为准）。
- 查询用 `query(operation={api_id, params})`，api_id 从 discover 返回中选取，不把带 `…` 的占位当 id。

## 记录 ID 与字段
- 记录用接口返回的 `record_id`，不猜 ID；保存准备 `prepare_business` 原 POST/PATCH 接口，平台展示水表、频次、班次下拉及日期、读数、照片上传控件。
- 新照片先由平台暂存再调用原保存接口，不索要 upload_ids；不得覆盖旧照片，不猜版本号。

## 办理顺序/确认/超时
- 仅管理员可新增；普通账号沿用原编辑次数限制。
- 大幅变化由平台要求填写异常原因并沿用原操作编号继续，**不得预先设 large_change_confirmed 跳过核对**。
- 用户确认后由原接口保存；接口返回任务/上传标识只代表**已受理**，须按原任务或原记录状态查到最终成功后才能说“已保存/已上传”。
- **部分失败如实报告成功/失败数量**，不整单冒充成功；超时只查询原任务或原记录，不接受自动重发新任务。

## 权限与禁止边界
- 查询范围由登录权限与楼栋限定，不得扩大。
- 水耗图片/读数只在核对用途展示，不作为凭据外传；不能编造读数或单位换算。

## 调用例子
查询：`discover(keyword="水耗", group="容量与水耗")` → `query(operation={api_id:"GET /api/capacity/water/bootstrap", params:{scope:"D"}})` 读该楼选项，再 `query(operation={api_id:"GET /api/capacity/water/records", params:{scope:"D", page:1, page_size:20}})` 读列表；修改时 `query(operation={api_id:"GET /api/capacity/water/records/{record_id}", path_params:{record_id}})` 读原记录详情。写操作：按 discover 返回的真实写接口（如 `POST /api/capacity/water/records`、`PATCH /api/capacity/water/records/{record_id}`）用 `prepare_business` 依据 schema 补齐水表、频次、班次、日期、读数等字段 → 用户确认后由原接口执行 → 按原任务/记录状态确认完成后才报成功；超时只查原任务。
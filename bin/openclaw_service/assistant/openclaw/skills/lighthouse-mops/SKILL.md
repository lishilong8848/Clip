---
name: lighthouse-mops
description: 维护单（MOP）：填写、已选人员姓名占位、服务端正式签名、文件生成和通告关联。用于问维护单状态、办理填写或上传已签文件。
---

# 灯塔维护单

> 本说明不是实时数据，也不授予权限。工具返回名为准，OpenClaw 展示可能带 `lighthouse_` 前缀。

## 适用场景
维护单（MOP）查询、填写、上传已签名本地文件、关联维保通告；正式文件生成与服务端签名。

## 查询口径
- 维护单、生成文件与维保通告是不同对象，不混计数量/状态。
- 维护单按楼栋与状态（待处理/已生成/已上传等）分别说明；未上传不等于无待办。
- 查询失败、未取得或无权限不能当作零。

## discover 原生 API
- `discover(keyword="维护单"/"MOP", group="维护单")` 找真实接口。填写或上传先 `query` 该楼 preview/bootstrap（路径以 discover 返回为准）读当前工作表与人员目录，不手填人员 ID。
- 查询用 `query(operation={api_id, params})`，api_id 从 discover 返回中选取。

## 记录 ID 与字段
- 维护单按原记录与楼栋；已有通告须携带原 `notice_key`，签名使用上下文按维护单附件自动生成。
- 表单展示当前工作表的日期、文本、勾选项、普通单元格和实施人/审核人选择；`local_file_path` 使用 document 文件引用，不索要签名位置或 JSON。

## 办理顺序/确认/超时
- 先读 preview/bootstrap，再 `prepare_business` 准备填写/上传；用户确认后由**原生成/上传接口**执行。
- 原本人确认和签名可用性由原接口校验，不能用姓名文字替代真实签名。
- 生成/填写/上传接口返回任务标识只代表**已受理**，须按原任务或原维护单状态查到最终成功后才能说“已生成/已上传”；**部分失败如实报告成功/失败数量**；超时只查询原任务，不自动重发或重复生成。

## 权限与禁止边界
- 正式签名仅由服务端写入文档，助手不采集、不展示、不猜签名图片。
- 修改涉及表单时按当前 schema 处理版本：仅在声明版本字段时保留原版本；未提供新信息时表单载入原值，不丢未修改字段。
- 维护单不是维保通告，不合并计数。

## 调用例子
查询：`discover(keyword="维护单", group="维护单")` → `query(operation={api_id:"GET /api/engineer/mop/bootstrap", params:{scope:"D"}})` 或 `query(operation={api_id:"GET /api/engineer/mop/preview", params:{scope:"D"}})` 读当前工作表与人员目录。填写/上传：先读 preview → `prepare_business` 依据 discover 返回的真实 schema 补齐日期、文本、勾选、人员、`local_file_path` 等字段（如 `POST /api/engineer/mop/fill`、`POST /api/engineer/mop/upload-local`、`POST /api/engineer/mop/upload-signed`）→ 用户确认后由原接口执行 → 按原任务/原维护单状态确认完成后才报成功；未完整取得写入 schema 时不手写 `body:{}`；超时只查原任务。
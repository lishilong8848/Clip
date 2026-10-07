---
name: lighthouse-drills
description: 演练模板、楼栋执行任务、步骤签名人数、审核人和评估人、执行填报与模板配置。用于问演练任务状态、步骤或办理演练填报/模板修改。
---

# 灯塔演练

> 本说明不是实时数据，也不授予权限。工具返回名为准，OpenClaw 展示可能带 `lighthouse_` 前缀。

## 适用场景
演练定义/模板、楼栋执行任务状态、记录表和评估表；管理员上传新演练模板、填报执行、修改未发布模板。

## 查询口径
- 区分演练定义（模板）、楼栋执行任务、记录表/评估表；一份执行任务按执行状态计，模板是配置不是任务。
- 管理员查模板用查询不传 scope（按 month 筛选），带 scope 返回的是楼栋已发布任务，不是模板配置。
- 查询失败、未取得或无权限不能当作零。

## discover 原生 API
- `discover(keyword="演练", group="演练")` 找真实接口。只读可用 `query(operation={api_id, params})` 查模板列表（month）与楼栋执行详情 `GET /api/drills/{drill_id}/execution`；人员目录可由 `GET /api/drills/bootstrap` 读取，路径以 discover 返回为准。
- 管理员上传新模板用 `discover` 后 `prepare_business` 展示名称、月份、参演楼栋和单个 .xlsx 文件选择。

## 记录 ID 与字段
- 模板用 `drill_id`，执行任务按 `drill_id`+楼栋标识；步骤签名人数由模板配置决定，指挥人计入参演人数，审核人与评估人共用一人，两个时间分别填写。
- 不对用户索要内部 JSON；`prepare_business` 表单按原接口字段生成。

## 办理顺序/确认/超时
- 填报表单先读该楼 execution，保存后再生成文件，**引用保存结果的 version，不沿用旧版本**。
- 管理员改模板先读未发布模板，再 `prepare_business` PUT configuration；有执行记录的模板不可修改，不能用 execution 填写接口改模板。
- 生成/上传/同步接口返回任务标识只代表**已受理**，须按原任务（如 `POST /api/drills/{drill_id}/generate` 对应的生成任务或原记录）状态查到最终成功后才能说“已生成/已上传”；**部分失败如实报告成功/失败数量**；超时只查询原任务，不自动重复生成、不自动发新任务。

## 权限与禁止边界
- **演练网页只显示姓名**，正式文件由服务端生成签名，不在助手展示或采集签名图片。
- 查询范围由登录权限与参演楼栋限定，不得扩大。

## 调用例子
查询：`discover(keyword="演练", group="演练")` → `query(operation={api_id:"GET /api/drills", params:{month}})` 读模板，或 `query(operation={api_id:"GET /api/drills/{drill_id}/execution", path_params:{drill_id}, params:{scope:"D"}})` 读该楼 execution。填报/生成：先读该楼 execution → `prepare_business` 依据 discover 返回的真实 schema 补齐执行状态、评估/审核人等字段（`PUT /api/drills/{drill_id}/execution`）或生成文件（`POST /api/drills/{drill_id}/generate`）→ 用户确认后由原接口执行 → 按原任务/原记录状态确认完成后才报成功；超时只查原任务。
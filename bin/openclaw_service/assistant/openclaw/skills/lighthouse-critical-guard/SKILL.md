---
name: lighthouse-critical-guard
description: 重保（重要保障）任务、按楼检查表、填写与提交情况、天气相关检查。用于问重保任务数、未填楼栋、填写状态或办理重保填报与模板。
---

# 灯塔重保管理

> 本说明不是实时数据，也不授予权限。工具返回名为准，OpenClaw 展示可能带 `lighthouse_` 前缀。

## 适用场景
重保任务、楼栋检查表、填写/未提交楼栋、模板配置、物资联络清单源文件、签名使用确认。

## 查询口径
- 区分重保任务与楼栋检查表；**检查表份数不能代替任务数**。
- 任务数/未填楼栋/已填未提交：以任务列表及 `GET /api/critical-guard/tasks/{task_id}` 详情里的楼栋 response、真实 task_id/response_id/scope 和填写/提交状态为准；没有独立的 responses 列表读取接口。查询失败、未取得或超时不能当作零。
- 重保待办只在 `pending_work` 有该模块且可读时按其返回计数；未读、无权限仍提示未知，不按零隐藏。
- scope_mode=single 的列表逐楼检查 ok/truncated/分页，某楼失败不作零。

## discover 原生 API
- `discover(keyword="重保", group="重保管理")` 找真实接口与 schema。
- 任务详情用 `query(operation={api_id:"GET /api/critical-guard/tasks/{task_id}", path_params:{task_id: 真实id}, params:{scope:"D"}})`，读取该楼真实 `response_id` 与版本。
- 楼栋填报原接口为 `PUT /api/critical-guard/responses/{response_id}`，response_id 必须取自任务详情；prepare 时平台保留原 cells、签名与版本，schema 以 discover 返回为准。

## 记录 ID 与字段
- 用接口返回的 `task_id`/`response_id`/`scope` 定位，不猜 ID；填报先读任务详情，不向用户索要 JSON。
- 物资/联络清单沿用 source-files 文件上传，连续生成须引用上传结果 cells 及实际 version，不能写回旧 source_file_id。

## 办理顺序/确认/超时
- 填报：先 query 任务详情（该楼真实 response 与 version），再 `prepare_business` 准备 PUT responses；平台生成检查日期、结果、备注及检查人选择。
- 修改楼栋重保检查模板先读 scope-template 的 revision；同时应用当前填报还须读原 response 与 version。
- 生成/上传（如源文件、归档、连续生成）接口返回任务标识只代表**已受理**，须按原任务或原记录状态查到最终成功后才能说“已生成/已上传”；**部分失败如实报告成功/失败数量**；超时只查询原任务，不自动重发或新增任务；异常备注/签名授权仍按原规则校验。

## 权限与禁止边界
- 签名授权与天气状态只读可用；不采集本人签名图片。
- 查询范围由登录权限与楼栋限定，不得扩大；物资只保留原文件通道。

## 调用例子
查询：`discover(keyword="重保", group="重保管理")` → `query(operation={api_id:"GET /api/critical-guard/tasks", params:{scope:"D"}})` 读任务列表 → `query(operation={api_id:"GET /api/critical-guard/tasks/{task_id}", path_params:{task_id}, params:{scope:"D"}})` 读该楼真实 `response_id` 与版本。填报/上传：先读任务详情 → `prepare_business` 按 `PUT /api/critical-guard/responses/{response_id}` 的真实 schema 补齐检查日期、结果、备注和检查人等字段，或准备源文件上传 → 用户确认后由原接口执行 → 按原任务/原记录状态确认完成后才报成功；未完整取得写入 schema 时先补读必要数据；超时只查原任务。

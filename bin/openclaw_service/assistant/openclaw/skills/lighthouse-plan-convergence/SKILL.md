---
name: lighthouse-plan-convergence
description: 计划收敛审查：智航认证、核对台、规则配置、本地规则库与未结束检修通告核对。用于问收敛规则、屏蔽记录、场景核验或修改规则集。
---

# 灯塔计划收敛审查

> 本说明不是实时数据，也不授予权限。工具返回名为准，OpenClaw 展示可能带 `lighthouse_` 前缀。

## 适用场景
智航告警/设备/规则目录、屏蔽记录与快照、规则视图、场景核验（Excel 比较）、规则集新建与配置。

## 查询口径
- 规则、核对结果、告警命中数与事件数分开，**不把命中规则数当事件数**。
- 区分规则集、屏蔽记录、实例快照与规则视图，分别查询。
- 当前未结束检修通告核对是只读操作，不用历史快照代替当前状态。
- 查询失败、未取得或无权限不能当作零条。

## discover 原生 API
- `discover(keyword="收敛"/"智航"/"规则", group="计划收敛审查")` 找真实接口与 schema。
- **`query` 是只读工具，不能直接执行上传 POST**。场景核验的 Excel（`POST /api/plan-convergence/excel`）上传与解析、工作表比较走**原受控表单**：`prepare_business` 展示文件选择与场景工作表，确认后由原接口执行。
- 实例快照与规则视图先读屏蔽记录详情，再从原明细选择；路径以 discover 返回为准。

## 记录 ID 与字段
- 规则集用返回的 `id`，不向用户索要设备/告警 ID；平台复用原四列选择器和公共/普通分组。
- 平台保留全部原行与语义，不手写 scenarios、不改语义键。

## 办理顺序/确认/超时
- 修改规则集：先读原 `rulesets/{id}` 详情，再 `prepare_business` 准备 PUT；新建并配置用 POST rulesets 再 PUT rulesets/{id}（前一步创建结果的 id 用 `$result` 引用），只展示一次配置表单。
- 仅新建空集则单独 POST，创建接口不保存 items。
- 用户确认后由原接口执行；超时不代表已保存，不自动重发。

## 权限与禁止边界
- 收敛核对是只读；规则/屏蔽记录选择器不让用户填 ID。
- 查询范围由登录权限限定，不得扩大；不直接写删多维记录。

## 调用例子
`discover(keyword="收敛规则")` → `query(operation={api_id:"GET /api/plan-convergence/rulesets/{id}", path_params:{id}})` 读原详情 → `prepare_business` 依据 discover 返回的真实 schema 补齐规则集字段（PUT `GET/PUT /api/plan-convergence/rulesets/{id}`），用户确认后由原接口执行；schema 未完整取得时不手写 `body:{}`，先补读原详情。场景核验的 Excel 走 `prepare_business` 原上传表单，不经只读 query 执行 POST。
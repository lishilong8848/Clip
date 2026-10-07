---
name: lighthouse-people-signatures
description: 人员、飞书消息与签名：按姓名工号查找收件人，将会话文字和文件发送给本人或指定人员；签名管理走原页面，签名使用确认独立申请。
---

# 灯塔人员与签名管理

> 本说明不是实时数据，也不授予权限。工具返回名为准，OpenClaw 展示可能带 `lighthouse_` 前缀。

## 适用场景
查询可授权的姓名/工号；将当前或较早的会话文字、文件发送到飞书；选择签名人员、发起签名使用确认；解释签名管理入口。

## 查询口径
- 人员记录只投影出 `name/姓名`、`employee_no/staff_no/工号`、`scope/building` 等允许字段。
- **身份证、住址、私密联系方式、凭证和真实签名图片一律不披露**；可提供权限内的工号姓名。
- 签名管理全部业务仅使用原页面（如 /signature-management），不在助手办理。

## discover 原生 API / 专用 tool
- 普通业务表单选择签名人员仍走原业务流程，可 `discover(keyword="签名"/"人员")` 找原入口与接口，路径以 discover 返回为准。
- 签名使用确认用 `prepare_business` 准备原发送接口 `POST /api/signatures/usage-confirmations/send`，携带原 scope、notice_key（维护单可给原通告 key）及 context_type=mop 或 critical_guard。

## 记录 ID 与字段
- 用原记录/人员选择器交平台，不索取人员 ID；usage-confirmations 平台生成用途和人员选择，不编造链接或要求手输人员 ID。
- 维护单先读 mop/bootstrap 与 mop/preview；重保先读 tasks/{task_id} 原详情。

## 办理顺序/确认/超时
- 会话文字或文件发送到飞书：用 `GET /api/message-delivery/recipients` 按姓名、工号查找可直接收消息的人员。目录复用本地缓存，发送只核对已选人员，不全表重拉。
- 用 `POST /api/message-delivery/send` 准备发送清单；`recipient_ids` 使用原人员记录ID，发给自己用 `__self__`，不填写 open_id。外部账号、离职/异动、无 open_id 者不发送。
- “前面那份报告”等指代用 `search_history` 按主题、时间、文件名定位，不默认上一条。多段文字和多个文件可多选；不明确时提供候选，确认前不发送。
- 文件用已有会话文件或原鉴权下载接口取得原文件，不发送本机链接。发送结果以原任务为准，失败通过原任务重试，仅补未成功部分。
- 发送签名使用确认只发请求，**不代表签名已授权，也不能代替本人批准**。
- 用户确认后由原接口执行；超时不代表已发送或已授权，不自动补发。

## 权限与禁止边界
- 密码、权限、本人签名采集只给原安全页面（`/?admin=status`、`/?admin=permissions`、`/?admin=handover` 等），不在模型填写。
- 签名管理不调用 prepare_business；不索取验证码、签名图片或凭证。

## 调用例子
`discover(keyword="签名")` → 读取原记录/附件（维护单读 mop/bootstrap 与 mop/preview，重保读 tasks/{task_id}）→ `prepare_business` 依据 discover 返回的真实 schema 补齐 `scope`、`context_type`、`notice_key` 等字段（`POST /api/signatures/usage-confirmations/send`），用户确认后原接口执行 → 返回结果只代表**已受理**，发送结果按原任务逐人核对，**部分失败如实报告成功/失败数量**，超时只查原任务、不自动补发。

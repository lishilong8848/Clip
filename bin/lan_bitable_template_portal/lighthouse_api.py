# -*- coding: utf-8 -*-
"""Inspecting the already-registered FastAPI routes into a business API catalogue.

This module is *not* a DOM adapter.  It reads the FastAPI route table and, where
present, the Pydantic request models and endpoint source (AST) to build safe
descriptors, validate operations against the real request model, and invoke
writes through an in-process ASGI client that reuses the caller's cookie/Origin.

The module only knows descriptors and validation.  It never bypasses existing
auth guards and never executes business logic on its own: ``invoke`` is intended
to be used only after the Codex orchestrator has confirmed a write.
"""

from __future__ import annotations

import asyncio
import ast
import inspect
import json
import math
import re
import textwrap
from contextlib import suppress
from copy import deepcopy
from typing import Any
from urllib.parse import quote, unquote

import httpx
from fastapi.routing import APIRoute
from pydantic import BaseModel, TypeAdapter, ValidationError

from .lighthouse_ai import AssistantError, safe_data, safe_text

_TEXT_CAP = 500_000
_BINARY_MAX = 20 * 1024 * 1024
_EXCEL_MAX = 8 * 1024 * 1024

# ---------------------------------------------------------------------------
# Classification rules
# ---------------------------------------------------------------------------

_EXCLUDED_PREFIXES = (
    "/api/assistant/",
    "/api/assistant",
    "/api/auth/",
    "/api/auth",
    "/api/backend/",
    "/api/backend",
    "/api/health",
    "/api/qt/",
    "/api/handover-links-auth",
    "/api/handover-password-reset/",
    "/api/plan-convergence/settings",
    # Native polling work-order execution is user-facing only (session/photo/
    # confirm/rollback/activate/release/retry/resend). It stays out of the
    # catalogue; SOP CRUD and delay status remain discoverable.
    "/api/polling-work-orders",
)
# Only system/auth/credential/diagnostic admin internals are excluded; business
# admin functionality (MOP settings, notice-memory scans, signature people and
# templates...) stays in the catalogue so frontend business features are covered.
_ADMIN_EXCLUDED_PREFIXES = (
    "/api/admin/system",
    "/api/admin/auth",
    "/api/admin/credential",
    "/api/admin/diagnostic",
    "/api/admin/session",
    "/api/admin/token",
    "/api/admin/secret",
    "/api/admin/shutdown",
    "/api/admin/backup",
)
_EXCLUDED_PATHS = (
    "/api/health",
    "/api/backend/shutdown",
    "/api/admin",
)
# Raw signature images, temporary credentials/sessions/tokens and external
# cross-context writes must never leak into the business catalogue.
_SIGNATURE_EXCLUDED_PREFIXES = (
    "/api/signatures/image",
    "/api/signatures/temporary/",
    "/api/signatures/temp",
    "/api/signatures/external/",
    "/api/signatures/send-link",
    "/api/signatures/usage-confirmations/",
    "/api/signatures/usage-confirmations",
    "/api/signatures/usage-confirm",
    "/api/signatures/save",
    "/api/signatures/management/request",
    "/api/signatures/management/submit",
    "/api/signatures/management/temporary",
)
# The native dispatcher also serves token sessions and raw signature submission.
# Catalogue only its verified business operations, never the open placeholder.
_SIGNATURE_MANAGEMENT_OPERATIONS = {
    "GET": ("people", "duplicates"),
    "POST": ("refresh", "associate", "merge", "migrate"),
}
_SIGNATURE_BUSINESS_PATHS = frozenset({
    "/api/signatures/temporary/people", "/api/signatures/temporary/list",
    "/api/signatures/usage-confirmations/send",
})
# SSE streams and internal remote/update/system endpoints are not business ops.
_STREAM_AND_INTERNAL = (
    "/stream",
    "/shutdown",
    "/api/system",
    "/api/systems/",
    "/api/remote",
    "/api/update",
    "/api/version",
    "/api/runtime",
)

_GROUP_BY_PREFIX = (
    ("/api/assistant/pending", "未完成工作"),
    ("/api/admin/mop-settings", "维护单"),
    ("/api/admin/notice-memory", "通告历史"),
    ("/api/notice", "通告与事件"),
    ("/api/workbench", "工作台"),
    ("/api/events", "事件"),
    ("/api/ongoing-items", "通告与事件"),
    ("/api/maintenance-actions", "通告与事件"),
    ("/api/repair", "维修单与跟进"),
    ("/api/cabinet-power", "机柜上下电"),
    ("/api/capacity", "容量与水耗"),
    ("/api/drills", "演练"),
    ("/api/daily", "日常工作"),
    ("/api/learning", "画像学练"),
    ("/api/plan-convergence", "计划收敛审查"),
    ("/api/polling", "轮巡工单"),
    ("/api/critical-guard", "重保管理"),
    ("/api/engineer/mop", "维护单"),
    ("/api/signature", "人员与签名管理"),
    ("/api/records", "记录"),
    ("/api/scope-overview", "楼栋概览"),
    ("/api/history-summary", "通告历史"),
    ("/api/history-memory", "通告历史"),
    ("/api/daily-tasks", "日常工作"),
    ("/api/change-confirmations", "变更确认"),
    ("/api/jobs", "后端任务"),
    ("/api/bootstrap", "门户引导"),
    ("/api/maintenance-refresh", "通告与事件"),
    ("/api/change-refresh", "通告与事件"),
    ("/api/change-target-candidates", "通告与事件"),
    ("/api/source-refresh-status", "通告与事件"),
    ("/api/refresh", "通告与事件"),
    ("/api/generate", "通告与事件"),
    ("/api/send-generated", "通告与事件"),
    ("/api/handover-links", "日常工作"),
)

# page URL for each group so the agent always has initial module knowledge about
# where a source lives in the frontend.
_PAGE_BY_GROUP = {
    "未完成工作": "/workbench-lite",
    "通告与事件": "/workbench-lite?entry=notice",
    "工作台": "/workbench-lite",
    "事件": "/?mode=events",
    "维修单与跟进": "/repair-management",
    "机柜上下电": "/cabinet-power",
    "容量与水耗": "/water-management",
    "演练": "/drill-management",
    "日常工作": "/daily-tasks",
    "画像学练": "/learning",
    "计划收敛审查": "/plan-convergence",
    "轮巡工单": "/?entry=tools",
    "重保管理": "/critical-guard",
    "维护单": "/engineer/mop",
    "人员与签名管理": "/signature-management",
    "记录": "/workbench-lite",
    "楼栋概览": "/",
    "通告历史": "/admin/history-memory",
    "变更确认": "/workbench-lite",
    "后端任务": "/",
    "门户引导": "/",
    "业务接口": "/",
}

_ACTION_LABELS = {
    "monthly": "月度事件", "transfer-repair": "事件转检修",
    "workbench": "通告工作台", "draft": "通告草稿", "source-options": "通告来源选项",
    "notice-target-candidates": "通告目标候选", "change-target-candidates": "变更目标候选",
    "notice-image-drafts": "通告图片草稿", "available": "可撤销通告",
    "change-confirmations": "变更确认", "screenshot": "变更截图",
    "repair-event-candidates": "维修事件候选", "repair-event-prefill": "维修事件预填",
    "repair-event-bind": "绑定维修事件", "event-prefill": "事件预填",
    "integration-check": "联动检查", "sync-retry": "重试同步", "sync-retry-all": "重试全部同步",
    "daily-tasks": "每日任务清单", "morning-meeting": "晨会报告", "handover-links": "值班交接链接",
    "fill": "填写并生成维护单", "upload-local": "上传本地维护单", "upload-signed": "上传已签维护单",
    "mop-settings": "维护单设置", "duplicates": "重复人员", "associate": "关联人员",
    "merge": "合并重复人员", "migrate": "迁移已关联签名",
    "responses": "重保填报", "scope-template": "楼栋清单模板", "source-files": "源文件",
    "archive-retry": "重试归档", "weather-status": "天气状态", "weather-refresh": "刷新天气",
    "weather-pause": "暂停天气更新", "workbooks": "工作簿",
    "snapshots": "屏蔽快照", "rule-view": "规则视图", "excel": "解析收敛表格",
    "compare": "比对收敛结果", "check": "检修核对",
    "jobs": "后台任务", "uploads": "上传图片", "mark-stuck-failed": "标记卡住任务失败",
    "history-summary": "通告历史汇总", "history-scan": "扫描通告历史", "history-save": "保存通告历史",
    "send-generated": "发送已生成通告", "source-refresh-status": "数据源刷新状态",
    "polling-sops": "轮巡SOP", "polling-delay-status": "轮巡延迟状态", "photo": "上传巡检照片",
    "activate": "激活工单", "release": "释放工单", "session": "工单会话",
    "resend-links": "重发工单链接", "retry-upload": "重试上传",
    "download": "下载文件", "drills": "演练记录", "execution": "执行记录",
    "exports": "导出表格", "export-batches": "全部楼栋月度归档",
    "recognize": "识别文件", "images": "图片", "files": "文件",
    "rollback": "回退", "restore": "恢复", "restore-rows": "恢复记录",
    "clear-overlaps": "清理重叠记录", "text-apply": "填充识别内容",
    "apply": "应用", "correct": "更正", "resume": "继续处理",
    "reconcile": "核验原操作", "retry-sync": "重试同步", "retry-handoffs": "重试通告联动",
    "rack-power": "机柜功率", "workbench-actions": "发送通告",
    "notice-attachments": "通告图片", "notice-images": "通告图片",
    "remove-local": "移除本地通告", "answer": "提交答案", "reveal": "查看答案",
    "notes": "学习笔记", "copy": "复制题目", "print-model": "打印预览",
    "bootstrap": "初始化",
    "refresh": "刷新",
    "maintenance-refresh": "刷新维保",
    "repair-refresh": "刷新维修",
    "change-refresh": "刷新变更",
    "send": "发送",
    "generate": "生成",
    "repair": "修复",
    "retry": "重试",
    "prefill": "预填",
    "preview": "预览",
    "candidates": "候选查询",
    "candidate": "候选查询",
    "parse": "解析",
    "parser": "解析",
    "save": "保存",
    "bind": "绑定",
    "confirm": "确认",
    "cancel": "取消",
    "submit": "提交",
    "close": "结束",
    "update": "更新",
    "create": "创建",
    "delete": "删除",
    "upload": "上传",
    "reset": "重置",
    "clear": "清空",
    "retract": "撤销",
    "undo": "回退",
    "transfer": "转检修",
    "revoke": "撤销",
    "approve": "审批通过",
    "reject": "审批拒绝",
    "mark": "标记",
    "sync": "同步",
    "import": "导入",
    "export": "导出",
    "publish": "发布",
    "compile": "生成表格",
    "build": "生成",
    "usage-confirm": "签名使用确认",
    "people": "人员",
    "management": "签名管理",
    "scope": "楼栋范围",
    "detail": "详情",
    "closed": "已结束",
    "history": "历史",
    "lite-fragment": "轻量片段",
    "lite-detail": "轻量详情",
    "overview": "概览",
    "status": "状态",
    "recent": "最近任务",
    "batch": "批量",
    "checkpoint": "检查点",
    "cleanup": "清理",
    "mock-pressure": "压力测试",
    "records": "记录",
    "attachments": "附件",
    "settings": "设置",
    "photos": "照片",
    "configuration": "配置",
    "maintenance-actions": "维护动作",
    "ongoing-items": "进行中事项",
    "templates": "模板",
    "list": "列表",
    "query": "查询",
    "tasks": "任务",
    "rulesets": "规则集",
    "rules": "规则",
    "blocks": "屏蔽记录",
    "points": "点位",
    "catalog": "目录",
    "profile": "画像",
    "review": "复习",
    "bootstrap": "初始化",
    "overview": "概览",
    "status": "状态",
    "records": "记录",
    "batches": "批量",
    "event-candidates": "候选事件",
    "repair-candidates": "维修候选",
    "cmdb-candidates": "设备候选",
    "followups": "跟进记录",
    "followup-bind-candidates": "跟进绑定候选",
    "changes": "变更",
    "sync-status": "同步状态",
    "export-history": "导出历史",
    "export-schedule": "导出排期",
    "browser-login": "浏览器登录",
    "text-preview": "文本预览",
    "layout": "布局",
    "rooms": "机房",
    "racks": "机柜位",
    "writes": "待写入",
    "operations": "操作记录",
    "evidence": "证据图片",
    "documents": "文档",
    "template": "模板",
    "storage": "存储",
    "buildings": "楼栋",
    "expand": "展开",
    "match": "匹配",
    "health": "健康状态",
    "failures": "异常",
    "integrity": "完整性",
    "critical-guard": "重保任务",
    "scope-overview": "楼栋概览",
    "papers": "试卷",
    "questions": "题库",
    "issues": "问题",
}

_GROUP_KEYWORDS = {
    "通告与事件": "通知 维保 维护 变更 通告工作台",
    "工作台": "通告工作台 通知 维保 维护 变更",
    "事件": "事件管理 故障 异常",
    "维修单与跟进": "维修管理 检修 维修单 跟进",
    "机柜上下电": "机柜管理 机架 上电 下电 正式电 测试电",
    "容量与水耗": "用水 水表 抄表 用水量 水耗管理",
    "画像学练": "学习 培训 考试 答题 学习画像",
    "日常工作": "每日任务清单 日常任务 日常清单 每日清单 值班",
    "计划收敛审查": "计划收敛 收敛核对 智航 告警屏蔽",
    "演练": "演练管理 应急演练 预案演练",
    "维护单": "MOP 工程师 维护操作单",
    "人员与签名管理": "人员目录 人员选择 签名管理 签字",
    "重保管理": "重大保障 重点保障 保障任务",
    "轮巡工单": "巡检 轮巡 SOP",
}
_PATH_KEYWORDS = {
    "/api/events/monthly": "月度事件 事件月报 月报 月份事件 月度统计",
    "/api/repair-management/records": "维修进度 检修进度 维修状态",
    "/api/repair-management/status": "维修进度 检修进度 检修状态 维修状态",
    "/api/repair-management/followups": "维修跟进 检修跟进 跟进进度",
    "/api/cabinet-power/batches": "机柜批次 批次识别 文本填充",
    "/api/daily-tasks/morning-meeting": "晨会 晨会报告 晨会表",
    "/api/learning/papers": "试卷 练习 答题",
    "/api/learning/review": "错题 复习",
    "/api/plan-convergence/blocks": "屏蔽记录 屏蔽列表",
    "/api/plan-convergence/maintenance": "检修核对 检修收敛 未结束检修通告",
    "/api/engineer/mop/fill": "签名选择 签名分配 签字 生成维护单 生成MOP MOP填写",
    "/api/engineer/mop/upload-signed": "签名选择 签名分配 签字 回填维护单 上传MOP",
    "/api/signatures/people": "签名选择 签名人员 选择签字人",
    "/api/signatures/management/people": "签名选择 签名人员 选择签字人",
    "/api/drills/{drill_id}/execution": "签名选择 签名分配 签字 指挥员 审核人 参演人员",
    "/api/drills/{drill_id}/generate": "签名选择 签字 生成演练报告 演练生成",
    "/api/critical-guard/responses": "重保填报 签名选择 签名分配 签字 生成重保清单",
}

# A GET is only an action when the *tail* of the URL is a write verb.  Business
# domain segments (e.g. ``repair-management``) must never trigger the classifier
# even if they embed a verb like ``repair``.
_GET_WRITE_ACTIONS = frozenset({
    "refresh", "send", "generate", "repair", "retry", "publish", "compile",
    "submit", "import", "export", "close", "reset", "approve", "reject",
    "mark", "sync", "upload", "recover", "resume", "restore", "rollback",
    "confirm", "cancel", "recognize", "apply", "build", "create", "update",
    "delete", "undo", "retract", "transfer", "revoke", "parse", "preview",
})
# Unambiguous read tails; kept explicit so a ``refresh/status`` or ``*/status``
# GET is never treated as the ``refresh``/``status`` write action.
_GET_READ_TAILS = frozenset({
    "status", "overview", "records", "candidates", "detail", "health",
    "history", "people", "settings", "review", "profile", "changes",
    "integrity", "failures", "bootstrap", "layout", "export-history",
    "export-schedule", "storage", "buildings", "rooms", "racks", "writes",
    "operations", "blocks", "points", "browser-login", "sync-status",
    "event-candidates", "cmdb-candidates", "repair-candidates", "followups",
    "followup-bind-candidates",
})
_GET_PREVIEWS = frozenset({
    "/api/engineer/mop/preview", "/api/drills/{drill_id}/preview",
    "/api/daily-tasks/morning-meeting/preview",
    "/api/change-confirmations/{record_id}/screenshot/preview",
})


def _is_get_operation(path: str) -> bool:
    """True only when a real write action is the tail of a GET path.

    Only the final concrete (non-placeholder) segment counts.  Domain words such
    as ``repair-management`` or ``critical-guard`` are never treated as actions,
    and status/read tails (``refresh/status``, ``*/overview``...) stay read-only.
    """
    segments = [s for s in str(path).split("/") if s and not s.startswith("{")]
    if path in _GET_PREVIEWS:
        return False
    if not segments:
        return False
    tail = segments[-1].lower()
    if tail in _GET_READ_TAILS:
        return False
    if tail == "refresh" or tail.endswith("-refresh"):
        return True
    return tail in _GET_WRITE_ACTIONS
# Writes that must require two confirmations.
_HIGH_RISK_WRITE_RE = re.compile(
    r"/(delete|publish|send|confirm|overwrite|reset|clear|rollback|formal)(?:/|$|[-_])",
    re.IGNORECASE,
)
# Reversible / low-impact creates and temporary uploads are normal risk.
_LOW_RISK_WRITE_RE = re.compile(
    r"/(create|temp|temporary|upload|draft|save|add|new)(?:/|$|[-_])",
    re.IGNORECASE,
)
_MUTATING_METHODS = frozenset(("PUT", "PATCH", "DELETE"))

# Underscore-prefixed keys and actual auth-impersonation fields are never
# accepted from the assistant.  A small set of seemingly identity-like names are
# allowed only when they are actually declared as business fields in the request
# schema (e.g. a role/open_id carried in a repair record form).
_HARD_SPOOF_FIELDS = frozenset({
    "auth", "authorization", "cookie", "csrf_token", "is_admin", "actor",
    "scopes", "permissions", "access_token", "refresh_token", "session_id",
    "provider", "credential", "api_key", "apikey", "secret", "password",
    "token", "app_token", "tenant_token", "auth_token",
})
_SCHEMA_GATED_SPOOF_FIELDS = frozenset({
    "open_id", "user_id", "userid", "role",
})
_CREDENTIAL_KEYS = frozenset({
    "password", "secret", "token", "api_key", "apikey", "authorization",
    "cookie", "signature_raw", "raw_signature", "private_key", "credential",
    "credential_cipher", "access_token", "refresh_token", "ticket",
    "signature_png", "signature_image", "signature_bytes", "signature_base64",
})

# Explicit, verified read-only POST allowlist.  An unknown POST /.../preview is a
# normal (high risk) write and must require confirmation; only these actual
# lookup/prefill/preview endpoints are treated as queries.
_READONLY_POST_PATHS = (
    "/api/notice-target-candidates",
    "/api/change-target-candidates",
    "/api/repair-management/prefill",
    "/api/cabinet-power/batches/text-preview",
    "/api/cabinet-power/batches/{batch_id}/text-preview",
    "/api/plan-convergence/excel",
)

# These existing handlers accept uploads without FastAPI File annotations.
_UPLOAD_FORMATS = {
    "/api/cabinet-power/batches/recognize": ("multipart", "files"),
    "/api/cabinet-power/batches/{batch_id}/images": ("multipart", "files"),
    "/api/learning/attachments": ("multipart", "files"),
    "/api/notice-attachments": ("raw", "file"),
    "/api/capacity/water/uploads": ("raw", "file"),
    "/api/plan-convergence/excel": ("raw-excel", "file"),
}


def _shared_dispatch_schema(path: str, method: str) -> dict | None:
    """Native learning/plan handlers dispatch untyped JSON; describe their forms."""
    if method == "GET" or not path.startswith(("/api/learning/", "/api/plan-convergence/")):
        return None
    text, boolean, integer = {"type": "string"}, {"type": "boolean"}, {"type": "integer"}
    strings = {"type": "array", "items": text}
    objects = {"type": "array", "items": {"type": "object"}}
    properties, required, description = {}, [], ""
    if path.startswith("/api/learning/"):
        action = path.removeprefix("/api/learning/")
        question = {"type": "object", "properties": {
            **{key: {"type": "string", "maxLength": limit} for key, limit in (
                ("stem", 12000), ("answer_text", 12000), ("analysis", 12000), ("hint", 5000),
                ("topic", 100), ("specialty", 50), ("difficulty", 30), ("year", 30), ("reason", 2000))},
            "bank": {"type": "string", "enum": ["written", "duty", "professional"]},
            "type": {"type": "string", "enum": ["single", "multiple", "interview"]},
            "status": {"type": "string", "enum": ["draft", "published", "disabled", "deleted"]},
            "options": {"type": "array", "maxItems": 26, "items": {"type": "object", "properties": {
                "id": {"type": "string", "maxLength": 100}, "text": {"type": "string", "maxLength": 5000}}}},
            "correct_option_ids": strings, "type_label": text, "new_id": text, "id": text, "version": text,
        }}
        if action in {"questions", "questions/{id}"} and method in {"POST", "PUT"}:
            properties = question["properties"]
            required = ["version"] if method == "PUT" else []
            description = "草稿可不完整；修改已有题目须携带原version，发布完整性由原题库校验。附件走attachments接口。"
        elif action == "import" and method == "POST":
            properties = {"questions": {"type": "array", "minItems": 1, "maxItems": 500, "items": question}, "preview": boolean}
            required = ["questions"]
        elif action.startswith("papers/{id}/") and method == "POST":
            properties = {"question_id": text}
            required = ["question_id"]
            if action.endswith("/answer"):
                properties.update(operation_id={"type": "string", "maxLength": 150}, version=integer,
                                  option_ids=strings, answer_text={"type": "string", "maxLength": 12000},
                                  self_rating={"type": "string", "enum": ["部分掌握", "需复习"]}, practice=boolean)
                required += ["operation_id", "version"]
                description = "选择题须option_ids；面试题须answer_text和self_rating。version来自当前题单记录。"
            elif action.endswith("/reveal"):
                properties["kind"] = {"type": "string", "enum": ["answer", "analysis", "hint"]}
                description = "查看答案会写入提示使用和辅助作答记录。"
            elif action.endswith("/notes"):
                properties.update(note={"type": "string", "maxLength": 5000}, favorite=boolean)
            else:
                return None
        elif action == "questions/{id}/status" and method == "POST":
            properties = {"status": question["properties"]["status"], "version": text,
                          "ids": {**strings, "minItems": 1, "maxItems": 100},
                          "versions": {"type": "object", "additionalProperties": text}}
            required = ["status"]
            description = "单题须version；批量ids超过一项时须versions，键为题目ID、值为当前版本。"
        elif action == "questions/{id}/copy" and method == "POST":
            properties = {"version": text}
        elif action == "issues" and method == "POST":
            properties = {"paper_id": text, "question_id": text,
                          "description": {"type": "string", "maxLength": 6000},
                          "suggestion": {"type": "string", "maxLength": 6000},
                          "category": {"type": "string", "enum": ["题目", "题干", "选项", "答案", "解析", "资料", "适用条件", "其他"]}}
            required = ["paper_id", "question_id", "description"]
        elif action == "issues/{id}" and method == "PATCH":
            properties = {"version": integer,
                          "status": {"type": "string", "enum": ["pending", "processing", "needs_info", "resolved", "no_change", "withdrawn"]},
                          "remark": {"type": "string", "maxLength": 6000}, "description": {"type": "string", "maxLength": 6000}}
            required = ["version"]
            description = "处理或补充须remark（兼容description）；撤回待处理质疑可不填说明。"
        elif action == "settings" and method == "PUT":
            properties = {"enabled": boolean, "reminder_enabled": boolean,
                          "publish_time": {"type": "string", "pattern": r"^(?:[01]\d|2[0-3]):[0-5]\d$"},
                          "reminder_time": {"type": "string", "pattern": r"^(?:[01]\d|2[0-3]):[0-5]\d$"}}
        elif action == "publish" and method == "POST":
            properties = {"date": {"type": "string", "format": "date"}}
            description = "仅可发布今天题单；手动发布不发送通知。"
        elif action == "attachments/{id}" and method == "DELETE":
            properties = {"version": {"type": ["string", "integer"]}}
            description = "须在query或body携带所属题目或质疑的当前version。"
        elif (action, method) not in {("refresh", "POST"), ("papers/{id}", "DELETE")}:
            return None
    else:
        action = path.removeprefix("/api/plan-convergence/")
        block_id = {"type": ["string", "integer"], "description": "原屏蔽记录的数字ID"}
        if action == "rulesets" and method == "POST":
            properties = {"name": {"type": "string", "minLength": 1, "maxLength": 120},
                          "remark": {"type": "string", "maxLength": 500}}
            required = ["name"]
        elif action == "rulesets/{id}" and method == "PUT":
            item = {"type": "object", "properties": {
                **{key: {"type": ["string", "null"], "maxLength": 1000} for key in (
                    "obj_name", "zone", "building", "floor", "room", "inst_name", "point_name", "rule_name", "alarm_config_id", "rule_label")},
                "scope_type": {"type": "string", "enum": ["all", "zone", "building", "floor", "room", "objtype", "objtype_room", "device", "point", "exclude_device", "exclude_point"]},
                "rule_type": {"type": "string", "enum": ["common", "normal"]},
                "rule_group_no": {"type": "integer", "minimum": 1, "maximum": 500},
            }}
            properties = {"name": {"type": "string", "minLength": 1, "maxLength": 120},
                          "remark": {"type": "string", "maxLength": 500}, "items": {"type": "array", "maxItems": 500, "items": item}}
            required = ["items"]
            description = "items完整替换规则项，空数组清空；省略name/remark保留原值。"
        elif action == "rulesets/{id}/match" and method == "POST":
            properties = {"block_id": block_id, "details": {**objects, "maxItems": 10000}}
            description = "传details原屏蔽明细，或传block_id由原接口读取明细。"
        elif action == "compare" and method == "POST":
            row = {"type": "object", "properties": {key: text for key in ("设备域", "关联资源", "关联设备", "关联告警规则")},
                   "required": ["设备域", "关联资源", "关联设备", "关联告警规则"],
                   "description": "保留Excel原列名；_excel_row仅为原解析器的可选行号。"}
            scenario = {"type": "object", "properties": {"scenario_name": text, "rows": {"type": "array", "minItems": 1, "items": row}},
                        "required": ["scenario_name", "rows"]}
            properties = {"block_id": block_id, "scenarios": {"type": "array", "minItems": 1, "maxItems": 1, "items": scenario}}
            required = ["block_id", "scenarios"]
        elif action == "maintenance/check" and method == "POST":
            properties = {"record_id": text}
            description = "可选单条未结束检修通告ID，省略核对全部当前检修通告。"
        elif action in {"snapshots", "rule-view"} and method == "POST":
            keys = ("blockId", "blockDetailId", "instanceIds") if action == "snapshots" else ("classifyModelId", "domainCode", "alarmName", "ruleName")
            properties = {key: {"description": "沿用屏蔽详情中的原字段值"} for key in keys}
        else:
            return None
    return {"type": "object", "properties": properties, "required": required, "description": description}


def _shared_cabinet_schema(path: str, method: str) -> dict | None:
    """Describe native cabinet-power untyped JSON bodies (batches, rack-power,
    monthly export schedule and export batches). Operations have a dedicated
    schema in `_cabinet_operation_schema` and are intentionally not repeated."""
    if not path.startswith("/api/cabinet-power/"):
        return None
    tail = path[len("/api/cabinet-power/"):].strip("/")
    if tail.startswith("operations"):
        return None
    text, boolean, integer = {"type": "string"}, {"type": "boolean"}, {"type": "integer"}
    strings = {"type": "array", "items": text}
    version = integer
    editable = {key: text for key in (
        "scope", "room", "rack", "supplier_rack", "rack_type", "type_detail",
        "action", "expected", "actual", "result", "failure_reason", "type_resolution")}
    source_item = {"type": "object", "properties": {
        "id": {"type": "string", "minLength": 8, "maxLength": 64},
        "text": {"type": "string"}}, "required": ["id", "text"]}
    sources = {"type": "array", "minItems": 1, "maxItems": 2000, "items": source_item}

    if tail == "batches" and method == "POST":
        row_item = {"type": "object", "properties": {**editable, "text_id": text, "text_row": integer}}
        properties = {
            "source": {"type": "string", "enum": ["image", "text"]},
            "scope": text,
            "request_id": {"type": "string", "minLength": 16, "maxLength": 128,
                           "pattern": r"^[A-Za-z0-9_-]+$"},
            "sources": sources,
            "rows": {"type": "array", "minItems": 1, "maxItems": 2000, "items": row_item},
        }
        return {"type": "object", "properties": properties, "required": [],
                "description": "三种创建模式：source=image 仅预占图片待办(可带scope)；source=text 须request_id/sources/rows；缺省按rows手工录入。request_id为文本批次稳定提交标识。"}

    if tail == "batches/{batch_id}" and method == "PATCH":
        patch_row = {"type": "object", "properties": {**editable, "row_id": text, "excluded": boolean}}
        properties = {
            "version": version,
            "rows": {"type": "array", "items": patch_row},
            "common": {"type": "object", "properties": editable},
            "row_ids": strings,
            "acknowledge_warnings": boolean,
            "response_mode": {"type": "string", "enum": ["delta"]},
        }
        return {"type": "object", "properties": properties, "required": ["version"],
                "description": "行级修改走rows(每项可含row_id与excluded)；批量统一值走common并可用row_ids限定；acknowledge_warnings核对整批异常；response_mode=delta返回变更明细。version必填。"}

    if tail == "batches/{batch_id}/clear-overlaps" and method == "POST":
        return {"type": "object", "properties": {"version": version}, "required": ["version"],
                "description": "清空完全重叠的待办行，携带当前批次version。"}

    if tail == "batches/{batch_id}/confirm" and method == "POST":
        return {"type": "object",
                "properties": {"version": version, "row_ids": strings, "scope": text, "all": boolean},
                "required": ["version"],
                "description": "确认有效记录：all整批、scope单楼或row_ids逐条，至少选择一种范围；version必填。"}

    if tail == "batches/{batch_id}/rollback" and method == "POST":
        return {"type": "object",
                "properties": {"version": version, "row_ids": strings, "all": boolean},
                "required": ["version"],
                "description": "回退已写入记录：all整批或row_ids逐条；version必填。"}

    if tail == "batches/{batch_id}/cancel" and method == "POST":
        return {"type": "object", "properties": {"version": version}, "required": [],
                "description": "作废未提交行，推荐携带当前批次version。"}

    if tail == "batches/{batch_id}/restore-rows" and method == "POST":
        return {"type": "object", "properties": {"version": version, "row_ids": strings},
                "required": ["version", "row_ids"],
                "description": "恢复已排除/作废的待办行，version与row_ids必填。"}

    if tail in ("batches/text-preview", "batches/{batch_id}/text-preview") and method == "POST":
        return {"type": "object", "properties": {"sources": sources}, "required": ["sources"],
                "description": "粘贴文本预览，sources每项含id与text；不落库。"}

    if tail == "batches/{batch_id}/text-apply" and method == "POST":
        selection = {"type": "object", "properties": {"text_id": text, "text_row": integer, "row_id": text},
                     "required": ["text_id", "text_row", "row_id"]}
        return {"type": "object",
                "properties": {"sources": sources,
                               "rows": {"type": "array", "minItems": 1, "maxItems": 2000, "items": selection},
                               "version": version},
                "required": ["sources", "rows", "version"],
                "description": "把文本识别结果回填到本批已选行；rows每项含text_id/text_row/row_id；version为当前批次版本。"}

    if tail.startswith("batches/{batch_id}/images/") and method == "POST":
        if tail.endswith("/apply"):
            fields = {"type": "object", "properties": {
                key: text for key in ("action", "result", "failure_reason", "expected", "actual", "supplier_rack")}}
            return {"type": "object",
                    "properties": {"version": version, "row_id": text, "fields": fields,
                                   "candidate_index": integer, "attach": boolean,
                                   "review_times": boolean, "review_business": boolean},
                    "required": ["version", "row_id"],
                    "description": "把截图识别结果关联到某行；fields可覆盖识别字段，attach控制是否关联截图，review_times/review_business记录时间/业务核对。"}
        if tail.endswith("/retry") or tail.endswith("/correct") or tail.endswith("/restore"):
            properties = {"version": version}
            required = ["version"]
            if tail.endswith("/correct"):
                fields = {"type": "object",
                          "properties": editable,
                          "required": ["scope", "room", "rack"],
                          "description": "更正后的机柜字段，scope/room/rack必填。"}
                properties["version"] = version
                properties["candidate_index"] = {"type": "integer", "default": -1,
                                                 "description": "识别候选序号；缺省/ -1 表示人工录入新机柜，非必填。"}
                properties["fields"] = fields
                required = ["version", "fields"]
                description = ("从截图补全机柜并关联原图：version与fields必填，fields须含scope/room/rack；"
                               "candidate_index缺省为-1表示人工补录新机柜，可选。")
            elif tail.endswith("/retry"):
                description = "重新识别截图，携带当前批次version。"
            else:
                description = "撤回截图删除并恢复原机柜关联，携带当前批次version。"
            return {"type": "object", "properties": properties, "required": required,
                    "description": description}

    if tail == "rack-power" and method == "PATCH":
        return {"type": "object",
                "properties": {"scope": text, "room": text, "rack": text,
                               "power": {"type": ["number", "null"]},
                               "expected_version": {"type": "string", "description": "取自机柜目录的power_version"},
                               "power_only": boolean,
                               "operation_id": {"type": "string", "minLength": 16, "maxLength": 128,
                                                "pattern": r"^[A-Za-z0-9_-]+$"}},
                "required": ["power", "operation_id"],
                "description": "修改机柜功率：operation_id为稳定提交标识(可重复提交实现幂等)，power须出现(可留空)，expected_version用于冲突校验。"}

    if tail == "export-schedule" and method == "PUT":
        return {"type": "object",
                "properties": {"enabled": boolean,
                               "day": {"type": "integer", "minimum": 1, "maximum": 31},
                               "time": {"type": "string", "pattern": r"^(?:[01]\d|2[0-3]):[0-5]\d$"}},
                "required": ["enabled", "day", "time"],
                "description": "配置机柜月度自动归档：enabled启用开关，day每月日期，time触发时间HH:MM。"}

    if tail == "export-batches" and method == "POST":
        return {"type": "object",
                "properties": {"batch_id": {"type": "string", "pattern": r"^all_[a-f0-9]{32}$"}},
                "required": ["batch_id"],
                "description": "启动五楼(A–E)月度归档导出，batch_id为稳定标识，重复提交复用原任务。"}

    if tail == "exports" and method == "POST":
        return {"type": "object", "properties": {"batch_id": text},
                "required": [],
                "description": "启动当前楼栋导出任务，batch_id可选稳定标识。"}

    return None


_SHARED_QUERY_FIELDS = {
    "/api/learning/bootstrap": ["scope"],
    "/api/learning/papers": ["scope", "date", "today", "from", "to", "page", "page_size"],
    "/api/learning/history": ["scope", "date", "today", "from", "to", "page", "page_size"],
    "/api/learning/review": ["scope", "kind", "bank", "search", "page", "page_size"],
    "/api/learning/profile": ["scope", "period", "from", "to"],
    "/api/learning/export": ["kind", "scope", "period", "from", "to", "bank", "status", "specialty", "topic", "search", "problems"],
    "/api/learning/questions": ["bank", "status", "specialty", "topic", "search", "problems", "page", "page_size"],
    "/api/learning/issues": ["scope", "status", "search", "page", "page_size"],
    "/api/learning/attachments": ["scope", "question_id", "issue_id", "kind", "version"],
    "/api/learning/attachments/{id}": ["version"],
    "/api/plan-convergence/blocks": ["refresh"],
    "/api/plan-convergence/points": ["name"],
    "/api/plan-convergence/catalog": ["kind", "kw", "zone", "building", "floor", "room", "objs", "rooms", "obj", "inst"],
}

# (supported buildings, native input section, explicit scope required).
# Keep account/resource defaults on learning and drill listing endpoints.
_SINGLE_SCOPE_ROUTES = {
    **{f"GET /api/capacity/water/{tail}": ("ABCDEH", "params", True)
       for tail in ("bootstrap", "records", "records/{record_id}", "images/{image_id}")},
    "POST /api/capacity/water/records": ("ABCDEH", "body", True),
    "PATCH /api/capacity/water/records/{record_id}": ("ABCDEH", "body", True),
    **{f"GET /api/learning/{tail}": ("ABCDEH", "params", False)
       for tail in ("bootstrap", "papers", "history", "review", "profile", "issues")},
    "POST /api/learning/issues": ("ABCDEH", "body", False),
    "GET /api/drills": ("ABCDE", "params", False),
    "GET /api/drills/bootstrap": ("ABCDE", "params", False),
    **{f"GET /api/drills/{{drill_id}}/{tail}": ("ABCDE", "params", True)
       for tail in ("execution", "preview", "print-model", "download")},
    "PUT /api/drills/{drill_id}/execution": ("ABCDE", "params", True),
    "POST /api/drills/{drill_id}/generate": ("ABCDE", "params", True),
    "POST /api/drills/{drill_id}/retry-sync": ("ABCDE", "params", True),
    **{f"GET /api/critical-guard/{tail}": ("ABCDE", "params", True)
       for tail in ("scope-template", "tasks", "tasks/{task_id}")},
    "PUT /api/critical-guard/scope-template": ("ABCDE", "body", True),
    "POST /api/critical-guard/scope-template/reset": ("ABCDE", "body", True),
    "PUT /api/critical-guard/responses/{response_id}": ("ABCDE", "body", True),
    "POST /api/critical-guard/source-files": ("ABCDE", "body", True),
}


def _cabinet_operation_schema(path, method):
    if method not in {"POST", "PATCH"} or path not in {"/api/cabinet-power/operations", "/api/cabinet-power/operations/{record_id}"}:
        return None
    group = {"type": "object", "properties": {
        "id": {"type": "string"}, "action": {"type": "string", "enum": ["上正式电", "上测试电", "测试电转正式电", "正式电转测试电", "下正式电", "下测试电"]},
        "actual": {"type": "string", "format": "date-time"}, "expected": {"type": "string", "format": "date-time"},
        "result": {"type": "string", "enum": ["成功", "失败"]}, "failure_reason": {"type": "string", "maxLength": 1000}
    }}
    properties = {
        "scope": {"type": "string", "enum": list("ABCDE")}, "operation_id": {"type": "string"},
        "room": {"type": "string"}, "rack": {"type": "string"}, "rack_type": {"type": "string", "enum": ["", "服务器机柜", "网络机柜"]},
        "expected_version": {"type": "string"}, "power": {"type": ["number", "null"]},
        "groups": {"type": "array", "items": group}, **group["properties"],
        "expected_state": {"type": "string", "enum": ["off", "test", "formal", "unknown"]}, "expected_latest_time": {"type": "string"},
        "target_state": {"type": "string", "enum": ["off", "test", "formal"]}
    }
    return {"type": "object", "properties": properties, "required": ["scope", "operation_id", "room", "rack", "rack_type"] if method == "POST" else ["scope", "operation_id", "expected_version"],
            "description": "登记操作前查询机柜目录和现有操作，携带真实状态与版本。D/E楼已有记录时必须PATCH原记录并保留原groups，新操作放在首组；不可新建第二条或清空历史。可用$concat引用完整旧groups。失败须填failure_reason，actual必填。"}

_BINARY_EXT_BY_MIME = {
    "image/png": ("report.png", "png"),
    "image/jpeg": ("report.jpg", "jpg"),
    "image/jpg": ("report.jpg", "jpg"),
    "image/webp": ("report.webp", "webp"),
    "image/gif": ("report.gif", "gif"),
    "image/bmp": ("report.bmp", "bmp"),
    "application/pdf": ("report.pdf", "pdf"),
    "application/zip": ("download.zip", "zip"),
    "application/msword": ("report.doc", "doc"),
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ("report.docx", "docx"),
    "application/vnd.ms-excel": ("report.xls", "xls"),
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ("report.xlsx", "xlsx"),
    "text/csv": ("report.csv", "csv"),
    "text/plain": ("report.txt", "txt"),
    "application/octet-stream": ("download.bin", "bin"),
}
_BINARY_CTYPE_PREFIXES = (
    "image/", "audio/", "video/", "font/",
    "application/octet-stream", "application/pdf", "application/zip",
    "application/msword", "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument",
)


def _route_excluded(path: str) -> bool:
    if path == "/api/assistant/pending":
        return False
    if path in _EXCLUDED_PATHS:
        return True
    if any(path == p or path.startswith(p) for p in _EXCLUDED_PREFIXES):
        return True
    if any(path == p or path.startswith(p) for p in _ADMIN_EXCLUDED_PREFIXES):
        return True
    if path not in _SIGNATURE_BUSINESS_PATHS and any(path.startswith(p) for p in _SIGNATURE_EXCLUDED_PREFIXES):
        return True
    if any(part in path for part in _STREAM_AND_INTERNAL):
        return True
    return False


def _is_readonly_post(path: str) -> bool:
    return path.rstrip("/") in _READONLY_POST_PATHS


def _is_binary_ctype(ctype: str) -> bool:
    if not ctype:
        return False
    return ctype in _BINARY_EXT_BY_MIME or any(
        ctype.startswith(prefix) for prefix in _BINARY_CTYPE_PREFIXES
    )


def _fallback_name(ctype: str) -> str:
    return _BINARY_EXT_BY_MIME.get(ctype, _BINARY_EXT_BY_MIME["application/octet-stream"])[0]


def _parse_content_disposition(value: Any) -> str:
    value = str(value or "")
    name = ""
    match = re.search(r"filename\*\s*=\s*([^;]+)", value, re.I)
    if match:
        token = match.group(1).strip().strip('"\'')
        if "utf-8''" in token.lower():
            token = token.split("''", 1)[-1]
        try:
            name = unquote(token)
        except Exception:  # noqa: BLE001
            name = token
    else:
        match = re.search(r'filename\s*=\s*"?([^";]+)"?', value, re.I)
        if match:
            name = match.group(1).strip()
    name = re.sub(r"[\x00-\x1f]", "", (name or ""))
    name = (name or "").replace("\\", "/").split("/")[-1][:180]
    return name.strip()


def _module_label(path: str) -> str:
    for prefix, label in _GROUP_BY_PREFIX:
        if path.startswith(prefix):
            return label
    # Fall back to admin business groups.
    if path.startswith("/api/admin/"):
        return "管理设置"
    return "业务接口"


def _humanize_segment(segment: str) -> str:
    text = str(segment or "")
    text = re.sub(r"[-_]", " ", text)
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", text)
    return text.strip() or "操作"


# Short Chinese module names added to generic object tails so catalog entries
# such as ``.../records`` or ``.../batches`` are not all named the same.
_MODULE_CONTEXT = {
    "通告与事件": "通告",
    "工作台": "工作台",
    "事件": "事件",
    "维修单与跟进": "维修单",
    "机柜上下电": "机柜",
    "容量与水耗": "容量水耗",
    "演练": "演练",
    "日常工作": "日常",
    "画像学练": "学练",
    "计划收敛审查": "收敛审查",
    "轮巡工单": "轮巡",
    "重保管理": "重保",
    "维护单": "维护单",
    "人员与签名管理": "签名",
    "记录": "记录",
    "楼栋概览": "楼栋",
    "通告历史": "通告历史",
    "变更确认": "变更确认",
    "后端任务": "后端",
    "门户引导": "门户",
    "管理设置": "管理",
    "业务接口": "",
}


def _module_context(path: str) -> str:
    return _MODULE_CONTEXT.get(_module_label(path), "")


# Generic object tails whose catalog name benefits from the module context.
_NEED_MODULE_CONTEXT = frozenset({
    "records", "batches", "batch", "list", "query", "tasks", "settings",
    "detail", "overview", "status", "people", "files", "images", "photos",
    "templates", "candidates", "items", "rules", "rulesets", "points",
    "blocks", "catalog", "operations", "rooms", "racks", "writes",
    "event-candidates", "repair-candidates", "cmdb-candidates", "followups",
    "followup-bind-candidates", "export-history", "sync-status", "layout",
    "buildings", "storage", "changes",
    "responses", "scope-template", "source-files", "workbooks", "jobs",
})


def _action_label(path: str, method: str) -> str:
    segments = [s for s in path.split("/") if s and not s.startswith("{")]
    last = segments[-1] if segments else ""
    raw = last.lower()
    action = _ACTION_LABELS.get(last, _ACTION_LABELS.get(raw, _humanize_segment(last)))
    module = _module_context(path)
    if module and raw in _NEED_MODULE_CONTEXT and not action.startswith(module):
        action = f"{module}{action}"
    if method == "GET":
        if _is_get_operation(path):
            # Operation GET (refresh variants, export, publish...) already
            # carries its own action meaning; do not prefix with "查询".
            return action or "操作"
        return f"查询{action}"
    if method == "DELETE":
        return "删除" + action.removeprefix("删除")
    if method in {"PUT", "PATCH"}:
        return "更新" + action.removeprefix("更新")
    if method == "POST" and raw in {"records", "batches", "drills", "questions", "rulesets", "followups", "issues"}:
        return "新增" + action
    return action or "操作"


def _path_fields(path: str):
    return [name for name in re.findall(r"\{([^{}]+)\}", path)]


# ---------------------------------------------------------------------------
# AST / route introspection
# ---------------------------------------------------------------------------


def _endpoint_source(route: APIRoute) -> str:
    try:
        src = inspect.getsource(route.endpoint)
    except (OSError, TypeError):
        return ""
    return textwrap.dedent(src).strip()


def _read_model_names_from_ast(route: APIRoute) -> list[str]:
    src = _endpoint_source(route)
    if not src:
        return []
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return []
    names: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        call_name = None
        if isinstance(func, ast.Name) and func.id == "_read_model_request":
            call_name = func.id
        elif isinstance(func, ast.Attribute) and func.attr == "_read_model_request":
            call_name = func.attr
        if call_name is None:
            continue
        if node.args and len(node.args) >= 2:
            arg = node.args[1]
            if isinstance(arg, ast.Name):
                names.append(arg.id)
            elif isinstance(arg, ast.Attribute):
                names.append(arg.attr)
    return names


def _native_body_model(route: APIRoute):
    """Resolve a native FastAPI body Pydantic model from ``dependant.body_params``.

    Some endpoints declare the request body directly as a typed parameter
    (``def foo(body: SomeModel)``) instead of parsing it through the helper
    ``_read_model_request(...)``.  The AST scan above cannot see that, but
    FastAPI stores the model annotation on the body parameter (via
    ``field_info.annotation`` on the relevant FastAPI version).
    """
    for p in getattr(route.dependant, "body_params", ()) or ():
        annotation = getattr(p, "annotation", None)
        if annotation is None:
            try:
                annotation = getattr(getattr(p, "field_info", None), "annotation", None)
            except Exception:  # noqa: BLE001
                annotation = None
        found = _model_from_annotation(annotation)
        if found is not None:
            return found
    return None


def _model_from_annotation(annotation):
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return annotation
    args = getattr(annotation, "__args__", ())
    for item in args:
        found = _model_from_annotation(item)
        if found is not None:
            return found
    return None


def _resolve_model(route: APIRoute, names: list[str]):
    if names:
        g = getattr(route.endpoint, "__globals__", None) or {}
        for name in names:
            obj = g.get(name)
            if isinstance(obj, type) and issubclass(obj, BaseModel):
                return obj
        # Fall back to module where the class name may be exported under a known
        # api_models alias.
        with suppress(Exception):
            from clipflow_backend import api_models as _am
            for name in names:
                obj = getattr(_am, name, None)
                if isinstance(obj, type) and issubclass(obj, BaseModel):
                    return obj
    # FastAPI native typed body parameter (no _read_model_request AST hint).
    return _native_body_model(route)


def _inferred_body_fields(route: APIRoute) -> list[str]:
    """Field names read via ``payload.get('x')`` in the endpoint body."""
    fields: dict[str, bool] = {}
    src = _endpoint_source(route)
    if src:
        for match in re.finditer(r"payload\.get\(\s*['\"]([^'\"]+)['\"]", src):
            fields.setdefault(match.group(1), True)
        try:
            tree = ast.parse(src)
        except SyntaxError:
            tree = None
        if tree is not None:
            for node in ast.walk(tree):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
                        and node.func.attr == "get" and isinstance(node.func.value, ast.Name) \
                        and node.func.value.id == "payload" and node.args \
                        and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                    fields.setdefault(node.args[0].value, True)
    return list(fields)


def _query_names_from_source(route: APIRoute) -> list[str]:
    fields: dict[str, bool] = {}
    src = _endpoint_source(route)
    if not src:
        return []
    try:
        tree = ast.parse(src)
    except SyntaxError:
        for match in re.finditer(r"query_params\.get\(\s*['\"]([^'\"]+)['\"]", src):
            fields.setdefault(match.group(1), True)
        return list(fields)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Attribute) or node.func.attr != "get":
            continue
        if isinstance(node.func.value, ast.Attribute) and node.func.value.attr == "query_params":
            if node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                fields.setdefault(node.args[0].value, True)
        elif isinstance(node.func.value, ast.Attribute) and node.func.value.attr == "query":
            if node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                fields.setdefault(node.args[0].value, True)
    return list(fields)


def _form_field_names(route: APIRoute) -> list[str]:
    names: dict[str, bool] = {}
    src = _endpoint_source(route)
    if src:
        for match in re.finditer(r"form\.get\(\s*['\"]([^'\"]+)['\"]", src):
            names.setdefault(match.group(1), True)
    for p in getattr(route.dependant, "body_params", ()) or ():
        try:
            fi = getattr(p, "field_info", None)
        except Exception:  # noqa: BLE001
            fi = None
        if fi is not None:
            names.setdefault(getattr(p, "name", ""), True)
    return list(names)


def _file_field_names(route: APIRoute) -> list[str]:
    names: dict[str, bool] = {}
    for p in getattr(route.dependant, "body_params", ()) or ():
        try:
            fi = getattr(p, "field_info", None)
        except Exception:  # noqa: BLE001
            fi = None
        if fi is not None and type(fi).__name__.lower() == "file":
            names.setdefault(getattr(p, "name", ""), True)
    return list(names)


def _route_multipart(route: APIRoute) -> bool:
    if _file_field_names(route):
        return True
    for p in getattr(route.dependant, "body_params", ()) or ():
        with suppress(Exception):
            if getattr(getattr(p, "field_info", None), "media_type", "") == "multipart/form-data":
                return True
    return False


def _schema_redact(schema: Any) -> Any:
    """Recursively strip defaults/examples and credential-looking properties."""
    if isinstance(schema, list):
        return [_schema_redact(v) for v in schema]
    if not isinstance(schema, dict):
        return schema
    out: dict[str, Any] = {}
    for key, value in schema.items():
        if key in ("default", "example", "examples", "const"):
            continue
        if key == "properties" and isinstance(value, dict):
            stripped = {
                k: _schema_redact(v) for k, v in value.items()
                if not _is_credential_key(k)
            }
            # remove stripped keys from the sibling `required` list
            required = out.get("required")
            if isinstance(required, list):
                out["required"] = [r for r in required if r not in set(value) - set(stripped)]
            out[key] = stripped
            continue
        if key == "$defs" and isinstance(value, dict):
            out[key] = {k: _schema_redact(v) for k, v in value.items() if not _is_credential_key(k)}
            continue
        if isinstance(value, (dict, list)):
            out[key] = _schema_redact(value)
        else:
            out[key] = value
    if isinstance(out.get("required"), list) and isinstance(out.get("properties"), dict):
        out["required"] = [name for name in out["required"] if name in out["properties"]]
    return out


def _is_credential_key(name: str) -> bool:
    lowered = str(name or "").lower()
    if lowered in {"file_token", "file_tokens", "attachment_token"}:
        return False
    if lowered.startswith("_") or lowered in _CREDENTIAL_KEYS:
        return True
    return any(token in lowered for token in ("password", "secret", "token", "api_key", "apikey", "credential", "cookie", "authorization"))


# ---------------------------------------------------------------------------
# Schema / field helpers
# ---------------------------------------------------------------------------


def _resolve_schema_ref(prop: Any, root: dict) -> Any:
    if isinstance(prop, dict) and "$ref" in prop:
        ref = str(prop["$ref"])
        if ref.startswith("#/$defs/"):
            name = ref[len("#/$defs/"):]
            defs = (root or {}).get("$defs") or {}
            if name in defs:
                return _resolve_schema_ref(defs[name], root)
        if ref.startswith("#/components/schemas/"):
            name = ref[len("#/components/schemas/"):]
            defs = (root or {}).get("$defs") or {}
            if name in defs:
                return _resolve_schema_ref(defs[name], root)
    return prop


def _resolve_anyof(prop: Any) -> Any:
    if not isinstance(prop, dict):
        return prop
    for key in ("anyOf", "oneOf"):
        alts = prop.get(key)
        if isinstance(alts, list):
            non_null = [a for a in alts if not (isinstance(a, dict) and a.get("type") == "null")]
            if non_null:
                out = dict(prop)
                out.pop(key, None)
                out.update(non_null[0])
                return out
    return prop


def _schema_allows_null(prop: Any, root: dict) -> bool:
    """True when the resolved schema permits an explicit ``null`` value.

    Nullability must be read from the RAW schema before ``_resolve_anyof``
    collapses ``anyOf``/``oneOf`` and silently drops the ``null`` branch.
    Handles ``type: \"null\"``, ``type: [X, \"null\"]`` and nullable unions
    such as ``anyOf:[{type:number},{type:null}]``.
    """
    prop = _resolve_schema_ref(prop, root)
    if not isinstance(prop, dict):
        return False
    t = prop.get("type")
    if t == "null" or (isinstance(t, list) and "null" in t):
        return True
    for key in ("anyOf", "oneOf"):
        alts = prop.get(key)
        if isinstance(alts, list):
            for alt in alts:
                if _schema_allows_null(alt, root):
                    return True
    return False


def _options_of(prop: dict) -> list | None:
    for key in ("enum", "options"):
        val = prop.get(key)
        if isinstance(val, list) and val:
            return [{"value": v if v != "" else "__empty__", "label": str(v) if v != "" else "暂不确定"} for v in val]
    return None


def _field_label(name: str) -> str:
    text = re.sub(r"[-_]+", " ", str(name or ""))
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", text).strip()
    zh = {
        "scope": "楼栋范围", "operation_id": "操作编号", "date": "日期", "month": "月份",
        "record_id": "记录编号", "source_record_id": "源记录", "work_type": "事项类型",
        "notice_type": "通告类型", "title": "标题", "content": "内容", "reason": "原因",
        "impact": "影响", "progress": "进度", "start time": "开始时间",
        "end time": "结束时间", "building": "楼栋", "specialty": "专业",
        "recipient open ids": "接收人", "target scopes": "目标楼栋",
        "room": "包间", "rack": "机柜", "rack type": "机柜类型", "actual": "实际完成时间",
        "expected": "期望完成时间", "result": "结果", "failure reason": "失败原因", "expected version": "记录版本", "groups": "操作历史",
        "time": "时间", "drill date": "演练日期", "check date": "检查日期", "statistic date": "统计日期",
        "first start time": "首步开始时间", "signature time": "演练审核人签名时间", "evaluation time": "演练评估人评估时间",
        "publish time": "发布时间", "reminder time": "提醒时间", "year": "年度", "source event id": "关联事件",
        "source repair ids": "关联检修通告", "summary record id": "维修项目", "cmdb record ids": "CMDB设备",
    }
    if text in zh:
        return zh[text]
    return text or "字段"


def _frontend_field(prop: Any, root: dict, *, name: str = "") -> dict:
    prop = _resolve_anyof(_resolve_schema_ref(prop, root) if isinstance(prop, dict) else prop)
    if not isinstance(prop, dict):
        return {"type": "text"}
    t = prop.get("type")
    if isinstance(t, list):
        t = next((x for x in t if x != "null"), None)
    fmt = prop.get("format")
    opts = _options_of(prop)
    if opts:
        return {"type": "select", "options": opts}
    if t == "array":
        opts = _options_of(_resolve_schema_ref(prop.get("items") or {}, root))
        if opts:
            return {"type": "multiselect", "options": opts, **{key: prop[key] for key in ("minItems", "maxItems") if key in prop}}
    if t == "string":
        if not fmt:
            if name == "date" or name.endswith("_date"):
                fmt = "date"
            elif name in {"time", "first_start_time", "publish_time", "reminder_time"}:
                fmt = "time"
            elif name in {"start_time", "end_time", "signature_time", "evaluation_time", "expected", "actual"}:
                fmt = "date-time"
        if fmt:
            lowered = str(fmt).lower()
            if lowered == "date":
                return {"type": "date"}
            if "date" in lowered and "time" in lowered:
                return {"type": "datetime-local", "step": 60 if prop.get("maxLength") == 16 else 1}
            if lowered in ("time",):
                return {"type": "time"}
        return {"type": "textarea" if (int(prop.get("maxLength") or 0) > 120) else "text", **({"maxlength": prop["maxLength"]} if "maxLength" in prop else {})}
    if t in ("number", "integer"):
        return {"type": "number", "step": 1 if t == "integer" else "any", **{key: prop[source] for key, source in (("min", "minimum"), ("max", "maximum")) if source in prop}}
    if t == "boolean":
        return {"type": "checkbox"}
    if t in ("array", "object"):
        return {"type": "textarea", "value_format": "json"}
    return {"type": "text"}


def _missing_scalars(schema: dict, body: dict, *, prefix: str = "", section: str = "body", root: dict | None = None) -> list[dict]:
    """Resolve missing required scalar fields into assistant form fields."""
    root = root if isinstance(root, dict) else schema
    missing: list[dict] = []
    props = schema.get("properties") or {}
    required = set(schema.get("required") or [])
    for name, raw_prop in props.items():
        if _is_credential_key(name):
            continue
        path = f"{prefix}.{name}" if prefix else name
        present = isinstance(body, dict) and name in body
        value = body.get(name) if present else None
        is_required = name in required
        prop = _resolve_anyof(_resolve_schema_ref(raw_prop, root))
        t = prop.get("type") if isinstance(prop, dict) else None
        if isinstance(t, list):
            t = next((x for x in t if x != "null"), None)
        # Nullability is decided from the RAW schema BEFORE anyOf/oneOf collapse
        # (e.g. type:[number,null] or anyOf:[{type:number},{type:null}]). An
        # explicit NULL is a complete, valid fill for nullable properties
        # (e.g. rack-power uses power:null to clear the reading); absent keys
        # and non-nullable NULLs still count as missing.
        nullable = _schema_allows_null(raw_prop, root)
        isEmpty = (not present) or (value is None and not nullable) or (isinstance(value, str) and not value.strip())
        if present and value == "" and isinstance(prop, dict) and "" in prop.get("enum", []):
            isEmpty = False
        if present and value is None and nullable:
            # Explicit null fully satisfies a nullable property; do not recurse
            # into optional nested objects or report it as missing.
            continue
        if t == "object" and isinstance(prop, dict):
            children = prop.get("properties") or {}
            # Nested object required recursion uses the CHILD's own required list.
            child_required = set(prop.get("required") or [])
            child_schema = {"type": "object", "properties": children, "required": sorted(child_required)}
            if is_required and (not present or not isinstance(value, dict)):
                if children:
                    missing.extend(_missing_scalars(child_schema, {}, prefix=path,
                                                    section=f"body.{path}".rstrip("."), root=root))
                else:
                    base = _frontend_field(prop, root, name=name)
                    item = {"name": name, "path": path, "label": _field_label(name),
                            "type": base.get("type", "textarea"), "required": True, "section": section}
                    item.update({key: value for key, value in base.items() if key != "type"})
                    missing.append(item)
            elif isinstance(value, dict):
                missing.extend(_missing_scalars(child_schema, value, prefix=path,
                                                section=f"body.{path}".rstrip("."), root=root))
            continue
        if is_required and isEmpty:
            base = _frontend_field(prop, root, name=name)
            item = {"name": name, "path": path, "label": _field_label(name),
                    "type": base.get("type", "text"), "required": True, "section": section}
            item.update({key: value for key, value in base.items() if key != "type"})
            missing.append(item)
    return missing


# ---------------------------------------------------------------------------
# The Portal API catalogue
# ---------------------------------------------------------------------------


class PortalAPICatalog:
    """Generic, route-table-driven catalogue over existing FastAPI business routes."""

    def __init__(self, app):
        self.app = app
        self._descriptors: dict[str, dict] = {}
        self._models: dict[str, Any] = {}
        self._route_for_id: dict[str, APIRoute] = {}
        self._order: list[str] = []
        self._scan(app)
        self._add_virtual_parse_notice()

    # -- scanning ----------------------------------------------------------

    def _scan(self, app) -> None:
        for route in app.routes:
            if not isinstance(route, APIRoute):
                continue
            path = route.path or ""
            if not path.startswith("/api/"):
                continue
            if _route_excluded(path):
                continue
            for method in sorted(route.methods or ()):
                if method in ("HEAD", "OPTIONS"):
                    continue
                paths = [path]
                if path == "/api/signatures/management/{operation}":
                    paths = [path.replace("{operation}", operation)
                             for operation in _SIGNATURE_MANAGEMENT_OPERATIONS.get(method, ())]
                for catalog_path in paths:
                    desc = self._build_descriptor(route, method, path=catalog_path)
                    self._descriptors[desc["id"]] = desc
                    self._route_for_id[desc["id"]] = route
                    self._models[desc["id"]] = self._body_model(route)
                    self._order.append(desc["id"])

    def _build_descriptor(self, route: APIRoute, method: str, *, path: str | None = None) -> dict:
        path = path or route.path
        method = method.upper()
        model = self._body_model(route)
        body_schema = None
        if model is not None:
            with suppress(Exception):
                body_schema = model.model_json_schema()
        if body_schema is None:
            inferred = _inferred_body_fields(route)
            body_schema = {
                "type": "object",
                "properties": {name: {"type": "string"} for name in inferred},
                "required": [],
            }
        cabinet_schema = _cabinet_operation_schema(path, method)
        if cabinet_schema:
            body_schema = cabinet_schema
        shared_schema = _shared_dispatch_schema(path, method)
        if shared_schema is not None and model is None:
            body_schema = shared_schema
        cabinet_schema = _shared_cabinet_schema(path, method)
        if cabinet_schema is not None and model is None:
            body_schema = cabinet_schema
        # Only add a fallback property when the native schema did not already
        # declare it; never overwrite precise constraints (e.g. batch pattern,
        # request_id minLength) surfaced by _shared_cabinet_schema.
        if method == "POST" and path in {"/api/cabinet-power/exports", "/api/cabinet-power/export-batches"}:
            body_schema["properties"].setdefault("batch_id", {"type": "string", "description": "稳定的导出批次标识，重复提交复用原任务"})
        if method == "POST" and path == "/api/cabinet-power/batches":
            body_schema["properties"].setdefault("request_id", {"type": "string", "description": "文本批次的稳定提交标识"})

        query_fields = list(dict.fromkeys([p.name for p in getattr(route.dependant, "query_params", ())]
                                          + _query_names_from_source(route) + _SHARED_QUERY_FIELDS.get(path, [])))
        query_fields = [name for name in query_fields if not _is_credential_key(name)]
        path_fields = _path_fields(path)
        file_fields = _file_field_names(route)
        multipart = bool(_route_multipart(route)) or bool(file_fields)
        if multipart and model is None:
            properties, required = {}, []
            for param in getattr(route.dependant, "body_params", ()):
                if param.name in file_fields:
                    continue
                info = param.field_info
                properties[param.name] = _schema_redact(TypeAdapter(info.annotation).json_schema())
                if info.is_required():
                    required.append(param.name)
            body_schema = {"type": "object", "properties": properties, "required": required}
        upload_format, upload_field = _UPLOAD_FORMATS.get(path, ("", "")) if method == "POST" else ("", "")
        if upload_field:
            file_fields = [upload_field]
            multipart = upload_format == "multipart"
            body_schema = {"type": "object", "properties": {}, "required": []}

        read_only, risk = self._risk(route, method, path)
        api_id = f"{method} {path}"
        scope_rule = _SINGLE_SCOPE_ROUTES.get(api_id)
        if scope_rule:
            if scope_rule[1] == "params" and "scope" not in query_fields:
                query_fields.append("scope")
            elif scope_rule[1] == "body":
                body_schema.setdefault("properties", {}).setdefault("scope", {"type": "string", "enum": list(scope_rule[0])})
        group = _module_label(path)
        keywords = [_GROUP_KEYWORDS.get(group, "")]
        keywords.extend(_ACTION_LABELS[part] for part in path.split("/") if part in _ACTION_LABELS)
        keywords.extend(words for prefix, words in _PATH_KEYWORDS.items()
                        if path == prefix or path.startswith(prefix + "/"))
        return {
            "id": api_id,
            "method": method,
            "path": path,
            "group": group,
            "page": "/repair-status" if path == "/api/repair-management/status" else _PAGE_BY_GROUP.get(group, "/"),
            "name": _action_label(path, method),
            "keywords": " ".join(keywords),
            "read_only": read_only,
            "risk": risk,
            **({"scope_mode": "single", "scope_values": list(scope_rule[0]),
                "scope_section": scope_rule[1], "scope_required": scope_rule[2]} if scope_rule else {}),
            "schema": {
                "body": _schema_redact(body_schema),
                "query": query_fields,
                "path": path_fields,
            },
            "multipart": multipart,
            **({"upload_format": upload_format, "required_files": file_fields} if upload_field else {}),
            **({"files": file_fields} if file_fields or multipart else {}),
        }

    def _body_model(self, route: APIRoute):
        return _resolve_model(route, _read_model_names_from_ast(route))

    @staticmethod
    def _risk(route: APIRoute, method: str, path: str) -> tuple[bool, str]:
        method = method.upper()
        if method == "GET":
            if _is_get_operation(path):
                return False, "high"
            return True, "normal"
        if method == "POST" and _is_readonly_post(path):
            return True, "normal"
        if method in _MUTATING_METHODS:
            return False, "high"
        if _HIGH_RISK_WRITE_RE.search(path):
            return False, "high"
        if _LOW_RISK_WRITE_RE.search(path):
            return False, "normal"
        return False, "high"

    def _add_virtual_parse_notice(self) -> None:
        api_id = "POST /api/_assistant/parse-notice"
        desc = {
            "id": api_id,
            "method": "POST",
            "path": "/api/_assistant/parse-notice",
            "group": "通告与事件",
            "page": "/workbench-lite?entry=notice",
            "name": "解析粘贴通告",
            "read_only": True,
            "risk": "normal",
            "assistant_only": True,
            "schema": {
                "body": {
                    "type": "object",
                    "properties": {"text": {"type": "string"}},
                    "required": ["text"],
                },
                "query": [],
                "path": [],
            },
            "multipart": False,
        }
        self._descriptors[api_id] = desc
        self._order.append(api_id)

    # -- catalogue API ------------------------------------------------------

    def discover(self, keyword: str = "", group: str = "", page: int = 1, page_size: int = 20) -> dict:
        items = []
        keyword = str(keyword or "").strip().lower()
        for api_id in self._order:
            desc = self._descriptors[api_id]
            if group and str(desc.get("group")) != str(group).strip():
                continue
            if keyword:
                hay = f"{api_id} {desc.get('name', '')} {desc.get('group', '')} {desc.get('keywords', '')}".lower()
                if not all(term in hay for term in keyword.split()):
                    continue
            items.append(deepcopy(desc))
        total = len(items)
        try:
            raw_page_size = int(page_size)
        except (TypeError, ValueError):
            raw_page_size = 20
        page_size = max(1, min(50, raw_page_size))
        total_pages = max(1, math.ceil(total / page_size))
        page = max(1, int(page or 1))
        page = min(page, total_pages)
        start = (page - 1) * page_size
        groups = []
        seen = set()
        for _, label in _GROUP_BY_PREFIX:
            if label not in seen:
                groups.append({"name": label, "page": _PAGE_BY_GROUP.get(label, "/")})
                seen.add(label)
        for label in ("管理设置", "业务接口"):
            if label not in seen:
                groups.append({"name": label, "page": _PAGE_BY_GROUP.get(label, "/")})
                seen.add(label)
        return {
            "items": items[start:start + page_size],
            "total": total,
            "page": page,
            "page_size": page_size,
            "groups": groups,
        }

    def get(self, api_id: str) -> dict:
        api_id = str(api_id or "").strip()
        desc = self._descriptors.get(api_id)
        if desc is None:
            raise AssistantError(f"未注册的接口：{api_id}", 404)
        return deepcopy(desc)

    # -- validation ----------------------------------------------------------

    def _declared_spoof_fields(self, desc: dict, model, route) -> set[str]:
        """Field names actually declared by the request body model / source."""
        names: set[str] = set()
        if model is not None:
            with suppress(Exception):
                schema = model.model_json_schema()

                def walk(node):
                    if not isinstance(node, dict):
                        return
                    for name in (node.get("properties") or {}):
                        names.add(str(name).lower())
                    for sub in (node.get("properties") or {}).values():
                        walk(sub)
                    for sub in (node.get("$defs") or {}).values():
                        walk(sub)

                walk(schema)
        for name in (desc.get("schema", {}).get("body", {}).get("properties") or {}):
            names.add(str(name).lower())
        for name in (desc.get("schema", {}).get("query", []) or []):
            names.add(str(name).lower())
        for name in (desc.get("schema", {}).get("path", []) or []):
            names.add(str(name).lower())
        if route is not None:
            for p in getattr(route.dependant, "query_params", ()) or ():
                names.add(str(getattr(p, "name", "")).lower())
            for p in getattr(route.dependant, "body_params", ()) or ():
                names.add(str(getattr(p, "name", "")).lower())
        return names

    def _is_spoof_key(self, key: str, declared_gated: set[str]) -> bool:
        lowered = str(key).lower()
        if lowered.startswith("_") or lowered in _HARD_SPOOF_FIELDS:
            return True
        if lowered in _SCHEMA_GATED_SPOOF_FIELDS and lowered not in declared_gated:
            return True
        return False

    def _reject_auth_spoof(self, body: dict, params: dict, path_params: dict, desc: dict, model, route) -> None:
        declared = self._declared_spoof_fields(desc, model, route)
        declared_gated = declared & _SCHEMA_GATED_SPOOF_FIELDS
        for container_name, container in (("body", body), ("query", params), ("path", path_params)):
            for key in list(container):
                if not isinstance(key, str):
                    continue
                if self._is_spoof_key(key, declared_gated):
                    raise AssistantError(f"不允许通过接口提交身份伪装字段：{key}")
        def nested(value, depth=0, parents=()):
            if depth > 20:
                raise AssistantError("请求结构过深。")
            if isinstance(value, dict):
                for key, item in value.items():
                    # The native Excel parser adds a row number, not actor metadata.
                    excel_row = (desc["id"] == "POST /api/plan-convergence/compare" and parents == ("scenarios", "rows")
                                 and key == "_excel_row" and type(item) is int and item >= 2)
                    if str(key).startswith("_") and not excel_row or str(key).lower() in _HARD_SPOOF_FIELDS and not (key == "scopes" and "evidence_files" in parents):
                        raise AssistantError("不允许提交身份伪装字段。")
                    nested(item, depth + 1, (*parents, key))
            elif isinstance(value, list):
                for item in value:
                    nested(item, depth + 1, parents)
        nested(body)

    def validate_operation(self, op: dict) -> tuple[dict, list[dict]]:
        op = dict(op or {})
        raw_id = str(op.get("api_id") or "").strip()
        desc = self._descriptors.get(raw_id)
        if desc is None:
            raise AssistantError(f"未注册的接口：{raw_id}")
        desc = deepcopy(desc)
        route = self._route_for_id.get(raw_id)

        path_params = dict(op.get("path_params") or {})
        params = dict(op.get("params") or {})
        body = dict(op.get("body") or {})
        files = dict(op.get("files") or {})

        model = self._models.get(raw_id)
        declared = self._declared_spoof_fields(desc, model, route)
        declared_gated = declared & _SCHEMA_GATED_SPOOF_FIELDS
        self._reject_auth_spoof(body, params, path_params, desc, model, route)

        # Path substitution using only exact registered placeholders.
        new_path = desc["path"]
        missing: list[dict] = []
        for name in desc["schema"]["path"]:
            if name not in path_params or path_params[name] in (None, ""):
                missing.append({
                    "name": name, "path": name, "label": _field_label(name),
                    "type": "text", "required": True, "section": "path_params",
                })
                continue
            raw = str(path_params[name])
            self._reject_value_unsafe(raw)
            new_path = new_path.replace("{" + name + "}", quote(raw, safe=""))
        # Only fail on placeholders that are NOT part of the registered path
        # (already-recorded missing path fields are reported, not raised).
        leftover = set(re.findall(r"\{([^{}]+)\}", new_path)) - set(desc["schema"]["path"])
        if leftover:
            raise AssistantError("路径参数未全部填充")

        # Files mapping: field -> list of assistant attachment IDs.
        normalized_files = {}
        for field, ids in files.items():
            if self._is_spoof_key(field, declared_gated):
                continue
            if not isinstance(ids, (list, tuple)):
                ids = [ids]
            attachment_ids = [str(i) for i in ids if str(i or "").strip()]
            if attachment_ids:
                normalized_files[field] = attachment_ids

        normalized_body = {k: v for k, v in body.items() if not self._is_spoof_key(k, declared_gated)}

        if model is not None:
            try:
                parsed = model.model_validate(normalized_body)
                normalized_body = parsed.model_dump(mode="json", exclude_none=False)
            except ValidationError as exc:
                type_errors = []
                for err in exc.errors():
                    loc = err.get("loc") or ()
                    path = ".".join(str(x) for x in loc)
                    err_type = err.get("type") or ""
                    if err_type == "missing":
                        missing.append(self._missing_from_error(desc["schema"]["body"], path))
                    else:
                        type_errors.append(f"{path or '请求体'}: {err.get('msg')}")
                if type_errors:
                    raise AssistantError("请求参数无效：" + "；".join(type_errors[:8]), status=422)
        else:
            # Generic endpoint: fields inferred from payload.get() are all optional.
            missing.extend(_missing_scalars(desc["schema"]["body"], normalized_body))
            if normalized_body:
                self._validate_generic_scalars(desc["schema"]["body"], normalized_body)
            if raw_id == "POST /api/cabinet-power/operations" and "groups" not in normalized_body:
                group_schema = desc["schema"]["body"]["properties"]
                missing.extend(_missing_scalars({"properties": group_schema, "required": ["action", "actual", "result"]}, normalized_body))

        # Required FastAPI query params (without a default) are also missing fields.
        for p in getattr(route.dependant, "query_params", ()) or ():
            name = getattr(p, "name", "")
            fi = None
            with suppress(Exception):
                fi = getattr(p, "field_info", None)
            required = False
            if fi is not None:
                try:
                    required = bool(fi.is_required())
                except Exception:  # noqa: BLE001
                    required = False
            if required and (name not in params or params[name] in (None, "")):
                missing.append({
                    "name": name, "path": name, "label": _field_label(name),
                    "type": "text", "required": True, "section": "params",
                })

        # Required FastAPI form / file fields are also missing fields.
        file_fields = _file_field_names(route)
        for p in getattr(route.dependant, "body_params", ()) or ():
            name = getattr(p, "name", "")
            fi = None
            with suppress(Exception):
                fi = getattr(p, "field_info", None)
            required = False
            if fi is not None:
                try:
                    required = bool(fi.is_required())
                except Exception:  # noqa: BLE001
                    required = False
            if required and name not in normalized_body and not (name in file_fields and name in normalized_files):
                if name in file_fields:
                    missing.append({
                        "name": name, "path": name, "label": _field_label(name),
                        "type": "file", "required": True, "section": "files",
                    })

        normalized_params = {k: v for k, v in params.items() if not self._is_spoof_key(k, declared_gated)}
        if desc.get("scope_mode") == "single":
            allowed = desc["scope_values"]
            for values in (params, body):
                if "scope" in values and values["scope"] not in (None, ""):
                    if not isinstance(values["scope"], str) or values["scope"] not in allowed:
                        raise AssistantError("该接口须选择单个楼栋：" + "、".join(allowed) + "，不能使用ALL或CAMPUS。", 422)
            section = desc["scope_section"]
            supplied = body if section == "body" else params
            # Only the list rejects non-admin admin=1; detail silently falls back.
            admin_view = (desc["path"] == "/api/critical-guard/tasks"
                          and str(params.get("admin", "")) == "1")
            if desc["scope_required"] and not supplied.get("scope") and not admin_view:
                if section == "body":
                    normalized_body.pop("scope", None)
                missing = [field for field in missing if not (field.get("path") == "scope" and field.get("section") == section)]
                missing.append({"name": "scope", "path": "scope", "label": "楼栋范围", "type": "select",
                                "required": True, "section": section,
                                "options": [{"value": value, "label": value + "楼"} for value in allowed]})
        for name in desc.get("required_files", []):
            if not normalized_files.get(name):
                missing.append({"name": name, "path": name, "label": "上传文件", "type": "file", "required": True, "section": "files"})
        if desc.get("upload_format") in {"raw", "raw-excel"} and sum(len(ids) for ids in normalized_files.values()) > 1:
            raise AssistantError("该接口每次上传一个文件，请分成多个操作步骤。")
        if desc.get("upload_format") == "raw-excel" and set(normalized_files) - {"file"}:
            raise AssistantError("Excel解析只接受file附件字段。")
        op_out = {
            "api_id": raw_id,
            "method": desc["method"],
            "path": new_path,
            "params": normalized_params,
            "body": normalized_body,
            "files": normalized_files,
            "risk": desc["risk"],
            "read_only": desc["read_only"],
        }
        return op_out, missing

    @staticmethod
    def _reject_value_unsafe(value: str) -> None:
        text = str(value or "")
        if "%" in text:
            raise AssistantError("路径标识不能包含预编码字符")
        for _ in range(3):
            decoded = unquote(text)
            if decoded == text:
                break
            text = decoded
        if "\x00" in text:
            raise AssistantError("路径参数包含非法字符")
        segments = [seg for seg in text.replace("\\", "/").split("/") if seg]
        if any(seg in (".", "..") for seg in segments) or \
                not text.strip() or text.strip() in (".", "..") or "\\" in text or \
                text.lstrip().startswith("/") or re.search(r"(?i)https?://", text):
            raise AssistantError("路径参数不允许穿越或外部地址")

    def _missing_from_error(self, schema: dict, path: str) -> dict:
        parts = [p for p in path.split(".") if p]
        if not parts:
            return {"name": "", "path": path, "label": "字段", "type": "text",
                    "required": True, "section": "body"}
        if _is_credential_key(parts[-1]):
            raise AssistantError("该操作需要原业务页面完成授权，助手不会收集或填写凭证。", 403)
        prop = schema
        for part in parts:
            raw = prop.get("items", {}) if part.isdigit() else (prop.get("properties") or {}).get(part, {})
            prop = _resolve_anyof(_resolve_schema_ref(raw, schema))
        base = _frontend_field(prop, schema, name=parts[-1])
        result = {
            "name": parts[-1], "path": path, "label": _field_label(parts[-1]),
            "type": base.get("type", "text"), "required": True, "section": "body",
        }
        result.update({key: value for key, value in base.items() if key != "type"})
        return result

    def _validate_generic_scalars(self, schema: dict, body: dict) -> None:
        props = schema.get("properties") or {}
        for name, value in body.items():
            prop = props.get(name)
            if prop and isinstance(value, (int, bool)) and _frontend_field(prop, schema).get("type") == "text":
                # String-typed inferred fields accept primitive coercion; keep as-is.
                continue

    # -- multipart / invoke --------------------------------------------------

    def parse_notice(self, text: str, *, fallback_work_type: str = "") -> dict:
        """Assistant-only virtual helper that reuses the project's parser unchanged."""
        from .workbench_lite import parse_pasted_notice_to_draft
        work_type, action, draft = parse_pasted_notice_to_draft(
            text, fallback_work_type=fallback_work_type)
        return {"work_type": work_type, "action": action, "draft": draft}

    async def invoke(self, op: dict, request, file_provider=None) -> dict:
        """Execute a validated write through the in-process ASGI gateway.

        `request` must be the original FastAPI Request.  Its cookie + Origin are
        forwarded unchanged so existing authentication and permission guards run
        against the SAME user.  This is a gateway, not an authorization bypass.
        """
        normalized, missing = self.validate_operation(op)
        if missing:
            raise AssistantError("缺少必填字段，请先补全后再执行写入操作。", status=422)
        desc = self._descriptors[normalized["api_id"]]
        if desc.get("assistant_only"):
            parsed = await asyncio.to_thread(self.parse_notice, str(normalized["body"].get("text", "")))
            return {"ok": True, "status": 200, "data": safe_data(parsed), "_raw": parsed, "api_id": desc["id"], "truncated": False}
        file_provider = file_provider or (lambda _id: None)

        base_url = request.base_url or ""
        origin = request.headers.get("origin") or str(base_url).rstrip("/")
        # Matching Origin / cookies are forwarded so endpoint guards behave as
        # if the browser made the same call.
        headers = {}
        cookie = request.headers.get("cookie")
        if cookie:
            headers["cookie"] = cookie
        if origin:
            headers["origin"] = origin
        for name in ("host", "x-forwarded-proto", "x-forwarded-host", "x-forwarded-port"):
            if request.headers.get(name):
                headers[name] = request.headers[name]

        client_host, client_port = ("127.0.0.1", 123)
        client = getattr(request, "client", None)
        if client is not None:
            if isinstance(client, tuple) or isinstance(client, list):
                client_host = client[0] or client_host
                client_port = client[1] if len(client) > 1 else client_port
            else:
                client_host = getattr(client, "host", None) or client_host
                client_port = getattr(client, "port", None) or client_port
        transport = httpx.ASGITransport(app=self.app, client=(client_host, client_port))

        try:
            async with httpx.AsyncClient(
                transport=transport,
                base_url=str(base_url).rstrip("/"),
                follow_redirects=False,
                timeout=httpx.Timeout(connect=5, read=90, write=30, pool=5),
            ) as client:
                kwargs: dict[str, Any] = {"headers": headers, "params": normalized.get("params") or None}
                if desc.get("upload_format") == "raw-excel":
                    aid = normalized["files"]["file"][0]
                    item = file_provider(aid)
                    if not isinstance(item, dict):
                        raise AssistantError("附件不存在或没有访问权限。", 404)
                    name = str(item.get("name") or "")
                    if not name.lower().endswith((".xlsx", ".xlsm")):
                        raise AssistantError("请选择xlsx或xlsm附件。")
                    raw = item.get("bytes")
                    if raw is None:
                        if not item.get("path"):
                            raise AssistantError("附件原文件不可用。", 404)
                        raw = await asyncio.to_thread(self._bounded_read_file, item["path"], aid, maximum=_EXCEL_MAX)
                    if not isinstance(raw, (bytes, bytearray)) or not raw.startswith(b"PK"):
                        raise AssistantError("Excel附件格式无效。")
                    if len(raw) > _EXCEL_MAX:
                        raise AssistantError("Excel文件不能超过8MiB。", 413)
                    kwargs["content"] = bytes(raw)
                    headers.update({"content-type": "application/octet-stream", "x-filename": quote(name, safe="")})
                elif desc.get("upload_format") == "raw":
                    attachment_ids = [fid for ids in normalized["files"].values() for fid in ids]
                    item = file_provider(attachment_ids[0])
                    if not item or not str(item.get("mime", "")).startswith("image/"):
                        raise AssistantError("此操作需要图片附件。")
                    raw = item.get("bytes")
                    if raw is None:
                        raw = await asyncio.to_thread(self._bounded_read_file, item["path"], item["id"])
                    kwargs["content"] = raw
                    headers["content-type"] = item["mime"]
                    kwargs["params"] = {**(normalized.get("params") or {}), "file_name": item["name"]}
                elif desc.get("multipart"):
                    files, data = await self._build_multipart(normalized, file_provider)
                    if files:
                        kwargs["files"] = files
                    if data:
                        kwargs["data"] = data
                else:
                    kwargs["json"] = normalized.get("body") or {}
                response = await asyncio.wait_for(client.request(normalized["method"], normalized["path"], **kwargs), timeout=45 if desc["read_only"] else 120)
        except AssistantError:
            raise
        except asyncio.TimeoutError:
            return {"ok": False, "status": 504, "error": "接口响应超时，写入是否生效尚未确认，请先核对原记录，未自动重发。" if not desc["read_only"] else "业务查询超时，暂无法取得数据。", "api_id": normalized["api_id"], "truncated": False}
        except Exception:  # noqa: BLE001 - network/transport errors
            # Never echo vendor request bodies, credentials or exception strings.
            return {"ok": False, "status": 502, "error": "网关调用失败，请稍后重试。",
                    "api_id": normalized["api_id"], "truncated": False}

        return self._consume_response(response, normalized["api_id"])

    async def _build_multipart(self, normalized: dict, file_provider) -> tuple[list | None, dict | None]:
        files: list[tuple] = []
        data: dict[str, str] = {}
        for name, value in (normalized.get("body") or {}).items():
            # Shallow scalar forwarding only; nested structures become JSON strings.
            if value is None:
                continue
            if isinstance(value, (str, int, float, bool)):
                data[name] = str(value)
            else:
                data[name] = json.dumps(value, ensure_ascii=False)
        for field, attachment_ids in (normalized.get("files") or {}).items():
            for aid in attachment_ids:
                meta = file_provider(aid)
                if meta is None:
                    raise AssistantError(f"无法读取附件：{aid}", 404)
                if isinstance(meta, dict):
                    name = str(meta.get("name") or "attachment")
                    mime = str(meta.get("mime") or meta.get("content_type") or "application/octet-stream")
                    raw = meta.get("bytes")
                    if raw is None:
                        path = meta.get("path")
                        if not path:
                            raise AssistantError(f"附件缺少数据源：{aid}")
                        raw = await asyncio.to_thread(self._bounded_read_file, path, aid)
                    body_bytes = raw if isinstance(raw, (bytes, bytearray)) else str(raw).encode("utf-8")
                else:
                    body_bytes = str(meta).encode("utf-8")
                    name, mime = "attachment", "application/octet-stream"
                if len(body_bytes) > _BINARY_MAX:
                    raise AssistantError(f"附件超过 {_BINARY_MAX // (1024 * 1024)}MiB 限制：{aid}")
                files.append((field, (name, body_bytes, mime)))
        return files or None, data or None

    @staticmethod
    def _bounded_read_file(path: str, aid: str, *, maximum: int = _BINARY_MAX) -> bytes:
        with open(path, "rb") as handle:
            sample = handle.read(maximum + 1)
        if len(sample) > maximum:
            raise AssistantError(
                f"附件超过 {maximum // (1024 * 1024)}MiB 限制：{aid}", 413
            )
        return sample

    # -- response consumption ------------------------------------------------

    def _consume_response(self, response: httpx.Response, api_id: str) -> dict:
        status = response.status_code
        ctype = (response.headers.get("content-type") or "").split(";")[0].strip().lower()
        body = response.content
        if _is_binary_ctype(ctype):
            return self._consume_binary(status, body, ctype, response.headers, api_id)
        raw = None
        text = body.decode("utf-8", errors="replace")
        try:
            raw = json.loads(text or "null")
        except (ValueError, RecursionError, UnicodeError):
            raw = None
        if raw is None:
            # Plain text/HTML or unparseable response.
            safe = safe_text(text)
            return {"ok": 200 <= status < 300, "status": status, "data": safe if safe else None,
                    "error": None if 200 <= status < 300 else self._error_from_response(raw, status),
                    "api_id": api_id, "truncated": _detect_truncation(raw) or len(text) > _TEXT_CAP}
        return self._consume_json(status, raw, api_id)

    def _consume_json(self, status: int, raw: Any, api_id: str) -> dict:
        ok = 200 <= status < 300
        if isinstance(raw, dict) and "ok" in raw:
            ok = ok and bool(raw.get("ok"))
            payload = raw.get("data", raw)
        else:
            payload = raw
        data = safe_data(payload)
        truncated = _detect_truncation(raw) or _serialized_truncated(data)
        return {
            "ok": ok,
            "status": status,
            "data": data if ok else data,
            "error": None if ok else self._error_from_response(raw, status),
            "api_id": api_id,
            "truncated": truncated,
            "_raw": payload,
        }

    def _consume_binary(self, status: int, body: bytes, ctype: str, headers, api_id: str) -> dict:
        if len(body) > _BINARY_MAX:
            raise AssistantError("二进制响应超过 20MiB 限制，已拒绝而非截断。", 413)
        name = _parse_content_disposition(headers.get("content-disposition")) or _fallback_name(ctype)
        return {
            "ok": 200 <= status < 300,
            "status": status,
            "data": {"name": name, "mime": ctype, "size": len(body)},
            "error": None if 200 <= status < 300 else "接口返回二进制错误",
            "api_id": api_id,
            "truncated": False,
            "_binary": {"content": body, "name": name, "mime": ctype, "size": len(body)},
        }

    @staticmethod
    def _error_from_response(data: Any, status: int) -> str:
        if isinstance(data, dict):
            err = data.get("error") or data.get("detail") or data.get("message")
            if err:
                return str(err)
        if isinstance(data, list):
            parts = [str(x.get("msg", x)) if isinstance(x, dict) else str(x) for x in data]
            return "；".join(parts)
        return f"接口返回错误（HTTP {status}）"


def _detect_truncation(value: Any, depth: int = 0) -> bool:
    """True when safe_data would cap rows/strings (so the result is not full data)."""
    if depth > 12:
        return value is not None
    if isinstance(value, list):
        if len(value) > 40:
            return True
        return any(_detect_truncation(v, depth + 1) for v in value)
    if isinstance(value, tuple):
        if len(value) > 40:
            return True
        return any(_detect_truncation(v, depth + 1) for v in value)
    if isinstance(value, dict):
        return any(_detect_truncation(v, depth + 1) for v in value.values())
    if isinstance(value, str) and len(value) > 6000:
        return True
    return False


def _serialized_truncated(data: Any) -> bool:
    try:
        return len(json.dumps(data, ensure_ascii=False)) > _TEXT_CAP
    except Exception:  # noqa: BLE001
        return True

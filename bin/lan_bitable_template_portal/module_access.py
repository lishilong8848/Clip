"""Shared module grants for page, API and assistant entry points."""
import asyncio
import copy
import re
from urllib.parse import parse_qs, urlsplit

MODULE_OPTIONS = [{"value": key, "label": label} for key, label in (
    ("maintenance", "维保通告"), ("change", "变更通告"), ("repair", "检修通告"),
    ("polling", "设备轮巡"), ("adjust", "设备调整"), ("power", "上下电通告"),
    ("event", "事件"), ("engineer_mop", "维保单办理"), ("repair_management", "维修项目"),
    ("cabinet", "机柜管理"), ("daily", "每日任务"), ("water", "水耗管理"),
    ("critical_guard", "重保管理"), ("risk_inspection", "专项排查"), ("drills", "演练管理"), ("learning", "画像学练"),
    ("plan_convergence", "计划收敛审查"), ("signatures", "签名管理"),
    ("knowledge", "共享知识库"), ("links", "多维表导航"), ("life_guide", "参考人生指南"),
    ("handover", "交接班审核"), ("work_handover", "人员工作交接"), ("assistant", "灯塔助手与飞书智能体"), ("accounts", "账号与权限"),
    ("tool_management", "工具管理"), ("daily_inspection", "日常巡检"), ("cold_source", "冷源运行状态"),
    ("eop_scenarios", "EOP 场景维护"), ("documents", "南通文件"), ("sops", "SOP 步骤编辑"),
    ("pre_shift", "班前检查"), ("zhihang_data", "智航数据拉取"), ("dingtalk_settings", "钉钉推送"),
    ("handover_settings", "交接班链接配置"), ("mop_settings", "MOP 配置"), ("feishu_settings", "飞书智能体配置"),
    ("release_journal", "开发问题与更新记录"), ("notice_history", "通告历史记忆"),
    ("backend_status", "后台状态"), ("backend_diagnostics", "高级诊断"),
)]
SETTINGS_MODULES = {
    "permissions": "accounts", "handover": "handover_settings", "mop": "mop_settings",
    "feishu": "feishu_settings", "dingtalk": "dingtalk_settings", "development": "release_journal",
    "status": "backend_status", "pressure": "backend_diagnostics",
}
SYSTEM_MODULES = frozenset(SETTINGS_MODULES.values()) - {"accounts"} | {"zhihang_data", "notice_history"}
LEGACY_MODULES = {
    "maintenance": ("sops",), "polling": ("sops",), "adjust": ("sops",),
    "daily": ("daily_inspection", "cold_source"), "risk_inspection": ("tool_management",),
    "drills": ("eop_scenarios",), "knowledge": ("documents",), "accounts": tuple(sorted(SYSTEM_MODULES)),
}
SOP_WORK_TYPES = frozenset({"maintenance", "polling", "adjust"})

_SHARED_PROFESSIONS = {'', '-', '—', '无', '未填写', '未分类', '未设置', '不限', '通用', '全部', '全部专业'}
_PROFESSION_ALIASES = {'强电': '电气', '供配电': '电气', '空调': '暖通', '暖通空调': '暖通'}
_PROFESSION_KEYS = ('specialty', 'profession', 'major', '专业', '所属专业', '专业类别', '专业（推送消息用）')


def profession_codes(value):
    if isinstance(value, (list, tuple, set, frozenset)):
        return frozenset().union(*(profession_codes(item) for item in value))
    if isinstance(value, dict):
        return profession_codes(value.get('text') or value.get('name') or value.get('value'))
    parts = (part.strip().removesuffix('专业').strip() if part.strip() not in _SHARED_PROFESSIONS else ''
             for part in re.split(r'[,，、;；/／|＋+&\n]+', str(value or '')))
    return frozenset(_PROFESSION_ALIASES.get(part, part) for part in parts if part not in _SHARED_PROFESSIONS)


def engineer_professions(profile):
    """None grants the existing view. An empty tuple grants unclassified records only."""
    category = profile.get('category')
    if profile.get('level') in {'L0', 'L1'} or profile.get('role') == 'admin':
        return None
    if category == 'engineer' or (not category and profile.get('level') == 'L2'):
        return tuple(sorted(profession_codes(profile.get('specialties'))))
    return None


def session_engineer_professions(auth, session):
    identity = (session.get('user') or {}).get('open_id') or session.get('open_id')
    profile = getattr(auth, 'account_profile', None)
    return engineer_professions(profile(identity)) if identity and callable(profile) else None


def profession_query(auth, session):
    professions = session_engineer_professions(auth, session)
    return {'professions': professions} if professions is not None else {}


def record_profession(record):
    for fields in (record, record.get('display_fields'), record.get('fields'), record.get('raw_fields'),
                   record.get('form_fields'), record.get('metadata'), record.get('draft')):
        if isinstance(fields, dict):
            for key in _PROFESSION_KEYS:
                value = fields.get(key)
                if profession_codes(value):
                    return value
    return ''


def profession_visible(professions, value):
    if professions is None:
        return True
    codes = profession_codes(value)
    return not codes or any(code in codes for code in professions)


def professional_record_visible(professions, record):
    return professions is None or isinstance(record, dict) and profession_visible(professions, record_profession(record))


def expand_legacy_modules(modules, level):
    enabled = dict.fromkeys([*modules, *(child for key in modules for child in LEGACY_MODULES.get(key, ())), *(["pre_shift"] if modules else [])])
    return [module for module in enabled if module not in SYSTEM_MODULES or level in {"L0", "L1"}]


for option in MODULE_OPTIONS:
    if option["value"] in SYSTEM_MODULES:
        option["admin_only"] = True
    option["group"] = ("通告办理" if option["value"] in {"maintenance", "change", "power", "polling", "adjust", "repair"}
                       else "协作与资料" if option["value"] in {"assistant", "knowledge", "links", "signatures", "life_guide", "handover", "work_handover"}
                       else "系统管理" if option["value"] in SYSTEM_MODULES | {"accounts"} else "业务管理")
MODULE_IDS = frozenset(item["value"] for item in MODULE_OPTIONS)
NOTICE_MODULES = frozenset({"maintenance", "change", "repair", "polling", "adjust", "power"})
PREFIXES = (
    ("/api/dingtalk-notices", "dingtalk_settings"),
    ("/api/risk-inspections", "risk_inspection"), ("/risk-inspections", "risk_inspection"),
    ("/api/tool-self-check", "tool_management"),
    ("/api/work-handovers", "work_handover"), ("/work-handover", "work_handover"),
    ("/api/assistant/knowledge", "knowledge"), ("/knowledge-base", "knowledge"),
    ("/api/document-library", "documents"), ("/document-library", "documents"),
    ("/api/release-journal", "release_journal"),
    ("/api/assistant/feishu-settings", "feishu_settings"),
    ("/api/handover-links-auth", "handover_settings"),
    ("/api/handover-links", "handover_settings"),
    ("/api/admin/mop-settings", "mop_settings"),
    ("/api/assistant", "assistant"), ("/assistant", "assistant"),
    ("/api/message-delivery", "assistant"),
    ("/api/feishu-workpartner", "assistant"),
    ("/api/auth/accounts", "accounts"), ("/api/admin", "accounts"), ("/admin", "accounts"),
    ("/api/signatures", "signatures"), ("/signature", "signatures"),
    ("/api/repair-management", "repair_management"), ("/repair-management", "repair_management"), ("/repair-status", "repair_management"),
    ("/api/events", "event"), ("/api/workbench-events", "event"), ("/event-management", "event"),
    ("/api/engineer", "engineer_mop"), ("/engineer", "engineer_mop"),
    ("/api/cabinet-power", "cabinet"), ("/cabinet-power", "cabinet"),
    ("/api/capacity", "water"), ("/water-management", "water"),
    ("/api/critical-guard", "critical_guard"), ("/critical-guard", "critical_guard"),
    ("/api/drills", "drills"), ("/drill-management", "drills"),
    ("/api/eop-scenarios", "eop_scenarios"), ("/eop-scenarios", "eop_scenarios"),
    ("/api/daily-tasks", "daily"), ("/daily-tasks", "daily"),
    ("/api/daily-inspection", "daily_inspection"), ("/daily-inspection", "daily_inspection"),
    ("/api/cold-source-monitor", "cold_source"), ("/cold-source-monitor", "cold_source"),
    ("/api/pre-shift-checks", "pre_shift"),
    ("/api/learning", "learning"), ("/learning", "learning"),
    ("/api/plan-convergence", "plan_convergence"), ("/plan-convergence", "plan_convergence"),
    ("/api/zhihang-data", "zhihang_data"), ("/zhihang-data", "zhihang_data"),
    ("/api/link-directory", "links"), ("/link-directory", "links"), ("/life-guide", "life_guide"),
    ("/api/handover", "handover"), ("/api/change-", "change"), ("/api/repair-refresh", "repair"),
)


def required_modules(path, work_type="", *, view="", tab="", method="GET"):
    path = path.rstrip("/") or "/"
    if path == "/risk-inspections" and view == "tool-self-check":
        return frozenset({"tool_management"})
    if path == "/admin/settings":
        return frozenset({SETTINGS_MODULES.get(tab or "permissions", "accounts")})
    if path == "/api/handover-links" and method in {"GET", "HEAD"}:
        return frozenset({"handover", "handover_settings"})
    if path.startswith("/api/backend/"):
        return frozenset({"backend_diagnostics" if path == "/api/backend/mock-pressure" else "backend_status"})
    if path == "/api/jobs/recent":
        return frozenset({"backend_status", *NOTICE_MODULES, "event"})
    if path == "/admin/history-memory" or path.startswith("/api/admin/notice-memory/"):
        return frozenset({"notice_history"})
    if path == "/signature-management":
        return frozenset({"signatures"})
    if path == "/api/signatures/management/refresh":
        return frozenset({"signatures", "engineer_mop"})
    if path == "/" and view in {"assistant", "handover", "sops"}:
        return frozenset({view})
    if path in {"/api/signatures/management/mop-people", "/api/signatures/management/mop-invite", "/api/signatures/management/mop-list"}:
        return frozenset({"engineer_mop"})
    if path.startswith("/api/qt/") or path in {"/api/assistant/feishu-event", "/api/health"}:
        return frozenset()
    if path.startswith("/api/auth/permissions") or path == "/api/auth/permission-requests/admin" or path.startswith("/api/auth/permission-requests/") and path.endswith(("/approve", "/reject", "/bulk-approve", "/bulk-reject")):
        return frozenset({"accounts"})
    if path == "/api/signatures/people":
        return frozenset({"signatures", "engineer_mop", "drills", "maintenance", "polling", "adjust"})
    if path.startswith("/api/polling-sops"):
        if method in {"GET", "HEAD"} and (not work_type or work_type in SOP_WORK_TYPES):
            return frozenset({"sops", work_type or "polling"})
        return frozenset({"sops"})
    if path.startswith("/api/polling-work-orders"):
        return frozenset({work_type}) if work_type in NOTICE_MODULES else frozenset({"maintenance", "polling", "adjust"})
    if path.startswith(("/api/workbench", "/workbench-lite", "/api/maintenance-", "/api/notice-", "/api/ongoing-items", "/api/jobs")) or path in {"/api/bootstrap", "/api/records", "/api/refresh", "/api/generate", "/api/send-generated", "/api/history-summary", "/api/source-refresh-status", "/api/qt-active-items"}:
        return frozenset({work_type}) if work_type in NOTICE_MODULES | {"event"} else NOTICE_MODULES
    for prefix, module in PREFIXES:
        if path == prefix or path.startswith(prefix + "/") or prefix.endswith("-") and path.startswith(prefix) or prefix == "/signature" and path == "/signature-management" or prefix == "/api/handover" and path.startswith(prefix + "-"):
            return frozenset({module})
    return frozenset()


def allowed_modules(auth, session):
    from .cabinet_guest import is_cabinet_guest
    if is_cabinet_guest(session):
        return {"cabinet"}
    from .portal_auth import topic_execution
    delegated = topic_execution(auth, session)
    if delegated is not None:
        return set(delegated.modules)
    profile = auth.account_profile((session.get("user") or {}).get("open_id") or session.get("open_id"))
    return set(profile.get("modules", []))


def require_module(auth, session, module):
    if module not in allowed_modules(auth, session):
        from .portal_service import PortalError
        raise PortalError("当前账号尚未获授该功能权限，请联系管理员。")


def visible_url(modules, url):
    parsed = urlsplit(url)
    query = parse_qs(parsed.query)
    needed = required_modules(parsed.path, query.get("work_type", [""])[0],
                              view=query.get("view", [""])[0], tab=query.get("tab", [""])[0])
    return not needed or bool(needed & set(modules))


def filter_workbench(payload, modules):
    allowed = set(modules)
    if NOTICE_MODULES <= allowed or not isinstance(payload, dict):
        return payload
    from .workbench_lite import _item_work_type
    result = copy.deepcopy(payload)
    for key in ("records", "ongoing", "closed", "undo_actions", "items"):
        if isinstance(result.get(key), list):
            result[key] = [row for row in result[key] if _item_work_type(row) in allowed]
    for key in ("record_type_counts", "ongoing_type_counts"):
        if isinstance(result.get(key), dict):
            result[key] = {kind: count for kind, count in result[key].items() if kind in allowed}
    if "change" not in allowed:
        result.pop("zhihang_change_records", None)
    if isinstance(result.get("days"), list):
        result["days"] = [filter_workbench({"daily_summary": day}, modules)["daily_summary"] for day in result["days"]]
        result["days"] = [day for day in result["days"] if day["items"]]
    summary = result.get("daily_summary")
    if isinstance(summary, dict):
        items = [row for row in summary.get("items", []) if _item_work_type(row) in allowed]
        summary["items"] = items
        summary["stats"] = {"started": sum(bool(row.get("started_at")) for row in items),
                            "updated": sum(any(action.get("action") == "update" for action in row.get("actions", [])) for row in items),
                            "ended": sum(row.get("status") == "已结束" for row in items),
                            "ongoing": sum(result["ongoing_type_counts"].values()) if "ongoing_type_counts" in result else sum(bool(row.get("started_at")) and row.get("status") != "已结束" for row in items)}
    return result


def filter_overview(payload, modules):
    result = copy.deepcopy(payload)
    allowed = set(modules)
    for row in (result.get("scopes") or {}).values():
        for kind in NOTICE_MODULES | {"event"}:
            if kind not in allowed:
                for key in list(row):
                    if key.startswith(kind + "_"):
                        row.pop(key, None)
        row["ongoing_titles"] = [item for item in row.get("ongoing_titles", []) if item.get("work_type") in allowed]
        row["ongoing_title_count"] = sum(row.get(kind + "_ongoing", 0) for kind in NOTICE_MODULES & allowed)
        if not NOTICE_MODULES <= allowed:
            row.pop("closed_today", None)
    if "prepared_workbenches" in result:
        result["prepared_workbenches"] = {scope: filter_workbench(data, modules) for scope, data in result["prepared_workbenches"].items()}
    return result


async def module_denial(request, controller, runtime):
    from fastapi.responses import JSONResponse
    path = request.url.path
    needed = required_modules(path, request.query_params.get("work_type", ""),
                              view=request.query_params.get("view", ""), tab=request.query_params.get("tab", ""),
                              method=request.method)
    if not needed:
        return None
    session = await asyncio.to_thread(controller._current_session, request)
    if not session:
        return None  # Original login or signed-link validation still applies.
    values = dict(request.query_params)
    if needed == NOTICE_MODULES and request.method in {"POST", "PUT", "PATCH", "DELETE"} and "application/json" in request.headers.get("content-type", ""):
        try:
            body = await controller._read_json_request(request, max_bytes=1024 * 1024)
            if isinstance(body, dict):
                values.update(body)
        except Exception:
            return JSONResponse({"ok": False, "error": "请求参数无效。"}, status_code=400)
        needed = required_modules(path, values.get("work_type", ""))
    if path.startswith("/api/jobs/") and path.split("/")[3] not in {"recent", "stream", "batch"}:
        job = await asyncio.to_thread(runtime.service.get_job, path.split("/")[3]) or {}
        needed = required_modules(path, (job.get("request") or {}).get("work_type", ""))
        needed = needed | {"backend_status"}
    match = re.match(r"^/api/notice-undo/([^/]+)", path)
    if match:
        undo = await asyncio.to_thread(runtime.state_store.get_notice_undo_action, match[1]) or {}
        needed = required_modules(path, undo.get("work_type", ""))
    allowed = await asyncio.to_thread(allowed_modules, runtime.auth_manager, session)
    request.state.allowed_modules = allowed
    if needed & allowed:
        return None
    if needed == frozenset({'risk_inspection'}):
        from .risk_inspection_routes import risk_task_access
        rid = request.query_params.get('id', '') if path.startswith('/risk-inspections') else ''
        match = re.match(r'^/api/risk-inspections/([a-f0-9]{32})(?:/|$)', path)
        if match:
            rid = match[1]
        if await asyncio.to_thread(risk_task_access, runtime, session, rid):
            request.state.allowed_modules = allowed | {'risk_inspection'}
            return None
    message = "当前账号尚未获授该功能权限，请联系管理员。"
    if request.method in {"GET", "HEAD"} and not path.startswith("/api/"):
        from .page_errors import page_error
        return page_error(403, message)
    return JSONResponse({"ok": False, "error": message, "error_code": "module_access_denied"},
                        status_code=403, media_type="application/json; charset=utf-8")

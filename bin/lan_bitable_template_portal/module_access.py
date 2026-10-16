"""Shared module grants for page, API and assistant entry points."""
import asyncio
import copy
import re
from urllib.parse import parse_qs, urlsplit

MODULE_OPTIONS = [{"value": key, "label": label} for key, label in (
    ("maintenance", "维保通告"), ("change", "变更通告"), ("repair", "检修通告"),
    ("polling", "设备轮巡"), ("adjust", "设备调整"), ("power", "上下电通告"),
    ("event", "事件管理"), ("engineer_mop", "维护单管理"), ("repair_management", "检修管理（维修单）"),
    ("cabinet", "机柜管理"), ("daily", "每日任务"), ("water", "水耗管理"),
    ("critical_guard", "重保管理"), ("risk_inspection", "风险排查"), ("drills", "演练管理"), ("learning", "画像学练"),
    ("plan_convergence", "计划收敛审查"), ("signatures", "签名管理"),
    ("knowledge", "共享知识库"), ("links", "多维表导航"), ("life_guide", "参考人生指南"),
    ("handover", "交接班审核"), ("work_handover", "人员工作交接"), ("assistant", "灯塔助手与飞书智能体"), ("accounts", "账号与设置"),
)]
for option in MODULE_OPTIONS:
    option["group"] = ("通告办理" if option["value"] in {"maintenance", "change", "power", "polling", "adjust", "repair"}
                       else "协作与资料" if option["value"] in {"assistant", "knowledge", "links", "signatures", "life_guide", "handover", "work_handover"}
                       else "系统管理" if option["value"] == "accounts" else "业务管理")
MODULE_IDS = frozenset(item["value"] for item in MODULE_OPTIONS)
NOTICE_MODULES = frozenset({"maintenance", "change", "repair", "polling", "adjust", "power"})
PREFIXES = (
    ("/api/risk-inspections", "risk_inspection"), ("/risk-inspections", "risk_inspection"),
    ("/api/work-handovers", "work_handover"), ("/work-handover", "work_handover"),
    ("/api/assistant/knowledge", "knowledge"), ("/knowledge-base", "knowledge"),
    ("/api/assistant", "assistant"), ("/assistant", "assistant"),
    ("/api/message-delivery", "assistant"),
    ("/api/feishu-workpartner", "assistant"),
    ("/api/auth/accounts", "accounts"), ("/api/admin", "accounts"), ("/admin", "accounts"),
    ("/api/signatures", "signatures"), ("/signature", "signatures"),
    ("/api/repair-management", "repair_management"), ("/repair-management", "repair_management"), ("/repair-status", "repair_management"),
    ("/api/events", "event"), ("/event-management", "event"),
    ("/api/engineer", "engineer_mop"), ("/engineer", "engineer_mop"),
    ("/api/cabinet-power", "cabinet"), ("/cabinet-power", "cabinet"),
    ("/api/capacity", "water"), ("/water-management", "water"),
    ("/api/critical-guard", "critical_guard"), ("/critical-guard", "critical_guard"),
    ("/api/drills", "drills"), ("/drill-management", "drills"),
    ("/api/daily-tasks", "daily"), ("/daily-tasks", "daily"),
    ("/api/learning", "learning"), ("/learning", "learning"),
    ("/api/plan-convergence", "plan_convergence"), ("/plan-convergence", "plan_convergence"),
    ("/api/link-directory", "links"), ("/link-directory", "links"), ("/life-guide", "life_guide"),
    ("/api/handover", "handover"), ("/api/change-", "change"), ("/api/repair-refresh", "repair"),
)


def required_modules(path, work_type=""):
    if path in {"/api/signatures/management/mop-people", "/api/signatures/management/mop-invite", "/api/signatures/management/mop-list"}:
        return frozenset({"engineer_mop"})
    if path.startswith("/api/qt/") or path in {"/api/assistant/feishu-event", "/api/health"}:
        return frozenset()
    if path.startswith("/api/auth/permissions") or path == "/api/auth/permission-requests/admin" or path.startswith("/api/auth/permission-requests/") and path.endswith(("/approve", "/reject", "/bulk-approve", "/bulk-reject")):
        return frozenset({"accounts"})
    if path == "/api/signatures/people":
        return frozenset({"signatures", "engineer_mop", "drills", "maintenance", "polling", "adjust"})
    if path.startswith(("/api/polling-sops", "/api/polling-work-orders")):
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
    profile = auth.account_profile((session.get("user") or {}).get("open_id") or session.get("open_id"))
    return set(profile.get("modules", []))


def require_module(auth, session, module):
    if module not in allowed_modules(auth, session):
        from .portal_service import PortalError
        raise PortalError("当前账号尚未获授该功能权限，请联系管理员。")


def visible_url(modules, url):
    parsed = urlsplit(url)
    needed = required_modules(parsed.path, parse_qs(parsed.query).get("work_type", [""])[0])
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
    needed = required_modules(path, request.query_params.get("work_type", ""))
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
    match = re.match(r"^/api/notice-undo/([^/]+)", path)
    if match:
        undo = await asyncio.to_thread(runtime.state_store.get_notice_undo_action, match[1]) or {}
        needed = required_modules(path, undo.get("work_type", ""))
    allowed = await asyncio.to_thread(allowed_modules, runtime.auth_manager, session)
    request.state.allowed_modules = allowed
    if needed & allowed:
        return None
    return JSONResponse({"ok": False, "error": "当前账号尚未获授该功能权限，请联系管理员。", "error_code": "module_access_denied"}, status_code=403)

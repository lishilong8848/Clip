"""Action-specific personnel management policy; business scopes remain separate."""
from __future__ import annotations

CATEGORY_OPTIONS = [
    {"value": "shift_leader", "label": "值班长"},
    {"value": "operator", "label": "值班员"},
    {"value": "engineer", "label": "工程师"},
    {"value": "supervisor", "label": "主管"},
    {"value": "manager", "label": "经理"},
    {"value": "duty", "label": "固定值班账号"},
    {"value": "other", "label": "其他/待分类"},
]
MANAGED_CATEGORIES = frozenset({"engineer", "operator", "shift_leader", "duty"})


def categorize_position(position):
    text = str(position or "").strip()
    for category, keyword in (("shift_leader", "值班长"), ("operator", "值班员"),
                              ("engineer", "工程师"), ("supervisor", "主管"), ("manager", "经理")):
        if keyword in text:
            return category
    return "other"


def account_allowed(profile, target, action):
    if action not in {"view", "edit", "delete"} or not profile.get("can_manage_accounts"):
        return False
    if action != "view" and (target.get("locked") or target.get("blocked_reason")):
        return False
    if action == "delete" and (target.get("kind") == "duty" or target.get("category") == "duty"):
        return False
    if profile["level"] in {"L0", "L1"}:
        return True
    if target.get("role") == "admin" or target.get("level") in {"L0", "L1", "L3"}:
        return False
    if profile["level"] == "L2":
        return action == "edit" and target.get("category") == "engineer"
    return (profile["level"] == "L3" and target.get("category") in MANAGED_CATEGORIES
            and target.get("own_scopes") == [profile.get("home_scope")])


async def business_delete_denial(request, controller, runtime):
    """Shared-record deletion whitelist, also used by assistant's ASGI API calls."""
    import asyncio
    import re
    from fastapi.responses import JSONResponse

    path = request.url.path
    # Local conversation/draft attachments are not deletion of business records.
    personal = (path.startswith("/api/assistant/") or path == "/api/workbench/draft"
                or path.startswith(("/api/notice-images/", "/api/notice-attachments/"))
                or re.fullmatch(r"/api/change-confirmations/[^/]+/screenshot", path))
    deleting = request.method == "DELETE" and path.startswith("/api/") and not personal
    deleting = deleting or (request.method == "POST" and bool(re.search(r"/(?:delete|remove)(?:/|$)", path)))
    undo_match = re.fullmatch(r"/api/notice-undo/([^/]+)/apply", path)
    if request.method == "POST" and undo_match:
        undo = await asyncio.to_thread(runtime.state_store.get_notice_undo_action, undo_match[1]) or {}
        deleting = undo.get("action_type") == "start"
    if request.method == "POST" and path in {"/api/workbench-actions", "/api/maintenance-actions"}:
        try:
            payload = await controller._read_json_request(request, max_bytes=1024 * 1024)
        except (ValueError, TypeError):
            return JSONResponse({"ok": False, "error": "通告参数无效。"}, status_code=400)
        deleting = isinstance(payload, dict) and str(payload.get("action", "")).startswith(("delete", "remove"))
    if not deleting:
        return None
    session = await asyncio.to_thread(controller._current_session, request)
    if not session:
        return JSONResponse({"ok": False, "error": "请先登录。"}, status_code=401)
    user = session.get("user") or {}
    profile = await asyncio.to_thread(runtime.auth_manager.account_profile, user.get("open_id"))
    if profile["can_delete_business"]:
        return None  # Original endpoint still validates the actual record, owner and building.
    await asyncio.to_thread(runtime.state_store.record_business_operation_audit,
        domain="permissions", action="delete", status="denied", actor_open_id=user.get("open_id", ""),
        metadata={"level": profile["level"], "path": path})
    return JSONResponse({"ok": False, "error": "仅超级管理员或本楼栋主管可删除业务记录。"}, status_code=403)

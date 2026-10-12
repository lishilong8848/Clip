"""Action-specific personnel management policy; business scopes remain separate."""
from __future__ import annotations

import copy

# module_access.py is the authoritative option list and is guaranteed to exist.
from .module_access import MODULE_OPTIONS, MODULE_IDS

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

LEVEL_RANK = {"L0": 0, "L1": 1, "L2": 2, "L3": 3, "L4": 4}
LEVEL_OPTIONS = [
    {"value": "L1", "label": "L1 全楼管理员"},
    {"value": "L2", "label": "L2 工程师编辑"},
    {"value": "L3", "label": "L3 本楼主管"},
    {"value": "L4", "label": "L4 本楼基础"},
]


class PolicyViolation(Exception):
    """Raised when an account access override violates the HR/level policy."""

    def __init__(self, message: str, status: int = 403) -> None:
        super().__init__(message)
        self.status = int(status)


def categorize_position(position):
    text = str(position or "").strip()
    for category, keyword in (("shift_leader", "值班长"), ("operator", "值班员"),
                              ("engineer", "工程师"), ("supervisor", "主管"), ("manager", "经理")):
        if keyword in text:
            return category
    return "other"


def level_rank(level):
    return LEVEL_RANK.get(str(level or "").strip(), 99)


def module_order():
    return tuple(option["value"] for option in MODULE_OPTIONS)


def default_modules(level):
    """Deterministic default module set for every assignable level.

    L0/L1/L2/L3 retain every previously available function; only L4 is the
    base tier that excludes the ``accounts`` management module.
    """
    ordered = module_order()
    level = str(level or "").strip()
    if level == "L4":
        allowed = set(ordered) - {"accounts"}
    else:  # L0/L1 are admins, L2/L3 managers keep all originally usable modules.
        allowed = set(ordered)
    return tuple(option for option in ordered if option in allowed)


def _grantable_levels(actor_level):
    """Levels the actor may themselves assign to a target.

    L0/L1 hand out the full L1-L4 range; L2 is edit-only and may only manage an
    already-L2 engineer (same level, no escalation); L3 only manages own-building
    L4 accounts and must never grant a peer supervisor (role conflict).
    """
    rank = level_rank(actor_level)
    if rank <= 1:
        return {"L1", "L2", "L3", "L4"}
    if rank == 2:
        return {"L2"}
    if rank == 3:
        return {"L4"}
    return set()


def level_options_for(actor_level):
    """Levels the actor is permitted to grant (L0 may grant as L1)."""
    grantable = _grantable_levels(actor_level)
    return [option for option in LEVEL_OPTIONS if option["value"] in grantable]


def category_options_for(actor_level):
    rank = level_rank(actor_level)
    if rank <= 1:
        return copy.deepcopy(CATEGORY_OPTIONS)
    if rank == 2:
        return [option for option in CATEGORY_OPTIONS if option["value"] == "engineer"]
    if rank == 3:
        return [option for option in CATEGORY_OPTIONS if option["value"] in MANAGED_CATEGORIES]
    return []


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
    # L2 是编辑型工程师：可跨楼编辑所有工程师，但从不查看/删除/升级。
    if profile["level"] == "L2":
        return action == "edit" and target.get("category") == "engineer"
    # L3 manages only own-building L4 accounts; peers (supervisors) stay out of reach.
    return (profile["level"] == "L3" and target.get("category") in MANAGED_CATEGORIES
            and target.get("level") == "L4"
            and target.get("own_scopes") == [profile.get("home_scope")])


def resolve_access_change(actor, target, *, level=None, category=None, scopes=None, modules=None):
    """Validate a desired access override against the actor's grantable authority.

    ``actor``/``target`` are ``account_profile`` dicts. Returns a dict with the
    resolved ``level``, ``category``, ``scopes``, ``modules`` and ``role``.
    Raises :class:`PolicyViolation` for any escalation, out-of-scope grant or
    module the actor itself does not hold.
    """
    actor_level = str(actor.get("level") or "").strip()
    actor_scopes = set(actor.get("scope_rules") or [])
    actor_modules = set(actor.get("modules") or [])
    target_level = str(target.get("level") or "").strip()
    target_home = str(target.get("home_scope") or "").strip()

    # Fixed super-user can never be re-granted through the management API.
    if target_level == "L0" or (target.get("home_scope") == "H" and target.get("category") == "duty"):
        raise PolicyViolation("固定管理员或固定值班账号权限不可修改。", 403)
    if actor_level == "L4":
        raise PolicyViolation("当前账号没有人员管理权限。", 403)

    # Desired level (defaults to keeping the target's current level).
    resolved_level = str(level or target_level or "").strip()
    if resolved_level not in {"L0", "L1", "L2", "L3", "L4"}:
        raise PolicyViolation("无效的管理级别。", 400)
    if resolved_level not in _grantable_levels(actor_level):
        raise PolicyViolation("不能将账号设置到超出自身可授予的级别。", 403)
    # L2 may only do same-level management and L3 only own-building L4 accounts,
    # so the target must already sit at the level the actor is allowed to grant.
    if actor_level == "L2" and target_level != "L2":
        raise PolicyViolation("L2 只能在同级工程师范围内管理，不能更改其管理级别。", 403)
    if actor_level == "L3" and target_level != "L4":
        raise PolicyViolation("L3 只能管理本楼栋的 L4 基础账号。", 403)

    level_changed = str(level or "").strip() not in ("", target_level)

    # Desired category.
    if category is not None:
        if not any(option["value"] == str(category).strip() for option in CATEGORY_OPTIONS):
            raise PolicyViolation("无效的业务岗位。", 400)
        resolved_category = str(category).strip()
        if actor_level not in {"L0", "L1"}:
            allowed_categories = category_options_for(actor_level)
            if not any(option["value"] == resolved_category for option in allowed_categories):
                raise PolicyViolation("不能将账号设置到超出自身可管理的岗位。", 403)
    else:
        resolved_category = str(target.get("category") or "other").strip()

    # Desired scopes. When omitted, keep the current explicit config unless the
    # level is changing (then seed the default capability for the new level).
    if scopes is None:
        if level_changed:
            if resolved_level in {"L0", "L1"}:
                resolved_scopes = sorted(actor_scopes)
            else:
                resolved_scopes = [target_home] if target_home else []
        else:
            resolved_scopes = list(target.get("scope_rules") or [])
            if not resolved_scopes and target_home:
                resolved_scopes = [target_home]
    else:
        if not isinstance(scopes, list) or not scopes:
            raise PolicyViolation("请选择至少一个可访问楼栋。", 400)
        incoming = set(str(scope).strip() for scope in scopes)
        if incoming.isdisjoint(actor_scopes | {"ALL"}):
            raise PolicyViolation("不能授予超过自身可管辖范围的楼栋。", 403)
        if not incoming.issubset(actor_scopes) and "ALL" not in actor_scopes:
            raise PolicyViolation("不能授予超过自身可管辖范围的楼栋。", 403)
        if "ALL" in incoming and resolved_level not in {"L0", "L1"}:
            raise PolicyViolation("普通级别不能授予全楼栋权限。", 403)
        if actor_level not in {"L0", "L1"}:
            # L2/L3 must stay inside their own managed building.
            if target_home and incoming != {target_home}:
                raise PolicyViolation("非超级级别只能在本人所属楼栋内授权。", 403)
            if not incoming.issubset({target_home}):
                raise PolicyViolation("非超级级别只能在本人所属楼栋内授权。", 403)
        resolved_scopes = [str(scope).strip() for scope in scopes]

    # Desired modules. When omitted, keep the current explicit modules unless the
    # level is changing. ``modules == []`` is a deliberate full revocation and is
    # never silently replaced with the level defaults.
    if modules is None:
        if level_changed:
            resolved_modules = list(default_modules(resolved_level))
        else:
            current_modules = target.get("modules")
            if isinstance(current_modules, list):
                # Preserve an explicit empty set (full revocation) too.
                resolved_modules = list(current_modules)
            else:
                resolved_modules = list(default_modules(resolved_level))
    else:
        if not isinstance(modules, list):
            raise PolicyViolation("模块参数无效。", 400)
        incoming = set(str(module).strip() for module in modules)
        if not incoming.issubset(MODULE_IDS):
            raise PolicyViolation("包含无效模块。", 400)
        if not incoming.issubset(actor_modules):
            raise PolicyViolation("不能授予自己没有的模块权限。", 403)
        if "accounts" in incoming and resolved_level == "L4":
            raise PolicyViolation("L4 基础账号不能授予账号管理模块。", 403)
        resolved_modules = [str(module).strip() for module in modules]

    if not set(resolved_modules).issubset(actor_modules):
        raise PolicyViolation("不能授予自己没有的模块权限。", 403)
    resolved_role = "admin" if resolved_level in {"L0", "L1"} else "building"
    return {
        "level": resolved_level,
        "category": resolved_category,
        "scopes": resolved_scopes,
        "modules": resolved_modules,
        "role": resolved_role,
    }


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

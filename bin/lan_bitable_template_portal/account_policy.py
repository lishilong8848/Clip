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
    {"value": "L0", "label": "L0 超级管理员"},
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

    Existing account managers may promote their editable targets to L0.
    Other grants retain the original hierarchy rules.
    """
    rank = level_rank(actor_level)
    if rank == 0:
        return {"L0", "L1", "L2", "L3", "L4"}
    if rank == 1:
        return {"L0", "L2", "L3", "L4"}
    if rank == 2:
        return {"L0", "L3", "L4"}
    if rank == 3:
        return {"L0", "L4"}
    return set()


def level_options_for(actor_level):
    """Levels available for an otherwise editable target."""
    grantable = _grantable_levels(actor_level)
    return [option for option in LEVEL_OPTIONS if option["value"] in grantable]


def category_options_for(actor_level):
    rank = level_rank(actor_level)
    if rank <= 3:
        return copy.deepcopy(CATEGORY_OPTIONS)
    return []


def account_in_scope(profile, target):
    # Both the identity's home and its current grants must fit the manager's scope.
    scopes = set(target.get('scope_rules', target.get('scopes', [])) or [])
    if target.get('home_scope'):
        scopes.add(target['home_scope'])
    elif 'home_scope' not in target:
        scopes.update(target.get('own_scopes') or [])
    return bool(scopes) and scopes.issubset(set(profile.get('scope_rules') or []))


def account_allowed(profile, target, action):
    if action not in {"view", "edit", "delete"} or not profile.get("can_manage_accounts"):
        return False
    if target.get('is_self') or profile.get('open_id') and profile.get('open_id') == target.get('open_id'):
        return action == 'view'
    peers_l0 = profile.get('level') == target.get('level') == 'L0'
    if level_rank(target.get('level')) <= level_rank(profile.get('level')) and not peers_l0:
        return False
    if action != "view" and (target.get("locked") or target.get("is_fixed_superuser") or target.get("blocked_reason")):
        return False
    if action == "delete" and (target.get("kind") == "duty" or target.get("category") == "duty"):
        return False
    if profile["level"] in {"L0", "L1"}:
        return True
    if target.get("role") == "admin" or target.get("level") not in {"L3", "L4"}:
        return False
    if profile["level"] in {"L2", "L3"} and account_in_scope(profile, target):
        return True
    # Preserve L2's existing cross-building, edit-only L4 engineer exception.
    return (profile["level"] == "L2" and action == "edit"
            and target.get("level") == "L4" and target.get("category") == "engineer")


def account_details_allowed(profile, target):
    # Peer personnel details do not grant access to signatures, audits or impersonation.
    return account_allowed(profile, target, 'view') or bool(
        profile.get('can_manage_accounts') and profile.get('level') == target.get('level') == 'L1')


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
    if target.get('is_self') or actor.get('open_id') and actor.get('open_id') == target.get('open_id'):
        raise PolicyViolation('不能修改自己的管理权限，请由其他有权限的人员处理。', 403)
    if level_rank(target_level) <= level_rank(actor_level) and not actor_level == target_level == 'L0':
        raise PolicyViolation("不能修改同级或更高级账号，请由其上级审批。", 403)

    # Fixed super-user can never be re-granted through the management API.
    if target.get('is_fixed_superuser') or target.get('locked'):
        raise PolicyViolation("固定管理员权限不可修改。", 403)
    if actor_level == "L4":
        raise PolicyViolation("当前账号没有人员管理权限。", 403)
    if not account_allowed(actor, target, 'edit'):
        raise PolicyViolation("当前账号无权管理此人员或其现有楼栋范围。", 403)

    # Desired level (defaults to keeping the target's current level).
    resolved_level = str(level or target_level or "").strip()
    if resolved_level not in {"L0", "L1", "L2", "L3", "L4"}:
        raise PolicyViolation("无效的管理级别。", 400)
    if resolved_level not in _grantable_levels(actor_level):
        raise PolicyViolation("不能将账号设置到超出自身可授予的级别。", 403)
    cross_building_engineer = actor_level == 'L2' and not account_in_scope(actor, target)
    if cross_building_engineer and resolved_level != 'L0':
        raise PolicyViolation("跨楼工程师仅保留原基础编辑和显式 L0 授权，不能修改其它访问权限。", 403)
    # Supervisors configure only lower-level accounts inside their managed scope.
    if actor_level == "L3" and target_level != "L4":
        raise PolicyViolation("L3 只能管理本楼栋的 L4 基础账号。", 403)

    level_changed = str(level or "").strip() not in ("", target_level)

    # Desired category.
    if category is not None:
        if not any(option["value"] == str(category).strip() for option in CATEGORY_OPTIONS):
            raise PolicyViolation("无效的业务岗位。", 400)
        resolved_category = str(category).strip()
        if cross_building_engineer and resolved_category != 'engineer':
            raise PolicyViolation("跨楼工程师不能更改为其它岗位。", 403)
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
        if not incoming.issubset(actor_scopes):
            raise PolicyViolation("不能授予超过自身可管辖范围的楼栋。", 403)
        if "ALL" in incoming and resolved_level not in {"L0", "L1"}:
            raise PolicyViolation("普通级别不能授予全楼栋权限。", 403)
        if actor_level not in {"L0", "L1"}:
            if not incoming.issubset(actor_scopes):
                raise PolicyViolation("只能授予本人管理范围内的楼栋。", 403)
        resolved_scopes = [str(scope).strip() for scope in scopes]
    if resolved_level in {'L0', 'L1'}:
        resolved_scopes = sorted(actor_scopes)
    if not set(resolved_scopes).issubset(actor_scopes):
        raise PolicyViolation("目标账号现有楼栋超出本人管理范围，请由更高权限人员处理。", 403)

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


def notice_delete_allowed(profile, work_type):
    from .module_access import NOTICE_MODULES
    return work_type in NOTICE_MODULES and work_type in profile.get("modules", []) and bool(profile.get("scope_rules"))


def require_notice_delete(auth, session, work_type):
    from .module_access import require_module
    from .portal_service import PortalError
    require_module(auth, session, work_type)
    profile = auth.account_profile((session.get("user") or {}).get("open_id"))
    if not notice_delete_allowed(profile, work_type) and not profile["can_delete_business"]:
        raise PortalError("当前账号无权删除该业务记录。")


async def business_delete_denial(request, controller, runtime):
    """Shared-record deletion whitelist, also used by assistant's ASGI API calls."""
    import asyncio
    import re
    from fastapi.responses import JSONResponse

    path = request.url.path
    notice_work_type = ""
    # Local conversation/draft attachments are not deletion of business records.
    personal = (path.startswith("/api/assistant/") or path == "/api/workbench/draft"
                or path.startswith(("/api/notice-images/", "/api/notice-attachments/"))
                or re.fullmatch(r"/api/pre-shift-checks/files/[a-f0-9]{32}", path)
                or re.fullmatch(r"/api/work-handovers/files/[a-f0-9]{32}", path)
                or re.fullmatch(r"/api/change-confirmations/[^/]+/screenshot", path))
    deleting = request.method == "DELETE" and path.startswith("/api/") and not personal
    deleting = deleting or (request.method == "POST" and bool(re.search(r"/(?:delete|remove)(?:/|$)", path)))
    undo_match = re.fullmatch(r"/api/notice-undo/([^/]+)/apply", path)
    if request.method == "POST" and undo_match:
        undo = await asyncio.to_thread(runtime.state_store.get_notice_undo_action, undo_match[1]) or {}
        deleting = undo.get("action_type") == "start"
    if request.method == "POST" and path in {"/api/workbench-actions", "/api/maintenance-actions", "/api/ongoing-items/delete"}:
        try:
            payload = await controller._read_json_request(request, max_bytes=1024 * 1024)
        except (ValueError, TypeError):
            return JSONResponse({"ok": False, "error": "通告参数无效。"}, status_code=400)
        deleting = path == "/api/ongoing-items/delete" or isinstance(payload, dict) and str(payload.get("action", "")).startswith(("delete", "remove"))
        if deleting and isinstance(payload, dict):
            notice_work_type = str(payload.get("work_type") or "maintenance").strip()
    if not deleting:
        return None
    session = await asyncio.to_thread(controller._current_session, request)
    if not session:
        return JSONResponse({"ok": False, "error": "请先登录。"}, status_code=401)
    user = session.get("user") or {}
    profile = await asyncio.to_thread(runtime.auth_manager.account_profile, user.get("open_id"))
    if notice_delete_allowed(profile, notice_work_type):
        return None  # Notice handling grants deletion; the endpoint still checks the actual target and scope.
    if profile["can_delete_business"]:
        return None  # Original endpoint still validates the actual record, owner and building.
    await asyncio.to_thread(runtime.state_store.record_business_operation_audit,
        domain="permissions", action="delete", status="denied", actor_open_id=user.get("open_id", ""),
        metadata={"level": profile["level"], "path": path})
    return JSONResponse({"ok": False, "error": "仅超级管理员或本楼栋主管可删除业务记录。"}, status_code=403)

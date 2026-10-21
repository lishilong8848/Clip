"""Unified account view over the existing personnel and permission stores."""
from __future__ import annotations

import asyncio
import copy
import datetime as dt
import hashlib
import json
from urllib.parse import urlsplit

from fastapi import Request
from fastapi.responses import JSONResponse

from .personnel_password_login import PasswordLoginError
from .portal_auth import BUILDING_OPEN_ID_MAP, REQUIRED_ADMIN_USERS
from .account_policy import (
    CATEGORY_OPTIONS,
    MODULE_OPTIONS,
    LEVEL_OPTIONS,
    PolicyViolation,
    account_allowed,
    account_details_allowed,
    categorize_position,
    category_options_for,
    level_options_for,
    resolve_access_change,
)


def revision(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


class AccountManagement:
    def __init__(self, directory):
        self.directory = directory
        self.auth, self.store = directory.auth, directory.store

    def accounts(self, refresh=False, actor=None):
        warning = ""
        try:
            snapshot = self.directory.directory(force=refresh)
        except Exception:
            snapshot = self.directory.snapshot
            warning = "人员目录同步失败，保留上次完整名单；请稍后重新同步。"
        with self.auth.account_listing_read(snapshot) as raw_permissions:
            return self._accounts(snapshot, warning, actor, raw_permissions)

    def _accounts(self, snapshot, warning, actor, raw_permissions):
        with self.auth._lock:
            permissions = self.auth.get_permissions_payload()
        raw_permissions = raw_permissions.get('users', {})
        by_id = {item["open_id"]: item for item in permissions["users"]}
        duties = {oid: scope for scope, oid in BUILDING_OPEN_ID_MAP.items()}
        for oid, scope in duties.items():
            by_id.setdefault(oid, {"name": self.auth.scope_label(scope) + "值班账号", "role": "building",
                                  "scopes": [], "enabled": True, "locked": False})
        people = snapshot.get("people", {})
        # Only an explicit inactive flag on the current personnel cache means
        # "departed". A `removed_identities` tombstone merely records a row being
        # removed from the directory and is deliberately not treated as a
        # departure, so a matching permission record may still surface as legacy.
        active_ids = {oid for person in people.values() if not person.get("inactive") for oid in (person.get("login_ids") or [])}
        departed_ids = {oid for person in people.values() if person.get("inactive") for oid in (person.get("login_ids") or [])}
        # A departed person's known login_ids must not resurface as legacy
        # permission rows. If the same open_id also belongs to a CURRENT active
        # record, that active personnel remains displayed (the id is in
        # active_ids). Shared duty accounts are always retained regardless.
        suppressed_legacy = departed_ids - active_ids
        activity = {row["key"]: row["payload"] for row in self.store.list_documents("account_last_login")}
        items, seen = [], set()
        for person in people.values():
            oid = next(iter(person.get("login_ids") or []), "")
            if oid in duties or person.get("inactive"):
                continue
            cfg = by_id.get(oid, {})
            nature = str(person.get("account_nature") or "").strip()
            blocked = person.get("disabled_reason") or self.auth.directory_block_reason(oid)
            position = str(person.get("position") or "").strip()
            items.append({"id": person["id"], "record_id": person["id"], "open_id": oid,
                          "name": person["name"], "employee_no": person.get("employee_no", ""),
                          "kind": "vnet" if nature.upper() == "VNET" else "external" if nature else "unknown",
                          "account_nature": nature, "position": position,
                          "category": categorize_position(position),
                          "own_scopes": person.get("scopes", []),
                          "role": cfg.get("role", "building"), "scopes": cfg.get("scopes", []),
                          "directory_scopes": cfg.get("directory_scopes", []),
                          "enabled": cfg.get("enabled", True), "authorized": bool(cfg),
                          "locked": oid in REQUIRED_ADMIN_USERS, "blocked_reason": blocked,
                          "password_set": not person.get("needs_setup", True),
                          "can_set_password": bool(nature and nature.upper() != "VNET" and person.get("selectable") and oid not in REQUIRED_ADMIN_USERS),
                          "revision": revision(raw_permissions.get(oid)),
                          "last_login": activity.get(oid, {}).get("at")})
            seen.add(oid)
        for oid, cfg in by_id.items():
            if oid in seen and oid not in duties:
                continue
            if oid not in duties and oid in suppressed_legacy:
                continue
            items.append({"id": "account:" + oid, "record_id": "", "open_id": oid,
                          "name": cfg["name"], "employee_no": "", "kind": "duty" if oid in duties else "legacy",
                          "account_nature": "值班账号" if oid in duties else "历史账号",
                          "position": "", "category": "duty" if oid in duties else "other",
                          "own_scopes": [duties[oid]] if oid in duties else [],
                          "role": cfg["role"], "scopes": cfg["scopes"], "enabled": cfg["enabled"],
                          "directory_scopes": cfg.get("directory_scopes", []), "authorized": oid in raw_permissions,
                          "locked": cfg["locked"], "blocked_reason": self.auth.directory_block_reason(oid),
                          "can_set_password": False, "password_set": False,
                          "revision": revision(raw_permissions.get(oid)), "last_login": activity.get(oid, {}).get("at")})
        profile = self.auth.account_profile(actor.get("open_id")) if actor is not None else None
        for item in items:
            target = self.auth.account_profile(item["open_id"])
            item["level"] = target["level"]
            item["category"] = target["category"]
            item["home_scope"] = target["home_scope"]
            item["scopes"] = target["scope_rules"]
            item["modules"] = list(target["modules"])
            item["role"] = target.get("role") or ("admin" if target["level"] in {"L0", "L1"} else "building")
            item['is_self'] = bool(actor and item['open_id'] == actor.get('open_id'))
            item["can_edit"] = not profile or account_allowed(profile, item, "edit")
            item["can_access_signature"] = not profile or any(account_allowed(profile, item, action) for action in ('view', 'edit'))
            item["can_edit_access"] = bool(item["can_edit"]) and self._access_editable(profile, item)
            item["can_grant_l0"] = bool(profile and item["can_edit"] and profile['level'] in {'L0', 'L1', 'L2', 'L3'}
                                        and not (item.get('category') == 'duty' and item.get('home_scope') == 'H'))
            item["can_delete"] = bool(profile and account_allowed(profile, item, "delete") and item["open_id"] != actor.get("open_id"))
            item["can_set_password"] = item["can_set_password"] and item["can_edit"]
            from .account_impersonation import can_impersonate
            item['can_impersonate'] = bool(actor and can_impersonate(self.auth, actor.get('open_id'), item['open_id']))
        if profile:
            items = [item for item in items if account_details_allowed(profile, item)
                     or account_allowed(profile, item, "edit") or item['can_impersonate']]
            for item in items:
                if not any(account_allowed(profile, item, action) for action in ('view', 'edit')):
                    for key in ('last_login', 'password_set'):
                        item.pop(key, None)
            if profile["level"] == "L2":
                # This is an edit selector, not a general personnel directory or activity report.
                for item in items:
                    for key in ("last_login", "password_set", "account_nature", "employee_no"):
                        item.pop(key, None)
        options = permissions["scope_options"]
        if profile and profile["level"] == "L3":
            options = [option for option in options if option["value"] in profile['scope_rules']]
        level_options = level_options_for(profile["level"]) if profile else copy.deepcopy(LEVEL_OPTIONS)
        category_options = category_options_for(profile["level"]) if profile else copy.deepcopy(CATEGORY_OPTIONS)
        return {"items": sorted(items, key=lambda x: (x["kind"] != "duty", x["name"], x["id"])),
                "scope_options": options, "category_options": category_options,
                "module_options": copy.deepcopy(MODULE_OPTIONS), "level_options": level_options,
                "policy": profile or {},
                "loaded_at": snapshot.get("loaded_at"), "warning": warning}

    @staticmethod
    def _access_editable(profile, item):
        """Whether the acting profile may edit the item's access override.

        Fixed H duty and protected accounts are never editable; other L0 peers are.
        The enabled toggle is governed separately by ``can_edit``. L2 ordinary
        access editing remains disabled (its L0 promotion uses ``can_grant_l0``); L3
        may configure L4 accounts inside its current managed building scope.
        """
        if profile is None:
            return False
        target_level = str(item.get("level") or "")
        if item.get('locked') or item.get('is_self'):
            return False
        if item.get("category") == "duty" and list(item.get("own_scopes") or []) == ["H"]:
            return False  # fixed H duty
        act_level = str(profile.get("level") or "")
        if act_level in {"L0", "L1"}:
            return True
        if act_level == "L2":
            # L2 只能做基础编辑/启用，不能编辑/传播访问等级或模块等更高能力。
            return False
        if act_level == "L3":
            return target_level == "L4"
        return False

    def check(self, actor, item, action):
        profile = self.auth.account_profile(actor.get("open_id"))
        target = self.auth.account_profile(item.get("open_id"))
        current = {**item, "category": target["category"], "level": target["level"],
                   "is_self": item.get('open_id') == actor.get('open_id'),
                   "own_scopes": [target["home_scope"]] if target["home_scope"] else [],
                   "role": "admin" if target["level"] in {"L0", "L1"} else "building"}
        if not account_allowed(profile, current, action):
            self.store.record_business_operation_audit(domain="accounts", action=action, status="denied",
                actor_open_id=actor.get("open_id", ""), actor_name=actor.get("name", ""),
                target_record_id=item.get("id", ""), metadata={"level": profile["level"]})
            raise PasswordLoginError("当前账号无权管理此人员，未修改任何数据。", 403)
        return profile

    def update(self, payload, actor):
        if not isinstance(payload, dict) or set(payload) != {"changes"} or not isinstance(payload["changes"], list) or not 1 <= len(payload["changes"]) <= 300:
            raise PasswordLoginError("请选择1至300个账号进行修改。")
        listing = self.accounts()
        accounts = {item["id"]: item for item in listing["items"]}
        actor_id = str(actor.get("open_id") or "")
        valid_scopes = {item["value"] for item in listing["scope_options"]}
        allowed_keys = {"id", "revision", "role", "scopes", "enabled", "level", "category", "modules"}
        with self.auth._lock:
            # Re-fetch the actor profile inside the lock so policy grants never use a
            # cached profile captured before a concurrent permission change.
            actor_profile = self.auth.account_profile(actor_id)
            previous = self.auth._load_permissions_locked()
            following = copy.deepcopy(previous)
            users = following.setdefault("users", {})
            seen, changes = set(), []
            for change in payload["changes"]:
                if not isinstance(change, dict):
                    raise PasswordLoginError("账号修改参数无效。")
                keys = set(change)
                if not {"id", "revision"}.issubset(keys) or not keys.issubset(allowed_keys):
                    raise PasswordLoginError("账号修改参数无效。")
                item = accounts.get(str(change["id"]))
                if not item or not item["open_id"] or item["blocked_reason"]:
                    raise PasswordLoginError("人员不存在或身份待核对，未修改权限。", 409)
                oid = item["open_id"]
                if oid in seen:
                    raise PasswordLoginError("同一账号不能重复提交。")
                seen.add(oid)
                if item["locked"]:
                    raise PasswordLoginError("固定管理员权限不可修改。", 403)
                self.check(actor, item, "edit")
                if change["revision"] != revision(users.get(oid)):
                    raise PasswordLoginError("该账号权限已变化，请刷新后重新确认。", 409)
                if "enabled" not in change or type(change["enabled"]) is not bool:
                    raise PasswordLoginError("启用状态无效。")
                enabled = change["enabled"]
                if oid == actor_id and not enabled:
                    raise PasswordLoginError("不能停用自己的登录权限。", 403)
                if oid == actor_id and any(key in change for key in ("level", "category", "modules", "role", "scopes")):
                    raise PasswordLoginError("不能修改自己的管理权限，仅可调整启用状态。", 403)
                before = copy.deepcopy(users.get(oid) or {})
                new_format = any(key in change for key in ("level", "category", "modules"))
                if not new_format and "role" not in change and "scopes" not in change:
                    # Legacy payload only toggling `enabled`: preserve any existing
                    # explicit grant and never overwrite level/category/modules/scopes.
                    next_cfg = {**before, "enabled": enabled}
                elif not new_format:
                    # Legacy format: only role/scopes/enabled is accepted and the
                    # HR-derived role/scopes remain read-only (no raw escalation).
                    if change["role"] not in ("admin", "building") or not isinstance(change["scopes"], list):
                        raise PasswordLoginError("角色或启用状态无效。")
                    if any(not isinstance(scope, str) or scope not in valid_scopes for scope in change["scopes"]):
                        raise PasswordLoginError("包含无效楼栋。")
                    if change["role"] != item["role"] or set(change["scopes"]) != set(item["scopes"]):
                        raise PasswordLoginError("管理级别和楼栋按人员目录统一确定，不能单独更改或跨楼授权。", 403)
                    scopes = self.auth._admin_scopes() if change["role"] == "admin" else self.auth._scopes_from_raw(change["scopes"])
                    own = item["own_scopes"] if len(item["own_scopes"]) == 1 else []
                    next_cfg = {**before, "name": before.get("name") or item["name"], "role": change["role"],
                                "scopes": scopes, "enabled": enabled,
                                "directory_scopes": [scope for scope in before.get("directory_scopes", []) if scope in scopes],
                                "denied_scopes": [scope for scope in own if scope not in scopes]}
                else:
                    if not self._access_editable(actor_profile, item) and not (
                            actor_profile['level'] == 'L2' and change.get('level') == 'L0'):
                        raise PasswordLoginError("当前账号不能修改此人员的级别、岗位或访问权限。", 403)
                    if "role" in change and change["role"] not in ("admin", "building"):
                        raise PasswordLoginError("角色无效。")
                    if "level" in change and str(change["level"]).strip() not in {"L0", "L1", "L2", "L3", "L4"}:
                        raise PasswordLoginError("无效的管理级别。")
                    if "category" in change and not any(option["value"] == str(change["category"]).strip() for option in CATEGORY_OPTIONS):
                        raise PasswordLoginError("无效的业务岗位。")
                    if "modules" in change and not isinstance(change["modules"], list):
                        raise PasswordLoginError("模块参数无效。")
                    target_profile = self.auth.account_profile(oid)
                    try:
                        resolved = resolve_access_change(
                            actor_profile, target_profile,
                            level=change.get("level"), category=change.get("category"),
                            scopes=change.get("scopes"), modules=change.get("modules"))
                    except PolicyViolation as exc:
                        raise PasswordLoginError(str(exc), exc.status)
                    if "role" in change and change["role"] != resolved["role"]:
                        raise PasswordLoginError("角色与管理级别不一致，请按级别选择真实角色。", 403)
                    final_scopes = self.auth._scopes_from_raw(resolved["scopes"])
                    if any(scope not in valid_scopes and scope not in {"ALL", "CAMPUS"} for scope in final_scopes):
                        raise PasswordLoginError("包含无效楼栋。")
                    final_modules = [str(module) for module in resolved["modules"]]
                    # Migrate a legacy record to an explicit override record, seeded
                    # from the HR-derived profile (historical dirty scopes ignored).
                    effective = users.get(oid) or {}
                    if effective.get("access_version") != 1:
                        effective = {**effective,
                                     "level": target_profile["level"],
                                     "category": target_profile["category"],
                                     "modules": list(target_profile["modules"]),
                                     "scopes": list(target_profile["scope_rules"]),
                                     "role": target_profile.get("role") or ("admin" if target_profile["level"] in {"L0", "L1"} else "building"),
                                     "access_version": 1}
                    else:
                        effective = copy.deepcopy(effective)
                    own = item["own_scopes"] if len(item["own_scopes"]) == 1 else []
                    next_cfg = {**effective,
                                "name": effective.get("name") or item["name"],
                                "level": resolved["level"], "category": resolved["category"],
                                "scopes": final_scopes, "modules": final_modules,
                                "role": resolved["role"], "enabled": enabled, "access_version": 1,
                                "directory_scopes": [scope for scope in effective.get("directory_scopes", []) if scope in final_scopes],
                                "denied_scopes": [scope for scope in own if scope not in final_scopes],
                                "locked": False}
                users[oid] = next_cfg
                before_summary = {key: before.get(key) for key in ("role", "scopes", "enabled")}
                after_summary = {key: users[oid].get(key) for key in ("role", "scopes", "enabled")}
                if new_format:
                    for key in ("level", "category", "modules"):
                        before_summary[key] = before.get(key)
                        after_summary[key] = users[oid].get(key)
                changes.append({"name": item["name"], "open_id": oid,
                                "before": before_summary, "after": after_summary})
            if not self.store.get_document("account_management", "permissions_before_upgrade"):
                self.store.put_document("account_management", "permissions_before_upgrade", previous)
            audit = self.store.record_business_operation_audit(domain="accounts", action="permissions", status="started",
                actor_open_id=actor_id, actor_name=actor.get("name", ""), metadata={"changes": changes})
            following.update(updated_at=dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), updated_by=actor_id)
            self.auth._save_permissions_locked(following)
            self.store.record_business_operation_audit(audit_id=audit["audit_id"], status="success")
        return {"changed": len(changes)}

    def password(self, rid, payload, actor):
        if not isinstance(payload, dict) or set(payload) != {"new_password", "confirm_password"}:
            raise PasswordLoginError("修改密码参数无效。")
        new = payload["new_password"]
        if not isinstance(new, str) or new != payload["confirm_password"]:
            raise PasswordLoginError("两次输入的密码不一致。")
        item = next((item for item in self.accounts()["items"] if item["id"] == rid), None)
        if not item:
            raise PasswordLoginError("所选人员不在当前目录中。", 404)
        self.check(actor, item, "edit")
        directory = self.directory
        directory.limited("admin-password:" + str(actor.get("open_id")), 30)
        directory.directory()
        with directory.person_locks[int(hashlib.sha256(rid.encode()).hexdigest()[:4], 16) % 32]:
            cached = directory.snapshot.get("people", {}).get(rid)
            if not cached:
                raise PasswordLoginError("所选人员不在当前目录中。", 404)
            row = directory.record(rid)
            person = directory.person(row)
            oid = directory.login_identity(person)
            if person["login_ids"] != cached["login_ids"] or not person["selectable"]:
                raise PasswordLoginError("人员身份或状态已变化，请刷新后核对。", 409)
            if person["scopes"] != cached["scopes"] or person.get("position") != cached.get("position"):
                raise PasswordLoginError("人员所属楼栋或岗位已变化，请刷新后重新核对。", 409)
            nature = person["account_nature"].upper()
            if not nature or nature == "VNET" or oid in BUILDING_OPEN_ID_MAP.values() or oid in REQUIRED_ADMIN_USERS:
                raise PasswordLoginError("此账号使用飞书登录，不能在灯塔修改飞书密码。", 403)
            identity = directory.service()._mop_field_text(row["fields"], ["身份证号"]).upper()
            if identity and new.upper() in (identity, identity[-6:]):
                raise PasswordLoginError("新密码不能使用身份证号或初始密码。")
            audit = self.store.record_business_operation_audit(domain="accounts", action="password", status="started",
                actor_open_id=actor.get("open_id", ""), actor_name=actor.get("name", ""),
                target_record_id=rid, metadata={"name": person["name"]})
            try:
                with self.auth._lock:
                    self.check(actor, item, "edit")
                directory.save_password(rid, new, allow_disabled=True)
            except Exception:
                self.store.record_business_operation_audit(audit_id=audit["audit_id"], status="failed", error="密码保存未完成，未记录密码内容")
                raise
            self.store.record_business_operation_audit(audit_id=audit["audit_id"], status="success")
        return {"password_changed": True}

    def delete(self, rid, payload, actor):
        if not isinstance(payload, dict) or set(payload) != {"revision"}:
            raise PasswordLoginError("删除账号参数无效。")
        item = next((item for item in self.accounts()["items"] if item["id"] == rid), None)
        if not item or not item["open_id"]:
            raise PasswordLoginError("账号不存在。", 404)
        self.check(actor, item, "delete")
        if item["open_id"] == actor.get("open_id"):
            raise PasswordLoginError("不能删除自己的账号权限。", 403)
        with self.auth._lock:
            self.check(actor, item, "delete")
            previous = self.auth._load_permissions_locked()
            following = copy.deepcopy(previous)
            users = following.setdefault("users", {})
            oid = item["open_id"]
            if payload["revision"] != revision(users.get(oid)):
                raise PasswordLoginError("该账号权限已变化，请刷新后重新确认。", 409)
            # Revocation is a tombstone: HR sync must not recreate a deleted grant.
            users[oid] = {**users.get(oid, {}), "name": item["name"], "role": item["role"], "enabled": False,
                          "scopes": users.get(oid, {}).get("scopes", item["scopes"])}
            audit = self.store.record_business_operation_audit(domain="accounts", action="delete", status="started",
                actor_open_id=actor.get("open_id", ""), actor_name=actor.get("name", ""), target_record_id=rid,
                metadata={"name": item["name"], "target_open_id": oid})
            self.auth._save_permissions_locked(following)
            self.store.record_business_operation_audit(audit_id=audit["audit_id"], status="success")
        return {"deleted": True, "history_preserved": True}

    def audit(self, actor):
        profile = self.auth.account_profile(actor.get("open_id"))
        if not profile["can_view_accounts"]:
            raise PasswordLoginError("当前级别不能查看人员变更记录。", 403)
        rows = self.store.list_business_operation_audits(domain="accounts", limit=100)
        if profile["level"] == "L0":
            return rows
        visible = [item for item in self.accounts(actor=actor)["items"]
                   if account_allowed(profile, item, 'view') or account_allowed(profile, item, 'edit')]
        ids = {item["id"] for item in visible}
        oids = {item["open_id"] for item in visible}
        result = []
        for row in rows:
            data = copy.deepcopy(row)
            metadata = data.get("metadata") or {}
            if "changes" in metadata:
                metadata["changes"] = [change for change in metadata["changes"] if change.get("open_id") in oids]
                if not metadata["changes"]:
                    continue
            elif data.get("target_record_id") not in ids and metadata.get("target_open_id") not in oids:
                continue
            result.append(data)
        return result


def install_account_management(app, controller, runtime, get_directory):
    from .account_impersonation import install
    install(app, controller, runtime, get_directory)

    def response(data=None, error=None, status=200):
        return JSONResponse({"ok": not error, **({"error": error} if error else {"data": data})},
                            status_code=status, headers={"Cache-Control": "no-store"})

    async def dispatch(request, operation):
        session = await asyncio.to_thread(controller._current_session, request)
        if not session:
            return response(error="请先登录。", status=401)
        actor = session.get("user") or {}
        profile = await asyncio.to_thread(runtime.auth_manager.account_profile, actor.get("open_id"))
        if not profile["can_manage_accounts"]:
            return response(error="当前账号没有人员管理权限。", status=403)
        try:
            manager = AccountManagement(await asyncio.to_thread(get_directory))
            if request.method == "GET":
                if operation == "audit":
                    rows = await asyncio.to_thread(manager.audit, actor)
                    return response({"items": rows})
                return response(await asyncio.to_thread(manager.accounts, request.query_params.get("refresh") == "true", actor))
            origin = urlsplit(request.headers.get("origin") or request.headers.get("referer") or "")
            base = urlsplit(controller._request_base_url(request))
            if (origin.scheme, origin.netloc) != (base.scheme, base.netloc) or request.headers.get("sec-fetch-site") == "cross-site":
                raise PasswordLoginError("不允许跨来源修改账号。", 403)
            payload = await controller._read_json_request(request, max_bytes=128 * 1024 if operation == "permissions" else 8192)
            if operation == "permissions":
                data = await asyncio.to_thread(manager.update, payload, actor)
            elif operation == "delete":
                data = await asyncio.to_thread(manager.delete, request.path_params["record_id"], payload, actor)
            else:
                data = await asyncio.to_thread(manager.password, request.path_params["record_id"], payload, actor)
            controller._clear_read_cache(("auth_status",))
            return response(data)
        except PasswordLoginError as exc:
            return response(error=str(exc), status=exc.status)
        except (ValueError, TypeError):
            return response(error="账号参数无效。", status=400)
        except Exception:
            return response(error="账号操作未完成，请刷新状态后重试。", status=503)

    @app.get("/api/auth/accounts")
    async def accounts(request: Request):
        return await dispatch(request, "accounts")

    @app.get("/api/auth/accounts/audit")
    async def audit(request: Request):
        return await dispatch(request, "audit")

    @app.post("/api/auth/accounts/permissions")
    async def permissions(request: Request):
        return await dispatch(request, "permissions")

    @app.post("/api/auth/accounts/{record_id}/password")
    async def password(request: Request):
        return await dispatch(request, "password")

    @app.delete("/api/auth/accounts/{record_id}")
    async def delete_account(request: Request):
        return await dispatch(request, "delete")

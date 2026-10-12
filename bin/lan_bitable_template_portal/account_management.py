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


def revision(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


class AccountManagement:
    def __init__(self, directory):
        self.directory = directory
        self.auth, self.store = directory.auth, directory.store

    def accounts(self, refresh=False):
        warning = ""
        try:
            snapshot = self.directory.directory(force=refresh)
        except Exception:
            snapshot = self.directory.snapshot
            warning = "人员目录同步失败，保留上次完整名单；请稍后重新同步。"
        with self.auth._lock:
            permissions = self.auth.get_permissions_payload()
            raw_permissions = self.auth._load_permissions_locked().get("users", {})
        by_id = {item["open_id"]: item for item in permissions["users"]}
        duties = {oid: scope for scope, oid in BUILDING_OPEN_ID_MAP.items()}
        for oid, scope in duties.items():
            by_id.setdefault(oid, {"name": self.auth.scope_label(scope) + "值班账号", "role": "building",
                                  "scopes": [], "enabled": True, "locked": False})
        activity = {row["key"]: row["payload"] for row in self.store.list_documents("account_last_login")}
        items, seen = [], set()
        for person in snapshot.get("people", {}).values():
            oid = next(iter(person.get("login_ids") or []), "")
            if oid in duties:
                continue
            cfg = by_id.get(oid, {})
            nature = str(person.get("account_nature") or "").strip()
            blocked = person.get("disabled_reason") or self.auth.directory_block_reason(oid)
            items.append({"id": person["id"], "record_id": person["id"], "open_id": oid,
                          "name": person["name"], "employee_no": person.get("employee_no", ""),
                          "kind": "vnet" if nature.upper() == "VNET" else "external" if nature else "unknown",
                          "account_nature": nature, "own_scopes": person.get("scopes", []),
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
            items.append({"id": "account:" + oid, "record_id": "", "open_id": oid,
                          "name": cfg["name"], "employee_no": "", "kind": "duty" if oid in duties else "legacy",
                          "account_nature": "值班账号" if oid in duties else "历史账号",
                          "own_scopes": [duties[oid]] if oid in duties else [],
                          "role": cfg["role"], "scopes": cfg["scopes"], "enabled": cfg["enabled"],
                          "directory_scopes": cfg.get("directory_scopes", []), "authorized": oid in raw_permissions,
                          "locked": cfg["locked"], "blocked_reason": self.auth.directory_block_reason(oid),
                          "can_set_password": False, "password_set": False,
                          "revision": revision(raw_permissions.get(oid)), "last_login": activity.get(oid, {}).get("at")})
        return {"items": sorted(items, key=lambda x: (x["kind"] != "duty", x["name"], x["id"])),
                "scope_options": permissions["scope_options"], "loaded_at": snapshot.get("loaded_at"), "warning": warning}

    def update(self, payload, actor):
        if not isinstance(payload, dict) or set(payload) != {"changes"} or not isinstance(payload["changes"], list) or not 1 <= len(payload["changes"]) <= 300:
            raise PasswordLoginError("请选择1至300个账号进行修改。")
        listing = self.accounts()
        accounts = {item["id"]: item for item in listing["items"]}
        actor_id = str(actor.get("open_id") or "")
        valid_scopes = {item["value"] for item in listing["scope_options"]}
        with self.auth._lock:
            previous = self.auth._load_permissions_locked()
            following = copy.deepcopy(previous)
            users = following.setdefault("users", {})
            seen, changes = set(), []
            for change in payload["changes"]:
                if not isinstance(change, dict) or set(change) != {"id", "revision", "role", "scopes", "enabled"}:
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
                if change["revision"] != revision(users.get(oid)):
                    raise PasswordLoginError("该账号权限已变化，请刷新后重新确认。", 409)
                if change["role"] not in ("admin", "building") or type(change["enabled"]) is not bool or not isinstance(change["scopes"], list):
                    raise PasswordLoginError("角色或启用状态无效。")
                if any(not isinstance(scope, str) or scope not in valid_scopes for scope in change["scopes"]):
                    raise PasswordLoginError("包含无效楼栋。")
                if oid == actor_id and (not change["enabled"] or change["role"] != "admin"):
                    raise PasswordLoginError("不能停用自己或取消自己的管理员权限。", 403)
                before = copy.deepcopy(users.get(oid) or {})
                scopes = self.auth._admin_scopes() if change["role"] == "admin" else self.auth._scopes_from_raw(change["scopes"])
                own = item["own_scopes"] if len(item["own_scopes"]) == 1 else []
                users[oid] = {**before, "name": before.get("name") or item["name"], "role": change["role"],
                              "scopes": scopes, "enabled": change["enabled"],
                              "directory_scopes": [scope for scope in before.get("directory_scopes", []) if scope in scopes],
                              "denied_scopes": [scope for scope in own if scope not in scopes]}
                changes.append({"name": item["name"], "open_id": oid,
                                "before": {key: before.get(key) for key in ("role", "scopes", "enabled")},
                                "after": {key: users[oid][key] for key in ("role", "scopes", "enabled")}})
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
                directory.save_password(rid, new, allow_disabled=True)
            except Exception:
                self.store.record_business_operation_audit(audit_id=audit["audit_id"], status="failed", error="密码保存未完成，未记录密码内容")
                raise
            self.store.record_business_operation_audit(audit_id=audit["audit_id"], status="success")
        return {"password_changed": True}


def install_account_management(app, controller, runtime, get_directory):
    def response(data=None, error=None, status=200):
        return JSONResponse({"ok": not error, **({"error": error} if error else {"data": data})},
                            status_code=status, headers={"Cache-Control": "no-store"})

    async def dispatch(request, operation):
        denied, session = await asyncio.to_thread(controller._require_admin_response, request)
        if denied is not None:
            return denied
        try:
            actor = session.get("user") or {}
            manager = AccountManagement(await asyncio.to_thread(get_directory))
            if request.method == "GET":
                if operation == "audit":
                    rows = await asyncio.to_thread(runtime.state_store.list_business_operation_audits, domain="accounts", limit=100)
                    return response({"items": rows})
                return response(await asyncio.to_thread(manager.accounts, request.query_params.get("refresh") == "true"))
            origin = urlsplit(request.headers.get("origin") or request.headers.get("referer") or "")
            base = urlsplit(controller._request_base_url(request))
            if (origin.scheme, origin.netloc) != (base.scheme, base.netloc) or request.headers.get("sec-fetch-site") == "cross-site":
                raise PasswordLoginError("不允许跨来源修改账号。", 403)
            payload = await controller._read_json_request(request, max_bytes=128 * 1024 if operation == "permissions" else 8192)
            if operation == "permissions":
                data = await asyncio.to_thread(manager.update, payload, actor)
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

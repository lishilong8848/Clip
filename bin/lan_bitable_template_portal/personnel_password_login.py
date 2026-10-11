"""Personnel-table password authentication using existing Feishu identities."""
from __future__ import annotations

import asyncio
import base64
from collections import Counter, deque
import copy
import hashlib
import hmac
import json
import logging
import re
import secrets
import threading
import time
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from fastapi import Request
from fastapi.responses import JSONResponse

from .portal_service import PERMISSION_DIRECTORY_APP_TOKEN as APP_TOKEN, PERMISSION_DIRECTORY_TABLE_ID as TABLE_ID, PortalError

PASSWORD_FIELD = "密码"
IDENTITY_FIELD = "身份证号"
DIRECTORY_NS = "personnel_password_login"
DIRECTORY_KEY = TABLE_ID
PRINCIPAL_PREFIX = "personnel_"  # Learning lookup only; never a login OpenID.
ROUNDS = 600_000
IDENTITY_PATTERN = re.compile(r"(?:[0-9]{15}|[0-9]{17}[0-9Xx])")
FIELDS = ["姓名", "员工姓名", "员工工号", "机楼/专业", "离职/异动情况", "飞书 open_id", "账号性质", PASSWORD_FIELD]
KDF_SLOTS = threading.BoundedSemaphore(2)


class PasswordLoginError(PortalError):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


def _derive(password, salt):
    if not KDF_SLOTS.acquire(blocking=False):
        raise PasswordLoginError("正在处理其他登录，请稍后重试。", 429)
    try:
        return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, ROUNDS)
    finally:
        KDF_SLOTS.release()


def hash_password(password):
    salt = secrets.token_bytes(16)
    encoded = lambda value: base64.urlsafe_b64encode(value).decode("ascii")
    return f"pbkdf2_sha256${ROUNDS}${encoded(salt)}${encoded(_derive(password, salt))}"


def verify_password(password, stored):
    try:
        algorithm, rounds, salt, expected = stored.split("$")
        if algorithm != "pbkdf2_sha256" or rounds != str(ROUNDS):
            return False
        salt, expected = base64.b64decode(salt, altchars=b"-_", validate=True), base64.b64decode(expected, altchars=b"-_", validate=True)
        if len(salt) != 16 or len(expected) != 32:
            return False
    except (ValueError, TypeError):
        return False
    return hmac.compare_digest(_derive(password, salt), expected)


class PersonnelPasswordLogin:
    def __init__(self, service, store, auth):
        self.service, self.store, self.auth = service, store, auth
        self.directory_lock = threading.RLock()
        self.person_locks = [threading.Lock() for _ in range(32)]
        self.limit_lock = threading.Lock()
        self.attempts = {}
        self.snapshot = store.get_document(DIRECTORY_NS, DIRECTORY_KEY) or {}

    def limited(self, key, limit):
        now = time.monotonic()
        with self.limit_lock:
            self.attempts = {k: q for k, q in self.attempts.items() if q and q[-1] > now - 600}
            if len(self.attempts) >= 2048 and key not in self.attempts:
                raise PasswordLoginError("登录请求较多，请稍后重试。", 429)
            queue = self.attempts.setdefault(key, deque())
            while queue and queue[0] <= now - 600:
                queue.popleft()
            if len(queue) >= limit:
                raise PasswordLoginError("登录尝试过于频繁，请10分钟后重试。", 429)
            queue.append(now)

    def person(self, row):
        service = self.service()
        fields = row.get("fields") or {}
        rid = str(row.get("record_id") or "")
        if not re.fullmatch(r"rec[A-Za-z0-9]+", rid):
            raise PasswordLoginError("人员目录返回了无效记录，未使用不完整名单。", 503)
        name = service._mop_field_text(fields, ["姓名"]) or service._signature_user_info(fields.get("员工姓名")).get("name", "")
        scopes = service._building_codes_from_value(fields.get("机楼/专业"))
        inactive = service._signature_person_inactive(fields.get("离职/异动情况"))
        reason = "该人员已离职或异动" if inactive else "人员姓名未填写" if not name else ""
        stored = service._mop_field_text(fields, [PASSWORD_FIELD])
        initial = not stored
        user = service._signature_user_info(fields.get("员工姓名"))
        open_id = user.get("open_id", "") or service._mop_field_text(fields, ["飞书 open_id"])
        if not reason and not re.fullmatch(r"ou_[A-Za-z0-9]+", open_id):
            reason = "缺少飞书OpenID，请管理员核对人员记录"
        return {"id": rid, "name": name, "employee_no": service._mop_field_text(fields, ["员工工号"]),
                "building": service._building_label_from_codes(scopes), "scopes": scopes,
                "inactive": inactive, "selectable": not reason, "disabled_reason": reason,
                "needs_setup": initial,
                "account_nature": service._mop_field_text(fields, ["账号性质"]),
                "login_ids": [open_id] if re.fullmatch(r"ou_[A-Za-z0-9]+", open_id) else [],
                "password_revision": "" if initial or not stored else hashlib.sha256(stored.encode("utf-8")).hexdigest()}

    def people(self):
        with self.directory_lock:
            if self.snapshot.get("schema") != 2 or time.time() - float(self.snapshot.get("loaded_at") or 0) >= 300:
                service = self.service()
                records, token, seen = {}, "", set()
                for _ in range(20):
                    params = {"page_size": 500}
                    if token:
                        params["page_token"] = token
                    payload = service._request_payload("POST",
                        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/search",
                        context="姓名密码登录人员目录", headers=service._auth_headers(), params=params,
                        json_payload={"field_names": FIELDS})
                    data = payload.get("data") or {}
                    if payload.get("code") != 0 or not isinstance(data.get("items"), list) or not isinstance(data.get("has_more"), bool):
                        raise PasswordLoginError("人员名单读取失败，请稍后重试。", 503)
                    for row in data["items"]:
                        item = self.person(row)
                        if item["id"] in records:
                            raise PasswordLoginError("人员名单分页重复，请稍后重试。", 503)
                        records[item["id"]] = item
                    if not data.get("has_more"):
                        break
                    token = str(data.get("page_token") or "")
                    if not token or token in seen:
                        raise PasswordLoginError("人员名单读取不完整，请稍后重试。", 503)
                    seen.add(token)
                else:
                    raise PasswordLoginError("人员名单超出读取范围，请联系管理员。", 503)
                identities = Counter(oid for item in records.values() if not item["inactive"] for oid in item["login_ids"])
                for item in records.values():
                    if any(identities[oid] > 1 for oid in item["login_ids"]):
                        item.update(selectable=False, disabled_reason="多人使用同一OpenID，请管理员核对人员记录")
                snapshot = {"people": records, "loaded_at": time.time(), "schema": 2}
                self.store.put_document(DIRECTORY_NS, DIRECTORY_KEY, snapshot)
                self.snapshot = snapshot
            keys = ("id", "name", "employee_no", "building", "selectable", "disabled_reason", "needs_setup", "account_nature")
            items = [{key: item[key] for key in keys} for item in self.snapshot.get("people", {}).values() if not item["inactive"]]
            return {"items": sorted(items, key=lambda item: (item["name"], item["building"], item["id"])), "loaded_at": self.snapshot.get("loaded_at")}

    def record(self, rid):
        payload = self.service()._request_json("records/" + rid, app_token=APP_TOKEN, table_id=TABLE_ID)
        row = (payload.get("data") or {}).get("record")
        if not isinstance(row, dict) or row.get("record_id") != rid:
            raise PasswordLoginError("无法核验所选人员，请重新读取名单。", 503)
        return row

    def login_identity(self, person):
        identities = person.get("login_ids") or []
        if len(identities) != 1:
            raise PasswordLoginError("缺少可用飞书OpenID，请管理员核对人员记录。", 403)
        identity = identities[0]
        if any(item["id"] != person["id"] and not item["inactive"] and identity in item["login_ids"]
               for item in self.snapshot.get("people", {}).values()):
            raise PasswordLoginError("多人使用同一OpenID，请管理员核对人员记录。", 403)
        return identity

    def remember(self, item):
        with self.directory_lock:
            snapshot = copy.deepcopy(self.snapshot)
            snapshot.setdefault("people", {})[item["id"]] = item
            self.store.put_document(DIRECTORY_NS, DIRECTORY_KEY, snapshot)
            self.snapshot = snapshot

    def identity_number(self, row):
        value = self.service()._mop_field_text(row.get("fields") or {}, [IDENTITY_FIELD]).upper()
        if not IDENTITY_PATTERN.fullmatch(value):
            raise PasswordLoginError("该人员身份证号缺失或格式无效，请联系管理员核对人员表。", 409)
        return value

    def save_password(self, rid, new_password):
        if not isinstance(new_password, str) or not 8 <= len(new_password.strip()) <= 128 or len(new_password) > 128:
            raise PasswordLoginError("新密码须为8至128个字符，不能继续使用初始密码。")
        replacement = hash_password(new_password)
        from .portal_service import external_real_write_guard
        if not external_real_write_guard()["real_write_allowed"]:
            raise PasswordLoginError("当前运行模式不允许保存人员密码。", 403)
        try:
            self.service()._patch_record_fields_exact(app_token=APP_TOKEN, table_id=TABLE_ID,
                record_id=rid, fields={PASSWORD_FIELD: replacement})
        except Exception:
            # A lost response can still mean the field was written; verify before accepting it.
            pass
        row = self.record(rid)
        if self.service()._mop_field_text(row["fields"], [PASSWORD_FIELD]) != replacement:
            raise PasswordLoginError("新密码保存未确认，请用新密码重新登录；若不成功，请稍后重试。", 503)
        person = self.person(row)
        if not person["selectable"] or self.auth._open_id_explicitly_disabled(self.login_identity(person)):
            raise PasswordLoginError("密码已保存，但人员状态或身份已变化，请联系管理员核对。", 403)
        self.remember(person)
        self.auth.invalidate_personnel_sessions(rid)
        return person

    def login(self, payload, client):
        if not isinstance(payload, dict) or set(payload) - {"person_id", "password", "new_password", "next"}:
            raise PasswordLoginError("登录参数无效。")
        rid, password = payload.get("person_id"), payload.get("password")
        if not isinstance(rid, str) or not re.fullmatch(r"rec[A-Za-z0-9]+", rid):
            raise PasswordLoginError("请选择姓名。")
        if not isinstance(password, str) or not 1 <= len(password) <= 128:
            raise PasswordLoginError("请输入密码，最多128个字符。")
        self.limited("ip:" + client, 40)
        self.limited("person:" + rid, 10)
        self.people()
        with self.person_locks[int(hashlib.sha256(rid.encode()).hexdigest()[:4], 16) % 32]:
            if rid not in self.snapshot.get("people", {}):
                raise PasswordLoginError("所选人员不在当前名单中，请重新选择。", 403)
            row = self.record(rid)
            person = self.person(row)
            if not person["selectable"] or self.auth._open_id_explicitly_disabled(self.login_identity(person)):
                raise PasswordLoginError(person["disabled_reason"] or "当前账号已停用。", 403)
            self.remember(person)
            if person["account_nature"].upper() == "VNET":
                raise PasswordLoginError("VNET人员请使用飞书登录授权。", 403)
            stored = self.service()._mop_field_text(row["fields"], [PASSWORD_FIELD])
            if person["needs_setup"]:
                identity = self.identity_number(row)
                if not hmac.compare_digest(password.upper().encode(), identity[-6:].encode()):
                    raise PasswordLoginError("姓名或密码不正确。", 401)
                new_password = payload.get("new_password")
                if new_password is None:
                    return {"requires_password_change": True}, None
                if isinstance(new_password, str) and new_password.upper() == identity:
                    raise PasswordLoginError("新密码不能使用身份证号或初始密码。")
                person = self.save_password(rid, new_password)
            elif not verify_password(password, stored):
                raise PasswordLoginError("姓名或密码不正确。", 401)
            if not person["selectable"]:
                raise PasswordLoginError("人员状态或所属楼栋已变化，请重新登录。", 403)
            session_id = self.auth.create_personnel_session(person)
            with self.limit_lock:
                self.attempts.pop("person:" + rid, None)
            target = self.auth._normalize_next_path(str(payload.get("next") or "/"))
            parts = urlsplit(target)
            login_query = dict(parse_qsl(parts.query))
            if login_query.get("login") == "password" and login_query.get("next"):
                parts = urlsplit(self.auth._normalize_next_path(login_query["next"]))
            if parts.path.startswith("/api/"):
                parts = urlsplit("/")
            query = dict(parse_qsl(parts.query))
            query.pop("admin", None)
            query.pop("login", None)
            session = self.auth.get_session(session_id)
            if not self.auth.session_scopes(session):
                return {"redirect_url": "/"}, session_id
            scope = person["scopes"][0] if len(person["scopes"]) == 1 else self.auth.default_scope(session)
            query["scope"] = scope if self.auth.scope_allowed(session, scope) else self.auth.default_scope(session)
            return {"redirect_url": urlunsplit(("", "", parts.path or "/", urlencode(query), ""))}, session_id

    def reset(self, payload, client):
        if not isinstance(payload, dict) or set(payload) - {"person_id", "identity_number", "new_password"}:
            raise PasswordLoginError("修改密码参数无效。")
        rid, identity = payload.get("person_id"), payload.get("identity_number")
        if not isinstance(rid, str) or not re.fullmatch(r"rec[A-Za-z0-9]+", rid):
            raise PasswordLoginError("请选择姓名。")
        if not isinstance(identity, str) or not IDENTITY_PATTERN.fullmatch(identity.strip()):
            raise PasswordLoginError("请填写完整的身份证号。")
        self.limited("ip:" + client, 40)
        self.limited("person:" + rid, 10)
        self.people()
        with self.person_locks[int(hashlib.sha256(rid.encode()).hexdigest()[:4], 16) % 32]:
            if rid not in self.snapshot.get("people", {}):
                raise PasswordLoginError("所选人员不在当前名单中，请重新选择。", 403)
            row = self.record(rid)
            person = self.person(row)
            if person["account_nature"] != "外部账号":
                raise PasswordLoginError("忘记密码仅适用于外部人员，其他人员请使用飞书登录。", 403)
            if not person["selectable"] or self.auth._open_id_explicitly_disabled(self.login_identity(person)):
                raise PasswordLoginError(person["disabled_reason"] or "当前账号已停用。", 403)
            if not hmac.compare_digest(identity.strip().upper().encode(), self.identity_number(row).encode()):
                raise PasswordLoginError("姓名与身份证号不匹配。", 401)
            if "new_password" not in payload:
                return {"requires_password_change": True}, None
            if isinstance(payload["new_password"], str) and payload["new_password"].upper() in (identity.strip().upper(), identity.strip().upper()[-6:]):
                raise PasswordLoginError("新密码不能使用身份证号或初始密码。")
            self.save_password(rid, payload["new_password"])
            return {"password_changed": True}, None

    def change(self, payload, client, session):
        if not isinstance(payload, dict) or set(payload) - {"current_password", "new_password"}:
            raise PasswordLoginError("修改密码参数无效。")
        if not session or session.get("source") != "personnel_password":
            raise PasswordLoginError("请先使用姓名密码登录，或通过忘记密码核验身份。", 403)
        rid = (session.get("user") or {}).get("personnel_record_id", "")
        password = payload.get("current_password")
        if not isinstance(password, str) or not 1 <= len(password) <= 128:
            raise PasswordLoginError("请输入原密码。")
        self.limited("ip:" + client, 40)
        self.limited("person:" + rid, 10)
        self.people()
        with self.person_locks[int(hashlib.sha256(rid.encode()).hexdigest()[:4], 16) % 32]:
            if rid not in self.snapshot.get("people", {}):
                raise PasswordLoginError("当前人员已不在登录名单中。", 403)
            row = self.record(rid)
            person = self.person(row)
            if not person["selectable"] or self.login_identity(person) != (session.get("user") or {}).get("open_id") or self.auth._open_id_explicitly_disabled(self.login_identity(person)):
                raise PasswordLoginError("当前人员已停用或所属楼栋需核对。", 403)
            stored = self.service()._mop_field_text(row["fields"], [PASSWORD_FIELD])
            if not verify_password(password, stored):
                raise PasswordLoginError("原密码不正确。", 401)
            new_password = payload.get("new_password")
            identity = self.service()._mop_field_text(row["fields"], [IDENTITY_FIELD]).upper()
            if new_password == password or (isinstance(new_password, str) and identity and new_password.upper() in (identity, identity[-6:])):
                raise PasswordLoginError("新密码不能与原密码相同，也不能使用身份证号或初始密码。")
            self.save_password(rid, new_password)
            return {"password_changed": True}, None


def install_personnel_password_login(app, controller, runtime):
    service, creation_lock = None, threading.Lock()
    refresher = None

    def get_service():
        nonlocal service
        with creation_lock:
            if service is None:
                service = PersonnelPasswordLogin(lambda: runtime.service, runtime.state_store, runtime.auth_manager)
        return service

    def result(data=None, error=None, status=200, cookie=None):
        headers = {"Cache-Control": "no-store"}
        if cookie:
            headers["Set-Cookie"] = cookie
        return JSONResponse({"ok": error is None, **({"error": error} if error else {"data": data})}, status_code=status, headers=headers)

    async def refresh_directory():
        while True:
            await asyncio.sleep(300)
            if service is not None:
                try:
                    await asyncio.to_thread(service.people)
                except Exception as exc:
                    logging.warning("Personnel login directory refresh deferred: %s", type(exc).__name__)

    async def startup():
        nonlocal refresher
        # No startup cloud read: refresh begins only after this login method is used.
        refresher = asyncio.create_task(refresh_directory())

    async def shutdown():
        if refresher:
            refresher.cancel()
            await asyncio.gather(refresher, return_exceptions=True)

    app.add_event_handler("startup", startup)
    app.add_event_handler("shutdown", shutdown)

    @app.get("/api/auth/password/people")
    async def people(request: Request):
        try:
            client = request.client.host if request.client else "unknown"
            current = await asyncio.to_thread(get_service)
            current.limited("list:" + client, 90)
            return result(await asyncio.to_thread(current.people))
        except PasswordLoginError as exc:
            return result(error=str(exc), status=exc.status)
        except Exception:
            return result(error="人员名单读取失败，请稍后重试。", status=503)

    @app.post("/api/auth/password/change")
    @app.post("/api/auth/password/reset")
    @app.post("/api/auth/password/login")
    async def login(request: Request):
        try:
            actual = urlsplit(request.headers.get("origin") or request.headers.get("referer") or "")
            expected = urlsplit(controller._request_base_url(request))
            if (actual.scheme, actual.netloc) != (expected.scheme, expected.netloc) or request.headers.get("sec-fetch-site") == "cross-site":
                raise PasswordLoginError("不允许跨来源登录。", 403)
            body = bytearray()
            async for chunk in request.stream():
                body.extend(chunk)
                if len(body) > 8192:
                    raise PasswordLoginError("登录内容过大。", 413)
            try:
                payload = json.loads(body)
            except (ValueError, UnicodeError, RecursionError):
                raise PasswordLoginError("登录参数无效。") from None
            current = await asyncio.to_thread(get_service)
            client = request.client.host if request.client else "unknown"
            if request.url.path.endswith("/change"):
                session = await asyncio.to_thread(controller._current_session, request)
                if not session:
                    return JSONResponse({"ok": False, "auth_required": True, "error": "登录已过期，请重新登录。"}, status_code=401)
                data, session_id = await asyncio.to_thread(current.change, payload, client, session)
            else:
                operation = current.reset if request.url.path.endswith("/reset") else current.login
                data, session_id = await asyncio.to_thread(operation, payload, client)
            cookie = runtime.auth_manager.cookie_header(session_id) if session_id else None
            if data.get("password_changed"):
                from .portal_auth import AUTH_COOKIE_NAME
                await asyncio.to_thread(runtime.auth_manager.clear_session, request.cookies.get(AUTH_COOKIE_NAME, ""))
                cookie = runtime.auth_manager.clear_cookie_header()
            if cookie and expected.scheme == "https":
                cookie += "; Secure"
            return result(data, cookie=cookie)
        except PasswordLoginError as exc:
            return result(error=str(exc), status=exc.status)
        except Exception:
            return result(error="登录核验未完成，请稍后重试；若刚设置新密码，请使用新密码登录。", status=503)

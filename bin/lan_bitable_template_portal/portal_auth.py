# -*- coding: utf-8 -*-
from __future__ import annotations

import copy
import datetime as dt
import hashlib
import hmac
import json
import secrets
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from upload_event_module.config import config
from upload_event_module.services.feishu_token_manager import (
    FeishuTokenError,
    token_manager,
)
from upload_event_module.utils import get_data_file_path

from .portal_service import (
    BUILDING_OPEN_ID_MAP,
    BUILDING_SCOPE_CODES,
    LI_SHILONG_OPEN_ID,
    MA_JINYU_OPEN_ID,
    SCOPE_OPTIONS,
    MaintenancePortalService,
    PortalError,
)
from .state_store import DEFAULT_STATE_DB_NAME, LanPortalStateStore


AUTH_COOKIE_NAME = "lan_portal_session"
AUTH_SESSION_TTL_SECONDS = 12 * 60 * 60
AUTH_STATE_TTL_SECONDS = 10 * 60
AUTH_MAX_PENDING_STATES = 200
AUTH_MAX_SESSIONS = 500
PERMISSION_REQUEST_TTL_SECONDS = 15 * 60
PERMISSION_REQUEST_MAX_ATTEMPTS = 5
FEISHU_AUTH_INDEX_URL = "https://open.feishu.cn/open-apis/authen/v1/index"
REQUIRED_ADMIN_USERS = {
    LI_SHILONG_OPEN_ID: "李世龙",
    MA_JINYU_OPEN_ID: "马进宇",
}


class PortalAuthManager:
    """Feishu OAuth session and local scope authorization for the LAN portal."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._sessions: dict[str, dict[str, Any]] = {}
        self._states: dict[str, dict[str, Any]] = {}
        self._permission_path = Path(get_data_file_path("lan_portal_auth.json"))
        self._permission_path.parent.mkdir(parents=True, exist_ok=True)
        self._state_store = LanPortalStateStore(
            self._permission_path.parent / DEFAULT_STATE_DB_NAME
        )

    @staticmethod
    def configured() -> bool:
        return bool(str(config.app_id or "").strip() and str(config.app_secret or "").strip())

    @staticmethod
    def cookie_header(session_id: str) -> str:
        return (
            f"{AUTH_COOKIE_NAME}={session_id}; Path=/; "
            f"Max-Age={AUTH_SESSION_TTL_SECONDS}; HttpOnly; SameSite=Lax"
        )

    @staticmethod
    def clear_cookie_header() -> str:
        return f"{AUTH_COOKIE_NAME}=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax"

    @staticmethod
    def _normalize_next_path(next_path: str) -> str:
        text = str(next_path or "/").strip()
        if not text.startswith("/") or text.startswith("//"):
            return "/"
        parts = urlsplit(text)
        path = parts.path or "/"
        if path.startswith("/api/"):
            return "/"
        query = urlencode(
            [
                (key, value)
                for key, value in parse_qsl(parts.query, keep_blank_values=True)
                if key not in {"code", "state"}
            ]
        )
        return urlunsplit(("", "", path, query, parts.fragment)) or "/"

    @staticmethod
    def normalize_scope(scope: Any) -> str:
        return MaintenancePortalService._normalize_scope(scope)

    @staticmethod
    def scope_label(scope: Any) -> str:
        return MaintenancePortalService._scope_label(scope)

    def _admin_scopes(self) -> list[str]:
        return ["ALL", "CAMPUS", *BUILDING_SCOPE_CODES]

    def _default_permissions(self) -> dict[str, Any]:
        users: dict[str, dict[str, Any]] = {}
        for code, open_id in BUILDING_OPEN_ID_MAP.items():
            users[open_id] = {
                "name": self.scope_label(code),
                "role": "building",
                "scopes": ["CAMPUS", *BUILDING_SCOPE_CODES] if code == "H" else [code],
            }
        users[LI_SHILONG_OPEN_ID] = {
            "name": "李世龙",
            "role": "admin",
            "scopes": self._admin_scopes(),
        }
        users[MA_JINYU_OPEN_ID] = {
            "name": "马进宇",
            "role": "admin",
            "scopes": self._admin_scopes(),
        }
        return {
            "version": 1,
            "updated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "default_scopes": [],
            "users": users,
        }

    def _ensure_required_admins_locked(self, payload: dict[str, Any]) -> dict[str, Any]:
        users = payload.get("users")
        if not isinstance(users, dict):
            users = {}
            payload["users"] = users
        changed = False
        for open_id, name in REQUIRED_ADMIN_USERS.items():
            existing = users.get(open_id) if isinstance(users.get(open_id), dict) else {}
            desired = {
                **existing,
                "name": str(existing.get("name") or name),
                "role": "admin",
                "scopes": self._admin_scopes(),
                "enabled": True,
                "locked": True,
            }
            if users.get(open_id) != desired:
                users[open_id] = desired
                changed = True
        if changed:
            payload["updated_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._save_permissions_locked(payload)
        return payload

    def _load_permissions_locked(self) -> dict[str, Any]:
        stored = self._state_store.get_auth_permissions()
        if isinstance(stored, dict):
            payload = stored
        elif not self._permission_path.exists():
            payload = self._default_permissions()
            self._save_permissions_locked(payload)
            return payload
        else:
            try:
                with self._permission_path.open("r", encoding="utf-8") as fp:
                    payload = json.load(fp)
                if not isinstance(payload, dict):
                    raise ValueError("payload is not object")
                self._save_permissions_locked(payload)
            except Exception:
                payload = self._default_permissions()
                self._save_permissions_locked(payload)
                return payload
        users = payload.get("users")
        if not isinstance(users, dict):
            payload["users"] = {}
        default_scopes = payload.get("default_scopes")
        if not isinstance(default_scopes, list):
            payload["default_scopes"] = []
        return self._ensure_required_admins_locked(payload)

    def _save_permissions_locked(self, payload: dict[str, Any]) -> None:
        self._state_store.put_auth_permissions(payload)

    def _backup_corrupt_permissions_locked(self) -> None:
        if not self._permission_path.exists():
            return
        stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        backup_path = self._permission_path.with_name(
            f"{self._permission_path.stem}.bad.{stamp}{self._permission_path.suffix}"
        )
        try:
            self._permission_path.replace(backup_path)
        except Exception:
            return

    @staticmethod
    def _trim_by_expiry_locked(mapping: dict[str, dict[str, Any]], max_size: int) -> None:
        if len(mapping) <= max_size:
            return
        ordered = sorted(
            mapping.items(),
            key=lambda item: float((item[1] or {}).get("expires_at") or 0),
        )
        for key, _payload in ordered[: len(mapping) - max_size]:
            mapping.pop(key, None)

    def _cleanup_expired_locked(self, now: float | None = None) -> None:
        now = time.time() if now is None else float(now)
        for session_id, session in list(self._sessions.items()):
            if now > float((session or {}).get("expires_at") or 0):
                self._sessions.pop(session_id, None)
        for state, payload in list(self._states.items()):
            if now > float((payload or {}).get("expires_at") or 0):
                self._states.pop(state, None)
        self._trim_by_expiry_locked(self._sessions, AUTH_MAX_SESSIONS)
        self._trim_by_expiry_locked(self._states, AUTH_MAX_PENDING_STATES)

    def _scopes_from_raw(self, raw_scopes: Any) -> list[str]:
        scopes = raw_scopes if isinstance(raw_scopes, list) else []
        normalized: list[str] = []
        for raw in scopes:
            scope = self.normalize_scope(raw)
            if scope == "ALL":
                normalized = self._admin_scopes()
                break
            if scope not in normalized:
                normalized.append(scope)
        valid = {"ALL", "CAMPUS", *BUILDING_SCOPE_CODES}
        return [scope for scope in normalized if scope in valid]

    def _permission_request_scopes_from_raw(self, raw_scopes: Any) -> list[str]:
        scopes = raw_scopes if isinstance(raw_scopes, list) else []
        normalized: list[str] = []
        valid = {"CAMPUS", *BUILDING_SCOPE_CODES}
        for raw in scopes:
            scope = self.normalize_scope(raw)
            if scope == "ALL":
                continue
            if scope in valid and scope not in normalized:
                normalized.append(scope)
        return normalized

    @staticmethod
    def _has_full_scope_access(scopes: list[str]) -> bool:
        required = {"CAMPUS", *BUILDING_SCOPE_CODES}
        return required.issubset(set(scopes))

    def scopes_for_open_id(self, open_id: str) -> list[str]:
        open_id = str(open_id or "").strip()
        with self._lock:
            payload = self._load_permissions_locked()
            users = payload.get("users") or {}
            user_cfg = users.get(open_id) if isinstance(users, dict) else None
            if isinstance(user_cfg, dict):
                if user_cfg.get("enabled") is False:
                    return []
                return self._scopes_from_raw(user_cfg.get("scopes"))
            return self._scopes_from_raw(payload.get("default_scopes"))

    def role_for_open_id(self, open_id: str) -> str:
        open_id = str(open_id or "").strip()
        with self._lock:
            payload = self._load_permissions_locked()
            users = payload.get("users") or {}
            user_cfg = users.get(open_id) if isinstance(users, dict) else None
            if isinstance(user_cfg, dict) and user_cfg.get("enabled") is False:
                return "building"
            role = str((user_cfg or {}).get("role") or "").strip().lower()
            return "admin" if role == "admin" else "building"

    def get_permissions_payload(self) -> dict[str, Any]:
        with self._lock:
            payload = copy.deepcopy(self._load_permissions_locked())
        users = payload.get("users") if isinstance(payload.get("users"), dict) else {}
        user_list = []
        for open_id, user_cfg in users.items():
            if not isinstance(user_cfg, dict):
                continue
            scopes = self._scopes_from_raw(user_cfg.get("scopes"))
            role = "admin" if str(user_cfg.get("role") or "").lower() == "admin" else "building"
            if role == "admin":
                scopes = self._admin_scopes()
            user_list.append(
                {
                    "open_id": str(open_id or "").strip(),
                    "name": str(user_cfg.get("name") or ""),
                    "role": role,
                    "scopes": scopes,
                    "enabled": user_cfg.get("enabled") is not False,
                    "locked": str(open_id or "").strip() in REQUIRED_ADMIN_USERS,
                }
            )
        user_list.sort(key=lambda item: (0 if item["locked"] else 1, item["name"], item["open_id"]))
        return {
            "updated_at": str(payload.get("updated_at") or ""),
            "default_scopes": self._scopes_from_raw(payload.get("default_scopes")),
            "scope_options": copy.deepcopy(SCOPE_OPTIONS),
            "users": user_list,
            "required_admin_open_ids": list(REQUIRED_ADMIN_USERS.keys()),
        }

    def save_permissions_payload(
        self,
        raw_users: Any,
        *,
        updated_by: str = "",
    ) -> tuple[dict[str, Any], list[str]]:
        if not isinstance(raw_users, list):
            raise PortalError("权限用户列表格式错误。")
        if len(raw_users) > 300:
            raise PortalError("权限用户数量过多，请控制在 300 人以内。")
        with self._lock:
            previous = self._load_permissions_locked()
            previous_users = previous.get("users") if isinstance(previous.get("users"), dict) else {}
            users: dict[str, dict[str, Any]] = {}
            for raw in raw_users:
                if not isinstance(raw, dict):
                    continue
                open_id = str(raw.get("open_id") or "").strip()
                if not open_id:
                    continue
                if open_id == "__meta__":
                    raise PortalError("__meta__ 是系统保留 open_id，不能作为用户。")
                name = str(raw.get("name") or "").strip()[:80]
                role = str(raw.get("role") or "").strip().lower()
                role = "admin" if role == "admin" else "building"
                enabled = raw.get("enabled") is not False
                scopes = self._admin_scopes() if role == "admin" else self._scopes_from_raw(raw.get("scopes"))
                if enabled and role != "admin" and not scopes:
                    raise PortalError(f"{name or open_id} 未选择可访问楼栋。")
                users[open_id] = {
                    "name": name or open_id,
                    "role": role,
                    "scopes": scopes,
                    "enabled": enabled,
                }
            payload = {
                "version": 1,
                "updated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "default_scopes": [],
                "users": users,
                "updated_by": str(updated_by or ""),
            }
            payload = self._ensure_required_admins_locked(payload)
            self._save_permissions_locked(payload)

            changed_open_ids: list[str] = []
            for open_id, user_cfg in payload["users"].items():
                if previous_users.get(open_id) != user_cfg:
                    changed_open_ids.append(open_id)
            for open_id in previous_users:
                if open_id not in payload["users"]:
                    changed_open_ids.append(open_id)
            changed_open_ids = list(dict.fromkeys([item for item in changed_open_ids if item]))
            return self.get_permissions_payload(), changed_open_ids

    def upsert_permission_user(
        self,
        *,
        open_id: str,
        name: str = "",
        role: str = "building",
        scopes: Any = None,
        enabled: bool = True,
        updated_by: str = "",
    ) -> dict[str, Any]:
        open_id = str(open_id or "").strip()
        if not open_id:
            raise PortalError("缺少 open_id，无法写入门户权限。")
        if open_id == "__meta__":
            raise PortalError("__meta__ 是系统保留 open_id，不能作为用户。")
        if open_id in REQUIRED_ADMIN_USERS:
            return self.get_permissions_payload()
        role = "admin" if str(role or "").strip().lower() == "admin" else "building"
        normalized_scopes = (
            self._admin_scopes() if role == "admin" else self._scopes_from_raw(scopes)
        )
        if enabled and role != "admin" and not normalized_scopes:
            raise PortalError("未选择可访问楼栋，无法写入门户权限。")
        with self._lock:
            payload = self._load_permissions_locked()
            users = payload.get("users") if isinstance(payload.get("users"), dict) else {}
            payload["users"] = users
            users[open_id] = {
                "name": str(name or open_id).strip()[:80] or open_id,
                "role": role,
                "scopes": normalized_scopes,
                "enabled": bool(enabled),
            }
            payload["updated_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            payload["updated_by"] = str(updated_by or "")
            self._save_permissions_locked(payload)
        return self.get_permissions_payload()

    def admin_open_ids(self) -> list[str]:
        with self._lock:
            payload = self._load_permissions_locked()
            users = payload.get("users") if isinstance(payload.get("users"), dict) else {}
            recipients = list(REQUIRED_ADMIN_USERS.keys())
            for open_id, user_cfg in users.items():
                if not isinstance(user_cfg, dict):
                    continue
                if user_cfg.get("enabled") is False:
                    continue
                role = str(user_cfg.get("role") or "").strip().lower()
                if role == "admin":
                    recipients.append(str(open_id or "").strip())
        return [item for item in dict.fromkeys(recipients) if item]

    @staticmethod
    def _permission_code_hash(code: str, salt: str) -> str:
        return hashlib.sha256(f"{salt}:{code}".encode("utf-8")).hexdigest()

    def _public_permission_request(self, payload: dict[str, Any] | None) -> dict[str, Any]:
        if not isinstance(payload, dict):
            return {}
        attempts = int(payload.get("attempts") or 0)
        max_attempts = int(payload.get("max_attempts") or PERMISSION_REQUEST_MAX_ATTEMPTS)
        scopes = self._permission_request_scopes_from_raw(payload.get("requested_scopes"))
        return {
            "request_id": str(payload.get("request_id") or ""),
            "open_id": str(payload.get("open_id") or ""),
            "name": str(payload.get("name") or ""),
            "requested_scopes": scopes,
            "requested_scope_labels": [self.scope_label(scope) for scope in scopes],
            "reason": str(payload.get("reason") or ""),
            "status": str(payload.get("status") or ""),
            "created_at": str(payload.get("created_at") or ""),
            "expires_at": str(payload.get("expires_at") or ""),
            "attempts": attempts,
            "max_attempts": max_attempts,
            "remaining_attempts": max(0, max_attempts - attempts),
        }

    def get_current_permission_request(self, open_id: str) -> dict[str, Any]:
        open_id = str(open_id or "").strip()
        if not open_id:
            return {}
        with self._lock:
            payload = self._state_store.get_active_permission_request(open_id)
        return self._public_permission_request(payload)

    def create_permission_request(
        self,
        *,
        open_id: str,
        name: str = "",
        scopes: Any = None,
        reason: str = "",
    ) -> dict[str, Any]:
        open_id = str(open_id or "").strip()
        if not open_id:
            raise PortalError("缺少 open_id，无法提交权限申请。")
        if self.scopes_for_open_id(open_id):
            raise PortalError("当前账号已经有门户权限，无需重复申请。")
        requested_scopes = self._permission_request_scopes_from_raw(scopes)
        if not requested_scopes:
            raise PortalError("请至少选择一个需要访问的楼栋或园区。")
        reason = str(reason or "").strip()[:500]
        code = f"{secrets.randbelow(1000000):06d}"
        salt = secrets.token_urlsafe(18)
        request_id = secrets.token_urlsafe(18)
        now_ts = time.time()
        expires_at_ts = now_ts + PERMISSION_REQUEST_TTL_SECONDS
        now = dt.datetime.now()
        expires_at = now + dt.timedelta(seconds=PERMISSION_REQUEST_TTL_SECONDS)
        payload = {
            "request_id": request_id,
            "open_id": open_id,
            "name": str(name or "飞书用户").strip()[:80] or "飞书用户",
            "requested_scopes": requested_scopes,
            "reason": reason,
            "status": "notifying",
            "code_hash": self._permission_code_hash(code, salt),
            "code_salt": salt,
            "attempts": 0,
            "max_attempts": PERMISSION_REQUEST_MAX_ATTEMPTS,
            "created_at": now.strftime("%Y-%m-%d %H:%M:%S"),
            "created_at_ts": now_ts,
            "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at_ts": now_ts,
            "expires_at": expires_at.strftime("%Y-%m-%d %H:%M:%S"),
            "expires_at_ts": expires_at_ts,
        }
        with self._lock:
            self._state_store.put_permission_request(payload)
        result = self._public_permission_request(payload)
        result["code"] = code
        return result

    def supersede_other_permission_requests(self, *, open_id: str, keep_request_id: str) -> None:
        open_id = str(open_id or "").strip()
        keep_request_id = str(keep_request_id or "").strip()
        if not open_id or not keep_request_id:
            return
        with self._lock:
            self._state_store.mark_permission_requests_for_open_id(
                open_id,
                from_status="pending",
                to_status="superseded",
                exclude_request_id=keep_request_id,
            )

    def activate_permission_request(self, request_id: str) -> dict[str, Any]:
        request_id = str(request_id or "").strip()
        if not request_id:
            raise PortalError("权限申请编号为空。")
        with self._lock:
            payload = self._state_store.get_permission_request(request_id)
            if not isinstance(payload, dict):
                raise PortalError("权限申请不存在或已失效，请重新申请。")
            if str(payload.get("status") or "") != "notifying":
                return self._public_permission_request(payload)
            payload["status"] = "pending"
            payload["updated_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            payload["updated_at_ts"] = time.time()
            self._state_store.put_permission_request(payload)
        return self._public_permission_request(payload)

    def mark_permission_request_notify_failed(self, request_id: str) -> None:
        with self._lock:
            payload = self._state_store.get_permission_request(request_id)
            if not isinstance(payload, dict):
                return
            payload["status"] = "notify_failed"
            payload["updated_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            payload["updated_at_ts"] = time.time()
            self._state_store.put_permission_request(payload)

    def confirm_permission_request(
        self,
        *,
        open_id: str,
        request_id: str,
        code: str,
        updated_by: str = "permission_request",
    ) -> dict[str, Any]:
        open_id = str(open_id or "").strip()
        request_id = str(request_id or "").strip()
        code = str(code or "").strip()
        if not open_id or not request_id or not code:
            raise PortalError("缺少申请编号或验证码。")
        with self._lock:
            payload = self._state_store.get_permission_request(request_id)
            if not isinstance(payload, dict):
                raise PortalError("权限申请不存在或已失效，请重新申请。")
            if str(payload.get("open_id") or "").strip() != open_id:
                raise PortalError("该验证码不属于当前登录账号。")
            if str(payload.get("status") or "") != "pending":
                raise PortalError("该权限申请已处理或已失效，请重新申请。")
            now_ts = time.time()
            if now_ts >= float(payload.get("expires_at_ts") or 0):
                payload["status"] = "expired"
                payload["updated_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                payload["updated_at_ts"] = now_ts
                self._state_store.put_permission_request(payload)
                raise PortalError("验证码已过期，请重新申请。")
            attempts = int(payload.get("attempts") or 0)
            max_attempts = int(payload.get("max_attempts") or PERMISSION_REQUEST_MAX_ATTEMPTS)
            if attempts >= max_attempts:
                payload["status"] = "failed"
                payload["updated_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                payload["updated_at_ts"] = now_ts
                self._state_store.put_permission_request(payload)
                raise PortalError("验证码错误次数过多，请重新申请。")
            attempts += 1
            payload["attempts"] = attempts
            payload["updated_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            payload["updated_at_ts"] = now_ts
            expected = str(payload.get("code_hash") or "")
            actual = self._permission_code_hash(code, str(payload.get("code_salt") or ""))
            if not hmac.compare_digest(expected, actual):
                if attempts >= max_attempts:
                    payload["status"] = "failed"
                    self._state_store.put_permission_request(payload)
                    raise PortalError("验证码错误次数过多，请重新申请。")
                self._state_store.put_permission_request(payload)
                raise PortalError(f"验证码错误，还可尝试 {max_attempts - attempts} 次。")
            requested_scopes = self._permission_request_scopes_from_raw(
                payload.get("requested_scopes")
            )
            if not requested_scopes:
                payload["status"] = "failed"
                self._state_store.put_permission_request(payload)
                raise PortalError("申请楼栋为空，请重新申请。")
            payload["status"] = "approved"
            payload["approved_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            payload["approved_at_ts"] = now_ts
            self._state_store.put_permission_request(payload)
            return self.upsert_permission_user(
                open_id=open_id,
                name=str(payload.get("name") or open_id),
                role="building",
                scopes=requested_scopes,
                enabled=True,
                updated_by=updated_by,
            )

    def is_admin(self, session: dict[str, Any] | None) -> bool:
        if not isinstance(session, dict):
            return False
        role = str(session.get("role") or "").strip().lower()
        if not role:
            role = str((session.get("user") or {}).get("role") or "").strip().lower()
        return role == "admin"

    def session_scopes(self, session: dict[str, Any] | None) -> list[str]:
        if not isinstance(session, dict):
            return []
        return self._scopes_from_raw(session.get("allowed_scopes"))

    def scope_allowed(self, session: dict[str, Any] | None, scope: str) -> bool:
        requested = self.normalize_scope(scope)
        scopes = self.session_scopes(session)
        if requested == "ALL":
            return "ALL" in scopes or self._has_full_scope_access(scopes)
        return "ALL" in scopes or requested in scopes

    def default_scope(self, session: dict[str, Any] | None) -> str:
        scopes = self.session_scopes(session)
        if "ALL" in scopes or self._has_full_scope_access(scopes):
            return "ALL"
        for scope in scopes:
            if scope in BUILDING_SCOPE_CODES or scope == "CAMPUS":
                return scope
        return ""

    def filter_scope_options(
        self, options: list[dict[str, Any]], session: dict[str, Any] | None
    ) -> list[dict[str, Any]]:
        return [
            copy.deepcopy(option)
            for option in options
            if self.scope_allowed(session, str(option.get("value") or ""))
        ]

    def filter_scope_overview(
        self, payload: dict[str, Any], session: dict[str, Any] | None
    ) -> dict[str, Any]:
        result = copy.deepcopy(payload or {})
        options = self.filter_scope_options(result.get("scope_options") or SCOPE_OPTIONS, session)
        allowed_values = {str(option.get("value") or "") for option in options}
        scopes = result.get("scopes") if isinstance(result.get("scopes"), dict) else {}
        result["scope_options"] = options
        result["scopes"] = {
            scope: value
            for scope, value in scopes.items()
            if self.normalize_scope(scope) in allowed_values
        }
        prepared = (
            result.get("prepared_workbenches")
            if isinstance(result.get("prepared_workbenches"), dict)
            else {}
        )
        filtered_prepared: dict[str, Any] = {}
        for scope, value in prepared.items():
            normalized = self.normalize_scope(scope)
            if normalized not in allowed_values or not isinstance(value, dict):
                continue
            prepared_value = copy.deepcopy(value)
            nested_options = prepared_value.get("scope_options")
            if isinstance(nested_options, list):
                prepared_value["scope_options"] = options
            filtered_prepared[normalized] = prepared_value
        if prepared:
            result["prepared_workbenches"] = filtered_prepared
        return result

    def filter_handover_links(
        self, payload: dict[str, Any], session: dict[str, Any] | None
    ) -> dict[str, Any]:
        result = copy.deepcopy(payload or {})
        options = self.filter_scope_options(result.get("scope_options") or [], session)
        allowed_values = {
            str(option.get("value") or "")
            for option in options
            if str(option.get("value") or "") in BUILDING_SCOPE_CODES
        }
        links = result.get("links") if isinstance(result.get("links"), dict) else {}
        result["scope_options"] = [
            option
            for option in options
            if str(option.get("value") or "") in BUILDING_SCOPE_CODES
        ]
        result["links"] = {
            scope: url
            for scope, url in links.items()
            if self.normalize_scope(scope) in allowed_values
        }
        return result

    def public_status(
        self,
        session: dict[str, Any] | None,
        *,
        next_path: str = "/",
        redirect_uri: str = "",
    ) -> dict[str, Any]:
        logged_in = isinstance(session, dict)
        scope_options = self.filter_scope_options(SCOPE_OPTIONS, session) if logged_in else []
        return {
            "configured": self.configured(),
            "logged_in": logged_in,
            "user": self.public_user(session) if logged_in else None,
            "allowed_scopes": self.session_scopes(session),
            "scope_options": scope_options,
            "default_scope": self.default_scope(session),
            "login_url": f"/api/auth/login?{urlencode({'next': self._normalize_next_path(next_path)})}",
            "redirect_uri": str(redirect_uri or "").strip(),
        }

    def public_user(self, session: dict[str, Any] | None) -> dict[str, Any]:
        if not isinstance(session, dict):
            return {}
        user = dict(session.get("user") or {})
        return {
            "open_id": str(user.get("open_id") or ""),
            "union_id": str(user.get("union_id") or ""),
            "user_id": str(user.get("user_id") or ""),
            "name": str(user.get("name") or user.get("en_name") or "飞书用户"),
            "avatar_url": str(user.get("avatar_url") or user.get("avatar_thumb") or ""),
            "role": "admin" if self.is_admin(session) else "building",
        }

    def start_login(self, *, redirect_uri: str, next_path: str = "/") -> str:
        if not self.configured():
            raise PortalError("未配置飞书 App ID 或 App Secret，无法启用扫码登录。")
        state = secrets.token_urlsafe(24)
        now = time.time()
        with self._lock:
            self._cleanup_expired_locked(now)
            self._states[state] = {
                "redirect_uri": redirect_uri,
                "next": self._normalize_next_path(next_path),
                "expires_at": now + AUTH_STATE_TTL_SECONDS,
            }
            self._trim_by_expiry_locked(self._states, AUTH_MAX_PENDING_STATES)
        params = {
            "app_id": str(config.app_id or "").strip(),
            "redirect_uri": redirect_uri,
            "state": state,
        }
        return f"{FEISHU_AUTH_INDEX_URL}?{urlencode(params)}"

    def complete_login(
        self, *, code: str, state: str, redirect_uri: str
    ) -> tuple[str, str]:
        code = str(code or "").strip()
        state = str(state or "").strip()
        if not code or not state:
            raise PortalError("飞书登录回调缺少 code 或 state。")
        with self._lock:
            self._cleanup_expired_locked()
            state_payload = self._states.pop(state, None)
        if not state_payload:
            raise PortalError("登录状态已失效，请重新扫码。")
        if time.time() > float(state_payload.get("expires_at") or 0):
            raise PortalError("登录状态已过期，请重新扫码。")
        expected_redirect = str(state_payload.get("redirect_uri") or "")
        if expected_redirect and expected_redirect != redirect_uri:
            raise PortalError("登录回调地址与发起地址不一致。")

        user = self._exchange_login_code(code)
        open_id = str(user.get("open_id") or "").strip()
        if not open_id:
            raise PortalError("飞书登录未返回 open_id。")
        allowed_scopes = self.scopes_for_open_id(open_id)
        role = self.role_for_open_id(open_id)
        session_id = secrets.token_urlsafe(32)
        now = time.time()
        session = {
            "session_id": session_id,
            "user": user,
            "role": role,
            "allowed_scopes": allowed_scopes,
            "created_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "expires_at": now + AUTH_SESSION_TTL_SECONDS,
        }
        with self._lock:
            self._cleanup_expired_locked(now)
            self._sessions[session_id] = session
            self._trim_by_expiry_locked(self._sessions, AUTH_MAX_SESSIONS)
        return session_id, self._normalize_next_path(str(state_payload.get("next") or "/"))

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        session_id = str(session_id or "").strip()
        if not session_id:
            return None
        with self._lock:
            self._cleanup_expired_locked()
            session = self._sessions.get(session_id)
            if not session:
                return None
            if time.time() > float(session.get("expires_at") or 0):
                self._sessions.pop(session_id, None)
                return None
            open_id = str((session.get("user") or {}).get("open_id") or "").strip()
            if open_id:
                session["allowed_scopes"] = self.scopes_for_open_id(open_id)
                session["role"] = self.role_for_open_id(open_id)
            session["expires_at"] = time.time() + AUTH_SESSION_TTL_SECONDS
            return copy.deepcopy(session)

    def clear_session(self, session_id: str) -> None:
        with self._lock:
            self._sessions.pop(str(session_id or "").strip(), None)

    def _get_app_access_token(self) -> str:
        try:
            return token_manager.get_app_access_token()
        except FeishuTokenError as exc:
            raise PortalError(f"获取飞书 app_access_token 失败: {exc}") from exc

    def _exchange_login_code(self, code: str) -> dict[str, Any]:
        try:
            return token_manager.exchange_login_code(code)
        except FeishuTokenError as exc:
            raise PortalError(f"获取飞书用户身份失败: {exc}") from exc

    def _fetch_user_info(self, user_access_token: str) -> dict[str, Any]:
        return token_manager.fetch_user_info(user_access_token)

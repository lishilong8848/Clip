# -*- coding: utf-8 -*-
from __future__ import annotations

import copy
import datetime as dt
import json
import secrets
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests

from upload_event_module.config import config
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


AUTH_COOKIE_NAME = "lan_portal_session"
AUTH_SESSION_TTL_SECONDS = 12 * 60 * 60
AUTH_STATE_TTL_SECONDS = 10 * 60
AUTH_MAX_PENDING_STATES = 200
AUTH_MAX_SESSIONS = 500
FEISHU_APP_TOKEN_TTL_FALLBACK_SECONDS = 90 * 60
FEISHU_AUTH_INDEX_URL = "https://open.feishu.cn/open-apis/authen/v1/index"
FEISHU_APP_ACCESS_TOKEN_URL = (
    "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
)
FEISHU_USER_ACCESS_TOKEN_URL = "https://open.feishu.cn/open-apis/authen/v1/access_token"
FEISHU_USER_INFO_URL = "https://open.feishu.cn/open-apis/authen/v1/user_info"


class PortalAuthManager:
    """Feishu OAuth session and local scope authorization for the LAN portal."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._sessions: dict[str, dict[str, Any]] = {}
        self._states: dict[str, dict[str, Any]] = {}
        self._app_token_cache: dict[str, Any] = {"token": "", "expires_at": 0.0}
        self._permission_path = Path(get_data_file_path("lan_portal_auth.json"))
        self._permission_path.parent.mkdir(parents=True, exist_ok=True)

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
                "scopes": [code],
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

    def _load_permissions_locked(self) -> dict[str, Any]:
        if not self._permission_path.exists():
            payload = self._default_permissions()
            self._save_permissions_locked(payload)
            return payload
        try:
            with self._permission_path.open("r", encoding="utf-8") as fp:
                payload = json.load(fp)
            if not isinstance(payload, dict):
                raise ValueError("payload is not object")
        except Exception:
            self._backup_corrupt_permissions_locked()
            payload = self._default_permissions()
            self._save_permissions_locked(payload)
            return payload
        users = payload.get("users")
        if not isinstance(users, dict):
            payload["users"] = {}
        default_scopes = payload.get("default_scopes")
        if not isinstance(default_scopes, list):
            payload["default_scopes"] = []
        return payload

    def _save_permissions_locked(self, payload: dict[str, Any]) -> None:
        tmp_path = self._permission_path.with_suffix(self._permission_path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False, indent=2)
        tmp_path.replace(self._permission_path)

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

    def scopes_for_open_id(self, open_id: str) -> list[str]:
        open_id = str(open_id or "").strip()
        with self._lock:
            payload = self._load_permissions_locked()
            users = payload.get("users") or {}
            user_cfg = users.get(open_id) if isinstance(users, dict) else None
            if isinstance(user_cfg, dict):
                return self._scopes_from_raw(user_cfg.get("scopes"))
            return self._scopes_from_raw(payload.get("default_scopes"))

    def is_admin(self, session: dict[str, Any] | None) -> bool:
        scopes = self.session_scopes(session)
        return "ALL" in scopes

    def session_scopes(self, session: dict[str, Any] | None) -> list[str]:
        if not isinstance(session, dict):
            return []
        return self._scopes_from_raw(session.get("allowed_scopes"))

    def scope_allowed(self, session: dict[str, Any] | None, scope: str) -> bool:
        requested = self.normalize_scope(scope)
        scopes = self.session_scopes(session)
        return "ALL" in scopes or requested in scopes

    def default_scope(self, session: dict[str, Any] | None) -> str:
        scopes = self.session_scopes(session)
        if "ALL" in scopes:
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
        session_id = secrets.token_urlsafe(32)
        now = time.time()
        session = {
            "session_id": session_id,
            "user": user,
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
            session["expires_at"] = time.time() + AUTH_SESSION_TTL_SECONDS
            return copy.deepcopy(session)

    def clear_session(self, session_id: str) -> None:
        with self._lock:
            self._sessions.pop(str(session_id or "").strip(), None)

    def _get_app_access_token(self) -> str:
        now = time.time()
        with self._lock:
            token = str(self._app_token_cache.get("token") or "")
            expires_at = float(self._app_token_cache.get("expires_at") or 0)
            if token and now < expires_at:
                return token
        payload = {
            "app_id": str(config.app_id or "").strip(),
            "app_secret": str(config.app_secret or "").strip(),
        }
        try:
            response = requests.post(
                FEISHU_APP_ACCESS_TOKEN_URL,
                json=payload,
                headers={"Content-Type": "application/json; charset=utf-8"},
                timeout=8.0,
            )
            response.raise_for_status()
            result = response.json()
        except Exception as exc:
            raise PortalError(f"获取飞书 app_access_token 失败: {exc}") from exc
        if result.get("code", 0) != 0:
            raise PortalError(f"获取飞书 app_access_token 失败: {result.get('msg') or 'unknown'}")
        token = str(result.get("app_access_token") or "").strip()
        if not token:
            raise PortalError("获取飞书 app_access_token 失败: 返回为空")
        expire = int(result.get("expire") or FEISHU_APP_TOKEN_TTL_FALLBACK_SECONDS)
        with self._lock:
            self._app_token_cache = {"token": token, "expires_at": time.time() + expire - 300}
        return token

    def _exchange_login_code(self, code: str) -> dict[str, Any]:
        app_access_token = self._get_app_access_token()
        try:
            response = requests.post(
                FEISHU_USER_ACCESS_TOKEN_URL,
                headers={
                    "Authorization": f"Bearer {app_access_token}",
                    "Content-Type": "application/json; charset=utf-8",
                },
                json={"grant_type": "authorization_code", "code": code},
                timeout=8.0,
            )
            response.raise_for_status()
            result = response.json()
        except Exception as exc:
            raise PortalError(f"获取飞书用户身份失败: {exc}") from exc
        if result.get("code", 0) != 0:
            raise PortalError(f"获取飞书用户身份失败: {result.get('msg') or 'unknown'}")
        data = result.get("data") if isinstance(result.get("data"), dict) else {}
        user = dict(data)
        if not user.get("open_id") and user.get("access_token"):
            user.update(self._fetch_user_info(str(user.get("access_token") or "")))
        return user

    def _fetch_user_info(self, user_access_token: str) -> dict[str, Any]:
        if not user_access_token:
            return {}
        try:
            response = requests.get(
                FEISHU_USER_INFO_URL,
                headers={"Authorization": f"Bearer {user_access_token}"},
                timeout=8.0,
            )
            response.raise_for_status()
            result = response.json()
        except Exception:
            return {}
        if result.get("code", 0) != 0:
            return {}
        data = result.get("data")
        return dict(data) if isinstance(data, dict) else {}

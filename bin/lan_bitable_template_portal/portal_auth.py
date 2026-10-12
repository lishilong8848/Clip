# -*- coding: utf-8 -*-
from __future__ import annotations

import copy
import datetime as dt
import hashlib
import hmac
import json
import logging
import secrets
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

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
from .state_store import DEFAULT_STATE_DB_NAME, LanPortalStateStore
from .cabinet_guest import GUEST_SCOPES, GUEST_SESSION_PREFIX, is_cabinet_guest
from .account_policy import (
    MODULE_IDS,
    default_modules,
    categorize_position,
)


AUTH_COOKIE_NAME = "lan_portal_session"
AUTH_RETRY_COOKIE_NAME = "lan_portal_auth_retry"
AUTH_SESSION_TTL_SECONDS = 12 * 60 * 60
AUTH_STATE_TTL_SECONDS = 10 * 60
AUTH_MAX_PENDING_STATES = 200
AUTH_MAX_SESSIONS = 500
PERMISSION_REQUEST_TTL_SECONDS = 15 * 60
PERMISSION_REVIEW_TTL_SECONDS = 30 * 24 * 60 * 60
PERMISSION_REQUEST_MAX_ATTEMPTS = 5
FEISHU_AUTH_INDEX_URL = "https://accounts.feishu.cn/open-apis/authen/v1/authorize"
OAUTH_STATE_PREFIX = "oauth3_"
REQUIRED_ADMIN_USERS = {
    LI_SHILONG_OPEN_ID: "李世龙",
}


class PortalAuthStateError(PortalError):
    def __init__(self, message: str, *, next_path: str = "/") -> None:
        super().__init__(message)
        self.next_path = str(next_path or "/")


class _LazyTokenManager:
    def __init__(self) -> None:
        self._manager: Any = None
        self._lock = threading.RLock()

    def _load(self) -> Any:
        manager = self._manager
        if manager is not None:
            return manager
        with self._lock:
            manager = self._manager
            if manager is None:
                from upload_event_module.services.feishu_token_manager import (
                    token_manager as loaded_manager,
                )

                manager = loaded_manager
                self._manager = manager
            return manager

    def __getattr__(self, name: str) -> Any:
        return getattr(self._load(), name)


token_manager = _LazyTokenManager()


class PortalAuthManager:
    """Feishu OAuth session and local scope authorization for the LAN portal."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._sessions: dict[str, dict[str, Any]] = {}
        self._states: dict[str, dict[str, Any]] = {}
        self._last_persistent_cleanup = 0.0
        self._permissions_cache: dict[str, Any] | None = None
        self._permissions_cache_until = 0.0
        self._cleanup_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="PortalAuthCleanup")
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
    def auth_retry_cookie_header(*, active: bool) -> str:
        if active:
            return (
                f"{AUTH_RETRY_COOKIE_NAME}=1; Path=/; Max-Age=120; "
                "HttpOnly; SameSite=Lax"
            )
        return (
            f"{AUTH_RETRY_COOKIE_NAME}=; Path=/; Max-Age=0; "
            "HttpOnly; SameSite=Lax"
        )

    @staticmethod
    def _secret_hash(value: str) -> str:
        return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()

    @staticmethod
    def _normalize_next_path(next_path: str) -> str:
        text = str(next_path or "/").strip()
        for _ in range(5):
            if (not text.startswith("/") or text.startswith("//") or "\\" in text
                    or any(ord(char) < 32 or ord(char) == 127 for char in text)):
                return "/"
            try:
                parts = urlsplit(text)
            except ValueError:
                return "/"
            path = parts.path or "/"
            if parts.netloc or parts.scheme or path.startswith("/api/"):
                return "/"
            pairs = parse_qsl(parts.query, keep_blank_values=True)
            query = dict(pairs)
            # Any login tab returns to the original page, not back into the login entry.
            if path == "/" and query.get("login") in {"password", "feishu", "guest"} and query.get("next"):
                text = query["next"].strip()
                continue
            query_text = urlencode([(key, value) for key, value in pairs if key not in {"code", "state"}
                                    and not (path == "/" and key in {"login", "auth_notice"})])
            return urlunsplit(("", "", path, query_text, parts.fragment)) or "/"
        return "/"

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
                "role": "admin" if code == "H" else "building",
                "scopes": self._admin_scopes() if code == "H" else [code],
            }
        users[LI_SHILONG_OPEN_ID] = {
            "name": "李世龙",
            "role": "admin",
            "scopes": self._admin_scopes(),
        }
        return {
            "version": 2,
            "updated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "default_scopes": [],
            "users": users,
        }

    def _ensure_required_admins_locked(self, payload: dict[str, Any]) -> dict[str, Any]:
        if payload.get("version", 1) < 2 and not self._state_store.get_document("account_management", "permissions_before_levels"):
            self._state_store.put_document("account_management", "permissions_before_levels", payload)
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
        for code, open_id in BUILDING_OPEN_ID_MAP.items():
            existing = users.get(open_id) if isinstance(users.get(open_id), dict) else {}
            desired = {**existing, "name": existing.get("name") or self.scope_label(code), "role": "admin" if code == "H" else "building",
                       "scopes": self._admin_scopes() if code == "H" else [code],
                       "enabled": existing.get("enabled") is not False, "locked": False}
            if existing != desired:
                users[open_id] = desired
                changed = True
        if changed:
            payload["version"] = 2
            payload["updated_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._save_permissions_locked(payload)
        return payload

    def _load_permissions_locked(self) -> dict[str, Any]:
        if self._permissions_cache is not None and time.monotonic() < self._permissions_cache_until:
            return copy.deepcopy(self._permissions_cache)
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
        payload = self._ensure_required_admins_locked(payload)
        self._permissions_cache = copy.deepcopy(payload)
        self._permissions_cache_until = time.monotonic() + 5.0
        return payload

    def _save_permissions_locked(self, payload: dict[str, Any]) -> None:
        self._state_store.put_auth_permissions(payload)
        self._permissions_cache = copy.deepcopy(payload)
        self._permissions_cache_until = time.monotonic() + 5.0

    def _permission_user(self, open_id: str) -> dict[str, Any]:
        with self._lock:
            if self._permissions_cache is None or time.monotonic() >= self._permissions_cache_until:
                self._load_permissions_locked()
            return copy.deepcopy((self._permissions_cache.get("users") or {}).get(open_id) or {})

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
        if now - float(self._last_persistent_cleanup or 0) >= 60:
            self._last_persistent_cleanup = now
            try:
                self._cleanup_executor.submit(
                    self._state_store.cleanup_auth_runtime,
                    now=now,
                    max_states=AUTH_MAX_PENDING_STATES,
                    max_sessions=AUTH_MAX_SESSIONS,
                )
            except RuntimeError:
                pass

    def shutdown(self) -> None:
        self._cleanup_executor.shutdown(wait=False, cancel_futures=True)

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
        if self.directory_block_reason(open_id):
            return []
        user_cfg = self._permission_user(open_id)
        if not user_cfg or user_cfg.get("enabled") is False:
            return []
        return self.account_profile(open_id)["scope_rules"]

    def role_for_open_id(self, open_id: str) -> str:
        open_id = str(open_id or "").strip()
        if self.directory_block_reason(open_id):
            return "building"
        profile = self.account_profile(open_id)
        return profile.get("role", "admin" if profile["level"] in {"L0", "L1"} and profile["can_manage_accounts"] else "building")

    def account_profile(self, open_id: str) -> dict[str, Any]:
        """Derive management authority from trusted identity, HR and saved grants.

        ``access_version == 1`` marks an explicitly configured permission record
        whose ``level``/``category``/``modules``/``scopes`` take priority over the
        HR-derived defaults. Legacy records (without the marker) keep deriving from
        the trusted HR identity so historical raw roles / admin / dirty scopes can
        never grant new authority by themselves.
        """
        open_id = str(open_id or "").strip()
        cfg = self._permission_user(open_id)
        overridden = bool(cfg) and cfg.get("access_version") == 1
        duty = next((code for code, oid in BUILDING_OPEN_ID_MAP.items() if oid == open_id), "")
        person = {} if duty else self.directory_person(open_id)
        homes = [code for code in person.get("scopes", []) if code in BUILDING_SCOPE_CODES]
        home = duty or (homes[0] if len(homes) == 1 else "")
        category = "duty" if duty else categorize_position(person.get("position"))
        if category == "other":
            category = categorize_position(" ".join(person.get("assignments") or []))
        enabled = bool(cfg) and cfg.get("enabled") is not False and not self.directory_block_reason(open_id)

        level = "L4"
        if open_id == LI_SHILONG_OPEN_ID:
            level = "L0"
        elif duty == "H":
            level = "L1"
        elif overridden and str(cfg.get("level") or "").strip() in {"L1", "L2", "L3", "L4"}:
            level = str(cfg.get("level")).strip()
        elif duty == "H" or (home == "H" and category == "manager"):
            level = "L1"
        elif home == "H" and category == "engineer":
            level = "L2"
        elif home and category == "supervisor":
            level = "L3"

        if overridden and str(cfg.get("category") or "").strip():
            category = str(cfg.get("category")).strip()

        # Scope rules: L0/L1/H duty are effectively full-ALL; manual override wins
        # for L2-L4; legacy records always follow HR home.
        if level in {"L0", "L1"} or duty == "H":
            scope_rules = self._admin_scopes()
        elif overridden and isinstance(cfg.get("scopes"), list):
            manual = self._scopes_from_raw(cfg.get("scopes"))
            scope_rules = manual or ([home] if home else [])
        else:
            scope_rules = [home] if home else []

        # Modules: L0/L1 are effectively ALL modules; an explicit override
        # (``access_version == 1``) wins; already-materialized legacy modules
        # (``module_access_version == 1``) are honoured; unknown or disabled
        # identities never receive functionality derived from level defaults.
        if not enabled:
            # Disabled / blocked identity fails closed: no module grants at all.
            modules = []
        elif level == "L0" or duty == "H":
            modules = list(default_modules(level))
        elif overridden and isinstance(cfg.get("modules"), list):
            # Empty is a valid explicit full revocation and must NOT fall back to
            # the level defaults. Only a genuinely absent module field would keep
            # deriving from the level defaults.
            modules = [str(module) for module in cfg.get("modules") if str(module) in MODULE_IDS]
        elif cfg.get("module_access_version") == 1 and isinstance(cfg.get("modules"), list):
            # Materialized in an earlier directory sync / migration. Empty set is
            # preserved as an explicit full revocation.
            modules = [str(module) for module in cfg.get("modules") if str(module) in MODULE_IDS]
        elif bool(person) or bool(duty):
            # Valid, currently-known, enabled identity: default to the level's
            # capability.
            modules = list(default_modules(level))
        else:
            # Unknown identity: never grant default modules.
            modules = []

        # 账号管理能力同时取决于账号管理模块是否仍然可用。
        can_manage = enabled and level != "L4" and "accounts" in modules
        role = "admin" if enabled and level in {"L0", "L1"} else "building"
        return {
            "level": level,
            "category": category,
            "home_scope": home,
            "role": role,
            "modules": modules,
            "scope_rules": scope_rules,
            "can_manage_accounts": can_manage,
            "can_view_accounts": can_manage and level != "L2",
            "can_delete_business": enabled and level in {"L0", "L1", "L3"},
        }

    def _open_id_explicitly_disabled(self, open_id: str) -> bool:
        open_id = str(open_id or "").strip()
        if not open_id:
            return False
        if self.directory_block_reason(open_id):
            return True
        return self._permission_user(open_id).get("enabled") is False

    def directory_person(self, open_id: str) -> dict[str, Any]:
        provider = getattr(self, "_account_directory_provider", None)
        snapshot = provider() if provider else {}
        cached = getattr(self, "_account_index_cache", (None, {}))
        if cached[0] is not snapshot:
            index = {}
            for person in snapshot.get("people", {}).values():
                for identity in person.get("login_ids", []):
                    if identity not in index or not person.get("inactive"):
                        index[identity] = person
            cached = (snapshot, index)
            self._account_index_cache = cached
        return cached[1].get(open_id, {})

    def directory_block_reason(self, open_id: str) -> str:
        # Shared duty accounts and recovery administrators are not personal HR records.
        if open_id in REQUIRED_ADMIN_USERS or open_id in BUILDING_OPEN_ID_MAP.values():
            return ""
        person = self.directory_person(open_id)
        if person and not person.get("selectable"):
            return str(person.get("disabled_reason") or "人员身份待核对")
        provider = getattr(self, "_account_directory_provider", None)
        snapshot = provider() if provider else {}
        return snapshot.get("removed_identities", {}).get(open_id, "")

    def _hr_derived_modules(self, identity: str, person: dict[str, Any]) -> list[str]:
        """Modules a *valid* HR identity is entitled to before any explicit grant.

        This deliberately ignores enabled/disabled store state: it is used when
        materialising the first legacy migration for an identity that has not yet
        been written into the permission store (where ``cfg`` would be empty and
        therefore look "disabled").
        """
        duty_code = next((code for code, oid in BUILDING_OPEN_ID_MAP.items() if oid == identity), "")
        homes = [code for code in person.get("scopes", []) if code in BUILDING_SCOPE_CODES]
        home = duty_code or (homes[0] if len(homes) == 1 else "")
        category = "duty" if duty_code else categorize_position(person.get("position"))
        if category == "other":
            category = categorize_position(" ".join(person.get("assignments") or []))
        level = "L4"
        if identity == LI_SHILONG_OPEN_ID:
            level = "L0"
        elif duty_code == "H":
            level = "L1"
        elif home == "H" and category == "manager":
            level = "L1"
        elif home == "H" and category == "engineer":
            level = "L2"
        elif home and category == "supervisor":
            level = "L3"
        return list(default_modules(level))

    def sync_directory_permissions(self, people, *, on_login=False) -> None:
        with self._lock:
            payload = self._load_permissions_locked()
            users = payload.setdefault("users", {})
            changed = False
            for person in people:
                identities = person.get("login_ids") or []
                if not person.get("selectable") or len(identities) != 1:
                    continue
                identity = identities[0]
                if identity in REQUIRED_ADMIN_USERS or identity in BUILDING_OPEN_ID_MAP.values():
                    continue
                previous = users.get(identity) or {}
                own = self._scopes_from_raw(person.get("scopes"))
                own = own if len(own) == 1 else []
                manager = own == ["H"] and categorize_position(person.get("position")) == "manager"
                scopes = self._admin_scopes() if manager else own
                # Manually configured access (access_version=1) must survive an
                # HR directory sync: only identity/name/enabled stay HR-managed.
                if previous.get("access_version") == 1:
                    following = {**previous, "name": person["name"],
                                 "enabled": previous.get("enabled") is not False,
                                 "directory_scopes": scopes, "denied_scopes": [],
                                 "locked": False}
                elif previous.get("module_access_version") == 1 and isinstance(previous.get("modules"), list):
                    # Legacy modules already materialized in an earlier migration:
                    # keep them and only refresh the HR-managed identity fields
                    # (without ever promoting the old role/scopes to access_version).
                    following = {**previous, "name": person["name"],
                                 "role": "admin" if manager else "building",
                                 "enabled": previous.get("enabled") is not False,
                                 "scopes": scopes, "directory_scopes": scopes,
                                 "denied_scopes": [], "locked": False,
                                 "modules": list(previous["modules"]),
                                 "module_access_version": 1}
                else:
                    # First sight of this valid identity: actually persist the
                    # HR-derived modules into the store and mark module_access_version.
                    # Old dirty role/scopes remain non-authoritative (no access_version).
                    derived = self._hr_derived_modules(identity, person)
                    following = {**previous, "name": person["name"],
                                 "role": "admin" if manager else "building",
                                 "enabled": previous.get("enabled") is not False,
                                 "scopes": scopes, "directory_scopes": scopes,
                                 "modules": derived,
                                 "module_access_version": 1,
                                 "denied_scopes": [], "locked": False}
                if previous != following:
                    users[identity] = following
                    changed = True
            if changed:
                if not self._state_store.get_document("account_management", "permissions_before_levels"):
                    self._state_store.put_document("account_management", "permissions_before_levels", self._load_permissions_locked())
                payload["version"] = 2
                payload["updated_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self._save_permissions_locked(payload)

    def get_permissions_payload(self) -> dict[str, Any]:
        with self._lock:
            payload = copy.deepcopy(self._load_permissions_locked())
        users = payload.get("users") if isinstance(payload.get("users"), dict) else {}
        user_list = []
        for open_id, user_cfg in users.items():
            if not isinstance(user_cfg, dict):
                continue
            scopes = self.scopes_for_open_id(open_id)
            role = self.role_for_open_id(open_id)
            user_list.append(
                {
                    "open_id": str(open_id or "").strip(),
                    "name": str(user_cfg.get("name") or ""),
                    "role": role,
                    "scopes": scopes,
                    "enabled": user_cfg.get("enabled") is not False,
                    "locked": str(open_id or "").strip() in REQUIRED_ADMIN_USERS,
                    "directory_scopes": list(user_cfg.get("directory_scopes") or []),
                    "denied_scopes": list(user_cfg.get("denied_scopes") or []),
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

    def validate_identity_grant(self, open_id, role, scopes):
        profile = self.account_profile(open_id)
        expected_role = "admin" if profile["level"] in {"L0", "L1"} else "building"
        if not profile["scope_rules"]:
            raise PortalError("该账号尚无明确人员身份和所属楼栋，请先核对人员目录。")
        if role != expected_role or set(scopes) != set(profile["scope_rules"]):
            raise PortalError("角色和楼栋由机楼/专业、体系岗位统一确定，不支持额外跨楼授权。")

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
            # Omitted accounts are not revocations; removal has its own endpoint.
            users: dict[str, dict[str, Any]] = copy.deepcopy(previous_users)
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
                self.validate_identity_grant(open_id, role, scopes)
                if enabled and role != "admin" and not scopes:
                    raise PortalError(f"{name or open_id} 未选择可访问楼栋。")
                users[open_id] = {
                    **previous_users.get(open_id, {}),
                    "name": name or open_id,
                    "role": role,
                    "scopes": scopes,
                    "enabled": enabled,
                }
            payload = {
                **previous,
                "version": 2,
                "updated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "default_scopes": self._scopes_from_raw(previous.get("default_scopes")),
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
        self.validate_identity_grant(open_id, role, normalized_scopes)
        if enabled and role != "admin" and not normalized_scopes:
            raise PortalError("未选择可访问楼栋，无法写入门户权限。")
        with self._lock:
            payload = self._load_permissions_locked()
            users = payload.get("users") if isinstance(payload.get("users"), dict) else {}
            payload["users"] = users
            users[open_id] = {
                **users.get(open_id, {}),
                "name": str(name or open_id).strip()[:80] or open_id,
                "role": role,
                "scopes": normalized_scopes,
                "enabled": bool(enabled),
            }
            payload["updated_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            payload["updated_by"] = str(updated_by or "")
            self._save_permissions_locked(payload)
        return self.get_permissions_payload()

    def remove_permission_user(
        self,
        open_id: str,
        *,
        updated_by: str = "",
    ) -> tuple[dict[str, Any], bool]:
        open_id = str(open_id or "").strip()
        if not open_id:
            raise PortalError("缺少 open_id，无法删除门户权限。")
        if open_id in REQUIRED_ADMIN_USERS or open_id in BUILDING_OPEN_ID_MAP.values():
            raise PortalError("固定管理员和值班账号不能删除。")
        with self._lock:
            payload = self._load_permissions_locked()
            users = payload.get("users") if isinstance(payload.get("users"), dict) else {}
            payload["users"] = users
            removed = open_id in users
            if removed:
                users[open_id] = {**users[open_id], "enabled": False}
                payload["updated_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                payload["updated_by"] = str(updated_by or "")
                self._save_permissions_locked(payload)
        return self.get_permissions_payload(), removed

    def bulk_merge_permission_users(
        self,
        raw_users: Any,
        *,
        updated_by: str = "",
    ) -> tuple[dict[str, Any], list[str], list[dict[str, Any]]]:
        if not isinstance(raw_users, list):
            raise PortalError("批量授权人员格式错误。")
        if not raw_users:
            raise PortalError("没有可授权的人员。")
        if len(raw_users) > 300:
            raise PortalError("单次授权人数不能超过 300 人。")

        changed_open_ids: list[str] = []
        results: list[dict[str, Any]] = []
        with self._lock:
            payload = self._load_permissions_locked()
            users = payload.get("users") if isinstance(payload.get("users"), dict) else {}
            payload["users"] = users
            for raw in raw_users:
                if not isinstance(raw, dict):
                    continue
                open_id = str(raw.get("open_id") or "").strip()
                if not open_id or open_id == "__meta__":
                    raise PortalError("批量授权人员缺少有效 open_id。")
                incoming_scopes = self._scopes_from_raw(raw.get("scopes"))
                profile = self.account_profile(open_id)
                expected_role = "admin" if profile["level"] in {"L0", "L1"} else "building"
                self.validate_identity_grant(open_id, expected_role, incoming_scopes)
                if not incoming_scopes:
                    raise PortalError(
                        f"{str(raw.get('name') or open_id)} 未配置可授权楼栋。"
                    )
                existing = users.get(open_id) if isinstance(users.get(open_id), dict) else {}
                existing_role = expected_role
                merged_scopes = list(profile["scope_rules"])
                next_user = {
                    **existing,
                    "name": str(
                        raw.get("name") or existing.get("name") or open_id
                    ).strip()[:80]
                    or open_id,
                    "role": "admin" if open_id in REQUIRED_ADMIN_USERS else existing_role,
                    "scopes": merged_scopes,
                    "enabled": True,
                    "denied_scopes": [scope for scope in existing.get("denied_scopes", []) if scope not in incoming_scopes],
                }
                if users.get(open_id) != next_user:
                    users[open_id] = next_user
                    changed_open_ids.append(open_id)
                results.append(
                    {
                        "open_id": open_id,
                        "name": next_user["name"],
                        "scopes": list(next_user["scopes"]),
                        "added_scopes": [
                            scope
                            for scope in incoming_scopes
                            if scope not in self._scopes_from_raw(existing.get("scopes"))
                        ],
                    }
                )
            payload["updated_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            payload["updated_by"] = str(updated_by or "")
            payload = self._ensure_required_admins_locked(payload)
            self._save_permissions_locked(payload)

        return (
            self.get_permissions_payload(),
            list(dict.fromkeys(changed_open_ids)),
            results,
        )

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
                profile = self.account_profile(open_id)
                if profile["level"] in {"L0", "L1"} and profile["can_manage_accounts"]:
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
        current_scopes = self.scopes_for_open_id(str(payload.get("open_id") or ""))
        return {
            "request_id": str(payload.get("request_id") or ""),
            "open_id": str(payload.get("open_id") or ""),
            "name": str(payload.get("name") or ""),
            "requested_scopes": scopes,
            "requested_scope_labels": [self.scope_label(scope) for scope in scopes],
            "current_scopes": current_scopes,
            "current_scope_labels": [self.scope_label(scope) for scope in current_scopes],
            "reason": str(payload.get("reason") or ""),
            "reject_reason": str(payload.get("reject_reason") or ""),
            "status": str(payload.get("status") or ""),
            "created_at": str(payload.get("created_at") or ""),
            "updated_at": str(payload.get("updated_at") or ""),
            "reviewed_at": str(payload.get("reviewed_at") or ""),
            "reviewed_by": str(payload.get("reviewed_by") or ""),
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
            if not isinstance(payload, dict):
                payload = self._state_store.get_latest_permission_request(
                    open_id,
                    statuses=("pending", "rejected"),
                )
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
        requested_scopes = self._permission_request_scopes_from_raw(scopes)
        current_scopes = self.scopes_for_open_id(open_id)
        if self._has_full_scope_access(current_scopes) or "ALL" in current_scopes:
            raise PortalError("当前账号已经拥有全部楼栋权限，无需继续申请。")
        if current_scopes:
            current_scope_set = set(current_scopes)
            requested_scopes = [
                scope for scope in requested_scopes if scope not in current_scope_set
            ]
        if not requested_scopes:
            raise PortalError("请选择尚未拥有权限的楼栋或园区。")
        reason = str(reason or "").strip()[:500]
        request_id = secrets.token_urlsafe(18)
        now_ts = time.time()
        expires_at_ts = now_ts + PERMISSION_REVIEW_TTL_SECONDS
        now = dt.datetime.now()
        expires_at = now + dt.timedelta(seconds=PERMISSION_REVIEW_TTL_SECONDS)
        payload = {
            "request_id": request_id,
            "open_id": open_id,
            "name": str(name or "飞书用户").strip()[:80] or "飞书用户",
            "requested_scopes": requested_scopes,
            "reason": reason,
            "status": "pending",
            "code_hash": "",
            "code_salt": "",
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
            self._state_store.mark_permission_requests_for_open_id(
                open_id,
                from_status="pending",
                to_status="superseded",
                exclude_request_id=request_id,
            )
            self._state_store.mark_permission_requests_for_open_id(
                open_id,
                from_status="notifying",
                to_status="superseded",
                exclude_request_id=request_id,
            )
        result = self._public_permission_request(payload)
        return result

    def list_permission_requests_for_admin(
        self, *, status: str = "pending", limit: int = 100
    ) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._state_store.list_permission_requests(status=status, limit=limit)
        return [self._public_permission_request(row) for row in rows]

    def approve_permission_request(
        self,
        request_id: str,
        *,
        scopes: Any = None,
        updated_by: str = "",
    ) -> dict[str, Any]:
        request_id = str(request_id or "").strip()
        if not request_id:
            raise PortalError("权限申请编号为空。")
        with self._lock:
            payload = self._state_store.get_permission_request(request_id)
            if not isinstance(payload, dict):
                raise PortalError("权限申请不存在或已失效。")
            if str(payload.get("status") or "") != "pending":
                raise PortalError("该权限申请已处理。")
            requested_scopes = self._permission_request_scopes_from_raw(scopes)
            if not requested_scopes:
                requested_scopes = self._permission_request_scopes_from_raw(
                    payload.get("requested_scopes")
                )
            if not requested_scopes:
                raise PortalError("审批楼栋为空。")
            open_id = str(payload.get("open_id") or "").strip()
            current_scopes = self.scopes_for_open_id(open_id)
            merged_scopes = self._scopes_from_raw([*current_scopes, *requested_scopes])
            now_ts = time.time()
            now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            payload["status"] = "approved"
            payload["approved_at"] = now
            payload["approved_at_ts"] = now_ts
            payload["reviewed_at"] = now
            payload["reviewed_at_ts"] = now_ts
            payload["reviewed_by"] = str(updated_by or "")
            payload["approved_scopes"] = requested_scopes
            payload["updated_at"] = now
            payload["updated_at_ts"] = now_ts
            permissions = self.upsert_permission_user(
                open_id=open_id,
                name=str(payload.get("name") or open_id),
                role="building",
                scopes=merged_scopes,
                enabled=True,
                updated_by=updated_by,
            )
            self._state_store.put_permission_request(payload)
        return {"request": self._public_permission_request(payload), "permissions": permissions}

    def reject_permission_request(
        self,
        request_id: str,
        *,
        reason: str = "",
        updated_by: str = "",
    ) -> dict[str, Any]:
        request_id = str(request_id or "").strip()
        if not request_id:
            raise PortalError("权限申请编号为空。")
        with self._lock:
            payload = self._state_store.get_permission_request(request_id)
            if not isinstance(payload, dict):
                raise PortalError("权限申请不存在或已失效。")
            if str(payload.get("status") or "") != "pending":
                raise PortalError("该权限申请已处理。")
            now_ts = time.time()
            now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            payload["status"] = "rejected"
            payload["reject_reason"] = str(reason or "管理员未通过该权限申请。").strip()[:500]
            payload["reviewed_at"] = now
            payload["reviewed_at_ts"] = now_ts
            payload["reviewed_by"] = str(updated_by or "")
            payload["updated_at"] = now
            payload["updated_at_ts"] = now_ts
            self._state_store.put_permission_request(payload)
        return self._public_permission_request(payload)

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
            current_scopes = self.scopes_for_open_id(open_id)
            merged_scopes = self._scopes_from_raw([*current_scopes, *requested_scopes])
            payload["status"] = "approved"
            payload["approved_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            payload["approved_at_ts"] = now_ts
            permissions = self.upsert_permission_user(
                open_id=open_id,
                name=str(payload.get("name") or open_id),
                role="building",
                scopes=merged_scopes,
                enabled=True,
                updated_by=updated_by,
            )
            self._state_store.put_permission_request(payload)
            return permissions

    def is_admin(self, session: dict[str, Any] | None) -> bool:
        if not isinstance(session, dict):
            return False
        identity = (session.get("user") or {}).get("open_id")
        if identity:
            profile = self.account_profile(identity)
            return profile["role"] == "admin"
        return False

    def session_scopes(self, session: dict[str, Any] | None) -> list[str]:
        if not isinstance(session, dict):
            return []
        scopes = self._scopes_from_raw(session.get("allowed_scopes"))
        if not is_cabinet_guest(session):
            identity = (session.get("user") or {}).get("open_id", "")
            if identity:
                return self.scopes_for_open_id(identity)
        return scopes

    def scope_allowed(self, session: dict[str, Any] | None, scope: str) -> bool:
        requested = self.normalize_scope(scope)
        scopes = self.session_scopes(session)
        allowed = ("ALL" in scopes or self._has_full_scope_access(scopes)) if requested == "ALL" else "ALL" in scopes or requested in scopes
        if not allowed and isinstance(session, dict):
            logging.warning("楼栋越权访问已拒绝: actor=%s scope=%s", (session.get("user") or {}).get("open_id", ""), requested)
        return allowed

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
        scopes = self.session_scopes(session)
        full_access = "ALL" in scopes or self._has_full_scope_access(scopes)
        return [
            copy.deepcopy(option)
            for option in options
            if (full_access or self.normalize_scope(option.get("value")) in scopes)
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
        provider = getattr(self, "_account_directory_provider", None)
        identity = ((session or {}).get("user") or {}).get("open_id", "")
        directory_missing = bool(logged_in and not is_cabinet_guest(session) and provider
                                 and identity not in REQUIRED_ADMIN_USERS and identity not in BUILDING_OPEN_ID_MAP.values()
                                 and not (provider() or {}).get("loaded_at"))
        directory_failed = bool(getattr(self, "_account_directory_refresh_failed", False))
        return {
            "configured": self.configured(),
            "logged_in": logged_in,
            "user": self.public_user(session) if logged_in else None,
            "allowed_scopes": self.session_scopes(session),
            "scope_options": scope_options,
            "directory_pending": directory_missing and not directory_failed,
            "directory_warning": "人员目录暂时无法读取，请稍后重试或联系管理员；未使用旧跨楼权限。" if directory_missing and directory_failed else "",
            "default_scope": self.default_scope(session),
            "login_url": f"/api/auth/login?{urlencode({'next': self._normalize_next_path(next_path)})}",
            "redirect_uri": str(redirect_uri or "").strip(),
        }

    def public_user(self, session: dict[str, Any] | None) -> dict[str, Any]:
        if not isinstance(session, dict):
            return {}
        user = dict(session.get("user") or {})
        open_id = str(user.get("open_id") or "")
        person = self.directory_person(open_id) if open_id not in BUILDING_OPEN_ID_MAP.values() else {}
        if person.get("selectable"):
            user.update(name=person.get("name") or user.get("name"), employee_no=person.get("employee_no", ""))
        return {
            "open_id": str(user.get("open_id") or ""),
            "union_id": str(user.get("union_id") or ""),
            "user_id": str(user.get("user_id") or ""),
            "name": str(user.get("name") or user.get("en_name") or "飞书用户"),
            "avatar_url": str(user.get("avatar_url") or user.get("avatar_thumb") or ""),
            "role": "guest" if is_cabinet_guest(session) else "admin" if self.is_admin(session) else "building",
            "login_method": "password" if session.get("source") == "personnel_password" else "feishu",
            **({"modules": ["cabinet"]} if is_cabinet_guest(session) else self.account_profile(open_id)),
            **({"personnel_record_id": user.get("personnel_record_id", "")} if session.get("source") == "personnel_password" else {}),
        }

    def create_guest_session(self) -> str:
        session_id = GUEST_SESSION_PREFIX + secrets.token_urlsafe(32)
        now = time.time()
        session = {
            "user": {"open_id": "guest_" + secrets.token_hex(16), "name": "机柜访客"},
            "role": "guest", "allowed_scopes": list(GUEST_SCOPES),
            "created_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "created_at_ts": now, "expires_at": now + AUTH_SESSION_TTL_SECONDS,
        }
        with self._lock:
            self._cleanup_expired_locked(now)
            self._sessions[session_id] = session
            self._trim_by_expiry_locked(self._sessions, AUTH_MAX_SESSIONS)
            self._state_store.put_auth_session(self._secret_hash(session_id), session)
        return session_id

    def invalidate_personnel_sessions(self, record_id: str) -> None:
        with self._lock:
            for session in self._sessions.values():
                if session.get("source") == "personnel_password" and (session.get("user") or {}).get("personnel_record_id") == record_id:
                    session["_last_personnel_check"] = 0

    def create_personnel_session(self, person: dict[str, Any]) -> str:
        identities = person.get("login_ids") or []
        if not person.get("selectable") or person.get("needs_setup") or not person.get("password_revision") or len(identities) != 1:
            raise PortalError("人员已停用或身份不明确，无法登录。")
        identity = identities[0]
        session_id = secrets.token_urlsafe(32)
        now = time.time()
        session = {"user": {"open_id": identity, "name": person["name"], "employee_no": person.get("employee_no", ""),
                            "personnel_record_id": person["id"]},
                   "source": "personnel_password",
                   "password_revision": person["password_revision"], "created_at_ts": now,
                   "created_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                   "expires_at": now + AUTH_SESSION_TTL_SECONDS}
        with self._lock:
            if self._open_id_explicitly_disabled(identity):
                raise PortalError("当前账号已停用，无法登录。")
            self.sync_directory_permissions([person], on_login=True)
            session["allowed_scopes"] = self.scopes_for_open_id(identity)
            session["role"] = self.role_for_open_id(identity)
            self._cleanup_expired_locked(now)
            self._sessions[session_id] = session
            self._trim_by_expiry_locked(self._sessions, AUTH_MAX_SESSIONS)
            self._state_store.put_auth_session(self._secret_hash(session_id), session)
            self._state_store.put_document("account_last_login", identity, {"at": now, "method": "password"})
        return session_id

    def start_login(self, *, redirect_uri: str, next_path: str = "/") -> str:
        if not self.configured():
            raise PortalError("未配置飞书 App ID 或 App Secret，无法启用扫码登录。")
        state = OAUTH_STATE_PREFIX + secrets.token_urlsafe(24)
        now = time.time()
        normalized_next = self._normalize_next_path(next_path)
        expires_at = now + AUTH_STATE_TTL_SECONDS
        with self._lock:
            self._cleanup_expired_locked(now)
            self._states[state] = {
                "redirect_uri": redirect_uri,
                "next": normalized_next,
                "expires_at": expires_at,
            }
            self._trim_by_expiry_locked(self._states, AUTH_MAX_PENDING_STATES)
            self._state_store.put_auth_oauth_state(
                self._secret_hash(state),
                redirect_uri=redirect_uri,
                next_path=normalized_next,
                expires_at=expires_at,
            )
        params = {
            "client_id": str(config.app_id or "").strip(),
            "response_type": "code",
            "redirect_uri": redirect_uri,
            "state": state,
        }
        return f"{FEISHU_AUTH_INDEX_URL}?{urlencode(params)}"

    def cancel_login(self, *, state: str, redirect_uri: str) -> str:
        state = str(state or "").strip()
        if not state:
            return "/"
        with self._lock:
            self._states.pop(state, None)
            payload = self._state_store.consume_auth_oauth_state(self._secret_hash(state), now=time.time())
        if not payload or payload.get("redirect_uri") != redirect_uri:
            return "/"
        return self._normalize_next_path(str(payload.get("next") or "/"))

    def complete_login(
        self, *, code: str, state: str, redirect_uri: str
    ) -> tuple[str, str]:
        code = str(code or "").strip()
        state = str(state or "").strip()
        if not code or not state:
            raise PortalError("飞书登录回调缺少 code 或 state。")
        now = time.time()
        with self._lock:
            self._cleanup_expired_locked(now)
            self._states.pop(state, None)
            state_payload = self._state_store.consume_auth_oauth_state(
                self._secret_hash(state),
                now=now,
            )
        if not state_payload:
            raise PortalAuthStateError("登录状态已失效，请重新扫码。")
        if bool(state_payload.get("expired")):
            raise PortalAuthStateError(
                "登录状态已过期，请重新扫码。",
                next_path=self._normalize_next_path(
                    str(state_payload.get("next") or "/")
                ),
            )
        expected_redirect = str(state_payload.get("redirect_uri") or "")
        if expected_redirect and expected_redirect != redirect_uri:
            raise PortalError("登录回调地址与发起地址不一致。")

        # Persisted pre-upgrade requests keep their original token exchange.
        user = self._exchange_login_code(code, redirect_uri=redirect_uri) if state.startswith(OAUTH_STATE_PREFIX) else self._exchange_login_code(code)
        open_id = str(user.get("open_id") or "").strip()
        if not open_id:
            raise PortalError("飞书登录未返回 open_id。")
        loader = getattr(self, "_account_directory_loader", None)
        if loader:
            directory_ready = False
            try:
                loader()
                directory_ready = True
            except Exception:
                # A directory outage must not drop existing verified Feishu permissions.
                logging.warning("Account directory refresh unavailable during Feishu login")
            person = self.directory_person(open_id)
            if self._open_id_explicitly_disabled(open_id):
                raise PortalError("当前账号已停用或人员身份待核对，请联系管理员。")
            if person and directory_ready:
                self.sync_directory_permissions([person], on_login=True)
        if user.get("union_id"):
            self._state_store.put_document("feishu_assistant_identity", user["union_id"], {
                key: user.get(key, "") for key in ("open_id", "union_id", "name", "employee_no")})
        allowed_scopes = self.scopes_for_open_id(open_id)
        role = self.role_for_open_id(open_id)
        session_id = secrets.token_urlsafe(32)
        now = time.time()
        session = {
            "user": user,
            "role": role,
            "allowed_scopes": allowed_scopes,
            "created_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "created_at_ts": now,
            "expires_at": now + AUTH_SESSION_TTL_SECONDS,
        }
        with self._lock:
            self._cleanup_expired_locked(now)
            self._sessions[session_id] = session
            self._trim_by_expiry_locked(self._sessions, AUTH_MAX_SESSIONS)
            self._state_store.put_auth_session(
                self._secret_hash(session_id),
                session,
            )
            self._state_store.put_document("account_last_login", open_id, {"at": now, "method": "feishu"})
        return session_id, self._normalize_next_path(str(state_payload.get("next") or "/"))

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        session_id = str(session_id or "").strip()
        if not session_id:
            return None
        with self._lock:
            now = time.time()
            self._cleanup_expired_locked(now)
            session = self._sessions.get(session_id)
            if not session:
                session = self._state_store.get_auth_session(
                    self._secret_hash(session_id),
                    now=now,
                )
                if not session:
                    return None
                session["_last_persisted_touch_at"] = now
                self._sessions[session_id] = session
            if now > float(session.get("expires_at") or 0):
                self._sessions.pop(session_id, None)
                self._state_store.revoke_auth_session(self._secret_hash(session_id))
                return None
            open_id = str((session.get("user") or {}).get("open_id") or "").strip()
            if is_cabinet_guest(session):
                session["allowed_scopes"] = list(GUEST_SCOPES)
            elif open_id:
                if self._open_id_explicitly_disabled(open_id):
                    self._sessions.pop(session_id, None)
                    self._state_store.revoke_auth_session(
                        self._secret_hash(session_id)
                    )
                    return None
                session["allowed_scopes"] = self.scopes_for_open_id(open_id)
                session["role"] = self.role_for_open_id(open_id)
            if session.get("source") == "personnel_password":
                from .personnel_password_login import DIRECTORY_NS, DIRECTORY_KEY
                rid = str((session.get("user") or {}).get("personnel_record_id") or "")
                valid = bool(open_id.startswith("ou_") and rid) and now < float(session.get("created_at_ts") or 0) + AUTH_SESSION_TTL_SECONDS
                if now - float(session.get("_last_personnel_check") or 0) >= 30:
                    snapshot = self._state_store.get_document(DIRECTORY_NS, DIRECTORY_KEY) or {}
                    person = (snapshot.get("people") or {}).get(rid) or {}
                    valid = valid and person.get("selectable") and person.get("password_revision") == session.get("password_revision") and person.get("login_ids") == [open_id]
                    session["_last_personnel_check"] = now
                if not valid:
                    self._sessions.pop(session_id, None)
                    self._state_store.revoke_auth_session(self._secret_hash(session_id))
                    return None
            session["expires_at"] = now + AUTH_SESSION_TTL_SECONDS
            if session.get("source") == "personnel_password":
                session["expires_at"] = min(session["expires_at"], float(session["created_at_ts"]) + AUTH_SESSION_TTL_SECONDS)
            if now - float(session.get("_last_persisted_touch_at") or 0) >= 300:
                session["_last_persisted_touch_at"] = now
                persisted = {
                    key: value
                    for key, value in session.items()
                    if not str(key).startswith("_")
                }
                self._state_store.touch_auth_session(
                    self._secret_hash(session_id),
                    persisted,
                )
            return copy.deepcopy(
                {
                    key: value
                    for key, value in session.items()
                    if not str(key).startswith("_")
                }
            )

    def clear_session(self, session_id: str) -> None:
        session_id = str(session_id or "").strip()
        with self._lock:
            self._sessions.pop(session_id, None)
            if session_id:
                self._state_store.revoke_auth_session(
                    self._secret_hash(session_id)
                )

    def _get_app_access_token(self) -> str:
        try:
            return token_manager.get_app_access_token()
        except Exception as exc:
            raise PortalError(f"获取飞书 app_access_token 失败: {exc}") from exc

    def _exchange_login_code(self, code: str, *, redirect_uri: str = "") -> dict[str, Any]:
        try:
            if redirect_uri:
                return token_manager.exchange_oauth_login_code(code, redirect_uri=redirect_uri)
            return token_manager.exchange_login_code(code)
        except Exception as exc:
            raise PortalError(f"获取飞书用户身份失败: {exc}") from exc

    def _fetch_user_info(self, user_access_token: str) -> dict[str, Any]:
        return token_manager.fetch_user_info(user_access_token)

# -*- coding: utf-8 -*-
"""Isolated regression tests for the Feishu login callback (cancel + success paths)."""
from __future__ import annotations

import sys
import time
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlencode, urlsplit

from starlette.requests import Request

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.portal_auth import (  # noqa: E402
    AUTH_COOKIE_NAME, AUTH_RETRY_COOKIE_NAME, AUTH_SESSION_TTL_SECONDS,
    BUILDING_OPEN_ID_MAP, LI_SHILONG_OPEN_ID, REQUIRED_ADMIN_USERS,
    PortalAuthManager, PortalAuthStateError,
)
from clipflow_backend.main import FastAPIPortalController  # noqa: E402
from test_portal_auth_persistence import auth_test_directory  # noqa: E402

REDIRECT_BASE = "http://portal.test"
CALLBACK_URI = f"{REDIRECT_BASE}/api/auth/feishu/callback"


def _shutdown_manager(manager: PortalAuthManager) -> None:
    # Wait on background workers / SQLite write thread so the temp store can be
    # removed; shutdown failures propagate instead of being suppressed.
    manager.shutdown()
    manager._cleanup_executor.shutdown(wait=True, cancel_futures=True)
    manager._state_store.shutdown_write_worker()


@contextmanager
def _isolated_manager():
    """Temp auth manager context; ``make()`` creates and tracks managers."""
    managers: list[PortalAuthManager] = []
    with auth_test_directory() as tmp:
        root = Path(tmp)
        with patch(
            "lan_bitable_template_portal.portal_auth.get_data_file_path",
            side_effect=lambda name: str(root / name),
        ):
            def make() -> PortalAuthManager:
                manager = PortalAuthManager()
                managers.append(manager)
                return manager

            try:
                yield make
            finally:
                for manager in managers:
                    _shutdown_manager(manager)


def _seed_oauth_state(manager, *, state, next_path, redirect_uri=CALLBACK_URI, expires_at=None):
    manager._state_store.put_auth_oauth_state(
        manager._secret_hash(state),
        redirect_uri=redirect_uri,
        next_path=next_path,
        expires_at=expires_at if expires_at is not None else time.time() + 600,
    )


def _seed_session(manager, session_id, open_id, name, role="building", scopes=("D",)):
    manager._state_store.put_auth_session(
        manager._secret_hash(session_id),
        {"user": {"open_id": open_id, "name": name}, "role": role,
         "allowed_scopes": list(scopes), "expires_at": time.time() + AUTH_SESSION_TTL_SECONDS},
    )


def _no_directory(manager) -> None:
    manager._account_directory_provider = lambda: {"people": {}, "removed_identities": {}}
    manager._account_directory_loader = lambda: None


def _request(params=None, cookies=None) -> Request:
    headers = [(b"host", b"portal.test")]
    if cookies:
        headers.append((b"cookie", "; ".join(f"{k}={v}" for k, v in cookies.items()).encode()))
    return Request({"type": "http", "scheme": "http", "server": ("portal.test", 80),
                    "path": "/api/auth/feishu/callback", "root_path": "",
                    "query_string": urlencode(params or {}).encode(), "headers": headers})


class _ControllerStub:
    def __init__(self):
        self._request_base_url_calls: list = []
        self._clear_read_cache_calls: list[tuple] = []
        self._base_url = REDIRECT_BASE

    def _request_base_url(self, request) -> str:
        self._request_base_url_calls.append(request)
        return self._base_url

    def _clear_read_cache(self, keys) -> None:
        self._clear_read_cache_calls.append((keys,))

    @staticmethod
    def _auth_failure_page(message):
        return FastAPIPortalController._auth_failure_page(message)


def _callback(manager, params, cookies=None):
    stub = _ControllerStub()
    with patch("clipflow_backend.main.PortalRuntime.auth_manager", manager):
        response = FastAPIPortalController._complete_oauth_callback_response(stub, _request(params, cookies))
    return response, stub


def _location_parts(location):
    parts = urlsplit(location)
    return parts.path, parse_qs(parts.query)


def _session_id(response) -> str:
    cookie = next(h for h in response.headers.getlist("set-cookie") if AUTH_COOKIE_NAME in h)
    return cookie.split("=", 1)[1].split(";", 1)[0]


class PortalAuthCancelLoginTests(unittest.TestCase):
    def test_cancel_login_returns_sanitized_original_and_consumes_once(self):
        with _isolated_manager() as make:
            m = make()
            state, original = "cancellable-state-abc", "/engineer/mop?scope=E&work_type=maintenance"
            _seed_oauth_state(m, state=state, next_path=original)
            self.assertEqual(m.cancel_login(state=state, redirect_uri=CALLBACK_URI), original)
            self.assertEqual(m.cancel_login(state=state, redirect_uri=CALLBACK_URI), "/")
            with self.assertRaises(PortalAuthStateError):
                m.complete_login(code="stale-code", state=state, redirect_uri=CALLBACK_URI)

    def test_cancel_login_unwraps_login_entry_to_original_destination(self):
        with _isolated_manager() as make:
            m = make()
            nested = "/workbench-lite?scope=A#today"
            _seed_oauth_state(m, state="unwrapped-state",
                              next_path="/?%s" % urlencode({"login": "feishu", "next": nested}))
            self.assertEqual(m.cancel_login(state="unwrapped-state", redirect_uri=CALLBACK_URI), nested)

    def test_cancel_login_rejects_external_backslash_control_and_api(self):
        with _isolated_manager() as make:
            m = make()
            hostile = ["https://outside.test/steal", "//outside.test/path", r"/\\outside.test",
                       "/\t/outside.test", "/\n/outside.test", "/\x7f/outside.test",
                       "/api/auth/logout", "/api/auth/login"]
            for value in hostile:
                with self.subTest(value=value):
                    state = "hostile-" + str(abs(hash(value)))
                    _seed_oauth_state(m, state=state, next_path=value)
                    self.assertEqual(m.cancel_login(state=state, redirect_uri=CALLBACK_URI), "/",
                                     f"expected {value!r} to be rejected")

    def test_cancel_login_missing_or_unknown_state_returns_slash(self):
        with _isolated_manager() as make:
            m = make()
            self.assertEqual(m.cancel_login(state="", redirect_uri=CALLBACK_URI), "/")
            self.assertEqual(m.cancel_login(state="never-seeded", redirect_uri=CALLBACK_URI), "/")

    def test_cancel_login_mismatched_redirect_returns_slash_without_identity(self):
        with _isolated_manager() as make:
            m = make()
            _seed_oauth_state(m, state="mismatched-redirect", next_path="/workbench-lite?scope=E")
            m._exchange_login_code = Mock(side_effect=AssertionError("must not exchange identity"))
            before = m._load_permissions_locked()
            self.assertEqual(m.cancel_login(state="mismatched-redirect", redirect_uri="http://evil.example/callback"), "/")
            self.assertFalse(m._sessions, "no session may be created")
            m._exchange_login_code.assert_not_called()
            self.assertEqual(m._load_permissions_locked(), before)

    def test_cancel_login_survives_restart_and_keeps_existing_sessions(self):
        with _isolated_manager() as make:
            m = make()
            _seed_oauth_state(m, state="restart-safe-cancel", next_path="/engineer/mop?scope=E")
            _seed_session(m, "existing-session-cancel", "ou_existing_cancel", "存量会话用户")
            restarted = make()
            self.assertEqual(restarted.cancel_login(state="restart-safe-cancel", redirect_uri=CALLBACK_URI),
                             "/engineer/mop?scope=E")
            session = restarted.get_session("existing-session-cancel")
            self.assertIsNotNone(session)
            self.assertEqual(session["user"]["open_id"], "ou_existing_cancel")

    def test_cancel_login_does_not_touch_grants_or_histories(self):
        with _isolated_manager() as make:
            m = make()
            _seed_oauth_state(m, state="no-grant-change", next_path="/learning")
            m._state_store.put_document("fixture_history", "history", {"keep": True})
            before_perms = m._load_permissions_locked()
            before_history = m._state_store.get_document("fixture_history", "history")
            m.cancel_login(state="no-grant-change", redirect_uri=CALLBACK_URI)
            self.assertEqual(m._load_permissions_locked(), before_perms)
            self.assertEqual(m._state_store.get_document("fixture_history", "history"), before_history)
            self.assertFalse(m._sessions, "no session may be created")


class PortalAuthCallbackDenialTests(unittest.TestCase):
    def test_error_query_returns_302_cancelled_with_original_destination(self):
        with _isolated_manager() as make:
            m = make()
            original = "/engineer/mop?scope=E&work_type=maintenance"
            _seed_oauth_state(m, state="denial-state", next_path=original)
            response, stub = _callback(m, {"error": "access_denied", "state": "denial-state", "code": "ignored"})
            self.assertEqual(response.status_code, 302)
            self.assertEqual(len(stub._request_base_url_calls), 1)
            path, params = _location_parts(response.headers["location"])
            self.assertEqual(path, "/")
            self.assertEqual(params.get("login"), ["feishu"])
            self.assertEqual(params.get("auth_notice"), ["cancelled"])
            self.assertEqual(params.get("next"), [original])
            self.assertFalse(m._sessions)

    def test_missing_code_is_denial_with_cancel_login(self):
        with _isolated_manager() as make:
            m = make()
            _seed_oauth_state(m, state="missing-code-state", next_path="/workbench-lite?scope=A")
            m.complete_login = Mock()
            m.start_login = Mock()
            response, _stub = _callback(m, {"state": "missing-code-state"})
            self.assertEqual(response.status_code, 302)
            _, params = _location_parts(response.headers["location"])
            self.assertEqual(params.get("auth_notice"), ["cancelled"])
            self.assertEqual(params.get("next"), ["/workbench-lite?scope=A"])
            m.complete_login.assert_not_called()
            m.start_login.assert_not_called()

    def test_denial_uses_request_base_url_for_cancel_redirect_uri(self):
        with _isolated_manager() as make:
            m = make()
            _seed_oauth_state(m, state="base-url-state", next_path="/learning?scope=C")
            with patch.object(m, "cancel_login", wraps=m.cancel_login) as spy:
                with patch("clipflow_backend.main.PortalRuntime.auth_manager", m):
                    stub = _ControllerStub()
                    FastAPIPortalController._complete_oauth_callback_response(
                        stub, _request({"error": "access_denied", "state": "base-url-state"}))
            spy.assert_called_once()
            self.assertEqual(spy.call_args.kwargs["state"], "base-url-state")
            self.assertEqual(spy.call_args.kwargs["redirect_uri"], CALLBACK_URI)

    def test_denial_clears_auth_retry_cookie_only(self):
        with _isolated_manager() as make:
            m = make()
            _seed_oauth_state(m, state="retry-cookie-state", next_path="/engineer/mop?scope=E")
            _seed_session(m, "valid-login-session", "ou_logged_in", "已登录用户")
            cookies = {AUTH_COOKIE_NAME: "valid-login-session", AUTH_RETRY_COOKIE_NAME: "1"}
            response, _stub = _callback(m, {"error": "access_denied", "state": "retry-cookie-state"}, cookies=cookies)
            set_cookies = response.headers.getlist("set-cookie")
            self.assertEqual(len(set_cookies), 1, set_cookies)
            retry_header = next(h for h in set_cookies if AUTH_RETRY_COOKIE_NAME in h)
            self.assertIn("Max-Age=0", retry_header)
            self.assertFalse(any(AUTH_COOKIE_NAME in h and "Max-Age=0" in h for h in set_cookies))
            session = m.get_session("valid-login-session")
            self.assertIsNotNone(session)
            self.assertEqual(session["user"]["open_id"], "ou_logged_in")

    def test_denial_does_not_reflect_error_description(self):
        with _isolated_manager() as make:
            m = make()
            _seed_oauth_state(m, state="no-leak-state", next_path="/")
            payload = "<script>fixture</script>evil"
            response, _stub = _callback(m, {"error": "access_denied", "state": "no-leak-state", "error_description": payload})
            location = response.headers.get("location", "")
            self.assertNotIn(payload, location)
            self.assertNotIn("script", location.lower())
            self.assertEqual(response.body, b"")

    def test_consumed_or_malformed_state_denial_uses_root_destination(self):
        with _isolated_manager() as make:
            m = make()
            _seed_oauth_state(m, state="consumed-again", next_path="/learning?scope=A")
            self.assertEqual(m.cancel_login(state="consumed-again", redirect_uri=CALLBACK_URI), "/learning?scope=A")
            response, _stub = _callback(m, {"error": "access_denied", "state": "consumed-again"})
            self.assertEqual(response.status_code, 302)
            _, params = _location_parts(response.headers["location"])
            self.assertEqual(params.get("next"), ["/"])
            response2, _stub2 = _callback(m, {"error": "access_denied", "state": "never-seeded"})
            _, params2 = _location_parts(response2.headers["location"])
            self.assertEqual(params2.get("next"), ["/"])

    def test_reused_state_with_retry_cookie_is_bounded_not_infinite(self):
        with _isolated_manager() as make:
            m = make()
            _seed_oauth_state(m, state="bounded-retry-state", next_path="/learning?scope=A")
            m.cancel_login(state="bounded-retry-state", redirect_uri=CALLBACK_URI)
            with patch.object(m, "start_login") as start_login:
                response, _stub = _callback(m, {"code": "stale-code", "state": "bounded-retry-state"},
                                            cookies={AUTH_RETRY_COOKIE_NAME: "1"})
            self.assertEqual(response.status_code, 400)
            self.assertIn("登录状态已失效", response.body.decode("utf-8", "ignore"))
            start_login.assert_not_called()


class PortalAuthCallbackSuccessTests(unittest.TestCase):
    def test_valid_callback_success_path_unchanged(self):
        with _isolated_manager() as make:
            m = make()
            identity = {"open_id": "ou_success_user", "name": "提供方身份"}
            _seed_oauth_state(m, state="success-state", next_path="/engineer/mop?scope=E")
            m._exchange_login_code = Mock(return_value=identity)
            response, _stub = _callback(m, {"code": "valid-code", "state": "success-state"})
            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.headers["location"], "/engineer/mop?scope=E")
            session = m.get_session(_session_id(response))
            self.assertIsNotNone(session)
            self.assertEqual(session["user"]["open_id"], identity["open_id"])
            self.assertEqual(session["user"]["name"], identity["name"])
            retry_header = next(h for h in response.headers.getlist("set-cookie") if AUTH_RETRY_COOKIE_NAME in h)
            self.assertIn("Max-Age=0", retry_header)

    def test_new_user_without_directory_succeeds_with_empty_permissions(self):
        with _isolated_manager() as make:
            m = make()
            _no_directory(m)
            self.assertEqual(m.scopes_for_open_id("ou_brand_new"), [])
            self.assertEqual(m.role_for_open_id("ou_brand_new"), "building")
            _seed_oauth_state(m, state="new-user-state", next_path="/learning?scope=A")
            m._exchange_login_code = Mock(return_value={"open_id": "ou_brand_new", "name": "新用户"})
            response, _stub = _callback(m, {"code": "valid-code", "state": "new-user-state"})
            self.assertEqual(response.status_code, 302)
            session = m.get_session(_session_id(response))
            self.assertEqual(session["user"]["open_id"], "ou_brand_new")
            self.assertEqual(session["allowed_scopes"], [])
            self.assertEqual(session["role"], "building")

    def test_browser_submitted_privileges_cannot_grant(self):
        with _isolated_manager() as make:
            m = make()
            _no_directory(m)
            _seed_oauth_state(m, state="grant-attack-state", next_path="/learning?scope=A")
            m._exchange_login_code = Mock(return_value={"open_id": "ou_browser_attacker", "name": "真实提供方姓名"})
            response, _stub = _callback(m, {"code": "valid-code", "state": "grant-attack-state",
                                            "role": "admin", "name": "攻击者", "open_id": "ou_hacker",
                                            "scopes": "ALL", "scope": "CAMPUS"})
            self.assertEqual(response.status_code, 302)
            session = m.get_session(_session_id(response))
            self.assertEqual(session["user"]["open_id"], "ou_browser_attacker")
            self.assertEqual(session["user"]["name"], "真实提供方姓名")
            self.assertEqual(session["role"], "building")
            self.assertEqual(session["allowed_scopes"], [])
            payload = m._load_permissions_locked()
            self.assertNotIn("ou_hacker", payload.get("users", {}))
            for required in REQUIRED_ADMIN_USERS:
                self.assertEqual(payload["users"][required]["role"], "admin", f"fixed admin {required} must stay admin")

    def test_vnet_external_account_metadata_ignored_for_browser_grants(self):
        # Browser metadata must not replace either provider identity or permissions.
        cases = [
            ("ou_vnet_external", "VNET"),
            ("ou_external_browser", "外部账号"),
        ]
        for provider_open_id, nature in cases:
            with self.subTest(account_nature=nature, provider_open_id=provider_open_id):
                with _isolated_manager() as make:
                    m = make()
                    _no_directory(m)
                    state = f"vnet-state-{nature}"
                    _seed_oauth_state(m, state=state, next_path="/")
                    m._exchange_login_code = Mock(
                        return_value={"open_id": provider_open_id, "name": "提供方姓名"})
                    response, _stub = _callback(m, {
                        "code": "valid-code", "state": state,
                        "role": "admin", "scope": "ALL", "name": "伪装管理员",
                        "open_id": "ou_pretend_admin", "account_nature": nature,
                    })
                    self.assertEqual(response.status_code, 302)
                    session = m.get_session(_session_id(response))
                    self.assertEqual(session["user"]["open_id"], provider_open_id)
                    self.assertEqual(session["user"]["name"], "提供方姓名")
                    self.assertEqual(session["role"], "building")
                    self.assertEqual(session["allowed_scopes"], [])
                    self.assertNotIn("ou_pretend_admin",
                                     m._load_permissions_locked().get("users", {}))

    def test_callback_preserves_fixed_admin_and_duty_and_old_history(self):
        with _isolated_manager() as make:
            m = make()
            _no_directory(m)
            m._state_store.put_document("fixture_history", "history", {"keep": True})
            before = m._load_permissions_locked()
            for _code, duty_oid in BUILDING_OPEN_ID_MAP.items():
                self.assertIn(duty_oid, before["users"])
            _seed_oauth_state(m, state="preserve-state", next_path="/")
            m._exchange_login_code = Mock(return_value={"open_id": "ou_new_duty_person", "name": "新人员"})
            _callback(m, {"code": "valid-code", "state": "preserve-state"})
            after = m._load_permissions_locked()
            for code, duty_oid in BUILDING_OPEN_ID_MAP.items():
                self.assertEqual(after["users"][duty_oid], before["users"][duty_oid],
                                 f"duty account {code} must be preserved")
            for admin_oid in REQUIRED_ADMIN_USERS:
                self.assertEqual(after["users"][admin_oid]["role"], "admin")
                self.assertEqual(after["users"][admin_oid], before["users"][admin_oid],
                                 f"fixed admin {admin_oid} must be preserved")
            self.assertEqual(m._state_store.get_document("fixture_history", "history"), {"keep": True})
            self.assertEqual(m.scopes_for_open_id(LI_SHILONG_OPEN_ID), m._admin_scopes())


if __name__ == "__main__":
    unittest.main()

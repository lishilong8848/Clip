import tempfile
import time
import os
from pathlib import Path
from unittest.mock import patch
from fastapi.testclient import TestClient
from lan_bitable_template_portal.portal_auth import PortalAuthManager, LI_SHILONG_OPEN_ID, AUTH_COOKIE_NAME
from lan_bitable_template_portal.server import PortalRuntime
from clipflow_backend.main import FastAPIPortalController
from test_lan_template_work_status import LanTemplateWorkStatusTests

t = LanTemplateWorkStatusTests(methodName="runTest")


class _LookupService:
    def __init__(self):
        self.calls = []

    def lookup_notice_target_candidates(self, **kwargs):
        self.calls.append(dict(kwargs))
        return {"candidates": []}


previous_service = PortalRuntime.service
previous_manager = PortalRuntime.auth_manager
service = _LookupService()
PortalRuntime.service = service
with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)

    def fake_data_path(name):
        return str(root / name)

    with patch("lan_bitable_template_portal.portal_auth.get_data_file_path", side_effect=fake_data_path):
        manager = PortalAuthManager()
        t._attach_portal_hr(manager, {
            "candidate-building": t._person(
                "candidate-building", "candidate-building", "E楼", ["E"], "暖通工程师"),
        })
        manager.upsert_permission_user(
            open_id="candidate-building", name="E楼", role="building",
            scopes=["E"], enabled=True, updated_by="test",
        )
        portal_manager = manager
        with portal_manager._lock:
            portal_manager._sessions = {
                "candidate-admin": {
                    "session_id": "candidate-admin",
                    "user": {"open_id": LI_SHILONG_OPEN_ID, "name": "管理员"},
                    "role": "admin",
                    "allowed_scopes": portal_manager._admin_scopes(),
                    "expires_at": time.time() + 3600,
                },
                "candidate-building": {
                    "session_id": "candidate-building",
                    "user": {"open_id": "candidate-building", "name": "E楼"},
                    "role": "building",
                    "allowed_scopes": ["E"],
                    "expires_at": time.time() + 3600,
                },
            }
    PortalRuntime.auth_manager = portal_manager
    print("LI scopes:", portal_manager.scopes_for_open_id(LI_SHILONG_OPEN_ID))
    print("building scopes:", portal_manager.scopes_for_open_id("candidate-building"))
    built = portal_manager.get_session("candidate-building")
    print("building session:", built)
    print("building scope_allowed E:", portal_manager.scope_allowed(built, "E"))
    print("building session_scopes:", portal_manager.session_scopes(built))
    client = TestClient(FastAPIPortalController(host="127.0.0.1", port=18766)._build_app())
    orig_scope_allowed = portal_manager.scope_allowed
    def traced_scope_allowed(session, scope):
        res = orig_scope_allowed(session, scope)
        ident = ((session or {}).get("user") or {}).get("open_id")
        print("TRACE scope_allowed", ident, scope, "->", res, "session_scopes:", portal_manager.session_scopes(session) if session else None)
        return res
    portal_manager.scope_allowed = traced_scope_allowed
    for i, (session_id, context, expected_scope) in enumerate((
        ("candidate-admin", "target_table", "ALL"),
        ("candidate-admin", "planned_target_table", "E"),
        ("candidate-admin", "ongoing_list", "E"),
        ("candidate-building", "target_table", "E"),
    )):
        if i >= 3:
            print("BEFORE BUILDING: scopes_for_open_id:", portal_manager.scopes_for_open_id("candidate-building"))
        response = client.post(
            "/api/notice-target-candidates",
            json={"scope": "E", "work_type": "change", "lookup_context": context},
            headers={"Cookie": f"{AUTH_COOKIE_NAME}={session_id}"},
        )
        print("ITER", session_id, context, "->", response.status_code, response.text[:120])
        if response.status_code == 200 and service.calls:
            print("   scope:", service.calls[-1]["scope"])
PortalRuntime.auth_manager = previous_manager
PortalRuntime.service = previous_service
os._exit(0)
"""Account/device preferences, isolated SQLite and HTTP; no external service."""
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from lan_bitable_template_portal.state_store import LanPortalStateStore
from lan_bitable_template_portal.workspace_layout_routes import install_workspace_layout_routes, validate_layout
from lan_bitable_template_portal.portal_service import PortalConflictError


def layout(title="原布局"):
    return {"version": 1, "widgets": [{"id": "one", "kind": "tasks", "title": title, "x": 0, "y": 0, "w": 6, "h": 5}]}


class WorkspaceLayoutTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "state.sqlite3"
        self.store = LanPortalStateStore(db_path=self.path)
        self.session = {"user": {"open_id": "alice", "name": "同名"}, "role": "building"}
        self.controller = SimpleNamespace(_current_session=lambda request: self.session,
            _auth_required_response=lambda: JSONResponse({"ok": False}, status_code=401),
            _request_base_url=lambda request: str(request.base_url).rstrip("/"))
        self.runtime = SimpleNamespace(state_store=self.store)
        self.client = self.client_for_app()
        self.addCleanup(self.client.close)
        self.url = "/api/workspace-layouts/desktop/workspace"

    def client_for_app(self):
        app = FastAPI()
        install_workspace_layout_routes(app, self.controller, self.runtime)
        return TestClient(app, headers={"Origin": "http://testserver"})

    def put(self, value=None, revision=0, **kwargs):
        return self.client.put(self.url, json={"revision": revision, "value": value or layout()}, **kwargs)

    def test_other_clients_and_restart_reuse_account_layout(self):
        self.assertEqual(self.client.get(self.url).json()["data"], {"revision": 0, "value": None})
        self.assertEqual(self.put().status_code, 200)
        self.runtime.state_store = LanPortalStateStore(db_path=self.path)
        with self.client_for_app() as other:
            response = other.get(self.url)
            self.assertEqual(response.json()["data"]["value"], layout())
            self.assertIn("no-store", response.headers["cache-control"])

    def test_person_device_page_are_separate(self):
        self.put()
        self.session["user"] = {"open_id": "bob", "name": "同名"}
        self.assertIsNone(self.client.get(self.url).json()["data"]["value"])
        self.put(layout("乙"))
        self.session["user"]["open_id"] = "alice"
        self.assertEqual(self.client.get(self.url).json()["data"]["value"], layout())
        self.assertIsNone(self.client.get(self.url.replace("desktop", "mobile")).json()["data"]["value"])
        mobile = self.url.replace("desktop", "mobile")
        self.client.put(mobile, json={"revision": 0, "value": layout("手机")})
        self.assertEqual(self.client.get(self.url).json()["data"]["value"], layout())
        self.assertEqual(self.client.get(mobile).json()["data"]["value"], layout("手机"))
        self.assertIsNone(self.client.get("/api/workspace-layouts/desktop/sops").json()["data"]["value"])

    def test_password_and_feishu_sessions_of_same_identity_share_layout(self):
        self.put()
        self.session.update(source="personnel_password", user={"open_id": "alice", "personnel_record_id": "rec-person"})
        self.assertEqual(self.client.get(self.url).json()["data"]["value"], layout())

    def test_missing_session_and_guests_never_write(self):
        for session, status in [(None, 401), ({"user": {"open_id": "a"}, "role": "guest"}, 403),
                                ({"user": {"open_id": "a"}, "role": "temporary"}, 403), ({"user": {}}, 403)]:
            self.session = session
            self.assertEqual(self.client.get(self.url).status_code, status)
            self.assertEqual(self.put().status_code, status)

    def test_cross_origin_identity_injection_and_oversize_rejected(self):
        for origin in ["", "https://foreign.test"]:
            self.assertEqual(self.put(headers={"Origin": origin}).status_code, 403)
        self.assertEqual(self.put(headers={"sec-fetch-site": "cross-site"}).status_code, 403)
        self.assertEqual(self.client.put(self.url, content=b"x" * 32769).status_code, 413)
        for payload in [{"revision": 0, "value": layout(), "owner": "bob"}, {"revision": True, "value": layout()}, {"revision": -1, "value": layout()}, []]:
            self.assertEqual(self.client.put(self.url, json=payload).status_code, 400)
        self.assertEqual(self.client.get(self.url + "?owner=bob").status_code, 400)
        for path in ["desktop/unknown", "watch/workspace"]:
            self.assertEqual(self.client.get("/api/workspace-layouts/" + path).status_code, 400)
        self.assertEqual(self.store.get_workspace_layout("alice", "desktop", "workspace")["revision"], 0)

    def test_schema_validation(self):
        bad = [None, {}, {**layout(), "secret": "not allowed"}, {"version": True, "widgets": []},
               {"version": 1, "widgets": layout()["widgets"] * 17}]
        for field, value in [("id", "../../a"), ("x", 12), ("w", 13), ("y", -1), ("h", True),
                             ("builtin", 1), ("links", ["https://bad.test"]), ("url", "bad"), ("title", {})]:
            row = layout(); row["widgets"][0][field] = value; bad.append(row)
        for value in bad:
            with self.subTest(value=value):
                self.assertEqual(self.client.put(self.url, json={"revision": 0, "value": value}).status_code, 400)
        page = {"layout": {"version": 1, "items": layout()["widgets"]}, "presentations": {"one": "compact"}}
        self.assertEqual(validate_layout("sops", page), page)
        for presentations in [{"one": {}}, {"missing": "default"}]:
            with self.assertRaises(ValueError): validate_layout("sops", {**page, "presentations": presentations})

    def test_conflict_and_lost_response_retry(self):
        saved = self.put().json()["data"]
        self.assertEqual(self.put().json()["data"], saved)
        self.assertEqual(self.put(layout("stale"), 0).status_code, 409)
        self.assertEqual(self.put(layout("new"), 1).json()["data"]["revision"], 2)
        self.assertEqual(self.put().status_code, 409)

    def test_two_store_instances_use_atomic_compare_and_swap(self):
        self.put()
        other = LanPortalStateStore(db_path=self.path)
        def update(store, title):
            try:
                store.save_workspace_layout("alice", "desktop", "workspace", layout(title), 1)
                return "saved"
            except PortalConflictError:
                return "conflict"
        with ThreadPoolExecutor(2) as pool:
            a = pool.submit(update, self.store, "a")
            b = pool.submit(update, other, "b")
            self.assertCountEqual([a.result(), b.result()], ["saved", "conflict"])

    def test_failure_is_not_success_and_empty_layout_is_preserved(self):
        with self.assertLogs(level="ERROR"), patch.object(self.store, "save_workspace_layout", side_effect=OSError("disk full")):
            self.assertEqual(self.put().status_code, 503)
        empty = {"version": 1, "widgets": []}
        self.assertEqual(self.put(empty).json()["data"]["value"], empty)
        with self.assertLogs(level="ERROR"), patch.object(self.store, "get_workspace_layout", side_effect=OSError("disk busy")):
            self.assertEqual(self.client.get(self.url).status_code, 503)


if __name__ == "__main__":
    unittest.main()

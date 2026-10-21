"""Admin-only history routes tested with synthetic entries and local ASGI requests."""
import hashlib
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

BIN = Path(__file__).resolve().parent
if str(BIN) not in sys.path:
    sys.path.insert(0, str(BIN))

from fastapi import FastAPI
from fastapi.testclient import TestClient
from lan_bitable_template_portal.release_journal import ReleaseJournalStore
from lan_bitable_template_portal.release_journal_routes import install_release_journal_routes
from upload_event_module.services.release_journal import write_json


class ReleaseJournalRouteTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="release-journal-routes-")
        self.addCleanup(directory.cleanup)
        self.store = ReleaseJournalStore(directory.name)
        self.entries = [self.entry(number) for number in range(1, 25)]
        write_json(self.store.path, {"schema": 1, "entries": self.entries})
        self.session = {"is_admin": True}
        controller = SimpleNamespace(_current_session=lambda request: self.session)
        self.admin = Mock(side_effect=lambda session: session.get("is_admin", False))
        runtime = SimpleNamespace(auth_manager=SimpleNamespace(is_admin=self.admin))
        app = FastAPI()
        install_release_journal_routes(app, controller, runtime, store=self.store)
        self.client = TestClient(app)
        self.addCleanup(self.client.close)

    def entry(self, number):
        return {"id": hashlib.sha256(str(number).encode()).hexdigest(), "version": "ClipFlow_fixture_" + str(number),
                "patch": number, "published_at": "2026-10-20T12:00:00+00:00", "commits": [],
                "items": [{"problem": "fixture issue " + str(number), "solution": "fixture correction",
                           "module": "transport" if number % 2 else "assistant"}]}

    def test_anonymous_and_non_admin_are_denied_before_history_read(self):
        self.store.entries = Mock(side_effect=AssertionError("history must not be read"))
        self.session = None
        response = self.client.get("/api/release-journal")
        self.assertEqual(response.status_code, 401)
        self.session = {"role": "admin", "is_admin": False}
        response = self.client.get("/api/release-journal/" + self.entries[0]["id"])
        self.assertEqual(response.status_code, 403)
        self.admin.assert_called_once_with(self.session)
        self.store.entries.assert_not_called()

    def test_guest_cannot_use_admin_flag_to_read_history(self):
        for session in ({"is_admin": True, "is_guest": True}, {"is_admin": True, "role": "guest"}):
            self.session = session
            self.assertEqual(self.client.get("/api/release-journal").status_code, 403)

    def test_list_search_modules_pagination_and_private_cache(self):
        response = self.client.get("/api/release-journal")
        self.assertEqual(response.status_code, 200)
        self.assertIn("no-store", response.headers["cache-control"])
        self.assertEqual(response.headers["vary"], "Cookie")
        value = response.json()["data"]
        self.assertEqual((value["total"], value["page"], value["page_size"]), (24, 1, 20))
        self.assertEqual(len(value["items"]), 20)
        self.assertEqual(value["modules"], ["assistant", "transport"])
        second = self.client.get("/api/release-journal?page=2").json()["data"]
        self.assertEqual(len(second["items"]), 4)
        self.assertFalse({e["id"] for e in value["items"]} & {e["id"] for e in second["items"]})
        matched = self.client.get("/api/release-journal", params={"q": "ClipFlow_fixture_24", "module": "assistant"}).json()["data"]
        self.assertEqual(matched["total"], 1)
        self.assertEqual(matched["items"][0]["patch"], 24)
        self.assertEqual(self.client.get("/api/release-journal?module=transport").json()["data"]["total"], 12)

    def test_detail_unknown_and_no_static_or_write_endpoint(self):
        response = self.client.get("/api/release-journal/" + self.entries[0]["id"])
        self.assertEqual(response.json()["data"], self.entries[0])
        self.assertEqual(self.client.get("/api/release-journal/unknown").status_code, 404)
        self.assertEqual(self.client.get("/release_history/history.json").status_code, 404)
        self.assertEqual(self.client.get("/assets/release_history/history.json").status_code, 404)
        self.assertEqual(self.client.post("/api/release-journal", json={"entries": []}).status_code, 405)
        self.assertEqual(self.store.list()["total"], 24)

    def test_invalid_query_does_not_modify_history(self):
        before = self.store.path.read_bytes()
        for params in ({"page": "0"}, {"page": "no"}, {"page": "100001"}, {"q": "x" * 121}, {"module": "x" * 81}):
            self.assertEqual(self.client.get("/api/release-journal", params=params).status_code, 400)
        self.assertEqual(self.store.path.read_bytes(), before)

    def test_corrupt_history_is_not_replaced_on_read(self):
        self.store.path.write_text("broken fixture", encoding="utf-8")
        response = self.client.get("/api/release-journal")
        self.assertEqual(response.status_code, 400)
        self.assertNotIn("broken fixture", response.text)
        self.assertEqual(self.store.path.read_text(encoding="utf-8"), "broken fixture")


if __name__ == "__main__":
    unittest.main()

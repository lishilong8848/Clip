"""Account preferences use the session identity and never initialize a model."""
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fastapi import FastAPI
from fastapi.testclient import TestClient
from lan_bitable_template_portal.lighthouse_appearance import DEFAULT_APPEARANCE, NAMESPACE
from tools.lighthouse_test_backend import install_test_backend as install_lighthouse_routes
from lan_bitable_template_portal.state_store import LanPortalStateStore


class AppearanceRouteTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "test.sqlite3"
        self.store = LanPortalStateStore(db_path=self.path)
        self.session = {"open_id": "user-a", "role": "building", "allowed_scopes": ["D"]}
        self.controller = SimpleNamespace(_current_session=lambda request: self.session,
            _request_base_url=lambda request: str(request.base_url).rstrip("/"))
        self.runtime = SimpleNamespace(state_store=self.store, auth_manager=SimpleNamespace(
            session_scopes=lambda session: session["allowed_scopes"], is_admin=lambda session: session["role"] == "admin"))
        self.client = self.app_client()
        self.addCleanup(self.client.close)

    def app_client(self):
        app = FastAPI()
        install_lighthouse_routes(app, self.controller, self.runtime)
        return TestClient(app, headers={"Origin": "http://testserver"})

    def test_ordinary_accounts_save_own_preferences_without_model_initialization(self):
        with patch("openclaw_service.assistant.routes.LighthouseAssistant", side_effect=AssertionError("no model needed")):
            self.assertEqual(self.client.get("/api/assistant/appearance").json()["data"], DEFAULT_APPEARANCE)
            response = self.client.put("/api/assistant/appearance", json={"color": "bleu", "snap_back": False})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.headers["cache-control"], "no-store")
            self.session["open_id"] = "user-b"
            self.assertEqual(self.client.get("/api/assistant/appearance?actor_id=user-a").json()["data"], DEFAULT_APPEARANCE)
            self.session["open_id"] = "user-a"
            self.assertFalse(self.client.get("/api/assistant/appearance").json()["data"]["snap_back"])

    def test_preferences_survive_new_store_and_conversation_document_changes(self):
        self.client.put("/api/assistant/appearance", json={"shape": "goutte", "size": 200})
        self.store.put_document("lighthouse_conversation", "user-a", {"turns": []})
        self.runtime.state_store = LanPortalStateStore(db_path=self.path)
        with self.app_client() as client:
            self.assertEqual(client.get("/api/assistant/appearance").json()["data"]["shape"], "goutte")
            self.assertEqual(client.get("/api/assistant/appearance").json()["data"]["size"], 200)

    def test_forged_identity_invalid_values_and_oversized_bodies_do_not_write(self):
        for payload in ({"actor_id": "user-b"}, {"color": "url(https://example.com)"}, {"snap_back": 1}, {"size": True}):
            self.assertEqual(self.client.put("/api/assistant/appearance", json=payload).status_code, 400)
        self.assertEqual(self.client.put("/api/assistant/appearance", content="x" * 16001).status_code, 413)
        self.assertIsNone(self.store.get_document(NAMESPACE, "user-a"))
        self.assertIsNone(self.store.get_document(NAMESPACE, "user-b"))

    def test_same_origin_and_formal_account_required(self):
        for origin in ("", "https://other.example"):
            self.assertEqual(self.client.put("/api/assistant/appearance", json={"color": "gris"}, headers={"Origin": origin}).status_code, 403)
        self.session["is_guest"] = True
        self.assertEqual(self.client.get("/api/assistant/appearance").status_code, 403)
        self.session = None
        self.assertEqual(self.client.get("/api/assistant/appearance").status_code, 401)

    def test_storage_failure_is_not_reported_as_saved(self):
        with patch.object(self.store, "put_document", side_effect=OSError("disk busy")):
            response = self.client.put("/api/assistant/appearance", json={"color": "rose"})
            self.assertEqual(response.status_code, 503)
            self.assertFalse(response.json()["ok"])
        self.assertEqual(self.client.get("/api/assistant/appearance").json()["data"], DEFAULT_APPEARANCE)

    def test_formal_account_model_api_only_changes_its_own_models(self):
        from lan_bitable_template_portal.lighthouse_ai import CustomModel, ENDPOINT, LighthouseAssistant
        model = CustomModel(self.store, protect=lambda value: "test:" + value, unprotect=lambda value: value[5:])
        self.addCleanup(model.close)
        service = LighthouseAssistant(self.store, lambda *_: ([], []), model=model)
        with patch("openclaw_service.assistant.routes.LighthouseAssistant", return_value=service):
            saved = self.client.put("/api/assistant/settings", json={"action": "upsert", "profile": {
                "id": "own", "name": "甲的模型", "endpoint": ENDPOINT, "model": "fixture-model", "api_key": "fake-key-a"}})
            self.assertEqual(saved.status_code, 200, saved.text)
            self.assertNotIn("fake-key-a", saved.text)
            self.assertNotIn("key_cipher", saved.text)
            self.session["open_id"] = "user-b"
            self.assertNotIn("own", [item["id"] for item in self.client.get("/api/assistant/settings").json()["data"]["models"]])
            self.client.put("/api/assistant/settings", json={"action": "toggle", "enabled": False})
            self.session["open_id"] = "user-a"
            self.assertTrue(self.client.get("/api/assistant/settings").json()["data"]["enabled"])
            self.assertIn("own", [item["id"] for item in self.client.get("/api/assistant/settings").json()["data"]["models"]])
            self.assertEqual(self.client.put("/api/assistant/settings", json={"action": "select", "id": "own"}, headers={"Origin": "https://other.example"}).status_code, 403)


if __name__ == "__main__":
    unittest.main()

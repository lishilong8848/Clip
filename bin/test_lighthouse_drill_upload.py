"""Drill-template assistant private-file upload tests.

Drives the actual installed ``/api/assistant/files`` route with a fake
controller/runtime (no service, no cloud, no business writes).  Also checks the
central ``upload_limit`` policy and that browser-facing ``public`` metadata hides
private keys (path / purpose / hash).

Only the normal assistant store and temp dirs are touched; no native/parsing
dependency is needed.
"""
import sys
import io
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lan_bitable_template_portal.lighthouse_ai import AssistantError
from lan_bitable_template_portal.lighthouse_files import (
    MAX_BATCH_BYTES,
    MAX_FILE_BYTES,
    DRILL_TEMPLATE_BYTES,
    LighthouseFiles,
    upload_limit,
)
from tools.lighthouse_test_backend import install_test_backend as install_lighthouse_routes
from lan_bitable_template_portal.state_store import LanPortalStateStore

XLSX_HEAD = b"PK\x03\x04" + b"\x00" * 128


def _make_session(role="building", open_id="drill-owner"):
    return {"user": {"open_id": open_id}, "role": role, "allowed_scopes": ["A"]}


class UploadLimitPolicyTests(unittest.TestCase):
    def test_default_limit_unchanged(self):
        limits = upload_limit({"id": "u", "is_admin": False})
        self.assertEqual(limits["purpose"], "default")
        self.assertEqual(limits["max_items"], 10)
        self.assertEqual(limits["max_bytes"], MAX_FILE_BYTES)
        self.assertEqual(limits["batch_bytes"], MAX_BATCH_BYTES)
        self.assertTrue(limits["extract"])

    def test_default_extensions_match_normal_allowlist(self):
        from lan_bitable_template_portal.lighthouse_files import ALLOWED_EXTENSIONS
        limits = upload_limit({"id": "u"}, None)
        self.assertEqual(limits["extensions"], ALLOWED_EXTENSIONS)

    def test_drill_template_requires_admin(self):
        with self.assertRaises(AssistantError) as cm:
            upload_limit({"id": "u", "is_admin": False}, "drill_template")
        self.assertEqual(cm.exception.status, 403)
        # Admin succeeds; covered by test_drill_template_admin_limits below.

    def test_drill_template_admin_limits(self):
        limits = upload_limit({"id": "admin", "is_admin": True}, "drill_template")
        self.assertEqual(limits["purpose"], "drill_template")
        self.assertEqual(limits["max_items"], 1)
        self.assertEqual(limits["max_bytes"], 64 * 1024 * 1024)
        self.assertEqual(limits["extensions"], {".xlsx"})
        self.assertFalse(limits["extract"])

    def test_unknown_purpose_rejected(self):
        with self.assertRaises(AssistantError) as cm:
            upload_limit({"id": "u", "is_admin": True}, "bogus")
        self.assertEqual(cm.exception.status, 400)

    def test_drill_template_bytes_constant(self):
        self.assertEqual(DRILL_TEMPLATE_BYTES, 64 * 1024 * 1024)


class DrillUploadRouteTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = LanPortalStateStore(db_path=Path(self.tmp.name) / "portal.sqlite3")
        self.session = _make_session("building")
        controller = SimpleNamespace(
            _current_session=lambda request: self.session,
            _auth_required_response=lambda: JSONResponse({"ok": False}, status_code=401),
            _request_base_url=lambda request: str(request.base_url).rstrip("/"),
        )
        runtime = SimpleNamespace(
            state_store=self.store,
            auth_manager=SimpleNamespace(
                is_admin=lambda s: s.get("role") == "admin",
                session_scopes=lambda s: s["allowed_scopes"],
            ),
        )
        self.app = FastAPI()
        install_lighthouse_routes(self.app, controller, runtime)

    def _client(self):
        return TestClient(self.app, headers={"Origin": "http://testserver"})

    def test_unknown_purpose_rejected_before_parse(self):
        with patch("lan_bitable_template_portal.lighthouse_files.LighthouseFiles.upload") as save, self._client() as client:
            response = client.post("/api/assistant/files?purpose=bogus",
                                   files={"files": ("a.xlsx", XLSX_HEAD)})
            self.assertEqual(response.status_code, 400, response.text)
            self.assertIn("不支持的上传用途", response.json()["error"])
            save.assert_not_called()

    def test_drill_template_requires_admin_route(self):
        self.session = _make_session("building")
        with patch("lan_bitable_template_portal.lighthouse_files.LighthouseFiles.upload") as save, self._client() as client:
            response = client.post("/api/assistant/files?purpose=drill_template",
                                   files={"files": ("a.xlsx", XLSX_HEAD)})
            self.assertEqual(response.status_code, 403, response.text)
            self.assertIn("仅管理员", response.json()["error"])
            save.assert_not_called()

    def test_admin_valid_drill_template_upload(self):
        self.session = _make_session("admin")
        captured = {}
        def fake_upload(self_, actor, name, content, *, extract=True, source_scopes=None, purpose=None):
            captured.update(actor=actor, name=name, content=content,
                            extract=extract, purpose=purpose, source_scopes=source_scopes)
            return {"id": "a" * 32}
        with patch("lan_bitable_template_portal.lighthouse_files.LighthouseFiles.upload", fake_upload) as save, self._client() as client:
            response = client.post("/api/assistant/files?purpose=drill_template",
                                   files={"files": ("template.xlsx", XLSX_HEAD)})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["data"]["files"][0]["id"], "a" * 32)
            self.assertEqual(captured["name"], "template.xlsx")
            self.assertEqual(captured["content"], XLSX_HEAD)
            self.assertFalse(captured["extract"])
            self.assertEqual(captured["purpose"], "drill_template")
            self.assertEqual(captured["actor"]["id"], "drill-owner")
            self.assertTrue(captured["actor"]["is_admin"])

    def test_drill_template_rejects_wrong_extension(self):
        self.session = _make_session("admin")
        with self._client() as client:
            for extension in ("txt", "zip", "doc", "xls", "bin", "xlsm"):
                with self.subTest(extension=extension):
                    response = client.post("/api/assistant/files?purpose=drill_template",
                                           files={"files": ("bad." + extension, b"x" * 8)})
                    self.assertEqual(response.status_code, 400, response.text)
                    self.assertIn("仅支持 .xlsx", response.json()["error"])

    def test_real_large_private_file_is_owned_and_not_extracted(self):
        self.session = _make_session("admin")
        content = XLSX_HEAD + b"x" * (21 * 1024 * 1024)
        with patch("lan_bitable_template_portal.lighthouse_files.extract_text", side_effect=AssertionError("must not parse template")), self._client() as client:
            response = client.post("/api/assistant/files?purpose=drill_template", files={"files": ("large.xlsx", content)})
            self.assertEqual(response.status_code, 200, response.text)
            item = response.json()["data"]["files"][0]
            self.assertEqual(item["size"], len(content))
            self.assertEqual(client.get(item["url"]).content, content)
            self.assertNotIn("path", item)
            self.session = _make_session("admin", "other-owner")
            self.assertEqual(client.get(item["url"]).status_code, 404)

    def test_drill_template_rejects_oversize(self):
        self.session = _make_session("admin")
        with patch("lan_bitable_template_portal.lighthouse_files.DRILL_TEMPLATE_BYTES", 8) as _, \
             patch("lan_bitable_template_portal.lighthouse_files.LighthouseFiles.upload") as save, self._client() as client:
            response = client.post("/api/assistant/files?purpose=drill_template",
                                   files={"files": ("big.xlsx", b"x" * 9)})
            self.assertEqual(response.status_code, 413, response.text)
            save.assert_not_called()

    def test_drill_template_rejects_more_than_one_file(self):
        self.session = _make_session("admin")
        with patch("lan_bitable_template_portal.lighthouse_files.LighthouseFiles.upload") as save, self._client() as client:
            response = client.post(
                "/api/assistant/files?purpose=drill_template",
                files=[("files", ("one.xlsx", XLSX_HEAD)), ("files", ("two.xlsx", XLSX_HEAD))],
            )
            self.assertEqual(response.status_code, 413, response.text)
            save.assert_not_called()

    def test_normal_upload_without_purpose_uses_default(self):
        captured = {}
        def fake_upload(self_, actor, name, content, *, extract=True, source_scopes=None, purpose=None):
            captured.update(name=name, content=content, extract=extract, purpose=purpose)
            return {"id": "b" * 32}
        with patch("lan_bitable_template_portal.lighthouse_files.LighthouseFiles.upload", fake_upload), self._client() as client:
            response = client.post("/api/assistant/files", files={"files": ("note.txt", b"hello")})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(captured["name"], "note.txt")
            self.assertEqual(captured["content"], b"hello")
            self.assertTrue(captured["extract"])
            self.assertIsNone(captured["purpose"])

    def test_normal_upload_still_uses_20_mib_limit(self):
        # Normal uploads (no purpose) keep the 20MiB policy; patched small so no huge body.
        with patch("lan_bitable_template_portal.lighthouse_files.MAX_FILE_BYTES", 5) as _, \
             patch("lan_bitable_template_portal.lighthouse_files.LighthouseFiles.upload") as save, self._client() as client:
            response = client.post("/api/assistant/files", files={"files": ("note.txt", b"same-name.txt")})
            self.assertEqual(response.status_code, 413, response.text)
            save.assert_not_called()

    def test_public_hides_path_and_purpose(self):
        item = {"id": "a" * 32, "name": "template.xlsx", "mime": "application/octet-stream",
                "size": 1, "error": "", "path": "/tmp/private/file", "purpose": "drill_template",
                "sha256": "hash", "extracted": False, "owner": "o"}
        out = LighthouseFiles.public(item)
        self.assertNotIn("path", out)
        self.assertNotIn("purpose", out)
        self.assertNotIn("sha256", out)
        self.assertNotIn("owner", out)
        self.assertEqual(out["url"], "/api/assistant/files/" + "a" * 32)

    def test_water_photos_skip_ocr_and_keep_native_image_limit(self):
        from PIL import Image
        stream = io.BytesIO()
        Image.new("RGB", (4, 4), "orange").save(stream, format="PNG")
        content = stream.getvalue()
        with patch("lan_bitable_template_portal.lighthouse_files.extract_text", side_effect=AssertionError("water proof is not OCR")), self._client() as client:
            result = client.post("/api/assistant/files?purpose=water_photo", files={"files": ("water.png", content)})
            self.assertEqual(result.status_code, 200, result.text)
            photo = result.json()["data"]["files"][0]
            self.assertTrue(photo["is_image"])
            self.assertEqual(client.get(photo["url"]).content, content)
            invalid = client.post("/api/assistant/files?purpose=water_photo", files={"files": ("water.pdf", b"PDF")})
            self.assertEqual(invalid.status_code, 400)
            with patch("lan_bitable_template_portal.lighthouse_files.WATER_PHOTO_BYTES", len(content) - 1):
                too_big = client.post("/api/assistant/files?purpose=water_photo", files={"files": ("water.png", content)})
            self.assertEqual(too_big.status_code, 413)


if __name__ == "__main__":
    unittest.main(verbosity=2)

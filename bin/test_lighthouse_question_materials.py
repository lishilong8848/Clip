"""Question material extraction respects native learning visibility, offsets and caches.

Only the native LearningService/FakeCloud/Store are used; the only mocked
external pieces are document OCR/text extraction for synthetic image content.
No network calls, credentials or production business writes are exercised.
"""
import copy
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fastapi import FastAPI
from fastapi.testclient import TestClient

from lan_bitable_template_portal.learning import LearningService
from lan_bitable_template_portal.lighthouse_ai import AssistantError
from lan_bitable_template_portal.lighthouse_sources import question_material, question_bank, question_material_file, question_material_text
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from tools.lighthouse_test_backend import install_test_backend as install_lighthouse_routes
from lan_bitable_template_portal.portal_service import BUILDING_OPEN_ID_MAP
import test_learning as learning_fixture
from test_lighthouse_stream import Store

ADMIN = {"id": "admin", "is_admin": True, "scopes": ["A"], "learning_scopes": ["A"]}


def h_actor():
    return {"id": BUILDING_OPEN_ID_MAP["H"], "is_admin": False,
            "scopes": list("ABCDEH"), "learning_scopes": ["H"], "scope": "H"}


class QuestionMaterialTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cloud = learning_fixture.FakeCloud(enabled=False)
        self.service = LearningService(self.root, self.cloud, learning_fixture.FakeSender())
        self.store = Store(self.root / "assistant.sqlite3")
        self._write_files()

        self.mat_h = {"id": "mat_h", "name": "material.txt", "kind": "material",
                      "file_token": "TOKEN-H-MAT", "local_file": "mat_h.txt", "size": 5}
        self.ans_h = {"id": "ans_h", "name": "answer.txt", "kind": "answer",
                      "file_token": "TOKEN-H-ANS", "local_file": "ans_h.txt", "size": 5}
        self.img_h = {"id": "img_h", "name": "diagram.png", "kind": "material",
                      "file_token": "TOKEN-H-IMG", "local_file": "img_h.png", "size": 5}
        self.qh = self._question(1, [self.mat_h, self.ans_h, self.img_h])

        self.xls_h = {"id": "xls_h", "name": "legacy.xls", "kind": "material",
                      "file_token": "TOKEN-H-XLS", "local_file": "xls_h.xls", "size": 5}
        self.q_xls = self._question(2, [self.xls_h])

        self.empty_h = {"id": "empty_h", "name": "empty.txt", "kind": "material",
                        "file_token": "TOKEN-H-EMPTY", "local_file": "empty_h.txt", "size": 5}
        self.q_empty = self._question(3, [self.empty_h])

        self.mat_a = {"id": "mat_a", "name": "material_a.txt", "kind": "material",
                      "file_token": "TOKEN-A-MAT", "local_file": "mat_a.txt", "size": 5}
        self.qa = self._question(100, [self.mat_a])

        with self.service.transaction() as conn:
            for q in (self.qh, self.q_xls, self.q_empty, self.qa):
                self.service._put("question", q["id"], q, conn, False)
            for attachment in (self.mat_h, self.ans_h, self.img_h):
                self.service._put("attachment", attachment["id"], {**attachment, "question_id": self.qh["id"]}, conn, False)
            self.service._put("attachment", self.xls_h["id"], {**self.xls_h, "question_id": self.q_xls["id"]}, conn, False)
            self.service._put("attachment", self.empty_h["id"], {**self.empty_h, "question_id": self.q_empty["id"]}, conn, False)
            self.service._put("attachment", self.mat_a["id"], {**self.mat_a, "question_id": self.qa["id"]}, conn, False)
            self.service._put("local", "refresh", {"at": "2026-10-03T08:00:00+08:00"}, conn, False)

        self.paper("h1", "H", [self.qh], entries={self.qh["id"]: {}})
        self.paper("h_xls", "H", [self.q_xls])
        self.paper("h_empty", "H", [self.q_empty])
        self.paper("a1", "A", [self.qa])

    def _write_files(self):
        directory = self.root / "files"
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "mat_h.txt").write_text("H 楼资料文字，供核对。", encoding="utf-8")
        (directory / "ans_h.txt").write_text("H answer secret", encoding="utf-8")
        (directory / "img_h.png").write_bytes(b"not-a-real-image")
        (directory / "xls_h.xls").write_bytes(b"legacy")
        (directory / "empty_h.txt").write_bytes(b"")
        (directory / "mat_a.txt").write_text("A building material", encoding="utf-8")

    def _question(self, number, attachments):
        return learning_fixture.LearningTests.question(self, number, attachments=copy.deepcopy(attachments))

    def paper(self, identity, scope, questions, *, deleted=False, entries=None):
        with self.service.transaction() as conn:
            self.service._put("paper", identity, {"id": identity, "date": "2026-10-03", "scope": scope,
                "shortage": {}, "created_at": "2026-10-03", "questions": copy.deepcopy(questions),
                "deleted_at": "2026-10-03" if deleted else ""}, conn, False)
            if entries:
                self.service._put("record", identity, {"id": identity, "entries": entries, "version": 1}, conn, False)

    def read(self, actor=None, material_id="mat_h", **params):
        defaults = {"scope": "ALL", "material_id": material_id, "offset": 0, "length": 4000}
        defaults.update(params)
        return question_material(self.service, self.store, actor or h_actor(), defaults)

    def assert_no_private_metadata(self, result):
        blob = json.dumps(result)
        for private in ("file_token", "local_file", "local_path", "_revision", "_dirty"):
            self.assertNotIn(private, blob)
        self.assertNotIn(str(self.root), blob)

    # --- permission gating ---

    def test_hidden_answer_blocked_before_file_retrieval_or_ocr(self):
        with patch("lan_bitable_template_portal.lighthouse_files.extract_text") as extract:
            with self.assertRaises(AssistantError) as caught:
                self.read(material_id="ans_h")
            self.assertEqual(caught.exception.status, 403)
            extract.assert_not_called()
        self.assertEqual(self.cloud.calls, [])

    def test_native_reveal_allows_answer_read(self):
        self.paper("h1", "H", [self.qh], entries={self.qh["id"]: {"revealed": "yes"}})
        result = self.read(material_id="ans_h")
        self.assertIn("answer secret", result["text"])
        self.assertEqual(self.cloud.calls, [])

    def test_broad_general_scopes_still_cannot_see_foreign_building_material(self):
        with self.assertRaises(AssistantError) as caught:
            self.read(material_id="mat_a", scope="A")
        self.assertEqual(caught.exception.status, 403)
        self.assertEqual(self.cloud.calls, [])

    def test_admin_can_read_any_native_attachment(self):
        result = question_material(self.service, self.store, ADMIN,
                                   {"scope": "ALL", "material_id": "mat_a", "offset": 0, "length": 4000})
        self.assertIn("A building material", result["text"])
        # A non-admin H account is still refused the A-building material.
        with self.assertRaises(AssistantError) as caught:
            self.read(material_id="mat_a", scope="A")
        self.assertEqual(caught.exception.status, 403)
        self.assertEqual(self.cloud.calls, [])

    def test_issue_attachment_rejected(self):
        with self.service.transaction() as conn:
            self.service._put("attachment", "issue_att", {"id": "issue_att", "question_id": self.qh["id"],
                "issue_id": "issue-1", "name": "issue.txt", "kind": "material"}, conn, False)
        with self.assertRaises(AssistantError) as caught:
            self.read(material_id="issue_att")
        self.assertEqual(caught.exception.status, 404)

    # --- extraction/cache ---

    def test_portal_only_reads_file_assistant_alone_extracts_and_caches(self):
        from openclaw_service.store import AssistantStore
        with patch('lan_bitable_template_portal.lighthouse_files.extract_text', return_value='extracted in service') as extract:
            source = question_material_file(self.service, h_actor(), {'material_id': 'mat_h'})
            extract.assert_not_called()
            self.assertEqual(self.store.list_documents('lighthouse_question_text'), [])
            assistant_store = AssistantStore(self.root / 'resident')
            result = question_material_text(assistant_store, source)
            self.assertEqual(result['text'], 'extracted in service')
            self.assertEqual(len(assistant_store.list_documents('lighthouse_question_text')), 1)
            self.assertEqual(self.store.list_documents('lighthouse_question_text'), [])
            self.assert_no_private_metadata(result)
            extract.assert_called_once()

    def test_incomplete_transferred_file_never_populates_text_cache(self):
        source = question_material_file(self.service, h_actor(), {'material_id': 'mat_h'})
        source['content_base64'] = source['content_base64'][:-4]
        with self.assertRaises(AssistantError) as caught:
            question_material_text(self.store, source)
        self.assertEqual(caught.exception.status, 502)
        self.assertEqual(self.store.list_documents('lighthouse_question_text'), [])

    def test_cache_reused_but_permission_always_reevaluated(self):
        with patch("lan_bitable_template_portal.lighthouse_files.extract_text", return_value="cached extraction") as extract:
            first = self.read(material_id="mat_h")
            second = self.read(material_id="mat_h")
            self.assertEqual(first["text"], "cached extraction")
            self.assertEqual(second["text"], "cached extraction")
            self.assertEqual(extract.call_count, 1, "second read must reuse cached extraction")
            # Six-building duty accounts can now access personal learning across
            # buildings, but deleting the source paper must revoke cached access.
            self.paper("h1", "H", [self.qh], deleted=True)
            with self.assertRaises(AssistantError) as caught:
                self.read(material_id="mat_h")
            self.assertEqual(caught.exception.status, 403)
            self.assertEqual(extract.call_count, 1)
        self.assertEqual(self.cloud.calls, [])

    def test_changed_content_re_extracts(self):
        first = self.read(material_id="mat_h")
        Path(self.root / "files" / "mat_h.txt").write_text("MODIFIED content line", encoding="utf-8")
        second = self.read(material_id="mat_h")
        self.assertNotEqual(first["text"], second["text"])
        self.assertIn("MODIFIED content line", second["text"])
        self.assertEqual(len(self.store.list_documents("lighthouse_question_text")), 2)

    def test_plain_text_real_extraction_and_next_offset(self):
        file = Path(self.root / "files" / "mat_h.txt")
        file.write_text("abcdefghij\nsecond line", encoding="utf-8")
        result = self.read(material_id="mat_h", offset=0, length=12)
        self.assertEqual(result["text"], "abcdefghij\ns")
        self.assertEqual(result["offset"], 0)
        self.assertEqual(result["next_offset"], 12)
        tail = self.read(material_id="mat_h", offset=12, length=6000)
        self.assertEqual(tail["text"], "econd line")
        self.assertEqual(tail["next_offset"], None)

    def test_image_extract_mocked_and_declares_ocr_limitation(self):
        with patch("lan_bitable_template_portal.lighthouse_files.extract_text", return_value="OCR recognition result") as extract:
            result = self.read(material_id="img_h")
            extract.assert_called_once()
            self.assertIn("OCR recognition result", result["text"])
            self.assertIn("图片文字识别", result["reading_method"])
            self.assertIn("核对原图", result["reading_method"])

    def test_no_text_returns_422(self):
        # empty.txt really extracts to an empty string.
        with self.assertRaises(AssistantError) as caught:
            self.read(material_id="empty_h")
        self.assertEqual(caught.exception.status, 422)

    def test_native_permission_revocation_blocks_previously_cached_answer(self):
        self.paper("h1", "H", [self.qh], entries={self.qh["id"]: {"revealed": "yes"}})
        self.assertIn("answer secret", self.read(material_id="ans_h")["text"])
        self.paper("h1", "H", [self.qh], entries={self.qh["id"]: {}})
        with self.assertRaises(AssistantError) as caught:
            self.read(material_id="ans_h")
        self.assertEqual(caught.exception.status, 403)

    def test_pdf_real_extraction_and_corrupt_pdf_error(self):
        from pypdf import PdfWriter
        from pypdf.generic import DictionaryObject, NameObject, DecodedStreamObject
        writer = PdfWriter()
        page = writer.add_blank_page(300, 200)
        page[NameObject("/Resources")] = DictionaryObject({NameObject("/Font"): DictionaryObject({
            NameObject("/F1"): DictionaryObject({NameObject("/Type"): NameObject("/Font"),
                NameObject("/Subtype"): NameObject("/Type1"), NameObject("/BaseFont"): NameObject("/Helvetica")})})})
        stream = DecodedStreamObject()
        stream.set_data(b"BT /F1 12 Tf 20 100 Td (UPS reference material) Tj ET")
        page[NameObject("/Contents")] = writer._add_object(stream)
        output = io.BytesIO()
        writer.write(output)
        path = self.root / "files" / "reference.pdf"
        path.write_bytes(output.getvalue())
        with self.service.transaction() as conn:
            meta = self.service._get("attachment", "mat_h", conn)
            meta.update(name="reference.pdf", local_file=path.name)
            self.service._put("attachment", "mat_h", meta, conn, False)
        self.assertIn("UPS reference material", self.read()["text"])
        path.write_bytes(b"%PDF-corrupt")
        with self.assertRaises(AssistantError) as caught:
            self.read()
        self.assertEqual(caught.exception.status, 422)
        self.assertNotIn(str(path), str(caught.exception))

    def test_invalid_scope_bounds_and_extraction_errors_are_explicit(self):
        for params in ({"scope": "invalid"}, {"offset": -1}, {"length": 6001}, {"length": 0}):
            with self.subTest(params=params), self.assertRaises(AssistantError) as caught:
                self.read(actor=ADMIN, **params)
            self.assertEqual(caught.exception.status, 400)
        with patch("lan_bitable_template_portal.lighthouse_files.extract_text", side_effect=OSError("private-path")):
            with self.assertRaises(AssistantError) as caught:
                self.read()
        self.assertNotIn("private-path", str(caught.exception))
        self.assertEqual(caught.exception.status, 422)

    def test_unsupported_format_returns_422(self):
        with self.assertRaises(AssistantError) as caught:
            self.read(material_id="xls_h")
        self.assertEqual(caught.exception.status, 422)
        self.assertEqual(self.cloud.calls, [])

    def test_result_has_no_cloud_token_or_local_path(self):
        result = self.read(material_id="mat_h")
        self.assert_no_private_metadata(result)
        self.assertEqual(self.cloud.calls, [])

    # --- visibility through public_paper / question_bank ---

    def test_materials_visible_only_according_to_public_paper(self):
        duty = h_actor()
        items = {item["id"]: item for item in question_bank(self.service, duty, {})["items"]}
        self.assertIn(self.qh["id"], items)
        item = items[self.qh["id"]]
        material_ids = {m["id"] for m in item["materials"]}
        self.assertIn("mat_h", material_ids)
        self.assertIn("img_h", material_ids)
        self.assertNotIn("ans_h", material_ids)  # hidden answer attachment not material
        # After native reveal the answer attachment becomes a visible material.
        self.paper("h1", "H", [self.qh], entries={self.qh["id"]: {"revealed": "yes"}})
        items = {item["id"]: item for item in question_bank(self.service, duty, {})["items"]}
        item = items[self.qh["id"]]
        material_ids = {m["id"] for m in item["materials"]}
        self.assertIn("ans_h", material_ids)
        # question_material refuses the still-hidden-only scenario consistently.
        self.paper("h1", "H", [self.qh], entries={self.qh["id"]: {}})
        with self.assertRaises(AssistantError):
            self.read(material_id="ans_h")

    # --- route/catalog ---

    def test_catalog_and_route_get_only_unauth_401_hidden_403(self):
        app = FastAPI()
        session = {"open_id": BUILDING_OPEN_ID_MAP["H"]}
        controller = SimpleNamespace(
            _current_session=lambda _: session,
            _request_base_url=lambda _: "http://testserver")
        auth = SimpleNamespace(session_scopes=lambda _: list("ABCDEH"), is_admin=lambda _: False)
        runtime = SimpleNamespace(auth_manager=auth, learning_service=self.service, state_store=self.store)
        install_lighthouse_routes(app, controller, runtime)
        catalog = PortalAPICatalog(app)
        route = catalog.get("GET /api/assistant/question-material")
        self.assertTrue(route["read_only"])
        self.assertEqual(set(route["schema"]["query"]), {"scope", "material_id", "offset", "length"})

        client = TestClient(app)
        # GET-only: no mutable verb registered.
        self.assertEqual(client.post("/api/assistant/question-material", json={}).status_code, 405)
        # unauth
        session.clear()
        self.assertEqual(client.get("/api/assistant/question-material?material_id=mat_h").status_code, 401)
        # authenticated H can read visible material
        session["open_id"] = BUILDING_OPEN_ID_MAP["H"]
        ok = client.get("/api/assistant/question-material?material_id=mat_h")
        self.assertEqual(ok.status_code, 200)
        self.assertIn("H 楼资料文字", ok.json()["data"]["text"])
        # hidden answer refused
        self.assertEqual(client.get("/api/assistant/question-material?material_id=ans_h").status_code, 403)
        # guest refused
        session["is_guest"] = True
        self.assertEqual(client.get("/api/assistant/question-material?material_id=mat_h").status_code, 403)


if __name__ == "__main__":
    unittest.main()

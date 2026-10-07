# -*- coding: utf-8 -*-
"""Bounded upload schema fixes for native raw upload endpoints.

This test only works against isolated in-process FastAPI apps that reuse the
real ``PortalAPICatalog`` gateway.  It never starts a server, never reaches the
cloud, and never mutates production data.  It verifies two bounded fixes made
in ``lighthouse_api.py``:

  1. POST /api/capacity/water/uploads is registered as a single-scope query
     route consistent with the other capacity/water permissions, and its
     ``scope`` + ``file_name`` query inputs are declared in the descriptor.
  2. POST /api/notice-attachments declares its real query fields
     (identity/kind/scope/target_record_id/file_name) and the ``kind`` choices
     (site/ali) in the catalogue, without making identity or scope globally
     required -- identity-free temporary attachments must remain allowed.
"""
import asyncio
import io
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from PIL import Image

from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog, AssistantError


WATER_SCOPES = list("ABCDEH")
NOTICE_KINDS = ["site", "ali"]


def _png_bytes():
    output = io.BytesIO()
    Image.new("RGB", (12, 12), "white").save(output, "PNG")
    return output.getvalue()


def _request():
    return Request({
        "type": "http",
        "scheme": "http",
        "server": ("testserver", 80),
        "client": ("127.0.0.1", 4567),
        "path": "/api/assistant/agent",
        "root_path": "",
        "query_string": b"",
        "headers": [(b"cookie", b"fixture=a"), (b"origin", b"http://testserver")],
    })


class _Files:
    """Minimal in-memory attachment store matching LighthouseFiles shape."""
    def __init__(self):
        self._items = {}
        self._seq = 0

    def upload(self, name, raw):
        self._seq += 1
        aid = f"up-{self._seq}"
        self._items[aid] = {
            "id": aid,
            "name": name,
            "bytes": raw,
            "mime": "image/png",
        }
        return self._items[aid]

    def get(self, aid):
        return self._items.get(aid)


class WaterUploadSchemaAndTransport(unittest.IsolatedAsyncioTestCase):
    """POST /api/capacity/water/uploads descriptor + actual ASGI transport."""

    def setUp(self):
        self.captured = {}

        app = FastAPI()

        @app.post("/api/capacity/water/uploads")
        async def water_upload(request: Request):
            # Mirror the real handler's permission gate: scope must be present
            # and permitted (native single-scope policy).
            scope = str(request.query_params.get("scope") or "").strip()
            if not scope or scope not in WATER_SCOPES:
                from fastapi import HTTPException
                raise HTTPException(status_code=403, detail="unauthorized scope")
            raw = await request.body()
            self.captured = {
                "raw": raw,
                "content_type": request.headers.get("content-type", ""),
                "scope": scope,
                "file_name": request.query_params.get("file_name", ""),
            }
            return JSONResponse(
                {"ok": True, "data": {"upload_id": "water-up-1"}},
                status_code=200,
            )

        self.catalog = PortalAPICatalog(app)
        self.files = _Files()

    def test_descriptor_declares_query_fields_and_single_scope(self):
        desc = self.catalog.get("POST /api/capacity/water/uploads")
        self.assertEqual(desc["scope_mode"], "single")
        self.assertEqual(desc["scope_values"], WATER_SCOPES)
        self.assertEqual(desc["scope_section"], "params")
        self.assertTrue(desc["scope_required"])
        self.assertIn("scope", desc["schema"]["query"])
        self.assertIn("file_name", desc["schema"]["query"])
        self.assertEqual(desc["upload_format"], "raw")

    def test_scope_is_required_before_native_invocation(self):
        # No scope must be reported as missing by the shared single-scope gate
        # before the business handler is ever reached.
        norm, missing = self.catalog.validate_operation({
            "api_id": "POST /api/capacity/water/uploads",
            "files": {"file": ["up-1"]},
        })
        self.assertTrue(any(f["path"] == "scope" and f["section"] == "params" for f in missing))
        # Missing scope is reported (not injected) so the native handler stays
        # the single authority on what is finally sent.
        self.assertFalse(norm.get("params", {}).get("scope"))
        # Invalid single-scope values are rejected before any native call.
        for bad in ("ALL", "CAMPUS", "G", "110", "A,B", "ABCDE"):
            with self.subTest(bad=bad), self.assertRaises(AssistantError):
                self.catalog.validate_operation({
                    "api_id": "POST /api/capacity/water/uploads",
                    "params": {"scope": bad},
                    "files": {"file": ["up-1"]},
                })

    async def test_upload_sends_raw_bytes_mime_and_scope_query(self):
        raw = _png_bytes()
        file = self.files.upload("水表照片.png", raw)
        file_provider = lambda fid: self.files.get(fid)
        result = await self.catalog.invoke({
            "api_id": "POST /api/capacity/water/uploads",
            "params": {"scope": "A"},
            "files": {"file": [file["id"]]},
        }, _request(), file_provider=file_provider)
        self.assertTrue(result["ok"], result)
        self.assertIsNotNone(self.captured)
        self.assertEqual(self.captured["raw"], raw)
        self.assertTrue(self.captured["content_type"].startswith("image/"))
        self.assertEqual(self.captured["scope"], "A")
        # Raw binary uploads carry the original name via query, not multipart.
        self.assertEqual(self.captured["file_name"], "水表照片.png")
        self.assertNotIn("multipart/form-data", self.captured["content_type"])


class NoticeAttachmentsSchemaAndTransport(unittest.IsolatedAsyncioTestCase):
    """POST /api/notice-attachments query declaration + identity-free upload."""

    def setUp(self):
        self.captured = {}

        app = FastAPI()

        @app.post("/api/notice-attachments")
        async def notice_attachments(request: Request):
            raw = await request.body()
            self.captured = {
                "raw": raw,
                "content_type": request.headers.get("content-type", ""),
                "identity": request.query_params.get("identity", ""),
                "kind": request.query_params.get("kind", "site"),
                "scope": request.query_params.get("scope", ""),
                "target_record_id": request.query_params.get("target_record_id", ""),
                "file_name": request.query_params.get("file_name", ""),
            }
            return JSONResponse(
                {"ok": True, "data": {"upload_id": "notice-up-1"}},
                status_code=200,
            )

        self.catalog = PortalAPICatalog(app)
        self.files = _Files()

    def test_descriptor_declares_real_query_fields_and_kind_choices(self):
        desc = self.catalog.get("POST /api/notice-attachments")
        for field in ("identity", "kind", "scope", "target_record_id", "file_name"):
            self.assertIn(field, desc["schema"]["query"])
        # kind choices are surfaced declaratively in the catalogue and are a
        # direct, validated descriptor mapping (no dead metadata key).
        self.assertEqual(desc.get("query_enums", {}), {"kind": NOTICE_KINDS})
        # Native identity is OPTIONAL and scope must NOT be required globally:
        # identity-free temporary attachments remain legitimate.
        self.assertNotIn("scope_mode", desc)
        self.assertNotIn("scope_required", desc)

    async def test_identity_free_temporary_upload_remains_allowed(self):
        raw = _png_bytes()
        file = self.files.upload("临时现场图.png", raw)
        file_provider = lambda fid: self.files.get(fid)
        # No identity / kind / scope / target_record_id is required by the
        # shared validation layer; the fake handler accepts the temporary upload.
        result = await self.catalog.invoke({
            "api_id": "POST /api/notice-attachments",
            "files": {"file": [file["id"]]},
        }, _request(), file_provider=file_provider)
        self.assertTrue(result["ok"], result)
        self.assertIsNotNone(self.captured)
        self.assertEqual(self.captured["raw"], raw)
        self.assertTrue(self.captured["content_type"].startswith("image/"))
        # Identity-free upload keeps identity blank and still carries file_name.
        self.assertEqual(self.captured["identity"], "")
        self.assertEqual(self.captured["kind"], "site")
        self.assertEqual(self.captured["file_name"], "临时现场图.png")
        self.assertNotIn("multipart/form-data", self.captured["content_type"])

    def test_invalid_kind_rejected_before_native_invocation(self):
        raw = _png_bytes()
        file = self.files.upload("现场图.png", raw)
        # An unsupported kind is rejected locally by validate_operation, before
        # the shared ASGI gateway (invoke) could ever forward it.
        for bad in ("campus", "sitex", "ali2", "ALI"):
            with self.subTest(bad=bad):
                with self.assertRaises(AssistantError):
                    self.catalog.validate_operation({
                        "api_id": "POST /api/notice-attachments",
                        "params": {"kind": bad},
                        "files": {"file": [file["id"]]},
                    })
        # Valid kinds still validate.  Optional fields stay optional: kind is
        # not globally required and, once supplied, is checked before ASGI.
        for good in NOTICE_KINDS:
            with self.subTest(good=good):
                norm, missing = self.catalog.validate_operation({
                    "api_id": "POST /api/notice-attachments",
                    "params": {"kind": good},
                    "files": {"file": [file["id"]]},
                })
                self.assertEqual(norm["params"]["kind"], good)
                self.assertEqual(missing, [])

    def test_conditional_scope_only_for_identity_without_target_record_id(self):
        raw = _png_bytes()
        file = self.files.upload("现场图.png", raw)
        op = lambda **params: {
            "api_id": "POST /api/notice-attachments",
            "params": params,
            "files": {"file": [file["id"]]},
        }
        # identity-free temporary upload: no scope is required.
        _, missing = self.catalog.validate_operation(op())
        self.assertFalse(any(f["path"] == "scope" and f["section"] == "params" for f in missing))
        # target_record_id native scope derivation: scope is NOT required here;
        # the native handler derives building codes and stays authoritative for
        # admin/H authorization and confirmation-task existence.
        _, missing = self.catalog.validate_operation(op(
            identity="change:rec123", kind="ali", target_record_id="rec123"))
        self.assertFalse(any(f["path"] == "scope" and f["section"] == "params" for f in missing))
        # identity supplied WITHOUT target_record_id: scope becomes required so
        # the prepare flow can request it (native still validates the value).
        _, missing = self.catalog.validate_operation(op(identity="user-openid"))
        self.assertTrue(any(
            f["path"] == "scope" and f["section"] == "params" and f["required"]
            for f in missing))

    def test_consume_json_keeps_whole_payload_under_raw(self):
        # Audit: _consume_json does NOT drop error_code/details -- the whole
        # parsed payload is preserved under "_raw", so no extra gateway response
        # envelope is required.
        raw = {"ok": False, "error_code": "E_ALREADY_STOPPED", "detail": "任务已停止"}
        result = self.catalog._consume_json(409, raw, "POST /api/notice-attachments")
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], 409)
        self.assertEqual(result["_raw"], raw)
        self.assertEqual(result["error"], raw["detail"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
"""Streaming multipart upload tests for the knowledge routes.

These exercise the REAL Starlette ``MultiPartParser`` against the ``knowledge/files``
route with a recording ``upload_file`` service double, so the parser, the bounded
streaming bounds, the 2-slot concurrency guard, limit mapping and temp-file cleanup
are all covered without touching a real index.  Limits are monkeypatched small for the
unit tests while the production constants (100MiB / 300MiB / 10 files / 2 slots) are
asserted explicitly.
"""
import sys
import asyncio
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import httpx
from fastapi import FastAPI

from openclaw_service.assistant.lighthouse_knowledge_routes import (
    MAX_BATCH,
    MAX_FILES,
    MAX_PER_FILE,
    MULTIPART_OVERHEAD,
    _UPLOAD_SEMAPHORE,
    _UPLOAD_SLOTS,
    install_knowledge_routes,
)
import openclaw_service.assistant.lighthouse_knowledge_routes as routes_module
import starlette.formparsers as starlette_formparsers
from starlette.requests import Request as StarletteRequest


def actor(uid="owner", *, admin=False, guest=False, role=None, name="用户"):
    role = role or ("guest" if guest else ("admin" if admin else "user"))
    return {"id": uid, "is_admin": admin, "is_guest": guest, "role": role, "name": name}


class RecordingUploadService:
    """Service double with a real seekable ``upload_file`` recording each call."""

    def __init__(self):
        self.calls = []
        self.fail_on = None  # filename -> AssistantError to raise per file

    def upload_file(self, owner, filename, file_obj, *, document_id="", revision=None):
        self.calls.append({"owner": owner, "filename": filename, "document_id": document_id, "revision": revision})
        if self.fail_on and filename in self.fail_on:
            raise self.fail_on[filename]
        # Read the whole file body so tests prove seekable bytes reached the service.
        file_obj.seek(0)
        body = file_obj.read()
        return {
            "id": "doc-" + str(len(self.calls)),
            "name": filename,
            "owner_name": owner.get("name", ""),
            "version": 1,
            "active_version": 1,
            "status": "queued",
            "updated_at": 0,
            "size": len(body),
            "chunks": 0,
            "can_edit": True,
        }


class _TrackingSpooledTemporaryFile:
    """Wrapper that records every SpooledTemporaryFile created by Starlette so tests
    can assert no file handle is left open after request handling."""

    created = []
    _real = None

    def __init__(self, *args, **kwargs):
        if _TrackingSpooledTemporaryFile._real is None:
            raise RuntimeError("tracking wrapper must be patched with the real class")
        self._wrapped = _TrackingSpooledTemporaryFile._real(*args, **kwargs)
        _TrackingSpooledTemporaryFile.created.append(self)
        self._closed = False

    def __getattr__(self, name):
        return getattr(self._wrapped, name)

    def close(self):
        self._closed = True
        return self._wrapped.close()

    @property
    def closed(self):
        return self._closed

    def __enter__(self):
        self._wrapped.__enter__()
        return self

    def __exit__(self, *args):
        return self._wrapped.__exit__(*args)


def _patch_tracking():
    real = starlette_formparsers.SpooledTemporaryFile
    _TrackingSpooledTemporaryFile.created.clear()
    _TrackingSpooledTemporaryFile._real = real
    return mock.patch.object(starlette_formparsers, "SpooledTemporaryFile", _TrackingSpooledTemporaryFile)


class StreamingUploadRoutes(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.service = RecordingUploadService()
        app = FastAPI()

        async def authorize(request):
            return actor(request.headers.get("user", "owner"), admin=request.headers.get("admin") == "1",
                         guest=request.headers.get("guest") == "1", role=None)

        install_knowledge_routes(app, authorize, lambda: self.service)
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://fixture")
        self.files_endpoint = None
        for r in app.routes:
            if getattr(r, "path", None) == "/api/assistant/knowledge/files":
                self.files_endpoint = r.endpoint
                break
        self.assertIsNotNone(self.files_endpoint, "knowledge/files route not installed")
        await self._restore_slots()

    async def asyncTearDown(self):
        await self.client.aclose()
        await self._restore_slots()
        _TrackingSpooledTemporaryFile.created.clear()
        _TrackingSpooledTemporaryFile._real = None

    async def _restore_slots(self):
        # Never leave fewer slots than full; release what a failed test may have held.
        expected = _UPLOAD_SLOTS
        actual = _UPLOAD_SEMAPHORE._value
        if actual > expected:
            for _ in range(actual - expected):
                await _UPLOAD_SEMAPHORE.acquire()
        elif actual < expected:
            for _ in range(expected - actual):
                _UPLOAD_SEMAPHORE.release()

    # ------------------------------------------------------------------ constants
    async def test_production_limits(self):
        self.assertEqual(MAX_PER_FILE, 100 * 1024 * 1024)
        self.assertEqual(MAX_BATCH, 300 * 1024 * 1024)
        self.assertEqual(MAX_FILES, 10)
        self.assertEqual(_UPLOAD_SLOTS, 2)

    # --------------------------------------------------------- multipart rendering
    async def test_route_streams_seekable_file_to_service(self):
        response = await self.client.post("/api/assistant/knowledge/files", files=[
            ("files", ("公司手册.txt", "公司报销需要真实凭证。".encode("utf-8"))),
            ("files", ("说明.md", "# 说明".encode("utf-8"))),
        ])
        self.assertEqual(response.status_code, 200, response.text)
        data = response.json()["data"]
        self.assertEqual(len(data["items"]), 2)
        self.assertEqual([c["filename"] for c in self.service.calls], ["公司手册.txt", "说明.md"])
        for c in self.service.calls:
            self.assertEqual(c["owner"], actor())
            self.assertEqual(c["document_id"], "")
            self.assertIsNone(c["revision"])

    async def test_replacement_route_sends_document_id_and_revision(self):
        response = await self.client.post(
            "/api/assistant/knowledge/files?document_id=doc-1&version=7",
            files={"files": ("新版本.txt", "v2".encode("utf-8"))})
        self.assertEqual(response.status_code, 200, response.text)
        c = self.service.calls[0]
        self.assertEqual(c["document_id"], "doc-1")
        self.assertEqual(c["revision"], 7)

    # ---------------------------------------------------------------- limit mapping
    async def test_per_file_limit_rejected(self):
        with mock.patch.object(routes_module, "MAX_PER_FILE", 256):
            response = await self.client.post("/api/assistant/knowledge/files",
                                              files={"files": ("big.txt", b"x" * 512)})
        self.assertEqual(response.status_code, 413, response.text)
        self.assertIn("100MiB", response.json()["error"])
        self.assertEqual(self.service.calls, [])

    async def test_batch_total_limit_rejected(self):
        with mock.patch.object(routes_module, "MAX_BATCH", 256):
            response = await self.client.post("/api/assistant/knowledge/files", files=[
                ("files", ("a.txt", b"a" * 200)), ("files", ("b.txt", b"b" * 200))])
        self.assertEqual(response.status_code, 413, response.text)
        self.assertIn("300MiB", response.json()["error"])
        self.assertEqual(self.service.calls, [])

    async def test_streaming_total_limit_rejected_during_parse(self):
        # The bounded() stream raises MultiPartException once the whole body exceeds
        # MAX_BATCH + MULTIPART_OVERHEAD, so an oversized body never reaches the service
        # and the parser closes its temp files on error.
        with mock.patch.object(routes_module, "MAX_BATCH", 64), \
                mock.patch.object(routes_module, "MULTIPART_OVERHEAD", 16):
            response = await self.client.post("/api/assistant/knowledge/files",
                                              files={"files": ("big.txt", b"x" * 4096)})
        self.assertEqual(response.status_code, 413, response.text)
        self.assertEqual(self.service.calls, [])

    async def test_max_files_rejected(self):
        with mock.patch.object(routes_module, "MAX_FILES", 2):
            response = await self.client.post("/api/assistant/knowledge/files", files=[
                ("files", ("a.txt", b"a")), ("files", ("b.txt", b"b")), ("files", ("c.txt", b"c"))])
        self.assertEqual(response.status_code, 413, response.text)
        self.assertEqual(self.service.calls, [])

    # ------------------------------------------------------------ bounded 2 slots
    async def test_third_concurrent_upload_rejected_when_slots_full(self):
        held = 0
        while held < _UPLOAD_SLOTS and _UPLOAD_SEMAPHORE._value > 0:
            await _UPLOAD_SEMAPHORE.acquire()
            held += 1
        self.assertEqual(held, _UPLOAD_SLOTS)
        try:
            response = await self.client.post("/api/assistant/knowledge/files",
                                              files={"files": ("a.txt", b"x")})
            self.assertEqual(response.status_code, 503, response.text)
            self.assertEqual(self.service.calls, [])
        finally:
            for _ in range(held):
                _UPLOAD_SEMAPHORE.release()

    # ------------------------------------------------- temp-file cleanup / handles
    async def test_oversize_and_success_leave_no_open_temp_handles(self):
        with mock.patch.object(routes_module, "MAX_PER_FILE", 256), _patch_tracking():
            response = await self.client.post("/api/assistant/knowledge/files",
                                              files={"files": ("big.txt", b"x" * 400)})
            self.assertEqual(response.status_code, 413, response.text)
            self.assertEqual(self.service.calls, [])
        # successful small upload also streams a parser-created temp file
        with _patch_tracking():
            response = await self.client.post("/api/assistant/knowledge/files",
                                              files={"files": ("ok.txt", "正常内容".encode("utf-8"))})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(len(self.service.calls), 1)
        created = list(_TrackingSpooledTemporaryFile.created)
        self.assertTrue(created, "expected the parser to spool upload files")
        for handle in created:
            self.assertTrue(handle.closed, f"temp file handle left open: {handle}")

    async def test_disconnect_cleans_up_temp_handles(self):
        # A client that disconnects while a multipart file part is being spooled should
        # never leak a temp file handle. We drive the real endpoint with a Starlette
        # Request whose receive() returns one body chunk and then http.disconnect.
        boundary = "XyZbOuNdArY"
        body = (
            f"--{boundary}\r\n"
            "Content-Disposition: form-data; name=\"files\"; filename=\"half.txt\"\r\n"
            "Content-Type: text/plain\r\n"
            "\r\n"
            + "x" * 2000
        ).encode("utf-8")
        state = {"sent": False}

        async def receive():
            if not state["sent"]:
                state["sent"] = True
                return {"type": "http.request", "body": body[:120], "more_body": True}
            return {"type": "http.disconnect"}

        scope = {
            "type": "http",
            "method": "POST",
            "path": "/api/assistant/knowledge/files",
            "raw_path": b"/api/assistant/knowledge/files",
            "query_string": b"",
            "headers": [(b"content-type", f"multipart/form-data; boundary={boundary}".encode())],
            "client": ("127.0.0.1", 1234),
            "server": ("test", 80),
            "scheme": "http",
        }
        request = StarletteRequest(scope, receive)
        with _patch_tracking():
            try:
                await self.files_endpoint(request)
            except Exception:
                pass  # a dropped upload may surface as an error; what matters is cleanup
        self.assertEqual(self.service.calls, [])
        created = list(_TrackingSpooledTemporaryFile.created)
        self.assertTrue(created, "expected parser to have started spooling before disconnect")
        for handle in created:
            self.assertTrue(handle.closed, f"temp file handle left open after disconnect: {handle}")

    async def test_task_cancel_closes_partial_upload(self):
        boundary = 'cancel-fixture'
        header = (f'--{boundary}\r\nContent-Disposition: form-data; name="files"; filename="a.txt"\r\n\r\n' + 'x' * 2000).encode()
        sent = False
        async def receive():
            nonlocal sent
            if not sent:
                sent = True
                return {'type': 'http.request', 'body': header, 'more_body': True}
            raise asyncio.CancelledError()
        request = StarletteRequest({'type': 'http', 'method': 'POST', 'path': '/api/assistant/knowledge/files',
            'query_string': b'', 'headers': [(b'content-type', f'multipart/form-data; boundary={boundary}'.encode())],
            'scheme': 'http', 'server': ('test', 80), 'client': ('127.0.0.1', 1234)}, receive)
        with _patch_tracking(), self.assertRaises(asyncio.CancelledError):
            await self.files_endpoint(request)
        self.assertTrue(_TrackingSpooledTemporaryFile.created)
        self.assertTrue(all(handle.closed for handle in _TrackingSpooledTemporaryFile.created))
        self.assertEqual(_UPLOAD_SEMAPHORE._value, _UPLOAD_SLOTS)


if __name__ == "__main__":
    unittest.main(verbosity=2)

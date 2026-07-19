import contextlib
import io
import os
import sys
import tempfile
import unittest
from pathlib import Path

import httpx


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from upload_event_module.services import query_record_by_record_id as query_module  # noqa: E402
from upload_event_module.services.http_client import FeishuHttpClient  # noqa: E402


class _FakeFeishuClient:
    def __init__(self):
        self.calls = []

    def request_json(self, method, url, *, headers=None, params=None, json_payload=None):
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": headers or {},
                "params": params or {},
                "json_payload": json_payload,
            }
        )
        if "tenant_access_token" in url:
            return {"code": 0, "tenant_access_token": "tenant-token"}
        if "/records/" in url:
            return {"code": 0, "data": {"record": {"record_id": "rec-1"}}}
        return {"code": 0, "data": {"node": {"obj_token": "app-token"}}}


class QueryRecordHttpClientTests(unittest.TestCase):
    def test_token_and_record_queries_use_unified_client(self):
        original = query_module._HTTP_CLIENT
        fake = _FakeFeishuClient()
        query_module._HTTP_CLIENT = fake
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                token, token_error = query_module.get_tenant_access_token(
                    "app-id", "secret"
                )
                data, record_error = query_module.get_bitable_record(
                    token,
                    "app-token",
                    "table-id",
                    "rec-1",
                )
        finally:
            query_module._HTTP_CLIENT = original

        self.assertIsNone(token_error)
        self.assertEqual(token, "tenant-token")
        self.assertIsNone(record_error)
        self.assertEqual(data["record"]["record_id"], "rec-1")
        self.assertEqual(fake.calls[0]["method"], "POST")
        self.assertEqual(fake.calls[1]["method"], "GET")
        self.assertEqual(fake.calls[1]["params"]["user_id_type"], "open_id")

    def test_multipart_upload_reopens_file_and_returns_json(self):
        captured = {}

        def handle(request: httpx.Request) -> httpx.Response:
            captured["authorization"] = request.headers.get("authorization")
            captured["content_type"] = request.headers.get("content-type")
            captured["body"] = request.content
            return httpx.Response(
                200,
                json={"code": 0, "data": {"file_token": "file-token-1"}},
            )

        temp_path = ""
        client = FeishuHttpClient(
            transport=httpx.MockTransport(handle),
            retries=0,
        )
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as temp_file:
                temp_file.write(b"clipflow-upload")
                temp_path = temp_file.name
            payload = client.request_file_json(
                "POST",
                "https://open.feishu.cn/open-apis/drive/v1/medias/upload_all",
                headers={"Authorization": "Bearer test-token"},
                data={
                    "file_name": "test.txt",
                    "parent_type": "bitable_file",
                    "parent_node": "app-token",
                    "size": "15",
                },
                file_path=temp_path,
                file_name="test.txt",
            )
        finally:
            client.close()
            if temp_path:
                with contextlib.suppress(OSError):
                    os.remove(temp_path)

        self.assertEqual(payload["data"]["file_token"], "file-token-1")
        self.assertEqual(captured["authorization"], "Bearer test-token")
        self.assertIn("multipart/form-data", captured["content_type"])
        self.assertIn(b"clipflow-upload", captured["body"])


if __name__ == "__main__":
    unittest.main()

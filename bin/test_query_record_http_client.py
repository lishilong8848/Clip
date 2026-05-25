import contextlib
import io
import sys
import unittest
from pathlib import Path


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from upload_event_module.services import query_record_by_record_id as query_module  # noqa: E402


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


if __name__ == "__main__":
    unittest.main()

import contextlib
import io
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from upload_event_module.services import query_record_by_record_id as query_module  # noqa: E402
from upload_event_module.services.http_client import FeishuHttpClient  # noqa: E402
from upload_event_module.services import http_client as client_module


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
    def test_per_request_timeout_does_not_change_the_shared_client_defaults(self):
        timeouts = []
        def handle(request):
            timeouts.append(request.extensions['timeout'])
            return httpx.Response(200, json={'code': 0})
        client = FeishuHttpClient(timeout=20, retries=0, transport=httpx.MockTransport(handle))
        try:
            client.request_json('GET', 'https://fixture.example/check', timeout=6)
            shared = client._client
            client.request_json('GET', 'https://fixture.example/check')
            self.assertIs(client._client, shared)
            self.assertEqual(timeouts[0]['read'], 6)
            self.assertEqual(timeouts[1]['read'], 20)
        finally:
            client.close()

    def test_feishu_frequency_code_retries_without_changing_search_page(self):
        for status in (200, 400, 429):
            with self.subTest(status=status):
                calls = []
                def handle(request):
                    calls.append(request)
                    return httpx.Response(status, headers={'x-ogw-ratelimit-reset': '2'}, json={'code': 99991400}) if len(calls) == 1 else httpx.Response(200, json={'code': 0, 'data': {'items': []}})
                client = FeishuHttpClient(transport=httpx.MockTransport(handle), retries=1)
                try:
                    with patch.object(client_module.time, 'sleep') as sleep:
                        result = client.request_json('POST', 'https://open.feishu.cn/records/search', params={'page_token': 'page-2'}, json_payload={'field_names': ['device']})
                    self.assertEqual(result['code'], 0)
                    self.assertEqual(len(calls), 2)
                    self.assertEqual(calls[0].url, calls[1].url)
                    self.assertEqual(calls[0].content, calls[1].content)
                    sleep.assert_called_once_with(2.0)
                finally:
                    client.close()

    def test_frequency_retries_are_bounded_and_do_not_retry_business_errors(self):
        for code, retries, expected in ((99991400, 2, 3), (99991400, 0, 1), (1254002, 2, 1)):
            with self.subTest(code=code, retries=retries):
                calls = []
                client = FeishuHttpClient(transport=httpx.MockTransport(lambda request: (calls.append(request) or httpx.Response(400, json={'code': code}))), retries=retries)
                try:
                    with patch.object(client_module.time, 'sleep') as sleep:
                        self.assertEqual(client.request_json('POST', 'https://open.feishu.cn/records/search')['code'], code)
                    self.assertEqual(len(calls), expected)
                    self.assertEqual(sleep.call_count, expected - 1)
                finally:
                    client.close()

    def test_retry_delay_uses_longer_server_cooldown_and_classifies_code(self):
        response = httpx.Response(400, headers={'Retry-After': '1', 'x-ogw-ratelimit-reset': '52'})
        self.assertEqual(FeishuHttpClient._retry_delay(response, 0), 52)
        self.assertEqual(client_module.classify_feishu_error(400, 99991400), 'rate_limit')

    def test_business_and_public_clients_reuse_the_same_verified_ca_policy(self):
        import ssl
        from openclaw_service.assistant.lighthouse_public import _verified_tls_context
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        first, second = FeishuHttpClient(), FeishuHttpClient()
        with patch.dict(os.environ, {}, clear=True), patch.object(client_module, '_tls_contexts', {}), \
                patch('ssl.create_default_context', return_value=context) as create, patch('httpx.Client') as clients:
            try:
                first._client_for_request()
                first._client_for_request()
                second._client_for_request()
                self.assertIs(_verified_tls_context(), context)
                self.assertEqual(create.call_count, 1)
                self.assertEqual(clients.call_count, 2)
                self.assertTrue(all(call.kwargs['verify'] is context for call in clients.call_args_list))
                self.assertTrue(context.check_hostname)
                self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
            finally:
                first.close()
                second.close()

    def test_query_logs_do_not_include_record_content_or_tokens(self):
        secret = "private-record-content"
        fake = _FakeFeishuClient()
        original = fake.request_json
        fake.request_json = lambda *args, **kwargs: {**original(*args, **kwargs), "data": {"record": {"body": secret}}}
        stdout = io.StringIO()
        with patch.object(query_module, "_HTTP_CLIENT", fake), contextlib.redirect_stdout(stdout), self.assertLogs(query_module._LOG, level="INFO") as logs:
            record, error = query_module.get_bitable_record("private-token", "app-secret", "table-id", "rec-private")
        self.assertIsNone(error)
        self.assertEqual(record["record"]["body"], secret)
        output = stdout.getvalue() + "\n".join(logs.output)
        for value in [secret, "private-token", "app-secret", "rec-private"]:
            self.assertNotIn(value, output)
        self.assertIn("elapsed_ms", output)

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

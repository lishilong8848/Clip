"""No live Feishu calls: bounded reads and non-replayed uncertain writes."""
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).parent))
from requests.exceptions import ReadTimeout
from upload_event_module.services import feishu_service as service
from upload_event_module.services.handlers.base import NoticePayload


class NoticeConnectionTests(unittest.TestCase):
    def test_same_table_writes_wait_but_other_tables_do_not(self):
        import threading
        from concurrent.futures import ThreadPoolExecutor
        first_started, attempted, second_started, release = (threading.Event() for _ in range(4))
        response = SimpleNamespace(success=lambda: True)
        def first(_token):
            first_started.set()
            release.wait(5)
            return response
        def second(_token):
            second_started.set()
            return response
        def write_second():
            attempted.set()
            return service._execute_bitable_write(second, 'same')
        with patch.object(service, '_resolve_handler', side_effect=lambda notice: (None, notice, '')), \
             patch.object(service.config, 'user_token', 'test'), ThreadPoolExecutor(max_workers=3) as pool:
            a = pool.submit(service._execute_bitable_write, first, 'same')
            try:
                self.assertTrue(first_started.wait(2))
                b = pool.submit(write_second)
                self.assertTrue(attempted.wait(2))
                self.assertFalse(second_started.wait(.1))
                other = pool.submit(service._execute_bitable_write, lambda _token: response, 'other')
                self.assertIs(other.result(timeout=2), response)
            finally:
                release.set()
            self.assertIs(a.result(timeout=2), response)
            self.assertIs(b.result(timeout=2), response)

    def test_notice_reuses_transport_preserving_tokens_json_and_multipart(self):
        import httpx
        from requests_toolbelt import MultipartEncoder
        from upload_event_module.services import http_client as http
        received = []

        def respond(request):
            received.append(request)
            return httpx.Response(200, headers={'Set-Cookie': 'account=first; Path=/'}, json={'code': 0, 'msg': 'success',
                'data': {'record': {'record_id': 'rec-test', 'fields': {'名称': '维保'}}}})

        pooled = http.FeishuHttpClient(transport=httpx.MockTransport(respond), retries=0)
        self.addCleanup(pooled.close)
        with patch.object(http, '_sdk_http_client', None), patch.object(http, 'FeishuHttpClient', return_value=pooled):
            for token in ('first-token', 'second-token'):
                response = service._feishu_request('POST', 'bitable/v1/apps/test-base/tables/test-table/records', token,
                    body={'fields': {'名称': '维保'}}, params={'client_token': 'e77259b9-49df-4669-b701-157398eed50a'})
                self.assertTrue(response.success())
                self.assertEqual(response.data.record.record_id, 'rec-test')
            encoder = MultipartEncoder(fields={'file': ('test.png', b'original-image', 'image/png')})
            http._sdk_request('POST', 'https://open.feishu.cn/media',
                headers={'Content-Type': encoder.content_type}, data=encoder, timeout=(5, 30))
        self.assertEqual([r.headers['authorization'] for r in received[:2]],
                         ['Bearer first-token', 'Bearer second-token'])
        self.assertTrue(all('cookie' not in r.headers for r in received))
        import json
        self.assertEqual(json.loads(received[0].content)['fields'], {'名称': '维保'})
        self.assertEqual(received[0].url.params['client_token'], 'e77259b9-49df-4669-b701-157398eed50a')
        self.assertIn(b'original-image', received[2].content)
        self.assertEqual(int(received[2].headers['content-length']), len(received[2].content))

    def test_sdk_transport_timeout_keeps_uncertain_write_and_never_retries(self):
        import httpx
        from upload_event_module.services import http_client as http
        calls = []
        def timeout(request):
            calls.append(request)
            raise httpx.ReadTimeout('fixture timeout')
        pooled = http.FeishuHttpClient(transport=httpx.MockTransport(timeout), retries=0)
        self.addCleanup(pooled.close)
        with patch.object(http, '_sdk_http_client', pooled), patch.object(service.config, 'user_token', 'test'):
            with self.assertRaises(service.BitableWriteUncertainError):
                service._execute_bitable_write(lambda _token: http._sdk_request('POST',
                    'https://open.feishu.cn/test', data=b'{}', timeout=(5, 30)), '维保通告')
        self.assertEqual(len(calls), 1)

    def test_sdk_retains_requests_custom_ca_policy(self):
        import os
        from upload_event_module.services import http_client as http
        with patch.dict(os.environ, {'REQUESTS_CA_BUNDLE': 'custom.pem', 'CURL_CA_BUNDLE': 'fallback.pem'}), \
             patch.object(http, '_sdk_http_client', None), \
             patch.object(http, 'verified_tls_context') as context, \
             patch.object(http, 'FeishuHttpClient') as client:
            http._sdk_request('GET', 'https://open.feishu.cn/test', timeout=30)
        context.assert_called_once_with(trust_env=False, cafile='custom.pem')
        self.assertEqual(client.call_args.kwargs['retries'], 0)
        self.assertIs(client.call_args.kwargs['verify'], context.return_value)

    def test_sdk_bounds_connect_without_shortening_write_response_timeout(self):
        builder = Mock()
        for method in ('enable_set_token', 'log_level', 'timeout'):
            getattr(builder, method).return_value = builder
        sdk = SimpleNamespace(Client=SimpleNamespace(builder=lambda: builder),
                              LogLevel=SimpleNamespace(ERROR='error'))
        with patch.object(service, 'lark', sdk):
            service._build_client()
            builder.timeout.assert_called_with((5.0, 30.0))
            service._build_client(timeout=2.0)
            builder.timeout.assert_called_with((2.0, 2.0))

    def test_release_guard_allows_exception_types_not_network_clients(self):
        from bin.tools import release_readiness_check as release
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            with patch.object(release, "PROJECT_ROOT", root), patch.object(release, "_git_tracked_files", return_value=["business.py"]):
                for source, allowed in (
                    ("from requests.exceptions import ReadTimeout as RequestTimeout\n", True),
                    ("import requests\nrequests.get('https://example.invalid')\n", False),
                    ("from requests import Session\n", False),
                    ("import requests as net\n", False),
                    ("from requests import Session as Client\n", False),
                    ("requests.get('https://example.invalid')\n", False),
                    ("example = 'import requests'\n", True),
                    ("from requests.exceptions import ReadTimeout\nimport requests\n", False),
                ):
                    (root / "business.py").write_text(source, encoding="utf-8")
                    self.assertEqual(release.check_requests_usage()[0], allowed)

    def test_record_read_retries_once_only(self):
        ok = SimpleNamespace(success=lambda: True)
        with patch.object(service.time, "sleep"), patch.object(service.config, "user_token", "test"):
            request = Mock(side_effect=[ReadTimeout(), ok])
            self.assertIs(service._query_with_transport_retry(request), ok)
            self.assertEqual(request.call_count, 2)
            request = Mock(side_effect=ReadTimeout())
            with self.assertRaisesRegex(RuntimeError, "查询飞书记录超时"):
                service._query_with_transport_retry(request)
            self.assertEqual(request.call_count, 2)

    def test_write_timeout_is_not_replayed(self):
        with patch.object(service.config, "user_token", "test"):
            for notice in ("事件通告", "维护通告", "变更通告", "设备检修", "设备轮巡", "设备调整"):
                with self.subTest(notice=notice):
                    request = Mock(side_effect=ReadTimeout())
                    with patch.object(service, 'log_warning') as log, self.assertRaisesRegex(RuntimeError, "远端结果暂不能确认"):
                        service._execute_bitable_write(request, notice)
                    self.assertIn('exception=ReadTimeout', log.call_args.args[0])
                    self.assertNotIn('test', log.call_args.args[0])
                    self.assertEqual(request.call_count, 1)

    def test_cold_record_read_never_loads_sdk_and_preserves_retry(self):
        fields = {"事件状态": "进行中", "过程更新时间": "existing-progress"}
        response = SimpleNamespace(success=lambda: True, data=SimpleNamespace(
            records=[SimpleNamespace(record_id="rec-cold", fields=fields)],
        ))
        for notice in ("事件通告", "维保通告", "变更通告", "设备检修", "设备轮巡", "设备调整", "上电通告", "下电通告"):
            with self.subTest(notice=notice):
                with (
                    patch.object(service, "lark", None),
                    patch.object(service, "config", SimpleNamespace(user_token="test-token", app_token="test-base")),
                    patch.object(service, "_resolve_handler", return_value=(None, "test-table", "")),
                    patch.object(service, "_ensure_lark_sdk_loaded", side_effect=AssertionError("must not load SDK")),
                    patch.object(service, "_feishu_request", side_effect=[ReadTimeout(), response, response]) as batch_get,
                    patch.object(service.time, "sleep"),
                ):
                    for _ in range(2):
                        ok, record = service.query_record_by_id("rec-cold", notice)
                        self.assertTrue(ok)
                        self.assertEqual(record["fields"], fields)
                        self.assertIsNone(service.lark)
                self.assertEqual(batch_get.call_count, 3)
                for call in batch_get.call_args_list:
                    self.assertEqual(call.args, ("POST", "bitable/v1/apps/test-base/tables/test-table/records/batch_get", "test-token"))
                    self.assertEqual(call.kwargs["body"], {"record_ids": ["rec-cold"], "automatic_fields": True})

    def test_record_read_uses_batch_id_without_losing_fields(self):
        fields = {"过程现场图片": [{"file_token": "old-photo"}], "过程更新时间": "old-progress"}
        record = SimpleNamespace(record_id="rec-test", fields=fields)
        for response_data, expected in (
            (SimpleNamespace(records=[record]), True),
            (SimpleNamespace(records=[], absent_record_ids=["rec-test"]), "1254043"),
            (SimpleNamespace(records=[], forbidden_record_ids=["rec-test"]), "1254302"),
            (SimpleNamespace(records=[]), "暂不能确认"),
        ):
            with self.subTest(expected=expected):
                response = SimpleNamespace(
                    success=lambda: True, data=response_data,
                )
                with (
                    patch.object(service.config, "user_token", "test"),
                    patch.object(service, "_resolve_handler", return_value=(None, "table-test", "")),
                    patch.object(service, "_feishu_request", return_value=response) as batch,
                    patch.object(service.time, "sleep"),
                ):
                    ok, result = service.query_record_by_id("rec-test", "维保通告")
                batch.assert_called_once()
                self.assertEqual(batch.call_args.kwargs["body"]["record_ids"], ["rec-test"])
                if expected is True:
                    self.assertTrue(ok)
                    self.assertEqual(result["fields"], fields)
                    self.assertTrue(result["record_version"])
                else:
                    self.assertFalse(ok)
                    self.assertIn(expected, result)

    def test_explicit_conflict_can_retry_but_other_rejection_cannot(self):
        ok = SimpleNamespace(success=lambda: True)
        rejected = SimpleNamespace(success=lambda: False, code=1254291)
        with patch.object(service.time, "sleep"), patch.object(service.config, "user_token", "test"):
            for notice in ("事件通告", "维护通告", "变更通告", "设备检修", "设备轮巡", "设备调整"):
                request = Mock(side_effect=[rejected, ok])
                self.assertIs(service._execute_bitable_write(request, notice), ok)
                self.assertEqual(request.call_count, 2)
            invalid = SimpleNamespace(success=lambda: False, code=1254015)
            request = Mock(return_value=invalid)
            self.assertIs(service._execute_bitable_write(request, "设备调整"), invalid)
            self.assertEqual(request.call_count, 1)

    def test_create_token_stable_and_scoped_to_operation_and_table(self):
        payload = NoticePayload(text="test", operation_id="one-submission")
        with patch.object(service.config, "app_token", "test-base"):
            token = service._notice_create_client_token(payload, "设备调整", "table-a")
            self.assertEqual(__import__("uuid").UUID(token).version, 4)
            self.assertEqual(token, service._notice_create_client_token(payload, "设备调整", "table-a"))
            self.assertNotEqual(token, service._notice_create_client_token(payload, "设备调整", "table-b"))
            self.assertNotEqual(token, service._notice_create_client_token(NoticePayload(text="test", operation_id="next-submission"), "设备调整", "table-a"))
            self.assertEqual(service._notice_create_client_token(NoticePayload(text="test"), "设备调整", "table-a"), "")

    def test_create_replay_preserves_original_uuid_and_legacy_undo_key_is_stable(self):
        response = SimpleNamespace(success=lambda: True, data=SimpleNamespace(record=SimpleNamespace(record_id="rec-original")))
        original = "e77259b9-49df-4669-b701-157398eed50a"
        with patch.object(service, "check_token_status"), patch.object(service.config, "user_token", "test"), \
             patch.object(service, "_resolve_handler", return_value=(None, "table", "")), \
             patch.object(service, "_filter_missing_optional_fields", side_effect=lambda _n, f: f), \
             patch.object(service, "_log_record_action"), \
             patch.object(service, "_ensure_lark_sdk_loaded", side_effect=AssertionError("no SDK")), \
             patch.object(service, "_feishu_request", return_value=response) as send:
            service.create_bitable_record_fields("维保通告", {"名称": "test"}, client_token=original)
            self.assertEqual(send.call_args.kwargs["params"]["client_token"], original)
            service.create_bitable_record_fields("维保通告", {"名称": "test"}, client_token="undo-create:record")
            legacy = send.call_args.kwargs["params"]["client_token"]
            service.create_bitable_record_fields("维保通告", {"名称": "test"}, client_token="undo-create:record")
            self.assertEqual(send.call_args.kwargs["params"]["client_token"], legacy)
            self.assertEqual(__import__("uuid").UUID(legacy).version, 4)

    def test_incomplete_create_response_is_uncertain_not_replayed(self):
        import httpx
        from upload_event_module.services import http_client as http
        for body in ({"code": 0, "data": {}}, {"msg": "missing code"}, {"code": 0, "data": "invalid"}):
            with self.subTest(body=body), patch.object(http, "_sdk_request", return_value=httpx.Response(200, json=body)) as send:
                with self.assertRaises(service.BitableWriteUncertainError):
                    service._execute_bitable_write(lambda token: service._feishu_request("POST", "bitable/v1/apps/b/tables/t/records", token, body={"fields": {"名称": "test"}}), "维保通告")
                send.assert_called_once()

    def test_all_notice_http_operations_and_media_work_without_sdk(self):
        import httpx
        import json
        from upload_event_module.services import http_client as http
        received = []
        fields = {"名称": "中文维保", "现场图片": [{"file_token": "original-photo"}]}
        def respond(request):
            received.append(request)
            if request.url.path.endswith("upload_all"):
                data = {"file_token": "uploaded-photo"}
            elif request.url.path.endswith("batch_get"):
                data = {"records": [{"record_id": "rec-original", "fields": fields, "last_modified_time": 123}]}
            else:
                data = {"record": {"record_id": "rec-original", "fields": fields}}
            return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})
        pooled = http.FeishuHttpClient(transport=httpx.MockTransport(respond), retries=0)
        self.addCleanup(pooled.close)
        handler = Mock()
        handler.build_create_fields.return_value = fields
        handler.build_update_fields.return_value = fields
        with patch.object(http, "_sdk_http_client", pooled), patch.object(service, "check_token_status"), \
             patch.object(service, "config", SimpleNamespace(app_token="test-base", user_token="test-token")), \
             patch.object(service, "_resolve_handler", return_value=(handler, "test-table", "")), \
             patch.object(service, "_filter_missing_optional_fields", side_effect=lambda _n, f: f), \
             patch.object(service, "_send_robot_message"), patch.object(service, "_log_record_action"), \
             patch.object(service, "_ensure_lark_sdk_loaded", side_effect=AssertionError("no SDK import")):
            for notice in ("事件通告", "维保通告", "变更通告", "设备检修", "设备轮巡", "设备调整", "上电通告", "下电通告"):
                checkpoint, guard = Mock(), Mock()
                payload = NoticePayload(text="test", operation_id="same-operation")
                payload._clipflow_create_checkpoint = checkpoint
                payload._clipflow_write_guard = guard
                self.assertEqual(service.create_bitable_record_by_payload(notice, payload), (True, "rec-original"))
                token = received[-1].url.params["client_token"]
                checkpoint.assert_called_once_with(fields, token)
                guard.assert_called_once()
                self.assertEqual(json.loads(received[-1].content), {"fields": fields})
                self.assertEqual(service.create_bitable_record_fields(notice, fields, client_token=token), (True, "rec-original"))
                self.assertEqual(received[-1].url.params["client_token"], token)
                for update in (lambda: service.update_bitable_record_by_payload("rec-original", notice, payload),
                               lambda: service.update_bitable_record_fields("rec-original", notice, fields)):
                    self.assertEqual(update(), (True, "rec-original"))
                    self.assertEqual(received[-1].method, "PUT")
                    self.assertTrue(received[-1].url.path.endswith("/rec-original"))
                    self.assertEqual(json.loads(received[-1].content), {"fields": fields})
                ok, record = service.query_record_by_id("rec-original", notice)
                self.assertTrue(ok)
                self.assertEqual(record["fields"], fields)
                self.assertTrue(record["record_version"])
                self.assertEqual(service.delete_bitable_record("rec-original", notice), (True, "rec-original"))
                self.assertEqual(received[-1].method, "DELETE")
            self.assertEqual(service.upload_media_to_feishu(b"original-image", "proof.png"), (True, "uploaded-photo"))
            self.assertIn(b"original-image", received[-1].content)
            self.assertIn(b"bitable_image", received[-1].content)
        self.assertTrue(all(r.headers["authorization"] == "Bearer test-token" for r in received))

    def test_non_event_optional_fields_do_not_add_a_schema_read_to_write_path(self):
        fields = {"名称": "测试", "过程现场图片": [{"file_token": "token"}]}
        with service._field_cache_lock:
            service._field_cache.pop("设备调整", None)
            service._field_cache.pop("事件通告", None)
        with patch.object(service, "_get_bitable_fields", return_value=list(fields)) as read_fields:
            self.assertIs(service._filter_missing_optional_fields("设备调整", fields), fields)
            read_fields.assert_not_called()
            service._filter_missing_optional_fields("事件通告", fields)
            read_fields.assert_called_once_with("事件通告")


if __name__ == "__main__":
    unittest.main()

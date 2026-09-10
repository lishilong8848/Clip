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
                    with self.assertRaisesRegex(RuntimeError, "远端结果暂不能确认"):
                        service._execute_bitable_write(request, notice)
                    self.assertEqual(request.call_count, 1)

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
            # Verify the installed SDK accepts this token without sending anything.
            service._ensure_lark_sdk_loaded()
            request = service.CreateAppTableRecordRequest.builder().client_token(token).build()
            self.assertEqual(request.client_token, token)

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

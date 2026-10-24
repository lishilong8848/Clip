"""Synthetic HTTP/auth only; sockets are forbidden and no settings are loaded."""
from copy import deepcopy
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal import assistant_diagnostics_cloud as cloud
from lan_bitable_template_portal import document_library_remote as transport
from openclaw_service.diagnostic_store import batch_digest


class SafeError(ValueError):
    def __init__(self, message, status=400, category=""):
        super().__init__(message)
        self.status, self.category = status, category


def batch():
    event = {"module": "assistant", "stage": "answer", "version": "ClipFlow_V2.500.20261024",
        "run_id": "abcd1234" * 4, "category": "model_protocol", "error_type": "AssistantError",
        "native_reason": "tools_not_registered", "native_fingerprint": "f" * 64,
        "http_status": 502, "frames": ["lighthouse_model.py:answer:42"],
        "count": 2, "first_at": 1792803600.0, "last_at": 1792803620.0}
    result = {"id": "fixture-machine:batch-0001", "machine_id": "fixture-machine",
        "started_at": 1792803600.0, "ended_at": 1792803660.0, "error_count": 2,
        "events": [event], "attempts": 0, "file_token": "", "auth_fingerprint": ""}
    result["payload_hash"] = batch_digest(result)
    return result


class FakeHTTP:
    def __init__(self):
        self.calls, self.uploads, self.records = [], [], []
        self.schema = dict(cloud.FIELDS)
        self.search_pages = []
        self.create_failure = self.field_failure = ""
        self.on_upload = None
        self.closed = 0

    def close(self):
        self.closed += 1

    def request_json(self, method, url, **kw):
        self.calls.append((method, url, deepcopy(kw)))
        if url.endswith("/fields"):
            if method == "GET":
                return {"code": 0, "data": {"items": [{"field_name": name, "type": kind}
                    for name, kind in self.schema.items()], "has_more": False}}
            field = kw["json_payload"]
            if self.field_failure == "rejected":
                return {"code": 1254302}
            if self.field_failure != "before":
                self.schema[field["field_name"]] = field["type"]
            if self.field_failure:
                raise TimeoutError("private field failure must not escape")
            return {"code": 0, "data": {}}
        if url.endswith("/records/search"):
            if self.search_pages:
                return self.search_pages.pop(0)
            identity = kw["json_payload"]["filter"]["conditions"][0]["value"][0]
            rows = [r for r in self.records if r["fields"]["批次标识"] == identity]
            return {"code": 0, "data": {"items": deepcopy(rows), "has_more": False}}
        if method == "POST" and url.endswith("/records"):
            record = {"record_id": "recFixture0001", "fields": deepcopy(kw["json_payload"]["fields"])}
            if self.create_failure != "before":
                self.records.append(record)
            if self.create_failure:
                raise TimeoutError("private create failure must not escape")
            return {"code": 0, "data": {"record": record}}
        raise AssertionError("Unexpected synthetic endpoint")

    def request_file_json(self, method, url, **kw):
        self.calls.append((method, url, deepcopy(kw)))
        path = Path(kw["file_path"])
        self.uploads.append((path, path.read_bytes()))
        if self.on_upload:
            self.on_upload()
        return {"code": 0, "data": {"file_token": f"boxFixtureFile{len(self.uploads):04d}"}}


class DiagnosticCloudTests(unittest.TestCase):
    def setUp(self):
        self.http = FakeHTTP()
        self.manager = Mock()
        self.manager._credentials.return_value = ("cli_fixture", "fixture-secret")
        self.manager.get_tenant_token.return_value = "fixture-tenant-token"
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("network forbidden")))
        self.enterContext(patch("socket.create_connection", side_effect=AssertionError("network forbidden")))
        for module in (cloud, transport):
            self.enterContext(patch.object(module, "_error", side_effect=lambda message, status=400, category="":
                SafeError(message, status, category)))
        self.enterContext(patch.object(transport.DocumentLibraryRemote, "_token_manager", lambda _self: self.manager))
        def http_client(remote):
            remote._http = self.http
            return self.http
        self.enterContext(patch.object(transport.DocumentLibraryRemote, "_http_client", http_client))
        self.guard = self.enterContext(patch.object(transport.DocumentLibraryRemote, "_require_write"))
        self.inspect = self.enterContext(patch.object(transport.DocumentLibraryRemote, "inspect_link",
            side_effect=AssertionError("fixed verified target needs no wiki calls")))
        self.remote = cloud.DiagnosticRemote()
        self.addCleanup(lambda: self.remote.close())

    def posts(self, suffix):
        return [call for call in self.http.calls if call[0] == "POST" and call[1].endswith(suffix)]

    def record(self, value, record_id="recFixture0001"):
        return {"record_id": record_id, "fields": {"批次标识": value["id"], "内容SHA256": value["payload_hash"]}}

    def test_lazy_fixed_target_and_close(self):
        self.assertEqual(self.http.calls, [])
        self.manager._credentials.assert_not_called()
        self.assertIsNone(self.remote._remote)
        for source in ({}, {"base_token": "other-base", "table_id": cloud.TABLE_ID},
                       {"base_token": cloud.BASE_TOKEN, "table_id": "tblOther"}):
            with self.assertRaises(SafeError):
                cloud.DiagnosticRemote(source)
        self.remote.close()
        with self.assertRaises(SafeError):
            self.remote.find_batch("batch", "a" * 64)
        self.assertEqual(self.http.calls, [])

    def test_one_batch_one_record_checkpoints_before_create_and_keeps_input(self):
        value, checkpoint = batch(), Mock()
        before = deepcopy(value)
        def save(**kw):
            self.assertEqual(self.posts("/records"), [])
            self.assertNotEqual(kw["auth_fingerprint"], "fixture-secret")
        checkpoint.side_effect = save
        self.assertEqual(self.remote.publish(value, checkpoint), "recFixture0001")
        self.assertEqual(value, before)
        checkpoint.assert_called_once()
        self.assertEqual(len(self.posts("/records")), 1)
        self.assertEqual(len(self.http.uploads), 1)
        path, raw = self.http.uploads[0]
        self.assertFalse(path.exists(), "attachment tempfile is removed")
        self.assertEqual(json.loads(raw), value["events"])
        self.assertNotIn("auth_fingerprint", raw.decode())
        self.assertEqual(raw, json.dumps(value["events"], ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode())
        fields = self.http.records[0]["fields"]
        self.assertEqual(set(fields), set(cloud.FIELDS))
        self.assertEqual(fields["诊断编号"], "abcd1234")
        self.assertIn("查询工具未注册", fields["错误摘要"])
        self.assertEqual(fields["涉及模块"], "灯塔助手")
        self.assertEqual(fields["开始时间"], int(value["started_at"] * 1000))
        self.assertEqual(fields["日志附件"], [{"file_token": "boxFixtureFile0001"}])
        create = self.posts("/records")[0]
        self.assertEqual(uuid.UUID(create[2]["params"]["client_token"]).version, 4)
        for method, url, kw in self.http.calls:
            if method == "POST":
                self.assertEqual(kw["retries"], 0)
            self.assertIn(cloud.BASE_TOKEN, url if "/medias/" not in url else kw["data"]["parent_node"])
        self.assertEqual(self.posts("/fields"), [], "publish never creates schema")
        self.inspect.assert_not_called()

    def test_existing_batch_no_upload_or_checkpoint(self):
        value, checkpoint = batch(), Mock()
        self.http.records = [self.record(value)]
        self.assertEqual(self.remote.publish(value, checkpoint), "recFixture0001")
        checkpoint.assert_not_called()
        self.assertEqual(self.http.uploads, [])
        self.assertEqual(self.posts("/records"), [])

    def test_search_refreshes_rejected_token_once_without_replaying_writes(self):
        value = batch()
        self.http.search_pages = [{"code": 99991663},
            {"code": 0, "data": {"items": [self.record(value)], "has_more": False}}]
        self.assertEqual(self.remote.publish(value, Mock()), "recFixture0001")
        self.assertEqual(len(self.posts("/records/search")), 2)
        self.assertEqual(sum(call.kwargs.get("force_refresh", False) for call in self.manager.get_tenant_token.call_args_list), 1)
        self.assertEqual(self.http.uploads, [])
        self.assertEqual(self.posts("/records"), [])

    def test_search_repeated_auth_rejection_and_refresh_rotation_fail_closed(self):
        self.http.search_pages = [{"code": 99991663}, {"code": 99991663}]
        with self.assertRaises(SafeError):
            self.remote.publish(batch(), Mock())
        self.assertEqual(len(self.posts("/records/search")), 2)
        self.http.search_pages = [{"code": 99991663}]
        def token(**kw):
            if kw.get("force_refresh"):
                self.manager._credentials.return_value = ("cli_rotated", "rotated-secret")
            return "fixture-token"
        self.manager.get_tenant_token.side_effect = token
        with self.assertRaises(SafeError):
            self.remote.publish(batch(), Mock())
        self.assertEqual(len(self.posts("/records/search")), 3, "rotation prevents the second search")
        self.assertEqual(self.http.uploads, [])
        self.assertEqual(self.posts("/records"), [])

    def test_search_refresh_budget_spans_all_pages(self):
        value = batch()
        self.http.search_pages = [{"code": 99991663}, {"code": 0, "data": {
            "items": [self.record(value)], "has_more": True, "page_token": "next"}}, {"code": 99991663}]
        with self.assertRaises(SafeError):
            self.remote.publish(value, Mock())
        self.assertEqual(len(self.posts("/records/search")), 3)
        self.assertEqual(sum(call.kwargs.get("force_refresh", False) for call in self.manager.get_tenant_token.call_args_list), 1)
        self.assertEqual(self.http.uploads, [])

    def test_checkpoint_cannot_change_frozen_payload_in_flight(self):
        value = batch()
        def checkpoint(**_kw):
            value["events"][0]["category"] = "private raw message"
            value["machine_id"] = "private owner name"
        self.remote.publish(value, checkpoint)
        self.assertNotIn("private", json.dumps(self.http.records))
        self.assertNotIn(b"private", self.http.uploads[0][1])

    def test_find_pages_to_end_uses_equal_filter_without_view_or_table_scan(self):
        value = batch()
        self.http.search_pages = [
            {"code": 0, "data": {"items": [self.record(value)], "has_more": True, "page_token": "next"}},
            {"code": 0, "data": {"items": [], "has_more": False}}]
        self.assertEqual(self.remote.find_batch(value["id"], value["payload_hash"]), "recFixture0001")
        calls = self.posts("/records/search")
        self.assertEqual(len(calls), 2)
        for _, _, kw in calls:
            self.assertNotIn("view_id", kw["params"])
            self.assertEqual(kw["json_payload"], {"field_names": ["批次标识", "内容SHA256"], "filter": {
                "conjunction": "and", "conditions": [{"field_name": "批次标识", "operator": "is", "value": [value["id"]]}]}})
        self.assertEqual(calls[1][2]["params"]["page_token"], "next")

    def test_duplicate_hash_conflict_and_malformed_pages_never_create(self):
        value = batch()
        good = self.record(value)
        for rows in ([good, self.record(value, "recOther")],
                     [{**good, "fields": {**good["fields"], "内容SHA256": "a" * 64}}],
                     [{**good, "fields": {**good["fields"], "批次标识": "other"}}]):
            with self.subTest(rows=rows), self.assertRaises(SafeError):
                self.http.search_pages = [{"code": 0, "data": {"items": rows, "has_more": False}}]
                self.remote.publish(value, Mock())
        for data in ({}, {"items": [], "has_more": True}, {"items": None, "has_more": False}):
            with self.subTest(data=data), self.assertRaises(SafeError):
                self.http.search_pages = [{"code": 0, "data": data}]
                self.remote.publish(value, Mock())
        self.assertEqual(self.http.uploads, [])
        self.assertEqual(self.posts("/records"), [])

    def test_later_page_duplicate_or_loop_cannot_return_first_match(self):
        value = batch()
        for second in ({"items": [self.record(value, "recOther")], "has_more": False},
                       {"items": [], "has_more": True, "page_token": "same"}):
            self.http.search_pages = [{"code": 0, "data": {"items": [self.record(value)], "has_more": True, "page_token": "same"}},
                                      {"code": 0, "data": second}]
            with self.assertRaises(SafeError):
                self.remote.find_batch(value["id"], value["payload_hash"])

    def test_upload_checkpoint_reused_after_restart_and_create_token_stable(self):
        value, receipt = batch(), {}
        self.http.create_failure = "before"
        with self.assertRaises(SafeError):
            self.remote.publish(value, lambda **kw: receipt.update(kw))
        self.remote.close()
        self.remote = cloud.DiagnosticRemote()
        self.http.create_failure = ""
        checkpoint = Mock()
        self.remote.publish({**value, **receipt, "attempts": 1}, checkpoint)
        checkpoint.assert_not_called()
        self.assertEqual(len(self.http.uploads), 1)
        creates = self.posts("/records")
        self.assertEqual(len(creates), 2)
        self.assertEqual(creates[0][2]["params"], creates[1][2]["params"])
        self.assertEqual(creates[0][2]["json_payload"], creates[1][2]["json_payload"])

    def test_create_timeout_after_acceptance_recovers_via_find(self):
        value, receipt = batch(), {}
        self.http.create_failure = "accepted"
        with self.assertRaises(SafeError):
            self.remote.publish(value, lambda **kw: receipt.update(kw))
        checkpoint = Mock()
        self.assertEqual(self.remote.publish({**value, **receipt}, checkpoint), "recFixture0001")
        self.assertEqual(len(self.posts("/records")), 1)
        self.assertEqual(len(self.http.uploads), 1)
        checkpoint.assert_not_called()

    def test_credential_rotation_reuploads_old_checkpoint(self):
        value, receipt = batch(), {}
        self.http.create_failure = "before"
        with self.assertRaises(SafeError):
            self.remote.publish(value, lambda **kw: receipt.update(kw))
        original = dict(receipt)
        self.manager._credentials.return_value = ("cli_new", "new-secret")
        self.http.create_failure = ""
        self.remote.publish({**value, **receipt}, lambda **kw: receipt.update(kw))
        self.assertEqual(len(self.http.uploads), 2)
        self.assertNotEqual(receipt["auth_fingerprint"], original["auth_fingerprint"])
        self.assertEqual(self.http.records[0]["fields"]["日志附件"], [{"file_token": receipt["file_token"]}])

    def test_rotation_during_upload_and_checkpoint_failure_stop_before_create(self):
        self.http.on_upload = lambda: setattr(self.manager._credentials, "return_value", ("cli_new", "new-secret"))
        checkpoint = Mock()
        with self.assertRaises(SafeError):
            self.remote.publish(batch(), checkpoint)
        checkpoint.assert_not_called()
        self.http.on_upload = None
        with self.assertRaisesRegex(OSError, "checkpoint"):
            self.remote.publish(batch(), Mock(side_effect=OSError("checkpoint")))
        self.assertEqual(self.posts("/records"), [])
        self.assertTrue(all(not path.exists() for path, _ in self.http.uploads))

    def test_schema_only_missing_fields_created_then_read_back(self):
        self.http.schema.pop("日志附件")
        self.http.schema["保留业务字段"] = 1
        result = self.remote.ensure_fields()
        self.assertEqual(result["created_fields"], ["日志附件"])
        self.assertEqual(len(self.posts("/fields")), 1)
        self.assertEqual(self.posts("/fields")[0][2]["json_payload"], {"field_name": "日志附件", "type": 17})
        self.remote.ensure_fields()
        self.assertEqual(len(self.posts("/fields")), 1)
        self.assertIn("保留业务字段", self.http.schema)

    def test_schema_type_conflict_and_missing_primary_fail_before_post(self):
        self.http.schema["错误次数"] = 1
        self.http.schema.pop("日志附件")
        with self.assertRaises(SafeError):
            self.remote.ensure_fields()
        self.http.schema = {k: v for k, v in cloud.FIELDS.items() if k != "文本"}
        with self.assertRaises(SafeError):
            self.remote.ensure_fields()
        self.assertEqual(self.posts("/fields"), [])

    def test_new_date_fields_show_minutes_without_modifying_existing_columns(self):
        self.http.schema.pop("开始时间")
        self.http.schema.pop("结束时间")
        self.remote.ensure_fields()
        posts = self.posts("/fields")
        self.assertEqual([call[2]["json_payload"]["field_name"] for call in posts], ["开始时间", "结束时间"])
        for _, _, kw in posts:
            self.assertEqual(kw["json_payload"]["property"], {"auto_fill": False, "date_formatter": "yyyy/MM/dd HH:mm"})
            self.assertEqual(kw["retries"], 0)
        self.remote.ensure_fields()
        self.assertEqual(len(self.posts("/fields")), 2)

    def test_schema_unknown_write_is_not_blindly_recreated(self):
        for failure in ("before", "rejected"):
            with self.subTest(failure=failure):
                self.remote.close()
                self.remote = cloud.DiagnosticRemote()
                self.http.schema.pop("日志附件", None)
                self.http.field_failure = failure
                initial = len(self.posts("/fields"))
                for _ in range(2):
                    with self.assertRaises(SafeError):
                        self.remote.ensure_fields()
                self.assertEqual(len(self.posts("/fields")), initial + 1)
        self.http.schema["日志附件"] = 17
        self.assertEqual(self.remote.ensure_fields()["created_fields"], [])

    def test_schema_lost_success_reconciles_by_readback(self):
        self.http.schema.pop("日志附件")
        self.http.field_failure = "accepted"
        self.assertEqual(self.remote.ensure_fields()["created_fields"], ["日志附件"])
        self.assertEqual(len(self.posts("/fields")), 1)

    def test_polluted_events_rejected_even_if_rehashed(self):
        cases = {"message": "private question sk-secret", "summary": "private body", "body": "secret",
            "stage": "private_question", "module": "password", "native_reason": "private text",
            "version": "sk-secret", "category": "Bearer token", "error_type": "my private question",
            "frames": ["C:/Users/private/secret.py:run:1"], "run_id": "ou_private", "native_fingerprint": "secret",
            "http_status": True, "count": -1, "first_at": float("nan")}
        for key, item in cases.items():
            value = batch()
            value["events"][0][key] = item
            if key != "first_at":
                value["payload_hash"] = batch_digest(value)
            with self.subTest(key=key), self.assertRaises(SafeError):
                self.remote.publish(value, Mock())
        self.assertEqual(self.http.calls, [])
        self.manager._credentials.assert_not_called()

    def test_changed_frozen_payload_fails_before_cloud(self):
        for key, changed in (("machine_id", "other-machine"), ("started_at", 1792803599.0)):
            value = batch()
            value[key] = changed
            with self.subTest(key=key), self.assertRaises(SafeError):
                self.remote.publish(value, Mock())
        value = batch()
        value["events"][0]["category"] = "connection"
        with self.assertRaises(SafeError):
            self.remote.publish(value, Mock())
        self.assertEqual(self.http.calls, [])


if __name__ == "__main__":
    unittest.main()

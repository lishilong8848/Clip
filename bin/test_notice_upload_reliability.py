import copy
import sys
import threading
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_event_remote_atomicity as fixtures
import lan_bitable_template_portal.server as server
import upload_event_module.services.feishu_service as feishu
from clipflow_backend.main import FastAPIPortalController
from fastapi.testclient import TestClient
from lan_bitable_template_portal.server import PortalRuntime
from lan_bitable_template_portal.state_store import LanPortalStateStore
from upload_event_module.config import MAINTENANCE_NOTICE_FIELDS
from upload_event_module.services.handlers import get_notice_handler
from upload_event_module.services.handlers.maintenance_notice import MaintenanceNoticeHandler


class NoticeUploadReliabilityTests(unittest.TestCase):
    setUp = fixtures.EventRemoteAtomicityTests.setUp
    tearDown = fixtures.EventRemoteAtomicityTests.tearDown

    def request(self, notice_type="维保通告", label="start", action="upload"):
        request = {
            "action_type": action, "operation_id": "qt_notice:" + label,
            "response_time": "2026-09-22 10:00", "robot_group_choice": "skip",
            "data_dict": {
                "record_id": "local-audit", "active_item_id": "local-audit",
                "notice_type": notice_type, "buildings": ["A楼"],
                "text": f"【{notice_type}】状态：开始\n【名称】A楼测试\n【时间】2026-09-22 10:00~2026-09-22 18:00\n【柜号】A-201包间A01\n【数量】1",
                "_is_placeholder_record": True,
            },
        }
        if action != "upload":
            request["data_dict"].update(record_id="rec-shared", target_record_id="rec-shared",
                                        active_item_id="rec-shared", _is_placeholder_record=False)
            request["data_dict"]["text"] = request["data_dict"]["text"].replace("状态：开始", "状态：更新") + "\n【进度】" + label
            attachment = PortalRuntime.state_store.put_notice_upload_attachment(
                open_id="qt-local", file_name=label + ".png", mime_type="image/png", content=label.encode(),
            )
            request["screenshot_upload_id"] = attachment["upload_id"]
        return request

    @staticmethod
    def write(remote):
        def update(record_id, notice_type, payload):
            fields = get_notice_handler(notice_type).build_update_fields(payload)
            payload._clipflow_written_fields = dict(fields)
            remote.update(fields)
            return True, record_id
        return update

    def test_power_start_and_replay_enqueue_one_handoff(self):
        for notice_type in ("上电通告", "下电通告"):
            request = self.request(notice_type, notice_type)
            with (
                patch.object(PortalRuntime, "local_upload_created_targets", {}),
                patch.object(PortalRuntime, "_existing_target_for_local_upload", return_value=""),
                patch.object(PortalRuntime, "ensure_cabinet_notice_worker"),
                patch.object(server, "create_bitable_record_by_payload", return_value=(True, "rec-" + notice_type)) as create,
                patch.object(server, "query_record_by_id", return_value=(True, {"fields": {"通告状态": "开始"}})),
            ):
                self.assertTrue(PortalRuntime.execute_local_notice_upload(request)["ok"])
                self.assertTrue(PortalRuntime.execute_local_notice_upload(request)["ok"])
            create.assert_called_once()
        events = PortalRuntime.state_store.list_outbox_events(PortalRuntime.cabinet_notice_queue_channel)
        self.assertEqual(len(events), 2)
        self.assertEqual({event["payload"]["notice_type"] for event in events}, {"上电通告", "下电通告"})
        self.assertTrue(all(event["payload"]["event_action"] == "start" for event in events))

    def test_handoff_failure_cannot_fail_notice(self):
        with (
            patch.object(PortalRuntime, "_existing_target_for_local_upload", return_value=""),
            patch.object(server, "create_bitable_record_by_payload", return_value=(True, "rec-power")),
            patch.object(PortalRuntime, "enqueue_cabinet_notice_batch", side_effect=OSError("handoff unavailable")),
        ):
            self.assertTrue(PortalRuntime.execute_local_notice_upload(self.request("上电通告"))["ok"])

    def test_all_qt_creates_forward_stable_client_token(self):
        for notice_type in ("维保通告", "变更通告", "设备检修", "上电通告", "下电通告", "设备轮巡", "设备调整", "事件通告"):
            request = (fixtures._request("upload", operation_id="qt-event", record_id="local-event")
                       if notice_type == "事件通告" else self.request(notice_type, notice_type))
            captured = {}

            def create(kind, payload):
                captured.update(operation_id=payload.operation_id, token=feishu._notice_create_client_token(payload, kind, "audit-table"))
                payload._clipflow_written_fields = get_notice_handler(kind).build_create_fields(payload)
                return True, "rec-" + kind

            with (
                patch.object(PortalRuntime, "_existing_target_for_local_upload", return_value=""),
                patch.object(server, "create_bitable_record_by_payload", side_effect=create),
                patch.object(PortalRuntime, "enqueue_cabinet_notice_batch"),
                patch.object(server, "send_robot_message_by_payload", return_value={"robot_skipped": True}),
            ):
                self.assertTrue(PortalRuntime.execute_local_notice_upload(request)["ok"], notice_type)
            self.assertEqual(captured["operation_id"], request["operation_id"])
            self.assertEqual(uuid.UUID(captured["token"]).version, 4)

    def test_http_failure_preserves_written_target_and_retry_identity(self):
        request = self.request()
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        with (
            patch.object(controller, "_local_only_response", return_value=None),
            patch.object(PortalRuntime, "_existing_target_for_local_upload", return_value=""),
            patch.object(server, "create_bitable_record_by_payload", return_value=(True, "rec-written")) as create,
            patch.object(server, "query_record_by_id", return_value=(True, {"fields": {"通告状态": "开始"}})),
            patch.object(PortalRuntime, "_remember_local_upload_target", side_effect=[OSError("local commit failure"), "v1"]),
        ):
            client = TestClient(controller._build_app())
            result = client.post("/api/qt/commands", json={"command": "notice_upload", "payload": request}).json()["data"]
            self.assertFalse(result["ok"])
            self.assertTrue(result["remote_written"])
            self.assertTrue(result["retry_same_operation"])
            self.assertEqual(result["real_record_id"], "rec-written")
            self.assertEqual(result["operation_id"], request["operation_id"])
            self.assertEqual(PortalRuntime.state_store.list_outbox_events("notice_robot"), [])
            result = client.post("/api/qt/commands", json={"command": "notice_upload", "payload": request}).json()["data"]
            self.assertTrue(result["ok"], result)
            create.assert_called_once()
        self.assertEqual(len(PortalRuntime.state_store.list_outbox_events("notice_robot")), 1)

    def test_concurrent_update_rejects_stale_read_then_preserves_both_attachments(self):
        field = MAINTENANCE_NOTICE_FIELDS["notice_images"]
        remote = {field: [{"file_token": "original"}]}
        first_read, release_first = threading.Event(), threading.Event()
        results, errors = {}, []
        first, second = self.request(label="first", action="update"), self.request(label="second", action="update")

        def query(record_id, notice_type):
            if threading.current_thread().name == "first":
                first_read.set()
                self.assertTrue(release_first.wait(5))
            return True, {"record_id": record_id, "fields": copy.deepcopy(remote)}

        def run_first():
            try:
                results["first"] = PortalRuntime.execute_local_notice_upload(first)
            except Exception as exc:
                errors.append(exc)

        with (
            patch.object(server, "query_record_by_id", side_effect=query) as read,
            patch.object(server, "upload_media_to_feishu", side_effect=lambda *a, **k: (True, threading.current_thread().name)),
            patch.object(server, "update_bitable_record_by_payload", side_effect=self.write(remote)) as write,
        ):
            worker = threading.Thread(target=run_first, name="first")
            worker.start()
            try:
                self.assertTrue(first_read.wait(5))
                blocked = PortalRuntime.execute_local_notice_upload(second)
                self.assertFalse(blocked["ok"])
                self.assertIn("正在处理", blocked["message"])
                self.assertEqual(read.call_count, 1)
            finally:
                release_first.set()
                worker.join(10)
            self.assertFalse(worker.is_alive())
            self.assertEqual(errors, [])
            self.assertTrue(results["first"]["ok"])
            self.assertTrue(PortalRuntime.execute_local_notice_upload(second)["ok"])
            self.assertEqual(write.call_count, 2)
        self.assertEqual([item["file_token"] for item in remote[field]], ["original", "first", threading.current_thread().name])

    def test_later_update_is_not_overwritten_by_earlier_projection(self):
        remote, results = {}, {}
        first, second = self.request(label="older", action="update"), self.request(label="newer", action="update")
        release_original = PortalRuntime._release_event_operation_lock
        interleaved = False

        def release(key, owner):
            nonlocal interleaved
            release_original(key, owner)
            if key and owner and not interleaved:
                interleaved = True
                results["newer"] = PortalRuntime.execute_local_notice_upload(second)

        with (
            patch.object(server, "query_record_by_id", side_effect=lambda rid, _n: (True, {"record_id": rid, "fields": copy.deepcopy(remote)})),
            patch.object(server, "upload_media_to_feishu", return_value=(True, "token")),
            patch.object(server, "update_bitable_record_by_payload", side_effect=self.write(remote)),
            patch.object(PortalRuntime, "_release_event_operation_lock", side_effect=release),
        ):
            results["older"] = PortalRuntime.execute_local_notice_upload(first)
        self.assertTrue(all(result["ok"] for result in results.values()), results)
        active = PortalRuntime.state_store.find_qt_active_items(record_id="rec-shared")
        self.assertEqual(remote[MAINTENANCE_NOTICE_FIELDS["progress"]], "newer")
        self.assertIn("【进度】newer", active[0]["payload"]["text"])

    def test_robot_is_deferred_and_restart_retry_never_reuploads(self):
        request = self.request()
        request["robot_group_choice"] = "auto"
        feishu._ensure_lark_sdk_loaded()
        handler = MaintenanceNoticeHandler("维保通告")
        response = SimpleNamespace(success=lambda: True, data=SimpleNamespace(record=SimpleNamespace(record_id="rec-robot")))
        with (
            patch.object(PortalRuntime, "_existing_target_for_local_upload", return_value=""),
            patch.object(feishu, "check_token_status"),
            patch.object(feishu, "config", SimpleNamespace(user_token="audit", app_token="audit")),
            patch.object(feishu, "_resolve_handler", return_value=(handler, "audit-table", "")),
            patch.object(feishu, "_filter_missing_optional_fields", side_effect=lambda _n, fields: fields),
            patch.object(feishu, "_build_client", return_value=SimpleNamespace()),
            patch.object(feishu, "_execute_bitable_write", return_value=response) as write,
            patch.object(feishu, "_log_record_action"),
            patch.object(handler, "send_group_robot_message", side_effect=AssertionError("synchronous message")),
        ):
            self.assertTrue(PortalRuntime.execute_local_notice_upload(request)["ok"])
            state = PortalRuntime.state_store.get_notice_remote_operation(request["operation_id"])
            self.assertEqual(state["status"], "completed")
            self.assertEqual(state["target_record_id"], "rec-robot")
            self.assertEqual(len(PortalRuntime.state_store.list_outbox_events("notice_robot")), 1)
            with patch.object(server, "send_robot_message_by_payload", side_effect=[
                {"last_robot_error": "timeout"}, {"robot_sent": True},
            ]) as send:
                PortalRuntime.process_notice_robot_messages()
                self.assertEqual(PortalRuntime.state_store.get_notice_remote_operation(request["operation_id"])["status"], "completed")
                previous_store = PortalRuntime.state_store
                restarted = LanPortalStateStore(previous_store.db_path)
                try:
                    PortalRuntime.state_store = restarted
                    PortalRuntime.process_notice_robot_messages()
                    PortalRuntime.process_notice_robot_messages()
                finally:
                    PortalRuntime.state_store = previous_store
                    restarted.shutdown_write_worker(timeout=2)
                self.assertEqual(send.call_count, 2)
                self.assertEqual(send.call_args_list[0].kwargs["message_uuid"], send.call_args_list[1].kwargs["message_uuid"])
            write.assert_called_once()

    def test_expired_lock_cannot_be_renewed_or_used_for_write(self):
        request = self.request(label="expired", action="update")
        with (
            patch.object(server, "query_record_by_id", return_value=(True, {"fields": {}})),
            patch.object(server, "upload_media_to_feishu", return_value=(True, "token")),
            patch.object(PortalRuntime.state_store, "renew_notice_operation_lock", return_value=False),
            patch.object(server, "update_bitable_record_by_payload") as write,
        ):
            result = PortalRuntime.execute_local_notice_upload(request)
        self.assertFalse(result["ok"])
        self.assertIn("锁已过期", result["message"])
        write.assert_not_called()
        store = PortalRuntime.state_store
        with patch("lan_bitable_template_portal.state_store.time.time", return_value=1000):
            self.assertTrue(store.acquire_notice_operation_lock("expiry", owner="old")[0])
        with patch("lan_bitable_template_portal.state_store.time.time", return_value=1300):
            self.assertFalse(store.renew_notice_operation_lock("expiry", "old"))
            self.assertTrue(store.acquire_notice_operation_lock("expiry", owner="new")[0])
            self.assertFalse(store.renew_notice_operation_lock("expiry", "old"))

    def test_web_updates_and_ends_queue_robot_after_write_and_replay_once(self):
        for action in ("update", "end"):
            operation_id = "web-" + action
            prepared = {"action": action, "notice_type": "维保通告", "work_type": "maintenance",
                        "target_record_id": "rec-web-" + action, "site_photo_count": 1,
                        "response_time": "2026-09-22 10:00", "robot_group_choice": "auto",
                        "text": f"【维保通告】状态：{'更新' if action == 'update' else '结束'}\n【名称】A楼测试\n【进度】已完成"}
            written = {}

            def write(record_id, notice_type, payload):
                self.assertTrue(payload._clipflow_defer_robot_message)
                self.assertEqual(payload.operation_id, operation_id)
                return self.write(written)(record_id, notice_type, payload)

            with (
                patch.object(server, "external_real_write_guard", return_value={"mock_external": False, "real_write_allowed": True}),
                patch.object(server, "query_record_by_id", return_value=(True, {"fields": {MAINTENANCE_NOTICE_FIELDS["status"]: "更新"}})),
                patch.object(PortalRuntime, "_upload_extra_images_for_notice", return_value=(True, "", ["new"], [])),
                patch.object(PortalRuntime, "_upload_change_confirmation_images", return_value=(True, "", [], [])),
                patch.object(PortalRuntime, "_create_backend_undo_checkpoint", return_value=""),
                patch.object(PortalRuntime, "_work_order_end_error", return_value=""),
                patch.object(server, "update_bitable_record_by_payload", side_effect=write) as update,
                patch.object(server, "send_robot_message_by_payload") as send,
            ):
                for _ in range(2):
                    ok, message, record_id = PortalRuntime._execute_durable_prepared_upload(
                        prepared, operation_id=operation_id, operation_type=action,
                    )
                    self.assertTrue(ok, message)
                    self.assertEqual(record_id, prepared["target_record_id"])
                update.assert_called_once()
                send.assert_not_called()
        self.assertEqual(len(PortalRuntime.state_store.list_outbox_events("notice_robot")), 2)

    def test_completion_and_robot_queue_commit_atomically(self):
        store = PortalRuntime.state_store
        store.begin_notice_remote_operation(operation_id="atomic", operation_type="start", lock_key="atomic", request={"notice_type": "维保通告"})
        store.mark_notice_remote_operation("atomic", status="remote_written", target_record_id="rec-atomic",
            result={"robot_background": True, "robot_delivery_state": "pending"})
        with patch.object(store, "_enqueue_outbox_event_locked", side_effect=OSError("queue unavailable")):
            with self.assertRaises(OSError):
                store.mark_notice_remote_operation("atomic", status="completed")
        self.assertEqual(store.get_notice_remote_operation("atomic")["status"], "remote_written")
        for _ in range(2):
            store.mark_notice_remote_operation("atomic", status="completed")
        self.assertEqual(len(store.list_outbox_events("notice_robot")), 1)

    def test_unconfigured_group_is_skipped_without_network(self):
        from upload_event_module.services import robot_webhook
        request = self.request()
        request["robot_group_choice"] = "auto"
        with (
            patch.object(PortalRuntime, "_existing_target_for_local_upload", return_value=""),
            patch.object(server, "create_bitable_record_by_payload", return_value=(True, "rec-skip")),
        ):
            self.assertTrue(PortalRuntime.execute_local_notice_upload(request)["ok"])
        with (
            patch.object(robot_webhook, "_resolve_group_name", return_value=""),
            patch.object(robot_webhook, "_find_chat_id_by_name") as find,
        ):
            PortalRuntime.process_notice_robot_messages()
        find.assert_not_called()
        self.assertEqual(PortalRuntime.state_store.get_notice_remote_operation("qt_notice:start")["result"]["robot_delivery_state"], "skipped")

    def test_two_successful_updates_each_keep_their_group_message(self):
        remote = {}
        with (
            patch.object(server, "query_record_by_id", side_effect=lambda rid, _n: (True, {"record_id": rid, "fields": copy.deepcopy(remote)})),
            patch.object(server, "upload_media_to_feishu", return_value=(True, "token")),
            patch.object(server, "update_bitable_record_by_payload", side_effect=self.write(remote)),
        ):
            for label in ("first", "second"):
                request = self.request(label=label, action="update")
                request["robot_group_choice"] = "auto"
                self.assertTrue(PortalRuntime.execute_local_notice_upload(request)["ok"])
        with patch.object(server, "send_robot_message_by_payload", return_value={"robot_sent": True}) as send:
            PortalRuntime.process_notice_robot_messages()
            self.assertEqual(send.call_count, 2)
            self.assertIn("【进度】first", send.call_args_list[0].args[1].text)
            self.assertIn("【进度】second", send.call_args_list[1].args[1].text)

    def test_expired_lock_after_waiting_for_feishu_client_blocks_actual_http(self):
        from unittest.mock import MagicMock
        feishu._ensure_lark_sdk_loaded()
        payload = PortalRuntime._prepared_to_notice_payload(self.request()["data_dict"])
        payload._clipflow_write_guard = PortalRuntime._check_request_notice_locks
        client = MagicMock()
        token = server._request_notice_locks.set([("expired", "owner")])
        try:
            with (
                patch.object(feishu, "check_token_status"),
                patch.object(feishu, "config", SimpleNamespace(user_token="audit", app_token="audit")),
                patch.object(feishu, "_resolve_handler", return_value=(MaintenanceNoticeHandler("维保通告"), "table", "")),
                patch.object(feishu, "_filter_missing_optional_fields", side_effect=lambda _n, fields: fields),
                patch.object(feishu, "_build_client", return_value=client),
                patch.object(PortalRuntime.state_store, "renew_notice_operation_lock", return_value=False),
            ):
                for send in (lambda: feishu.create_bitable_record_by_payload("维保通告", payload),
                             lambda: feishu.update_bitable_record_by_payload("record", "维保通告", payload)):
                    with self.assertRaisesRegex(server.PortalError, "锁已过期"):
                        send()
            client.bitable.v1.app_table_record.create.assert_not_called()
            client.bitable.v1.app_table_record.update.assert_not_called()
        finally:
            server._request_notice_locks.reset(token)


if __name__ == "__main__":
    unittest.main()

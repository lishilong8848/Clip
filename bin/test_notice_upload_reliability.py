import copy
import sys
import threading
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

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

    def test_replacement_reuses_original_create_identity_and_survives_restart(self):
        store = PortalRuntime.state_store
        root, current = 'notice_action:original', 'notice_action:latest'
        for identity in (root, current):
            store.begin_notice_remote_operation(operation_id=identity, operation_type='start')
        frozen = {'名称': 'original', '内容': 'original fields', '附件': [{'file_token': 'old-image'}]}
        token = str(uuid.uuid4())
        store.mark_notice_remote_operation(root, status='failed', target_record_id='manual_original',
            result={'create_fields': frozen, 'create_client_token': token})
        prepared = {'_remote_operation_id': current}
        with patch.object(feishu, 'create_bitable_record_fields', return_value=(True, 'rec-original')) as create:
            target = PortalRuntime._replacement_start_target(prepared, '维保通告', root)
            self.assertEqual(target, 'rec-original')
            create.assert_called_once_with('维保通告', frozen, client_token=token)
        self.assertEqual(store.get_notice_remote_operation(current)['target_record_id'], 'rec-original')
        self.assertEqual(store.get_notice_remote_operation(root)['result']['robot_delivery_state'], 'superseded')
        store.shutdown_write_worker(timeout=2)
        PortalRuntime.state_store = LanPortalStateStore(Path(self._tmp.name) / 'state.sqlite3')
        with patch.object(feishu, 'create_bitable_record_fields', side_effect=AssertionError('must not create twice')):
            self.assertEqual(PortalRuntime._replacement_start_target(prepared, '维保通告', root), 'rec-original')

    def test_replacement_without_original_fields_refuses_a_fresh_create(self):
        from lan_bitable_template_portal.portal_service import PortalError
        PortalRuntime.state_store.begin_notice_remote_operation(operation_id='notice_action:missing', operation_type='start')
        with patch.object(feishu, 'create_bitable_record_fields') as create:
            with self.assertRaisesRegex(PortalError, '原创建内容缺失'):
                PortalRuntime._replacement_start_target({'_remote_operation_id': 'notice_action:new'}, '维保通告', 'notice_action:missing')
        create.assert_not_called()

    def test_previous_generation_payload_reconstructs_only_original_create(self):
        from dataclasses import asdict
        payload = PortalRuntime._prepared_to_notice_payload(self.request()['data_dict'])
        root, current = 'notice_action:legacy', 'notice_action:new'
        for identity in (root, current):
            PortalRuntime.state_store.begin_notice_remote_operation(operation_id=identity, operation_type='start')
        PortalRuntime.state_store.mark_notice_remote_operation(root, status='failed',
            result={'robot_payload': asdict(payload)})
        token, fields = str(uuid.uuid4()), {'原始通告': payload.text}
        handler = SimpleNamespace(build_create_fields=lambda original: {'原始通告': original.text}, get_table_id=lambda _: 'fixture-table')
        with (
            patch.object(server, 'get_notice_handler', return_value=handler),
            patch.object(feishu, '_filter_missing_optional_fields', side_effect=lambda _, value: value),
            patch.object(feishu, '_notice_create_client_token', return_value=token),
            patch.object(feishu, 'create_bitable_record_fields', return_value=(True, 'rec-original')) as create,
        ):
            self.assertEqual(PortalRuntime._replacement_start_target({'_remote_operation_id': current}, '维保通告', root), 'rec-original')
        create.assert_called_once_with('维保通告', fields, client_token=token)

    def test_latest_fields_update_original_id_without_create_or_old_group_message(self):
        payload = PortalRuntime._prepared_to_notice_payload(self.request()['data_dict'])
        fields = {'名称': 'latest', '内容': 'latest content'}
        handler = SimpleNamespace(build_create_fields=lambda _: fields)
        with (
            patch.object(feishu, 'check_token_status'),
            patch.object(feishu, 'config', SimpleNamespace(user_token='synthetic')),
            patch.object(feishu, '_resolve_handler', return_value=(handler, 'fixture-table', '')),
            patch.object(feishu, '_filter_missing_optional_fields', side_effect=lambda _, value: value),
            patch.object(feishu, '_build_client', side_effect=AssertionError('no create client')),
            patch.object(feishu, 'update_bitable_record_fields', return_value=(True, 'rec-original')) as update,
            patch.object(feishu, '_send_robot_message') as send,
        ):
            self.assertEqual(feishu.create_bitable_record_by_payload('维保通告', payload, target_record_id='rec-original'), (True, 'rec-original'))
        update.assert_called_once_with('rec-original', '维保通告', fields)
        send.assert_called_once_with(handler, payload)
        self.assertEqual(payload._clipflow_written_fields, fields)

    def test_superseded_notice_does_not_deliver_old_pending_group_message(self):
        store = PortalRuntime.state_store
        root = 'notice_action:old-job'
        store.begin_notice_remote_operation(operation_id=root, operation_type='start')
        store.put_document('notice_action_job', 'old-job', {'superseded_by_job_id': 'new-job'})
        store.mark_notice_remote_operation(root, status='completed', result={
            'robot_delivery_state': 'pending', 'robot_background': True, 'robot_notice_type': '维保通告'})
        with patch.object(server, 'send_robot_message_by_payload') as send:
            PortalRuntime.process_notice_robot_messages()
        send.assert_not_called()
        self.assertEqual(store.get_notice_remote_operation(root)['status'], 'superseded')

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

    def test_native_web_api_skips_list_only_for_new_standalone_start(self):
        import clipflow_backend.main as backend
        native = Mock()
        native.create_action_job.return_value = ("job-original", False)
        native.get_job.return_value = {"business_audit_id": "audit-original", "phase": "processing"}
        native.expand_workbench_action_command.side_effect = lambda payload, **_kw: payload
        with patch.object(backend, "LanPortalStateStore", return_value=PortalRuntime.state_store):
            controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        with patch.object(PortalRuntime.auth_manager, "account_profile", return_value={"modules": ["maintenance"]}), \
             patch.object(PortalRuntime, "service", native), \
             patch.object(controller, "_current_session", return_value={"user": {"open_id": "fixture", "name": "fixture"}}), \
             patch.object(controller, "_authorized_scope_or_error", return_value="A"), \
             patch.object(controller, "_get_ongoing", return_value=[{"record_id": "rec-existing"}]) as ongoing:
            client = TestClient(controller._build_app())
            for action, choice, active, expected_reads in (("start", "unbound", "", 0), ("start", "bind", "", 1),
                                                          ("start", "unbound", "active-existing", 1), ("update", "unbound", "", 1), ("end", "unbound", "", 1)):
                ongoing.reset_mock()
                response = client.post("/api/workbench-actions", json={"scope": "A", "action": action, "work_type": "maintenance",
                    "command_format": "notice_command", "manual_binding_choice": choice, "active_item_id": active,
                    "patch": {"title": "fixture-title"}})
                self.assertEqual(response.status_code, 202, response.text)
                self.assertEqual(ongoing.call_count, expected_reads, (action, choice, active))
                self.assertEqual(native.expand_workbench_action_command.call_args.kwargs["ongoing_items"], [] if not expected_reads else [{"record_id": "rec-existing"}])
                submitted = native.create_action_job.call_args.args[0]
                self.assertEqual(submitted["patch"], {"title": "fixture-title"})
                self.assertEqual(submitted["_auth_open_id"], "fixture")
                self.assertTrue(submitted["_web_action_request"])

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
                patch.object(feishu, "_feishu_request", client),
                patch.object(PortalRuntime.state_store, "renew_notice_operation_lock", return_value=False),
            ):
                for send in (lambda: feishu.create_bitable_record_by_payload("维保通告", payload),
                             lambda: feishu.update_bitable_record_by_payload("record", "维保通告", payload)):
                    with self.assertRaisesRegex(server.PortalError, "锁已过期"):
                        send()
            client.assert_not_called()
        finally:
            server._request_notice_locks.reset(token)


class StandaloneStartRecoveryTests(unittest.TestCase):
    validation_errors = (
        "原关联目标已结束或未开始，不能作为本次开始通告复用，请重新核对关联。",
        "原关联目标与本次通告名称或楼栋不一致，未修改旧记录，请重新核对关联。",
    )
    source_id = "rec-fixture-source"
    tearDown = fixtures.EventRemoteAtomicityTests.tearDown

    def setUp(self):
        fixtures.EventRemoteAtomicityTests.setUp(self)
        for name in ("socket.create_connection", "socket.socket.connect", "socket.socket.connect_ex"):
            self.enterContext(patch(name, side_effect=AssertionError("real network forbidden")))
        self.handler = SimpleNamespace(
            build_create_fields=Mock(side_effect=lambda payload: {
                "原文": payload.text, "附件": list(payload.file_tokens or []),
            }),
            get_table_id=Mock(return_value="fixture-table"),
        )
        self.enterContext(patch.object(server, "get_notice_handler", return_value=self.handler))
        self.enterContext(patch.object(feishu, "config", SimpleNamespace(app_token="fixture-app")))
        self.enterContext(patch.object(feishu, "_filter_missing_optional_fields", side_effect=lambda _, fields: fields))
        self.token = self.enterContext(patch.object(feishu, "_notice_create_client_token", wraps=feishu._notice_create_client_token))
        self.create = self.enterContext(patch.object(feishu, "create_bitable_record_fields", return_value=(True, "rec-created")))

    def prepared(self, **overrides):
        title = "EA118-A楼HVDC蓄电池汇流排防护罩更换变更（二）"
        return {
            "action": "start", "manual": True, "work_type": "change", "notice_type": "变更通告",
            "scope": "A", "title": title, "building": "A楼", "buildings": ["A楼"], "building_codes": ["A"],
            "start_time": "2026-10-28 09:00", "end_time": "2026-10-28 23:00", "level": "I3",
            "text": f"【变更通告】状态：开始\n【标题】{title}\n【时间】2026-10-28 09:00~23:00",
            "record_id": "manual:lite:A:change:fixture", "source_record_id": "", "target_record_id": "",
            "operation_id": "latest-submit", "robot_group_choice": "skip", **overrides,
        }

    def replacement(self, *, status="failed", error="", result=None, target=None):
        store = PortalRuntime.state_store
        root, current = "notice_action:" + uuid.uuid4().hex, "notice_action:" + uuid.uuid4().hex
        store.begin_notice_remote_operation(operation_id=root, operation_type="start",
            request={"action": "start", "source_record_id": self.source_id, "record_id": self.source_id},
            target_record_id=self.source_id if target is None else target)
        store.mark_notice_remote_operation(root, status=status, error=error, result=result or {})
        store.begin_notice_remote_operation(operation_id=current, operation_type="start")
        return root, self.prepared(_remote_operation_id=current, replacement_create_operation_id=root)

    def worker_begin(self, prepared):
        class StopAfterBegin(BaseException):
            pass

        native = Mock()
        native.get_job.return_value = {"request": copy.deepcopy(prepared)}
        native.prepare_action_job.return_value = copy.deepcopy(prepared)
        native._synchronize_prepared_notice_text.side_effect = lambda value: value
        native._action_message_signature.return_value = "fixture-signature"
        recorded = {}

        def begin(**kwargs):
            recorded.update(PortalRuntime.state_store.begin_notice_remote_operation(**kwargs))
            # Inspect the real first ledger write without running upload or delivery.
            raise StopAfterBegin()

        with (
            patch.object(PortalRuntime, "service", native),
            patch.object(PortalRuntime, "_hold_for_active_event", return_value=False),
            patch.object(PortalRuntime, "_resolve_polling_work_order_mode", side_effect=lambda value, **_: value),
            patch.object(PortalRuntime, "_begin_notice_remote_operation", side_effect=begin),
        ):
            with self.assertRaises(StopAfterBegin):
                PortalRuntime._process_maintenance_action_job_impl(uuid.uuid4().hex)
        return recorded

    def test_start_worker_ledger_excludes_source_and_manual_record_ids(self):
        for record_id, source_id in ((self.source_id, self.source_id), ("manual:lite:A:change:fixture", "")):
            with self.subTest(record_id=record_id):
                operation = self.worker_begin(self.prepared(record_id=record_id, source_record_id=source_id))
                self.assertEqual(operation["target_record_id"], "")
                self.assertEqual(operation["request"]["source_record_id"], source_id)
                self.assertEqual(operation["operation_type"], "start")

    def test_update_end_worker_ledger_keeps_actual_target(self):
        for action in ("update", "end"):
            for explicit_target in (False, True):
                with self.subTest(action=action, explicit_target=explicit_target):
                    operation = self.worker_begin(self.prepared(action=action,
                        record_id=self.source_id if explicit_target else "rec-existing-target",
                        source_record_id=self.source_id, target_record_id="rec-existing-target" if explicit_target else ""))
                    self.assertEqual(operation["target_record_id"], "rec-existing-target")
                    self.assertEqual(operation["operation_type"], action)

    def test_legacy_precreate_validation_freezes_latest_payload_under_root_token(self):
        for status in ("failed", "superseded"):
            for error in self.validation_errors:
                with self.subTest(status=status, error=error):
                    self.create.reset_mock()
                    self.token.reset_mock()
                    root, prepared = self.replacement(status=status, error=error,
                        result={"created_record_id": self.source_id})
                    latest = PortalRuntime._prepared_to_notice_payload(prepared, file_tokens=["latest-image"])
                    before = copy.deepcopy(latest)
                    fields = {"原文": latest.text, "附件": ["latest-image"]}

                    def create(kind, frozen_fields, *, client_token):
                        saved = PortalRuntime.state_store.get_notice_remote_operation(root)["result"]
                        self.assertEqual(saved["create_fields"], fields)
                        self.assertEqual(saved["create_client_token"], client_token)
                        self.assertEqual(frozen_fields, fields)
                        self.assertEqual(uuid.UUID(client_token).version, 4)
                        return True, "rec-created"

                    self.create.side_effect = create
                    target = PortalRuntime._replacement_start_target(prepared, "变更通告", root, latest_payload=latest)
                    self.assertEqual(target, "rec-created")
                    self.assertEqual(latest, before)
                    self.create.assert_called_once()
                    self.token.assert_called_once()
                    token_payload, kind, table = self.token.call_args.args
                    self.assertEqual((token_payload.operation_id, kind, table), (root, "变更通告", "fixture-table"))
                    self.assertEqual(PortalRuntime.state_store.get_notice_remote_operation(prepared["_remote_operation_id"])["target_record_id"], target)

    def test_persisted_validation_marker_allows_checkpointed_root_create(self):
        for status in ("failed", "superseded"):
            with self.subTest(status=status):
                self.create.reset_mock()
                root, prepared = self.replacement(status=status, error="旧任务已被最新提交替代。",
                    result={"start_validation_failed": True, "created_record_id": self.source_id})
                latest = PortalRuntime._prepared_to_notice_payload(prepared)
                self.assertEqual(PortalRuntime._replacement_start_target(prepared, "变更通告", root, latest_payload=latest), "rec-created")
                self.create.assert_called_once()
                saved = PortalRuntime.state_store.get_notice_remote_operation(root)["result"]
                self.assertEqual(saved["create_fields"]["原文"], latest.text)
                self.assertEqual(saved["create_client_token"], self.create.call_args.kwargs["client_token"])

    def test_backend_persists_marker_only_for_exact_precreate_validation_errors(self):
        for error in self.validation_errors:
            with self.subTest(error=error):
                root, prepared = self.replacement(error="")
                prepared.pop("replacement_create_operation_id")
                prepared["_remote_operation_id"] = root
                with (
                    patch.object(server, "external_real_write_guard", return_value={"mock_external": False, "real_write_allowed": True}),
                    patch.object(PortalRuntime, "_existing_target_for_prepared_start", side_effect=server.PortalError(error)),
                    patch.object(server, "create_bitable_record_by_payload") as upload,
                ):
                    with self.assertRaisesRegex(server.PortalError, error):
                        PortalRuntime._execute_backend_prepared_upload(prepared)
                self.assertIs(PortalRuntime.state_store.get_notice_remote_operation(root)["result"]["start_validation_failed"], True)
                upload.assert_not_called()
        self.create.assert_not_called()

    def test_unreadable_target_does_not_mark_validation_failure_or_allow_replacement_create(self):
        for raises in (False, True):
            with self.subTest(query_raises=raises):
                root, prepared = self.replacement(error="", result={"start_validation_failed": True})
                bound = self.prepared(_remote_operation_id=root, target_record_id="rec-existing-target",
                    record_id=self.source_id, source_record_id=self.source_id)
                with (
                    patch.object(server, "external_real_write_guard", return_value={"mock_external": False, "real_write_allowed": True}),
                    patch.object(server, "query_record_by_id", return_value=(False, "Read timed out"),
                        side_effect=TimeoutError("Read timed out") if raises else None) as query,
                    patch.object(server, "create_bitable_record_by_payload") as upload,
                    patch.object(server, "update_bitable_record_fields") as update,
                ):
                    with self.assertRaises(server.PortalError) as failure:
                        PortalRuntime._execute_backend_prepared_upload(bound)
                query.assert_called_once_with("rec-existing-target", "变更通告")
                upload.assert_not_called()
                update.assert_not_called()
                saved = PortalRuntime.state_store.get_notice_remote_operation(root)["result"]
                self.assertIs(saved["start_validation_failed"], False)
                self.assertFalse(any(key in saved for key in ("create_fields", "create_client_token", "robot_payload")))
                PortalRuntime.state_store.mark_notice_remote_operation(root, status="failed", error=str(failure.exception))
                with self.assertRaises(server.PortalError):
                    PortalRuntime._replacement_start_target(prepared, "变更通告", root,
                        latest_payload=PortalRuntime._prepared_to_notice_payload(prepared))
        self.create.assert_not_called()
        self.token.assert_not_called()

    def test_precreate_validation_without_latest_payload_stays_blocked(self):
        root, prepared = self.replacement(error=self.validation_errors[0])
        with self.assertRaises(server.PortalError):
            PortalRuntime._replacement_start_target(prepared, "变更通告", root)
        self.create.assert_not_called()

    def test_unknown_or_nonterminal_original_cannot_start_a_new_create(self):
        for status, error, result in (
            ("failed", "", {}), ("failed", "Read timed out", {}), ("superseded", "未知失败", {}),
            ("failed", "前缀" + self.validation_errors[0], {}),
            ("executing", self.validation_errors[0], {"start_validation_failed": True}),
        ):
            with self.subTest(status=status, error=error):
                root, prepared = self.replacement(status=status, error=error, result=result)
                with self.assertRaises(server.PortalError):
                    PortalRuntime._replacement_start_target(prepared, "变更通告", root,
                        latest_payload=PortalRuntime._prepared_to_notice_payload(prepared))
        self.create.assert_not_called()

    def test_missing_original_operation_stays_blocked(self):
        with self.assertRaises(server.PortalError):
            PortalRuntime._replacement_start_target(self.prepared(), "变更通告", "notice_action:missing",
                latest_payload=PortalRuntime._prepared_to_notice_payload(self.prepared()))
        self.create.assert_not_called()

    def test_partial_create_or_write_evidence_cannot_be_replaced_by_latest_payload(self):
        for evidence in (
            {"create_fields": {"原文": "original"}}, {"create_client_token": str(uuid.uuid4())},
            {"robot_payload": {"text": ""}}, {"written_fields": {"原文": "original"}},
            {"record_id": self.source_id},
        ):
            with self.subTest(evidence=evidence):
                root, prepared = self.replacement(error=self.validation_errors[0],
                    result={"start_validation_failed": True, **evidence})
                with self.assertRaises(server.PortalError):
                    PortalRuntime._replacement_start_target(prepared, "变更通告", root,
                        latest_payload=PortalRuntime._prepared_to_notice_payload(prepared))
        self.create.assert_not_called()

    def test_source_contamination_replays_frozen_original_fields_and_token(self):
        fields, token = {"原文": "frozen original"}, str(uuid.uuid4())
        root, prepared = self.replacement(status="superseded", error=self.validation_errors[0], result={
            "created_record_id": self.source_id, "create_fields": fields, "create_client_token": token,
        })
        self.assertEqual(PortalRuntime._replacement_start_target(prepared, "变更通告", root), "rec-created")
        self.create.assert_called_once_with("变更通告", fields, client_token=token)
        self.token.assert_not_called()

    def test_lost_create_response_replays_root_checkpoint_even_after_latest_text_changes(self):
        root, prepared = self.replacement(error=self.validation_errors[0])
        latest = PortalRuntime._prepared_to_notice_payload(prepared)
        original = copy.deepcopy(latest)
        self.create.side_effect = [(False, "Read timed out after create"), (True, "rec-created")]
        with self.assertRaisesRegex(server.PortalError, "Read timed out"):
            PortalRuntime._replacement_start_target(prepared, "变更通告", root, latest_payload=latest)
        frozen = copy.deepcopy(PortalRuntime.state_store.get_notice_remote_operation(root)["result"])
        self.assertEqual(frozen["create_fields"]["原文"], original.text)
        self.assertTrue(frozen["create_client_token"])
        current = "notice_action:" + uuid.uuid4().hex
        PortalRuntime.state_store.begin_notice_remote_operation(operation_id=current, operation_type="start")
        prepared["_remote_operation_id"] = current
        latest.text += "\n【进度】另一次编辑，不能覆盖已冻结的创建内容"
        latest.operation_id = "another-latest-submit"
        changed = copy.deepcopy(latest)
        self.assertEqual(PortalRuntime._replacement_start_target(prepared, "变更通告", root, latest_payload=latest), "rec-created")
        self.assertEqual(latest, changed)
        self.assertEqual(self.create.call_count, 2)
        self.assertEqual(self.create.call_args_list[0], self.create.call_args_list[1])
        self.token.assert_called_once()
        self.handler.build_create_fields.assert_called_once()
        self.assertEqual(PortalRuntime._replacement_start_target(prepared, "变更通告", root, latest_payload=latest), "rec-created")
        self.assertEqual(self.create.call_count, 2)

    def test_revoked_actor_blocks_create_but_preserves_root_checkpoint_for_authorized_retry(self):
        root, prepared = self.replacement(error=self.validation_errors[0])
        latest = PortalRuntime._prepared_to_notice_payload(prepared)
        with patch.object(PortalRuntime, "_check_current_notice_actor",
                side_effect=server.PortalError("办理权限已撤销")) as denied:
            with self.assertRaisesRegex(server.PortalError, "办理权限已撤销"):
                PortalRuntime._replacement_start_target(prepared, "变更通告", root, latest_payload=latest)
        denied.assert_called_once()
        self.create.assert_not_called()
        frozen = copy.deepcopy(PortalRuntime.state_store.get_notice_remote_operation(root)["result"])
        self.assertEqual(frozen["create_fields"]["原文"], latest.text)
        self.assertTrue(frozen["create_client_token"])
        latest.text += "\n【进度】授权恢复后的新编辑"
        with patch.object(PortalRuntime, "_check_current_notice_actor") as allowed:
            self.assertEqual(PortalRuntime._replacement_start_target(prepared, "变更通告", root, latest_payload=latest), "rec-created")
        allowed.assert_called_once()
        self.create.assert_called_once_with("变更通告", frozen["create_fields"], client_token=frozen["create_client_token"])
        self.token.assert_called_once()

    def test_replacement_preserves_real_created_target_when_other_candidate_is_source(self):
        for saved_target, ledger_target in (("rec-real", self.source_id), (self.source_id, "rec-real"), ("", "rec-real")):
            with self.subTest(saved_target=saved_target, ledger_target=ledger_target):
                root, prepared = self.replacement(status="superseded", target=ledger_target,
                    result={"created_record_id": saved_target, "start_validation_failed": True})
                self.assertEqual(PortalRuntime._replacement_start_target(prepared, "变更通告", root), "rec-real")
                self.assertEqual(PortalRuntime.state_store.get_notice_remote_operation(prepared["_remote_operation_id"])["target_record_id"], "rec-real")
        self.create.assert_not_called()

    def test_start_reconcile_source_or_local_placeholder_uses_uncertain_create_lookup(self):
        for value in (self.source_id, "manual:lite:A:change:fixture", "local_fixture"):
            for location in ("operation", "prepared_target", "prepared_record"):
                with self.subTest(value=value, location=location):
                    prepared = self.prepared(source_record_id="" if location == "operation" else self.source_id, record_id="",
                        target_record_id=value if location == "prepared_target" else "")
                    if location == "prepared_record":
                        prepared["record_id"] = value
                    operation = {"status": "executing", "target_record_id": value if location == "operation" else "",
                        "request": {"source_record_id": self.source_id}}
                    with (
                        patch.object(PortalRuntime, "_existing_target_for_prepared_start", return_value=""),
                        patch.object(PortalRuntime, "_find_replayed_start_target", return_value=("rec-recovered", "")) as replay,
                        patch.object(server, "external_real_write_guard", return_value={"mock_external": False}),
                        patch.object(server, "query_record_by_id", return_value=(True, {"fields": {}, "record_version": "v1"})) as query,
                        patch.object(PortalRuntime, "_remote_record_matches_prepared", return_value=True),
                    ):
                        ok, target, _, blocked = PortalRuntime._reconcile_notice_remote_intent(prepared, operation)
                    replay.assert_called_once_with(prepared, "变更通告")
                    query.assert_called_once_with("rec-recovered", "变更通告")
                    self.assertEqual((ok, target, blocked), (True, "rec-recovered", False))

    def test_start_reconcile_unconfirmed_create_stays_blocked(self):
        for replay_result in (("", ""), ("", "候选记录存在歧义")):
            with self.subTest(replay_result=replay_result):
                with (
                    patch.object(PortalRuntime, "_existing_target_for_prepared_start", return_value=""),
                    patch.object(PortalRuntime, "_find_replayed_start_target", return_value=replay_result) as replay,
                    patch.object(server, "query_record_by_id", side_effect=AssertionError("must not read a local placeholder")) as query,
                ):
                    ok, target, message, blocked = PortalRuntime._reconcile_notice_remote_intent(
                        self.prepared(), {"status": "failed", "error": "Read timed out", "target_record_id": "local_fixture"})
                self.assertEqual((ok, target, blocked), (False, "", True))
                self.assertTrue(message)
                replay.assert_called_once()
                query.assert_not_called()

    def test_update_end_reconcile_keeps_actual_target(self):
        for action in ("update", "end"):
            with self.subTest(action=action):
                with (
                    patch.object(PortalRuntime, "_find_replayed_start_target") as replay,
                    patch.object(server, "external_real_write_guard", return_value={"mock_external": False}),
                    patch.object(server, "query_record_by_id", return_value=(True, {"fields": {}})) as query,
                    patch.object(PortalRuntime, "_remote_record_matches_prepared", return_value=True),
                ):
                    ok, target, _, blocked = PortalRuntime._reconcile_notice_remote_intent(
                        self.prepared(action=action, target_record_id="rec-real"), {"status": "executing", "target_record_id": "rec-real"})
                self.assertEqual((ok, target, blocked), (True, "rec-real", False))
                query.assert_called_once_with("rec-real", "变更通告")
                replay.assert_not_called()

    def test_backend_replacement_updates_only_recovered_target_and_fresh_start_posts_once(self):
        for replacement in (False, True):
            with self.subTest(replacement=replacement):
                self.create.reset_mock()
                root, prepared = self.replacement(error=self.validation_errors[0])
                if not replacement:
                    prepared.pop("replacement_create_operation_id")
                with (
                    patch.object(server, "external_real_write_guard", return_value={"mock_external": False, "real_write_allowed": True}),
                    patch.object(PortalRuntime, "_existing_target_for_prepared_start", return_value=""),
                    patch.object(PortalRuntime, "_upload_extra_images_for_notice", return_value=(True, "", [], [])),
                    patch.object(PortalRuntime, "_upload_change_confirmation_images", return_value=(True, "", [], [])),
                    patch.object(PortalRuntime, "_create_backend_undo_checkpoint", return_value=""),
                    patch.object(PortalRuntime, "_mark_local_notice_images_target_written"),
                    patch.object(PortalRuntime, "_check_current_notice_actor"),
                    patch.object(server, "create_bitable_record_by_payload", return_value=(True, "rec-created")) as upload,
                    patch.object(server, "update_bitable_record_fields") as direct_update,
                    patch.object(server, "send_robot_message_by_payload") as send,
                ):
                    self.assertEqual(PortalRuntime._execute_backend_prepared_upload(prepared), (True, "rec-created", "rec-created"))
                upload.assert_called_once()
                self.assertEqual(upload.call_args.kwargs.get("target_record_id", ""), "rec-created" if replacement else "")
                self.assertEqual(self.create.call_count, int(replacement))
                if replacement:
                    self.assertEqual(self.token.call_args.args[0].operation_id, root)
                direct_update.assert_not_called()
                send.assert_not_called()


if __name__ == "__main__":
    unittest.main()

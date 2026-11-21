"""Qt delivery receipts with a local card, mocked transport and no live backend."""
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
sys.path.insert(0, str(Path(__file__).resolve().parent))

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QApplication, QListWidgetItem
from clipflow_backend.process_controller import BackendProcessPortalController
from test_active_notice_model import _ReplaceRecordIdHarness
from upload_event_module.ui.active_notice_model import ActiveNoticeModel
from upload_event_module.ui.display_state import persistent_active_item_data


class QtEventDeliveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = QApplication.instance() or QApplication([])

    def setUp(self):
        self.window = w = _ReplaceRecordIdHarness()
        self.local = "local_event_card"
        self.op = "qt_notice:fixture-start"
        self.data = {"notice_type": "事件通告", "text": "【事件通告】状态：开始\n【标题】隔离事件",
                     "active_item_id": "fixture-active", "record_id": self.local,
                     "_is_placeholder_record": True, "_has_unuploaded_changes": True,
                     "_upload_operation_id": self.op}
        self.card = QListWidgetItem()
        w.list_active_event.addItem(self.card)
        self.write(self.data)
        w.list_active_event.itemChanged.connect(lambda _: w._active_notice_store().invalidate())
        w._closing = False
        w.cache_store = None
        w.current_screenshot_record_id = None
        w.pending_action_record_ids = {self.local}
        w.pending_replace_by_record_id.clear()
        w._set_last_ui_op = w._try_process_deferred_events = w.request_active_cache_save = Mock()
        w._upsert_active_cache_record = Mock(return_value=True)
        w.show_message = Mock()
        w._do_move_to_history = Mock()
        w._emit_system_alert = Mock()
        w._schedule_pending_update_after_upload = Mock()
        w._notify_lan_portal_upload_result = Mock()
        w._enqueue_ui_mutation = lambda _, callback: callback()
        w._post_request_finished = w.on_request_finished
        w.lan_template_portal_controller = self.client = Mock()
        self.errors = self.enterContext(patch("upload_event_module.ui.main_window_workflow.log_error"))
        self.addCleanup(self.errors.assert_not_called)

    def read(self):
        return self.card.data(Qt.ItemDataRole.UserRole)

    def write(self, data):
        self.card.setData(Qt.ItemDataRole.UserRole, dict(data))

    def pending(self):
        return dict(queued=True, event_delivery_pending=True, job_id=self.op,
                    event_wait_attempt=0, record_id=self.local, message="正在发送事件群消息")

    def final(self, **changes):
        return {"qt_event_delivery": True, "ok": True, "robot_sent": True,
                "remote_written": False, "archive_state": "pending", "record_id": self.local,
                "real_record_id": "", "message": "群消息已发送，多维表后台同步", **changes}

    def submit(self, reply, action="upload"):
        self.client.execute_qt_notice_upload.return_value = reply
        self.window._delegate_qt_notice_upload_to_backend(data_snapshot=self.data,
            screenshot_bytes=None, extra_images=[], action_type=action,
            response_time="", recover_selected=False, robot_group_choice="auto")

    def deliver(self, result, action="upload"):
        command = dict(operation_id=self.op, action_type=action, data_dict=self.data)
        return self.window._apply_event_wait_result(dict(job_id=self.op, attempt=0, command=command, result=result))

    def test_group_receipt_releases_card_and_keeps_local_identity(self):
        self.submit(self.pending())
        self.assertEqual(ActiveNoticeModel.action_label_for_record(self.read()), "正在发送事件群消息")
        self.assertEqual(ActiveNoticeModel.action_for_record(self.read()), "")
        self.deliver(self.final())
        record = self.read()
        self.assertEqual(record["record_id"], self.local)
        self.assertFalse(record.get("target_record_id"))
        self.assertTrue(record["_is_placeholder_record"])
        self.assertEqual(ActiveNoticeModel.action_label_for_record(record), "群消息已发送")
        self.assertNotIn("_event_wait_job_id", record)
        self.assertNotIn(self.local, self.window.pending_action_record_ids)
        self.assertEqual(ActiveNoticeModel.action_for_record({**record, "_has_unuploaded_changes":True}), "update")
        self.assertTrue(self.deliver(self.final())["ignored"])

    def test_receipt_before_ack_and_replayed_http_final_finish_end_once(self):
        self.deliver(self.final(name="结束"), "end")
        self.window._do_move_to_history.assert_called_once()
        self.submit(self.pending(), "end")
        self.submit(self.final(name="结束"), "end")
        self.window._do_move_to_history.assert_called_once()
        self.assertNotIn("_event_wait_job_id", self.read())

    def test_reopened_card_polls_original_id_and_can_retry_failed_end(self):
        self.submit(self.pending(), "end")
        self.write(persistent_active_item_data({**self.read(), "text":"【事件通告】状态：结束\n【标题】隔离事件"}))
        self.window.pending_action_record_ids.clear()
        self.client.get_qt_notice_operation.return_value = {"operation_type":"end", "status":"failed", "result":{
            "qt_event_delivery":True, "qt_result":self.final(ok=False, name="结束", robot_sent=False, message="隔离失败")}}
        with patch("upload_event_module.ui.main_window_records.threading.Thread",
                   side_effect=lambda *,target,**_: Mock(start=target)):
            self.window._recover_stale_upload_states()
        self.client.get_qt_notice_operation.assert_called_once_with(self.op)
        self.window._do_move_to_history.assert_not_called()
        self.assertFalse(self.read().get("_event_delivery_pending"))
        self.assertEqual(ActiveNoticeModel.action_for_record(self.read()), "end")
        self.client.execute_qt_notice_upload.assert_called_once()

    def test_queued_end_dispatches_after_start_without_remote_id(self):
        self.submit(self.pending())
        end = {**self.read(), "text":"【事件通告】状态：结束\n【标题】隔离事件",
               "_queued_after_upload":True,"_queued_upload_requested":False,"_queued_action":"end"}
        self.write(end)
        self.window._resolve_upload_fields_from_cache = Mock(return_value={"buildings":["A楼"]})
        self.deliver(self.final())
        self.window._dialog_block_reason = Mock(return_value="")
        self.window._show_screenshot_dialog = Mock()
        self.window.handle_action(self.read(), "end")
        self.window._show_screenshot_dialog.assert_called_once()
        self.assertFalse(self.window.pending_action_record_ids)
        self.assertTrue(self.window._queue_confirmed_upload_if_busy(
            end, screenshot_bytes=b"end", action_type="end", response_time="",
            buildings=["A楼"], extra_images=[], specialty="电气", change_level="",
            event_level="I3", event_source="BMS", recover_selected=False, robot_group_choice="auto",
        ))
        self.window.do_feishu_upload = Mock()
        self.window._dispatch_pending_update(self.local, "")
        self.window.do_feishu_upload.assert_called_once()
        payload, _, action = self.window.do_feishu_upload.call_args.args
        self.assertEqual(action, "end")
        self.assertEqual(payload["record_id"], self.local)
        self.assertFalse(payload.get("target_record_id"))
        self.assertNotEqual(payload["_upload_operation_id"], self.op)
        self.assertNotIn("_event_wait_job_id", payload)
        self.assertNotIn("_event_delivery_pending", payload)

    def test_failed_predecessor_holds_confirmed_end_for_retry(self):
        self.submit(self.pending())
        end = {**self.read(), "text": self.data["text"].replace("开始", "结束")}
        self.window.pending_update_after_upload[self.local] = {"data": end, "action_type": "end"}
        self.deliver(self.final(ok=False, robot_sent=False))
        self.window.do_feishu_upload = Mock()
        self.window._dispatch_pending_update(self.local, "")
        self.window.do_feishu_upload.assert_not_called()
        self.assertTrue(self.window.pending_update_after_upload[self.local]["waiting_for_retry"])
        self.assertEqual(ActiveNoticeModel.action_for_record(self.read()), "upload")
        self.op = "qt_notice:fixture-start-retry"
        self.write({**self.read(), "_upload_operation_id": self.op})
        self.deliver(self.final())
        self.window._dispatch_pending_update(self.local, "")
        self.assertEqual(self.window.do_feishu_upload.call_args.args[2], "end")

    def test_two_confirmed_updates_and_end_keep_their_own_text_screenshot_and_receipt(self):
        w = self.window
        w._pending_cache_refresh = False
        w._try_process_pending_force_uploads = Mock()
        w._resolve_upload_fields_from_cache = Mock(return_value={"buildings": ["A楼"], "level": "I3"})
        w._set_delete_interaction_enabled = w._resume_clipboard_timer = Mock()
        w._apply_cache_to_item = w._schedule_record_binding_validation = w._schedule_active_route_reconcile = Mock()
        w._dispatch_backend_notice_upload = lambda _, task, **kwargs: task()
        w._upload_qt_notice_attachment = Mock(side_effect=lambda _, raw, __: {"upload_id": raw.decode()})

        def backend(command):
            return {**self.pending(), "job_id": command["operation_id"]}

        self.submit(self.pending())
        self.client.execute_qt_notice_upload.side_effect = backend
        queued = [
            ("update", "更新", "第二条更新", b"update-2"),
            ("update", "更新", "第三条更新", b"update-3"),
            ("end", "结束", "确认结束", b"end"),
        ]
        w._dialog_block_reason = Mock(return_value="")
        w._show_screenshot_dialog = Mock()
        for action, status, content, screenshot in queued:
            text = self.data["text"].replace("状态：开始", "状态：" + status) + "\n【进展】" + content
            self.assertTrue(w._queue_pending_content(self.local, text, status, active_item_id=self.data["active_item_id"]))
            w.handle_action(self.read(), action)
            w._show_screenshot_dialog.assert_called_with(self.read(), action)
            w.do_feishu_upload(self.read(), screenshot, action)
            self.assertEqual(self.client.execute_qt_notice_upload.call_count, 1)

        self.deliver(self.final())
        for index, (action, _, content, screenshot) in enumerate(queued, 2):
            w._dispatch_pending_update(self.local, "")
            command = self.client.execute_qt_notice_upload.call_args.args[0]
            op = command["operation_id"]
            self.assertEqual(self.client.execute_qt_notice_upload.call_count, index)
            self.assertEqual(command["action_type"], action)
            self.assertTrue(command["data_dict"]["text"].endswith(content))
            self.assertEqual(command["screenshot_upload_id"], screenshot.decode())
            self.assertEqual(self.read()["_event_wait_job_id"], op)
            self.assertIn("确认结束", self.read()["text"])
            self.assertEqual(w._apply_event_wait_result({
                "job_id": op, "attempt": 0, "command": command, "result": self.final(
                    name="结束" if action == "end" else "更新"),
            }), {"ok": True})
        w._do_move_to_history.assert_called_once()
        self.assertEqual(w.pending_update_after_upload, {})

    def test_recovered_response_uses_authoritative_receipt(self):
        command = {"data_dict":self.data}
        recover = BackendProcessPortalController._recover_qt_notice_upload_result
        for status,terminal in [("executing",None),("completed",self.final()),
                                ("failed",self.final(ok=False,robot_sent=False))]:
            with self.subTest(status=status):
                self.client.get_qt_notice_operation.return_value = {"status":status, "result":{
                    "qt_event_delivery":True,"qt_result":terminal}}
                self.assertEqual(recover(self.client,command,action_type="end",operation_id=self.op),
                                 terminal or self.pending())
        self.client.execute_qt_notice_upload.assert_not_called()

    def test_preflight_failure_is_immediately_actionable(self):
        self.submit({"ok":False,"name":"上传","message":"请核对事件时间","record_id":self.local})
        self.assertFalse(self.read().get("_event_wait_job_id"))
        self.assertEqual(ActiveNoticeModel.action_for_record(self.read()), "upload")
        self.assertTrue(any("请核对事件时间" in str(call) for call in self.window.show_message.call_args_list))

    def test_late_receipt_cannot_finish_a_newer_operation(self):
        self.write({**self.read(), "_upload_operation_id":"qt_notice:new", "_event_wait_job_id":"qt_notice:new"})
        self.assertTrue(self.deliver(self.final(name="结束"), "end")["ignored"])
        self.window._do_move_to_history.assert_not_called()
        self.assertEqual(self.read()["_event_wait_job_id"], "qt_notice:new")

    def test_reopen_after_group_delivery_uses_receipt_without_target_or_resend(self):
        self.submit(self.pending())
        self.write(persistent_active_item_data(self.read()))
        self.window.pending_action_record_ids.clear()
        self.client.get_qt_notice_operation.return_value = {"status":"completed","operation_type":"start",
            "result":{"qt_event_delivery":True, "qt_result":self.final()}}
        with patch("upload_event_module.ui.main_window_records.threading.Thread",
                   side_effect=lambda *,target,**_: Mock(start=target)):
            self.window._recover_stale_upload_states()
        self.assertEqual(ActiveNoticeModel.action_label_for_record(self.read()), "群消息已发送")
        self.assertFalse(self.read().get("target_record_id"))
        self.client.execute_qt_notice_upload.assert_called_once()

    def test_explicit_skip_waits_for_archive_and_binds_only_real_id(self):
        self.submit(self.pending())
        self.assertFalse(self.read().get("_event_delivery_sent"))
        self.deliver(self.final(robot_sent=False,robot_skipped=True,remote_written=True,
                               real_record_id="recFixture",archive_state="completed",message="归档成功"))
        self.assertEqual(self.read()["record_id"],"recFixture")
        self.assertFalse(self.read().get("_event_delivery_sent"))
        self.assertNotIn("_event_wait_job_id",self.read())


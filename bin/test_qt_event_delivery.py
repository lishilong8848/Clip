import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_event_remote_atomicity as fixture
from lan_bitable_template_portal import event_priority, server
from lan_bitable_template_portal.portal_service import PortalError
from lan_bitable_template_portal.qt_event_delivery import ARCHIVES, MESSAGES, OPERATIONS, QtEventDelivery
from lan_bitable_template_portal.state_store import LanPortalStateStore
from upload_event_module.config import EVENT_NOTICE_FIELDS
from upload_event_module.services.handlers import get_notice_handler


class QtEventDeliveryTests(unittest.TestCase):
    target = "rec-event-atomic"

    def setUp(self):
        self.directory = self.enterContext(tempfile.TemporaryDirectory())
        self.stack = self.enterContext(ExitStack())
        self.db = Path(self.directory) / "test.sqlite3"
        self.runtime = server.PortalRuntime
        self.store = LanPortalStateStore(self.db)
        self.stack.callback(self.store.shutdown_write_worker)
        self.stack.enter_context(patch.object(self.runtime, "state_store", self.store))
        self.stack.enter_context(patch.object(self.runtime, "local_upload_locks", {}))
        self.stack.enter_context(patch.object(self.runtime, "local_upload_created_targets", {}))
        self.delivery = QtEventDelivery(self.runtime)
        remote = self.remote = {}
        self.create = self.stack.enter_context(patch.object(server, "create_bitable_record_by_payload",
            side_effect=fixture.EventRemoteAtomicityTests._fake_create(remote)))
        self.update = self.stack.enter_context(patch.object(server, "update_bitable_record_by_payload",
            side_effect=fixture.EventRemoteAtomicityTests._fake_update(remote)))
        self.query = self.stack.enter_context(patch.object(server, "query_record_by_id",
            side_effect=fixture.EventRemoteAtomicityTests._query(remote)))
        self.upload = self.stack.enter_context(patch.object(server, "upload_media_to_feishu",
            return_value=(True, "isolation-screenshot-token")))
        self.send = self.stack.enter_context(patch.object(server, "send_robot_message_by_payload",
            return_value={"robot_sent": True, "robot_skipped": False}))
        self.stack.enter_context(patch.object(self.runtime, "_enqueue_qt_notice_followups"))

    def request(self, action, *, target=None):
        record = self.target if target is None else target
        request = fixture._request(action, operation_id="qt_notice:" + action, record_id=record)
        if record.startswith("localid"):
            request["data_dict"].pop("target_record_id", None)
            request["data_dict"]["_is_placeholder_record"] = True
        if action != "upload":
            attachment = self.store.put_notice_upload_attachment(file_name="isolated.png", content=b"local")
            request["screenshot_upload_id"] = attachment["upload_id"]
        return request

    def seed_existing(self):
        data = self.request("upload")["data_dict"]
        payload = self.runtime._prepared_to_notice_payload(data)
        payload.occurrence_date = data["time_str"]
        fields = get_notice_handler("事件通告").build_create_fields(payload)
        self.remote["fields"] = fields
        self.runtime._save_event_local_fields(self.target, fields, "seed")
        event_priority.record_result(self.store, data, "start", self.target)
        self.store.upsert_qt_active_item({**data, "target_record_id": self.target, "_is_placeholder_record": False},
                                         section="event", origin="fixture")

    def restart(self):
        self.store.shutdown_write_worker()
        self.store = LanPortalStateStore(self.db)
        self.runtime.state_store = self.store
        self.stack.callback(self.store.shutdown_write_worker)
        self.delivery = QtEventDelivery(self.runtime)

    def make_due(self, operation_id, key):
        row = self.delivery.operation(operation_id)
        row[key] = 0
        self.store.put_document(OPERATIONS, operation_id, row)
        channel = MESSAGES if key == "next_message_at" else ARCHIVES
        task = next(task for task in self.store.list_outbox_events(channel)
                    if task["payload"]["operation_id"] == operation_id)
        self.store.mark_outbox_event(task["id"], "pending", retry_at=0, count_attempt=False)

    def test_start_uses_explicit_i3_from_text_and_waits_for_confirmed_group_delivery(self):
        from upload_event_module.services import feishu_service, robot_webhook

        request = self.request("upload", target="localid-explicit-level")
        request["data_dict"].pop("level")
        request["data_dict"]["text"] += "\n【事件等级】I3级"
        self.send.side_effect = feishu_service.send_robot_message_by_payload
        self.delivery.submit(request)
        with (
            patch.object(robot_webhook.config, "group_name_event_i3", ""),
            patch.object(robot_webhook, "send_text_to_chat_id") as post,
        ):
            self.delivery.process_messages()
            post.assert_not_called()
        operation_id = request["operation_id"]
        self.assertIn("未配置飞书 I3 事件群", self.delivery.status(operation_id)["error"])
        self.assertEqual(self.store.list_outbox_events(ARCHIVES), [])

        self.make_due(operation_id, "next_message_at")
        with (
            patch.object(robot_webhook.config, "group_name_event_i3", "隔离内部群"),
            patch.object(robot_webhook, "_find_chat_id_by_name", return_value=("isolated-chat", "")) as find,
            patch.object(robot_webhook, "send_text_to_chat_id", return_value=(True, "ok")) as post,
        ):
            self.delivery.process_messages()
            find.assert_called_once_with("隔离内部群")
            post.assert_called_once()
            self.assertEqual(post.call_args.args[1], "isolated-chat")
        self.assertTrue(self.delivery.status(operation_id)["result"]["robot_sent"])
        self.create.assert_not_called()

    def test_end_releases_waiters_before_bitable_and_retries_the_original_archive_after_restart(self):
        self.seed_existing()
        request = self.request("end")
        operation_id = request["operation_id"]
        self.remote["error"] = "Bitable read timeout"
        self.assertTrue(self.delivery.submit(request)["event_delivery_pending"])
        self.delivery.process_messages()
        self.assertEqual(event_priority.blockers(self.store), [])
        self.assertEqual(self.store.list_qt_active_items(), [])
        self.query.assert_not_called()
        self.upload.assert_not_called()
        self.update.assert_not_called()
        self.assertTrue(self.delivery.submit(request)["robot_sent"])
        self.assertFalse(self.delivery.submit(request)["remote_written"])
        receipt, = [task["payload"] for task in self.store.list_outbox_events("qt_action")
                    if task["payload"].get("kind") == "notice_wait_result"]
        self.assertTrue(receipt["payload"]["result"]["ok"])
        self.assertEqual(self.store.cleanup_notice_upload_attachments(now=time.time() + 172800), 0)
        self.delivery.process_archives()
        self.assertEqual(self.delivery.status(operation_id)["result"]["archive_state"], "retry")

        self.restart()
        self.make_due(operation_id, "next_archive_at")
        self.remote.pop("error")
        self.delivery.process_messages()
        self.delivery.process_archives()
        self.delivery.process_archives()
        self.send.assert_called_once()
        self.update.assert_called_once()
        self.assertTrue(self.remote["fields"][EVENT_NOTICE_FIELDS["end_time"]])
        self.assertEqual(event_priority.blockers(self.store), [])
        self.assertEqual(self.delivery.submit(request)["archive_state"], "synced")
        self.assertEqual(self.store.cleanup_notice_upload_attachments(now=time.time() + 172800), 1)

    def test_group_failure_keeps_event_open_and_reuses_the_same_send_id(self):
        self.seed_existing()
        request = self.request("end")
        self.send.side_effect = [{"robot_sent": False, "last_robot_error": "temporary group timeout"},
                                 {"robot_sent": True}]
        self.delivery.submit(request)
        self.delivery.process_messages()
        self.assertEqual(len(event_priority.blockers(self.store)), 1)
        self.assertEqual(self.store.list_outbox_events(ARCHIVES), [])
        self.restart()
        self.make_due(request["operation_id"], "next_message_at")
        self.delivery.process_messages()
        self.assertEqual(event_priority.blockers(self.store), [])
        self.assertEqual(self.send.call_args_list[0].kwargs, self.send.call_args_list[1].kwargs)
        self.update.assert_not_called()

    def test_end_delivers_failure_receipt_when_retries_are_exhausted(self):
        self.seed_existing()
        request = self.request("end")
        operation_id = request["operation_id"]
        self.send.return_value = {"robot_sent": False, "last_robot_error": "隔离群发送失败"}
        self.delivery.submit(request)
        for _ in range(5):
            self.make_due(operation_id, "next_message_at")
            self.delivery.process_messages()
        receipt, = self.store.list_outbox_events("qt_action")
        result = receipt["payload"]["payload"]["result"]
        self.assertFalse(result["ok"])
        self.assertIn("隔离群发送失败", result["message"])
        self.assertEqual(self.delivery.status(operation_id)["status"], "failed")
        self.assertEqual(len(event_priority.blockers(self.store)), 1)
        self.update.assert_not_called()

    def test_manual_end_retry_after_lost_receipts_and_restart_reuses_group_message_id(self):
        self.seed_existing()
        request = self.request("end")
        original_id = request["operation_id"]
        accepted = set()
        lose_reply = True

        def send(_kind, _payload, *, message_uuid):
            accepted.add(message_uuid)
            return ({"robot_sent": False, "last_robot_error": "隔离回执丢失"}
                    if lose_reply else {"robot_sent": True})

        self.send.side_effect = send
        self.delivery.submit(request)
        for _ in range(5):
            self.make_due(original_id, "next_message_at")
            self.delivery.process_messages()
        self.assertEqual(self.delivery.status(original_id)["status"], "failed")
        old_operation = self.delivery.operation(original_id)
        old_operation.pop("message_uuid", None)
        self.store.put_document(OPERATIONS, original_id, old_operation)
        self.restart()
        retry = self.request("end")
        retry["operation_id"] = "qt_notice:manual-end-retry"
        retry["response_time"] = "2026-08-31 10:06"
        self.delivery.submit(retry)
        lose_reply = False
        self.delivery.process_messages()
        self.assertEqual(len(accepted), 1)
        self.assertEqual(event_priority.blockers(self.store), [])
        self.delivery.process_archives()
        self.update.assert_called_once()

    def test_independent_send_and_archive_pass_retrying_batches(self):
        requests = []
        for index in range(6):
            request = self.request("upload", target=f"localid-qt-backlog-{index}")
            request["operation_id"] = f"qt_notice:backlog-{index}"
            request["data_dict"]["active_item_id"] = f"active-qt-backlog-{index}"
            requests.append(request)
            self.delivery.submit(request)
        self.send.return_value = {"robot_sent": False, "last_robot_error": "temporary delivery error"}
        self.delivery.process_messages()
        self.assertEqual(self.send.call_count, 5)
        self.send.return_value = {"robot_sent": True}
        self.delivery.process_messages()
        latest = requests[-1]["operation_id"]
        self.assertTrue(self.delivery.status(latest)["result"]["robot_sent"])

        for request in requests[:-1]:
            self.make_due(request["operation_id"], "next_message_at")
        self.delivery.process_messages()
        self.create.side_effect = lambda *args: (False, "temporary archive error")
        self.delivery.process_archives()
        self.assertEqual(self.delivery.status(latest)["result"]["archive_state"], "retry")
        self.create.side_effect = fixture.EventRemoteAtomicityTests._fake_create(self.remote)
        self.delivery.process_archives()
        self.assertEqual(self.delivery.status(requests[4]["operation_id"])["result"]["archive_state"], "synced")

    def test_start_and_end_can_send_before_archive_and_sync_in_order_to_one_record(self):
        local = "localid-delivery-001"
        start = self.request("upload", target=local)
        end = self.request("end", target=local)
        self.delivery.submit(start)
        self.delivery.process_messages()
        self.assertEqual(len(event_priority.blockers(self.store)), 1)
        executing, release = threading.Event(), threading.Event()
        execute = self.runtime.execute_local_notice_upload

        def delayed(command):
            executing.set()
            if not release.wait(8):
                raise AssertionError("archive was not released by the test")
            return execute(command)

        with ThreadPoolExecutor(max_workers=1) as worker, patch.object(self.runtime, "execute_local_notice_upload", side_effect=delayed):
            in_flight = worker.submit(self.delivery.process_archives)
            try:
                self.assertTrue(executing.wait(5))
                self.delivery.submit(end)
                self.delivery.process_messages()
                self.assertEqual(event_priority.blockers(self.store), [])
                self.create.assert_not_called()
            finally:
                release.set()
            in_flight.result(timeout=8)
        self.update.assert_not_called()
        self.delivery.process_archives()
        self.assertEqual(self.send.call_count, 2)
        self.create.assert_called_once()
        self.update.assert_called_once()
        self.assertEqual(self.update.call_args.args[0], self.target)
        self.assertEqual(self.store.list_qt_active_items(), [])
        self.assertEqual(event_priority.blockers(self.store), [])

    def test_skipped_group_does_not_close_event_before_archive(self):
        self.seed_existing()
        request = self.request("end")
        self.send.return_value = {"robot_skipped": True}
        self.delivery.submit(request)
        self.delivery.process_messages()
        self.assertEqual(len(event_priority.blockers(self.store)), 1)
        self.assertTrue(self.delivery.submit(request)["queued"])
        self.delivery.process_archives()
        result = self.delivery.submit(request)
        self.assertTrue(result["ok"] and result["robot_skipped"])
        self.assertEqual(event_priority.blockers(self.store), [])
        self.send.assert_called_once()

    def test_mismatched_event_is_rejected_before_sending(self):
        self.seed_existing()
        request = self.request("end")
        request["data_dict"]["time_str"] = "2026-08-30 09:00"
        with self.assertRaisesRegex(PortalError, "发生时间"):
            self.delivery.submit(request)
        self.send.assert_not_called()

    def test_qt_http_endpoint_returns_receipt_before_archive(self):
        from urllib.parse import quote
        from fastapi.testclient import TestClient
        from clipflow_backend.main import FastAPIPortalController
        from lan_bitable_template_portal.event_notice_delivery import EventNoticeDelivery

        self.seed_existing()
        command = self.request("end")
        operation_id = command["operation_id"]
        client = TestClient(FastAPIPortalController(host="127.0.0.1", port=18766)._build_app())
        self.addCleanup(client.close)
        path = "/api/qt/notice-operations/" + quote(operation_id, safe="")
        body = {"command": "notice_end", "payload": command}
        receipt = client.post("/api/qt/commands", json=body).json()["data"]
        self.assertTrue(receipt["event_delivery_pending"])
        self.assertEqual(client.get(path).json()["data"]["status"], "executing")
        self.delivery.process_messages()
        settled = client.get(path).json()["data"]
        self.assertEqual(settled["status"], "completed")
        self.assertEqual(settled["result"]["archive_state"], "pending")
        self.assertTrue(settled["result"]["qt_result"]["robot_sent"])
        self.update.assert_not_called()
        self.assertEqual(client.post("/api/qt/commands", json=body).json()["data"], settled["result"]["qt_result"])
        web = EventNoticeDelivery(self.runtime).public({"state": "active", "target_record_id": self.target})
        self.assertEqual(web["state"], "ended")

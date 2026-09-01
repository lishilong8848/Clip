import hashlib
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

import lan_bitable_template_portal.server as server_module
import upload_event_module.services.feishu_service as feishu_service
from lan_bitable_template_portal.portal_service import PortalError
from lan_bitable_template_portal.server import PortalRuntime
from lan_bitable_template_portal.state_store import LanPortalStateStore
from upload_event_module.services.handlers import NoticePayload
from upload_event_module.services.handlers.event_notice import EventNoticeHandler


def _event_text(status: str) -> str:
    return (
        f"【事件通告】状态：{status}\n"
        "【概述】A楼BMS测试事件\n"
        "【事件发生时间】2026-08-31 10:00\n"
        "【来源】BMS\n"
        f"【进展】{status}"
    )


def _request(action: str, *, operation_id: str, record_id: str) -> dict:
    status = {"upload": "开始", "update": "更新", "end": "结束"}[action]
    data = {
        "active_item_id": "active-event-atomic",
        "record_id": record_id,
        "notice_type": "事件通告",
        "text": _event_text(status),
        "time_str": "2026-08-31 10:00",
        "buildings": ["A楼"],
        "specialty": "电气",
        "event_source": "BMS动环系统告警",
        "level": "I3",
    }
    if action == "upload":
        data["_is_placeholder_record"] = True
    else:
        data["target_record_id"] = record_id
        data["_is_placeholder_record"] = False
    return {
        "action_type": action,
        "operation_id": operation_id,
        "data_dict": data,
        "response_time": "2026-08-31 10:05",
        "robot_group_choice": "auto",
    }


class EventRemoteAtomicityTests(unittest.TestCase):
    def setUp(self):
        self._old_store = PortalRuntime.state_store
        self._old_locks = PortalRuntime.local_upload_locks
        self._old_targets = PortalRuntime.local_upload_created_targets
        self._tmp = tempfile.TemporaryDirectory()
        PortalRuntime.state_store = LanPortalStateStore(
            Path(self._tmp.name) / "state.sqlite3"
        )
        PortalRuntime.local_upload_locks = {}
        PortalRuntime.local_upload_created_targets = {}

    def tearDown(self):
        PortalRuntime.state_store.shutdown_write_worker(timeout=2.0)
        PortalRuntime.state_store = self._old_store
        PortalRuntime.local_upload_locks = self._old_locks
        PortalRuntime.local_upload_created_targets = self._old_targets
        self._tmp.cleanup()

    @staticmethod
    def _fake_create(remote: dict):
        def create(notice_type, payload):
            fields = EventNoticeHandler(notice_type).build_create_fields(payload)
            setattr(payload, "_clipflow_written_fields", dict(fields))
            remote["fields"] = dict(fields)
            remote["version"] = "create-v1"
            return True, "rec-event-atomic"

        return create

    @staticmethod
    def _fake_update(remote: dict):
        def update(record_id, notice_type, payload):
            fields = EventNoticeHandler(notice_type).build_update_fields(payload)
            setattr(payload, "_clipflow_written_fields", dict(fields))
            remote.setdefault("fields", {}).update(fields)
            remote["version"] = f"{record_id}-updated"
            return True, record_id

        return update

    @staticmethod
    def _query(remote: dict):
        def query(record_id, _notice_type):
            if remote.get("error"):
                return False, remote["error"]
            return True, {
                "record_id": record_id,
                "fields": dict(remote.get("fields") or {}),
                "record_version": str(remote.get("version") or "v1"),
            }

        return query

    def test_create_readback_failure_does_not_send_message_or_report_success(self):
        remote = {"error": "query timeout"}
        request = _request(
            "upload",
            operation_id="event-create-readback-fails",
            record_id="local-event-create",
        )
        with patch.object(
            server_module,
            "create_bitable_record_by_payload",
            side_effect=self._fake_create(remote),
        ), patch.object(
            server_module,
            "query_record_by_id",
            side_effect=self._query(remote),
        ), patch.object(
            server_module,
            "send_robot_message_by_payload",
        ) as send_robot:
            # The write succeeds, then its point read fails.
            remote["error"] = "query timeout"
            result = PortalRuntime.execute_local_notice_upload(request)

        self.assertFalse(result["ok"], result)
        self.assertTrue(result.get("remote_written"), result)
        self.assertIn("回读校验失败", result["message"])
        send_robot.assert_not_called()
        operation = PortalRuntime.state_store.get_notice_remote_operation(
            request["operation_id"]
        )
        self.assertEqual(operation["status"], "remote_written")
        self.assertFalse(operation["result"].get("remote_verified"))

        remote.pop("error", None)
        with patch.object(
            server_module,
            "create_bitable_record_by_payload",
            side_effect=AssertionError("恢复时不应重新创建目标记录"),
        ), patch.object(
            server_module,
            "query_record_by_id",
            side_effect=self._query(remote),
        ), patch.object(
            server_module,
            "send_robot_message_by_payload",
            return_value={
                "robot_sent": True,
                "robot_skipped": False,
                "last_robot_error": "",
            },
        ) as send_robot:
            recovered = PortalRuntime.execute_local_notice_upload(request)

        self.assertTrue(recovered["ok"], recovered)
        send_robot.assert_called_once()
        self.assertEqual(
            PortalRuntime.state_store.get_notice_remote_operation(
                request["operation_id"]
            )["status"],
            "completed",
        )

    def test_create_verifies_before_sending_once(self):
        remote = {}
        request = _request(
            "upload",
            operation_id="event-create-success",
            record_id="local-event-create-success",
        )
        with patch.object(
            server_module,
            "create_bitable_record_by_payload",
            side_effect=self._fake_create(remote),
        ) as create, patch.object(
            server_module,
            "query_record_by_id",
            side_effect=self._query(remote),
        ), patch.object(
            server_module,
            "send_robot_message_by_payload",
            return_value={
                "robot_sent": True,
                "robot_skipped": False,
                "last_robot_error": "",
            },
        ) as send_robot:
            first = PortalRuntime.execute_local_notice_upload(request)
            replay = PortalRuntime.execute_local_notice_upload(request)

        self.assertTrue(first["ok"], first)
        self.assertTrue(replay["ok"], replay)
        create.assert_called_once()
        send_robot.assert_called_once()
        self.assertEqual(
            PortalRuntime.state_store.get_notice_remote_operation(
                request["operation_id"]
            )["status"],
            "completed",
        )

    def test_update_and_end_verify_before_message(self):
        for action in ("update", "end"):
            with self.subTest(action=action):
                operation_id = f"event-{action}-success"
                record_id = f"rec-event-{action}"
                request = _request(
                    action,
                    operation_id=operation_id,
                    record_id=record_id,
                )
                attachment = PortalRuntime.state_store.put_notice_upload_attachment(
                    open_id="qt-local",
                    file_name=f"{action}.png",
                    mime_type="image/png",
                    content=b"image",
                )
                request["screenshot_upload_id"] = attachment["upload_id"]
                initial = NoticePayload(
                    text=_event_text("开始"),
                    level="I3",
                    buildings=["A楼"],
                    specialty="电气",
                    event_source="BMS动环系统告警",
                    response_time="2026-08-31 10:00",
                    occurrence_date="2026-08-31 10:00",
                )
                remote = {
                    "fields": EventNoticeHandler("事件通告").build_create_fields(
                        initial
                    ),
                    "version": "before",
                }
                with patch.object(
                    server_module,
                    "query_record_by_id",
                    side_effect=self._query(remote),
                ), patch.object(
                    server_module,
                    "upload_media_to_feishu",
                    return_value=(True, f"token-{action}"),
                ), patch.object(
                    server_module,
                    "update_bitable_record_by_payload",
                    side_effect=self._fake_update(remote),
                ) as update, patch.object(
                    server_module,
                    "send_robot_message_by_payload",
                    return_value={
                        "robot_sent": True,
                        "robot_skipped": False,
                        "last_robot_error": "",
                    },
                ) as send_robot:
                    result = PortalRuntime.execute_local_notice_upload(request)

                self.assertTrue(result["ok"], result)
                update.assert_called_once()
                send_robot.assert_called_once()
                operation = PortalRuntime.state_store.get_notice_remote_operation(
                    operation_id
                )
                self.assertEqual(operation["status"], "completed")
                self.assertTrue(operation["result"].get("remote_verified"))

    def test_event_end_rejects_invalid_end_time_before_any_remote_write(self):
        request = _request(
            "end",
            operation_id="event-end-invalid-time",
            record_id="rec-event-end-invalid-time",
        )
        request["response_time"] = "not-a-time"
        initial = NoticePayload(
            text=_event_text("开始"),
            level="I3",
            buildings=["A楼"],
            specialty="电气",
            event_source="BMS动环系统告警",
            response_time="2026-08-31 10:00",
            occurrence_date="2026-08-31 10:00",
        )
        remote = {
            "fields": EventNoticeHandler("事件通告").build_create_fields(initial),
            "version": "before-end",
        }
        with patch.object(
            server_module,
            "query_record_by_id",
            side_effect=self._query(remote),
        ), patch.object(
            server_module,
            "upload_media_to_feishu",
        ) as upload_media, patch.object(
            server_module,
            "update_bitable_record_by_payload",
        ) as update_record, patch.object(
            server_module,
            "send_robot_message_by_payload",
        ) as send_robot:
            result = PortalRuntime.execute_local_notice_upload(request)

        self.assertFalse(result["ok"], result)
        self.assertIn("结束时间", result["message"])
        upload_media.assert_not_called()
        update_record.assert_not_called()
        send_robot.assert_not_called()

    def test_message_failure_retries_only_message_with_stable_uuid(self):
        remote = {}
        request = _request(
            "upload",
            operation_id="event-message-retry",
            record_id="local-event-message-retry",
        )
        calls = []

        def send(_notice_type, _payload, *, message_uuid=""):
            calls.append(message_uuid)
            if len(calls) == 1:
                return {
                    "robot_sent": False,
                    "robot_skipped": False,
                    "last_robot_error": "network reset",
                }
            return {
                "robot_sent": True,
                "robot_skipped": False,
                "last_robot_error": "",
            }

        with patch.object(
            server_module,
            "create_bitable_record_by_payload",
            side_effect=self._fake_create(remote),
        ) as create, patch.object(
            server_module,
            "query_record_by_id",
            side_effect=self._query(remote),
        ), patch.object(
            server_module,
            "send_robot_message_by_payload",
            side_effect=send,
        ):
            result = PortalRuntime.execute_local_notice_upload(request)
            PortalRuntime.state_store.mark_notice_remote_operation(
                request["operation_id"],
                status="remote_written",
                result={"robot_next_retry_at": 0},
            )
            retried = PortalRuntime.retry_pending_event_robot_messages()

        self.assertTrue(result["ok"], result)
        self.assertEqual(retried["sent"], 1)
        create.assert_called_once()
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0], calls[1])

    def test_delayed_start_message_is_superseded_by_newer_update(self):
        target_id = "rec-event-superseded-message"
        start_id = "event-start-pending-message"
        update_id = "event-update-newer-message"
        PortalRuntime.state_store.begin_notice_remote_operation(
            operation_id=start_id,
            operation_type="start",
            request={"notice_type": "事件通告", "action": "start"},
            target_record_id=target_id,
        )
        PortalRuntime.state_store.mark_notice_remote_operation(
            start_id,
            status="remote_written",
            target_record_id=target_id,
            result={
                "record_id": target_id,
                "remote_verified": True,
                "local_projection_completed": True,
                "robot_delivery_state": "pending",
                "robot_next_retry_at": 0,
                "robot_payload": {"text": _event_text("开始")},
            },
        )
        PortalRuntime.state_store.begin_notice_remote_operation(
            operation_id=update_id,
            operation_type="update",
            request={"notice_type": "事件通告", "action": "update"},
            target_record_id=target_id,
        )
        PortalRuntime.state_store.mark_notice_remote_operation(
            update_id,
            status="completed",
            target_record_id=target_id,
            result={
                "record_id": target_id,
                "remote_verified": True,
                "local_projection_completed": True,
                "robot_sent": True,
                "robot_delivery_state": "sent",
            },
        )

        with patch.object(
            server_module,
            "send_robot_message_by_payload",
        ) as send_robot, patch.object(
            server_module,
            "query_record_by_id",
        ) as query_record:
            result = PortalRuntime.retry_pending_event_robot_messages(limit=1)

        self.assertEqual(result["superseded"], 1)
        send_robot.assert_not_called()
        query_record.assert_not_called()
        operation = PortalRuntime.state_store.get_notice_remote_operation(start_id)
        self.assertEqual(operation["status"], "completed")
        self.assertEqual(
            operation["result"]["robot_delivery_state"],
            "superseded",
        )

    def test_delayed_message_target_check_uses_backoff_without_sending(self):
        operation_id = "event-message-target-check-retry"
        target_id = "rec-event-message-target-check"
        PortalRuntime.state_store.begin_notice_remote_operation(
            operation_id=operation_id,
            operation_type="start",
            request={"notice_type": "事件通告", "action": "start"},
            target_record_id=target_id,
        )
        PortalRuntime.state_store.mark_notice_remote_operation(
            operation_id,
            status="remote_written",
            target_record_id=target_id,
            result={
                "record_id": target_id,
                "remote_verified": True,
                "local_projection_completed": True,
                "robot_delivery_state": "pending",
                "robot_next_retry_at": 0,
                "robot_payload": {"text": _event_text("开始")},
            },
        )
        other_id = "non-event-remote-written"
        PortalRuntime.state_store.begin_notice_remote_operation(
            operation_id=other_id,
            operation_type="start",
            request={"notice_type": "维保通告"},
        )
        PortalRuntime.state_store.mark_notice_remote_operation(
            other_id,
            status="remote_written",
            result={"robot_delivery_state": "pending"},
        )

        with patch.object(
            server_module,
            "query_record_by_id",
            side_effect=ConnectionError("network timeout"),
        ), patch.object(
            server_module,
            "send_robot_message_by_payload",
        ) as send_robot:
            result = PortalRuntime.retry_pending_event_robot_messages(limit=1)

        self.assertEqual(result["failed"], 1)
        send_robot.assert_not_called()
        operation = PortalRuntime.state_store.get_notice_remote_operation(operation_id)
        self.assertEqual(operation["status"], "remote_written")
        self.assertEqual(operation["result"]["robot_retry_count"], 1)
        self.assertGreater(operation["result"]["robot_next_retry_at"], 0)
        self.assertEqual(
            PortalRuntime.state_store.list_pending_event_robot_operations(limit=1),
            [],
        )

    def test_projection_failure_recovers_without_rewrite_or_duplicate_message(self):
        remote = {}
        first_request = _request(
            "upload",
            operation_id="event-projection-first",
            record_id="local-event-projection",
        )
        retry_request = dict(first_request)
        retry_request["operation_id"] = "event-projection-retry"
        projection_calls = []

        def remember(*_args, **_kwargs):
            projection_calls.append(1)
            if len(projection_calls) == 1:
                raise RuntimeError("本地投影落盘失败")
            return "projection-v2"

        with patch.object(
            server_module,
            "create_bitable_record_by_payload",
            side_effect=self._fake_create(remote),
        ) as create, patch.object(
            server_module,
            "query_record_by_id",
            side_effect=self._query(remote),
        ), patch.object(
            PortalRuntime,
            "_remember_local_upload_target",
            side_effect=remember,
        ), patch.object(
            server_module,
            "send_robot_message_by_payload",
            return_value={
                "robot_sent": True,
                "robot_skipped": False,
                "last_robot_error": "",
            },
        ) as send_robot:
            with self.assertRaisesRegex(RuntimeError, "投影落盘失败"):
                PortalRuntime.execute_local_notice_upload(first_request)
            recovered = PortalRuntime.execute_local_notice_upload(retry_request)

        self.assertTrue(recovered["ok"], recovered)
        create.assert_called_once()
        send_robot.assert_called_once()
        self.assertEqual(len(projection_calls), 2)
        retry_operation = PortalRuntime.state_store.get_notice_remote_operation(
            retry_request["operation_id"]
        )
        self.assertEqual(retry_operation["status"], "completed")

    def test_update_pending_verification_recovers_without_new_screenshot_or_write(self):
        record_id = "rec-event-update-recovery"
        operation_id = "event-update-recovery"
        initial_payload = NoticePayload(
            text=_event_text("开始"),
            level="I3",
            buildings=["A楼"],
            specialty="电气",
            event_source="BMS动环系统告警",
            response_time="2026-08-31 10:00",
            occurrence_date="2026-08-31 10:00",
        )
        remote = {
            "fields": {
                **EventNoticeHandler("事件通告").build_create_fields(
                    initial_payload
                ),
                "事件状态": "处理中",
            },
            "version": "before-update",
            "fail_readback": False,
        }
        attachment = PortalRuntime.state_store.put_notice_upload_attachment(
            open_id="qt-local",
            file_name="event-update.png",
            mime_type="image/png",
            content=b"event-update",
        )
        request = _request(
            "update",
            operation_id=operation_id,
            record_id=record_id,
        )
        request["screenshot_upload_id"] = attachment["upload_id"]

        def query(current_record_id, _notice_type):
            if remote["fail_readback"]:
                return False, "point read timeout"
            return True, {
                "record_id": current_record_id,
                "fields": dict(remote["fields"]),
                "record_version": remote["version"],
            }

        def update(current_record_id, notice_type, notice_payload):
            fields = EventNoticeHandler(notice_type).build_update_fields(
                notice_payload
            )
            setattr(notice_payload, "_clipflow_written_fields", dict(fields))
            remote["fields"].update(fields)
            remote["version"] = "after-update"
            remote["fail_readback"] = True
            return True, current_record_id

        with patch.object(
            server_module,
            "query_record_by_id",
            side_effect=query,
        ), patch.object(
            server_module,
            "upload_media_to_feishu",
            return_value=(True, "event-update-token"),
        ) as upload_media, patch.object(
            server_module,
            "update_bitable_record_by_payload",
            side_effect=update,
        ) as update_record, patch.object(
            server_module,
            "send_robot_message_by_payload",
            return_value={
                "robot_sent": True,
                "robot_skipped": False,
                "last_robot_error": "",
            },
        ) as send_robot:
            first = PortalRuntime.execute_local_notice_upload(request)
            self.assertFalse(first["ok"], first)
            remote["fail_readback"] = False
            retry = _request(
                "update",
                operation_id=operation_id,
                record_id=record_id,
            )
            retry["response_time"] = "2026-08-31 10:09"
            retry["data_dict"]["_remote_written_pending_verification"] = True
            recovered = PortalRuntime.execute_local_notice_upload(retry)

        self.assertTrue(recovered["ok"], recovered)
        update_record.assert_called_once()
        upload_media.assert_called_once()
        send_robot.assert_called_once()

    def test_legacy_completed_operation_without_delivery_marker_is_not_resent(self):
        operation_id = "legacy-event-operation"
        PortalRuntime.state_store.begin_notice_remote_operation(
            operation_id=operation_id,
            operation_type="start",
            request={"notice_type": "事件通告"},
            target_record_id="rec-legacy-event",
        )
        PortalRuntime.state_store.mark_notice_remote_operation(
            operation_id,
            status="completed",
            target_record_id="rec-legacy-event",
            result={"record_id": "rec-legacy-event"},
        )
        with patch.object(
            server_module,
            "send_robot_message_by_payload",
        ) as send_robot:
            result = PortalRuntime._send_deferred_event_robot(
                operation_id,
                NoticePayload(text=_event_text("开始")),
                target_record_id="rec-legacy-event",
                result_message="rec-legacy-event",
            )
        self.assertTrue(result.get("robot_delivery_uncertain"))
        send_robot.assert_not_called()

    def test_adopted_operation_sends_original_stored_robot_payload(self):
        operation_id = "event-original-message"
        original = NoticePayload(
            text="原始事件消息",
            level="I3",
            buildings=["A楼"],
            robot_group_choice="auto",
        )
        PortalRuntime.state_store.begin_notice_remote_operation(
            operation_id=operation_id,
            operation_type="start",
            request={"notice_type": "事件通告"},
            target_record_id="rec-original-message",
        )
        PortalRuntime.state_store.mark_notice_remote_operation(
            operation_id,
            status="remote_written",
            target_record_id="rec-original-message",
            result={
                "record_id": "rec-original-message",
                "robot_delivery_state": "pending",
                "robot_message_uuid": PortalRuntime._event_robot_message_uuid(
                    operation_id
                ),
                "robot_payload": PortalRuntime._event_robot_retry_payload(original),
            },
        )
        sent_text = []

        def send(_notice_type, payload, *, message_uuid=""):
            sent_text.append((payload.text, message_uuid))
            return {
                "robot_sent": True,
                "robot_skipped": False,
                "last_robot_error": "",
            }

        with patch.object(
            server_module,
            "send_robot_message_by_payload",
            side_effect=send,
        ):
            PortalRuntime._send_deferred_event_robot(
                operation_id,
                NoticePayload(text="重试时的新文本"),
                target_record_id="rec-original-message",
                result_message="ok",
            )

        self.assertEqual(sent_text[0][0], "原始事件消息")

    def test_explicit_concurrent_write_rejection_has_bounded_retry(self):
        rejected = SimpleNamespace(success=lambda: False, code=1254291)
        accepted = SimpleNamespace(success=lambda: True, code=0)
        calls = []

        def request(_token):
            calls.append(1)
            return accepted

        with patch.object(
            feishu_service,
            "_with_token_retry",
            side_effect=[rejected, rejected, accepted],
        ), patch.object(feishu_service.time, "sleep") as sleep:
            result = feishu_service._with_rejected_bitable_write_retry(request)

        self.assertIs(result, accepted)
        self.assertEqual(sleep.call_count, 2)

    def test_existing_event_target_transient_read_failure_fails_closed(self):
        prepared = {
            "record_id": "rec-existing-event",
            "target_record_id": "rec-existing-event",
            "notice_type": "事件通告",
            "text": _event_text("开始"),
        }
        with patch.object(
            server_module,
            "external_real_write_guard",
            return_value={"mock_external": False},
        ), patch.object(
            server_module,
            "query_record_by_id",
            return_value=(False, "network timeout"),
        ):
            with self.assertRaisesRegex(PortalError, "核验失败"):
                PortalRuntime._existing_target_for_prepared_start(
                    prepared,
                    "事件通告",
                )

    def test_finished_event_identity_is_not_reused_for_new_start(self):
        data = {
            "active_item_id": "active-finished-event-restart",
            "record_id": "local-finished-event-restart",
            "notice_type": "事件通告",
            "text": _event_text("开始"),
            "time_str": "2026-08-31 10:00",
            "buildings": ["A楼"],
            "specialty": "电气",
            "event_source": "BMS动环系统告警",
            "level": "I3",
        }
        existing = {
            **data,
            "record_id": "rec-finished-event-restart",
            "target_record_id": "rec-finished-event-restart",
            "_is_placeholder_record": False,
        }
        PortalRuntime.state_store.upsert_qt_active_item(
            existing,
            section="event",
            origin="test",
        )
        fields = EventNoticeHandler("事件通告").build_create_fields(
            NoticePayload(
                text=data["text"],
                level="I3",
                buildings=["A楼"],
                specialty="电气",
                event_source="BMS动环系统告警",
                response_time="2026-08-31 10:01",
                occurrence_date="2026-08-31 10:00",
            )
        )
        fields["事件结束时间"] = 1788148800000

        with patch.object(
            server_module,
            "external_real_write_guard",
            return_value={"mock_external": False},
        ), patch.object(
            server_module,
            "query_record_by_id",
            return_value=(True, {"fields": fields}),
        ):
            target = PortalRuntime._existing_target_for_local_upload(
                data,
                "事件通告",
            )

        self.assertEqual(target, "")

    def test_append_field_match_requires_every_submitted_minute(self):
        desired = "1、2026/08/31 10:00   2、2026/08/31 10:05"
        stale_remote = "1、2026/08/31 10:00"
        current_remote = desired + "   3、2026/08/31 10:10"

        self.assertFalse(
            PortalRuntime._remote_append_field_matches(
                "进展更新时间",
                desired,
                stale_remote,
            )
        )
        self.assertTrue(
            PortalRuntime._remote_append_field_matches(
                "进展更新时间",
                desired,
                current_remote,
            )
        )

    def test_event_operation_request_covers_remote_and_robot_inputs(self):
        prepared = {
            "action": "start",
            "notice_type": "事件通告",
            "text": _event_text("开始"),
            "active_item_id": "active-event-request-hash",
        }
        first_payload = NoticePayload(
            text=prepared["text"],
            level="I3",
            buildings=["A楼"],
            specialty="电气",
            event_source="BMS动环系统告警",
            occurrence_date="2026-08-31 10:00",
            response_time="2026-08-31 10:05",
            recover=False,
            transfer_to_overhaul=False,
            robot_group_choice="i3",
            file_tokens=["file-token-1"],
        )
        second_payload = NoticePayload(
            **{
                **first_payload.__dict__,
                "level": "I2",
                "response_time": "2026-08-31 10:06",
                "robot_group_choice": "i2",
                "file_tokens": ["file-token-2"],
            }
        )
        first = PortalRuntime._notice_remote_operation_request(
            prepared,
            payload=first_payload,
        )
        replay_payload = NoticePayload(
            **{
                **first_payload.__dict__,
                "existing_file_tokens": ["older-token", "file-token-1"],
                "existing_response_time": (
                    "1、2026/08/31 10:00   2、2026/08/31 10:05"
                ),
            }
        )
        second = PortalRuntime._notice_remote_operation_request(
            prepared,
            payload=second_payload,
        )

        self.assertEqual(first["file_tokens"], ["file-token-1"])
        self.assertEqual(
            first,
            PortalRuntime._notice_remote_operation_request(
                prepared,
                payload=replay_payload,
            ),
        )
        self.assertNotEqual(first, second)
        PortalRuntime.state_store.begin_notice_remote_operation(
            operation_id="event-request-hash",
            operation_type="start",
            request=first,
        )
        replay = PortalRuntime.state_store.begin_notice_remote_operation(
            operation_id="event-request-hash",
            operation_type="start",
            request=second,
        )
        self.assertTrue(replay["conflict"])

    def test_expanded_event_request_replays_legacy_operation(self):
        legacy_request = {
            "action": "start",
            "notice_type": "事件通告",
            "text_sha256": "same-text",
        }
        PortalRuntime.state_store.begin_notice_remote_operation(
            operation_id="legacy-event-request",
            operation_type="start",
            request=legacy_request,
        )
        replay = PortalRuntime._begin_notice_remote_operation(
            operation_id="legacy-event-request",
            operation_type="start",
            request={
                **legacy_request,
                "level": "I3",
                "event_source": "BMS动环系统告警",
                "response_time": "2026-08-31 10:05",
                "file_tokens": ["file-token-1"],
            },
        )

        self.assertFalse(replay["conflict"])
        self.assertFalse(replay["created"])
        self.assertTrue(replay["legacy_request_compatible"])

    def test_repeated_readback_failure_keeps_original_written_fields(self):
        operation_id = "event-repeat-readback-failure"
        written_fields = {"事件描述": "原始已写入内容", "事件等级": "I3"}
        robot_payload = {"text": "原始群消息", "level": "I3"}
        PortalRuntime.state_store.begin_notice_remote_operation(
            operation_id=operation_id,
            operation_type="update",
            request={"action": "update", "notice_type": "事件通告"},
            target_record_id="rec-repeat-readback",
        )
        PortalRuntime.state_store.mark_notice_remote_operation(
            operation_id,
            status="remote_written",
            target_record_id="rec-repeat-readback",
            result={
                "written_fields": written_fields,
                "robot_payload": robot_payload,
                "robot_delivery_state": "pending",
            },
        )
        payload = NoticePayload(text=_event_text("更新"))

        with patch.object(
            PortalRuntime,
            "_verify_event_remote_write",
            return_value=(False, {}, "still unavailable"),
        ) as verify:
            for _ in range(2):
                verified, _query, _error, _robot = (
                    PortalRuntime._verify_and_send_event_remote_write(
                        operation_id,
                        payload,
                        target_record_id="rec-repeat-readback",
                        action="update",
                        result_message="pending",
                        send_message=False,
                    )
                )
                self.assertFalse(verified)

        self.assertEqual(verify.call_count, 2)
        operation = PortalRuntime.state_store.get_notice_remote_operation(operation_id)
        self.assertEqual(operation["result"]["written_fields"], written_fields)
        self.assertEqual(operation["result"]["robot_payload"], robot_payload)

    def test_robot_delivery_is_serial_and_terminal_state_never_regresses(self):
        operation_id = "event-robot-serial"
        PortalRuntime.state_store.begin_notice_remote_operation(
            operation_id=operation_id,
            operation_type="start",
            request={"action": "start", "notice_type": "事件通告"},
            target_record_id="rec-robot-serial",
        )
        PortalRuntime.state_store.mark_notice_remote_operation(
            operation_id,
            status="remote_written",
            target_record_id="rec-robot-serial",
            result={
                "remote_verified": True,
                "robot_delivery_state": "pending",
                "robot_message_uuid": PortalRuntime._event_robot_message_uuid(
                    operation_id
                ),
                "robot_payload": PortalRuntime._event_robot_retry_payload(
                    NoticePayload(text=_event_text("开始"), level="I3")
                ),
            },
        )
        send_calls = []
        results = []

        def send(*_args, **_kwargs):
            send_calls.append(1)
            time.sleep(0.05)
            return {
                "robot_sent": True,
                "robot_skipped": False,
                "last_robot_error": "",
            }

        def run():
            results.append(
                PortalRuntime._send_deferred_event_robot(
                    operation_id,
                    NoticePayload(text=_event_text("开始")),
                    target_record_id="rec-robot-serial",
                    result_message="ok",
                )
            )

        with patch.object(
            server_module,
            "send_robot_message_by_payload",
            side_effect=send,
        ):
            threads = [threading.Thread(target=run) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=2)

        self.assertEqual(send_calls, [1])
        self.assertEqual(len(results), 2)
        self.assertTrue(all(item.get("robot_sent") for item in results))
        operation = PortalRuntime.state_store.get_notice_remote_operation(operation_id)
        self.assertTrue(operation["result"]["robot_sent"])
        self.assertEqual(operation["result"]["robot_delivery_state"], "sent")
        self.assertEqual(len(PortalRuntime.event_robot_locks), 0)

    def test_robot_delivery_serializes_different_operations_for_same_target(self):
        target_record_id = "rec-robot-shared-target"
        operation_payloads = (
            ("event-robot-old-start", _event_text("开始")),
            ("event-robot-new-update", _event_text("更新")),
        )
        for operation_id, text in operation_payloads:
            PortalRuntime.state_store.begin_notice_remote_operation(
                operation_id=operation_id,
                operation_type="start" if "start" in operation_id else "update",
                request={"notice_type": "事件通告"},
                target_record_id=target_record_id,
            )
            PortalRuntime.state_store.mark_notice_remote_operation(
                operation_id,
                status="remote_written",
                target_record_id=target_record_id,
                result={
                    "remote_verified": True,
                    "robot_delivery_state": "pending",
                    "robot_payload": {"text": text, "level": "I3"},
                },
            )

        first_entered = threading.Event()
        release_first = threading.Event()
        sent_texts = []

        def send(_notice_type, payload, **_kwargs):
            sent_texts.append(payload.text)
            if len(sent_texts) == 1:
                first_entered.set()
                release_first.wait(timeout=2)
            return {
                "robot_sent": True,
                "robot_skipped": False,
                "last_robot_error": "",
            }

        def run(operation_id, text):
            PortalRuntime._send_deferred_event_robot(
                operation_id,
                NoticePayload(text=text),
                target_record_id=target_record_id,
                result_message="ok",
            )

        with patch.object(
            server_module,
            "send_robot_message_by_payload",
            side_effect=send,
        ):
            first = threading.Thread(target=run, args=operation_payloads[0])
            second = threading.Thread(target=run, args=operation_payloads[1])
            first.start()
            self.assertTrue(first_entered.wait(timeout=2))
            second.start()
            time.sleep(0.02)
            self.assertEqual(len(sent_texts), 1)
            release_first.set()
            first.join(timeout=2)
            second.join(timeout=2)

        self.assertEqual(
            sent_texts,
            [operation_payloads[0][1], operation_payloads[1][1]],
        )
        self.assertEqual(len(PortalRuntime.event_robot_locks), 0)

    def test_completed_start_with_finished_target_is_not_adopted(self):
        request = {
            "action": "start",
            "notice_type": "事件通告",
            "text_sha256": hashlib.sha256(
                _event_text("开始").encode("utf-8")
            ).hexdigest(),
        }
        PortalRuntime.state_store.begin_notice_remote_operation(
            operation_id="event-old-finished-start",
            operation_type="start",
            request=request,
        )
        PortalRuntime.state_store.mark_notice_remote_operation(
            "event-old-finished-start",
            status="completed",
            target_record_id="rec-old-finished-event",
            result={"record_id": "rec-old-finished-event", "robot_sent": True},
        )
        current = PortalRuntime.state_store.begin_notice_remote_operation(
            operation_id="event-new-start-generation",
            operation_type="start",
            request=request,
        )

        with patch.object(
            server_module,
            "external_real_write_guard",
            return_value={"mock_external": False},
        ), patch.object(
            server_module,
            "query_record_by_id",
            return_value=(
                True,
                {
                    "fields": {"事件结束时间": 1788170400000},
                    "record_version": "ended-v1",
                },
            ),
        ):
            adopted = PortalRuntime._adopt_matching_event_remote_operation(current)

        self.assertTrue(adopted["created"])
        self.assertFalse(adopted["replay"])
        self.assertEqual(adopted.get("target_record_id"), "")

    def test_remote_written_resume_rejects_changed_text(self):
        operation_id = "event-resume-changed-text"
        old_text = _event_text("更新")
        PortalRuntime.state_store.begin_notice_remote_operation(
            operation_id=operation_id,
            operation_type="update",
            request={
                "action": "update",
                "notice_type": "事件通告",
                "text_sha256": hashlib.sha256(old_text.encode("utf-8")).hexdigest(),
            },
            target_record_id="rec-resume-changed-text",
        )
        PortalRuntime.state_store.mark_notice_remote_operation(
            operation_id,
            status="remote_written",
            target_record_id="rec-resume-changed-text",
            result={"written_fields": {"事件描述": "旧正文"}},
        )
        request = _request(
            "update",
            operation_id=operation_id,
            record_id="rec-resume-changed-text",
        )
        request["data_dict"]["text"] = _event_text("更新") + "\n新一代正文"
        request["data_dict"]["_remote_written_pending_verification"] = True

        with self.assertRaisesRegex(PortalError, "当前通告内容不一致"):
            PortalRuntime.execute_local_notice_upload(request)

    def test_event_lock_is_held_through_verification_and_projection(self):
        record_id = "rec-event-lock-order"
        request = _request(
            "update",
            operation_id="event-lock-order",
            record_id=record_id,
        )
        attachment = PortalRuntime.state_store.put_notice_upload_attachment(
            open_id="qt-local",
            file_name="lock-order.png",
            mime_type="image/png",
            content=b"lock-order",
        )
        request["screenshot_upload_id"] = attachment["upload_id"]
        initial = NoticePayload(
            text=_event_text("开始"),
            level="I3",
            buildings=["A楼"],
            specialty="电气",
            event_source="BMS动环系统告警",
            response_time="2026-08-31 10:00",
            occurrence_date="2026-08-31 10:00",
        )
        remote = {
            "fields": EventNoticeHandler("事件通告").build_create_fields(initial),
            "version": "before-lock-update",
        }
        sequence = []
        query_count = 0

        def query(current_record_id, _notice_type):
            nonlocal query_count
            query_count += 1
            sequence.append("verify" if query_count > 1 else "prequery")
            return True, {
                "record_id": current_record_id,
                "fields": dict(remote["fields"]),
                "record_version": remote["version"],
            }

        def update(current_record_id, notice_type, notice_payload):
            sequence.append("write")
            fields = EventNoticeHandler(notice_type).build_update_fields(
                notice_payload
            )
            setattr(notice_payload, "_clipflow_written_fields", dict(fields))
            remote["fields"].update(fields)
            remote["version"] = "after-lock-update"
            return True, current_record_id

        original_release = PortalRuntime._release_event_operation_lock

        def release(lock_key, owner):
            sequence.append("release")
            return original_release(lock_key, owner)

        def project(*_args, **_kwargs):
            sequence.append("projection")
            return "projection-v1"

        with patch.object(
            server_module,
            "query_record_by_id",
            side_effect=query,
        ), patch.object(
            server_module,
            "upload_media_to_feishu",
            return_value=(True, "lock-order-token"),
        ), patch.object(
            server_module,
            "update_bitable_record_by_payload",
            side_effect=update,
        ), patch.object(
            PortalRuntime,
            "_remember_local_upload_target",
            side_effect=project,
        ), patch.object(
            PortalRuntime,
            "_release_event_operation_lock",
            side_effect=release,
        ), patch.object(
            server_module,
            "send_robot_message_by_payload",
            return_value={
                "robot_sent": True,
                "robot_skipped": False,
                "last_robot_error": "",
            },
        ):
            result = PortalRuntime.execute_local_notice_upload(request)

        self.assertTrue(result["ok"], result)
        self.assertLess(sequence.index("write"), sequence.index("verify"))
        self.assertLess(sequence.index("verify"), sequence.index("projection"))
        self.assertLess(sequence.index("projection"), sequence.index("release"))

    def test_event_lock_is_released_when_remote_update_raises(self):
        record_id = "rec-event-lock-exception"
        request = _request(
            "update",
            operation_id="event-lock-exception",
            record_id=record_id,
        )
        attachment = PortalRuntime.state_store.put_notice_upload_attachment(
            open_id="qt-local",
            file_name="lock-exception.png",
            mime_type="image/png",
            content=b"lock-exception",
        )
        request["screenshot_upload_id"] = attachment["upload_id"]
        remote = {
            "fields": EventNoticeHandler("事件通告").build_create_fields(
                NoticePayload(
                    text=_event_text("开始"),
                    level="I3",
                    buildings=["A楼"],
                    specialty="电气",
                    event_source="BMS动环系统告警",
                    response_time="2026-08-31 10:00",
                    occurrence_date="2026-08-31 10:00",
                )
            ),
            "version": "before-exception",
        }
        with patch.object(
            server_module,
            "query_record_by_id",
            side_effect=self._query(remote),
        ), patch.object(
            server_module,
            "upload_media_to_feishu",
            return_value=(True, "lock-exception-token"),
        ), patch.object(
            server_module,
            "update_bitable_record_by_payload",
            side_effect=RuntimeError("write exploded"),
        ):
            with self.assertRaisesRegex(RuntimeError, "write exploded"):
                PortalRuntime.execute_local_notice_upload(request)

        lock_key = f"event:target:{record_id}"
        acquired, owner = PortalRuntime.state_store.acquire_notice_operation_lock(
            lock_key,
            action="post-exception-check",
        )
        self.assertTrue(acquired)
        PortalRuntime.state_store.release_notice_operation_lock(lock_key, owner)


if __name__ == "__main__":
    unittest.main()

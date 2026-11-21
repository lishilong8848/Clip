"""Qt event delivery receipts and ordered background Bitable synchronization."""
import copy
import time
import uuid
from contextlib import contextmanager
from contextvars import ContextVar

from . import event_priority
from .identity_utils import canonical_target_record_id, normalize_notice_identity_payload
from .notice_actions import trusted_notice_request
from .operation_audit import finish_business_audit
from .portal_service import PortalError
from upload_event_module.config import EVENT_NOTICE_FIELDS
from upload_event_module.services.handlers import get_notice_handler
from upload_event_module.services.handlers.base import NoticePayload

EVENTS = "qt_event_delivery_events"
OPERATIONS = "qt_event_delivery_operations"
BINDINGS = "qt_event_delivery_bindings"
MESSAGES = "qt_event_messages"
ARCHIVES = "qt_event_archives"
ARCHIVING = ContextVar("qt_event_archiving", default="")
NOTICE_TYPE = "事件通告"


class DeliveryBusy(Exception):
    pass


class QtEventDelivery:
    def __init__(self, runtime):
        self.runtime = runtime
        self.store = runtime.state_store

    @contextmanager
    def lock(self, event_id):
        key, owner = f"qt-event-delivery:{event_id}", uuid.uuid4().hex
        acquired, _ = self.store.acquire_notice_operation_lock(
            key, owner=owner, action="qt_event_delivery", ttl_seconds=180,
        )
        if not acquired:
            raise DeliveryBusy("这条事件正在发送，请稍后核对原任务。")
        try:
            yield
        finally:
            self.store.release_notice_operation_lock(key, owner)

    def operation(self, operation_id):
        return self.store.get_document(OPERATIONS, operation_id)

    def status(self, operation_id):
        operation = self.operation(operation_id)
        if not operation:
            return None
        result = operation.get("qt_result") or {}
        return {
            "operation_id": operation_id,
            "operation_type": operation["action"],
            "status": ("completed" if result.get("ok") else "failed") if result else "executing",
            "target_record_id": result.get("real_record_id") or operation["target_record_id"],
            "observed_record_version": result.get("record_version") or "",
            "result": {"qt_event_delivery": True, "qt_result": result,
                       "archive_state": operation.get("archive_state", ""),
                       "robot_sent": operation.get("robot_sent", False),
                       "remote_written": result.get("remote_written", False)},
            "error": operation.get("error", "") if not result.get("ok") else "",
            "updated_at": operation["updated_at"],
        }

    def submit(self, command, audit_id=""):
        command = trusted_notice_request(command)
        data = normalize_notice_identity_payload(command["data_dict"], action=command["action_type"])
        action = "start" if command["action_type"] == "upload" else command["action_type"]
        operation_id = str(command.get("operation_id") or data.get("operation_id") or "").strip()
        active = str(data.get("active_item_id") or "").strip()
        record = str(data.get("record_id") or "").strip()
        target = canonical_target_record_id(data)
        if not record or not operation_id or len(operation_id) > 160 or not data.get("text"):
            raise PortalError("事件通告缺少原文或操作标识，请重新打开原通告后提交。")
        prior = self.operation(operation_id)
        if action == "start" and target and (not prior or prior["action"] != "start"):
            action = command["action_type"] = "update"
        identity = [action, active or record, str(data["text"]), self.runtime._event_notice_building_key(data),
                    str(command.get("response_time") or ""), bool(command.get("recover_selected")),
                    str(command.get("robot_group_choice") or "auto")]
        if prior:
            return self.replay(prior, identity, target)
        event_id = ""
        for key in (target, active, record):
            if key and not event_id:
                event_id = str((self.store.get_document(BINDINGS, key) or {}).get("event_id") or "")
        event_id = event_id or target or active or record
        with self.lock(event_id):
            prior = self.operation(operation_id)
            if prior:
                return self.replay(prior, identity, target)
            event = self.store.get_document(EVENTS, event_id) or {}
            latest = self.operation(event.get("latest", "")) if event.get("latest") else None
            if latest and latest["delivery_state"] == "pending":
                raise PortalError("上一条事件群消息仍在发送，请先核对原任务。")
            if event.get("target_record_id") and target and event["target_record_id"] != target:
                raise PortalError("所选多维记录与原事件不一致，请核对。")
            target = target or event.get("target_record_id") or ""
            lifecycle = (self.store.get_document(event_priority.NAMESPACE, target) or {}) if target else {}
            reopened = (event.get("state") == "ended" and latest and latest.get("archive_state") == "synced"
                        and lifecycle.get("state") == "active" and not lifecycle.get("qt_event_id"))
            if reopened:
                event["state"] = "active"
            if event.get("state") == "ended" or (latest and latest["action"] == "end"
                                                and latest["delivery_state"] == "skipped"
                                                and not latest.get("qt_result")):
                raise PortalError("这条事件已有结束通告，请核对原任务。")
            fields = {}
            if action != "start":
                if not target and event.get("state") != "active":
                    raise PortalError("尚未定位到原事件，请绑定正确记录后再发送。")
                explicit_time = self.runtime._event_notice_time_key({"time_str": data.get("time_str")})
                if target:
                    if lifecycle.get("state") == "ended":
                        raise PortalError("目标事件已结束，请核对原事件。")
                    if not reopened:
                        try:
                            fields = self.runtime._event_local_fields(target, data)
                        except PortalError:
                            pass
                    if not fields:
                        # Manual bindings and explicit rollbacks need a fresh baseline.
                        from . import server
                        ok, row = server.query_record_by_id(target, NOTICE_TYPE)
                        fields = row.get("fields") if ok and isinstance(row, dict) else {}
                    if not fields:
                        raise PortalError("原事件状态暂未核实，请核对绑定的记录。")
                    error = self.runtime._event_followup_target_error(data, target, fields)
                    if error:
                        raise PortalError(error)
                    original_time = self.runtime._remote_event_time_key_from_fields(fields)
                else:
                    original = latest["command"]["data_dict"]
                    original_time = self.runtime._event_notice_time_key(original)
                    if self.runtime._event_notice_building_key(data) != self.runtime._event_notice_building_key(original):
                        raise PortalError("通告楼栋与原事件不一致，请核对。")
                if explicit_time and original_time and explicit_time != original_time:
                    raise PortalError("通告发生时间与原事件不一致，请核对。")
                screenshot = str(command.get("screenshot_upload_id") or "")
                if not screenshot or not self.store.get_notice_upload_attachment(screenshot):
                    raise PortalError("请重新截取当前通告后再发送。")
            elif event.get("state") == "active":
                raise PortalError("这条事件已经发送开始，请在原事件中更新。")

            data.update(operation_id=operation_id, notice_type=NOTICE_TYPE, work_type="event")
            payload = self.runtime._prepared_to_notice_payload(data)
            payload.response_time = str(command.get("response_time") or "") or None
            payload.occurrence_date = str(data.get("time_str") or "") or None
            payload.recover = bool(command.get("recover_selected"))
            suppressed = self.runtime._apply_event_group_robot_policy(
                data, action_type=command["action_type"], remote_fields=fields,
            )
            payload.robot_group_choice = "skip" if suppressed else str(command.get("robot_group_choice") or "auto")
            handler = get_notice_handler(NOTICE_TYPE)
            if action == "end":
                if not handler.build_update_fields(payload).get(EVENT_NOTICE_FIELDS["end_time"]):
                    raise PortalError("事件结束时间无效，请重新选择。")
            elif handler.is_end_state(payload):
                raise PortalError("通告正文是结束，请使用结束操作。")

            command.update(operation_id=operation_id, data_dict=data)
            previous = event.get("last_archive_operation", "")
            now = time.time()
            robot_payload = self.runtime._event_robot_retry_payload(payload)
            message_uuid = self.runtime._event_robot_message_uuid(operation_id)
            if (latest and latest["delivery_state"] == "failed"
                    and latest["action"] == action and latest["robot_payload"] == robot_payload):
                # A lost receipt can exhaust retries even though Feishu accepted the message.
                message_uuid = latest.get("message_uuid") or self.runtime._event_robot_message_uuid(latest["operation_id"])
            operation = {"operation_id": operation_id, "event_id": event_id, "intent": identity,
                         "action": action, "target_record_id": target, "record_id": record,
                         "previous_archive_operation": previous, "command": command, "audit_id": audit_id,
                         "robot_payload": robot_payload, "message_uuid": message_uuid,
                         "delivery_state": "pending", "updated_at": now}
            event.update(latest=operation_id, target_record_id=target,
                         active_item_id=active or event.get("active_item_id") or record)
            documents = {(OPERATIONS, operation_id): operation, (EVENTS, event_id): event}
            for key in {active, record, target} - {""}:
                documents[(BINDINGS, key)] = {"event_id": event_id}
            self.store.enqueue_outbox_event(MESSAGES, {"operation_id": operation_id, "idempotency_key": operation_id},
                                            documents=documents)
            return self.reply(operation)

    def replay(self, operation, intent, target):
        previously_bound = operation["target_record_id"] or (operation.get("qt_result") or {}).get("target_record_id")
        if operation["intent"] != intent or (target and previously_bound and target != previously_bound):
            raise PortalError("原发送任务内容或目标有变化，请核对原任务。")
        return self.reply(operation)

    @staticmethod
    def reply(operation):
        if operation.get("qt_result"):
            return copy.deepcopy(operation["qt_result"])
        message = "群消息按配置跳过，正在同步多维表。" if operation["delivery_state"] == "skipped" else "正在发送事件群消息。"
        return {"ok": False, "queued": True, "event_delivery_pending": True,
                "job_id": operation["operation_id"], "event_wait_attempt": 0,
                "record_id": operation["record_id"], "message": message}

    def result(self, operation, *, ok, message, archived=None):
        archived = archived or {}
        target = archived.get("real_record_id") or operation["target_record_id"]
        return {"ok": ok, "message": message, "qt_event_delivery": True,
                "name": {"start": "上传", "update": "更新", "end": "结束"}[operation["action"]],
                "record_id": operation["record_id"], "real_record_id": target if ok else "",
                "target_record_id": target, "operation_id": operation["operation_id"],
                "robot_sent": bool(operation.get("robot_sent")), "message_sent": bool(operation.get("robot_sent")),
                "robot_skipped": operation["delivery_state"] == "skipped",
                "remote_written": bool(archived.get("remote_written")),
                "archive_state": operation.get("archive_state", ""), "record_version": archived.get("record_version", "")}

    @staticmethod
    def priority_documents(event_id, event, command):
        alias = "qt-message:" + event_id
        target = event.get("target_record_id")
        doc = {"state": event["state"], "target_record_id": target or "",
               "active_item_id": event.get("active_item_id", ""), "qt_event_id": event_id,
               "summary": event_priority.summary(command["data_dict"]), "updated_at": time.time()}
        return {(event_priority.NAMESPACE, alias): {**doc, "state": "linked"},
                (event_priority.NAMESPACE, target): doc} if target else {(event_priority.NAMESPACE, alias): doc}

    def notify_qt(self, operation):
        if operation.get("qt_result"):
            operation_id = operation["operation_id"]
            self.store.enqueue_outbox_event("qt_action", {
                "kind": "notice_wait_result", "idempotency_key": f"qt-event-result:{operation_id}",
                "payload": {"job_id": operation_id, "attempt": 0, "command": operation["command"],
                            "result": operation["qt_result"]},
            })

    def process_messages(self):
        from . import server

        for task in self.store.lease_outbox_events(MESSAGES, limit=5, lease_seconds=180, respect_retry_at=True):
            operation_id = task["payload"]["operation_id"]
            operation = self.operation(operation_id)
            if not operation:
                self.store.mark_outbox_event(task["id"], "failed", error="原发送任务不存在。")
                continue
            event_id = operation["event_id"]
            try:
                with self.lock(event_id):
                    operation = self.operation(operation_id)
                    if operation["delivery_state"] == "pending":
                        if operation.get("next_message_at", 0) > time.time():
                            raise DeliveryBusy()
                        sent = server.send_robot_message_by_payload(
                            NOTICE_TYPE, NoticePayload(**operation["robot_payload"]),
                            message_uuid=operation.get("message_uuid") or self.runtime._event_robot_message_uuid(operation_id),
                        )
                        if not (sent.get("robot_sent") or sent.get("robot_skipped")):
                            raise PortalError(sent.get("last_robot_error") or "事件群消息未确认送达。")
                        sent_to_group = bool(sent.get("robot_sent"))
                        operation.update(delivery_state="sent" if sent_to_group else "skipped", archive_state="pending",
                                         robot_sent=sent_to_group, updated_at=time.time(), error="")
                        event = self.store.get_document(EVENTS, event_id)
                        event["last_archive_operation"] = operation_id
                        documents = {(EVENTS, event_id): event, (OPERATIONS, operation_id): operation}
                        if sent_to_group:
                            event["state"] = "ended" if operation["action"] == "end" else "active"
                            operation["qt_result"] = self.result(operation, ok=True, message="事件群消息已发送，多维表后台同步中。")
                            documents.update(self.priority_documents(event_id, event, operation["command"]))
                        elif operation["action"] == "start":
                            event["state"] = "active"
                        with event_priority.STATE_LOCK:
                            self.store.enqueue_outbox_event(ARCHIVES, {"operation_id": operation_id, "idempotency_key": operation_id},
                                                            documents=documents)
                        if sent_to_group:
                            self.runtime.action_queue_event.set()
                            self.runtime.clear_payload_cache()
                self.notify_qt(operation)
                if operation.get("robot_sent") and operation["action"] == "end":
                    self.runtime._enqueue_active_delete_for_ended_notice(
                        operation["command"]["data_dict"], remote_record_id=operation["target_record_id"],
                        job_id=operation_id, enqueue_if_missing=False,
                    )
                if operation["delivery_state"] == "failed" and operation.get("audit_id"):
                    finish_business_audit(self.store, operation["audit_id"], success=False,
                                          error=operation.get("error", ""), error_stage="group_delivery")
                self.store.mark_outbox_event(task["id"], "done")
            except DeliveryBusy:
                self.store.mark_outbox_event(task["id"], "pending", count_attempt=False,
                                             retry_at=max(time.time() + 2, (operation or {}).get("next_message_at", 0)))
            except Exception as exc:
                operation = self.operation(operation_id)
                retry_at = time.time() + 2
                if operation and operation["delivery_state"] == "pending":
                    attempts = int(operation.get("message_attempts", 0)) + 1
                    operation.update(message_attempts=attempts, next_message_at=time.time() + min(120, 5 * 2 ** min(attempts, 5)),
                                     error=str(exc), updated_at=time.time())
                    if attempts >= 5:
                        operation.update(delivery_state="failed", qt_result=self.result(
                            operation, ok=False, message=f"事件群消息发送失败：{exc}",
                        ))
                    else:
                        retry_at = operation["next_message_at"]
                    self.store.put_document(OPERATIONS, operation_id, operation)
                self.store.mark_outbox_event(task["id"], "pending", error=str(exc), count_attempt=False, retry_at=retry_at)
                if operation and operation["delivery_state"] == "failed":
                    self.notify_qt(operation)

    def process_archives(self):
        for task in self.store.lease_outbox_events(ARCHIVES, limit=5, lease_seconds=300, respect_retry_at=True):
            operation_id = task["payload"]["operation_id"]
            operation = self.operation(operation_id) or {}
            if not operation:
                self.store.mark_outbox_event(task["id"], "failed", error="原归档任务不存在。")
                continue
            pending = operation.get("previous_archive_operation")
            previous = (self.operation(pending) or {}) if pending else {}
            if operation.get("next_archive_at", 0) > time.time() or pending and previous.get("archive_state") != "synced":
                self.store.mark_outbox_event(task["id"], "pending", count_attempt=False, retry_at=max(
                    time.time() + 5, operation.get("next_archive_at", 0), previous.get("next_archive_at", 0)))
                continue
            try:
                if operation["archive_state"] != "synced":
                    event_id = operation["event_id"]
                    event = self.store.get_document(EVENTS, event_id)
                    command = copy.deepcopy(operation["command"])
                    target = operation["target_record_id"] or event.get("target_record_id")
                    if target:
                        command["data_dict"].update(record_id=target, target_record_id=target,
                                                    _is_placeholder_record=False)
                    token = ARCHIVING.set(operation_id)
                    try:
                        archived = self.runtime.execute_local_notice_upload(command)
                    finally:
                        ARCHIVING.reset(token)
                    if not archived.get("ok") or not archived.get("real_record_id"):
                        raise PortalError(archived.get("message") or "归档回读待核验。")
                    with self.lock(event_id):
                        event = self.store.get_document(EVENTS, event_id)
                        event["target_record_id"] = target = archived["real_record_id"]
                        if operation["action"] == "end":
                            event["state"] = "ended"
                        operation.update(archive_state="synced", updated_at=time.time(), error="")
                        operation["qt_result"] = self.result(operation, ok=True, message="事件多维表已同步。", archived=archived)
                        documents = {(EVENTS, event_id): event, (OPERATIONS, operation_id): operation,
                                     (BINDINGS, target): {"event_id": event_id}}
                        latest = self.operation(event["last_archive_operation"]) or operation
                        documents.update(self.priority_documents(event_id, event, latest["command"]))
                        with event_priority.STATE_LOCK:
                            self.store.put_documents_atomic(documents)
                    self.runtime.action_queue_event.set()
                    self.runtime.clear_payload_cache()
                    self.runtime.service.clear_target_record_cache()
                if operation["delivery_state"] == "skipped":
                    self.notify_qt(operation)
                if operation.get("audit_id"):
                    finish_business_audit(self.store, operation["audit_id"], success=True,
                                          result=operation["qt_result"], remote_written=True)
                self.store.mark_outbox_event(task["id"], "done")
            except Exception as exc:
                retry_at = time.time() + 5
                if operation.get("archive_state") != "synced":
                    attempts = int(operation.get("archive_attempts", 0)) + 1
                    operation.update(archive_state="retry", archive_attempts=attempts, error=str(exc),
                                     updated_at=time.time(), next_archive_at=time.time() + min(600, 10 * 2 ** min(attempts, 6)))
                    if operation.get("qt_result"):
                        operation["qt_result"].update(archive_state="retry", archive_warning=str(exc))
                    self.store.put_document(OPERATIONS, operation_id, operation)
                    retry_at = operation["next_archive_at"]
                self.store.mark_outbox_event(task["id"], "pending", error=str(exc), count_attempt=False, retry_at=retry_at)

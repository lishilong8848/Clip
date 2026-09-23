"""Repair mutation checkpoints; remote writes and local recovery stay separate."""
from contextvars import ContextVar
from functools import wraps
import inspect
import json
import os
import threading
import time

_current = ContextVar("repair_mutation", default=None)
_running = set()
_guard = threading.RLock()


def repair_mutation(kind):
    def decorate(function):
        signature = inspect.signature(function)

        @wraps(function)
        def run(self, *args, **kwargs):
            from .portal_service import PortalError
            arguments = signature.bind(self, *args, **kwargs)
            arguments.apply_defaults()
            request = dict(arguments.arguments)
            request.pop("self")
            operation_id = str(request.pop("operation_id", "") or "").strip()
            if not operation_id:
                return function(self, *args, **kwargs)
            request.pop("retry_failed_operation", None)
            request = json.loads(json.dumps(request, ensure_ascii=False, default=str))
            key = (str(self._state_store.db_path), operation_id)
            with _guard:
                if key in _running:
                    raise PortalError("该维修操作仍在处理，请核验原提交，勿重复创建。")
                _running.add(key)
            token = None
            context = None
            try:
                existing = self._state_store.get_repair_management_operation(operation_id)
                fingerprint = self._repair_operation_payload_hash(request)
                if existing:
                    if existing.get("payload_hash") != fingerprint or existing.get("operation_type") != kind:
                        raise PortalError("操作标识对应的提交内容已变化，请先核验原提交。")
                    if existing["status"] in ("completed", "sync_pending") and existing.get("record_id"):
                        return self._repair_public_result(existing)
                    if existing["status"] == "remote_written":
                        return self._recover_repair_write(existing)
                    saved = existing.get("result") or {}
                    if existing["status"] in ("started", "processing", "uncertain"):
                        if self._repair_writer_alive(saved, operation_id, ignore_current=True):
                            raise PortalError("该维修操作仍在处理，请稍后核验。")
                        if saved.get("checkpoint", {}).get("phase") != "preparing":
                            raise PortalError("上次写入结果待核实，请核验原提交；不会重新创建记录。")
                        self._state_store.update_repair_management_operation(operation_id, status="failed")
                operation = self._state_store.begin_repair_management_operation(
                    operation_id, operation_type=kind, scope=request.get("scope") or "ALL",
                    summary_record_id=request.get("summary_record_id") or request.get("record_id") or "",
                    payload_hash=fingerprint, restart_failed=True,
                )
                if not operation.get("created"):
                    raise PortalError("原操作尚未核验完成，请先核验原提交。")
                import psutil
                context = {"service": self, "operation_id": operation_id, "kind": kind,
                           "request": request, "checkpoint": {"phase": "preparing", "pid": os.getpid(),
                           "process_started": psutil.Process().create_time()}}
                self._repair_checkpoint(context, "started")
                token = _current.set(context)
                arguments.arguments["operation_id"] = ""
                result = function(*arguments.args, **arguments.kwargs)
                record_id = str(result.get("record_id") or "")
                self._state_store.update_repair_management_operation(
                    operation_id, status="completed", record_id=record_id,
                    summary_record_id=result.get("summary_record_id") or record_id,
                    result={**result, "request": request, "checkpoint": context["checkpoint"]}, error="",
                )
                return {**result, "operation_id": operation_id, "remote_written": True}
            except Exception as exc:
                if context is not None:
                    phase = context["checkpoint"].get("phase")
                    status = "remote_written" if phase == "remote_written" else "uncertain" if phase == "writing" else "failed"
                    try:
                        self._repair_checkpoint(context, status, str(exc))
                    except Exception:
                        pass
                raise
            finally:
                if token is not None:
                    _current.reset(token)
                with _guard:
                    _running.discard(key)
        return run
    return decorate


class RepairOperationsMixin:
    @staticmethod
    def _repair_public_result(operation):
        result = {key: value for key, value in (operation.get("result") or {}).items()
                  if key not in ("request", "checkpoint")}
        return {**result, "record_id": operation.get("record_id") or result.get("record_id", ""),
                "operation_id": operation["operation_id"], "idempotent_replay": True,
                "remote_written": True}

    def _repair_writer_alive(self, saved, operation_id, *, ignore_current=False):
        pid = int((saved.get("checkpoint") or {}).get("pid") or 0)
        if pid == os.getpid():
            return not ignore_current and (str(self._state_store.db_path), operation_id) in _running
        if not pid:
            return False
        import psutil
        try:
            return psutil.Process(pid).create_time() == saved.get("checkpoint", {}).get("process_started")
        except psutil.NoSuchProcess:
            return False
        except psutil.AccessDenied:
            return True

    def _repair_checkpoint(self, context, status, error=""):
        checkpoint = context["checkpoint"]
        self._state_store.update_repair_management_operation(
            context["operation_id"], status=status,
            record_id=checkpoint.get("record_id", "") if checkpoint.get("phase") == "remote_written" else "",
            result={"request": context["request"], "checkpoint": checkpoint}, error=error,
        )

    def _repair_before_write(self, table_id, fields, record_id="", *, delete=False):
        from .portal_service import REPAIR_FOLLOWUP_TABLE_ID, REPAIR_MANAGEMENT_TABLE_ID
        context = _current.get()
        if not context or context["service"] is not self:
            return None
        expected = REPAIR_FOLLOWUP_TABLE_ID if context["kind"].startswith("followup_") else REPAIR_MANAGEMENT_TABLE_ID
        if table_id != expected:
            return None
        if context["checkpoint"].get("phase") == "remote_written":
            if record_id != context["checkpoint"].get("record_id") or delete:
                return None
            context["checkpoint"]["pending_fields"] = fields
            self._repair_checkpoint(context, "remote_written")
            return context
        context["checkpoint"].update(phase="writing", table_id=table_id, fields=fields,
                                     record_id=record_id, delete=delete, started_at=time.time())
        self._repair_checkpoint(context, "processing")
        return context

    def _repair_after_write(self, context, record_id):
        if context is None:
            return
        pending = context["checkpoint"].pop("pending_fields", {})
        if pending:
            context["checkpoint"]["fields"] = {**context["checkpoint"].get("fields", {}), **pending}
        context["checkpoint"].update(phase="remote_written", record_id=record_id)
        self._repair_checkpoint(context, "remote_written")

    def _repair_rejected_write(self, context):
        if context is not None and context["checkpoint"].get("phase") != "remote_written":
            context["checkpoint"]["phase"] = "preparing"
        elif context is not None:
            context["checkpoint"].pop("pending_fields", None)

    def _recover_repair_write(self, operation):
        from .portal_service import (REPAIR_SNAPSHOT_SOURCE_FOLLOWUPS, REPAIR_SNAPSHOT_SOURCE_PROJECTS,
                                     PortalError)
        saved = operation.get("result") or {}
        checkpoint = saved.get("checkpoint") or {}
        record_id = str(operation.get("record_id") or checkpoint.get("record_id") or "")
        if not record_id or checkpoint.get("phase") != "remote_written":
            raise PortalError("旧操作缺少完整写入检查点，请先核对云端记录，禁止重新创建。")
        request = saved.get("request") or {}
        followup = operation["operation_type"].startswith("followup_")
        source = REPAIR_SNAPSHOT_SOURCE_FOLLOWUPS if followup else REPAIR_SNAPSHOT_SOURCE_PROJECTS
        fields = self._repair_logical_record_fields(checkpoint["table_id"], checkpoint.get("fields") or {})
        summary_id = str(request.get("summary_record_id") or record_id)
        with self._repair_management_record_lock(summary_id):
            latest = self._state_store.get_repair_management_operation(operation["operation_id"])
            if latest and latest["status"] == "completed":
                return self._repair_public_result(latest)
            if checkpoint.get("pending_fields"):
                from .portal_service import REPAIR_SOURCE_APP_TOKEN
                data = self._request_json("records/" + record_id, app_token=REPAIR_SOURCE_APP_TOKEN,
                                          table_id=checkpoint["table_id"])
                actual = ((data.get("data") or {}).get("record") or {}).get("fields")
                if not isinstance(actual, dict) or not actual:
                    raise PortalError("主记录已写入，但补充字段回读不完整，请继续核验原提交。")
                fields = self._repair_logical_record_fields(checkpoint["table_id"], actual)
            if checkpoint.get("delete"):
                self._delete_repair_snapshot_item(source, record_id)
            else:
                self._upsert_repair_snapshot_fields(source_key=source, record_id=record_id,
                    fields=fields, **({"parent_record_id": summary_id} if followup else {}))
            if followup:
                self._schedule_repair_sync_task("followup_summary_sync", summary_record_id=summary_id,
                    scope=operation["scope"], run_immediately=True)
                from .portal_service import REPAIR_FOLLOWUP_EVENT_EMERGENCY_FIELD_NAME as emergency
                if emergency in (request.get("fields") or {}):
                    value = self._repair_management_plain_text(request["fields"][emergency]).strip()
                    self._schedule_repair_sync_task("relation_field_sync", summary_record_id=summary_id,
                        scope=operation["scope"], target_record_id="event_emergency",
                        task_payload={"project_overrides": {emergency: value}, "event_overrides": {emergency: value},
                                      "include_target_fields": False}, run_immediately=True)
            else:
                event = request.get("source_event_id") or ""
                self._schedule_repair_sync_task("project_relations_sync", summary_record_id=summary_id,
                    scope=operation["scope"], task_payload={"before_fields": checkpoint.get("before_fields") or {}}, run_immediately=True)
                self._schedule_repair_sync_task("summary_followup_copy_sync", summary_record_id=summary_id,
                    scope=operation["scope"], run_immediately=True)
                if event and operation["operation_type"] == "project_create" and request.get("sync_event_transfer_status", True):
                    self._schedule_repair_sync_task("event_transfer_sync", summary_record_id=summary_id,
                        scope=operation["scope"], task_payload={"event_record_id": event,
                        "month": request.get("source_month") or ""}, run_immediately=True)
            result = {"record_id": record_id, "summary_record_id": summary_id, "fields": fields,
                      "record_version": self._repair_snapshot_record_version(source, record_id),
                      "deleted": bool(checkpoint.get("delete")), "summary_sync_pending": True,
                      "warnings": [], "request": request, "checkpoint": checkpoint}
            self._state_store.update_repair_management_operation(operation["operation_id"],
                status="completed", record_id=record_id, result=result, error="")
            return self._repair_public_result({**operation, "record_id": record_id, "result": result})

    def repair_operation_status(self, operation_id, *, recover=False):
        from .portal_service import (PortalError, PortalNotFoundError, REPAIR_SOURCE_APP_TOKEN,
                                     REPAIR_SNAPSHOT_SOURCE_FOLLOWUPS, REPAIR_SNAPSHOT_SOURCE_PROJECTS)
        operation = self._state_store.get_repair_management_operation(operation_id)
        if not operation:
            raise PortalNotFoundError("尚未收到该提交，请保留当前输入后继续核验。")
        if operation["operation_type"] not in {"project_create", "project_update", "followup_create", "followup_update", "followup_delete"}:
            raise PortalError("该操作不属于维修表单提交。")
        saved = operation.get("result") or {}
        checkpoint = saved.get("checkpoint") or {}
        status = operation["status"]
        running = self._repair_writer_alive(saved, operation_id)
        if status in ("started", "processing") and not running:
            status = "failed" if checkpoint.get("phase") == "preparing" else "uncertain"
            changed = self._state_store.update_repair_management_operation(operation_id, status=status,
                error="执行已中断，请继续核验原提交。", expected_updated_at=operation["updated_at"])
            operation = self._state_store.get_repair_management_operation(operation_id)
            saved = operation.get("result") or {}
            checkpoint = saved.get("checkpoint") or {}
            status = operation["status"]
            running = self._repair_writer_alive(saved, operation_id)
        if recover and not running and status == "uncertain" and checkpoint.get("record_id"):
            if not checkpoint.get("delete") and operation["operation_type"] in ("project_update", "followup_update"):
                source = REPAIR_SNAPSHOT_SOURCE_FOLLOWUPS if operation["operation_type"] == "followup_update" else REPAIR_SNAPSHOT_SOURCE_PROJECTS
                snapshot = self._state_store.get_repair_snapshot(source, record_ids=[checkpoint["record_id"]])
                local = next((item for item in snapshot.get("records", [])
                              if item.get("record_id") == checkpoint["record_id"]), None)
                expected = self._repair_logical_record_fields(checkpoint["table_id"], checkpoint.get("fields") or {})
                if local and expected:
                    meta = self._state_store.get_repair_snapshot_meta(source)
                    by_name = {field.field_name: field for field in
                               (self._repair_snapshot_field_meta(item) for item in meta.get("fields", []) if isinstance(item, dict))}
                    actual = self._repair_logical_record_fields(checkpoint["table_id"],
                        {**(local.get("display_fields") or {}), **(local.get("raw_fields") or {})})
                    if all(key in actual and self._repair_followup_comparable_value(by_name.get(key), actual[key]) ==
                           self._repair_followup_comparable_value(by_name.get(key), value)
                           for key, value in expected.items()):
                        checkpoint["phase"] = "remote_written"
                        self._state_store.update_repair_management_operation(operation_id, status="remote_written",
                            record_id=checkpoint["record_id"], result={**saved, "checkpoint": checkpoint})
                        status = "remote_written"
        if recover and not running and status == "uncertain" and checkpoint.get("record_id"):
            try:
                data = self._request_json("records/" + checkpoint["record_id"], app_token=REPAIR_SOURCE_APP_TOKEN,
                                          table_id=checkpoint["table_id"])
            except PortalError as exc:
                if not checkpoint.get("delete") or "1254043" not in str(exc):
                    raise
                data = {"confirmed_deleted": True}
            fields = ((data.get("data") or {}).get("record") or {}).get("fields") or {}
            expected = checkpoint.get("fields") or {}
            _, by_name = self._load_table_fields(app_token=REPAIR_SOURCE_APP_TOKEN, table_id=checkpoint["table_id"]) if expected else ([], {})
            fields = self._repair_logical_record_fields(checkpoint["table_id"], fields)
            expected = self._repair_logical_record_fields(checkpoint["table_id"], expected)
            matches = bool(expected) and all(self._repair_followup_comparable_value(by_name.get(key), fields.get(key)) ==
                self._repair_followup_comparable_value(by_name.get(key), value) for key, value in expected.items())
            if data.get("confirmed_deleted") or (not checkpoint.get("delete") and matches):
                checkpoint["phase"] = "remote_written"
                self._state_store.update_repair_management_operation(operation_id, status="remote_written",
                    record_id=checkpoint["record_id"], result={**saved, "checkpoint": checkpoint})
                status = "remote_written"
            elif not checkpoint.get("delete") and operation["operation_type"] in ("project_update", "followup_update") and expected:
                source = REPAIR_SNAPSHOT_SOURCE_FOLLOWUPS if operation["operation_type"] == "followup_update" else REPAIR_SNAPSHOT_SOURCE_PROJECTS
                snapshot = self._state_store.get_repair_snapshot(source, record_ids=[checkpoint["record_id"]])
                original = next((item for item in snapshot.get("records", [])
                                 if item.get("record_id") == checkpoint["record_id"]), None)
                if original:
                    before = self._repair_logical_record_fields(
                        checkpoint["table_id"],
                        {**(original.get("display_fields") or {}), **(original.get("raw_fields") or {})},
                    )
                    comparable = lambda values, key: self._repair_followup_comparable_value(by_name.get(key), values.get(key))
                    changed_keys = [key for key in expected
                                    if comparable(expected, key) != comparable(before, key)]
                    unchanged = all(comparable(fields, key) == comparable(before, key) for key in changed_keys)
                    if changed_keys and unchanged:
                        checkpoint["phase"] = "preparing"
                        error = "云端未保留本次修改；请核对最新内容后重试原提交。"
                        updated = self._state_store.update_repair_management_operation(
                            operation_id, status="failed", result={**saved, "checkpoint": checkpoint},
                            error=error, expected_updated_at=operation["updated_at"],
                        )
                        if updated:
                            status = "failed"
        if recover and not running and status == "remote_written":
            self._recover_repair_write(self._state_store.get_repair_management_operation(operation_id))
            status = "completed"
        operation = self._state_store.get_repair_management_operation(operation_id)
        status = operation["status"]
        return {"operation_id": operation_id, "scope": operation["scope"], "status": status,
                "retryable": status == "failed" and checkpoint.get("phase") == "preparing",
                "record_id": operation.get("record_id") or checkpoint.get("record_id") or "",
                "remote_written": status in ("remote_written", "completed", "sync_pending"),
                "error": operation.get("last_error") or "",
                "result": self._repair_public_result(operation) if status in ("completed", "sync_pending") else None}

    def _assert_repair_remote_unchanged(self, baseline, current, meta_by_name, *, ignored_fields=()):
        from .portal_service import PortalConflictError
        before = baseline.get("raw_fields") or baseline.get("display_fields") or {}
        after = current.get("raw_fields") or current.get("display_fields") or {}
        changed = [name for name, meta in meta_by_name.items()
                   if name not in ignored_fields and not self._field_meta_is_readonly(meta)
                   and (name in before or name in after)
                   and self._repair_followup_comparable_value(meta, before.get(name)) !=
                       self._repair_followup_comparable_value(meta, after.get(name))]
        if changed:
            raise PortalConflictError("云端记录已被修改，请重新读取后保存：" + "、".join(changed[:6]))
        context = _current.get()
        if context and context["service"] is self:
            context["checkpoint"]["before_fields"] = after

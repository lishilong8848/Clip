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

        def execute(self, *args, **kwargs):
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
                    if kind == "project_update" and self._repair_project_update_superseded(existing):
                        return {"record_id": request.get("record_id", ""), "superseded": True}
                    if existing["status"] in ("completed", "sync_pending") and existing.get("record_id"):
                        return self._repair_public_result(existing)
                    if kind == "project_update" and (existing.get("result") or {}).get("checkpoint", {}).get("phase") in ("writing", "remote_written"):
                        return self._resume_repair_project_update(existing, ignore_current=True)
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
                if kind == "project_update" and context["checkpoint"].get("phase") == "remote_written":
                    confirmed = self._repair_logical_record_fields(context["checkpoint"]["table_id"], context["checkpoint"].get("fields") or {})
                    result["fields"] = {**result.get("fields", {}), **confirmed}
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

        @wraps(function)
        def run(self, *args, **kwargs):
            if kind != "project_update":
                return execute(self, *args, **kwargs)
            arguments = signature.bind(self, *args, **kwargs)
            arguments.apply_defaults()
            with self._repair_management_record_lock(arguments.arguments.get("record_id", "")):
                return execute(self, *args, **kwargs)
        return run
    return decorate


class RepairOperationsMixin:
    def _repair_project_update_superseded(self, operation):
        latest = self._state_store.latest_repair_project_update(operation.get("summary_record_id") or "")
        if operation["status"] != "superseded" and (not latest or latest["operation_id"] == operation["operation_id"]):
            return False
        self._state_store.update_repair_management_operation(operation["operation_id"], status="superseded",
            error="已有较新的保存，本次旧提交已停止。")
        return True

    def _remember_repair_project_fields(self, record):
        from .portal_service import PortalError, REPAIR_MANAGEMENT_TABLE_ID
        context = _current.get()
        if context and context["service"] is self:
            context["checkpoint"]["before_fields"] = record.get("raw_fields") or record.get("display_fields") or {}
            previous = self._state_store.latest_repair_project_update(
                record["record_id"], exclude_operation_id=context["operation_id"])
            checkpoint = ((previous or {}).get("result") or {}).get("checkpoint") or {}
            if previous and previous["status"] not in ("completed", "superseded") and checkpoint.get("phase") in ("writing", "remote_written"):
                before = checkpoint.get("before_fields") or {}
                target = self._repair_target_record_id({"raw_fields": before, "source_table_id": REPAIR_MANAGEMENT_TABLE_ID})
                if target and not self._schedule_repair_sync_task("project_relations_sync", summary_record_id=record["record_id"],
                        scope=context["request"].get("scope") or "ALL", target_record_id=target,
                        task_payload={"before_fields": before}):
                    raise PortalError("旧关联清理任务未能保存，请稍后重新保存维修单。")

    def _resume_repair_project_update(self, operation, *, ignore_current=False):
        from .portal_service import PortalError, REPAIR_SOURCE_APP_TOKEN, REPAIR_MANAGEMENT_TABLE_ID
        operation_id = operation["operation_id"]
        with self._repair_management_record_lock(operation["summary_record_id"]):
            operation = self._state_store.get_repair_management_operation(operation_id)
            saved = operation.get("result") or {}
            checkpoint = saved.get("checkpoint") or {}
            if self._repair_writer_alive(saved, operation_id, ignore_current=ignore_current):
                raise PortalError("维修单正在保存，请稍候。")
            if self._repair_project_update_superseded(operation):
                return {"record_id": operation["summary_record_id"], "superseded": True}
            if operation["status"] == "completed":
                return self._repair_public_result(operation)
            record_id = checkpoint.get("record_id")
            if not record_id or checkpoint.get("table_id") != REPAIR_MANAGEMENT_TABLE_ID:
                raise PortalError("原保存内容不完整，请重新保存维修单。")
            key = (str(self._state_store.db_path), operation_id)
            with _guard:
                owns_running = key not in _running
                _running.add(key)
            try:
                if checkpoint.get("phase") == "writing" or checkpoint.get("pending_fields"):
                    fields = {**checkpoint.get("fields", {}), **checkpoint.get("pending_fields", {})}
                    if not fields:
                        raise PortalError("原保存字段为空，请重新填写后保存。")
                    import psutil
                    checkpoint.update(pid=os.getpid(), process_started=psutil.Process().create_time())
                    self._state_store.update_repair_management_operation(operation_id, status="processing",
                        result={**saved, "checkpoint": checkpoint}, error="")
                    self._patch_record_fields(app_token=REPAIR_SOURCE_APP_TOKEN, table_id=REPAIR_MANAGEMENT_TABLE_ID,
                        record_id=record_id, fields=fields)
                    checkpoint.update(phase="remote_written", fields=fields)
                    checkpoint.pop("pending_fields", None)
                    self._state_store.update_repair_management_operation(operation_id, status="remote_written",
                        record_id=record_id, result={**saved, "checkpoint": checkpoint}, error="")
                return self._recover_repair_write(self._state_store.get_repair_management_operation(operation_id))
            except Exception as exc:
                self._state_store.update_repair_management_operation(operation_id, status="failed",
                    result={**saved, "checkpoint": checkpoint}, error=f"保存未完成：{exc}。可修改后重新保存。")
                raise
            finally:
                if owns_running:
                    with _guard:
                        _running.discard(key)

    @staticmethod
    def _repair_public_result(operation):
        result = {key: value for key, value in (operation.get("result") or {}).items()
                  if key not in ("request", "checkpoint")}
        if operation.get("operation_type") == "project_update":
            request = (operation.get("result") or {}).get("request") or {}
            result.update({key: request[key] for key in ("source_event_id", "source_repair_ids") if key in request})
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
                                     REPAIR_MANAGEMENT_TABLE_ID, PortalError)
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
                before_fields = checkpoint.get("before_fields") or {}
                before_target = self._repair_target_record_id({"raw_fields": before_fields, "source_table_id": REPAIR_MANAGEMENT_TABLE_ID})
                relation_task = self._schedule_repair_sync_task("project_relations_sync", summary_record_id=summary_id,
                    scope=operation["scope"], target_record_id=before_target,
                    task_payload={"before_fields": before_fields}, run_immediately=True)
                followup_task = self._schedule_repair_sync_task("summary_followup_copy_sync", summary_record_id=summary_id,
                    scope=operation["scope"], run_immediately=True)
                if operation["operation_type"] == "project_update" and (not relation_task or not followup_task):
                    raise PortalError("维修单已写入，关联同步任务未能保存，请重新保存维修单。")
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
        if operation["operation_type"] == "project_update":
            return self._repair_project_update_status(operation, recover=recover)
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

    def _repair_project_update_status(self, operation, *, recover):
        operation_id = operation["operation_id"]
        if not operation.get("summary_record_id"):
            self._state_store.update_repair_management_operation(operation_id, status="failed", error="原维修单标识不完整，请重新保存。")
            return {"operation_id": operation_id, "scope": operation["scope"], "status": "failed", "retryable": True,
                    "error": "原维修单标识不完整，请重新保存。", "result": None}
        if self._repair_writer_alive(operation.get("result") or {}, operation_id):
            return {"operation_id": operation_id, "scope": operation["scope"], "status": "processing", "error": "", "retryable": False}
        with self._repair_management_record_lock(operation["summary_record_id"]):
            operation = self._state_store.get_repair_management_operation(operation_id)
            saved = operation.get("result") or {}
            checkpoint = saved.get("checkpoint") or {}
            running = self._repair_writer_alive(saved, operation_id)
            if not running and self._repair_project_update_superseded(operation):
                return {"operation_id": operation_id, "scope": operation["scope"], "status": "superseded",
                        "result": {"record_id": operation["summary_record_id"], "superseded": True}}
            if recover and not running and operation["status"] in ("started", "processing", "uncertain", "remote_written"):
                try:
                    if checkpoint.get("phase") in ("writing", "remote_written"):
                        self._resume_repair_project_update(operation)
                    else:
                        request = saved.get("request") or {}
                        if not request.get("record_id"):
                            raise ValueError("原保存内容不完整，请重新保存维修单")
                        self._state_store.update_repair_management_operation(operation_id, status="failed")
                        self.update_repair_management_record(**request, operation_id=operation_id)
                except Exception as exc:
                    current = self._state_store.get_repair_management_operation(operation_id)
                    self._state_store.update_repair_management_operation(operation_id, status="failed",
                        error=current.get("last_error") or f"保存未完成：{exc}。可修改后重新保存。")
            elif not running and operation["status"] in ("started", "processing"):
                self._state_store.update_repair_management_operation(operation_id, status="failed",
                    error="保存已中断，可重新保存维修单。")
            operation = self._state_store.get_repair_management_operation(operation_id)
            status = operation["status"]
            return {"operation_id": operation_id, "scope": operation["scope"], "status": status,
                    "retryable": status == "failed", "error": operation.get("last_error") or "",
                    "record_id": operation.get("record_id") or operation["summary_record_id"],
                    "remote_written": status in ("remote_written", "completed", "sync_pending"),
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

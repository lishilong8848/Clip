import sys
import tempfile
import time
import unittest
import io
import logging
import queue
from logging.handlers import QueueHandler
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.portal_service import (
    MaintenancePortalService, FieldMeta, PortalError, PortalConflictError,
    REPAIR_FOLLOWUP_TABLE_ID, REPAIR_MANAGEMENT_TABLE_ID, REPAIR_SOURCE_APP_TOKEN,
)
from lan_bitable_template_portal.repair_operations import repair_mutation
from lan_bitable_template_portal.state_store import LanPortalStateStore
from lan_bitable_template_portal.server import PortalRuntime
import lan_bitable_template_portal.server as server


class TestService(MaintenancePortalService):
    @repair_mutation("followup_create")
    def create_test(self, fields, *, operation_id="", scope="A", summary_record_id="project"):
        prepared = {**fields, "first": self.first}
        result = self._create_record_fields(app_token=REPAIR_SOURCE_APP_TOKEN, table_id=REPAIR_FOLLOWUP_TABLE_ID, fields=prepared)
        record_id = self._created_record_id(result)
        self.first = "no"
        self._upsert_repair_snapshot_fields(source_key="followups", record_id=record_id, fields=prepared)
        return {"record_id": record_id, "summary_record_id": summary_record_id, "fields": prepared}

    @repair_mutation("project_update")
    def update_test(self, fields, *, operation_id="", record_id="project", scope="A"):
        self._patch_record_fields(app_token=REPAIR_SOURCE_APP_TOKEN, table_id=REPAIR_MANAGEMENT_TABLE_ID, record_id=record_id, fields=fields)
        self._upsert_repair_snapshot_fields(source_key="projects", record_id=record_id, fields=fields)
        return {"record_id": record_id, "fields": fields}


class SubmissionReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = TestService(enable_repair_snapshots=True)
        self.store = LanPortalStateStore(Path(self.tmp.name) / "state.sqlite3")
        self.service._state_store = self.store
        self.service.first = "yes"
        for name in ("_auth_headers", "_upsert_repair_snapshot_fields", "_schedule_repair_sync_task"):
            setattr(self.service, name, Mock(return_value={}))
        self.service._schedule_repair_sync_task.return_value = "sync-task"
        self.service._repair_physical_record_fields = lambda table, fields: fields
        self.service._repair_logical_record_fields = lambda table, fields: fields
        self.service._repair_snapshot_record_version = Mock(return_value="v2")
        self.service._request_payload = Mock(return_value={"code": 0, "data": {"record": {"record_id": "remote1"}}})
        self.service._request_json = Mock(side_effect=AssertionError("unexpected network read"))
        self.old_store = PortalRuntime.state_store
        PortalRuntime.state_store = self.store

    def tearDown(self):
        PortalRuntime.state_store = self.old_store
        self.store.shutdown_write_worker(timeout=2)
        self.tmp.cleanup()

    def test_local_failure_keeps_remote_id_and_recovers_without_second_create(self):
        self.service._upsert_repair_snapshot_fields.side_effect = OSError("disk busy")
        with self.assertRaises(OSError):
            self.service.create_test({"text": "one"}, operation_id="create")
        saved = self.store.get_repair_management_operation("create")
        self.assertEqual((saved["status"], saved["record_id"]), ("remote_written", "remote1"))
        self.service._upsert_repair_snapshot_fields.side_effect = None
        result = self.service.create_test({"text": "one"}, operation_id="create")
        self.assertEqual(result["record_id"], "remote1")
        self.service._request_payload.assert_called_once()

    def test_derived_first_followup_value_does_not_change_request_identity(self):
        first = self.service.create_test({"text": "one"}, operation_id="create")
        retry = self.service.create_test({"text": "one"}, operation_id="create")
        self.assertEqual(retry["record_id"], first["record_id"])
        self.service._request_payload.assert_called_once()
        with self.assertRaisesRegex(PortalError, "内容已变化"):
            self.service.create_test({"text": "changed"}, operation_id="create")

    def test_unknown_create_never_blindly_retries(self):
        self.service._request_payload.side_effect = TimeoutError("response lost")
        with self.assertRaises(TimeoutError):
            self.service.create_test({"text": "one"}, operation_id="unknown")
        old = self.store.get_repair_management_operation("unknown")
        checkpoint = dict(old["result"]["checkpoint"])
        checkpoint.pop("client_token")
        self.store.update_repair_management_operation("unknown", status="uncertain",
            result={**old["result"], "checkpoint": checkpoint})
        state = self.service.repair_operation_status("unknown", recover=True)
        self.assertEqual(state["status"], "uncertain")
        self.assertFalse(state["retryable"])
        with self.assertRaisesRegex(PortalError, "待核实"):
            self.service.create_test({"text": "one"}, operation_id="unknown")
        self.service._request_payload.assert_called_once()

    def test_followup_create_retries_frozen_payload_with_same_token(self):
        self.service._request_payload.side_effect = TimeoutError("response lost")
        with self.assertRaises(TimeoutError):
            self.service.create_test({"text": "one"}, operation_id="safe-create")
        first = self.service._request_payload.call_args
        self.assertEqual(first.kwargs["json_payload"]["fields"]["first"], "yes")
        token = first.kwargs["params"]["client_token"]
        self.assertEqual(len(token), 36)
        self.service._request_payload.side_effect = None
        result = self.service.create_test({"text": "one"}, operation_id="safe-create")
        self.assertEqual(result["record_id"], "remote1")
        second = self.service._request_payload.call_args
        self.assertEqual(second.kwargs["json_payload"], first.kwargs["json_payload"])
        self.assertEqual(second.kwargs["params"]["client_token"], token)
        self.service._request_json.assert_not_called()
        self.assertEqual(self.service.first, "yes")

    def test_interrupted_before_write_is_retryable_not_permanently_started(self):
        self.store.begin_repair_management_operation("interrupted", operation_type="project_update", scope="A", payload_hash="h")
        self.store.update_repair_management_operation("interrupted", status="started", result={"checkpoint": {"phase": "preparing", "pid": 0}})
        state = self.service.repair_operation_status("interrupted")
        self.assertEqual(state["status"], "failed")
        self.assertTrue(state["retryable"])

    def test_explicit_remote_rejection_is_safe_to_edit(self):
        self.service._request_payload.return_value = {"code": 1254060, "msg": "TextFieldConvFail"}
        with self.assertRaises(PortalError):
            self.service.create_test({"text": "one"}, operation_id="rejected")
        self.assertTrue(self.service.repair_operation_status("rejected")["retryable"])

    def test_project_update_replays_same_record_without_readback(self):
        self.service._request_payload.side_effect = TimeoutError("response lost")
        with self.assertRaises(TimeoutError):
            self.service.update_test({"text": "one"}, operation_id="update")
        self.service._request_payload.side_effect = None
        state = self.service.repair_operation_status("update", recover=True)
        self.assertEqual(state["status"], "completed")
        self.assertEqual(self.service._request_payload.call_count, 2)
        first, second = self.service._request_payload.call_args_list
        self.assertEqual(first, second)
        self.assertEqual(second.args[0], "PUT")
        self.assertTrue(second.args[1].endswith("/records/project"))
        self.service._request_json.assert_not_called()

    def test_interrupted_update_does_not_treat_cache_as_cloud_success(self):
        self.service._request_payload.side_effect = TimeoutError("response lost")
        with self.assertRaises(TimeoutError):
            self.service.update_test({"text": "new"}, operation_id="local-confirmed")
        self.store.upsert_repair_snapshot_record(
            "repair_projects", "project", {"record_id": "project", "raw_fields": {"text": "new"}},
        )
        state = self.service.repair_operation_status("local-confirmed", recover=True)
        self.assertEqual((state["status"], state["retryable"]), ("failed", True))
        self.assertEqual(state["record_id"], "project")
        self.service._request_json.assert_not_called()
        self.assertEqual(self.service._request_payload.call_count, 2)
        self.service._request_payload.side_effect = None
        result = self.service.update_test({"text": "new"}, operation_id="local-confirmed")
        self.assertEqual(result["record_id"], "project")
        self.assertEqual(self.service._request_payload.call_count, 3)

    def test_interrupted_project_update_restarts_frozen_write(self):
        self.store.upsert_repair_snapshot_record(
            "repair_projects", "project", {"record_id": "project", "raw_fields": {"text": "old"}},
        )
        self.service._request_payload.side_effect = TimeoutError("response lost")
        with self.assertRaises(TimeoutError):
            self.service.update_test({"text": "new"}, operation_id="update-unchanged")
        interrupted = self.store.get_repair_management_operation("update-unchanged")
        checkpoint = {**interrupted["result"]["checkpoint"], "pid": 0}
        self.store.update_repair_management_operation(
            "update-unchanged", status="processing",
            result={**interrupted["result"], "checkpoint": checkpoint},
        )
        self.service._request_payload.side_effect = None
        state = self.service.repair_operation_status("update-unchanged", recover=True)
        self.assertEqual(state["status"], "completed")
        self.assertEqual(self.store.get_repair_management_operation("update-unchanged")["result"]["checkpoint"]["phase"], "remote_written")
        self.service._request_json.assert_not_called()
        self.assertEqual(self.service._request_payload.call_count, 2)

    def test_legacy_update_recovers_missing_checkpoint_target(self):
        self.service._request_payload.side_effect = TimeoutError("response lost")
        with self.assertRaises(TimeoutError):
            self.service.update_test({"text": "new"}, record_id="recProject", operation_id="legacy-update")
        old = self.store.get_repair_management_operation("legacy-update")
        checkpoint = dict(old["result"]["checkpoint"])
        checkpoint.pop("record_id")
        checkpoint.pop("table_id")
        checkpoint["pid"] = 0
        self.store.update_repair_management_operation("legacy-update", status="uncertain",
            result={**old["result"], "checkpoint": checkpoint})
        self.service._request_payload.side_effect = None
        state = self.service.repair_operation_status("legacy-update", recover=True)
        self.assertEqual(state["status"], "completed")
        self.assertEqual(self.service._request_payload.call_count, 2)
        self.assertEqual(self.service._request_payload.call_args.kwargs["json_payload"]["fields"], {"text": "new"})
        self.service._request_json.assert_not_called()

    def test_incomplete_update_keeps_fields_for_new_save(self):
        self.store.begin_repair_management_operation("broken-update", operation_type="project_update", scope="A",
            summary_record_id="recProject", payload_hash="old")
        self.store.update_repair_management_operation("broken-update", status="uncertain", result={
            "request": {"record_id": "recProject", "fields": {"text": "new"}},
            "checkpoint": {"phase": "writing", "pid": 0}}, error="原保存内容不完整")
        state = self.service.repair_operation_status("broken-update", recover=True)
        self.assertEqual((state["status"], state["retryable"]), ("failed", True))
        self.assertIn("缺少写入字段", state["error"])
        self.service.update_test({"text": "new"}, record_id="recProject", operation_id="fresh-update")
        self.assertEqual(self.store.get_repair_management_operation("fresh-update")["status"], "completed")
        self.service._request_payload.assert_called_once()

    def test_unwritten_repair_relation_mirror_is_retryable(self):
        self.store.upsert_repair_snapshot_record(
            "repair_projects", "project", {"record_id": "project", "raw_fields": {"设备检修关联-L": None}},
        )
        self.service._repair_physical_record_fields = MaintenancePortalService._repair_physical_record_fields
        self.service._repair_logical_record_fields = MaintenancePortalService._repair_logical_record_fields
        self.service._request_payload.side_effect = TimeoutError("response lost")
        with self.assertRaises(TimeoutError):
            self.service.update_test({"设备检修关联": "recTarget123"}, operation_id="link-update")
        self.service._request_payload.side_effect = None
        state = self.service.repair_operation_status("link-update", recover=True)
        self.assertEqual(state["status"], "completed")
        self.assertEqual(self.service._request_payload.call_args.kwargs["json_payload"]["fields"], {"设备检修关联-L": "recTarget123"})
        self.service._request_json.assert_not_called()

    def test_old_project_retry_cannot_overwrite_later_save(self):
        self.service._request_payload.side_effect = TimeoutError("response lost")
        with self.assertRaises(TimeoutError):
            self.service.update_test({"text": "old"}, operation_id="old-save")
        self.service._request_payload.side_effect = None
        self.service.update_test({"text": "latest"}, operation_id="new-save")
        state = self.service.repair_operation_status("old-save", recover=True)
        self.assertEqual(state["status"], "superseded")
        replay = self.service.update_test({"text": "old"}, operation_id="old-save")
        self.assertTrue(replay["superseded"])
        self.assertEqual(self.service._request_payload.call_count, 2)
        self.assertEqual(self.service._request_payload.call_args.kwargs["json_payload"]["fields"], {"text": "latest"})

    def test_project_recovery_keeps_failed_relation_queue_retryable(self):
        self.service._upsert_repair_snapshot_fields.side_effect = OSError("disk busy")
        with self.assertRaises(OSError):
            self.service.update_test({"text": "new"}, operation_id="recover-queue")
        self.service._upsert_repair_snapshot_fields.side_effect = None
        self.service._schedule_repair_sync_task.return_value = ""
        state = self.service.repair_operation_status("recover-queue", recover=True)
        self.assertEqual(state["status"], "failed")
        self.assertTrue(state["retryable"])
        self.service._schedule_repair_sync_task.return_value = "queued"
        self.service.update_test({"text": "new"}, operation_id="recover-queue")
        self.assertEqual(self.service.repair_operation_status("recover-queue")["status"], "completed")
        self.service._request_payload.assert_called_once()

    def test_status_does_not_replay_active_project_writer(self):
        def write(*args, **kwargs):
            state = self.service.repair_operation_status("running", recover=True)
            self.assertEqual(state["status"], "processing")
            self.assertFalse(state["retryable"])
            return {"code": 0}
        self.service._request_payload.side_effect = write
        self.service.update_test({"text": "new"}, operation_id="running")
        self.service._request_payload.assert_called_once()

    def test_new_save_retains_interrupted_old_backlink_cleanup(self):
        @repair_mutation("project_update")
        def save(self, fields, *, record_id="recProject", operation_id="", scope="A"):
            self._remember_repair_project_fields({"record_id": record_id, "raw_fields": {"设备检修关联-L": "recOldRepair"}})
            return self.update_test(fields, record_id=record_id)
        self.service._request_payload.side_effect = TimeoutError("lost")
        with self.assertRaises(TimeoutError):
            save(self.service, {"text": "first"}, operation_id="first")
        self.service._request_payload.side_effect = None
        save(self.service, {"text": "second"}, operation_id="second")
        queued = self.service._schedule_repair_sync_task.call_args
        self.assertEqual(queued.args[0], "project_relations_sync")
        self.assertEqual(queued.kwargs["target_record_id"], "recOldRepair")
        self.assertEqual(queued.kwargs["task_payload"]["before_fields"]["设备检修关联-L"], "recOldRepair")

    def test_relation_recovery_uses_latest_cloud_binding(self):
        self.service.get_repair_management_record = Mock(return_value={"record": {
            "record_id": "recProject", "source_table_id": REPAIR_MANAGEMENT_TABLE_ID,
            "raw_fields": {"关联事件单": "recNewEvent", "设备检修关联-L": "recNewRepair"}}})
        self.service._sync_repair_project_relations = Mock(return_value={"warnings": []})
        self.service._execute_repair_sync_task({"operation_type": "project_relations_sync", "summary_record_id": "recProject", "scope": "A",
            "result": {"task_payload": {"before_fields": {"设备检修关联-L": "recOldRepair"}}}})
        self.service.get_repair_management_record.assert_called_once_with("recProject", scope="A", force_refresh=True)
        synced = self.service._sync_repair_project_relations.call_args.kwargs
        self.assertEqual(synced["event_record_id"], "recNewEvent")
        self.assertEqual(synced["target_record_ids"], ["recNewRepair"])
        self.assertEqual(synced["previous_target_record_ids"], ["recOldRepair"])

    def test_remote_change_detected_even_when_local_version_matches(self):
        meta = FieldMeta("field", "text", "Text", 1, False, {}, [], False)
        with self.assertRaises(PortalConflictError):
            self.service._assert_repair_remote_unchanged({"raw_fields": {"text": "old"}}, {"raw_fields": {"text": "new"}}, {"text": meta})

    def test_project_rebind_replaces_source_fields_and_removes_old_end_time(self):
        names = ["关联事件单", "设备检修关联", "对应来源", "对应事件等级", "事件描述", "事件应急措施",
                 "故障发生时间", "故障维修原因", "故障发生现象描述", "所属专业", "专业（推送消息用）",
                 "所属数据中心/楼栋-使用", "维修开始时间", "维修结束时间（2026）", "检修通告名称",
                 "随工人员（或我方维修人员）", "设备名称", "维修进展描述", "维修方"]
        metas = [FieldMeta(str(i), name, "Text", 1, False, {}, [], False) for i, name in enumerate(names)]
        by_name = {meta.field_name: meta for meta in metas}
        old = {"record_id": "recProject", "source_table_id": REPAIR_MANAGEMENT_TABLE_ID, "record_version": "cloud-version", "raw_fields": {
            **dict.fromkeys(names, "旧内容"), "关联事件单": "recOldEvent", "设备检修关联": "recOldRepair"},
            "display_fields": {"所属数据中心/楼栋-使用": "南通A楼"}}
        self.service._repair_management_snapshot_schema = Mock(return_value=(metas, by_name))
        self.service._ensure_repair_management_record_in_scope = Mock(return_value=old)
        self.service._load_table_records_by_ids = Mock(return_value=[old])
        self.service._repair_management_record_in_scope = Mock(return_value=True)
        self.service._repair_management_fields_in_scope = Mock(return_value=True)
        self.service._load_repair_followups_for_summary = Mock(return_value=([], {}, []))
        self.service._event_snapshot_record_for_repair = Mock(return_value={"record_id": "recNewEvent", "display_fields": {
            "事件发现来源（统一）": "巡检", "事件等级": "I2", "机楼": "A楼", "专业": "暖通",
            "事件发生时间": "2026-09-23 10:00", "告警描述": "新故障", "事件简述": "新事件"}})
        self.service._load_repair_management_target_records_by_ids = Mock(return_value=([], {}, [{
            "record_id": "recNewRepair", "raw_fields": {}, "display_fields": {"楼栋": "A楼",
            "名称（标题）": "新检修", "维修设备": "新设备", "故障原因": "新原因", "维修方式": "自维"}}]))
        self.service._repair_management_target_record_in_scope = Mock(return_value=True)
        self.service._sync_repair_target_summary_id = Mock(return_value={"synced": True})
        self.service._ensure_repair_followup_select_options = Mock(return_value=(metas, by_name))
        self.service._sync_repair_project_relations = Mock(return_value={})
        self.service._sync_repair_management_workflow = Mock(return_value=(False, []))
        result = self.service.update_repair_management_record("recProject",
            {"所属专业": "旧专业", "故障发生现象描述": "旧故障"}, scope="A", expected_version="stale",
            source_event_id="recNewEvent", source_repair_ids=["recNewRepair"], replace_source_relations=True,
            operation_id="rebind")
        saved = result["fields"]
        self.assertEqual(saved["关联事件单"], "recNewEvent")
        self.assertEqual(saved["设备检修关联"], "recNewRepair")
        self.assertEqual(saved["所属专业"], "暖通")
        self.assertEqual(saved["故障发生现象描述"], "新故障")
        self.assertEqual(saved["故障维修原因"], "新原因")
        self.assertEqual(saved["检修通告名称"], "新检修")
        self.assertEqual(saved["设备名称"], "新设备")
        self.assertIsNone(saved["维修结束时间（2026）"])
        self.assertIsNone(saved["事件应急措施"])
        self.assertEqual(self.service._sync_repair_project_relations.call_args.kwargs["previous_target_record_ids"], ["recOldRepair"])
        checkpoint = self.store.get_repair_management_operation("rebind")["result"]["checkpoint"]
        self.assertEqual(checkpoint["before_fields"]["关联事件单"], "recOldEvent")

    def test_project_rebind_copies_new_identity_to_existing_followups(self):
        names = ["维修名称", "维修简述", "维修来源", "维修开始时间", "维修结束时间", "结束时间", "设备名称"]
        metas = [FieldMeta(str(i), name, "Text", 1, False, {}, [], False) for i, name in enumerate(names)]
        self.service._ensure_repair_followup_parent_id_field = Mock(return_value=(metas, {m.field_name: m for m in metas}))
        self.service._ensure_repair_followup_select_options = Mock(return_value=(metas, {m.field_name: m for m in metas}))
        result = self.service._sync_repair_followups_from_summary(summary_record_id="recProject",
            summary_record={"display_fields": {"检修通告名称": "新检修", "维修名称": "旧名称",
                "对应来源": "巡检发现", "维修开始时间": "2026-09-23 10:00", "设备名称": "汇总设备"}},
            linked_followups=[{"record_id": "recFollowup", "raw_fields": {"维修汇总记录ID": "recProject",
                "设备名称": "跟进填写设备", "随工人员（我方维修人员）": "测试操作人"}}])
        self.assertEqual(result["synced_count"], 1)
        fields = self.service._request_payload.call_args.kwargs["json_payload"]["fields"]
        self.assertEqual(fields["维修名称"], "新检修")
        self.assertEqual(fields["维修简述"], "2026/09/23 - 测试操作人")
        self.assertEqual(fields["维修来源"], "巡检发现")
        self.assertIsNone(fields["维修结束时间"])
        self.assertNotIn("设备名称", fields)

    def test_due_task_after_twenty_delayed_tasks_is_processed(self):
        for index in range(21):
            op = f"sync-{index}"
            self.store.begin_repair_management_operation(op, operation_type="followup_summary_sync", scope="A", payload_hash=op, summary_record_id="project")
            self.store.update_repair_management_operation(op, status="sync_pending", result={"available_at": time.time() + 600 if index < 20 else 0})
        self.service._execute_repair_sync_task = Mock(return_value=[])
        result = self.service._process_due_repair_sync_tasks_unlocked(limit=20)
        self.assertEqual(result["completed"], 1)
        self.assertEqual(self.store.get_repair_management_operation("sync-20")["status"], "completed")

    def test_operation_claim_rejects_stale_version(self):
        old = self.store.begin_repair_management_operation("cas", operation_type="followup_summary_sync", scope="A", payload_hash="h")
        self.store.update_repair_management_operation("cas", status="sync_pending")
        self.assertFalse(self.store.update_repair_management_operation("cas", status="completed", expected_updated_at=old["updated_at"]))

    def test_delete_collects_all_501_followups_and_checks_parent_last(self):
        children = {f"child{i}" for i in range(501)}
        def load(summary, **kwargs):
            self.assertIsNone(kwargs["limit"])
            self.assertTrue(kwargs["force_refresh"])
            return [], {}, [{"record_id": rid} for rid in sorted(children)]
        deleted = []
        def delete(**kwargs):
            rid = kwargs["record_id"]
            if rid == "recProject": self.assertFalse(children)
            else: children.discard(rid)
            deleted.append(rid)
        self.service._load_repair_followups_for_summary = load
        self.service._delete_record_fields = delete
        self.service._delete_repair_snapshot_item = Mock()
        self.service._remove_repair_source_projection = Mock(return_value=True)
        op = self.store.begin_repair_management_operation("delete", operation_type="project_delete", scope="A", payload_hash="h", summary_record_id="recProject")
        self.service._execute_repair_project_delete_task(op)
        self.assertEqual(len(deleted), 502)
        self.assertEqual(deleted[-1], "recProject")

    def test_fresh_projection_uses_accepted_fields_without_remote_query(self):
        payload = {"_accepted_target_fields": {"status": "ended"}, "_accepted_target_at": time.time()}
        with patch.object(server, "query_record_by_id", side_effect=AssertionError("extra GET")):
            ok, result = PortalRuntime._projection_record(payload, "record", "维保通告")
        self.assertTrue(ok)
        self.assertEqual(result["fields"], {"status": "ended"})

    def test_source_sync_failure_is_durable_secondary_work(self):
        payload = {"source_record_id": "recSource", "target_record_id": "recTarget", "work_type": "maintenance", "notice_type": "维保通告"}
        self.assertIn("后台", PortalRuntime._queue_source_end_sync(payload, "recTarget", "维保通告"))
        with patch.object(server, "query_record_by_id", return_value=(False, "offline")):
            PortalRuntime.process_source_end_sync()
        events = self.store.list_outbox_events("notice_source_finalize", status="pending")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["attempts"], 1)

    def test_superseded_projection_reads_latest_state(self):
        payload = {"_accepted_target_fields": {"status": "ended"}, "_accepted_target_at": time.time() - 5}
        with patch.object(self.store, "list_notice_remote_operations_for_target", return_value=[
            {"operation_id": "newer", "updated_at": time.time(), "status": "completed"}
        ]), patch.object(server, "query_record_by_id", return_value=(True, {"fields": {"status": "active"}})) as query:
            self.assertEqual(PortalRuntime._projection_record(payload, "record", "维保通告")[1]["fields"]["status"], "active")
            query.assert_called_once()

    def test_old_source_end_task_does_not_end_reopened_notice(self):
        PortalRuntime._queue_source_end_sync({"source_record_id": "recSource", "work_type": "maintenance"}, "recTarget", "维保通告")
        with patch.object(PortalRuntime, "service") as service, patch.object(server, "query_record_by_id", return_value=(True, {"fields": {"status": "active"}})):
            service._target_record_lifecycle.return_value = {"active": True, "finished": False}
            PortalRuntime.process_source_end_sync()
            service.sync_notice_source_ended_fields.assert_not_called()
        self.assertEqual(len(self.store.list_outbox_events("notice_source_finalize", status="done")), 1)

    def test_logging_formats_once_and_does_not_echo_stream_twice(self):
        from upload_event_module.logger import SafeConsoleHandler, StreamLogger
        console = io.StringIO()
        handler = SafeConsoleHandler(console)
        handler.setFormatter(logging.Formatter("%(levelname)s:%(message)s"))
        target = logging.Logger("test", logging.INFO)
        target.addHandler(handler)
        writer = StreamLogger(console, logging.ERROR)
        writer.logger = target
        writer.write("once\n")
        self.assertEqual(console.getvalue(), "once\n")
        items = queue.Queue()
        queued = QueueHandler(items)
        queued.setFormatter(logging.Formatter("%(message)s"))
        queued.emit(logging.LogRecord("test", logging.ERROR, __file__, 1, "message", (), None))
        handler.emit(items.get())
        self.assertEqual(console.getvalue(), "once\nERROR:message\n")

    def test_operation_api_checks_stored_scope_before_recovery(self):
        from clipflow_backend.main import FastAPIPortalController
        from fastapi.testclient import TestClient
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        self.store.begin_repair_management_operation("scope-check", operation_type="project_update", scope="B", payload_hash="h")
        with patch.object(PortalRuntime, "service", self.service), patch.object(self.service, "repair_operation_status", return_value={"status": "processing"}) as recover:
            client = TestClient(controller._build_app())
            with patch.object(controller, "_current_session", return_value=None):
                self.assertEqual(client.get("/api/repair-management/operations/scope-check").status_code, 401)
            with patch.object(controller, "_current_session", return_value={"user": {"open_id": "test"}}), patch.object(controller, "_authorized_scope_or_error", side_effect=PortalError("denied")) as authorize:
                response = client.post("/api/repair-management/operations/scope-check?scope=A")
                self.assertGreaterEqual(response.status_code, 400)
                self.assertEqual(authorize.call_args.args[1], "B")
                recover.assert_not_called()
            with patch.object(controller, "_current_session", return_value={"user": {"open_id": "test"}}), patch.object(controller, "_authorized_scope_or_error", return_value="B"):
                self.assertEqual(client.post("/api/repair-management/operations/scope-check").status_code, 200)
                recover.assert_called_once_with("scope-check", recover=True)

    def test_source_end_recovery_preserves_actual_end_time(self):
        config = server.get_field_config("维保通告")
        with patch.object(PortalRuntime, "service", self.service):
            payload = PortalRuntime._source_end_payload({}, {config["actual_end"]: "2026-09-20 12:34:56"}, "维保通告")
            self.assertEqual(payload["ended_at"], "2026-09-20 12:34:56")
            with self.assertRaisesRegex(RuntimeError, "缺少实际结束"):
                PortalRuntime._source_end_payload({}, {}, "维保通告")


if __name__ == "__main__":
    unittest.main()

"""Ledger integration checks. All cloud writes are test doubles."""
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal import repair_ledger as ledger
from lan_bitable_template_portal.portal_service import (
    MaintenancePortalService, PortalError, REPAIR_FOLLOWUP_TABLE_ID,
    REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME, REPAIR_MANAGEMENT_TABLE_ID,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore


class RepairLedgerIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = LanPortalStateStore(Path(self.tmp.name) / "state.sqlite3")
        self.addCleanup(self.store.shutdown_write_worker)
        s = self.service = object.__new__(MaintenancePortalService)
        s._state_store = self.store
        s._repair_management_record_locks_guard = threading.RLock()
        s._repair_management_record_locks = {}
        s._repair_secondary_sync_deferred = False
        s._repair_snapshots_enabled = True
        self.summary = {"record_id": "rec_parent", "raw_fields": {}, "display_fields": {}}
        s._ensure_repair_management_record_in_scope = Mock(return_value=self.summary)
        s._load_repair_followups_for_summary = Mock(return_value=([], {}, []))
        s._load_repair_management_project_records = Mock(return_value=([], {}, []))
        s._ensure_repair_followup_parent_id_field = Mock(return_value=([], {}))
        s._prepare_repair_followup_fields = Mock(side_effect=lambda **kw: ({**kw["fields"], REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: kw["summary_record_id"]}, []))
        s._ensure_repair_followup_select_options = Mock(side_effect=lambda fields, meta, **kw: ([], meta))
        s._upsert_repair_snapshot_fields = Mock()
        s._repair_snapshot_record_version = Mock(return_value="v1")
        s._sync_repair_management_from_followup = Mock(return_value=[])
        s._schedule_repair_sync_task = Mock(return_value="retry-ledger")
        s._request_payload = Mock(side_effect=AssertionError("unexpected cloud request"))
        s._request_json = Mock(side_effect=AssertionError("unexpected cloud request"))
        self.created = 0
        def create(fields, _metadata):
            self.created += 1
            ctx = s._repair_before_write(REPAIR_FOLLOWUP_TABLE_ID, fields, client_token="stable-token")
            record_id = f"rec_followup{self.created}"
            s._repair_after_write(ctx, record_id)
            return {"data": {"record": {"record_id": record_id}}}, dict(fields), []
        s._create_repair_followup_fields = Mock(side_effect=create)
        ledger.catalog(s).replace([
            {"record_id": "rec_devA", "机楼": "南通A楼", "设备编号": "A-TRB-01", "设备名称": "变压器", "scope_codes": ["A"]},
            {"record_id": "rec_devB", "机楼": "南通B楼", "scope_codes": ["B"]},
            {"record_id": "rec_blank", "机楼": "", "scope_codes": []},
        ])

    def create(self, ids, operation=""):
        return self.service.create_repair_followup_record(summary_record_id="rec_parent", scope="A",
            fields={"维修进展描述": "检查完毕"}, ledger_device_ids=ids, operation_id=operation)

    def test_followups_accumulate_and_retry_without_new_record(self):
        self.service._sync_repair_management_from_followup.side_effect = [OSError("offline"), [], []]
        first = self.create(["rec_devA"], "op1")
        self.assertTrue(first["summary_sync_pending"])
        self.create(["rec_devA"], "op1")
        self.create(["rec_blank", "rec_devA"], "op2")
        self.assertEqual(self.created, 2)
        fields = ledger.summary_fields(self.service, "rec_parent", {"关联台账记录ID": "rec_cloud"})
        self.assertEqual(fields, {"台账关联状态": "已关联", "关联台账记录ID": "rec_cloud,rec_devA,rec_blank"})
        rows = [{"record_id": "rec_followup1"}, {"record_id": "rec_followup2"}]
        ledger.add_followup_selections(self.service, "rec_parent", rows, "A")
        self.assertEqual(rows[0]["ledger_device_ids"], ["rec_devA"])
        self.assertEqual(rows[1]["ledger_devices"][0]["record_id"], "rec_blank")

    def test_scope_and_invalid_selection_rejected_before_write(self):
        for ids in (["rec_devB"], ["rec_unknown"], ["bad"], "rec_devA"):
            with self.assertRaises(PortalError):
                self.create(ids)
        self.assertEqual(self.created, 0)
        self.create(["rec_blank"])
        self.assertEqual(self.created, 1)

    def test_existing_followup_edit_keeps_cumulative_devices(self):
        self.create(["rec_devA"])
        self.service._load_table_records_by_ids = Mock(return_value=[{
            "record_id": "rec_followup1", "raw_fields": {REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_parent"},
        }])
        self.service._patch_record_fields = Mock()
        self.service.update_repair_followup_record("rec_followup1", summary_record_id="rec_parent", scope="A",
            fields={"维修进展描述": "调整"}, ledger_device_ids=["rec_blank"])
        saved = ledger.associations(self.service, "rec_parent")
        self.assertEqual(saved["record_ids"], ["rec_devA", "rec_blank"])
        self.assertEqual(saved["followups"]["rec_followup1"], ["rec_blank"])
        self.assertEqual(self.service._patch_record_fields.call_args.kwargs["record_id"], "rec_followup1")

    def test_remote_written_local_failure_can_recover_selection(self):
        with patch.object(ledger, "remember_selection", side_effect=OSError("disk busy")):
            with self.assertRaises(OSError):
                self.create(["rec_devA"], "recover-op")
        operation = self.store.get_repair_management_operation("recover-op")
        self.assertEqual(operation["status"], "remote_written")
        self.service._state_store = LanPortalStateStore(self.store.db_path)
        self.addCleanup(self.service._state_store.shutdown_write_worker)
        self.create(["rec_devA"], "recover-op")
        self.assertEqual(self.created, 1)
        self.assertEqual(ledger.associations(self.service, "rec_parent")["record_ids"], ["rec_devA"])

    def test_summary_sync_patches_new_fields_with_existing_ids(self):
        s = self.service
        ledger.remember_selection(s, "rec_parent", "rec_followup1", ["rec_devA"])
        self.summary["raw_fields"] = {"关联台账记录ID": "rec_cloud", "台账关联说明": "待核对"}
        s._require_repair_management_followup_link_meta = Mock()
        s._build_repair_management_prefill = Mock(return_value={"fields": {}})
        s._coerce_repair_management_fields = Mock(return_value=({}, []))
        s._patch_record_fields = Mock()
        s._sync_repair_management_workflow = Mock(return_value=(True, []))
        s._sync_repair_relation_business_fields = Mock(return_value={"warnings": []})
        s._invalidate_repair_management_status_cache = Mock()
        s._sync_repair_management_from_followup_unlocked(summary_record_id="rec_parent", scope="A")
        written = s._patch_record_fields.call_args.kwargs
        self.assertEqual(written["table_id"], REPAIR_MANAGEMENT_TABLE_ID)
        self.assertEqual(written["fields"]["关联台账记录ID"], "rec_cloud,rec_devA")
        self.assertEqual(written["fields"]["台账关联状态"], "已关联")
        self.assertNotIn("台账关联说明", written["fields"], "followup sync must preserve existing notes")

    def test_create_defaults_only_apply_to_new_project(self):
        s = self.service
        s._clean_repair_management_fields = Mock(return_value={"故障维修原因": "测试"})
        s._build_repair_management_prefill = Mock(return_value={"fields": {}})
        s._coerce_repair_management_fields = Mock(side_effect=lambda fields, *a, **kw: (dict(fields), []))
        s._repair_management_fields_in_scope = Mock(return_value=True)
        s._repair_management_business_identity = Mock(return_value="")
        s._create_record_fields = Mock(side_effect=RuntimeError("captured before cloud"))
        with self.assertRaisesRegex(RuntimeError, "captured before cloud"):
            s.create_repair_management_record({"台账关联状态": "已关联"}, scope="A")
        fields = s._create_record_fields.call_args.kwargs["fields"]
        self.assertEqual({key: fields[key] for key in ledger.LINK_FIELDS}, ledger.LINK_FIELDS)

    def test_cache_query_after_restart_does_not_fetch_cloud(self):
        self.service._equipment_ledger_catalog = ledger.LedgerCatalog(ledger.catalog(self.service)._path)
        result = ledger.candidates(self.service, scope="A", query="变压器", filters={"机楼": "南通A楼"})
        self.assertEqual(result["total"], 1)
        self.service._request_payload.assert_not_called()
        self.service._request_json.assert_not_called()

    def test_refresh_uses_full_source_and_failed_refresh_preserves_cache(self):
        s = self.service
        s._load_table_fields = Mock(return_value=([], {"设备编号": object(), "机楼": object()}))
        s._search_table_records = Mock(return_value=[{
            "record_id": "rec_cached", "display_fields": {"机楼": "南通D楼", "设备编号": "D-GPS-01", "设备名称": "柴油发电机"},
        }])
        with patch('upload_event_module.services.http_client.FeishuHttpClient') as client:
            ledger.start_refresh(s)
            s._equipment_ledger_worker.join(3)
            self.assertFalse(s._equipment_ledger_worker.is_alive())
            self.assertEqual(s._search_table_records.call_args.kwargs['limit'], None)
            self.assertEqual(s._search_table_records.call_args.kwargs['table_id'], ledger.TABLE_ID)
            self.assertEqual(ledger.candidates(s, scope="D")["total"], 1)
            self.assertEqual(ledger.candidates(s, scope="A")["total"], 0)
            s._search_table_records.side_effect = OSError("partial page failure")
            ledger.start_refresh(s)
            s._equipment_ledger_worker.join(3)
            self.assertEqual(ledger.candidates(s, scope="D")["total"], 1)
            self.assertIn("partial page failure", ledger.cache_status(s)["error"])
            self.assertEqual(client.return_value.close.call_count, 2)

    def test_restricted_read_does_not_return_other_building_ids(self):
        ledger.remember_selection(self.service, "rec_parent", "rec_followup1", ["rec_devA", "rec_devB"])
        rows = [{"record_id": "rec_followup1"}]
        ledger.add_followup_selections(self.service, "rec_parent", rows, "A")
        self.assertEqual(rows[0]["ledger_device_ids"], ["rec_devA"])
        self.assertEqual([d["record_id"] for d in rows[0]["ledger_devices"]], ["rec_devA"])
        with self.assertRaises(PortalError):
            ledger.validate_selection(self.service, "rec_parent", ["rec_devB"], "A")
        self.assertEqual(ledger.associations(self.service, "rec_parent")["record_ids"], ["rec_devA", "rec_devB"])


if __name__ == "__main__":
    unittest.main()

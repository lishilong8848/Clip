"""Repair list identity and automatic event-transfer replay use isolated data."""
import copy
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.portal_service import (
    MaintenancePortalService, PortalError, REPAIR_MANAGEMENT_TABLE_ID,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore


class RepairProjectIdentityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = LanPortalStateStore(Path(self.temp.name) / "state.sqlite3")
        self.addCleanup(self.store.shutdown_write_worker)
        self.service = MaintenancePortalService.__new__(MaintenancePortalService)
        self.service._state_store = self.store
        self.service._repair_management_record_locks_guard = threading.RLock()
        self.service._repair_management_record_locks = {}
        self.description = "BMS报A-445-TRB-202-3 光伏并网接入点通信中断"
        self.project = {
            "record_id": "rec_project_existing",
            "raw_fields": {"关联事件单-L": "rec_event_new"},
            "display_fields": {
                "故障维修原因": "电量仪通讯故障",
                "故障发生现象描述": self.description,
                "故障发生时间": "2026-10-01 00:21:00",
                "所属数据中心/楼栋-使用": "南通A楼",
                "维修跟进记录-L": "rec_followup",
                "当前维修进度": "58%",
            },
        }
        self.event = {
            "record_id": "rec_event_new",
            "display_fields": {
                "告警描述": self.description,
                "事件发生时间": "2026-10-01 00:21:00",
                "机楼": "A楼", "是否转检修": True,
                "最终状态": "事件闭环转检修中",
            },
            "raw_fields": {"是否转检修": True},
        }
        self.service._load_repair_management_project_records = Mock(return_value=([], {}, [self.project]))
        self.service._load_repair_management_event_records = Mock(return_value=([], {}, [self.event]))
        self.service.create_repair_management_record = Mock(return_value={"record_id": "rec_new_project"})

    def ensure(self, event=None):
        event = event or self.event
        return self.service.ensure_repair_management_record_for_event_notice(
            event_record_id=event["record_id"], notice_data={},
            remote_fields=event["display_fields"], scope="A", source_month="2026-10",
        )

    def test_changed_cause_does_not_create_empty_project_for_linked_event_id(self):
        before = copy.deepcopy(self.project)
        result = self.ensure()
        self.assertEqual(result["record_id"], self.project["record_id"])
        self.assertFalse(result["created"])
        self.service.create_repair_management_record.assert_not_called()
        self.assertEqual(self.project, before, "replay must not reset progress, cause or links")

    def test_next_day_backfill_does_not_requeue_linked_event_after_cause_edit(self):
        self.assertEqual(self.service.list_unlinked_transferred_events_for_repair(), [])

    def test_different_event_ids_with_identical_title_time_building_are_independent(self):
        for event_id in ("rec_independent_one", "rec_independent_two"):
            event = copy.deepcopy(self.event)
            event["record_id"] = event_id
            self.service.create_repair_management_record.reset_mock()
            self.assertTrue(self.ensure(event)["created"])
            self.service.create_repair_management_record.assert_called_once()

    def test_completed_auto_operation_replays_before_payload_comparison(self):
        operation_id = f"event-end-transfer:{REPAIR_MANAGEMENT_TABLE_ID}:{self.event['record_id']}"
        self.store.begin_repair_management_operation(operation_id, operation_type="project_create", scope="A",
            summary_record_id=self.project["record_id"], payload_hash="old-event-payload")
        self.store.update_repair_management_operation(operation_id, status="completed", record_id=self.project["record_id"],
            result={"record_id": self.project["record_id"]})
        self.project["display_fields"]["故障发生现象描述"] = "人工修改后的描述"
        result = self.ensure()
        self.assertEqual(result["record_id"], self.project["record_id"])
        self.assertTrue(result["idempotent_replay"])
        self.service.create_repair_management_record.assert_not_called()

    def test_deleted_completed_auto_project_is_not_recreated(self):
        operation_id = f"event-end-transfer:{REPAIR_MANAGEMENT_TABLE_ID}:{self.event['record_id']}"
        self.store.begin_repair_management_operation(operation_id, operation_type="project_create", scope="A", payload_hash="old")
        self.store.update_repair_management_operation(operation_id, status="completed", record_id="rec_deleted_project")
        self.service._load_repair_management_project_records.return_value = ([], {}, [])
        result = self.ensure()
        self.assertFalse(result["created"])
        self.assertEqual(result["record_id"], "rec_deleted_project")
        self.service.create_repair_management_record.assert_not_called()

    def test_event_id_reuse_is_durable_after_project_deletion_and_restart(self):
        first = self.ensure()
        self.service._load_repair_management_project_records.return_value = ([], {}, [])
        # A new store instance simulates a new process, with no in-memory result.
        self.service._state_store = LanPortalStateStore(self.store.db_path)
        self.addCleanup(self.service._state_store.shutdown_write_worker)
        second = self.ensure()
        self.assertEqual(second["record_id"], first["record_id"])
        self.assertFalse(second["created"])
        self.service.create_repair_management_record.assert_not_called()

    def test_interrupted_auto_operation_is_completed_by_successful_reuse(self):
        operation_id = f"event-end-transfer:{REPAIR_MANAGEMENT_TABLE_ID}:{self.event['record_id']}"
        for status in ("started", "processing", "failed", "uncertain"):
            with self.subTest(status=status):
                self.store.begin_repair_management_operation(operation_id, operation_type="project_create", scope="A", payload_hash="interrupted")
                self.store.update_repair_management_operation(operation_id, status=status, record_id="", result={})
                self.service._load_repair_management_project_records.return_value = ([], {}, [self.project])
                self.ensure()
                saved = self.store.get_repair_management_operation(operation_id)
                self.assertEqual(saved["status"], "completed")
                self.assertEqual(saved["record_id"], self.project["record_id"])
                self.service._load_repair_management_project_records.return_value = ([], {}, [])
                self.assertFalse(self.ensure()["created"])
        self.service.create_repair_management_record.assert_not_called()

    def test_reuse_does_not_overwrite_active_writer_or_other_known_target(self):
        operation_id = f"event-end-transfer:{REPAIR_MANAGEMENT_TABLE_ID}:{self.event['record_id']}"
        self.store.begin_repair_management_operation(operation_id, operation_type="project_create", scope="A", payload_hash="active")
        with patch.object(self.service, "_repair_writer_alive", return_value=True):
            self.ensure()
        self.assertEqual(self.store.get_repair_management_operation(operation_id)["status"], "started")
        self.store.update_repair_management_operation(operation_id, status="remote_written", record_id="rec_other_project")
        with self.assertRaisesRegex(PortalError, "已有不同的维修单"):
            self.ensure()
        self.assertEqual(self.store.get_repair_management_operation(operation_id)["record_id"], "rec_other_project")
        self.service.create_repair_management_record.assert_not_called()

    def test_exact_event_link_reuse_is_also_durable(self):
        self.project["raw_fields"]["关联事件单-L"] = self.event["record_id"]
        self.ensure()
        self.service._load_repair_management_project_records.return_value = ([], {}, [])
        self.assertEqual(self.ensure()["record_id"], self.project["record_id"])
        self.service.create_repair_management_record.assert_not_called()

    def test_identical_fault_for_other_event_does_not_block_backfill(self):
        self.project["raw_fields"]["关联事件单-L"] = "rec_event_other"
        candidates = self.service.list_unlinked_transferred_events_for_repair()
        self.assertEqual([row["event_record_id"] for row in candidates], [self.event["record_id"]])
        self.assertTrue(self.ensure()["created"])
        self.service.create_repair_management_record.assert_called_once()

    def test_non_transferred_event_is_skipped_without_create_or_warning(self):
        event = copy.deepcopy(self.event)
        event["display_fields"]["是否转检修"] = False
        event["raw_fields"]["是否转检修"] = False
        result = self.ensure(event)
        self.assertTrue(result["skipped"])
        self.assertFalse(result["created"])
        self.service.create_repair_management_record.assert_not_called()
        self.service._load_repair_management_project_records.assert_not_called()
        self.service._load_repair_management_event_records.return_value = ([], {}, [event])
        self.assertEqual(self.service.list_unlinked_transferred_events_for_repair(), [])

    def test_transferred_unlinked_event_does_not_require_specific_status_or_fault_fields(self):
        self.service._load_repair_management_project_records.return_value = ([], {}, [])
        self.service._load_repair_management_event_records.return_value = ([], {}, [{
            "record_id": "rec_minimal_transfer", "raw_fields": {"是否转检修": True}, "display_fields": {},
        }])
        self.assertEqual([row["event_record_id"] for row in self.service.list_unlinked_transferred_events_for_repair()], ["rec_minimal_transfer"])

    def test_current_event_id_field_is_authoritative_over_legacy_relation(self):
        self.project["raw_fields"]["关联事件单"] = "rec_legacy_event"
        self.project["display_fields"]["关联事件单"] = "rec_legacy_event"
        self.assertEqual(self.service._repair_management_event_ids_from_record(self.project), [self.event["record_id"]])
        self.project["raw_fields"]["关联事件单-L"] = ""
        self.assertEqual(self.service._repair_management_event_ids_from_record(self.project), [])

    def test_concurrent_same_event_id_is_created_once(self):
        self.service._load_repair_management_project_records.return_value = ([], {}, [])
        operation_id = f"event-end-transfer:{REPAIR_MANAGEMENT_TABLE_ID}:{self.event['record_id']}"
        def create(**unused):
            time.sleep(.02)
            self.store.begin_repair_management_operation(operation_id, operation_type="project_create", scope="A", payload_hash="synthetic")
            self.store.update_repair_management_operation(operation_id, status="completed", record_id="rec_new_project")
            return {"record_id": "rec_new_project"}
        self.service.create_repair_management_record.side_effect = lambda *args, **kwargs: create()
        barrier = threading.Barrier(2)
        results, errors = [], []
        def submit():
            try:
                barrier.wait(1)
                results.append(self.ensure())
            except Exception as exc:
                errors.append(exc)
        workers = [threading.Thread(target=submit) for _ in range(2)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(2)
        self.assertFalse(any(worker.is_alive() for worker in workers))
        self.assertEqual(errors, [])
        self.assertEqual(sorted(row["created"] for row in results), [False, True])
        self.service.create_repair_management_record.assert_called_once()

    def test_cleanup_preserves_successful_auto_replay_but_removes_regular_operations(self):
        self.ensure()
        self.store.begin_repair_management_operation("manual-old", operation_type="project_update", scope="A", payload_hash="x")
        self.store.update_repair_management_operation("manual-old", status="completed", record_id="rec_manual")
        deleted = self.store.cleanup_repair_management_operations(completed_before=time.time() + 1, failed_before=0)
        self.assertEqual(deleted["completed"], 1)
        self.assertIsNone(self.store.get_repair_management_operation("manual-old"))
        self.service._load_repair_management_project_records.return_value = ([], {}, [])
        self.assertEqual(self.ensure()["record_id"], self.project["record_id"])
        self.service.create_repair_management_record.assert_not_called()

    def test_list_keeps_distinct_record_ids_even_with_identical_fault_and_event(self):
        second = {**copy.deepcopy(self.project), "record_id": "rec_second_project"}
        first = copy.deepcopy(self.project)
        second["display_fields"]["维修跟进记录-L"] = ""
        second["display_fields"]["当前维修进度"] = "0%"
        records, duplicates = self.service._canonical_repair_management_projects([first, second])
        self.assertEqual([row["record_id"] for row in records], [first["record_id"], second["record_id"]])
        self.assertEqual(duplicates, [])

    def test_same_record_id_only_is_collapsed_local_placeholder_not_exposed(self):
        repeated = copy.deepcopy(self.project)
        placeholder = {**copy.deepcopy(self.project), "record_id": "localid-repair-test"}
        records, _ = self.service._canonical_repair_management_projects([self.project, repeated, placeholder])
        self.assertEqual([row["record_id"] for row in records], [self.project["record_id"]])


if __name__ == "__main__":
    unittest.main()

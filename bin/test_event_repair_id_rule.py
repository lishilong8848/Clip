"""Bounded regression tests for the event-repair ID rule.

Rule under test (changed business rule):
  * Automatic backfill for a closed event creates a repair management record
    ONLY when the event transfer flag is true AND no repair is already linked
    to THAT event ID.
  * Description/time/building similarity with another event's repair must NOT
    be used to reuse that other event's repair record during backfill.
  * Ordinary manual creation business dedupe (same fault/time/building reuse)
    is unchanged.

These tests are self-contained integration checks:
  * temporary sqlite state store (LanPortalStateStore in TemporaryDirectory)
  * native ensure_repair_management_record_for_event_notice /
    create_repair_management_record / _create_repair_management_record_unlocked
    paths are exercised (no patch of create_repair_management_record)
  * no real Feishu calls -- _create_record_fields is faked, project loading is
    stubbed, and downstream snapshot/workflow/relation writes are patched.
  * outbox queue test uses a real temporary store and a fake service that
    returns "skipped" so the task is marked done with no retries/no remote
    writes and the next processing call is idle.
"""
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.portal_service import (  # noqa: E402
    FieldMeta,
    MaintenancePortalService,
)
from lan_bitable_template_portal.server import PortalRuntime  # noqa: E402
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402


class _TestMaintenancePortalService(MaintenancePortalService):
    """Native service with only the load/refresh no-ops required by fixtures."""

    def refresh(self) -> None:
        return

    def ensure_loaded(self) -> None:
        return


class EventRepairIdRuleTests(unittest.TestCase):
    def _new_temp_service(self, root: Path):
        def fake_data_path(name):
            return str(root / name)

        portal_patcher = patch(
            "lan_bitable_template_portal.portal_service.get_data_file_path",
            side_effect=fake_data_path,
        )
        store_patcher = patch(
            "lan_bitable_template_portal.state_store.get_data_file_path",
            side_effect=fake_data_path,
        )
        portal_patcher.start()
        store_patcher.start()
        self.addCleanup(portal_patcher.stop)
        self.addCleanup(store_patcher.stop)
        return _TestMaintenancePortalService()

    def _repair_metas(self):
        return [
            FieldMeta(
                "fld_reason", "故障维修原因", "Text", 1, False, {}, [], False
            ),
            FieldMeta(
                "fld_time", "故障发生时间", "DateTime", 5, False, {}, [], False
            ),
            FieldMeta(
                "fld_building", "所属数据中心/楼栋-使用", "Text", 1, False, {}, [],
                False,
            ),
            FieldMeta(
                "fld_event", "关联事件单", "Text", 1, False, {}, [], False
            ),
        ]

    def test_auto_backfill_different_event_creates_despite_same_semantics(self):
        """Backfill for a new event creates a second repair with identical
        fault/time/building because the existing repair belongs to a different
        event ID (semantic reuse is bypassed for auto backfill)."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        service = self._new_temp_service(Path(temporary.name))

        metas = self._repair_metas()
        meta_by_name = {item.field_name: item for item in metas}
        existing = {
            "record_id": "rec_existing_project",
            "display_fields": {
                "故障维修原因": "巡检发现制冷单元压差异常",
                "故障发生时间": "2026-07-17 09:30",
                "所属数据中心/楼栋-使用": "南通C楼",
            },
            "raw_fields": {"关联事件单-L": "rec_event_archived"},
        }

        service._load_repair_management_project_records = (  # type: ignore[method-assign]
            lambda: (metas, meta_by_name, [existing])
        )
        service._repair_management_event_from_notice_payload = (  # type: ignore[method-assign]
            lambda **_kwargs: {
                "record_id": "rec_event_current",
                "alarm_desc": "巡检发现制冷单元压差异常",
                "occurrence_time": "2026-07-17 09:30",
                "building_codes": ["C"],
                "display_fields": {},
            }
        )
        service._build_repair_management_prefill = (  # type: ignore[method-assign]
            lambda **_kwargs: {
                "fields": {
                    "故障维修原因": "巡检发现制冷单元压差异常",
                    "故障发生时间": "2026-07-17 09:30",
                    "所属数据中心/楼栋-使用": "南通C楼",
                },
                "warnings": [],
            }
        )
        service._missing_repair_management_required_fields = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: []
        )

        captured = {}

        def fake_create_record_fields(**kwargs):
            captured.update(kwargs)
            return {
                "data": {"record": {"record_id": "rec_current_event_project"}}
            }

        service._create_record_fields = (  # type: ignore[method-assign]
            fake_create_record_fields
        )

        with (
            patch.object(service, "_upsert_repair_snapshot_fields"),
            patch.object(service, "_save_source_scope_snapshots"),
            patch.object(service, "_touch_state_cache_version"),
            patch.object(
                service,
                "_sync_repair_management_workflow",
                return_value=(True, []),
            ),
            patch.object(
                service,
                "_sync_repair_project_relations",
                return_value={"target_results": [], "projection": {}, "warnings": []},
            ),
            patch.object(service, "_invalidate_repair_management_status_cache"),
        ):
            result = service.ensure_repair_management_record_for_event_notice(
                event_record_id="rec_event_current",
                notice_data={},
                remote_fields={"是否转检修": True},
                scope="C",
                source_month="2026-07",
            )

        try:
            service._state_store.shutdown_write_worker(timeout=2.0)
        except Exception:
            pass

        # Native remote create must be reached (semantic dedupe bypassed).
        self.assertTrue(result["created"])
        self.assertEqual(result["record_id"], "rec_current_event_project")
        self.assertNotEqual(result["record_id"], "rec_existing_project")

        # The created record is explicitly linked to the current event.
        self.assertEqual(captured["fields"].get("关联事件单"), "rec_event_current")

        # Prove the semantic identity really matched: had this been a manual
        # create (sync_event_transfer_status=True) it would have reused the
        # existing build, but the auto backfill creates a distinct one.
        self.assertEqual(
            service._repair_management_business_identity(existing),
            service._repair_management_business_identity(
                {"display_fields": result["fields"], "raw_fields": {}}
            ),
        )
        self.assertTrue(result["fields"].get("所属数据中心/楼栋-使用"))

    def test_manual_create_still_reuses_same_business_identity(self):
        """Ordinary manual create with sync_event_transfer_status=True keeps
        existing 故障/时间/楼栋 business dedupe even with a different event ID."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        service = self._new_temp_service(Path(temporary.name))

        metas = self._repair_metas()
        meta_by_name = {item.field_name: item for item in metas}
        existing = {
            "record_id": "rec_existing_project",
            "display_fields": {
                "故障维修原因": "巡检发现制冷单元压差异常",
                "故障发生时间": "2026-07-17 09:30",
                "所属数据中心/楼栋-使用": "南通C楼",
            },
            "raw_fields": {"关联事件单-L": "rec_event_archived"},
        }

        service._load_repair_management_project_records = (  # type: ignore[method-assign]
            lambda: (metas, meta_by_name, [existing])
        )
        service._build_repair_management_prefill = (  # type: ignore[method-assign]
            lambda **_kwargs: {"fields": {}, "warnings": []}
        )
        service._missing_repair_management_required_fields = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: []
        )

        with (
            patch.object(service, "_create_record_fields") as create_remote,
            patch.object(service, "mark_event_transferred_to_repair") as mark,
        ):
            result = service.create_repair_management_record(
                {
                    "故障维修原因": "巡检发现制冷单元压差异常",
                    "故障发生时间": "2026-07-17 09:30",
                    "所属数据中心/楼栋-使用": "南通C楼",
                },
                scope="C",
                source_event_id="rec_event_current",
            )

        # Manual business dedupe is unchanged: identical semantic identity is
        # reused regardless of the different event ID.
        self.assertEqual(result["record_id"], "rec_existing_project")
        self.assertTrue(result["duplicate_prevented"])
        self.assertTrue(result["idempotent_replay"])
        create_remote.assert_not_called()
        mark.assert_called_once_with(
            record_id="rec_event_current",
            month="",
            refresh_snapshot=False,
        )

    def test_queue_skipped_event_marks_done_and_next_is_idle(self):
        """A queued event that the service skips (transfer flag false) is marked
        done with no retries/remote writes, and the next process call is idle."""
        previous_store = PortalRuntime.state_store
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            PortalRuntime.state_store = store
            try:
                event_id = store.enqueue_outbox_event(
                    PortalRuntime.event_repair_queue_channel,
                    {
                        "event_record_id": "rec-event-not-transferred",
                        "notice_data": {},
                        "remote_fields": {"是否转检修": False},
                        "scope": "C",
                        "source_month": "2026-07",
                    },
                )
                with patch.object(
                    PortalRuntime.service,
                    "ensure_repair_management_record_for_event_notice",
                    return_value={
                        "record_id": "",
                        "skipped": True,
                        "reason": "事件未转检修，不自动补建。",
                    },
                ) as ensure_project:
                    first = PortalRuntime._process_event_repair_queue_once()
                    second = PortalRuntime._process_event_repair_queue_once()
                done = store.list_outbox_events(
                    PortalRuntime.event_repair_queue_channel, status="done"
                )
                pending = store.list_outbox_events(
                    PortalRuntime.event_repair_queue_channel, status="pending"
                )
                failed = store.list_outbox_events(
                    PortalRuntime.event_repair_queue_channel, status="failed"
                )
            finally:
                store.shutdown_write_worker(timeout=2.0)
                PortalRuntime.state_store = previous_store

        self.assertEqual(first["status"], "skipped")
        self.assertTrue(first["processed"])
        self.assertEqual(first["event_id"], event_id)
        self.assertEqual(first["event_record_id"], "rec-event-not-transferred")
        self.assertEqual(second["status"], "idle")
        self.assertFalse(second["processed"])
        self.assertEqual([item["id"] for item in done], [event_id])
        self.assertEqual(pending, [])
        self.assertEqual(failed, [])
        ensure_project.assert_called_once_with(
            event_record_id="rec-event-not-transferred",
            notice_data={},
            remote_fields={"是否转检修": False},
            scope="C",
            source_month="2026-07",
        )


if __name__ == "__main__":
    unittest.main()
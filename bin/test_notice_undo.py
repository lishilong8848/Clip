import gc
import sys
import tempfile
import unittest
from pathlib import Path

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.portal_service import (  # noqa: E402
    MaintenancePortalService,
    NOTICE_TYPE_MAINTENANCE,
    STATE_NS_DAILY_SUMMARY,
    STATE_NS_WORK_STATUS,
    WORK_TYPE_MAINTENANCE,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402


class NoticeUndoTests(unittest.TestCase):
    def _service(self, tmpdir: str) -> MaintenancePortalService:
        service = MaintenancePortalService()
        service._state_store = LanPortalStateStore(Path(tmpdir) / "state.sqlite3")
        service._summary_dir = Path(tmpdir) / "summary"
        service._summary_dir.mkdir(parents=True, exist_ok=True)
        service._work_status_dir = Path(tmpdir) / "work_status"
        service._work_status_dir.mkdir(parents=True, exist_ok=True)
        service._hidden_ongoing_path = Path(tmpdir) / "hidden.json"
        return service

    def test_checkpoint_supersedes_prior_available(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LanPortalStateStore(Path(tmpdir) / "state.sqlite3")
            try:
                first = store.create_notice_undo_action(
                    {
                        "identity_key": "maintenance:active:aid-1",
                        "action_type": "update",
                        "scope": "A",
                        "work_type": "maintenance",
                        "notice_type": "维保通告",
                        "active_item_id": "aid-1",
                        "payload": {"n": 1},
                    }
                )
                second = store.create_notice_undo_action(
                    {
                        "identity_key": "maintenance:active:aid-1",
                        "action_type": "end",
                        "scope": "A",
                        "work_type": "maintenance",
                        "notice_type": "维保通告",
                        "active_item_id": "aid-1",
                        "payload": {"n": 2},
                    }
                )

                self.assertTrue(first)
                self.assertTrue(second)
                available = store.list_notice_undo_actions(scope="A")
                self.assertEqual([item["undo_id"] for item in available], [second])
                self.assertEqual(store.get_notice_undo_action(first)["status"], "superseded")
            finally:
                store.shutdown_write_worker(timeout=1.0)

    def test_restore_local_snapshot_returns_ended_item_to_ongoing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._service(tmpdir)
            try:
                item = {
                    "key": "maintenance:source:src-1",
                    "active_item_id": "aid-1",
                    "source_record_id": "src-1",
                    "target_record_id": "rec-1",
                    "work_type": WORK_TYPE_MAINTENANCE,
                    "notice_type": NOTICE_TYPE_MAINTENANCE,
                    "title": "A楼测试维保",
                    "building": "A楼",
                    "building_codes": ["A"],
                    "status": "进行中",
                    "started_at": "2026-05-26 09:30",
                }
                service._state_store.upsert_qt_active_item(
                    {**item, "record_id": "rec-1"},
                    section="other",
                    origin="portal",
                )
                service._state_store.put_document(
                    STATE_NS_DAILY_SUMMARY,
                    "2026-05-26",
                    {"date": "2026-05-26", "items": [dict(item)]},
                )
                service._state_store.put_document(
                    STATE_NS_WORK_STATUS,
                    "A",
                    {"version": 1, "items": [dict(item)]},
                )
                undo_id = service.create_notice_undo_checkpoint(
                    "end",
                    item,
                    remote_fields={"名称": "A楼测试维保", "维保状态": "开始"},
                    scope="A",
                )
                undo = service._state_store.get_notice_undo_action(undo_id)

                ended_item = {**item, "status": "已结束", "ended_at": "2026-05-26 18:30"}
                service._state_store.delete_qt_active_item(active_item_id="aid-1")
                service._state_store.put_document(
                    STATE_NS_DAILY_SUMMARY,
                    "2026-05-26",
                    {"date": "2026-05-26", "items": [ended_item]},
                )
                service._state_store.put_document(
                    STATE_NS_WORK_STATUS,
                    "A",
                    {"version": 1, "items": [ended_item]},
                )

                result = service.restore_notice_undo_local(undo, target_record_id="rec-1")
                self.assertTrue(result["restored_active"])
                active = service._state_store.list_qt_active_items()
                self.assertEqual(len(active), 1)
                self.assertEqual(active[0]["payload"]["status"], "进行中")
                summary = service._state_store.get_document(STATE_NS_DAILY_SUMMARY, "2026-05-26")
                self.assertEqual(summary["items"][0]["status"], "进行中")
            finally:
                service._state_store.shutdown_write_worker(timeout=1.0)
                del service
                gc.collect()

    def test_checkpoint_enriches_scope_and_reason_from_qt_active_item(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._service(tmpdir)
            service._state_store.upsert_qt_active_item(
                {
                    "active_item_id": "aid-reason",
                    "record_id": "rec-reason",
                    "work_type": WORK_TYPE_MAINTENANCE,
                    "notice_type": NOTICE_TYPE_MAINTENANCE,
                    "title": "EA118机房C楼冷却塔清洗",
                    "reason": "5#冷却塔脏堵",
                    "building": "C楼",
                    "building_codes": ["C"],
                    "text": (
                        "【维保通告】状态：开始\n\n"
                        "【名称】EA118机房C楼冷却塔清洗\n\n"
                        "【原因】5#冷却塔脏堵"
                    ),
                },
                section="other",
                origin="portal",
            )

            undo_id = service.create_notice_undo_checkpoint(
                "delete",
                {
                    "active_item_id": "aid-reason",
                    "target_record_id": "rec-reason",
                    "work_type": WORK_TYPE_MAINTENANCE,
                    "notice_type": NOTICE_TYPE_MAINTENANCE,
                },
                remote_fields={"名称": "EA118机房C楼冷却塔清洗"},
                scope="C",
            )
            undo = service._state_store.get_notice_undo_action(undo_id)
            available = service.list_available_notice_undos(scope="C")

            self.assertEqual(undo["building_codes"], ["C"])
            self.assertEqual(undo["reason"], "5#冷却塔脏堵")
            self.assertEqual(len(available), 1)
            self.assertTrue(service.create_notice_undo_job(undo_id, scope="C"))


if __name__ == "__main__":
    unittest.main()

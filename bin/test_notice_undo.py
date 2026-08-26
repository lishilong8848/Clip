import gc
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.portal_service import (  # noqa: E402
    MaintenancePortalService,
    NOTICE_TYPE_MAINTENANCE,
    PortalError,
    STATE_NS_DAILY_SUMMARY,
    STATE_NS_WORK_STATUS,
    WORK_TYPE_MAINTENANCE,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402
from lan_bitable_template_portal.server import PortalRuntime  # noqa: E402
from clipflow_backend.main import FastAPIPortalController  # noqa: E402


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

    def test_history_delete_list_filters_in_sql_and_reuses_identity_snapshots(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._service(tmpdir)
            store = service._state_store
            now = time.time()
            try:
                recent_delete_ids = []
                for index in range(3):
                    recent_delete_ids.append(
                        store.create_notice_undo_action(
                            {
                                "identity_key": f"maintenance:active:recent-{index}",
                                "action_type": "delete",
                                "scope": "A",
                                "work_type": "maintenance",
                                "notice_type": "维保通告",
                                "active_item_id": f"recent-{index}",
                                "target_record_id": f"rec-recent-{index}",
                                "title": f"近期删除 {index}",
                                "building": "A楼",
                                "building_codes": ["A"],
                                "created_at": now - index,
                                "expires_at": now + 86400,
                            }
                        )
                    )
                store.create_notice_undo_action(
                    {
                        "identity_key": "maintenance:active:recent-update",
                        "action_type": "update",
                        "scope": "A",
                        "work_type": "maintenance",
                        "notice_type": "维保通告",
                        "active_item_id": "recent-update",
                        "target_record_id": "rec-recent-update",
                        "title": "近期更新",
                        "building_codes": ["A"],
                        "created_at": now,
                        "expires_at": now + 86400,
                    }
                )

                with patch.object(
                    store,
                    "list_qt_active_items",
                    wraps=store.list_qt_active_items,
                ) as active_rows, patch.object(
                    store,
                    "list_notice_identities",
                    wraps=store.list_notice_identities,
                ) as identities:
                    items = service.list_available_notice_undos(
                        scope="ALL",
                        action_type="delete",
                        since_seconds=3 * 24 * 60 * 60,
                    )

                self.assertEqual(
                    {item["undo_id"] for item in items},
                    set(recent_delete_ids),
                )
                self.assertEqual(active_rows.call_count, 1)
                self.assertEqual(identities.call_count, 1)
            finally:
                store.shutdown_write_worker(timeout=1.0)

    def test_backend_runtime_reuses_service_state_store(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            shared_store = LanPortalStateStore(Path(tmpdir) / "state.sqlite3")
            fake_service = MagicMock()
            fake_service._state_store = shared_store
            controller = object.__new__(FastAPIPortalController)
            controller.app_token = "app"
            controller.table_id = "table"
            controller.notice_callback = None
            controller.ongoing_callback = None
            controller.ongoing_delete_callback = None
            controller.maintenance_action_callback = None
            previous = (
                PortalRuntime.service,
                PortalRuntime.state_store,
                PortalRuntime.auth_manager,
            )
            try:
                with patch(
                    "clipflow_backend.main.MaintenancePortalService",
                    return_value=fake_service,
                ), patch.object(PortalRuntime, "apply_runtime_settings"):
                    controller._initialize_portal_handler_state()
                self.assertIs(PortalRuntime.state_store, shared_store)
                self.assertIs(PortalRuntime.service._state_store, shared_store)
                self.assertIs(controller._state_store, shared_store)
            finally:
                (
                    PortalRuntime.service,
                    PortalRuntime.state_store,
                    PortalRuntime.auth_manager,
                ) = previous
                shared_store.shutdown_write_worker(timeout=1.0)

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
                self.assertEqual(active[0]["payload"]["status"], "更新")
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

    def test_multibuilding_undo_is_visible_to_each_related_building(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._service(tmpdir)
            try:
                undo_id = service.create_notice_undo_checkpoint(
                    "update",
                    {
                        "scope": "A",
                        "work_type": WORK_TYPE_MAINTENANCE,
                        "notice_type": NOTICE_TYPE_MAINTENANCE,
                        "active_item_id": "aid-multi",
                        "target_record_id": "rec-multi",
                        "title": "A、B楼联合维保",
                        "building": "A楼、B楼",
                        "building_codes": ["A", "B"],
                    },
                    remote_fields={"名称": "A、B楼联合维保"},
                    scope="A",
                )
                self.assertEqual(
                    [item["undo_id"] for item in service.list_available_notice_undos(scope="B")],
                    [undo_id],
                )
            finally:
                service._state_store.shutdown_write_worker(timeout=1.0)

    def test_expired_undo_is_rejected_before_job_creation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._service(tmpdir)
            try:
                undo_id = service._state_store.create_notice_undo_action(
                    {
                        "identity_key": "maintenance:active:expired",
                        "action_type": "update",
                        "scope": "A",
                        "work_type": WORK_TYPE_MAINTENANCE,
                        "notice_type": NOTICE_TYPE_MAINTENANCE,
                        "active_item_id": "expired",
                        "created_at": time.time() - 100,
                        "expires_at": time.time() - 1,
                    }
                )
                with self.assertRaisesRegex(Exception, "已过期"):
                    service.create_notice_undo_job(undo_id, scope="A")
                self.assertEqual(
                    service._state_store.get_notice_undo_action(undo_id)["status"],
                    "expired",
                )
            finally:
                service._state_store.shutdown_write_worker(timeout=1.0)

    def test_delete_undo_does_not_recreate_on_transient_query_failure(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._service(tmpdir)
            previous = (PortalRuntime.service, PortalRuntime.state_store)
            PortalRuntime.service = service
            PortalRuntime.state_store = service._state_store
            try:
                undo_id = service._state_store.create_notice_undo_action(
                    {
                        "identity_key": "maintenance:target:rec-transient",
                        "action_type": "delete",
                        "scope": "A",
                        "work_type": WORK_TYPE_MAINTENANCE,
                        "notice_type": NOTICE_TYPE_MAINTENANCE,
                        "target_record_id": "rec-transient",
                        "remote": {
                            "missing": False,
                            "fields": {"名称": "A楼瞬时失败测试"},
                        },
                    }
                )
                with patch(
                    "lan_bitable_template_portal.server.external_real_write_guard",
                    return_value={
                        "mock_external": False,
                        "real_write_allowed": True,
                        "reason": "",
                    },
                ), patch(
                    "lan_bitable_template_portal.server.query_record_by_id",
                    return_value=(False, "Data not ready"),
                ), patch(
                    "lan_bitable_template_portal.server.create_bitable_record_fields"
                ) as create_record:
                    with self.assertRaisesRegex(Exception, "Data not ready"):
                        PortalRuntime.execute_notice_undo(undo_id)
                create_record.assert_not_called()
            finally:
                PortalRuntime.service, PortalRuntime.state_store = previous
                service._state_store.shutdown_write_worker(timeout=1.0)

    def test_delete_undo_retry_reuses_recreated_target(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._service(tmpdir)
            previous = (PortalRuntime.service, PortalRuntime.state_store)
            PortalRuntime.service = service
            PortalRuntime.state_store = service._state_store
            try:
                undo_id = service._state_store.create_notice_undo_action(
                    {
                        "identity_key": "maintenance:target:rec-deleted",
                        "action_type": "delete",
                        "scope": "A",
                        "work_type": WORK_TYPE_MAINTENANCE,
                        "notice_type": NOTICE_TYPE_MAINTENANCE,
                        "target_record_id": "rec-deleted",
                        "remote": {
                            "missing": False,
                            "fields": {"名称": "A楼重建幂等测试"},
                        },
                    }
                )
                with patch(
                    "lan_bitable_template_portal.server.external_real_write_guard",
                    return_value={
                        "mock_external": False,
                        "real_write_allowed": True,
                        "reason": "",
                    },
                ), patch(
                    "lan_bitable_template_portal.server.query_record_by_id",
                    side_effect=[
                        (False, "RecordIdNotFound"),
                        (True, {"fields": {"名称": "A楼重建幂等测试"}}),
                    ],
                ), patch(
                    "lan_bitable_template_portal.server.create_bitable_record_fields",
                    return_value=(True, "rec-recreated"),
                ) as create_record, patch(
                    "lan_bitable_template_portal.server.update_bitable_record_fields",
                    return_value=(True, "rec-recreated"),
                ) as update_record, patch.object(
                    service,
                    "restore_notice_undo_local",
                    side_effect=[PortalError("本地恢复暂时失败"), {"restored_active": True}],
                ):
                    with self.assertRaisesRegex(PortalError, "本地恢复暂时失败"):
                        PortalRuntime.execute_notice_undo(undo_id)
                    stored = service._state_store.get_notice_undo_action(undo_id)
                    self.assertEqual(
                        stored["restored_target_record_id"], "rec-recreated"
                    )
                    result = PortalRuntime.execute_notice_undo(undo_id)
                self.assertTrue(result["ok"])
                create_record.assert_called_once()
                update_record.assert_called_once()
            finally:
                PortalRuntime.service, PortalRuntime.state_store = previous
                service._state_store.shutdown_write_worker(timeout=1.0)


if __name__ == "__main__":
    unittest.main()

"""Missing remote records and stale repair snapshots; no Feishu calls."""
import copy
from contextlib import closing
from pathlib import Path
import sys
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).parent))
from lan_bitable_template_portal.portal_service import (
    MaintenancePortalService, PortalError, PortalNotFoundError,
    REPAIR_SOURCE_APP_TOKEN, REPAIR_MANAGEMENT_TABLE_ID,
    REPAIR_SNAPSHOT_SOURCE_PROJECTS as PROJECTS,
    REPAIR_SNAPSHOT_SOURCE_FOLLOWUPS as FOLLOWUPS,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore


def project(identity, scope='A', workflow='未开始'):
    fields = {'维修名称': '测试检修', '所属数据中心/楼栋-使用': '南通' + scope + '楼',
              '流程-L': workflow, '故障发生时间': '2026-10-21 21:02'}
    return {'record_id': identity, 'work_type': 'repair', 'notice_type': '检修通告',
            'source_table_id': REPAIR_MANAGEMENT_TABLE_ID, 'source_app_token': REPAIR_SOURCE_APP_TOKEN,
            'raw_fields': copy.deepcopy(fields), 'display_fields': copy.deepcopy(fields)}


class DeletedProjectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = self.enterContext(tempfile.TemporaryDirectory())
        self.store = LanPortalStateStore(Path(self.temp) / 'state.sqlite3')
        self.addCleanup(self.store.shutdown_write_worker)
        self.service = MaintenancePortalService.__new__(MaintenancePortalService)
        self.service._state_store = self.store
        self.service._repair_snapshots_enabled = True
        self.service._repair_field_meta_by_name = {}
        self.service._refresh_lock = threading.RLock()
        self.service._repair_management_record_locks_guard = threading.RLock()
        self.service._repair_management_record_locks = {}
        self.service._repair_loaded_once = True
        self.service._repair_records = []
        self.service._source_refresh_status = {}
        self.service._save_source_scope_snapshots = Mock()
        self.service._touch_state_cache_version = Mock()
        self.service._invalidate_repair_management_status_cache = Mock()
        self.service._emit_repair_management_change = Mock()

    def replace(self, records, source=PROJECTS):
        return self.store.replace_repair_snapshot(source, records=records,
            app_token=REPAIR_SOURCE_APP_TOKEN, table_id=REPAIR_MANAGEMENT_TABLE_ID)

    def test_confirmed_delete_survives_stale_replace_upsert_and_restart_without_affecting_other_ids(self):
        stale = [project('recDeleted'), project('recRetained')]
        self.replace(stale)
        self.assertTrue(self.store.delete_repair_snapshot_record(PROJECTS, 'recDeleted', confirmed_deleted=True))
        self.assertEqual(self.replace(stale)['record_count'], 1)
        self.store.upsert_repair_snapshot_record(PROJECTS, 'recDeleted', project('recDeleted'))
        reopened = LanPortalStateStore(self.store.db_path)
        self.addCleanup(reopened.shutdown_write_worker)
        reopened.replace_repair_snapshot(PROJECTS, records=stale)
        self.assertEqual([row['record_id'] for row in reopened.get_repair_snapshot(PROJECTS)['records']], ['recRetained'])
        self.replace(stale, FOLLOWUPS)
        self.assertEqual(len(self.store.get_repair_snapshot(FOLLOWUPS)['records']), 2)

    def test_cache_eviction_is_not_a_remote_deletion(self):
        self.replace([project('recCurrent')])
        self.store.delete_repair_snapshot_record(PROJECTS, 'recCurrent')
        self.replace([project('recCurrent')])
        self.assertEqual(len(self.store.get_repair_snapshot(PROJECTS)['records']), 1)

    def test_cache_delete_failure_rolls_back_its_marker(self):
        self.replace([project('recCurrent')])
        with closing(sqlite3.connect(self.store.db_path)) as db:
            db.execute("CREATE TRIGGER reject_fixture_delete BEFORE DELETE ON repair_snapshot_records BEGIN SELECT RAISE(ABORT, 'fixture failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.delete_repair_snapshot_record(PROJECTS, 'recCurrent', confirmed_deleted=True)
        self.assertEqual(self.store.repair_deleted_record_ids(PROJECTS), set())
        self.assertEqual(len(self.store.get_repair_snapshot(PROJECTS)['records']), 1)

    def test_explicitly_cleared_event_mirror_does_not_restore_legacy_link(self):
        record = project('recCurrent')
        record['raw_fields'].update({'关联事件单-L': '', '关联事件单': ['recOldEvent']})
        self.assertEqual(self.service._repair_management_event_ids_from_record(record), [])

    def test_workbench_uses_current_project_snapshot_not_92_stale_source_rows(self):
        stale = [project('recOld' + str(i)) for i in range(92)]
        active = project('recCurrent')
        completed = project('recCompleted', workflow='维修完成')
        self.replace([active, active, completed, project('recForeign', scope='B')])
        self.service._source_snapshot_records = Mock(return_value=stale)
        self.service._apply_source_work_type_rules = lambda rows: rows
        self.service._sort_workbench_records = lambda rows: rows
        self.service._is_valid_repair_record = lambda _: True
        result = self.service._workbench_records(scope='A', month='10月')
        self.assertEqual([row['record_id'] for row in result], ['recCurrent'])

    def test_explicit_completed_workflow_hides_old_notice_without_end_time(self):
        completed = project('recCompleted', workflow='维修完成')
        self.assertTrue(self.service._repair_has_ended(completed))
        self.assertEqual(self.service._repair_source_status(completed), '已结束')
        self.assertFalse(self.service._repair_has_ended(project('recCurrent', workflow='维修中')))

    def test_missing_primary_update_removes_only_confirmed_missing_row_and_never_creates(self):
        missing = project('recMissing')
        self.replace([missing, project('recOther')])
        self.service._repair_records = [missing, project('recOther')]
        self.service._repair_management_snapshot_schema = Mock(return_value=([], {}))
        self.service._ensure_repair_management_record_in_scope = Mock(return_value=missing)
        self.service._load_table_records_by_ids = Mock(side_effect=PortalError('code=1254043, msg=RecordIdNotFound'))
        self.service._patch_record_fields = Mock()
        with self.assertRaisesRegex(PortalNotFoundError, '本次修改未保存'):
            self.service.update_repair_management_record('recMissing', {}, scope='A', replace_source_relations=True)
        self.service._patch_record_fields.assert_not_called()
        self.assertEqual(self.store.repair_deleted_record_ids(PROJECTS), {'recMissing'})
        self.assertEqual([row['record_id'] for row in self.service._repair_records], ['recOther'])

    def test_timeout_and_forbidden_never_turn_into_delete_or_success(self):
        current = project('recCurrent')
        self.replace([current])
        self.service._repair_management_snapshot_schema = Mock(return_value=([], {}))
        self.service._ensure_repair_management_record_in_scope = Mock(return_value=current)
        for message in ('code=1254302, RolePermNotAllow', 'Read timed out'):
            self.service._load_table_records_by_ids = Mock(side_effect=PortalError(message))
            with self.subTest(message=message), self.assertRaises(PortalError):
                self.service.update_repair_management_record('recCurrent', {}, scope='A')
            self.assertEqual(self.store.repair_deleted_record_ids(PROJECTS), set())
            self.assertEqual(len(self.store.get_repair_snapshot(PROJECTS)['records']), 1)

    def test_completed_delete_replays_without_remote_write_and_rechecks_building(self):
        operation_id = self.service._repair_sync_operation_id('project_delete', 'recDeleted')
        self.store.begin_repair_management_operation(operation_id, operation_type='project_delete',
            scope='A', summary_record_id='recDeleted', payload_hash='fixture')
        self.store.update_repair_management_operation(operation_id, status='completed',
            result={'deleted': True, 'record_id': 'recDeleted', 'record_scope_codes': ['A']})
        self.service._load_repair_management_project_records = Mock(side_effect=AssertionError('must not read old row'))
        self.assertTrue(self.service.delete_repair_management_record('recDeleted', scope='A')['deleted'])
        with self.assertRaisesRegex(PortalError, '无权'):
            self.service.delete_repair_management_record('recDeleted', scope='B')


if __name__ == '__main__':
    unittest.main()

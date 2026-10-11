"""A follow-up must not erase an existing completion timestamp."""
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.portal_service import (
    FieldMeta, MaintenancePortalService, REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME,
    REPAIR_MANAGEMENT_TABLE_ID,
)


class FollowupCompletionTimeTests(unittest.TestCase):
    def test_followup_preserves_missing_time_but_writes_valid_replacement(self):
        old_time = 1791536700000
        for new_time in (None, '', [], {}, old_time + 60000):
            with self.subTest(new_time=new_time):
                service = object.__new__(MaintenancePortalService)
                fields = {'维修跟进记录': 1, '当前维修进度': 2, '维修结束时间（2026）': 5}
                metas = {name: FieldMeta(f'fld-{index}', name, '', kind, False, {}, [], False)
                         for index, (name, kind) in enumerate(fields.items())}
                summary = {'record_id': 'rec-summary', 'raw_fields': {'维修结束时间（2026）': old_time}}
                followup = {'record_id': 'rec-followup', 'raw_fields': {
                    REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: 'rec-summary', '维修进度': 1}}
                writer = Mock()
                workflow = Mock(return_value=(True, []))
                with patch.multiple(service,
                    _ensure_repair_management_record_in_scope=Mock(return_value=summary),
                    _load_repair_management_project_records=Mock(return_value=(list(metas.values()), metas, [summary])),
                    _load_repair_followups_for_summary=Mock(return_value=([], {}, [followup])),
                    _build_repair_management_prefill=Mock(return_value={'fields': {
                        '维修结束时间（2026）': new_time, '当前维修进度': 1}}),
                    _ensure_repair_followup_select_options=Mock(return_value=(list(metas.values()), metas)),
                    _patch_record_fields=writer, _upsert_repair_snapshot_fields=Mock(),
                    _sync_repair_management_workflow=workflow,
                    _sync_repair_relation_business_fields=Mock(return_value={}),
                    _invalidate_repair_management_status_cache=Mock(),
                ), patch('lan_bitable_template_portal.repair_ledger.summary_fields', return_value={}):
                    service._sync_repair_management_from_followup_unlocked(
                        summary_record_id='rec-summary', followup_record_id='rec-followup', scope='B')
                writer.assert_called_once()
                sent = MaintenancePortalService._repair_physical_record_fields(
                    REPAIR_MANAGEMENT_TABLE_ID, writer.call_args.kwargs['fields'])
                if new_time in (None, '', [], {}):
                    self.assertNotIn('维修结束时间（2026）-L', sent)
                else:
                    self.assertEqual(sent['维修结束时间（2026）-L'], new_time)
                self.assertEqual(sent['当前维修进度-L'], 1)
                self.assertEqual(sent['维修跟进记录-L'], 'rec-followup')
                self.assertEqual(workflow.call_args.kwargs['workflow'], '维修完成')

    def test_sparse_target_projection_does_not_hide_existing_completion_time(self):
        service = object.__new__(MaintenancePortalService)
        for raw_end in (None, '', [], {}):
            projected = service._repair_management_relation_projection_fields(
                target_record={'display_fields': {'名称（标题）': 'B楼检修', '实际结束时间': raw_end}})
            self.assertNotIn('维修结束时间（2026）', projected)
        projected = service._repair_management_relation_projection_fields(
            target_record={'display_fields': {'名称（标题）': 'B楼检修', '实际结束时间': 1791536700000}})
        self.assertEqual(projected['维修结束时间（2026）'], 1791536700000)


if __name__ == '__main__':
    unittest.main()

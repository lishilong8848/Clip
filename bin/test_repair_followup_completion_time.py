"""A follow-up must not erase an existing completion timestamp, and a normal
repair-management save must not send an empty ``-L`` write for the completion
time either.

This module keeps the original follow-up and relation-projection coverage and
adds full-method-path coverage for ``_update_repair_management_record_unlocked``:

* only editing a normal editable field keeps the old completion time (the
  repair-auto prefill / source-controlled empty value is dropped),
* re-linking a repair reads a valid new completion time and writes it,
* re-linking to a repair that has no actual-end writes a physical ``-L``
  ``None`` and lets the new (empty) end drive the workflow,
* unlinking a repair clears the old source start/end, writes the empty link,
  and obeys the real ``_repair_management_workflow_for_state`` result instead
  of keeping the stale completion time.

All network/bitable I/O is mocked and the service is built via ``object.__new__``
with real logical ``FieldMeta``. The temporary ``CLIPFLOW_DATA_DIR`` and
``CLIPFLOW_REAL_EXTERNAL_CONFIRMED`` isolation settings are provided by the
parent runner, so this module keeps no global environment side effects.
"""
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.portal_service import (  # noqa: E402
    FieldMeta,
    MaintenancePortalService,
    REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME,
    REPAIR_MANAGEMENT_IN_PROGRESS_WORKFLOW,
    REPAIR_MANAGEMENT_NOT_STARTED_WORKFLOW,
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


class RepairManagementSavePreservesCompletionTimeTests(unittest.TestCase):
    OLD_END = 1791536700000
    NEW_END = OLD_END + 60000
    CLOUD_FAILURE_TIME = 1790000000000

    def _metas(self):
        def meta(name, field_type, ui_type='', options=()):
            return FieldMeta(
                f'fld-{name}', name, ui_type, field_type, False, {},
                list(options), False,
            )

        return {
            '维修结束时间（2026）': meta('维修结束时间（2026）', 5, 'datetime'),
            '维修开始时间': meta('维修开始时间', 5, 'datetime'),
            '设备检修关联': meta('设备检修关联', 18, 'link'),
            '关联事件单': meta('关联事件单', 18, 'link'),
            '故障发生现象描述': meta('故障发生现象描述', 1, 'text'),
            '故障发生时间': meta('故障发生时间', 5, 'datetime'),
            '故障维修原因': meta('故障维修原因', 1, 'text'),
            '所属专业': meta('所属专业', 3, 'singleSelect', ['电气', '暖通']),
            '所属数据中心/楼栋-使用': meta(
                '所属数据中心/楼栋-使用', 3, 'singleSelect', ['南通A楼', '南通B楼']),
        }

    def _cloud_record(self):
        raw = {
            '维修结束时间（2026）': self.OLD_END,
            '设备检修关联': 'rec-repair-old',
            '设备检修关联-L': 'rec-repair-old',
            '故障发生时间': self.CLOUD_FAILURE_TIME,
            '故障维修原因': '设备老化',
            '所属专业': '电气',
            '所属数据中心/楼栋-使用': '南通A楼',
            '故障发生现象描述': '旧故障描述',
        }
        return {
            'record_id': 'rec-summary',
            'source_table_id': REPAIR_MANAGEMENT_TABLE_ID,
            'raw_fields': dict(raw),
            'display_fields': dict(raw),
        }

    def _normal_fields(self, description):
        # A realistic normal save keeps the editable form/required fields and
        # changes only the fault-description field.
        return {
            '故障发生现象描述': description,
            '故障发生时间': self.CLOUD_FAILURE_TIME,
            '故障维修原因': '设备老化',
            '所属专业': '电气',
            '所属数据中心/楼栋-使用': '南通A楼',
        }

    def _linked_repair_auto(self, *, end):
        # Prefill from a readable linked repair: the repair supplies its start,
        # cause, and building, while ``end`` is the (possibly empty) actual_end.
        # The empty end is exactly the regression case: it must not erase the
        # previously saved completion timestamp.
        return {
            'fields': {
                '维修开始时间': 1791000000000,
                '维修结束时间（2026）': end,
                '故障维修原因': '设备老化',
                '所属数据中心/楼栋-使用': '南通A楼',
            },
            'warnings': [],
            'source_field_names': [
                '维修开始时间', '维修结束时间（2026）', '故障维修原因', '所属数据中心/楼栋-使用',
            ],
        }

    def _run_save(self, *, fields, auto, source_repair_ids=None, replace_source_relations=False):
        service = object.__new__(MaintenancePortalService)
        metas = self._metas()
        cloud = self._cloud_record()
        writer = Mock()
        workflow = Mock(return_value=(True, []))
        workflow_state = Mock(wraps=service._repair_management_workflow_for_state)
        relations = Mock(return_value={
            'target_results': [],
            'cleared_target_results': [],
            'projection': {},
            'warnings': [],
            'sync_pending': False,
        })
        save_kwargs = {'record_id': 'rec-summary', 'fields': fields}
        if source_repair_ids is not None:
            save_kwargs['source_repair_ids'] = source_repair_ids
        if replace_source_relations:
            save_kwargs['replace_source_relations'] = True
        with patch.multiple(
            service,
            _repair_management_snapshot_schema=Mock(
                return_value=(list(metas.values()), metas)),
            _load_table_records_by_ids=Mock(return_value=[cloud]),
            _ensure_repair_management_record_in_scope=Mock(return_value=cloud),
            _repair_management_record_in_scope=Mock(return_value=True),
            _remember_repair_project_fields=Mock(),
            _sync_repair_target_summary_id=Mock(
                return_value={'conflict': False, 'warning': ''}),
            _load_repair_followups_for_summary=Mock(return_value=([], {}, [])),
            _build_repair_management_prefill=Mock(return_value=auto),
            _ensure_repair_followup_select_options=Mock(
                return_value=(list(metas.values()), metas)),
            _repair_management_fields_in_scope=Mock(return_value=True),
            _patch_record_fields=writer,
            _upsert_repair_snapshot_fields=Mock(),
            _sync_repair_project_relations=relations,
            _schedule_repair_sync_task=Mock(return_value=False),
            _sync_repair_followups_from_summary=Mock(
                return_value={'synced_count': 0, 'failed_count': 0, 'warnings': []}),
            _sync_repair_management_workflow=workflow,
            _invalidate_repair_management_status_cache=Mock(),
            _repair_snapshot_record_version=Mock(return_value='v1'),
        ):
            with patch.object(
                service, '_repair_management_workflow_for_state', workflow_state
            ):
                result = service._update_repair_management_record_unlocked(**save_kwargs)
        writer.assert_called_once()
        sent = MaintenancePortalService._repair_physical_record_fields(
            REPAIR_MANAGEMENT_TABLE_ID, writer.call_args.kwargs['fields'])
        return sent, result, workflow, workflow_state, relations

    def test_only_editing_description_keeps_old_end_and_skips_empty_L_write(self):
        # The linked repair's actual_end is empty in the prefill, so the normal
        # save must not overwrite the existing completion timestamp. This holds
        # for every empty spelling the prefill may surface.
        for empty_end in (None, '', [], {}):
            with self.subTest(empty_end=empty_end):
                auto = self._linked_repair_auto(end=empty_end)
                sent, result, workflow, workflow_state, relations = self._run_save(
                    fields=self._normal_fields('机房渗水'),
                    auto=auto,
                )
                self.assertNotIn('维修结束时间（2026）-L', sent)
                self.assertEqual(sent['故障发生现象描述-L'], '机房渗水')
                self.assertEqual(sent['设备检修关联-L'], ['rec-repair-old'])
                self.assertEqual(result['workflow'], REPAIR_MANAGEMENT_IN_PROGRESS_WORKFLOW)
                self.assertEqual(
                    workflow.call_args.kwargs['workflow'], REPAIR_MANAGEMENT_IN_PROGRESS_WORKFLOW)
                self.assertEqual(workflow_state.call_args.kwargs['end_value'], self.OLD_END)
                relations.assert_called_once()
                self.assertEqual(relations.call_args.kwargs['target_record_ids'], ['rec-repair-old'])
                self.assertEqual(relations.call_args.kwargs['previous_target_record_ids'], [])

    def test_relink_reads_new_valid_end_and_writes_it(self):
        # Re-selecting a repair reads a valid new completion time that must be
        # written through the ``-L`` physical column.
        auto = self._linked_repair_auto(end=self.NEW_END)
        auto['repair_record_ids'] = ['rec-repair-new']
        sent, result, workflow, workflow_state, relations = self._run_save(
            fields=self._normal_fields('换用新检修'),
            auto=auto,
            source_repair_ids=['rec-repair-new'],
            replace_source_relations=True,
        )
        self.assertEqual(sent['维修结束时间（2026）-L'], self.NEW_END)
        self.assertEqual(sent['设备检修关联-L'], ['rec-repair-new'])
        self.assertEqual(result['workflow'], REPAIR_MANAGEMENT_IN_PROGRESS_WORKFLOW)
        self.assertEqual(workflow_state.call_args.kwargs['end_value'], self.NEW_END)
        relations.assert_called_once()
        self.assertEqual(relations.call_args.kwargs['target_record_ids'], ['rec-repair-new'])
        self.assertEqual(relations.call_args.kwargs['previous_target_record_ids'], ['rec-repair-old'])

    def test_unlink_clears_old_source_end_and_writes_empty_link(self):
        # Unlinking the repair clears the old source-controlled start/end,
        # writes an empty link, and lets the real workflow helper conclude the
        # record is not started rather than keeping the stale completion time.
        auto = {'fields': {}, 'warnings': [], 'source_field_names': []}
        sent, result, workflow, workflow_state, relations = self._run_save(
            fields=self._normal_fields('解除关联'),
            auto=auto,
            source_repair_ids=[],
            replace_source_relations=True,
        )
        self.assertIsNone(sent['维修结束时间（2026）-L'])
        self.assertIsNone(sent['维修开始时间-L'])
        self.assertEqual(sent['设备检修关联-L'], [])
        self.assertEqual(result['workflow'], REPAIR_MANAGEMENT_NOT_STARTED_WORKFLOW)
        self.assertEqual(
            workflow.call_args.kwargs['workflow'], REPAIR_MANAGEMENT_NOT_STARTED_WORKFLOW)
        self.assertIsNone(workflow_state.call_args.kwargs['end_value'])
        self.assertIsNone(workflow_state.call_args.kwargs['start_value'])
        relations.assert_called_once()
        self.assertEqual(relations.call_args.kwargs['target_record_ids'], [])
        self.assertEqual(relations.call_args.kwargs['previous_target_record_ids'], ['rec-repair-old'])

    def test_relink_to_new_repair_with_empty_actual_end_writes_none_end(self):
        # Re-linking to a NEW repair replaces the old source-controlled end with
        # the new repair's actual_end. Because that actual_end is empty, the
        # physical ``-L`` completion time is explicitly cleared to ``None`` and
        # the new linkage is genuinely written (not silently reverted).
        auto = self._linked_repair_auto(end=None)
        auto['repair_record_ids'] = ['rec-repair-new']
        sent, result, workflow, workflow_state, relations = self._run_save(
            fields=self._normal_fields('换绑定新检修（新actual_end为空）'),
            auto=auto,
            source_repair_ids=['rec-repair-new'],
            replace_source_relations=True,
        )
        self.assertIsNone(sent['维修结束时间（2026）-L'])
        self.assertEqual(sent['维修开始时间-L'], 1791000000000)
        self.assertEqual(sent['设备检修关联-L'], ['rec-repair-new'])
        self.assertEqual(result['workflow'], REPAIR_MANAGEMENT_IN_PROGRESS_WORKFLOW)
        self.assertEqual(
            workflow.call_args.kwargs['workflow'], REPAIR_MANAGEMENT_IN_PROGRESS_WORKFLOW)
        self.assertIsNone(workflow_state.call_args.kwargs['end_value'])
        relations.assert_called_once()
        self.assertEqual(relations.call_args.kwargs['target_record_ids'], ['rec-repair-new'])
        self.assertEqual(relations.call_args.kwargs['previous_target_record_ids'], ['rec-repair-old'])


if __name__ == '__main__':
    unittest.main()
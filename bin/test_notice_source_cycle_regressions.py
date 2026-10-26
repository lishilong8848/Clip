"""Local-only regressions for source provenance, lifecycle and cycle isolation."""
import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from bin.test_notice_memory_history import _HistoryMemoryService, _source_record
from lan_bitable_template_portal.portal_service import (
    STATE_NS_DAILY_SUMMARY, STATE_NS_MEMORY, STATE_NS_WORK_STATUS, FieldMeta, MaintenancePortalService, NoticeSourcePending,
    PortalError, REPAIR_SNAPSHOT_SOURCE_PROJECTS, REPAIR_SOURCE_APP_TOKEN, REPAIR_MANAGEMENT_TABLE_ID,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore


class NoticeSourceCycleRegressionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = self.new_service()

    def new_service(self):
        store = LanPortalStateStore(Path(self.tmp.name) / 'state.sqlite3')
        with (
            patch('lan_bitable_template_portal.portal_service.LanPortalStateStore', return_value=store),
            patch('lan_bitable_template_portal.portal_service.get_data_file_path', side_effect=lambda name: str(Path(self.tmp.name) / name)),
            patch.object(MaintenancePortalService, '_ensure_signature_crypto_ready'),
        ):
            service = _HistoryMemoryService(Path(self.tmp.name) / 'state.sqlite3')
        service._memory_dir = Path(self.tmp.name) / 'memory'
        service._summary_dir = Path(self.tmp.name) / 'summaries'
        service._work_status_dir = Path(self.tmp.name) / 'work-status'
        service._field_meta_by_name['维护实施状态'] = FieldMeta(
            'status-formula', '维护实施状态', 'Formula', 20, False, {}, [], True)
        return service

    def tearDown(self):
        self.service._state_store.shutdown_write_worker(timeout=2)
        self.tmp.cleanup()

    def repair_binding_snapshot(self):
        self.service._repair_snapshots_enabled = True
        self.service.ensure_snapshot_loaded = Mock()
        self.service._source_snapshot_records = MaintenancePortalService._source_snapshot_records.__get__(self.service)
        records = [dict(record_id=f'rec_bind_{index:04d}', work_type='repair', notice_type='设备检修',
            source_app_token=REPAIR_SOURCE_APP_TOKEN, source_table_id=REPAIR_MANAGEMENT_TABLE_ID,
            raw_fields={}, display_fields={'检修通告名称':f'EA118_C01机房D楼{index:04d}设备检修',
                '维修名称':f'D楼{index:04d}设备检修', '所属数据中心/楼栋-使用':'D楼',
                '故障发生时间':'2026-10-26 08:00', '期望完成时间':'2026-10-26 18:00',
                '故障维修原因':'计划原因', '区域':'计划位置'}) for index in range(505)]
        for suffix, fields, raw in (
            ('ended', {'维修结束时间':'2026-10-26 19:00'}, {}),
            ('other_building', {'所属数据中心/楼栋-使用':'E楼'}, {}),
            ('linked', {}, {'设备检修关联-L':'rec_other_target'}),
        ):
            record = copy.deepcopy(records[-1])
            record['record_id'] = 'rec_bind_' + suffix
            record['display_fields'].update(fields)
            record['raw_fields'].update(raw)
            records.append(record)
        self.service._state_store.replace_repair_snapshot(REPAIR_SNAPSHOT_SOURCE_PROJECTS,
            records=[self.service._repair_snapshot_record_payload(row) for row in records],
            app_token=REPAIR_SOURCE_APP_TOKEN, table_id=REPAIR_MANAGEMENT_TABLE_ID)
        return records[504]['record_id']

    def test_binding_searched_plan_after_500_uses_all_eligible_records(self):
        selected = self.repair_binding_snapshot()
        candidates = self.service.list_bindable_source_items(
            scope='D', work_type='repair', month='10月', search='0504', limit=200)
        self.assertIn(selected, [row['source_record_id'] for row in candidates])
        for target in ('', 'rec_uploaded_target'):
            with self.subTest(target=target):
                bound = self.service.validate_manual_source_binding(scope='D', work_type='repair',
                    source_record_id=selected, month='10月', target_record_id=target)
                self.assertEqual(bound['source_record_id'], selected)

    def test_large_binding_keeps_building_type_state_and_existing_relation_checks(self):
        selected = self.repair_binding_snapshot()
        for source, work, ongoing in (
            ('rec_bind_ended', 'repair', []),
            ('rec_bind_other_building', 'repair', []),
            ('rec_bind_linked', 'repair', []),
            (selected, 'maintenance', []),
            ('rec_missing', 'repair', []),
            (selected, 'repair', [{'source_record_id':selected, 'target_record_id':'rec_busy',
                                   'work_type':'repair', 'building_codes':['D']}]),
        ):
            with self.subTest(source=source, work=work, ongoing=bool(ongoing)), self.assertRaises(PortalError):
                self.service.validate_manual_source_binding(scope='D', work_type=work,
                    source_record_id=source, month='10月', ongoing_items=ongoing)

    def test_large_repair_binding_start_preserves_submitted_fields_and_target_identity(self):
        selected = self.repair_binding_snapshot()
        manual = {'manual':True, 'manual_id':'manual:lite:D:repair:regression',
            'manual_binding_required':True, 'manual_binding_choice':'bind', 'source_record_id':selected,
            'title':'EA118_C01机房D楼手填故障检修', 'reason':'手填原因', 'location':'手填位置',
            'start_time':'2026-10-26 20:00', 'end_time':'2026-10-26 09:00',
            'site_photos':[{'upload_id':'fixture-local-photo'}]}
        before = copy.deepcopy(manual)
        expanded = self.service.expand_workbench_action_command(
            {'command_format':'notice_command', 'action':'start', 'scope':'D', 'work_type':'repair',
             'record_id':selected, 'source_record_id':selected, 'patch':manual}, scope='D', ongoing_items=[])
        for key in ('title', 'reason', 'location', 'start_time', 'end_time', 'site_photos'):
            self.assertEqual(expanded[key], manual[key])
        self.assertEqual(expanded['source_record_id'], selected)
        self.assertEqual(expanded['repair_management_record_id'], selected)
        self.assertFalse(expanded.get('target_record_id'))
        self.assertEqual(manual, before)

    def remember(self, cycle, content, work_type='maintenance'):
        self.service._remember_draft_fields(
            building='A楼', maintenance_total='冷站Y型过滤器检查', work_type=work_type,
            maintenance_cycle=cycle, location='冷站', content=content, reason='保养', impact='无影响')

    def source(self, cycle):
        return _source_record('rec-source', 'maintenance', {
            '楼栋':'A楼', '维护总项':'冷站Y型过滤器检查', '维护周期':cycle})

    def test_cycles_have_independent_content_and_aliases_share_a_key(self):
        for cycle, content in [('月度','monthly'), ('季度','quarterly'), ('年度','annual')]:
            self.remember(cycle, content)
            self.assertEqual(self.service._get_record_memory(self.source(cycle))['content'], content)
        self.assertEqual(len(self.service._state_store.get_document(STATE_NS_MEMORY,'A楼')['items']), 3)
        self.assertEqual(self.service._memory_item_key('maintenance','检查','每月'),
                         self.service._memory_item_key('maintenance','检查','月度'))

    def test_old_cycle_keys_are_still_readable_without_cross_cycle_fallback(self):
        self.service._state_store.put_document(STATE_NS_MEMORY,'A楼', {'building':'A楼','items':{
            '冷站y型过滤器检查|cycle:每月': {'work_type':'maintenance','memory_name':'冷站Y型过滤器检查',
                'maintenance_cycle':'每月','content':'old-monthly'}}})
        self.assertEqual(self.service._get_record_memory(self.source('月度'))['content'],'old-monthly')
        self.assertFalse(self.service._get_record_memory(self.source('季度')).get('content'))

    def test_explicit_cycles_are_distinct_for_other_notice_types_too(self):
        self.remember('月度','monthly','change')
        self.remember('年度','annual','change')
        payload = self.service._state_store.get_document(STATE_NS_MEMORY,'A楼')
        self.assertEqual(len(payload['items']), 2)

    def test_history_recommendations_do_not_offer_another_cycle(self):
        source = {'id':'source','work_type':'maintenance','canonical_title':'检查','building_codes':['A'],
                  'maintenance_cycle':'每月'}
        quarterly = {'id':'quarter','work_type':'maintenance','canonical_title':'检查','building_codes':['A'],
                     'maintenance_cycle':'每季','fields':{'content':'quarterly'}}
        self.assertEqual(self.service._recommend_history_memory_matches([source],[quarterly]),[])
        monthly = {**quarterly,'id':'month','maintenance_cycle':'月度'}
        self.assertEqual(self.service._recommend_history_memory_matches([source],[monthly])[0]['candidate_id'],'month')

    def test_import_refuses_source_candidate_cycle_conflict(self):
        result = self.service.save_historical_notice_memory_matches(matches=[{
            'source_item':{'work_type':'maintenance','building':'A楼','memory_name':'检查','maintenance_cycle':'每月'},
            'fields':{'maintenance_cycle':'年度','content':'annual'}, 'selected':True}])
        self.assertEqual(result['saved_count'],0)
        self.assertIn('周期不同',result['skipped'][0]['reason'])

    def test_new_target_does_not_inherit_old_source_lifecycle(self):
        service = self.service
        old = {'work_type':'maintenance','source_record_id':'rec-plan','target_record_id':'rec-old',
               'active_item_id':'active-shared','status':'已结束','ended_at':'2026-09-17 17:22'}
        self.assertIsNone(service._find_work_status_item([old],work_type='maintenance',
            source_record_id='rec-plan',target_record_id='rec-new',active_item_id='active-shared'))
        self.assertIs(service._find_work_status_item([old],work_type='maintenance',
            source_record_id='rec-plan',target_record_id='rec-old'),old)

    def test_old_target_end_does_not_close_new_target_on_same_source(self):
        for kind in ['maintenance','change','repair','polling','adjust','power']:
            old = {'work_type':kind,'source_record_id':'rec-plan','target_record_id':'rec-old'}
            current = {**old,'target_record_id':'rec-new'}
            self.assertFalse(self.service._work_status_matches_identity(current,old),kind)
            self.assertTrue(self.service._work_status_matches_identity(old,old),kind)

    def test_work_status_read_prefers_new_lifecycle_not_last_list_entry(self):
        old = {'work_type':'maintenance','source_record_id':'rec-plan','target_record_id':'rec-old',
               'status':'已结束','started_at':'2026-09-17 09:06','ended_at':'2026-09-17 17:22'}
        current = {**old,'target_record_id':'rec-new','status':'进行中','started_at':'2026-10-15 10:00','ended_at':''}
        self.service._load_work_status_items_locked = Mock(return_value=[current,old])
        result = self.service._work_status_by_records([{'record_id':'rec-plan','work_type':'maintenance'}])
        self.assertEqual(result['rec-plan']['target_record_id'],'rec-new')
        old.pop('started_at')
        old['updated_at']='2026-10-16 12:00'
        result = self.service._work_status_by_records([{'record_id':'rec-plan','work_type':'maintenance'}])
        self.assertEqual(result['rec-plan']['target_record_id'],'rec-new','refreshing an old completion is not a new lifecycle')

    def test_new_same_title_source_start_does_not_reuse_old_finished_daily_row(self):
        service = self.service
        title = 'EA118机房B楼（瑞福莱定压）定压补水系统月度维护'
        old = {'work_type':'maintenance','source_record_id':'rec-september',
               'target_record_id':'rec-old','active_item_id':'active-old',
               'title':title,'building':'B楼','status':'已结束',
               'started_at':'2026-09-17 09:06','ended_at':'2026-09-17 17:22',
               'fallback_key':service._summary_key(title=title,building='B楼',work_type='maintenance')}
        day = {'items':[old]}
        service._load_day_summary_locked = Mock(return_value=day)
        service._save_day_summary_locked = Mock()
        service._upsert_work_status_item_locked = Mock()
        service._resolve_successful_action_identity = lambda job, **kw: job['prepared']
        service.sync_repair_management_notice_action = Mock(return_value={})
        service._schedule_repair_link_task_after_success = Mock()
        service._jobs['test-new'] = {'prepared':{'action':'start','work_type':'maintenance',
            'source_record_id':'rec-october','target_record_id':'rec-new','active_item_id':'active-new',
            'title':title,'building':'B楼','building_codes':['B']}}
        service._record_successful_action('test-new',record_id='rec-new',active_item_id='active-new')
        self.assertEqual(len(day['items']),2)
        current=day['items'][1]
        self.assertEqual(current['source_record_id'],'rec-october')
        self.assertEqual(current['status'],'进行中')
        self.assertFalse(current.get('ended_at'))
        self.assertEqual(old['target_record_id'],'rec-old')

    def test_finished_snapshot_does_not_change_other_target_daily_row(self):
        current={'work_type':'maintenance','source_record_id':'rec-plan','target_record_id':'rec-new',
                 'status':'进行中','started_at':'2026-10-15 10:00'}
        day={'items':[current]}
        service=self.service
        service._upsert_work_status_item_locked=Mock()
        service._iter_day_summary_payloads_locked=Mock(return_value=[day])
        service._save_day_summary_locked=Mock()
        service._mark_local_notice_finished_from_target(
            {**current,'target_record_id':'rec-old'},ended_at='2026-09-17 17:22')
        self.assertEqual(current['status'],'进行中')
        self.assertFalse(current.get('ended_at'))

    def test_b_building_october_reference_excludes_september_completed_notice(self):
        service=self.service
        title='（瑞福莱定压）定压补水系统月度维护'
        source=_source_record('recvcTu2F2a7a4','maintenance',{'楼栋':'B楼','维护总项':title,
            '维护周期':'每月','维护实施状态':'未开始','计划维护月份':'10月',
            '计划开始维护时间':'2026-10-15 00:00','计划结束维护时间':'2026-10-21 00:00'})
        def target(record_id,date,state,cycle='每月'):
            return {'record_id':record_id,'display_fields':{'名称':'EA118机房B楼'+title,
                '楼栋':'B楼','维保周期':cycle,'维保状态':state,
                '计划开始时间':date+' 09:30','计划结束时间':date+' 18:30'}}
        service.target_records['maintenance']=[target('rec-old','2026-09-17','结束'),
            target('rec-new','2026-10-15','开始'),target('rec-quarterly','2026-10-15','开始','每季')]
        result=service._resolve_target_record_reference(work_type='maintenance',notice_type='维保通告',
            source_record=source,include_finished=True)
        self.assertEqual(result,'rec-new')

    def monthly_source(self, actual_start='46311.40625'):
        return _source_record('recvcTu2F2a7a4', 'maintenance', {
            '楼栋': 'B楼', '维护总项': '（瑞福莱定压）定压补水系统月度维护',
            '维护周期': '每月', '维护实施状态': '进行中', '计划维护月份': '10月',
            '实际开始时间': actual_start, '计划开始维护时间': '2026-10-15 00:00',
            '计划结束维护时间': '2026-10-21 00:00',
        })

    def old_monthly_status(self):
        return {
            'work_type': 'maintenance', 'source_record_id': 'recvcTu2F2a7a4',
            'target_record_id': 'rec-old-july', 'active_item_id': 'rec-old-july',
            'building': 'B楼', 'building_code': 'B', 'status': '未开始',
            'started_at': '2026-07-15 10:46', 'ended_at': '2026-07-15 17:30',
            'completed_date': '2026-07-15', 'reset_after_delete': True,
            'reset_at': '2026-10-15 10:04', 'updated_at': '2026-10-15 10:04',
            'actions': [{'action': 'end', 'time': '2026-07-15 17:30', 'job_id': 'old-end'}],
        }

    def save_work_status(self, *items):
        self.service._save_work_status_locked({
            'building': 'B楼', 'building_code': 'B', 'items': list(items),
        })

    def test_reset_with_old_end_does_not_override_restarted_monthly_source(self):
        old = self.old_monthly_status()
        result = self.service._serialize_record(self.monthly_source(), {
            old['source_record_id']: old,
        })
        self.assertEqual(result['source_progress'], '进行中')
        self.assertFalse(result['work_summary'].get('ended_at'))
        self.assertEqual(old['ended_at'], '2026-07-15 17:30', 'do not modify history on read')

    def test_reset_removes_stale_identity_but_preserves_genuine_completion(self):
        old = self.old_monthly_status()
        self.save_work_status(old)
        loaded = self.service._load_work_status_items_locked('B')[0]
        for name in ('target_record_id', 'active_item_id', 'started_at', 'ended_at', 'completed_date'):
            self.assertFalse(loaded.get(name), name)
        self.assertEqual(loaded['actions'], [])
        self.assertEqual(loaded['status'], '未开始')
        self.assertEqual(self.service._target_record_id_from_work_status(
            work_type='maintenance', source_record_id=old['source_record_id']), '')
        finished = {**old, 'status': '已结束', 'reset_after_delete': False}
        result = self.service._serialize_record(self.monthly_source('2026-07-15 10:46'), {
            old['source_record_id']: finished,
        })
        self.assertEqual(result['source_progress'], '进行中')
        self.assertEqual(result['work_summary']['status'], '已结束')

    def test_reset_upsert_clears_existing_old_lifecycle(self):
        old = self.old_monthly_status()
        self.save_work_status({**old, 'status': '已结束', 'reset_after_delete': False})
        reset = {key: value for key, value in old.items() if key not in {
            'target_record_id', 'active_item_id', 'started_at', 'ended_at', 'completed_date',
        }}
        reset['actions'] = []
        self.service._upsert_work_status_item_locked(reset, now=reset['reset_at'])
        saved = self.service._state_store.get_document(STATE_NS_WORK_STATUS, 'B')['items'][0]
        self.assertFalse(saved.get('ended_at'))
        self.assertFalse(saved.get('target_record_id'))
        self.assertEqual(saved['actions'], [])

    def test_restart_backfill_cannot_replay_pre_reset_completion(self):
        old = self.old_monthly_status()
        self.save_work_status(old)
        history = {**old, 'status': '已结束', 'reset_after_delete': False}
        history.pop('reset_at')
        self.service._state_store.put_document(STATE_NS_DAILY_SUMMARY, '2026-07-15', {
            'date': '2026-07-15', 'updated_at': '2026-10-16 15:00', 'items': [history],
        })
        for _ in range(2):
            self.service._state_store.shutdown_write_worker(timeout=2)
            self.service = self.new_service()
            loaded = self.service._load_work_status_items_locked('B')
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded[0]['status'], '未开始')
            self.assertFalse(loaded[0].get('ended_at'))
        preserved = self.service._state_store.get_document(STATE_NS_DAILY_SUMMARY, '2026-07-15')
        self.assertEqual(preserved['items'][0]['ended_at'], '2026-07-15 17:30')

    def test_corrupted_reset_daily_row_cannot_replay_its_old_end_action(self):
        old = self.old_monthly_status()
        self.save_work_status(old)
        self.service._state_store.put_document(STATE_NS_DAILY_SUMMARY, '2026-10-15', {
            'date': '2026-10-15', 'updated_at': old['updated_at'], 'items': [old],
        })
        loaded = self.service._load_work_status_items_locked('B')[0]
        self.assertEqual(loaded['status'], '未开始')
        self.assertFalse(loaded.get('ended_at'))
        self.assertEqual(loaded['actions'], [])

    def test_post_reset_start_end_and_late_reset_replay_keep_current_lifecycle(self):
        old = self.old_monthly_status()
        self.save_work_status(old)
        current = {
            'work_type': 'maintenance', 'source_record_id': old['source_record_id'],
            'target_record_id': 'rec-current', 'active_item_id': 'rec-current',
            'building': 'B楼', 'building_code': 'B', 'status': '进行中',
            'started_at': '2026-10-15 10:04',
        }
        self.service._upsert_work_status_item_locked(current, action='start', now=current['started_at'])
        self.service._upsert_work_status_item_locked(old, now=old['reset_at'])
        active = self.service._load_work_status_items_locked('B')[0]
        self.assertEqual(active['status'], '进行中')
        self.assertEqual(active['target_record_id'], 'rec-current')
        self.service._upsert_work_status_item_locked({
            **current, 'status': '已结束', 'ended_at': '2026-10-16 17:00',
        }, action='end', now='2026-10-16 17:00')
        self.service._upsert_work_status_item_locked(old, now=old['reset_at'])
        summaries = self.service._work_status_by_records([self.monthly_source()], scope='B')
        result = self.service._serialize_record(self.monthly_source(), summaries)
        self.assertEqual(result['source_progress'], '进行中')
        self.assertEqual(result['work_summary']['target_record_id'], 'rec-current')
        self.assertEqual(result['work_summary']['ended_at'], '2026-10-16 17:00')

    def test_older_reset_cannot_rewind_newer_reset_boundary(self):
        old = self.old_monthly_status()
        self.save_work_status({**old, 'reset_at': '2026-10-16 12:00'})
        self.service._upsert_work_status_item_locked(old, now=old['reset_at'])
        self.service._upsert_work_status_item_locked({
            **old, 'reset_after_delete': False, 'status': '已结束',
            'ended_at': '2026-10-16 11:00', 'actions': [],
        }, action='end', now='2026-10-16 11:00')
        loaded = self.service._load_work_status_items_locked('B')[0]
        self.assertEqual(loaded['reset_at'], '2026-10-16 12:00')
        self.assertEqual(loaded['status'], '未开始')
        self.assertFalse(loaded.get('ended_at'))

    def test_local_terminal_history_never_overrides_ongoing_source(self):
        old = {**self.old_monthly_status(), 'status': '已结束', 'reset_after_delete': False}
        for actual_start in ('', 'not-a-date', '2026-07-15 10:46'):
            with self.subTest(actual_start=actual_start):
                result = self.service._serialize_record(self.monthly_source(actual_start), {
                    old['source_record_id']: old,
                })
                self.assertEqual(result['source_progress'], '进行中')
                self.assertEqual(result['work_summary']['ended_at'], old['ended_at'])

    def test_unbound_monthly_can_bind_without_reusing_old_target_or_quarterly(self):
        source = self.monthly_source()
        self.save_work_status(self.old_monthly_status())
        self.service.ensure_snapshot_loaded = Mock()
        quarterly = _source_record('rec-quarterly', 'maintenance', {
            **source['display_fields'], '维护总项': '（瑞福莱定压）定压补水系统季度维护',
            '维护周期': '每季',
        })
        self.service._workbench_records = Mock(return_value=[source, quarterly])
        manual = {'work_type': 'maintenance', 'title': '月度维护',
                  'target_record_id': 'rec-current', 'active_item_id': 'rec-current', 'manual': True}
        self.service._project_ongoing_items = Mock(return_value=[manual, {
            'work_type': 'maintenance', 'source_record_id': quarterly['record_id'],
            'target_record_id': 'rec-current-quarterly',
        }])
        options = self.service.list_bindable_source_items(scope='B', work_type='maintenance', month='10月')
        self.assertEqual([item['source_record_id'] for item in options], [source['record_id']])
        self.assertEqual(options[0]['progress'], '进行中')
        validated = self.service.validate_manual_source_binding(
            scope='B', work_type='maintenance', source_record_id=source['record_id'],
            month='10月', ongoing_items=[manual], target_record_id='rec-current',
            active_item_id='rec-current',
        )
        self.assertEqual(validated['source_record_id'], source['record_id'])
        self.assertFalse(manual.get('source_record_id'), 'name matching must not bind on read')
        summaries = self.service._work_status_by_records([source], scope='B')
        self.assertFalse(summaries.get(source['record_id'], {}).get('target_record_id'))

    def test_plan_status_uses_only_source_for_every_notice_type_and_local_state(self):
        from lan_bitable_template_portal.workbench_lite import _linked_ongoing_for_source, _record_progress

        for work_type in ('maintenance', 'change', 'repair', 'polling', 'adjust', 'power'):
            for source_status in ('未开始', '进行中', '已结束'):
                for local_status in ('未开始', '进行中', '已结束'):
                    with self.subTest(work_type=work_type, source=source_status, local=local_status):
                        fields = {'楼栋': 'B楼', '维护总项': '测试维护', '维护实施状态': source_status,
                                  '变更进度': source_status, '变更简述': '测试变更'}
                        if source_status != '未开始':
                            fields['维修开始时间'] = '2026-10-16 09:00'
                        if source_status == '已结束':
                            fields['维修结束时间'] = '2026-10-16 17:00'
                        notice_type = {'maintenance': '维保通告', 'change': '变更通告', 'repair': '设备检修',
                                       'polling': '设备轮巡', 'adjust': '设备调整', 'power': '上电通告'}[work_type]
                        record = {'record_id': 'rec-source', 'work_type': work_type, 'notice_type': notice_type,
                                  'display_fields': fields,
                                  'source_work_type': work_type if work_type in ('change', 'repair') else 'maintenance'}
                        summary = {'source_record_id': 'rec-source', 'status': local_status,
                                   'started_at': '2026-07-15 09:00',
                                   'ended_at': '2026-07-15 17:30' if local_status == '已结束' else ''}
                        serialized = self.service._serialize_record(record, {'rec-source': summary})
                        self.assertEqual(serialized['source_progress'], source_status)
                        self.assertEqual(serialized['source_status'], source_status)
                        self.assertEqual(_record_progress(serialized), source_status)
                        linked = _linked_ongoing_for_source(serialized, [{
                            'work_type': work_type, 'source_record_id': 'rec-source',
                            'target_record_id': 'rec-target', 'active_item_id': 'rec-target', 'status': '更新',
                        }])
                        self.assertEqual(_record_progress(linked), source_status)

    def test_plan_status_is_not_optimistically_rewritten_after_submission_or_binding(self):
        from lan_bitable_template_portal import workbench_lite

        source = Path(workbench_lite.__file__).read_text(encoding='utf-8')
        binding = source.split('function applyIdentityBindingToInbox(', 1)[1].split(
            'async function refreshTaskInboxAfterIdentityBinding()', 1)[0]
        self.assertNotIn('setOngoingRowStatus(sourceRow,', binding)
        submission = source.split('function removeSourceRowForDraft(', 1)[1].split(
            'function applyOptimisticSubmission(', 1)[0]
        self.assertNotIn("querySelector('.row-status')", submission)
        self.assertIn('function lockSourceRowUntilRefresh(', submission)

    def test_source_end_sync_changes_snapshot_only_after_confirmed_remote_write(self):
        source = self.monthly_source()
        self.service._records = [source]
        payload = {'work_type': 'maintenance', 'source_record_id': source['record_id'],
                   'target_record_id': 'rec-target', 'ended_at': '2026-10-16 17:00',
                   'plan_month': '10月', 'maintenance_cycle': '每月'}
        self.service._request_json = Mock(return_value={'data': {'record': {
            'record_id': source['record_id'], 'fields': dict(source['display_fields']),
        }}})
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={
            'mock_external': False, 'real_write_allowed': True,
        }), patch.object(self.service, '_patch_record_fields', side_effect=RuntimeError('write failed')):
            with self.assertRaisesRegex(RuntimeError, 'write failed'):
                self.service.sync_notice_source_ended_fields(payload)
        self.assertEqual(self.service._serialize_record(source)['source_progress'], '进行中')
        self.assertNotIn('实际结束时间', source['display_fields'])
        self.service._request_json.side_effect = [
            {'data': {'record': {'record_id': source['record_id'], 'fields': dict(source['display_fields'])}}},
            {'data': {'record': {'record_id': source['record_id'], 'fields': {
                **source['display_fields'], '维护实施状态': '已结束', '实际结束时间': '2026-10-16 17:00',
            }}}},
        ]
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={
            'mock_external': False, 'real_write_allowed': True,
        }), patch.object(self.service, '_patch_record_fields') as remote_write:
            self.assertTrue(self.service.sync_notice_source_ended_fields(payload))
        remote_write.assert_called_once()
        self.assertEqual(self.service._serialize_record(source)['source_progress'], '已结束')

    def test_source_end_uses_readback_status_even_if_formula_is_still_ongoing(self):
        source = self.monthly_source()
        self.service._records = [source]
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={
            'mock_external': False, 'real_write_allowed': True,
        }), patch.object(self.service, '_request_json', side_effect=[
            {'data': {'record': {'record_id': source['record_id'], 'fields': dict(source['display_fields'])}}},
            {'data': {'record': {'record_id': source['record_id'], 'fields': {
                **source['display_fields'], '实际结束时间': '2026-10-16 17:00',
            }}}},
        ]), patch.object(self.service, '_patch_record_fields'):
            with self.assertRaises(NoticeSourcePending):
                self.service.sync_notice_source_ended_fields({
                    'work_type': 'maintenance', 'source_record_id': source['record_id'],
                    'target_record_id': 'rec-target', 'ended_at': '2026-10-16 17:00',
                    'plan_month': '10月', 'maintenance_cycle': '每月',
                })
        self.assertEqual(self.service._serialize_record(source)['source_progress'], '进行中')
        self.assertEqual(source['display_fields']['实际结束时间'], '2026-10-16 17:00')

    def test_source_end_readback_failure_retains_task_without_guessing_plan_status(self):
        from lan_bitable_template_portal import server
        from lan_bitable_template_portal.server import PortalRuntime

        source = self.monthly_source()
        self.service._records = [source]
        store = self.service._state_store
        config = server.get_field_config('维保通告')
        with patch.object(PortalRuntime, 'service', self.service), patch.object(PortalRuntime, 'state_store', store), \
            patch.object(server, 'query_record_by_id', return_value=(True, {'fields': {
                config['status']: '结束', config['actual_end']: '2026-10-16 17:00',
            }})), patch('lan_bitable_template_portal.portal_service.external_real_write_guard',
                return_value={'mock_external': False, 'real_write_allowed': True}), \
            patch.object(self.service, '_request_json', side_effect=[
                {'data': {'record': {'record_id': source['record_id'], 'fields': dict(source['display_fields'])}}},
                RuntimeError('readback offline'),
            ]), patch.object(self.service, '_patch_record_fields') as write:
            PortalRuntime._queue_source_end_sync({'work_type': 'maintenance',
                'source_record_id': source['record_id'], 'plan_month': '10月',
                'maintenance_cycle': '每月'}, 'rec-target', '维保通告')
            PortalRuntime.process_source_end_sync()
            write.assert_called_once()
            tasks = store.list_outbox_events('notice_source_finalize', status='deferred')
            self.assertEqual(len(tasks), 1)
            self.assertIn('readback offline', tasks[0]['last_error'])
            self.assertEqual(tasks[0]['attempts'], 0)
            self.assertFalse(store.list_outbox_events('notice_source_finalize', status='done'))
        self.assertEqual(self.service._serialize_record(source)['source_progress'], '进行中')
        self.assertNotIn('实际结束时间', source['display_fields'])

    def test_empty_source_status_is_unknown_not_a_local_or_target_status(self):
        from lan_bitable_template_portal.workbench_lite import _record_disabled_reason, _record_progress, _record_rows

        for kind in ('maintenance', 'change', 'repair', 'polling', 'adjust', 'power'):
            for local_status in ('未开始', '进行中', '已结束'):
                with self.subTest(kind=kind, local=local_status):
                    row = {'work_type': kind, 'source_progress': '', 'source_status': '',
                           'status': local_status, 'work_summary': {'status': local_status}}
                    self.assertEqual(_record_progress(row), '状态未取得')
                    self.assertIn('重新读取', _record_disabled_reason(_record_progress(row)))
                    html = _record_rows([{**row, 'record_id': 'rec-source', 'title': '测试维护'}],
                        ongoing_items=[], scope='B', work_type=kind, search='', specialty='', selected_id='')
                    self.assertIn('aria-disabled="true"', html)
                    self.assertIn('>状态未取得</span>', html)
        self.assertFalse(self.service._source_progress_allows_start(''))
        self.assertEqual(_record_progress({'work_type': 'maintenance', 'status': '已结束'}), '状态未取得')

    def test_delayed_end_task_cannot_end_a_new_source_cycle(self):
        from lan_bitable_template_portal import server
        from lan_bitable_template_portal.server import PortalRuntime

        source = self.monthly_source()
        self.service._records = [source]
        self.service._request_json = Mock(return_value={'data': {'record': {
            'record_id': source['record_id'], 'fields': dict(source['display_fields']),
        }}})
        store = self.service._state_store
        with patch.object(PortalRuntime, 'service', self.service), patch.object(PortalRuntime, 'state_store', store):
            PortalRuntime._queue_source_end_sync({'work_type': 'maintenance',
                'source_record_id': source['record_id']}, 'rec-old-july', '维保通告')
            config = server.get_field_config('维保通告')
            with patch.object(server, 'query_record_by_id', return_value=(True, {'fields': {
                config['status']: '结束', config['actual_end']: '2026-07-15 17:30',
            }})), patch('lan_bitable_template_portal.portal_service.external_real_write_guard',
                return_value={'mock_external': False, 'real_write_allowed': True}), \
                patch.object(self.service, '_patch_record_fields') as source_write:
                PortalRuntime.process_source_end_sync()
            source_write.assert_not_called()
            self.assertEqual(len(store.list_outbox_events('notice_source_finalize', status='done')), 1)
        self.assertEqual(source['display_fields']['维护实施状态'], '进行中')
        self.assertNotIn('实际结束时间', source['display_fields'])

    def test_source_end_reads_current_cycle_and_does_not_trust_stale_cache(self):
        source = self.monthly_source('2026-07-15 09:00')
        source['display_fields']['计划开始维护时间'] = '2026-07-15 00:00'
        self.service._records = [source]
        payload = {'work_type': 'maintenance', 'source_record_id': source['record_id'],
                   'target_record_id': 'rec-old-july', 'ended_at': '2026-07-15 17:30'}
        current_fields = dict(self.monthly_source()['display_fields'])
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={
            'mock_external': False, 'real_write_allowed': True,
        }), patch.object(self.service, '_request_json', return_value={'data': {'record': {
            'record_id': source['record_id'], 'fields': current_fields,
        }}}) as read, patch.object(self.service, '_patch_record_fields') as write:
            self.assertFalse(self.service.sync_notice_source_ended_fields(payload))
        read.assert_called_once()
        write.assert_not_called()
        self.assertNotIn('实际结束时间', source['display_fields'])

    def test_source_end_read_failure_keeps_task_retryable_without_writing(self):
        from lan_bitable_template_portal import server
        from lan_bitable_template_portal.server import PortalRuntime

        source = self.monthly_source()
        self.service._records = [source]
        store = self.service._state_store
        with patch.object(PortalRuntime, 'service', self.service), patch.object(PortalRuntime, 'state_store', store):
            PortalRuntime._queue_source_end_sync({'work_type': 'maintenance',
                'source_record_id': source['record_id']}, 'rec-target', '维保通告')
            config = server.get_field_config('维保通告')
            with patch.object(server, 'query_record_by_id', return_value=(True, {'fields': {
                config['status']: '结束', config['actual_end']: '2026-10-16 17:00',
            }})), patch('lan_bitable_template_portal.portal_service.external_real_write_guard',
                return_value={'mock_external': False, 'real_write_allowed': True}), \
                patch.object(self.service, '_request_json', side_effect=RuntimeError('source offline')), \
                patch.object(self.service, '_patch_record_fields') as write:
                PortalRuntime.process_source_end_sync()
            write.assert_not_called()
            tasks = store.list_outbox_events('notice_source_finalize', status='deferred')
            self.assertEqual(len(tasks), 1)
            self.assertIn('source offline', tasks[0]['last_error'])
            self.assertEqual(tasks[0]['attempts'], 0)
            self.assertFalse(store.list_outbox_events('notice_source_finalize', status='done'))
        self.assertEqual(source['display_fields']['维护实施状态'], '进行中')

    def test_source_end_refuses_conflicting_current_target_binding(self):
        source = self.monthly_source()
        self.service._records = [source]
        self.service._state_store.upsert_notice_identity({'work_type': 'maintenance',
            'source_record_id': source['record_id'], 'target_record_id': 'rec-current',
            'status': '进行中', 'building_codes': ['B']})
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={
            'mock_external': False, 'real_write_allowed': True,
        }), patch.object(self.service, '_request_json', return_value={'data': {'record': {
            'record_id': source['record_id'], 'fields': dict(source['display_fields']),
        }}}), patch.object(self.service, '_patch_record_fields') as write:
            with self.assertRaisesRegex(Exception, '关联.*变化'):
                self.service.sync_notice_source_ended_fields({'work_type': 'maintenance',
                    'source_record_id': source['record_id'], 'target_record_id': 'rec-old',
                    'ended_at': '2026-10-16 17:00'})
        write.assert_not_called()

    def test_source_end_requires_actual_end_time_and_complete_source_read(self):
        payload = {'work_type': 'maintenance', 'source_record_id': 'rec-source',
                   'target_record_id': 'rec-target'}
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={
            'mock_external': False, 'real_write_allowed': True,
        }), patch.object(self.service, '_patch_record_fields') as write:
            with self.assertRaisesRegex(Exception, '实际结束时间'):
                self.service.sync_notice_source_ended_fields(payload)
            for response in ({}, {'data': {'record': {'record_id': 'rec-other', 'fields': {'维护实施状态': '进行中'}}}},
                             {'data': {'record': {'record_id': 'rec-source', 'fields': {'维护总项': '测试'}}}},
                             {'data': {'record': {'record_id': 'rec-source', 'fields': {
                                 '维护实施状态': '进行中', '实际开始时间': 'invalid-date',
                             }}}}):
                with patch.object(self.service, '_request_json', return_value=response):
                    with self.assertRaisesRegex(Exception, '源记录'):
                        self.service.sync_notice_source_ended_fields({**payload, 'ended_at': '2026-10-16 17:00'})
        write.assert_not_called()

    def test_source_end_checks_plan_start_without_actual_start_in_both_source_tables(self):
        for kind, source_kind, fields in (
            ('maintenance', 'maintenance', {'维护实施状态': '进行中', '计划开始维护时间': '2026-10-15'}),
            ('change', 'change', {'变更进度': '进行中', '变更开始日期（阿里）': '2026-10-15'}),
            ('change', 'maintenance', {'维护实施状态': '进行中', '计划开始维护时间': '2026-10-15'}),
            ('maintenance', 'maintenance', {'维护实施状态': '进行中', '实际开始时间': [{'text': '46311.40625'}]}),
        ):
            with self.subTest(kind=kind, source=source_kind), \
                patch('lan_bitable_template_portal.portal_service.external_real_write_guard',
                      return_value={'mock_external': False, 'real_write_allowed': True}), \
                patch.object(self.service, '_request_json', return_value={'data': {'record': {
                    'record_id': 'rec-source', 'fields': fields,
                }}}), patch.object(self.service, '_patch_record_fields') as write:
                self.assertFalse(self.service.sync_notice_source_ended_fields({
                    'work_type': kind, 'source_work_type': source_kind, 'source_record_id': 'rec-source',
                    'target_record_id': 'rec-old', 'ended_at': '2026-07-15 17:30',
                }))
                write.assert_not_called()

    def test_source_end_waits_for_target_operation_lock_and_releases_after_failure(self):
        from lan_bitable_template_portal import server
        from lan_bitable_template_portal.server import PortalRuntime

        store = self.service._state_store
        payload = {'work_type': 'maintenance', 'source_record_id': 'rec-source',
                   'target_record_id': 'rec-target', 'notice_type': '维保通告'}
        key = 'maintenance:target:rec-target'
        self.assertTrue(store.acquire_notice_operation_lock(key, owner='undo-owner',
            action='undo', ttl_seconds=180)[0])
        with patch.object(PortalRuntime, 'service', self.service), patch.object(PortalRuntime, 'state_store', store), \
            patch.object(server, 'query_record_by_id') as query:
            with self.assertRaises(Exception):
                PortalRuntime._sync_confirmed_source_end(payload)
            query.assert_not_called()
            store.release_notice_operation_lock(key, 'undo-owner')
            query.side_effect = RuntimeError('target offline')
            with self.assertRaisesRegex(RuntimeError, 'target offline'):
                PortalRuntime._sync_confirmed_source_end(payload)
            self.assertTrue(store.acquire_notice_operation_lock(key, owner='next-owner',
                action='update', ttl_seconds=180)[0], 'failed read must not leave the target locked')
            store.release_notice_operation_lock(key, 'next-owner')

    def test_incomplete_target_read_remains_retryable_instead_of_becoming_done(self):
        from lan_bitable_template_portal import server
        from lan_bitable_template_portal.server import PortalRuntime

        store = self.service._state_store
        with patch.object(PortalRuntime, 'service', self.service), patch.object(PortalRuntime, 'state_store', store), \
            patch.object(server, 'query_record_by_id', return_value=(True, {})), \
            patch.object(self.service, 'sync_notice_source_ended_fields') as write:
            PortalRuntime._queue_source_end_sync({'work_type': 'maintenance',
                'source_record_id': 'rec-source'}, 'rec-target', '维保通告')
            PortalRuntime.process_source_end_sync()
            write.assert_not_called()
            self.assertEqual(len(store.list_outbox_events('notice_source_finalize', status='deferred')), 1)
            self.assertFalse(store.list_outbox_events('notice_source_finalize', status='done'))

    def test_expired_target_lock_after_source_read_cannot_write(self):
        from lan_bitable_template_portal import server
        from lan_bitable_template_portal.server import PortalRuntime

        source = self.monthly_source()
        self.service._records = [source]
        store = self.service._state_store
        config = server.get_field_config('维保通告')
        with patch.object(PortalRuntime, 'service', self.service), patch.object(PortalRuntime, 'state_store', store), \
            patch.object(server, 'query_record_by_id', return_value=(True, {'fields': {
                config['status']: '结束', config['actual_end']: '2026-10-16 17:00',
            }})), patch('lan_bitable_template_portal.portal_service.external_real_write_guard',
                return_value={'mock_external': False, 'real_write_allowed': True}), \
            patch.object(self.service, '_request_json', return_value={'data': {'record': {
                'record_id': source['record_id'], 'fields': dict(source['display_fields']),
            }}}), patch.object(store, 'renew_notice_operation_lock', return_value=False), \
            patch.object(self.service, '_patch_record_fields') as write:
            PortalRuntime._queue_source_end_sync({'work_type': 'maintenance',
                'source_record_id': source['record_id'], 'plan_month': '10月',
                'maintenance_cycle': '每月'}, 'rec-target', '维保通告')
            PortalRuntime.process_source_end_sync()
            write.assert_not_called()
            tasks = store.list_outbox_events('notice_source_finalize', status='deferred')
            self.assertEqual(len(tasks), 1)
            self.assertIn('操作锁已过期', tasks[0]['last_error'])
            self.assertEqual(tasks[0]['attempts'], 0)
            self.assertFalse(store.list_outbox_events('notice_source_finalize', status='done'))


if __name__ == '__main__':
    unittest.main()

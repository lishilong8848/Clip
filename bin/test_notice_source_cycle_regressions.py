"""Local-only regressions for source provenance, lifecycle and cycle isolation."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from bin.test_notice_memory_history import _HistoryMemoryService, _source_record
from lan_bitable_template_portal.portal_service import STATE_NS_MEMORY, MaintenancePortalService
from lan_bitable_template_portal.state_store import LanPortalStateStore


class NoticeSourceCycleRegressionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        store = LanPortalStateStore(Path(self.tmp.name) / 'state.sqlite3')
        with (
            patch('lan_bitable_template_portal.portal_service.LanPortalStateStore', return_value=store),
            patch('lan_bitable_template_portal.portal_service.get_data_file_path', side_effect=lambda name: str(Path(self.tmp.name) / name)),
            patch.object(MaintenancePortalService, '_ensure_signature_crypto_ready'),
        ):
            self.service = _HistoryMemoryService(Path(self.tmp.name) / 'state.sqlite3')
        self.service._memory_dir = Path(self.tmp.name) / 'memory'
        self.service._summary_dir = Path(self.tmp.name) / 'summaries'
        self.service._work_status_dir = Path(self.tmp.name) / 'work-status'

    def tearDown(self):
        self.service._state_store.shutdown_write_worker(timeout=2)
        self.tmp.cleanup()

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


if __name__ == '__main__':
    unittest.main()

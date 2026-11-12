import copy
import datetime as dt
import json
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path
from lan_bitable_template_portal.daily_inspection import (CN, SOURCES, DailyInspection, inspection_report,
    original_templates, required_sheets, default_sheet_groups, RiskError)
from lan_bitable_template_portal.state_store import LanPortalStateStore
from test_daily_inspection import InspectionCloud, DAY

class SheetWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.temp = self.enterContext(tempfile.TemporaryDirectory())
        self.store = LanPortalStateStore(Path(self.temp)/'state.sqlite3')
        self.cloud = InspectionCloud()
        self.svc = DailyInspection(self.store,self.cloud)
        self.svc.download_archive=lambda a:self.cloud.files[a['file_token']]
        self.active=self.svc.change('a','甲',DAY,'start',{'duty':DAY})['active']
    def report(self):
        return inspection_report(self.svc,'D',DAY['date'],'a','甲',include_active=True,drafts={})
    def target(self):
        return next(c for c in self.report()['rounds'][0]['original_cells'] if not c['read_only'] and '状态' not in c['parameter'])
    def finish(self,version):
        return self.svc.change('a','甲',DAY,'finish',{'id':self.active['id'],'review_version':version,'sheet_workflow':True,
            'visited_sheets':[json.dumps([t['name'],s['name']],ensure_ascii=False,separators=(',',':')) for t in original_templates() if t['scope']=='D' for s in t['sheets']]})
    def test_current_edit_persisted_retry_and_gate(self):
        c=self.target();edit={k:c[k] for k in ('template','sheet','cell')};edit['value']='123'
        body={'id':self.active['id'],'review_version':0,'sheet_cells':[edit]}
        saved=self.svc.change('a','甲',DAY,'draft',body)['active']
        self.assertEqual(saved['manual_sheet_cells'],[{k:edit[k] for k in ('template','sheet','cell')}])
        restored=DailyInspection(self.store,self.cloud)
        replay=restored.change('a','甲',DAY,'draft',body)['active']
        self.assertEqual(replay['review_version'],saved['review_version'])
        self.assertEqual(replay['manual_sheet_cells'],saved['manual_sheet_cells'])
        with self.assertRaisesRegex(RiskError,'逐页'):
            self.svc.change('a','甲',DAY,'finish',{'id':self.active['id'],'review_version':saved['review_version'],'sheet_workflow':True,'visited_sheets':[]})
        self.assertEqual(self.cloud.creates,0)
        self.finish(saved['review_version'])
        archived=self.report()['rounds'][0]
        self.assertEqual(next(x['value'] for x in archived['original_cells'] if all(x[k]==edit[k] for k in ('template','sheet','cell'))),'123')

    def test_workbook_progress_and_signoff_follow_effective_groups(self):
        duty = {**DAY, 'scope': 'E'}
        defaults = default_sheet_groups('E')
        moved = next(key for key, name in defaults.items() if name == '暖通')
        self.svc.change('a', '甲', duty, 'sheet-group', {'id': '', 'duty': duty, 'groups': {moved: '电气'}})
        active = self.svc.change('a', '甲', duty, 'start',
            {'duty': duty, 'worksheet_mode': True, 'professions': ['电气']})['active']
        identity = active['id']
        self.assertEqual(active['sheet_groups'], {moved: '电气'})
        self.assertEqual(self.svc.load('D')['active']['id'], self.active['id'])
        chosen = self.svc.change('b', '乙', duty, 'append-profession', {'id': identity,
            'profession': '暖通', 'review_version': active['review_version']})['active']
        self.assertEqual(chosen['professions'], ['电气', '暖通'])
        required = required_sheets(self.svc.load('E')['active'], 'E', ['电气'])
        self.assertIn(moved, required)
        with self.assertRaisesRegex(RiskError, '尚未确认'):
            self.svc.change('a', '甲', duty, 'finish', {'id': identity, 'review_version': chosen['review_version'],
                'sheet_workflow': True, 'visited_sheets': list(defaults)})
        self.svc.change('a', '甲', duty, 'view-sheet', {'id': identity, 'sheet': moved, 'visited_sheets': list(required)})
        restored = DailyInspection(self.store, self.cloud)
        receipt = restored.view(restored.load('E'), 'a', duty)['active']
        self.assertEqual(set(receipt['visited_sheets']), required)
        self.assertEqual(receipt['review_version'], chosen['review_version'])
        sign = {'id': identity, 'profession': '电气', 'profession_version': receipt['profession_versions']['电气']}
        with self.assertRaisesRegex(RiskError, '逐页'):
            restored.change('b', '乙', duty, 'confirm-profession', sign)
        signed = restored.change('a', '甲', duty, 'confirm-profession', sign)['active']
        self.assertEqual(signed['profession_confirmations']['电气']['actor'], '甲')
        reset = restored.change('b', '乙', duty, 'sheet-group', {'id': identity,
            'review_version': signed['review_version'], 'groups': {moved: None}})['active']
        self.assertEqual(reset['sheet_groups'], {})
        self.assertIn('归属已调整', reset['confirmation_invalidations']['电气'])
        for owner, actor, name in (('a', '甲', '电气'), ('b', '乙', '暖通')):
            pages = sorted(required_sheets(restored.load('E')['active'], 'E', [name]))
            restored.change(owner, actor, duty, 'view-sheet', {'id': identity, 'sheet': pages[0], 'visited_sheets': pages})
            reset = restored.change(owner, actor, duty, 'confirm-profession', {'id': identity, 'profession': name,
                'profession_version': reset['profession_versions'][name]})['active']
        self.assertEqual({name: row['actor'] for name, row in reset['profession_confirmations'].items()}, {'电气': '甲', '暖通': '乙'})
        restored.download_archive = self.svc.download_archive
        result = restored.change('b', '乙', duty, 'finish', {'id': identity, 'review_version': reset['review_version'],
            'sheet_workflow': True})
        self.assertIsNone(result['active'])
        self.assertEqual(self.cloud.creates, 1)
        self.assertEqual(restored.load('D')['active']['id'], self.active['id'])

    def test_workbook_start_defaults_professions_and_cannot_skip_confirmation(self):
        duty = {**DAY, 'scope': 'E'}
        active = self.svc.change('a', '甲', duty, 'start', {'duty': duty, 'worksheet_mode': True})['active']
        self.assertEqual(active['professions'], ['电气', '暖通'])
        with self.assertRaisesRegex(RiskError, '尚未确认'):
            self.svc.change('a', '甲', duty, 'finish', {'id': active['id'], 'review_version': active['review_version']})
        self.assertEqual(self.cloud.creates, 0)
    def test_history_cells_retry_loss_and_free_cell_preserved(self):
        c=self.target();self.finish(0);r=self.report()['rounds'][0]
        import io
        from lan_bitable_template_portal.daily_inspection_template_art import read_artwork
        before_attachment=self.svc.archive_record('D',r['id'])[2]
        before_art=read_artwork(io.BytesIO(self.svc.download_archive(before_attachment)))
        edits=[{k:c[k] for k in ('template','sheet','cell')},{'template':c['template'],'sheet':c['sheet'],'cell':'A1','value':'现场更正'}]
        edits[0]['value']='234'
        body={'version':r['version'],'cells':edits}
        self.cloud.lose_receipt=True
        with self.assertRaises(RiskError):self.svc.edit_archive('D',r['id'],body,'乙')
        uploads=self.cloud.uploads;updates=self.cloud.updates
        self.cloud.lose_receipt=False
        self.svc.edit_archive('D',r['id'],body,'乙')
        self.assertEqual((self.cloud.uploads,self.cloud.updates),(uploads,updates))
        revised=self.report()['rounds'][0]
        self.assertEqual(next(x['value'] for x in revised['original_cells'] if all(x[k]==c[k] for k in ('template','sheet','cell'))),'234')
        self.assertTrue(any(x['cell']=='A1' and x['value']=='现场更正' for x in revised['extra_cells']))
        after_attachment=self.svc.archive_record('D',r['id'])[2]
        after_art=read_artwork(io.BytesIO(self.svc.download_archive(after_attachment)))
        self.assertEqual({k:(v['xml'],v['assets']) for k,v in after_art.items()},
                         {k:(v['xml'],v['assets']) for k,v in before_art.items()})
        with self.assertRaisesRegex(RiskError,'已被修改'):
            self.svc.edit_archive('D',r['id'],{**body,'cells':[{**edits[0],'value':'999'}]},'丙')

    def test_blank_history_cell_cannot_be_saved_to_a_different_round(self):
        self.finish(0)
        self.active = self.svc.change('a', '甲', DAY, 'start', {'duty': DAY})['active']
        self.finish(0)
        rounds = self.report()['rounds']
        template = next(item for item in original_templates() if item['scope'] == 'D' and item['profession'] == '电气')
        sheet = next(item for item in template['sheets'] if item['name'].strip() == '电气系统西区')
        edit = {'template': template['name'], 'sheet': sheet['name'], 'cell': 'B5', 'value': '18'}
        with self.assertRaisesRegex(RiskError, '另一巡检轮次'):
            self.svc.edit_archive('D', rounds[1]['id'], {'version': rounds[1]['version'], 'cells': [edit]}, '甲')
        self.assertEqual(self.cloud.updates, 0)
        self.svc.edit_archive('D', rounds[0]['id'], {'version': rounds[0]['version'], 'cells': [edit]}, '甲')
        self.assertEqual(self.cloud.updates, 1)

    def test_sheet_summary_and_single_sheet_keep_same_round_identity(self):
        self.finish(0)
        full = self.report()
        reference = next(cell for cell in full['rounds'][0]['original_cells'] if not cell['read_only'])
        with patch.object(self.svc, 'download_archive', side_effect=AssertionError('仅摘要不应读附件')):
            summary = inspection_report(self.svc, 'D', DAY['date'], 'a', '甲', drafts={}, summary_only=True)
        self.assertEqual(summary['rounds'][0]['round_id'], full['rounds'][0]['round_id'])
        self.assertEqual(summary['slot_states'][0]['status'], '已归档')
        self.assertEqual(summary['rounds'][0]['original_cells'], [])
        pair = (reference['template'], reference['sheet'])
        filtered = inspection_report(self.svc, 'D', DAY['date'], 'a', '甲', drafts={}, view_sheet=pair)
        self.assertEqual(filtered['sheet_filter'], list(pair))
        self.assertIn(reference, filtered['rounds'][0]['original_cells'])
        self.assertTrue(all((cell['template'], cell['sheet']) == pair for cell in filtered['rounds'][0]['original_cells']))
    def test_foreign_building_and_stale_version_rejected(self):
        t=next(t for t in original_templates() if t['scope']=='E')
        with self.assertRaises(RiskError):
            self.svc.change('a','甲',DAY,'draft',{'id':self.active['id'],'review_version':0,'sheet_cells':[{'template':t['name'],'sheet':t['sheets'][0]['name'],'cell':'A1','value':'wrong'}]})
        self.assertEqual(self.svc.load('D')['active'].get('review_version',0),0)
        with self.assertRaisesRegex(RiskError,'其他修改'):
            self.svc.change('a','甲',DAY,'draft',{'id':self.active['id'],'review_version':77,'sheet_cells':[{'value':'1'}]})

    def test_sheet_group_is_shared_for_shift_but_not_next_shift_or_building(self):
        t=next(t for t in original_templates() if t['scope']=='D' and t['profession']=='暖通')
        key=json.dumps([t['name'],t['sheets'][0]['name']],ensure_ascii=False,separators=(',',':'))
        changed=self.svc.change('a','甲',DAY,'sheet-group',{'id':self.active['id'],'review_version':0,'sheet':key,'profession':'电气'})['active']
        self.assertEqual(self.report()['sheet_groups'][key],'电气')
        self.finish(changed['review_version'])
        active=self.svc.change('b','乙',DAY,'start',{'duty':DAY})['active']
        self.assertEqual(self.svc.load('D')['active']['sheet_groups'][key],'电气')
        self.active=active;self.finish(active['review_version'])
        night={**DAY,'shift':'夜'}
        self.svc.change('b','乙',night,'start',{'duty':night})
        self.assertEqual(self.svc.load('D')['active']['sheet_groups'],{})
        self.assertIsNone(self.store.get_document('daily_de_inspection','sheet-groups:E:'+DAY['date']+':白'))

    def test_d_and_e_edit_and_archive_keep_distinct_snapshots(self):
        from lan_bitable_template_portal.daily_inspection import NS
        e_duty={**DAY,'scope':'E'}
        e=self.svc.change('b','乙',e_duty,'start',{'duty':e_duty})['active']
        before_e=copy.deepcopy(self.svc.load('E'))
        d_target=self.target()
        d_edit={k:d_target[k] for k in ('template','sheet','cell')};d_edit['value']='116'
        d=self.svc.change('a','甲',DAY,'draft',{'id':self.active['id'],'review_version':0,'sheet_cells':[d_edit]})['active']
        self.assertEqual(self.svc.load('E'),before_e)
        e_report=inspection_report(self.svc,'E',DAY['date'],'b','乙',include_active=True,drafts={})
        e_target=next(c for c in e_report['rounds'][0]['original_cells'] if not c['read_only'] and '状态' not in c['parameter'])
        e_edit={k:e_target[k] for k in ('template','sheet','cell')};e_edit['value']='235'
        self.svc.change('b','乙',e_duty,'draft',{'id':e['id'],'review_version':0,'sheet_cells':[e_edit]})
        self.finish(d['review_version'])
        self.assertIsNotNone(self.svc.load('E')['active'])
        self.assertIsNone(self.svc.load('D')['active'])
        latest_e=inspection_report(self.svc,'E',DAY['date'],'b','乙',include_active=True,drafts={})
        self.assertEqual(next(c['value'] for c in latest_e['rounds'][0]['original_cells'] if all(c[k]==e_edit[k] for k in ('template','sheet','cell'))),'235')
        self.assertIsNotNone(self.store.get_document(NS,'submitted:'+self.active['id']))
        self.assertIsNone(self.store.get_document(NS,'submitted:'+e['id']))

    def test_e_cover_is_not_required_and_d_stays_independent(self):
        duty={**DAY,'scope':'E'}
        active=self.svc.change('b','乙',duty,'start',{'duty':duty})['active']
        visited=[json.dumps([template['name'],sheet['name']],ensure_ascii=False,separators=(',',':'))
                 for template in original_templates() if template['scope']=='E'
                 for sheet in template['sheets'] if sheet['name'].strip()!='封面']
        self.assertTrue(any(sheet['name'].strip()=='封面' for t in original_templates() if t['scope']=='E' for sheet in t['sheets']))
        with self.assertRaisesRegex(RiskError,'逐页'):
            self.svc.change('b','乙',duty,'finish',{'id':active['id'],'review_version':0,'sheet_workflow':True,'visited_sheets':visited[:-1]})
        self.svc.change('b','乙',duty,'finish',{'id':active['id'],'review_version':0,'sheet_workflow':True,'visited_sheets':visited})
        self.assertEqual(self.svc.load('D')['active']['id'],self.active['id'])

    def test_d_archive_receipt_failure_does_not_block_e_draft(self):
        e_duty={**DAY,'scope':'E'}
        e=self.svc.change('b','乙',e_duty,'start',{'duty':e_duty})['active']
        self.cloud.lose_receipt=True
        with self.assertRaises(RiskError):self.finish(0)
        self.cloud.lose_receipt=False
        self.assertIn('completed',self.svc.load('D')['active'])
        e_report=inspection_report(self.svc,'E',DAY['date'],'b','乙',include_active=True,drafts={})
        c=next(c for c in e_report['rounds'][0]['original_cells'] if not c['read_only'] and '状态' not in c['parameter'])
        edit={k:c[k] for k in ('template','sheet','cell')};edit['value']='7'
        updated=self.svc.change('b','乙',e_duty,'draft',{'id':e['id'],'review_version':0,'sheet_cells':[edit]})
        self.assertGreater(updated['active']['review_version'],0)
        self.svc.change('a','甲',DAY,'finish',{'id':self.active['id']})
        self.assertEqual(self.cloud.creates,1)
        self.assertIsNotNone(self.svc.load('E')['active'])
        self.assertIsNone(self.svc.load('D')['active'])

    def test_unconfirmed_round_is_frozen_at_next_slot_and_retried_once(self):
        import datetime as dt
        from lan_bitable_template_portal.daily_inspection import CN
        self.svc.rollover_due('D',dt.datetime(2026,11,3,11,59,tzinfo=CN))
        self.assertIsNotNone(self.svc.load('D')['active'])
        self.svc.rollover_due('D',dt.datetime(2026,11,3,12,0,tzinfo=CN))
        state=self.svc.load('D')
        self.assertIsNone(state['active'])
        self.assertEqual(self.svc.completed_rounds(state,DAY),1)
        report=self.report()
        self.assertEqual(report['rounds'][0]['status'],'自动确认·待上传')
        self.assertTrue(report['rounds'][0]['auto_closed'])
        self.assertTrue(report['rounds'][0]['original_cells'])
        next_round=self.svc.change('a','甲',DAY,'start',{'duty':DAY})['active']
        self.assertEqual(next_round['round'],2)
        self.cloud.lose_receipt=True
        with self.assertLogs(level='ERROR'):self.svc.retry_rollovers('D')
        self.cloud.lose_receipt=False
        self.svc=DailyInspection(self.store,self.cloud)
        self.svc.download_archive=lambda a:self.cloud.files[a['file_token']]
        self.svc.retry_rollovers('D')
        self.svc.retry_rollovers('D')
        self.assertEqual(self.cloud.creates,1)
        self.assertEqual(self.svc.load('D')['active']['id'],next_round['id'])
        self.assertEqual(len(self.svc.load('D')['history']),1)
        self.assertEqual(self.svc.load('D')['rollovers'],{})
        self.assertTrue(self.report()['rounds'][0]['auto_closed'])

    def test_unvisited_building_archives_each_scheduled_round(self):
        scope = 'E'
        source_rows = self.cloud.tables[SOURCES[scope][0][0]]
        roster = lambda day: {'date': day, 'complete': True, 'people': []}
        service = DailyInspection(self.store, self.cloud, scheduled=True)
        service.download_archive = lambda attachment: self.cloud.files[attachment['file_token']]

        slots = [(3, 9, 30, 8, 1, '白'), (3, 12, 10, 12, 2, '白'),
                 (3, 20, 10, 20, 1, '夜'), (4, 0, 10, 0, 2, '夜')]
        for day, hour, minute, batch_hour, number, shift in slots:
            now = dt.datetime(2026, 11, day, hour, minute, tzinfo=CN)
            service.rollover_due(scope, now)
            for row in source_rows:
                stamp = int(now.replace(hour=batch_hour, minute=5).timestamp() * 1000)
                row['fields'].update(上传日期=stamp, 源文件时间=stamp)
            with service.source_cache_lock:
                cached = service.cached_source(scope)
                if cached:
                    cached['refreshed_at'] = 0
            service.open_scheduled_round(scope, roster, now)
            state = service.load(scope)
            self.assertEqual((state['active']['duty']['date'], state['active']['duty']['shift'], state['active']['round']),
                             (DAY['date'], shift, number))
            service.open_scheduled_round(scope, roster, now)
            self.assertEqual(service.load(scope)['active']['id'], state['active']['id'])
            service.retry_rollovers(scope)

        service.rollover_due(scope, dt.datetime(2026, 11, 4, 9, 30, tzinfo=CN))
        service.retry_rollovers(scope)
        restored = DailyInspection(self.store, self.cloud, scheduled=True)
        restored.download_archive = service.download_archive
        restored.retry_rollovers(scope)
        report = inspection_report(restored, scope, DAY['date'], 'a', '甲', drafts={})
        self.assertEqual([(round_['shift'], round_['round']) for round_ in report['rounds']],
                         [('白', 1), ('白', 2), ('夜', 1), ('夜', 2)])
        self.assertTrue(all(round_['auto_closed'] and round_['original_cells'] for round_ in report['rounds']))
        self.assertEqual(self.cloud.creates, 4)
        self.assertEqual(restored.load('D')['active']['id'], self.active['id'])

    def test_scheduled_round_waits_for_matching_batch_after_missed_slot(self):
        service = DailyInspection(self.store, self.cloud, scheduled=True)
        now = dt.datetime(2026, 11, 3, 12, 10, tzinfo=CN)
        duty = {**DAY, 'scope': 'E'}
        roster = lambda day: {'date': day, 'complete': True, 'people': []}
        for row in self.cloud.tables[SOURCES['E'][0][0]]:
            row['fields'].update(上传日期=int(now.timestamp() * 1000),
                                 源文件时间=int(now.replace(hour=10).timestamp() * 1000))
        service.open_scheduled_round('E', roster, now)
        self.assertIsNone(service.load('E')['active'])
        self.assertEqual(service.next_round(service.load('E'), duty, now), 2)
        for row in self.cloud.tables[SOURCES['E'][0][0]]:
            row['fields']['源文件时间'] = int(now.timestamp() * 1000)
        service.cached_source('E')['refreshed_at'] = 0
        service.open_scheduled_round('E', roster, now)
        self.assertEqual(service.load('E')['active']['round'], 2)
        service.rollover_due('E', now.replace(hour=20))
        self.assertEqual(service.completed_rounds(service.load('E'), duty), 1)
        self.assertEqual(service.next_round(service.load('E'), duty, now), 3)

    def test_unconfirmed_cloud_rollover_is_not_editable(self):
        import datetime as dt
        from lan_bitable_template_portal.daily_inspection import CN
        self.svc.rollover_due('D',dt.datetime(2026,11,3,12,tzinfo=CN))
        self.cloud.lose_receipt=True
        with self.assertLogs(level='ERROR'):self.svc.retry_rollovers('D')
        self.cloud.lose_receipt=False
        row=self.report()['rounds'][0]
        self.assertEqual(row['status'],'待归档核验')
        with self.assertRaisesRegex(RiskError,'归档尚未确认'):
            self.svc.edit_archive('D','recArchive1',{'version':row['version'],'cells':[]},'甲')
        self.svc.retry_rollovers('D')
        self.assertEqual(self.report()['rounds'][0]['status'],'已归档')

    def test_rollover_upload_does_not_lock_or_overwrite_current_round(self):
        import datetime as dt
        import threading
        from concurrent.futures import ThreadPoolExecutor
        from unittest.mock import patch
        from lan_bitable_template_portal.daily_inspection import CN, NS
        self.svc.rollover_due('D',dt.datetime(2026,11,3,12,tzinfo=CN))
        next_round=self.svc.change('a','甲',DAY,'start',{'duty':DAY})['active']
        entered,release=threading.Event(),threading.Event()
        original=self.svc.finish
        def delayed(*args):
            entered.set()
            if not release.wait(5):raise RuntimeError('test gate timed out')
            return original(*args)
        with patch.object(self.svc,'finish',side_effect=delayed),ThreadPoolExecutor(max_workers=1) as pool:
            task=pool.submit(self.svc.retry_rollovers,'D')
            try:
                self.assertTrue(entered.wait(2))
                acquired=self.svc.locks['D'].acquire(timeout=.5)
                self.assertTrue(acquired,'remote upload must not block editing')
                if acquired:
                    try:
                        state=self.svc.load('D');state['active']['concurrent_marker']='kept'
                        self.store.put_document(NS,'building:D',state)
                    finally:self.svc.locks['D'].release()
            finally:release.set()
            task.result(timeout=5)
        self.assertEqual(self.svc.load('D')['active']['id'],next_round['id'])
        self.assertEqual(self.svc.load('D')['active']['concurrent_marker'],'kept')

    def test_rollover_history_keeps_newest_forty(self):
        from lan_bitable_template_portal.daily_inspection import NS
        state=self.svc.load('D')
        state['history']=[{'id':str(i),'duty':DAY,'round':1,'completed':i} for i in range(240,200,-1)]
        state['rollovers']={'old':{'id':'old','duty':DAY,'round':1}}
        self.store.put_documents(NS,{'building:D':state,'rollover:old':{'active':None,'history':[{'id':'old','duty':DAY,'round':1,'completed':100}]}})
        self.svc.retry_rollovers('D')
        self.assertEqual([r['completed'] for r in self.svc.load('D')['history']],list(range(240,200,-1)))

    def test_night_rollover_preserves_previous_duty_date_across_restart(self):
        import datetime as dt
        from lan_bitable_template_portal.daily_inspection import CN, NS
        state=self.svc.load('D')
        state['active']['duty']={**DAY,'shift':'夜'}
        state['active']['round']=2
        self.store.put_document(NS,'building:D',state)
        self.svc.rollover_due('D',dt.datetime(2026,11,4,9,29,tzinfo=CN))
        self.assertIsNotNone(self.svc.load('D')['active'])
        self.svc=DailyInspection(self.store,self.cloud)
        self.svc.download_archive=lambda a:self.cloud.files[a['file_token']]
        self.svc.rollover_due('D',dt.datetime(2026,11,4,9,30,tzinfo=CN))
        self.svc.retry_rollovers('D')
        history=self.svc.load('D')['history'][0]
        self.assertEqual(history['duty'],{**DAY,'shift':'夜'})
        self.assertEqual(history['round'],2)
        self.assertEqual(self.report()['rounds'][0]['date'],DAY['date'])
        self.assertEqual(self.cloud.creates,1)

    def test_restart_between_cloud_confirmation_and_history_merge(self):
        import datetime as dt
        from unittest.mock import patch
        from lan_bitable_template_portal.daily_inspection import CN, NS
        self.svc.rollover_due('D',dt.datetime(2026,11,3,12,tzinfo=CN))
        original=self.store.put_document
        def crash(namespace,key,value):
            if namespace==NS and key=='building:D':raise RuntimeError('模拟历史合并前停机')
            return original(namespace,key,value)
        with patch.object(self.store,'put_document',side_effect=crash),self.assertLogs(level='ERROR'):
            self.svc.retry_rollovers('D')
        pending=self.store.get_document(NS,'rollover:'+self.active['id'])
        self.assertIsNone(pending['active'])
        self.assertEqual(len(self.report()['rounds']),1)
        self.svc=DailyInspection(self.store,self.cloud)
        self.svc.retry_rollovers('D')
        self.assertEqual(self.cloud.creates,1)
        self.assertEqual(len(self.svc.load('D')['history']),1)
        self.assertEqual(self.svc.load('D')['rollovers'],{})

    def test_excluded_auxiliary_ranges_keep_history_but_reject_current_edits(self):
        from lan_bitable_template_portal.daily_inspection_original import cell_excluded
        t=next(t for t in original_templates() if t['scope']=='D' and t['profession']=='暖通')
        aux=next(s for s in t['sheets'] if s['name']=='辅助区空调巡检表 (2)')
        self.assertTrue(cell_excluded(aux,'I14'))
        self.assertTrue(cell_excluded(aux,'L23'))
        self.assertTrue(cell_excluded(aux,'A24'))
        self.assertFalse(cell_excluded(aux,'H16'))
        e=next(t for t in original_templates() if t['scope']=='E' and t['profession']=='暖通')
        aux3=next(s for s in e['sheets'] if s['name']=='辅助区空调巡检表 (3)')
        self.assertFalse(cell_excluded(aux3,'I16'))
        self.assertTrue(cell_excluded(aux3,'A24'))
        with self.assertRaises(RiskError):
            self.svc.change('a','甲',DAY,'draft',{'id':self.active['id'],'review_version':0,'sheet_cells':[{'template':t['name'],'sheet':aux['name'],'cell':'I16','value':'20'}]})
        self.assertEqual(self.svc.load('D')['active'].get('review_version',0),0)

    def test_three_personal_orders_are_account_isolated_and_conflict_checked(self):
        from lan_bitable_template_portal.workspace_layout_routes import validate_layout
        from lan_bitable_template_portal.portal_service import PortalConflictError
        value={'version':1,'orders':{'ALL':['a','b'],'电气':['b'],'暖通':['a']}}
        validate_layout('inspection-sheets-D',value)
        self.store.save_workspace_layout('a','desktop','inspection-sheets-D',value,0)
        self.assertEqual(self.store.get_workspace_layout('a','desktop','inspection-sheets-D')['value'],value)
        self.assertIsNone(self.store.get_workspace_layout('b','desktop','inspection-sheets-D')['value'])
        self.assertIsNone(self.store.get_workspace_layout('a','desktop','inspection-sheets-E')['value'])
        with self.assertRaises(PortalConflictError):
            self.store.save_workspace_layout('a','desktop','inspection-sheets-D',{'version':1,'orders':{'ALL':['b','a'],'电气':['b'],'暖通':['a']}},0)

if __name__=='__main__':unittest.main()

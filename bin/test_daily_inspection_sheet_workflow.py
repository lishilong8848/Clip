import copy
import json
import tempfile
import unittest
from pathlib import Path
from lan_bitable_template_portal.daily_inspection import DailyInspection, inspection_report, original_templates, RiskError
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
        restored=DailyInspection(self.store,self.cloud)
        self.assertEqual(restored.change('a','甲',DAY,'draft',body)['active']['review_version'],saved['review_version'])
        with self.assertRaisesRegex(RiskError,'逐页'):
            self.svc.change('a','甲',DAY,'finish',{'id':self.active['id'],'review_version':saved['review_version'],'sheet_workflow':True,'visited_sheets':[]})
        self.assertEqual(self.cloud.creates,0)
        self.finish(saved['review_version'])
        archived=self.report()['rounds'][0]
        self.assertEqual(next(x['value'] for x in archived['original_cells'] if all(x[k]==edit[k] for k in ('template','sheet','cell'))),'123')
    def test_history_cells_retry_loss_and_free_cell_preserved(self):
        c=self.target();self.finish(0);r=self.report()['rounds'][0]
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
        with self.assertRaisesRegex(RiskError,'已被修改'):
            self.svc.edit_archive('D',r['id'],{**body,'cells':[{**edits[0],'value':'999'}]},'丙')
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

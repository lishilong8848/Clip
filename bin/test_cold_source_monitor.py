import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from lan_bitable_template_portal import cold_source_monitor as monitor
from lan_bitable_template_portal.state_store import LanPortalStateStore


class Cloud:
    def table_path(self,base,table): return f'{base}/{table}'
    def request(self,method,path,**kwargs):
        return {'has_more':False,'total':1,'items':[{'record_id':'rec1','fields':{'日期':1793772001000,'楼栋':'E楼',
            '天气湿球温度':0,'冷源运行模式':'预冷模式','是否需要提示':{'type':1,'value':[{'text':'是'}]}}}]}


class ColdSourceTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.store=LanPortalStateStore(Path(self.tmp.name)/'state.sqlite3')

    def test_formula_and_zero_are_read(self):
        row=monitor.snapshot(Cloud())[0]
        self.assertEqual(row['temperature'],0)
        self.assertEqual(row['hint'],'是')
        self.assertTrue(monitor.actionable(row))

    def test_baseline_and_changed_records_are_deduplicated(self):
        first=monitor.snapshot(Cloud())
        monitor.observe(self.store,first)
        self.assertEqual(self.store.list_documents(monitor.NS,key_prefix='job:'),[])
        second=[{**first[0],'temperature':18}]
        monitor.observe(self.store,second);monitor.observe(self.store,second)
        jobs=self.store.list_documents(monitor.NS,key_prefix='job:')
        self.assertEqual(len(jobs),1)
        self.assertEqual(jobs[0]['payload']['row']['temperature'],18)
        monitor.observe(self.store,first)
        self.assertEqual(len(self.store.list_documents(monitor.NS,key_prefix='job:')),2)

    def test_lease_blocks_competing_worker(self):
        monitor.observe(self.store,monitor.snapshot(Cloud()))
        row=monitor.snapshot(Cloud())[0];row['temperature']=18
        monitor.observe(self.store,[row]);key=self.store.list_documents(monitor.NS,key_prefix='job:')[0]['key']
        job=monitor.claim(self.store,key)
        self.assertIsNotNone(job);self.assertIsNone(monitor.claim(self.store,key))
        monitor.save_claim(self.store,job,release=True)
        self.assertIsNotNone(monitor.claim(self.store,key))

    def test_false_or_incomplete_does_not_send(self):
        row=monitor.snapshot(Cloud())[0]
        for item in ({**row,'hint':'否'},{**row,'temperature':None},{**row,'mode':''}):
            self.assertFalse(monitor.actionable(item))
        self.assertTrue(monitor.actionable({**row,'hint':'请关注冷源运行模式是否需要切换'}))

    def test_retry_does_not_resend_successful_recipient(self):
        monitor.observe(self.store,monitor.snapshot(Cloud()))
        row=monitor.snapshot(Cloud())[0];row['temperature']=18; monitor.observe(self.store,[row])
        recipients=[{'id':'ou_A','name':'A楼长'},{'id':'ou_B','name':'值班账号'}]
        calls=[]
        def send(card,identities,*,message_uuid):
            identity=identities[0];calls.append(identity)
            if identity=='ou_A' and calls.count('ou_A')==1: return False,'暂不可用',[]
            return True,'',[{'open_id':identity,'ok':True,'message_id':'om_1','chat_id':'oc_1','sender_app_id':'cli_1','message_uuid':message_uuid}]
        with patch.object(monitor,'snapshot',return_value=[row]), patch.object(monitor,'recipients',return_value=(recipients,False)), \
             patch('lan_bitable_template_portal.feishu_delivery.current_recipient_open_id',side_effect=lambda runtime,identity,app:identity), \
             patch('upload_event_module.services.robot_webhook.current_app_credentials',return_value=('cli_1','secret')), \
             patch('lan_bitable_template_portal.daily_inspection_reminders.saved_receipt',return_value=None), \
             patch('lan_bitable_template_portal.daily_inspection_reminders.delivery_route',return_value={}):
            monitor.process(self.store,Cloud(),object(),send)
            monitor.process(self.store,Cloud(),object(),send)
        self.assertEqual(calls,['ou_A','ou_B','ou_A'])
        job=self.store.list_documents(monitor.NS,key_prefix='job:')[0]['payload']
        self.assertEqual(job['status'],'sent')

    def test_outdated_alert_is_not_sent(self):
        first=monitor.snapshot(Cloud());monitor.observe(self.store,first)
        row={**first[0],'temperature':18};monitor.observe(self.store,[row])
        changed={**row,'hint':''}
        with patch.object(monitor,'snapshot',return_value=[changed]),patch.object(monitor,'recipients') as resolve:
            monitor.process(self.store,Cloud(),object(),lambda *args,**kwargs:None)
        resolve.assert_not_called()
        self.assertEqual(self.store.list_documents(monitor.NS,key_prefix='job:')[0]['payload']['status'],'superseded')

    def test_old_unconfirmed_delivery_never_blindly_resends(self):
        monitor.observe(self.store,monitor.snapshot(Cloud()))
        row=monitor.snapshot(Cloud())[0];row['temperature']=18;monitor.observe(self.store,[row])
        doc=self.store.list_documents(monitor.NS,key_prefix='job:')[0]
        job=doc['payload'];job['targets']={'ou_A':{'id':'ou_A','name':'楼长','status':'pending','attempted_at':1}}
        self.store.put_document(monitor.NS,doc['key'],job)
        with patch.object(monitor,'snapshot',return_value=[row]), patch.object(monitor,'recipients',return_value=([{'id':'ou_A','name':'楼长'}],False)), \
             patch('lan_bitable_template_portal.daily_inspection_reminders.saved_receipt',return_value=None), \
             patch('lan_bitable_template_portal.daily_inspection_reminders.delivery_route',return_value={}), \
             patch('upload_event_module.services.robot_webhook.current_app_credentials',return_value=('cli_1','secret')):
            monitor.process(self.store,Cloud(),object(),lambda *args,**kwargs:self.fail('结果不明不得重发'))
        self.assertEqual(self.store.list_documents(monitor.NS,key_prefix='job:')[0]['payload']['status'],'uncertain')

    def test_invalid_receipt_is_not_reported_as_sent(self):
        monitor.observe(self.store,monitor.snapshot(Cloud()))
        row=monitor.snapshot(Cloud())[0];row['temperature']=18;monitor.observe(self.store,[row])
        with patch.object(monitor,'snapshot',return_value=[row]), patch.object(monitor,'recipients',return_value=([{'id':'ou_A','name':'楼长'}],False)), \
             patch('lan_bitable_template_portal.feishu_delivery.current_recipient_open_id',side_effect=lambda runtime,identity,app:identity), \
             patch('upload_event_module.services.robot_webhook.current_app_credentials',return_value=('cli_1','secret')), \
             patch('lan_bitable_template_portal.daily_inspection_reminders.saved_receipt',return_value=None), \
             patch('lan_bitable_template_portal.daily_inspection_reminders.delivery_route',return_value={}):
            monitor.process(self.store,Cloud(),object(),lambda *args,**kwargs:(True,'ok',[{'open_id':'ou_A','ok':True}]))
        self.assertEqual(self.store.list_documents(monitor.NS,key_prefix='job:')[0]['payload']['status'],'pending')

    def test_api_filters_scopes_and_paginates_history(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from types import SimpleNamespace
        class Controller:
            def _current_session(self, request): return {'user':{'open_id':'ou_test'}}
        auth=SimpleNamespace(session_scopes=lambda session:['E'])
        runtime=SimpleNamespace(state_store=self.store,auth_manager=auth)
        app=FastAPI();monitor.install_cold_source_monitor(app,Controller(),runtime)
        row=monitor.snapshot(Cloud())[0]
        for i in range(55):
            self.store.put_document(monitor.NS,'job:'+str(i),{'id':str(i),'row':row,'created_at':i,'status':'sent'})
        self.store.put_document(monitor.NS,'job:other',{'id':'other','row':{**row,'scope':'D'},'created_at':99,'status':'sent'})
        with patch.object(monitor,'snapshot',return_value=[row,{**row,'id':'rec2','scope':'D'}]), \
             patch.object(monitor,'allowed_modules',return_value=['daily']):
            response=TestClient(app).get('/api/cold-source-monitor?page=2')
        self.assertEqual(response.status_code,200)
        data=response.json()['data']
        self.assertEqual((len(data['items']),data['history_total'],len(data['history'])),(1,55,5))
        self.assertEqual(data['items'][0]['scope'],'E')


if __name__=='__main__': unittest.main()

import base64
import hashlib
import hmac
import json
import tempfile
import unittest
import threading
from contextvars import ContextVar
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from lan_bitable_template_portal.dingtalk_notices import DingTalkNotices,DingTalkTransport,DingTalkError,NS,CHANNEL,_SecretFilter
from lan_bitable_template_portal.state_store import LanPortalStateStore


class FakeTransport:
    def __init__(self):self.calls=[];self.results=[]
    def send(self,token,secret,text,mention=None):
        self.calls.append((token,secret,text,mention))
        result=self.results.pop(0) if self.results else {'state':'sent','reason':''}
        if isinstance(result,Exception):raise result
        return result


class DingTalkTests(unittest.TestCase):
    def setUp(self):
        folder=self.enterContext(tempfile.TemporaryDirectory());self.store=LanPortalStateStore(Path(folder)/'state.sqlite3')
        self.transport=FakeTransport();self.service=DingTalkNotices(self.store,self.transport)
        self.runtime=SimpleNamespace(_notice_robot_impersonation=lambda _:nullcontext(),_event_robot_retry_guard=lambda *a,**kw:('',''),service=SimpleNamespace(get_job=lambda _:{}))

    def robot(self,char='a'):
        return {'name':'测试机器人','remark':'仅测试备注，不能发到群里','enabled':True,'scopes':[],'types':[],
                'webhook':'https://oapi.dingtalk.com/robot/send?access_token='+char*64,'secret':'SEC'+'s'*64}

    def complete(self):
        targets=self.service.snapshot('维保通告',{'buildings':['南通D楼'],'dingtalk_robot_ids':[r['id'] for r in self.service.config()['robots']]})
        self.store.begin_notice_remote_operation(operation_id='op',operation_type='start',request={'text':'测试通告','dingtalk_robot_ids':[t['id'] for t in targets]})
        self.store.mark_notice_remote_operation('op',status='completed',result={'robot_notice_type':'维保通告',
            'robot_payload':{'text':'【维保通告】状态：开始\n隔离测试正文'},'dingtalk_targets':targets})
        return self.store.list_outbox_events(CHANNEL)[0]['id']

    def run_again(self,event_id):
        self.store.mark_outbox_event(event_id,'pending',count_attempt=False);self.service.process(self.runtime)

    def test_multiple_robots_remark_secret_masking_and_config_conflict(self):
        result=self.service.save({'revision':0,'robots':[self.robot(),self.robot('b')]})
        public=json.dumps(result,ensure_ascii=False)
        self.assertIn('仅测试备注',public);self.assertNotIn('s'*64,public);self.assertNotIn('a'*64,public)
        row=result['robots'][0];row['remark']='新备注'
        saved=self.service.save({'revision':1,'robots':result['robots']})
        self.assertEqual(saved['robots'][0]['remark'],'新备注');self.assertEqual(saved['robots'][0]['revision'],row['revision'])
        with self.assertRaises(DingTalkError):self.service.save({'revision':0,'robots':[]})
        with self.assertRaises(DingTalkError):self.service.save({'revision':2,'robots':[self.robot(),self.robot()]})
        bad=self.robot();bad['webhook']='http://127.0.0.1/robot/send?access_token='+'a'*64
        with self.assertRaises(DingTalkError):self.service.save({'revision':2,'robots':[bad]})

    def test_qt_operations_cannot_enqueue_or_resume_dingtalk_delivery(self):
        self.service.save({'revision':0,'robots':[self.robot()]})
        operation='qt_notice:isolated'
        self.store.begin_notice_remote_operation(operation_id=operation,operation_type='start',request={})
        targets=self.service.snapshot('维保通告',{'dingtalk_robot_ids':[self.service.config()['robots'][0]['id']]})
        self.store.mark_notice_remote_operation(operation,status='completed',result={'robot_notice_type':'维保通告',
            'robot_payload':{'text':'隔离文本'},'dingtalk_targets':targets})
        self.assertEqual(self.store.list_outbox_events(CHANNEL),[])
        # A queued item from an older installation is discarded without sending.
        event=self.store.enqueue_outbox_event(CHANNEL,{'operation_id':operation,'idempotency_key':'qt-old'})
        self.service.process(self.runtime)
        self.assertEqual(self.transport.calls,[])
        self.assertTrue(any(item['id']==event for item in self.store.list_outbox_events(CHANNEL,status='done')))

    def test_mismatched_selected_robots_never_send_to_any_group(self):
        saved=self.service.save({'revision':0,'robots':[self.robot(),self.robot('b'),self.robot('c')]})
        ids=[r['id'] for r in saved['robots']]
        targets=self.service.snapshot('变更通告',{'dingtalk_robot_ids':ids})
        self.store.begin_notice_remote_operation(operation_id='notice_action:fixture',operation_type='start',
            request={'dingtalk_robot_ids':ids[:2]})
        self.store.mark_notice_remote_operation('notice_action:fixture',status='completed',result={
            'robot_notice_type':'变更通告','robot_payload':{'text':'隔离通告'},'dingtalk_targets':targets})
        self.service.process(self.runtime)
        self.assertFalse(self.transport.calls)
        result=self.store.get_notice_remote_operation('notice_action:fixture')['result']
        self.assertEqual(result['dingtalk_delivery_state'],'failed')
        self.assertIn('本次勾选不一致',result['dingtalk_selection_error'])

    def test_mentions_are_private_frozen_per_robot_and_only_on_final_segment(self):
        robot=self.robot();robot.update(mention_mode='people',mention_user_ids=['employee_1'],mention_mobiles=['13800138000'])
        saved=self.service.save({'revision':0,'robots':[robot]})
        identity=saved['robots'][0]['id'];target=self.service.snapshot('变更通告',{'dingtalk_robot_ids':[identity]})[0]
        self.assertNotIn('13800138000',json.dumps(self.service.config(),ensure_ascii=False))
        text='巡检'*3000
        first=self.service.deliver('mention-fixture',target,text,operation_id='test:fixture')
        self.assertEqual(first['state'],'pending');self.assertIsNone(self.transport.calls[0][3])
        saved['robots'][0]['mention_mode']='all';self.service.save(saved)
        self.store.put_document(NS,'rate:'+identity,{'next_at':0})
        first['next_at']=0;self.store.put_document(NS,'delivery:mention-fixture',first)
        last=self.service.deliver('mention-fixture',target,text,operation_id='test:fixture')
        self.assertEqual(last['state'],'sent');self.assertEqual(len(self.transport.calls),2)
        self.assertEqual(self.transport.calls[-1][3],{'mode':'people','user_ids':['employee_1'],'mobiles':['13800138000']})
        self.assertNotIn('13800138000',json.dumps(self.store.get_document(NS,'delivery:mention-fixture'),ensure_ascii=False))

    def test_transport_renders_at_fields_and_keeps_original_body(self):
        captured=[]
        def send(*args,**kwargs):captured.append(kwargs['json']);return SimpleNamespace(status_code=200,json=lambda:{'errcode':0})
        with patch('lan_bitable_template_portal.dingtalk_notices._http',return_value=SimpleNamespace(request=send)),patch('lan_bitable_template_portal.portal_service.external_real_write_guard',return_value={'real_write_allowed':True}):
            transport=DingTalkTransport()
            transport.send('token','secret','原始通告',mention={'mode':'people','user_ids':['u01'],'mobiles':['13800138000']})
            transport.send('token','secret','原始通告',mention={'mode':'all'})
            transport.send('token','secret','原始通告')
        self.assertEqual(captured[0]['at'],{'atUserIds':['u01'],'atMobiles':['13800138000'],'isAtAll':False})
        self.assertEqual(captured[0]['text']['content'],'原始通告\n@u01 @13800138000')
        self.assertEqual(captured[1]['at'],{'isAtAll':True});self.assertEqual(captured[1]['text']['content'],'原始通告\n@所有人')
        self.assertNotIn('at',captured[2]);self.assertEqual(captured[2]['text']['content'],'原始通告')

    def test_building_mentions_are_frozen_deduplicated_and_do_not_guess(self):
        robot=self.robot();robot.update(mention_mode='people',mention_user_ids=['fixture'],
            mention_mobiles=['13800138000'],mention_building_mobiles={
                'D':['13800138001','13800138000'],'E':['13800138002','13800138001']})
        saved=self.service.save({'revision':0,'robots':[robot]});identity=saved['robots'][0]['id']
        def snapshot(buildings):
            target=self.service.snapshot('维保通告',{'dingtalk_robot_ids':[identity],
                'buildings':['D','E'],'mention_buildings':buildings})[0]
            return target,self.service._mention(target['mention_cipher'])
        d_target,d=snapshot(['D楼']);_,e=snapshot(['E']);_,both=snapshot(['D','E'])
        self.assertEqual(d['mobiles'],['13800138000','13800138001'])
        self.assertEqual(e['mobiles'],['13800138000','13800138002','13800138001'])
        self.assertEqual(both['mobiles'],['13800138000','13800138001','13800138002'])
        unknown,u=snapshot(['ALL']);self.assertEqual(u['mobiles'],['13800138000'])
        self.assertIn('未识别通告楼栋',unknown['mention_warning'])
        self.assertNotIn('13800138001',json.dumps(self.service.config(),ensure_ascii=False))
        saved['robots'][0]['mention_building_mobiles']={'D':['13800138003']}
        self.service.save(saved)
        self.service.deliver('building-fixture',d_target,'隔离正文',operation_id='test:fixture')
        self.assertEqual(self.transport.calls[-1][3],d)

    def test_disabling_mentions_preserves_building_configuration(self):
        robot=self.robot();robot.update(mention_mode='people',mention_building_mobiles={'D':['13800138001']})
        saved=self.service.save({'revision':0,'robots':[robot]})
        saved['robots'][0]['mention_mode']='none';saved=self.service.save(saved)
        self.assertEqual(saved['robots'][0]['mention_building_mobiles'],{'D':['13800138001']})
        saved['robots'][0]['mention_mode']='people';saved=self.service.save(saved)
        self.assertEqual(saved['robots'][0]['mention_building_mobiles'],{'D':['13800138001']})

    def test_manual_retry_keeps_original_building_audience(self):
        robot=self.robot();robot.update(mention_mode='people',mention_building_mobiles={'D':['13800138001']})
        saved=self.service.save({'revision':0,'robots':[robot]})
        self.complete()
        self.transport.results=[{'state':'failed','reason':'隔离测试失败'},{'state':'sent','reason':''}]
        self.service.process(self.runtime)
        delivery=self.service.recent()[0]
        self.assertEqual(delivery['state'],'failed')
        saved['robots'][0]['mention_building_mobiles']={'D':['13800138002']}
        self.service.save(saved)
        self.service.retry(delivery['id'])
        self.store.put_document(NS,'rate:'+saved['robots'][0]['id'],{'next_at':0})
        self.service.process(self.runtime)
        self.assertEqual(self.transport.calls[-1][3]['mobiles'],['13800138001'])
        self.assertEqual(self.service.recent()[0]['state'],'sent')

    def test_atomic_enqueue_independent_robots_and_no_repeat_or_remark(self):
        self.service.save({'revision':0,'robots':[self.robot(),self.robot('b')]});event_id=self.complete()
        self.transport.results=[{'state':'failed','reason':'模拟权限拒绝'},{'state':'sent','reason':''}]
        self.service.process(self.runtime);self.run_again(event_id)
        self.assertEqual(len(self.transport.calls),2)
        self.assertTrue(all('备注' not in call[2] for call in self.transport.calls))
        self.run_again(event_id);self.assertEqual(len(self.transport.calls),2)
        self.assertEqual({d['state'] for d in self.service.recent()},{'sent','failed'})
        self.assertEqual(self.store.get_notice_remote_operation('op')['status'],'completed')

    def test_selected_robots_start_concurrently_and_failed_peer_does_not_repeat_success(self):
        self.service.save({'revision':0,'robots':[self.robot(),self.robot('b')]})
        event_id=self.complete()
        both_started=threading.Barrier(2,timeout=5)
        identity=ContextVar('dingtalk_test_identity',default='missing')
        context_token=identity.set('authorized')
        self.addCleanup(identity.reset,context_token)
        seen=[]
        def send(token,secret,text):
            seen.append((token,identity.get()))
            both_started.wait()
            return {'state':'failed','reason':'模拟拒绝'} if token.startswith('a') else {'state':'sent','reason':''}
        self.transport.send=send
        self.service.process(self.runtime)
        self.assertEqual(len(seen),2)
        self.assertTrue(all(value=='authorized' for _,value in seen))
        self.assertEqual({row['state'] for row in self.service.recent()},{'sent','failed'})
        self.run_again(event_id)
        self.assertEqual(len(seen),2)
        success=next(row['payload'] for row in self.store.list_documents(NS,key_prefix='delivery:') if row['payload']['state']=='sent')
        self.assertGreater(success['first_sent_at'],0)

    def test_worker_exception_does_not_prevent_other_robot_delivery(self):
        self.service.save({'revision':0,'robots':[self.robot(),self.robot('b')]})
        event_id=self.complete();calls=[]
        def send(token,secret,text):
            calls.append(token)
            if token.startswith('a'):raise RuntimeError('模拟写后进程异常')
            return {'state':'sent','reason':''}
        self.transport.send=send
        self.service.process(self.runtime)
        self.run_again(event_id)
        self.assertEqual(len(calls),2)
        self.assertEqual({row['state'] for row in self.service.recent()},{'sent','uncertain'})

    def test_crash_posting_becomes_uncertain_after_restart_without_resend(self):
        self.service.save({'revision':0,'robots':[self.robot()]});event_id=self.complete()
        self.transport.results=[RuntimeError('模拟发送后进程中断')]
        self.service.process(self.runtime)
        self.service=DingTalkNotices(self.store,self.transport);self.run_again(event_id)
        self.assertEqual(len(self.transport.calls),1)
        job=self.service.recent()[0];self.assertEqual(job['state'],'uncertain')
        with self.assertRaises(DingTalkError):self.service.retry(job['id'])

    def test_event_wait_and_disabled_robot_do_not_send(self):
        result=self.service.save({'revision':0,'robots':[self.robot()]});event_id=self.complete()
        with patch('lan_bitable_template_portal.event_priority.blockers',return_value=[{'id':'event'}]):self.service.process(self.runtime)
        self.assertFalse(self.transport.calls)
        result['robots'][0]['enabled']=False;self.service.save(result)
        self.run_again(event_id);self.assertFalse(self.transport.calls)

    def test_filters_and_partial_message_recovery(self):
        r=self.robot();r.update(scopes=['D'],types=['变更通告'])
        result=self.service.save({'revision':0,'robots':[r]})
        self.assertFalse(self.service.snapshot('变更通告',{'buildings':['E楼']}))
        self.assertFalse(self.service.snapshot('维保通告',{'buildings':['D楼']}))
        selected=[result['robots'][0]['id']]
        with self.assertRaises(DingTalkError):self.service.snapshot('变更通告',{'buildings':['E楼'],'dingtalk_robot_ids':selected})
        target=self.service.snapshot('变更通告',{'buildings':['D楼'],'dingtalk_robot_ids':selected})[0]
        data=self.service.deliver('long',target,'中'*6000)
        self.assertEqual(data['sent_parts'],1);self.assertEqual(data['state'],'pending')
        self.store.put_document(NS,'rate:'+target['id'],{'next_at':0})
        data['next_at']=0;self.store.put_document(NS,'delivery:long',data)
        data=self.service.deliver('long',target,'中'*6000)
        self.assertEqual(data['state'],'sent');self.assertEqual(len(self.transport.calls),2)
        self.service.deliver('long',target,'中'*6000);self.assertEqual(len(self.transport.calls),2)

    def test_no_selection_sends_nothing_and_only_selected_robot_is_frozen(self):
        data=self.service.save({'revision':0,'robots':[self.robot(),self.robot('b')]})
        self.assertEqual(self.service.snapshot('维保通告',{'buildings':['D楼']}),[])
        selected=data['robots'][1]['id']
        self.assertEqual([r['id'] for r in self.service.snapshot('维保通告',{'buildings':['D楼'],'dingtalk_robot_ids':[selected]})],[selected])

    def test_signing_business_receipt_and_log_redaction(self):
        captured={}
        def send(*args,**kwargs):captured.update(kwargs);return SimpleNamespace(status_code=200,json=lambda:{'errcode':0})
        with patch('lan_bitable_template_portal.dingtalk_notices._http',return_value=SimpleNamespace(request=send)),patch('lan_bitable_template_portal.dingtalk_notices.time.time',return_value=1000),patch('lan_bitable_template_portal.portal_service.external_real_write_guard',return_value={'real_write_allowed':True}):
            self.assertEqual(DingTalkTransport().send('token','secret','正文')['state'],'sent')
        expected=base64.b64encode(hmac.new(b'secret',b'1000000\nsecret',hashlib.sha256).digest()).decode()
        self.assertEqual(captured['params']['sign'],expected)
        self.assertFalse(captured['follow_redirects'])
        import logging
        record=logging.LogRecord('httpx',20,'',0,'POST https://oapi.dingtalk.com/robot/send?access_token=PRIVATE&sign=SIGNED',(),None)
        _SecretFilter().filter(record)
        self.assertNotIn('PRIVATE',record.getMessage());self.assertNotIn('SIGNED',record.getMessage())

    def test_options_and_job_receipts_hide_secrets_and_keep_admin_settings_private(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from lan_bitable_template_portal.dingtalk_notice_routes import install_dingtalk_notice_routes
        self.service.save({'revision':0,'robots':[self.robot()]})
        self.complete();self.service.process(self.runtime)
        session={'user':{'open_id':'owner'}}
        controller=SimpleNamespace(_current_session=lambda _:session,_job_visible_to_session=lambda job,s:s['user']['open_id']=='owner',_request_base_url=lambda _: 'http://testserver')
        self.runtime.auth_manager=SimpleNamespace(is_admin=lambda _:False)
        self.runtime.state_store=self.store
        self.runtime.service.get_job=lambda _: {'job_id':'job','remote_operation_id':'op','request':{'_auth_open_id':'owner','work_type':'maintenance'}}
        app=FastAPI();install_dingtalk_notice_routes(app,controller,self.runtime)
        with TestClient(app) as client,patch('lan_bitable_template_portal.module_access.allowed_modules',return_value=['maintenance']):
            result=client.get('/api/workbench/dingtalk-options?work_type=maintenance')
            self.assertEqual(result.status_code,200);self.assertNotIn('token_cipher',result.text);self.assertNotIn('s'*64,result.text)
            self.assertEqual(client.get('/api/dingtalk-notices').status_code,403)
            result=client.get('/api/jobs/job/dingtalk');self.assertEqual(result.status_code,200);self.assertEqual(result.json()['data']['state'],'sent')
            session['user']['open_id']='another-user'
            self.assertEqual(client.get('/api/jobs/job/dingtalk').status_code,403)

    def test_background_dispatch_is_nonblocking_and_does_not_start_duplicate_worker(self):
        import lan_bitable_template_portal.dingtalk_notices as module
        entered=threading.Event();release=threading.Event();calls=[]
        def blocked(runtime):calls.append(runtime);entered.set();release.wait(5)
        runtime=SimpleNamespace(state_store=self.store)
        with patch.object(module.DingTalkNotices,'process',side_effect=blocked):
            try:
                module.dispatch(runtime)
                self.assertTrue(entered.wait(2))
                module.dispatch(runtime)
                self.assertEqual(len(calls),1)
                self.assertTrue(module._dispatch_thread.is_alive())
            finally:release.set();module._dispatch_thread.join(5)


if __name__=='__main__':unittest.main()

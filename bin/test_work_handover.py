"""Isolated handover lifecycle, callback and access tests; never contacts Feishu."""
import asyncio
import copy
import datetime as dt
import json
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0,str(Path(__file__).resolve().parent))
from fastapi import FastAPI
from fastapi.testclient import TestClient
from lan_bitable_template_portal.work_handover import Handovers,HandoverError
from lan_bitable_template_portal.work_handover_cards import HandoverCards,card,parse_event
from lan_bitable_template_portal.work_handover_routes import install_work_handover_routes
from lan_bitable_template_portal.work_handover_sources import HandoverSources,explicit_participation
from lan_bitable_template_portal.module_access import required_modules


class HandoverTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.actors={name:{'id':name,'open_id':'ou_'+name,'name':'人员'+name,'role':'building','level':'L4',
            'modules':['work_handover','maintenance','repair_management'],'scope_rules':['A']} for name in ('alice','bob','eve')}
        self.a,self.b,self.e=[self.actors[k] for k in ('alice','bob','eve')]
        self.path=Path(self.temp.name)/'state.sqlite3'
        self.store=Handovers(self.path,lambda:[],lambda rid:copy.deepcopy(self.actors[rid]))

    def draft(self,identity='1'*32,source=False):
        data=self.store.create(self.a,identity)
        item={'id':'a'*32,'title':'待处理工作','notes':'检查结果及下一步','scope':'A','kind':'ongoing','source_id':''}
        if source:
            ref={'id':'f'*64,'title':'维保通告','scope':'A','module':'maintenance','period':'2026-10',
                 'source_url':'/workbench-lite?scope=A&record_id=rec_test','business_status':'进行中','source_checked_at':'2026-10-16'}
            self.store.cache_sources([ref]);item['source_id']=ref['id']
        payload={'title':'A楼工作交接','recipient_id':'bob','notes':'需继续跟进','items':[item,
            {'id':'b'*32,'title':'既往完成工作','notes':'历史结论','scope':'A','kind':'history','source_id':''}]}
        return self.store.save(self.a,data['id'],data['version'],payload)

    def pending(self,source=False):
        data=self.draft(source=source)
        return self.store.action(self.a,data['id'],data['version'],'send')

    def test_confirmation_required_and_checklist_never_writes_source(self):
        pending=self.pending(True)
        with self.assertRaises(HandoverError):
            self.store.action(self.b,pending['id'],pending['version'],'complete',item_id='a'*32)
        accepted=self.store.action(self.b,pending['id'],pending['version'],'accept')
        done=self.store.action(self.b,accepted['id'],accepted['version'],'complete',item_id='a'*32)
        self.assertTrue(done['items'][0]['done'])
        self.assertEqual(done['items'][0]['business_status'],'进行中')
        self.assertEqual(done['items'][0]['confirmed_by'],self.b['name'])
        with self.assertRaises(HandoverError):
            self.store.action(self.b,done['id'],done['version'],'complete',item_id='b'*32)
        removed=self.store.action(self.b,done['id'],done['version'],'remove',item_id='a'*32)
        self.assertTrue(removed['items'][0]['removed'])
        restored=self.store.action(self.b,removed['id'],removed['version'],'restore',item_id='a'*32)
        reopened=self.store.action(self.b,restored['id'],restored['version'],'reopen',item_id='a'*32)
        self.assertFalse(reopened['items'][0]['done'])
        self.assertEqual(len(reopened['items']),2)
        self.assertNotIn('card_token',reopened)

    def test_other_person_and_admin_cannot_accept_or_read(self):
        data=self.pending()
        for actor in [self.a,self.e,{**self.e,'level':'L0','role':'admin','scope_rules':['ALL']}]:
            with self.assertRaises(HandoverError):
                self.store.action(actor,data['id'],data['version'],'accept')
        with self.assertRaises(HandoverError):
            self.store.get(self.e,data['id'])

    def test_concurrent_web_and_card_confirmation_only_changes_once(self):
        data=self.pending()
        with ThreadPoolExecutor(max_workers=6) as pool:
            results=list(pool.map(lambda _:self.store.action(self.b,data['id'],data['version'],'accept'),range(6)))
        self.assertTrue(all(r['status']=='accepted' for r in results))
        with self.store.connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM work_handover_audit WHERE action='确认接收'").fetchone()[0],1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM work_handover_outbox WHERE kind='result'").fetchone()[0],1)

    def test_withdraw_edit_resend_invalidates_old_card_and_retains_snapshot(self):
        data=self.pending()
        with self.store.connect() as db:
            original=self.store._read(db,data['id'])
        withdrawn=self.store.action(self.a,data['id'],data['version'],'withdraw')
        with self.assertRaises(HandoverError):
            self.store.action(self.b,data['id'],data['version'],'accept',token=original['card_token'])
        saved=self.store.save(self.a,data['id'],withdrawn['version'],{'title':'新内容','notes':'补充说明','recipient_id':'bob','items':[{'id':'c'*32,'title':'新事项','scope':'A','kind':'todo','notes':'','source_id':''}]})
        sent=self.store.action(self.a,data['id'],saved['version'],'send')
        with self.assertRaises(HandoverError):
            self.store.action(self.b,data['id'],sent['version'],'accept',token=original['card_token'])
        with self.store.connect() as db:
            versions=[json.loads(r[0]) for r in db.execute('SELECT payload FROM work_handover_versions ORDER BY version')]
        self.assertEqual([v['title'] for v in versions],['A楼工作交接','新内容'])

    def test_restart_keeps_outbox_and_response_loss_keeps_one_handover(self):
        data=self.pending()
        recovered=Handovers(self.path,lambda:[],lambda rid:self.actors[rid])
        self.assertEqual(recovered.get(self.b,data['id'])['status'],'pending')
        self.assertEqual(recovered.create(self.a,data['id'])['status'],'pending')
        self.assertEqual(recovered.list(self.a,'sent')['total'],1)
        with recovered.connect() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM work_handover_outbox').fetchone()[0],1)

    def test_revoke_scope_or_module_cannot_accept_and_redacts_business(self):
        data=self.pending(True)
        for changed in [{**self.b,'scope_rules':['B']},{**self.b,'modules':['work_handover']}]:
            with self.assertRaises(HandoverError):
                self.store.action(changed,data['id'],data['version'],'accept')
            self.assertTrue(self.store.get(changed,data['id'])['items'][0]['restricted'])
        with self.assertRaises(HandoverError):
            self.store.get({**self.b,'modules':[]},data['id'])

    def test_bad_fields_stale_saves_and_cross_scope_are_rejected(self):
        data=self.draft()
        payload={'title':'X','recipient_id':'bob','notes':'','items':[{'id':'c'*32,'title':'X','scope':'B','kind':'todo','notes':'','source_id':''}]}
        with self.assertRaises(HandoverError):self.store.save(self.a,data['id'],data['version'],payload)
        payload['items'][0]['scope']='A'
        with self.assertRaises(HandoverError):self.store.save(self.a,data['id'],1,payload)
        payload['items'][0]['source_url']='https://example.com'
        with self.assertRaises(HandoverError):self.store.save(self.a,data['id'],data['version'],payload)

    def test_return_requires_reason_and_no_implicit_acceptance(self):
        data=self.pending()
        with self.assertRaises(HandoverError):self.store.action(self.b,data['id'],data['version'],'return')
        result=self.store.action(self.b,data['id'],data['version'],'return',reason='请补资料')
        self.assertEqual(result['status'],'returned')
        self.assertEqual(result['decision_reason'],'请补资料')

    def test_duplicate_pending_source_is_rejected(self):
        self.pending(True)
        other=self.draft('2'*32,True)
        with self.assertRaisesRegex(HandoverError,'另一份'):
            self.store.action(self.a,other['id'],other['version'],'send')

    def test_callback_authentication_and_card_actions(self):
        data=self.pending()
        with self.store.connect() as db:private=self.store._read(db,data['id'])
        payload={'header':{'app_id':'cli_bot','event_type':'card.action.trigger','event_id':'evt1'},'event':{
            'operator':{'open_id':'ou_bob','union_id':'on_bob'},'context':{'open_chat_id':'oc_dm','open_message_id':'om_card'},
            'action':{'value':{'kind':'work_handover','id':data['id'],'version':data['version'],'token':private['card_token'],'action':'accept'},'form_value':{}}}}
        self.assertIsNone(parse_event(payload,'cli_login'))
        message=parse_event(payload,'cli_bot')
        self.assertEqual(message['kind'],'work_handover')
        self.assertEqual(parse_event(payload,'cli_bot')['message_id'],message['message_id'])
        bot=SimpleNamespace(_resolve_user=AsyncMock(return_value={'open_id':'ou_bob'}),runtime=SimpleNamespace(work_handovers=lambda:self.store,work_handover_actor=lambda *_:self.b))
        cards=HandoverCards(bot)
        cards.tick=Mock()
        result=asyncio.run(cards.handle(message))
        self.assertIn('已确认接收',result['text'])
        self.assertNotIn('form_action_type',json.dumps(card(self.store.get(self.b,data['id']),'https://portal')))

    def test_notification_failure_is_recoverable_and_never_accepts(self):
        data=self.pending()
        bot=SimpleNamespace(runtime=SimpleNamespace(work_handovers=lambda:self.store))
        cards=HandoverCards(bot)
        cards.deliver=AsyncMock(side_effect=RuntimeError('network'))
        asyncio.run(cards.drain())
        self.assertEqual(self.store.get(self.b,data['id'])['status'],'pending')
        with self.store.connect(True) as db:
            row=db.execute('SELECT * FROM work_handover_outbox').fetchone()
            self.assertEqual(row['attempts'],1)
            db.execute('UPDATE work_handover_outbox SET next_at=0')
        cards.deliver=AsyncMock(return_value='om_delivered')
        asyncio.run(cards.drain())
        self.assertEqual(self.store.get(self.a,data['id'])['notification']['message_id'],'om_delivered')

    def test_participation_never_uses_title_or_building(self):
        self.assertFalse(explicit_participation({'title':'alice工作','scope':'A'},self.a))
        self.assertTrue(explicit_participation({'operator':{'open_id':'ou_alice'}},self.a))
        self.assertEqual(required_modules('/api/work-handovers'),{'work_handover'})
        self.assertFalse(explicit_participation({'operator':{'id':['alice']}},self.a))

    def test_delayed_cards_keep_original_version_and_receiver(self):
        data=self.pending()
        with self.store.connect(True) as db:
            db.execute("UPDATE work_handover_outbox SET state='sent',message_id='om_original'")
        withdrawn=self.store.action(self.a,data['id'],data['version'],'withdraw')
        saved=self.store.save(self.a,data['id'],withdrawn['version'],{'title':'另一份交接内容','notes':'不应发给原接收人',
            'recipient_id':'eve','items':[{'id':'c'*32,'title':'新资料','scope':'A','kind':'reference','notes':'','source_id':''}]})
        self.store.action(self.a,data['id'],saved['version'],'send')
        with self.store.connect() as db:
            refresh=dict(db.execute("SELECT * FROM work_handover_outbox WHERE kind='refresh'").fetchone())
        client=SimpleNamespace(patch=AsyncMock(return_value=SimpleNamespace(json=lambda:{'code':0})))
        bot=SimpleNamespace(messenger=SimpleNamespace(client=AsyncMock(return_value=client),headers=AsyncMock(return_value={})),
            runtime=SimpleNamespace(service=SimpleNamespace(_critical_guard_public_base_url=lambda:'https://portal')))
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard',return_value={'real_write_allowed':True}):
            mid=asyncio.run(HandoverCards(bot).deliver(self.store,refresh))
        self.assertEqual(mid,'om_original')
        call=client.patch.call_args
        self.assertTrue(call.args[0].endswith('/om_original'))
        content=call.kwargs['json']['content']
        self.assertIn('已撤回',content)
        self.assertIn('人员bob',content)
        self.assertNotIn('另一份交接内容',content)
        self.assertNotIn('form_action_type',content)

    def test_bot_uses_app_scoped_verified_binding_not_login_open_id(self):
        directory=Mock(return_value=[{'key':'cli_bot:ou_botBob','payload':{'open_id':'ou_bob','record_id':'bob'}}])
        union=Mock(return_value='on_bob')
        bot=SimpleNamespace(config={'app_id':'cli_bot'},runtime=SimpleNamespace(state_store=SimpleNamespace(list_documents=directory)),
            permission_cards=SimpleNamespace(union_id=union))
        cards=HandoverCards(bot)
        self.assertEqual(cards.recipient_id(self.b),('open_id','ou_botBob'))
        union.assert_not_called()
        directory.return_value=[{'key':'cli_bot:ou_wrong','payload':{'open_id':'ou_bob','record_id':'eve'}}]
        self.assertEqual(cards.recipient_id(self.b),('union_id','on_bob'))

    def test_sources_skip_missing_ids_and_preserve_zero_reading(self):
        from lan_bitable_template_portal.portal_service import MaintenancePortalService
        period=__import__('datetime').datetime.now().strftime('%Y-%m')
        service=SimpleNamespace(ensure_snapshot_loaded=lambda:None,_source_snapshot_exists=lambda _:True,
            get_daily_summary=lambda **_:{'items':[]},_record_work_type=lambda _: 'maintenance',
            _source_record_progress=MaintenancePortalService._maintenance_status_value,
            _target_status_is_finished=MaintenancePortalService._target_status_is_finished,
            _source_progress_is_ongoing=MaintenancePortalService._source_progress_is_ongoing,
            _workbench_records=lambda **_:[{'display_fields':{'维护总项':'缺记录ID的事项'}}])
        catalog=HandoverSources(SimpleNamespace(service=service),SimpleNamespace(_get_ongoing=lambda _:[]),self.store)
        result=catalog.load(self.a,'maintenance','A',period)
        self.assertEqual(result['items'],[])
        self.assertIn('稳定标识',result['warnings'][0])
        service.list_water_consumption_records=lambda **_:{'records':[{'record_id':'rec1','meter_name':'水表1','meter_value':0},
            {'record_id':'rec2','meter_name':'水表2','meter_value':''}],'total':2}
        result=catalog.load({**self.a,'modules':self.a['modules']+['water']},'water','A',period)
        self.assertEqual([r['kind'] for r in result['items']],['history','todo'])

    def test_archive_and_outbox_roll_back_together_on_database_error(self):
        draft=self.draft()
        with patch.object(self.store,'_queue',side_effect=RuntimeError('storage fault')):
            with self.assertRaises(RuntimeError):self.store.action(self.a,draft['id'],draft['version'],'send')
        self.assertEqual(self.store.get(self.a,draft['id'])['status'],'draft')
        with self.store.connect() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM work_handover_versions').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM work_handover_outbox').fetchone()[0],0)

    def test_send_resolves_directory_outside_shared_database_write_lock(self):
        import sqlite3
        draft=self.draft()
        def resolve(person_id):
            db=sqlite3.connect(self.path,timeout=0)
            try:
                db.execute('BEGIN IMMEDIATE')
                db.rollback()
            finally:
                db.close()
            return self.actors[person_id]
        self.store.actor_by_person=resolve
        self.assertEqual(self.store.action(self.a,draft['id'],draft['version'],'send')['status'],'pending')

    def test_source_plan_status_uses_existing_business_parser(self):
        from lan_bitable_template_portal.portal_service import MaintenancePortalService
        real=object.__new__(MaintenancePortalService)
        statuses=['进行中','已完成','已结束','延期未开始','未开始','']
        rows=[{'record_id':'plan'+str(n),'work_type':'maintenance','display_fields':{'维护总项':'项目'+str(n),'维护实施状态':s}} for n,s in enumerate(statuses)]
        service=SimpleNamespace(ensure_snapshot_loaded=lambda:None,_source_snapshot_exists=lambda _:True,
            get_daily_summary=lambda **_:{'items':[]},_record_work_type=real._record_work_type,
            _source_record_progress=real._source_record_progress,_target_status_is_finished=real._target_status_is_finished,
            _source_progress_is_ongoing=real._source_progress_is_ongoing,_workbench_records=lambda **_:rows)
        catalog=HandoverSources(SimpleNamespace(service=service),SimpleNamespace(_get_ongoing=lambda _:[]),self.store)
        period=dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).strftime('%Y-%m')
        data=catalog.load(self.a,'maintenance','A',period)
        self.assertEqual([r['kind'] for r in data['items']],['ongoing','history','history','todo','todo','todo'])
        self.assertEqual([r['business_status'] for r in data['items']],statuses[:-1]+['状态待核对'])

    def test_active_source_cannot_be_sent_twice_or_restored_after_transfer(self):
        first=self.pending(True)
        first=self.store.action(self.b,first['id'],first['version'],'accept')
        second=self.draft('2'*32,True)
        fields=('id','title','notes','scope','kind','source_id')
        second=self.store.save(self.a,second['id'],second['version'],{'title':second['title'],'notes':'转交给其他人','recipient_id':'eve',
            'items':[{k:i.get(k,'') for k in fields} for i in second['items']]})
        with self.assertRaisesRegex(HandoverError,'接收人：人员bob'):
            self.store.action(self.a,second['id'],second['version'],'send')
        first=self.store.action(self.b,first['id'],first['version'],'remove',item_id='a'*32)
        second=self.store.action(self.a,second['id'],second['version'],'send')
        second=self.store.action(self.e,second['id'],second['version'],'accept')
        with self.assertRaisesRegex(HandoverError,'另一份'):
            self.store.action(self.b,first['id'],first['version'],'restore',item_id='a'*32)
        self.assertTrue(self.store.get(self.b,first['id'])['items'][0]['removed'])
        second=self.store.action(self.e,second['id'],second['version'],'complete',item_id='a'*32)
        restored=self.store.action(self.b,first['id'],first['version'],'restore',item_id='a'*32)
        self.assertFalse(restored['items'][0]['removed'])
        with self.assertRaises(HandoverError):
            self.store.action(self.e,second['id'],second['version'],'reopen',item_id='a'*32)

    def test_concurrent_sends_for_same_source_reserve_only_one_checklist(self):
        drafts=[self.draft('1'*32,True),self.draft('2'*32,True)]
        def send(d):
            try:return self.store.action(self.a,d['id'],d['version'],'send')['status']
            except HandoverError as exc:return exc.status
        with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(send,drafts))
        self.assertCountEqual(results,['pending',409])

    def test_result_notifications_recover_after_six_failures_without_accepting_twice(self):
        data=self.pending()
        with self.store.connect(True) as db:db.execute("UPDATE work_handover_outbox SET state='sent',message_id='om_invite'")
        accepted=self.store.action(self.b,data['id'],data['version'],'accept')
        cards=HandoverCards(SimpleNamespace(runtime=SimpleNamespace(work_handovers=lambda:self.store)))
        cards.deliver=AsyncMock(side_effect=RuntimeError('offline'))
        with patch('lan_bitable_template_portal.work_handover_cards.logging.warning'):
            for _ in range(6):
                with self.store.connect(True) as db:db.execute('UPDATE work_handover_outbox SET next_at=0')
                asyncio.run(cards.drain())
        self.assertCountEqual(self.store.get(self.a,data['id'])['notification_failures'],['result','refresh'])
        cards.deliver=AsyncMock(return_value='om_recovered')
        asyncio.run(cards.drain())
        self.assertEqual(cards.deliver.await_count,0,'Backoff prevents frequent failed requests')
        with patch('lan_bitable_template_portal.work_handover_cards.time.time',return_value=10**12):
            asyncio.run(cards.drain())
        self.assertEqual(cards.deliver.await_count,2)
        current=self.store.get(self.a,data['id'])
        self.assertEqual(current['notification_failures'],[])
        self.assertEqual(current['version'],accepted['version'])
        self.assertEqual(sum(a['action']=='确认接收' for a in current['audit']),1)

    def test_participants_can_retry_only_failed_notifications_not_resend_business(self):
        data=self.pending()
        data=self.store.action(self.b,data['id'],data['version'],'accept')
        for actor in [self.a,self.b]:
            with self.store.connect(True) as db:
                db.execute("UPDATE work_handover_outbox SET state='failed',attempts=6 WHERE kind IN ('result','refresh')")
            retry=self.store.action(actor,data['id'],data['version'],'retry-notification')
            self.assertEqual(retry['status'],'accepted')
            self.assertEqual(retry['version'],data['version'])
            self.assertEqual(retry['notification_failures'],[])
        with self.assertRaises(HandoverError):self.store.action(self.e,data['id'],data['version'],'retry-notification')
        with self.assertRaises(HandoverError):self.store.action(self.a,data['id'],data['version'],'retry-notification')


class HandoverRouteTests(HandoverTests):
    def setUp(self):
        super().setUp()
        people=[{'record_id':a['id'],'name':a['name'],'open_id':a['open_id'],'selectable':True,'scopes':['A'],'employee_no':'fixture'} for a in self.actors.values()]
        def actor(oid):return self.actors[oid.removeprefix('ou_')]
        auth=SimpleNamespace(account_profile=lambda oid:actor(oid),directory_person=lambda oid:{**actor(oid),'selectable':True},public_user=lambda s:s['user'])
        runtime=SimpleNamespace(state_store=SimpleNamespace(db_path=self.path),auth_manager=auth,service=SimpleNamespace(get_permission_directory_people=lambda:{'items':people}))
        async def body(request,**_):return await request.json()
        controller=SimpleNamespace(_current_session=lambda r:{'user':actor(r.headers.get('x-user','ou_alice'))},_read_json_request=body,_request_base_url=lambda r:'http://testserver')
        app=FastAPI()
        install_work_handover_routes(app,controller,runtime)
        self.client=TestClient(app)
        self.addCleanup(self.client.close)

    def test_origin_protection_and_file_stream_access(self):
        identity='3'*32
        response=self.client.post('/api/work-handovers',json={'id':identity},headers={'Origin':'https://attacker.test'})
        self.assertEqual(response.status_code,403)
        data=self.client.post('/api/work-handovers',json={'id':identity}).json()['data']
        response=self.client.post('/api/work-handovers/'+identity+'/files?name=note.txt',content=b'fixture text')
        self.assertEqual(response.status_code,200,response.text)
        data=response.json()['data'];fid=data['files'][0]['id']
        self.assertEqual(self.client.get('/api/work-handovers/files/'+fid).content,b'fixture text')
        self.assertEqual(self.client.get('/api/work-handovers/files/'+fid,headers={'x-user':'ou_bob'}).status_code,403)
        response=self.client.request('DELETE','/api/work-handovers/files/'+fid,json={'version':data['version']})
        self.assertEqual(response.status_code,200,response.text)
        self.assertEqual(response.json()['data']['files'],[])
        self.assertEqual(self.client.post('/api/work-handovers/'+identity+'/files?name=script.html',content=b'<script>').status_code,400)

    def test_removed_attachment_is_not_inherited_by_a_new_receiver(self):
        data=self.draft()
        identity=data['id']
        data=self.client.post('/api/work-handovers/'+identity+'/files?name=original.txt',content=b'original snapshot').json()['data']
        fid=data['files'][0]['id']
        data=self.store.action(self.a,identity,data['version'],'send')
        self.assertEqual(self.client.get('/api/work-handovers/files/'+fid,headers={'x-user':'ou_bob'}).status_code,200)
        data=self.store.action(self.b,identity,data['version'],'return',reason='补充资料')
        data=self.client.request('DELETE','/api/work-handovers/files/'+fid,json={'version':data['version']}).json()['data']
        self.assertEqual(self.client.get('/api/work-handovers/files/'+fid,headers={'x-user':'ou_bob'}).content,b'original snapshot')
        data=self.client.post('/api/work-handovers/'+identity+'/files?name=new.txt',content=b'new receiver only').json()['data']
        new_id=data['files'][0]['id']
        self.assertEqual(self.client.get('/api/work-handovers/files/'+new_id,headers={'x-user':'ou_bob'}).status_code,403)
        fields=('id','title','notes','scope','kind','source_id')
        data=self.store.save(self.a,identity,data['version'],{'title':data['title'],'notes':'','recipient_id':'eve',
            'items':[{k:i.get(k,'') for k in fields} for i in data['items']]})
        data=self.store.action(self.a,identity,data['version'],'send')
        self.assertEqual(self.client.get('/api/work-handovers/files/'+fid,headers={'x-user':'ou_eve'}).status_code,403)
        self.assertEqual(self.client.get('/api/work-handovers/files/'+new_id,headers={'x-user':'ou_eve'}).content,b'new receiver only')
        self.assertEqual(self.client.get('/api/work-handovers/files/'+new_id,headers={'x-user':'ou_bob'}).status_code,403)
        self.e['scope_rules']=['B']
        self.assertEqual(self.client.get('/api/work-handovers/files/'+new_id,headers={'x-user':'ou_eve'}).status_code,403)


if __name__=='__main__':unittest.main()

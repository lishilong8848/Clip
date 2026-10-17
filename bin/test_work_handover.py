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
from lan_bitable_template_portal.work_handover_sources import HandoverSources,explicit_participation,responsibility_modules
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
        runtime=SimpleNamespace(state_store=SimpleNamespace(db_path=self.path),auth_manager=auth,service=SimpleNamespace(get_permission_directory_people=Mock(return_value={'items':people})))
        self.runtime=runtime
        async def body(request,**_):return await request.json()
        controller=SimpleNamespace(_current_session=lambda r:{'user':actor(r.headers.get('x-user','ou_alice'))},_read_json_request=body,_request_base_url=lambda r:'http://testserver')
        app=FastAPI()
        install_work_handover_routes(app,controller,runtime)
        self.client=TestClient(app)
        self.addCleanup(self.client.close)

    def test_recipient_search_matches_name_and_employee_number_without_exposing_ids(self):
        data=self.client.get('/api/work-handovers/people?query=人员bob').json()['data']
        self.assertEqual([r['record_id'] for r in data['items']],['bob'])
        self.assertTrue(data['items'][0]['selectable'])
        self.assertNotIn('open_id',data['items'][0])
        by_number=self.client.get('/api/work-handovers/people?query=fixture').json()['data']
        self.assertCountEqual([r['record_id'] for r in by_number['items']],['bob','eve'])
        self.client.get('/api/work-handovers/people?refresh=1')
        self.runtime.service.get_permission_directory_people.assert_called_with(force_refresh=True)

    def test_recipient_without_handover_grant_is_explained_not_silently_hidden_or_granted(self):
        self.b['modules']=[]
        row=next(r for r in self.client.get('/api/work-handovers/people').json()['data']['items'] if r['record_id']=='bob')
        self.assertFalse(row['selectable'])
        self.assertIn('权限',row['unavailable_reason'])
        self.assertEqual(self.b['modules'],[])
        data=self.client.post('/api/work-handovers',json={'id':'4'*32}).json()['data']
        result=self.client.put('/api/work-handovers/'+data['id'],json={'version':data['version'],
            'data':{'title':'交接','recipient_id':'bob','notes':'','items':[]}})
        self.assertEqual(result.status_code,403)

    def test_sources_responsibilities_route_uses_trusted_actor_role(self):
        self.a.update(category='engineer',home_scope='A')
        people=self.client.get('/api/work-handovers/people').json()['data']
        self.assertEqual(people['default_modules'],['repair_management'])
        self.assertEqual(people['self']['home_scope'],'A')
        with patch.object(HandoverSources,'defaults',return_value={'items':[],'warnings':[]}) as load:
            response=self.client.get('/api/work-handovers/sources?mode=responsibilities&scope=A&period=2026-10')
            self.assertEqual(response.status_code,200)
            self.assertEqual(load.call_args.args[0]['category'],'engineer')
            self.assertEqual(load.call_args.args[1:],('A','2026-10'))

    def test_mismatched_directory_id_is_not_silently_reassigned(self):
        original=self.runtime.auth_manager.directory_person
        self.runtime.auth_manager.directory_person=lambda oid:{**original(oid),'id':'different-person'} if oid=='ou_bob' else original(oid)
        data=self.client.get('/api/work-handovers/people').json()['data']
        self.assertNotIn('bob',[r['record_id'] for r in data['items']])
        self.assertTrue(data['warnings'])
        draft=self.client.post('/api/work-handovers',json={'id':'5'*32}).json()['data']
        result=self.client.put('/api/work-handovers/'+draft['id'],json={'version':draft['version'],
            'data':{'title':'交接','recipient_id':'bob','notes':'','items':[]}})
        self.assertEqual(result.status_code,409)
        self.assertEqual(self.client.get('/api/work-handovers/'+draft['id']).json()['data']['recipient_id'],'')

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


class HandoverSourceTests(unittest.TestCase):
    def setUp(self):
        self.actor={'id':'recEngineer','open_id':'ou_fixtureEngineer','name':'工程师甲','category':'engineer',
                    'level':'L4','role':'building','scope_rules':['A'],
                    'modules':['work_handover','repair_management','risk_inspection','event','cabinet']}
        self.store=SimpleNamespace(cache_sources=Mock())
        self.service=SimpleNamespace()
        self.runtime=SimpleNamespace(service=self.service)
        self.catalog=HandoverSources(self.runtime,SimpleNamespace(),self.store)

    def test_role_defaults_do_not_change_permissions_or_claim_personal_participation(self):
        self.assertEqual(responsibility_modules(self.actor),['repair_management','risk_inspection'])
        supervisor={**self.actor,'category':'supervisor','level':'L3'}
        self.assertEqual(responsibility_modules(supervisor),['event','repair_management','risk_inspection','cabinet'])
        self.assertEqual(responsibility_modules({**self.actor,'category':'other','position':'A楼楼长'}),responsibility_modules(supervisor))
        self.assertEqual(responsibility_modules({**supervisor,'modules':[]}),[])
        self.service.get_repair_management_records=Mock(return_value={'records':[
            {'record_id':'recPending','title':'待处理维修','workflow':'未开始'},
            {'record_id':'recOngoing','title':'维修中项目','workflow':'维修中'},
            {'record_id':'recDone','title':'已完成维修','workflow':'维修完成','is_completed':True}], 'total':3})
        data=self.catalog.load(self.actor,'repair_management','A','2026-10',active_only=True)
        self.assertEqual([r['title'] for r in data['items']],['待处理维修','维修中项目'])
        self.assertTrue(all(r['responsible'] and not r['participated'] for r in data['items']))
        self.service.get_repair_management_records.assert_called_once_with(scope='A',limit=200,offset=0,period='all')
        with self.assertRaises(HandoverError):self.catalog.load(self.actor,'repair_management','B','2026-10')

    def test_repair_defaults_read_the_second_page_and_keep_unverified_state_visible(self):
        first=[{'record_id':'done'+str(i),'title':'已完成','workflow':'维修完成'} for i in range(200)]
        self.service.get_repair_management_records=Mock(side_effect=[{'records':first,'total':201},
            {'records':[{'record_id':'recUnverified','title':'待核验维修','state_locked':True,'workflow':'维修完成'}],'total':201}])
        data=self.catalog.load(self.actor,'repair_management','A','2026-10',active_only=True)
        self.assertEqual(len(data['items']),1)
        self.assertEqual(data['items'][0]['business_status'],'状态待核对')
        self.assertEqual(self.service.get_repair_management_records.call_args.kwargs['offset'],200)

    def test_default_failure_preserves_other_modules_and_is_not_reported_as_no_work(self):
        expected={'id':'a'*64,'kind':'ongoing','title':'待处理维修'}
        with patch.object(self.catalog,'load',side_effect=[{'items':[expected],'warnings':[]},RuntimeError('offline')]) as load:
            data=self.catalog.defaults(self.actor,'A','2026-10')
        self.assertEqual(data['items'],[expected])
        self.assertEqual(data['failed_modules'],['risk_inspection'])
        self.assertIn('不代表没有',data['warnings'][0])
        self.assertTrue(all(call.kwargs['active_only'] for call in load.call_args_list))
        with self.assertRaises(HandoverError):self.catalog.defaults(self.actor,'B','2026-10')
        with self.assertRaises(HandoverError):self.catalog.defaults(self.actor,'A','bad-month')

    def test_risk_inspection_uses_existing_visibility_and_keeps_each_unfinished_assignment(self):
        task={'id':'risk1','name':'月度风险排查','people':[
            {'id':self.actor['id'],'name':'工程师甲','scope':'A','items':[
                {'id':'p','title':'配电排查','status':'pending'}, {'id':'s','title':'暖通排查','status':'submitted'},
                {'id':'a','title':'已复核项','status':'approved'}, {'id':'r','title':'已撤销项','status':'revoked'}]},
            {'id':'recOther','name':'工程师乙','scope':'B','items':[{'id':'x','title':'B楼排查','status':'pending'}]}]}
        inspections=SimpleNamespace(list=Mock(return_value={'items':[{'id':'risk1','status':'active'},
            {'id':'draft','status':'publishing'}],'total':2}),get=Mock(return_value=task))
        self.runtime.risk_inspections=lambda:inspections
        data=self.catalog.load(self.actor,'risk_inspection','A','2026-10',active_only=True)
        self.assertEqual([r['business_status'] for r in data['items']],['未填写','待复核'])
        self.assertTrue(all(r['participated'] and r['responsible'] for r in data['items']))
        self.assertEqual(len({r['id'] for r in data['items']}),2)
        inspections.get.assert_called_once_with(self.actor,'risk1')
        self.assertIn('id=risk1',data['items'][0]['source_url'])
        self.actor['modules'].remove('risk_inspection')
        with self.assertRaises(HandoverError):self.catalog.load(self.actor,'risk_inspection','A','2026-10')

    def test_supervisor_includes_previous_month_unclosed_events_not_newly_closed_duplicates(self):
        supervisor={**self.actor,'category':'supervisor','level':'L3'}
        self.runtime.state_store=SimpleNamespace(event_month_snapshot_stats=lambda:{'active_months':[{'month':'2026-09'}]})
        def events(**kw):
            if kw['month']=='2026-09':
                return {'snapshot_exists':True,'records':[{'record_id':'old','title':'上月未闭环','status':'处理中'},
                    {'record_id':'cross','title':'跨月事件','status':'处理中'}]}
            return {'snapshot_exists':True,'records':[{'record_id':'cross','title':'跨月事件','status':'已结束'}]}
        self.service.get_event_monthly_snapshot=Mock(side_effect=events)
        data=self.catalog.load(supervisor,'event','A','2026-10',active_only=True)
        self.assertEqual([r['title'] for r in data['items']],['上月未闭环'])
        self.assertEqual(data['items'][0]['period'],'2026-09')
        self.assertIn('month=2026-09',data['items'][0]['source_url'])
        self.assertTrue(data['items'][0]['responsible'])

    def test_supervisor_cabinet_defaults_exclude_done_and_cancelled_batches(self):
        self.actor.update(category='supervisor',level='L3')
        batches=SimpleNamespace(list=Mock(return_value={'items':[
            {'batch_id':'open','name':'待上电','is_todo':True,'scopes':['A']},
            {'batch_id':'done','name':'已完成上电','is_todo':False,'scopes':['A']},
            {'batch_id':'cancel','name':'取消上电','status':'cancelled','is_todo':True,'scopes':['A']},
            {'batch_id':'cross','name':'越权批次','is_todo':True,'scopes':['A','B']}], 'total':4}))
        self.catalog.controller=SimpleNamespace(_cabinet_power=SimpleNamespace(batches=batches))
        data=self.catalog.load(self.actor,'cabinet','A','2026-10',active_only=True)
        self.assertEqual([r['title'] for r in data['items']],['待上电'])
        self.assertTrue(data['items'][0]['responsible'])

    def test_missing_current_event_snapshot_keeps_prior_events_and_reports_partial_load(self):
        self.actor.update(category='supervisor',level='L3',modules=['work_handover','event'])
        self.runtime.state_store=SimpleNamespace(event_month_snapshot_stats=lambda:{'active_months':[{'month':'2026-09'}]})
        self.service.get_event_monthly_snapshot=lambda **kw:{'snapshot_exists':kw['month']=='2026-09',
            'records':[{'record_id':'old','title':'上月未闭环','status':'处理中'}] if kw['month']=='2026-09' else []}
        data=self.catalog.defaults(self.actor,'A','2026-10')
        self.assertEqual([r['title'] for r in data['items']],['上月未闭环'])
        self.assertEqual(data['failed_modules'],['event'])


if __name__=='__main__':unittest.main()

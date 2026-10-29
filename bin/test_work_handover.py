"""Isolated handover lifecycle, callback and access tests; never contacts Feishu."""
import asyncio
import copy
import datetime as dt
import hashlib
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
from upload_event_module.config import config
from bin.test_feishu_delivery import TARGET_APP_ID, PREVIOUS_APP_ID, active_config, qt_credentials_fixture


class HandoverTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(qt_credentials_fixture())
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

    def test_only_owner_can_delete_unsent_draft_and_retry_never_recreates_it(self):
        draft=self.draft(source=True)
        with self.assertRaises(HandoverError):
            self.store.action(self.e,draft['id'],draft['version'],'delete_draft')
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda _:self.store.action(self.a,draft['id'],draft['version'],'delete_draft'),range(2)))
        self.assertTrue(all(result=={'id':draft['id'],'deleted':True} for result in results))
        self.assertEqual(self.store.list(self.a,'sent')['total'],0)
        self.assertEqual(self.store.list(self.b,'received')['total'],0)
        for operation in (lambda:self.store.get(self.a,draft['id']),lambda:self.store.create(self.a,draft['id'])):
            with self.assertRaises(HandoverError) as error:operation()
            self.assertEqual(error.exception.status,410)
        with self.store.connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM work_handover_audit WHERE action='删除草稿'").fetchone()[0],1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM work_handover_outbox').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM work_handover_sources').fetchone()[0],1,'source business remains intact')

    def test_sent_returned_and_withdrawn_handover_history_cannot_be_deleted(self):
        pending=self.pending()
        for current in (pending,self.store.action(self.a,pending['id'],pending['version'],'withdraw')):
            with self.assertRaises(HandoverError):
                self.store.action(self.a,current['id'],current['version'],'delete_draft')
        current=self.store.get(self.a,pending['id'])
        payload={k:current[k] for k in ('title','recipient_id','notes')}
        payload['items']=[{k:i.get(k,'') for k in ('id','title','notes','scope','kind','source_id')} for i in current['items']]
        saved=self.store.save(self.a,current['id'],current['version'],payload)
        with self.assertRaises(HandoverError):
            self.store.action(self.a,saved['id'],saved['version'],'delete_draft')
        with self.store.connect() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM work_handover_versions').fetchone()[0],1)

    def test_delete_draft_and_send_race_have_only_one_winner(self):
        draft=self.draft()
        def run(action):
            try:return self.store.action(self.a,draft['id'],draft['version'],action)
            except HandoverError:return None
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(run,['send','delete_draft']))
        self.assertEqual(sum(result is not None for result in results),1)
        with self.store.connect() as db:
            state=db.execute('SELECT status FROM work_handovers WHERE id=?',(draft['id'],)).fetchone()[0]
            queued=db.execute('SELECT COUNT(*) FROM work_handover_outbox').fetchone()[0]
            self.assertIn(state,{'pending','deleted'})
            self.assertEqual(queued,1 if state=='pending' else 0)

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

    def test_expired_card_cannot_accept_but_web_handover_stays_available(self):
        from lan_bitable_template_portal.feishu_card_lifetime import timestamp, MAX_CARD_AGE
        data = self.pending()
        with self.store.connect() as db:
            private = self.store._read(db, data['id'])
        with patch('time.time', return_value=timestamp(private['sent_at']) + MAX_CARD_AGE):
            with self.assertRaisesRegex(HandoverError, '7天'):
                self.store.action(self.b, data['id'], data['version'], 'accept', token=private['card_token'])
            self.assertNotIn('form_action_type', json.dumps(card(private, 'https://fixture.invalid')))
        self.assertEqual(self.store.get(self.b, data['id'])['status'], 'pending')
        self.assertEqual(self.store.action(self.b, data['id'], data['version'], 'accept')['status'], 'accepted')

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
            row=db.execute('SELECT id,payload FROM work_handover_outbox').fetchone()
            payload=json.loads(row['payload'])
            payload['_delivery']={'sender_app_id':'cli_bot','status':'sent'}
            db.execute('UPDATE work_handover_outbox SET payload=? WHERE id=?',(json.dumps(payload),row['id']))
        withdrawn=self.store.action(self.a,data['id'],data['version'],'withdraw')
        saved=self.store.save(self.a,data['id'],withdrawn['version'],{'title':'另一份交接内容','notes':'不应发给原接收人',
            'recipient_id':'eve','items':[{'id':'c'*32,'title':'新资料','scope':'A','kind':'reference','notes':'','source_id':''}]})
        self.store.action(self.a,data['id'],saved['version'],'send')
        with self.store.connect() as db:
            refresh=dict(db.execute("SELECT * FROM work_handover_outbox WHERE kind='refresh'").fetchone())
        client=SimpleNamespace(patch=AsyncMock(return_value=SimpleNamespace(status_code=200,json=lambda:{'code':0})))
        bot=SimpleNamespace(config=active_config('cli_bot'),messenger=SimpleNamespace(client=AsyncMock(return_value=client),headers=AsyncMock(return_value={})),
            runtime=SimpleNamespace(service=SimpleNamespace(_critical_guard_public_base_url=lambda:'https://portal')))
        with patch.object(config,'app_id','cli_bot'), patch('lan_bitable_template_portal.portal_service.external_real_write_guard',return_value={'real_write_allowed':True}):
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
        directory=Mock(return_value=[{'key':'cli_bot:ou_bob','payload':{'open_id':'ou_bob','record_id':'bob','matched_by':'record_id'}}])
        union=Mock(return_value='on_bob')
        bot=SimpleNamespace(config={'app_id':'cli_bot'},runtime=SimpleNamespace(state_store=SimpleNamespace(list_documents=directory)),
            permission_cards=SimpleNamespace(union_id=union))
        cards=HandoverCards(bot)
        self.assertEqual(cards.recipient_id(self.b),('open_id','ou_bob'))
        union.assert_not_called()
        directory.return_value=[{'key':'cli_bot:ou_wrong','payload':{'open_id':'ou_bob','record_id':'eve','matched_by':'record_id'}}]
        with self.assertRaises(HandoverError): cards.recipient_id(self.b)
        union.assert_not_called()

    def test_name_only_binding_cannot_send_and_record_evidence_can_send(self):
        self.pending()
        with self.store.connect() as db:
            row=dict(db.execute("SELECT * FROM work_handover_outbox WHERE kind='invite'").fetchone())
        binding={'key':'cli_bot:ou_bob','payload':{'open_id':'ou_bob','record_id':'bob','matched_by':'name'}}
        union=Mock(side_effect=HandoverError('没有可信身份关联',403))
        post=AsyncMock(return_value=SimpleNamespace(status_code=200,json=lambda:{'code':0,'data':{'message_id':'om_verified'}}))
        bot=SimpleNamespace(config=active_config('cli_bot'),
            permission_cards=SimpleNamespace(union_id=union),
            runtime=SimpleNamespace(state_store=SimpleNamespace(list_documents=Mock(return_value=[binding])),
                service=SimpleNamespace(_critical_guard_public_base_url=lambda:'https://portal.invalid')),
            messenger=SimpleNamespace(client=AsyncMock(return_value=SimpleNamespace(post=post)),
                                      headers=AsyncMock(return_value={'Authorization':'fixture'})))
        cards=HandoverCards(bot)
        with patch.object(config,'app_id','cli_bot'), patch('lan_bitable_template_portal.portal_service.external_real_write_guard',return_value={'real_write_allowed':True}):
            with self.assertRaises(HandoverError):
                asyncio.run(cards.deliver(self.store,row))
            post.assert_not_awaited()
            union.assert_not_called()
            union.reset_mock()
            binding['payload']['matched_by']='record_id'
            self.assertEqual(asyncio.run(cards.deliver(self.store,row)),'om_verified')
        union.assert_not_called()
        post.assert_awaited_once()
        self.assertEqual(post.call_args.kwargs['params'],{'receive_id_type':'open_id'})
        self.assertEqual(post.call_args.kwargs['json']['receive_id'],'ou_bob')
        self.assertEqual(post.call_args.kwargs['json']['receive_id'],self.b['open_id'])

    def test_recipient_binding_requires_trusted_provenance_and_exact_current_app(self):
        binding={'key':'cli_bot:ou_bob','payload':{'open_id':'ou_bob','record_id':'bob'}}
        directory=Mock(return_value=[binding])
        union=Mock(return_value='on_bob')
        cards=HandoverCards(SimpleNamespace(config={'app_id':'cli_bot'},
            runtime=SimpleNamespace(state_store=SimpleNamespace(list_documents=directory)),
            permission_cards=SimpleNamespace(union_id=union)))
        for method in ('name',None,'unknown','record_id','union_id','admin'):
            with self.subTest(method=method):
                binding['payload']['matched_by']=method
                union.reset_mock()
                trusted=method in {'record_id','union_id','admin'}
                if trusted:
                    self.assertEqual(cards.recipient_id(self.b),('open_id','ou_bob'))
                    union.assert_not_called()
                else:
                    with self.assertRaises(HandoverError): cards.recipient_id(self.b)
                    union.assert_not_called()
        binding['key']='cli_old:ou_oldBob'
        binding['payload']['matched_by']='record_id'
        with self.assertRaises(HandoverError): cards.recipient_id(self.b)
        directory.assert_called_with('feishu_assistant_sender',key_prefix='cli_bot:')

    def test_untrusted_name_binding_does_not_override_or_ambiguate_trusted_binding(self):
        directory=Mock(return_value=[
            {'key':'cli_bot:ou_nameOnly','payload':{'open_id':'ou_bob','record_id':'bob','matched_by':'name'}},
            {'key':'cli_bot:ou_bob','payload':{'open_id':'ou_bob','record_id':'bob','matched_by':'record_id'}}])
        union=Mock(side_effect=AssertionError('A unique verified binding needs no union lookup'))
        cards=HandoverCards(SimpleNamespace(config={'app_id':'cli_bot'},
            runtime=SimpleNamespace(state_store=SimpleNamespace(list_documents=directory)),
            permission_cards=SimpleNamespace(union_id=union)))
        self.assertEqual(cards.recipient_id(self.b),('open_id','ou_bob'))
        union.assert_not_called()

    def test_gateway_recipient_fallback_rejects_name_only_binding(self):
        from lan_bitable_template_portal.feishu_delivery import PublicFeishuSender
        binding={'key':'cli_bot:ou_bob','payload':{'open_id':'ou_bob','record_id':'recBob','matched_by':'name'}}
        union=Mock(side_effect=HandoverError('没有可信身份关联',403))
        runtime=SimpleNamespace(auth_manager=SimpleNamespace(directory_person=Mock(
            return_value={'id':'recBob','selectable':True})),
            state_store=SimpleNamespace(list_documents=Mock(return_value=[binding])))
        bot=SimpleNamespace(config={'app_id':'cli_bot'},runtime=runtime,
            permission_cards=SimpleNamespace(union_id=union),
            messenger=SimpleNamespace(recipient_open_id=AsyncMock(return_value=None)))
        bot.handover_cards=HandoverCards(bot)
        gateway=PublicFeishuSender(runtime)
        self.assertEqual(asyncio.run(gateway.recipient(bot,'ou_bob')).status,'blocked')
        bot.messenger.recipient_open_id.assert_not_awaited()
        binding['payload']['matched_by']='record_id'
        union.reset_mock()
        self.assertEqual(asyncio.run(gateway.recipient(bot,'ou_bob')),('open_id','ou_bob'))
        union.assert_not_called()

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


class HandoverNotificationTests(unittest.TestCase):
    draft=HandoverTests.draft
    pending=HandoverTests.pending

    def setUp(self):
        HandoverTests.setUp(self)
        self.data=self.pending()
        self.base_payload=self.row()['payload']
        for item in (patch.object(config,'app_id',TARGET_APP_ID),
                     patch('lan_bitable_template_portal.portal_service.external_real_write_guard',return_value={'real_write_allowed':True})):
            item.start()
            self.addCleanup(item.stop)
        self.post=AsyncMock(return_value=self.response())
        self.patch=AsyncMock(return_value=self.response({'code':0}))
        self.headers=AsyncMock(return_value={'Authorization':'offline-token'})
        self.bindings=Mock(side_effect=lambda _ns,*,key_prefix:[{
            'key':key_prefix+'ou_'+name,
            'payload':{'open_id':'ou_'+name,'record_id':name,'matched_by':'record_id'}} for name in ('alice','bob','eve')])
        self.bot=SimpleNamespace(config=active_config(),
            runtime=SimpleNamespace(work_handovers=lambda:self.store,
                work_handover_actor=lambda open_id,_name:self.actors[open_id.removeprefix('ou_')],
                state_store=SimpleNamespace(list_documents=self.bindings),
                service=SimpleNamespace(_critical_guard_public_base_url=lambda:'https://portal.invalid')),
            permission_cards=SimpleNamespace(union_id=Mock(return_value='on_verified')),
            messenger=SimpleNamespace(client=AsyncMock(return_value=SimpleNamespace(post=self.post,patch=self.patch)),headers=self.headers))
        self.cards=HandoverCards(self.bot)

    @staticmethod
    def response(value=None,status=200):
        return SimpleNamespace(status_code=status,json=lambda:value if value is not None else {
            'code':0,'data':{'message_id':'om_current','chat_id':'oc_private'}})

    def row(self,kind='invite'):
        with self.store.connect() as db:
            return dict(db.execute('SELECT * FROM work_handover_outbox WHERE kind=? ORDER BY rowid DESC LIMIT 1',(kind,)).fetchone())

    def delivery(self,kind='invite'):
        return json.loads(self.row(kind)['payload']).get('_delivery',{})

    def send(self,kind='invite'):
        return asyncio.run(self.cards.deliver(self.store,self.row(kind)))

    def switch_app(self,app_id='cli_future'):
        config.app_id=app_id
        self.bot.config['app_id']=app_id

    def due(self):
        with self.store.connect(True) as db:
            db.execute('UPDATE work_handover_outbox SET next_at=0')

    def test_producer_invite_initializes_unsent_marker_and_drains_once(self):
        self.assertEqual(self.delivery(),{'sender_app_id':'','status':'not_sent'})
        self.assertEqual(self.row()['state'],'queued')
        self.assertEqual(self.row()['attempts'],0)
        asyncio.run(self.cards.drain())
        self.assertEqual(self.row()['state'],'sent')
        self.assertEqual(self.delivery()['status'],'sent')
        self.assertEqual(self.delivery()['sender_app_id'],TARGET_APP_ID)
        self.assertEqual(self.post.call_args.kwargs['json']['uuid'],
                         hashlib.sha256(self.row()['id'].encode()).hexdigest()[:32])
        asyncio.run(self.cards.drain())
        self.post.assert_awaited_once()
        self.patch.assert_not_awaited()

    def test_producer_accept_initializes_result_and_refresh_and_drains_once(self):
        self.send()
        original=self.row()
        self.post.reset_mock()
        self.store.action(self.b,self.data['id'],self.data['version'],'accept')
        for kind in ('result','refresh'):
            with self.subTest(kind=kind):
                self.assertEqual(self.delivery(kind),{'sender_app_id':'','status':'not_sent'})
                self.assertEqual(self.row(kind)['state'],'queued')
                self.assertEqual(self.row(kind)['attempts'],0)
        self.post.return_value=self.response({'code':0,'data':{'message_id':'om_result','chat_id':'oc_private'}})
        asyncio.run(self.cards.drain())
        for kind in ('result','refresh'):
            with self.subTest(kind=kind):
                self.assertEqual(self.row(kind)['state'],'sent')
                self.assertEqual(self.delivery(kind)['status'],'sent')
                self.assertEqual(self.delivery(kind)['sender_app_id'],TARGET_APP_ID)
        self.assertEqual(self.row('result')['message_id'],'om_result')
        self.assertEqual(self.row('refresh')['message_id'],'om_current')
        self.assertEqual(self.post.call_args.kwargs['json']['uuid'],
                         hashlib.sha256(self.row('result')['id'].encode()).hexdigest()[:32])
        self.assertTrue(self.patch.call_args.args[0].endswith('/om_current'))
        self.assertEqual(self.row(),original)
        asyncio.run(self.cards.drain())
        self.post.assert_awaited_once()
        self.patch.assert_awaited_once()
        self.assertEqual(self.store.get(self.b,self.data['id'])['status'],'accepted')

    def test_producer_withdraw_initializes_refresh_and_patches_once(self):
        self.send()
        original=self.row()
        self.post.reset_mock()
        self.store.action(self.a,self.data['id'],self.data['version'],'withdraw')
        self.assertEqual(self.delivery('refresh'),{'sender_app_id':'','status':'not_sent'})
        self.assertEqual(self.row('refresh')['state'],'queued')
        asyncio.run(self.cards.drain())
        self.assertEqual(self.row('refresh')['state'],'sent')
        self.assertEqual(self.row('refresh')['message_id'],'om_current')
        self.assertEqual(self.delivery('refresh')['sender_app_id'],TARGET_APP_ID)
        self.assertTrue(self.patch.call_args.args[0].endswith('/om_current'))
        self.assertEqual(self.row(),original)
        asyncio.run(self.cards.drain())
        self.patch.assert_awaited_once()
        self.post.assert_not_awaited()

    def test_first_send_persists_app_before_http_and_keeps_original_uuid(self):
        async def inspect(*_args,**kwargs):
            stored=self.delivery()
            self.assertEqual(stored['sender_app_id'],TARGET_APP_ID)
            self.assertEqual(stored['status'],'posting')
            self.assertEqual(stored['target'],['open_id','ou_bob'])
            self.assertEqual(kwargs['json']['uuid'],hashlib.sha256(self.row()['id'].encode()).hexdigest()[:32])
            return self.response()
        self.post.side_effect=inspect
        self.assertEqual(self.send(),'om_current')
        self.assertEqual(self.row()['state'],'sent')
        self.assertEqual(self.delivery()['sender_app_id'],TARGET_APP_ID)
        self.assertGreater(self.delivery()['accepted_at'],0)
        self.switch_app()
        self.assertEqual(self.send(),'om_current')
        self.post.assert_awaited_once()

    def test_permission_failure_is_definitely_unsent_and_may_use_future_app_once(self):
        self.actors['bob']['scope_rules']=[]
        asyncio.run(self.cards.drain())
        self.assertEqual(self.delivery()['status'],'not_sent')
        self.post.assert_not_awaited()
        self.headers.assert_not_awaited()
        self.actors['bob']['scope_rules']=['A']
        self.switch_app()
        self.due()
        asyncio.run(self.cards.drain())
        self.assertEqual(self.row()['state'],'sent')
        self.assertEqual(self.delivery()['sender_app_id'],'cli_future')
        self.assertEqual(self.post.call_args.kwargs['json']['receive_id'],'ou_bob')
        self.assertEqual(self.send(),'om_current')
        self.post.assert_awaited_once()

    def test_initialized_previous_not_sent_can_bind_current_app_but_stale_bot_never_sends(self):
        payload=json.loads(self.base_payload)
        payload['_delivery']={'sender_app_id':PREVIOUS_APP_ID,'status':'not_sent'}
        with self.store.connect(True) as db:
            db.execute('UPDATE work_handover_outbox SET payload=?',(json.dumps(payload),))
        self.bot.config['app_id']=PREVIOUS_APP_ID
        with self.assertRaises(HandoverError):self.send()
        self.post.assert_not_awaited()
        self.headers.assert_not_awaited()
        self.switch_app(TARGET_APP_ID)
        self.assertEqual(self.send(),'om_current')
        self.assertEqual(self.delivery()['sender_app_id'],TARGET_APP_ID)
        self.post.assert_awaited_once()

    def test_same_app_secret_rotation_requires_bot_and_messenger_to_match_qt(self):
        with patch.object(config,'app_secret','fixture-rotated-secret'):
            with self.assertRaises(HandoverError):self.send()
            self.bot.config=active_config(secret='fixture-rotated-secret')
            self.bot.messenger.config=active_config()
            with self.assertRaises(HandoverError):self.send()
            self.post.assert_not_awaited()
            self.headers.assert_not_awaited()
            self.assertEqual(self.delivery()['status'],'not_sent')
            self.bot.messenger.config=dict(self.bot.config)
            self.assertEqual(self.send(),'om_current')
        self.post.assert_awaited_once()

    def test_secret_change_during_post_keeps_original_unknown_outcome(self):
        async def send(*args,**kwargs):
            config.app_secret='fixture-rotated-secret'
            return self.response()
        self.post.side_effect=send
        with self.assertRaises(HandoverError):self.send()
        self.assertEqual(self.delivery()['status'],'posting')
        self.assertEqual(self.row()['message_id'],'')
        self.post.assert_awaited_once()

    def test_timeout_cannot_repost_in_a_different_app(self):
        self.post.side_effect=TimeoutError('offline uncertain request')
        with self.assertRaises(TimeoutError):self.send()
        original=self.delivery()
        self.switch_app()
        with self.assertRaisesRegex(HandoverError,'未换应用重发'):self.send()
        self.assertEqual(self.delivery(),original)
        self.assertEqual(self.row()['message_id'],'')
        self.post.assert_awaited_once()
        self.headers.assert_awaited_once()

    def test_manual_retry_cannot_clear_persisted_unknown_app_ownership(self):
        self.post.side_effect=TimeoutError('offline uncertain request')
        asyncio.run(self.cards.drain())
        original=self.delivery()
        with self.store.connect(True) as db:
            db.execute("UPDATE work_handover_outbox SET state='failed',attempts=6,next_at=0")
        self.store.action(self.a,self.data['id'],self.data['version'],'retry-notification')
        self.switch_app()
        asyncio.run(self.cards.drain())
        self.assertEqual(self.row()['state'],'failed')
        self.assertEqual(self.delivery(),original)
        self.assertEqual(self.store.get(self.b,self.data['id'])['status'],'pending')
        self.post.assert_awaited_once()

    def test_legacy_attempts_without_app_scope_are_held_even_after_retry_reset(self):
        legacy=json.loads(self.base_payload)
        legacy.pop('_delivery')
        for state,attempts,next_at,error in [('queued',0,0,''),('queued',1,0,''),('failed',6,0,''),('queued',0,50,''),('queued',0,0,'unknown')]:
            with self.subTest(state=state,attempts=attempts,next_at=next_at,error=error):
                with self.store.connect(True) as db:
                    db.execute('UPDATE work_handover_outbox SET state=?,attempts=?,next_at=?,error=?,payload=?',
                               (state,attempts,next_at,error,json.dumps(legacy)))
                with self.assertRaisesRegex(HandoverError,'未换应用重发'):self.send()
                self.assertEqual(self.delivery(),{'sender_app_id':'','status':'uncertain'})
                with self.store.connect(True) as db:
                    db.execute("UPDATE work_handover_outbox SET state='queued',attempts=0,next_at=0,error=''")
                with self.assertRaises(HandoverError):self.send()
        self.post.assert_not_awaited()
        self.headers.assert_not_awaited()

    def test_legacy_sent_receipts_are_read_only_with_or_without_known_app(self):
        for scoped in (False,True):
            with self.subTest(scoped=scoped):
                payload=json.loads(self.base_payload)
                payload.pop('_delivery')
                if scoped:payload['_delivery']={'sender_app_id':PREVIOUS_APP_ID,'status':'sent'}
                with self.store.connect(True) as db:
                    db.execute("UPDATE work_handover_outbox SET state='sent',message_id='om_historical',error='history',payload=?",(json.dumps(payload),))
                original=self.row()
                self.assertEqual(self.send(),'om_historical')
                self.assertEqual(self.row(),original)
        self.post.assert_not_awaited()
        self.headers.assert_not_awaited()

    def test_refresh_never_patches_another_apps_sent_card(self):
        self.send()
        self.store.action(self.a,self.data['id'],self.data['version'],'withdraw')
        original=self.row()
        self.post.reset_mock()
        self.headers.reset_mock()
        self.switch_app()
        with self.assertRaisesRegex(HandoverError,'未跨应用更新'):self.send('refresh')
        self.assertEqual(self.row(),original)
        self.patch.assert_not_awaited()
        self.post.assert_not_awaited()
        self.headers.assert_not_awaited()

    def test_refresh_cannot_guess_app_for_unscoped_legacy_sent_card(self):
        legacy=json.loads(self.base_payload)
        legacy.pop('_delivery')
        with self.store.connect(True) as db:
            db.execute("UPDATE work_handover_outbox SET state='sent',message_id='om_old',payload=?",(json.dumps(legacy),))
        self.store.action(self.a,self.data['id'],self.data['version'],'withdraw')
        original=self.row()
        with self.assertRaisesRegex(HandoverError,'未跨应用更新'):self.send('refresh')
        self.assertEqual(self.row(),original)
        self.patch.assert_not_awaited()
        self.post.assert_not_awaited()

    def test_first_explicit_rejection_can_retry_current_app_with_original_uuid(self):
        self.post.return_value=self.response({'code':230013},400)
        asyncio.run(self.cards.drain())
        self.assertEqual(self.delivery()['status'],'not_sent')
        self.switch_app()
        self.due()
        self.post.return_value=self.response()
        asyncio.run(self.cards.drain())
        self.assertEqual(self.row()['state'],'sent')
        self.assertEqual(self.delivery()['sender_app_id'],'cli_future')
        self.assertEqual(len({call.kwargs['json']['uuid'] for call in self.post.call_args_list}),1)
        self.assertEqual(self.post.await_count,2)

    def test_later_rejection_cannot_erase_an_earlier_unknown_request(self):
        self.post.side_effect=TimeoutError('offline uncertain request')
        with self.assertRaises(TimeoutError):self.send()
        self.post.side_effect=None
        self.post.return_value=self.response({'code':230013},400)
        with self.assertRaises(HandoverError):self.send()
        self.assertEqual(self.delivery()['status'],'uncertain')
        self.switch_app()
        with self.assertRaisesRegex(HandoverError,'未换应用重发'):self.send()
        self.assertEqual(self.delivery()['sender_app_id'],TARGET_APP_ID)
        self.assertEqual(self.post.await_count,2)

    def test_unknown_request_cannot_change_mapped_person(self):
        self.post.side_effect=TimeoutError('offline uncertain request')
        with self.assertRaises(TimeoutError):self.send()
        original=self.delivery()
        self.bindings.side_effect=None
        self.bindings.return_value=[{'key':TARGET_APP_ID+':ou_changed',
            'payload':{'open_id':'ou_bob','record_id':'bob','matched_by':'record_id'}}]
        with self.assertRaisesRegex(HandoverError,'接收人或内容已变化'):self.send()
        self.assertEqual(self.delivery(),original)
        self.post.assert_awaited_once()

    def test_app_change_during_headers_is_unsent_and_during_post_is_uncertain(self):
        async def headers():
            self.switch_app()
            return {'Authorization':'offline-token'}
        self.headers.side_effect=headers
        with self.assertRaises(HandoverError):self.send()
        self.assertEqual(self.delivery()['status'],'not_sent')
        self.post.assert_not_awaited()
        self.headers.side_effect=None
        async def post(*_args,**_kwargs):
            self.switch_app('cli_later')
            return self.response()
        self.post.side_effect=post
        with self.assertRaises(HandoverError):self.send()
        self.assertEqual(self.delivery()['status'],'posting')
        self.assertEqual(self.delivery()['sender_app_id'],'cli_future')
        with self.assertRaisesRegex(HandoverError,'未换应用重发'):self.send()
        self.post.assert_awaited_once()

    def test_http408_or_invalid_receipt_cannot_claim_success_or_clear_app(self):
        for body,status in [({'code':230013},408),({'code':0,'data':{'message_id':'om_maybe'}},408),
                            ({'code':0,'data':{'message_id':'om_redirect'}},302),
                            ({'code':False},200),({'code':0,'data':{'message_id':'invalid'}},200),({'code':230013},503)]:
            with self.subTest(body=body,status=status):
                with self.store.connect(True) as db:
                    db.execute("UPDATE work_handover_outbox SET state='queued',payload=?",(self.base_payload,))
                self.post.return_value=self.response(body,status)
                with self.assertRaises(HandoverError):self.send()
                self.assertEqual(self.delivery()['status'],'posting')
                self.assertEqual(self.delivery()['sender_app_id'],TARGET_APP_ID)
                self.assertEqual(self.row()['message_id'],'')

    def test_same_app_retry_is_bounded_and_keeps_uuid(self):
        self.post.side_effect=TimeoutError('offline uncertain request')
        with self.assertRaises(TimeoutError):self.send()
        with patch('lan_bitable_template_portal.work_handover_cards.time.time',return_value=self.delivery()['attempted_at']+3600):
            with self.assertRaisesRegex(HandoverError,'超出去重安全时限'):self.send()
        self.post.assert_awaited_once()
        self.post.side_effect=None
        self.assertEqual(self.send(),'om_current')
        self.assertEqual(len({call.kwargs['json']['uuid'] for call in self.post.call_args_list}),1)

    def test_receipt_store_failure_keeps_app_pin(self):
        with patch.object(self.cards,'_finish',side_effect=OSError('offline receipt store failed')):
            with self.assertRaises(OSError):self.send()
        self.assertEqual(self.delivery()['status'],'posting')
        self.assertNotEqual(self.row()['state'],'sent')
        self.switch_app()
        with self.assertRaisesRegex(HandoverError,'未换应用重发'):self.send()
        self.post.assert_awaited_once()

    def test_config_change_after_claim_but_before_http_remains_definitely_unsent(self):
        claim=self.cards._claim
        def changed(*args):
            claim(*args)
            self.switch_app()
        with patch.object(self.cards,'_claim',side_effect=changed):
            with self.assertRaises(HandoverError):self.send()
        self.assertEqual(self.delivery()['status'],'not_sent')
        self.post.assert_not_awaited()
        self.assertEqual(self.send(),'om_current')
        self.assertEqual(self.delivery()['sender_app_id'],'cli_future')
        self.post.assert_awaited_once()

    def test_cancelled_http_attempt_retains_ownership_after_restart(self):
        self.post.side_effect=asyncio.CancelledError
        with self.assertRaises(asyncio.CancelledError):self.send()
        self.assertEqual(self.delivery()['status'],'posting')
        self.switch_app()
        self.cards=HandoverCards(self.bot)
        with self.assertRaisesRegex(HandoverError,'未换应用重发'):self.send()
        self.post.assert_awaited_once()


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

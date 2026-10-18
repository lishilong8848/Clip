"""Roster names, agent-only delivery and local completion summaries; no real writes."""
import asyncio
import datetime as dt
import json
from pathlib import Path
import sys
import tempfile
import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.pre_shift_cloud import CheckCloud, CheckSender, read_roster
from lan_bitable_template_portal.pre_shift_checks import CheckService, CheckError, ROSTER_VERSION, TZ
from test_pre_shift_checks import receipt

DAY = '2026-10-17'
NOW = dt.datetime.fromisoformat(DAY + 'T09:00:00').replace(tzinfo=TZ)
ADMIN = {'id':'ou_admin','name':'管理员','level':'L1','category':'manager','scope_rules':['ALL']}
WORKER = {'id':'ou_one','name':'人员一','level':'L4','category':'operator','home_scope':'A','scope_rules':['A']}


def person(name, oid, scope='A', **extra):
    return {'record_id':'rec' + oid.removeprefix('ou_'),'name':name,'open_id':oid,'scopes':[scope],'selectable':True, **extra}


def roster_fields(shift='白', scope='A', **extra):
    return {'排班日期':int(NOW.timestamp()*1000),'机楼':scope+'楼','班组':'A班','班次':shift,
            '人员（文本）':'人员一、人员二','人员':{'users':[{'id':'ou_one','name':'人员一'}]}, **extra}


def reader(fields, directory=None):
    return SimpleNamespace(_request_json=Mock(return_value={'code':0,'data':{'items':[{'fields':f} for f in fields],'has_more':False}}),
        get_permission_directory_people=Mock(return_value={'items':directory if directory is not None else [person('人员一','ou_one'),person('人员二','ou_two',account_nature='外部账号')]}))


class RosterTests(unittest.TestCase):
    def test_text_names_complete_both_shifts_when_linked_people_has_only_one(self):
        service=reader([roster_fields(),roster_fields('夜')])
        result=read_roster(service,DAY)
        self.assertEqual([(p['shift'],p['open_id']) for p in result['people']], [('白','ou_one'),('白','ou_two'),('夜','ou_one'),('夜','ou_two')])
        self.assertEqual(result['warnings'],{})
        self.assertEqual(service.get_permission_directory_people.call_count,1)
        self.assertEqual(service._request_json.call_args.kwargs['params']['view_id'],'vew6jLlGzq')

    def test_linked_stranger_does_not_replace_a_name_in_text(self):
        service=reader([roster_fields(人员={'users':[{'id':'ou_stranger','name':'其他人'}]})],
                       [person('人员一','ou_one'),person('人员二','ou_two'),person('其他人','ou_stranger')])
        self.assertEqual([p['open_id'] for p in read_roster(service,DAY)['people']],['ou_one','ou_two'])

    def test_ambiguous_name_keeps_group_waiting_without_guessing_or_losing_other_building(self):
        service=reader([roster_fields(),roster_fields(scope='B',**{'人员（文本）':'乙一、乙二','人员':[]})],
            [person('人员一','ou_one'),person('人员二','ou_two'),person('人员二','ou_duplicate'),person('乙一','ou_bone','B'),person('乙二','ou_btwo','B')])
        result=read_roster(service,DAY)
        self.assertEqual([p['scope'] for p in result['people']],['A','B','B'])
        self.assertIn('人员二',result['warnings']['A'])
        self.assertIn('1/2',result['warnings']['A'])

    def test_pre_shift_can_resolve_110_without_changing_learning_exclusion(self):
        service=reader([roster_fields(scope='110')],[person('人员一','ou_one','110'),person('人员二','ou_two','110')])
        self.assertEqual(len(read_roster(service,DAY)['people']),2)
        from lan_bitable_template_portal.learning_reminders import resolve_roster
        values=[{'scope':'110','shift':'白','name':'人员一','open_id':''}]
        directory={'one':{'name':'人员一','scopes':['110'],'selectable':True,'login_ids':['ou_one']}}
        self.assertEqual(resolve_roster(values,directory)[0],[])

    def test_unconfigured_building_is_not_resolved_or_reported(self):
        service=reader([roster_fields(),roster_fields('夜'),roster_fields(scope='110')])
        result=read_roster(service,DAY,scopes=['A'])
        self.assertEqual({p['scope'] for p in result['people']},{'A'})
        self.assertEqual(result['warnings'],{})
        service._request_json.reset_mock()
        self.assertEqual(read_roster(service,DAY,scopes=[]),{'people':[],'warnings':{}})
        service._request_json.assert_not_called()

    def test_missing_text_names_only_holds_the_affected_shift(self):
        service=reader([roster_fields(**{'人员（文本）':''}),roster_fields('夜')])
        result=read_roster(service,DAY)
        self.assertEqual([p['shift'] for p in result['people']],['夜','夜'])
        self.assertIn('人员（文本）姓名名单缺失',result['warnings']['A'])


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.roster=Mock(return_value={'people':[{'scope':'A','shift':shift,'open_id':oid,'name':name} for shift in ('白','夜') for oid,name in [('ou_one','人员一'),('ou_two','人员二')]],'warnings':{}})
        self.sender=Mock(side_effect=receipt)
        self.cloud=Mock()
        self.cloud.upsert.return_value='recSynced'
        self.service=CheckService(temp.name,roster=self.roster,sender=self.sender,cloud=self.cloud,portal_url=lambda:'https://fixture.invalid')
        with patch('lan_bitable_template_portal.pre_shift_checks.now',return_value=NOW):
            self.service.save_definition(ADMIN,'A',0,[{'id':'one','text':'检查配电设备'}])

    def test_legacy_empty_roster_cache_is_refreshed_after_update(self):
        with self.service.connection(True) as db:
            db.execute('INSERT INTO rosters(day,people,error,version) VALUES(?,?,?,0)',(DAY,'[]','旧版排班为1人'))
        self.service.tick(NOW)
        self.assertEqual(self.roster.call_count,1)
        self.assertEqual(self.sender.call_count,2)
        self.service.tick(NOW + dt.timedelta(minutes=1))
        self.assertEqual(self.sender.call_count,2)
        with self.service.connection() as db:
            self.assertEqual(db.execute('SELECT version FROM rosters').fetchone()[0],ROSTER_VERSION)

    def test_partial_roster_retries_but_does_not_resend_to_valid_buildings(self):
        with patch('lan_bitable_template_portal.pre_shift_checks.now',return_value=NOW):
            self.service.save_definition(ADMIN,'B',0,[{'id':'b','text':'B楼检查'}])
        good=self.roster.return_value['people']
        self.roster.side_effect=[{'people':good,'warnings':{'B':'等待补齐人员'}},
            {'people':good+[{'scope':'B','shift':'白','open_id':oid,'name':oid} for oid in ('ou_bone','ou_btwo')],'warnings':{}}]
        self.service.tick(NOW)
        self.assertEqual(self.sender.call_count,2)
        self.service.tick(NOW + dt.timedelta(minutes=4))
        self.assertEqual(self.roster.call_count,1)
        self.service.tick(NOW + dt.timedelta(minutes=5))
        self.assertEqual(self.sender.call_count,4)
        self.assertEqual([c.args[0] for c in self.sender.call_args_list],['ou_one','ou_two','ou_bone','ou_btwo'])

    def test_summary_is_authorized_and_submission_is_queued_and_synced_once(self):
        self.service.prepare(NOW)
        self.assertIsNone(self.service.state(WORKER,'A',NOW)['overview'])
        self.service.submit(WORKER,DAY+':A:白',1,[{'id':'one','status':'normal','note':''}],NOW)
        self.assertTrue(self.service.wake.is_set())
        self.service.tick(NOW)
        self.assertEqual(self.cloud.upsert.call_count,1)
        overview=self.service.state(ADMIN,'A',NOW)['overview']['rows']
        row=next(r for r in overview if r['scope']=='A' and r['shift']=='白')
        self.assertEqual((row['status'],row['completed'],row['total'],row['submitted_by']),('已完成',1,1,'人员一'))
        supervisor={**ADMIN,'level':'L3','category':'supervisor','scope_rules':['A']}
        self.assertEqual({r['scope'] for r in self.service.state(supervisor,'A',NOW)['overview']['rows']},{'A'})
        limited_admin={**ADMIN,'scope_rules':['A']}
        self.assertEqual({r['scope'] for r in self.service.state(limited_admin,'A',NOW)['overview']['rows']},{'A'})
        with self.assertRaises(CheckError):self.service.state(limited_admin,'B',NOW)
        self.service.tick(NOW + dt.timedelta(minutes=1))
        self.assertEqual(self.cloud.upsert.call_count,1)

    def test_cloud_fields_and_v4_idempotency_token_include_complete_checklist(self):
        self.service.prepare(NOW)
        self.service.submit(WORKER,DAY+':A:白',1,[{'id':'one','status':'abnormal','note':'指示灯异常'}],NOW)
        with self.service.connection() as db:
            rows=[dict(r) for r in db.execute("SELECT * FROM tasks WHERE submitted!=''")]
        cloud=CheckCloud(None)
        cloud.schema_ready=True
        cloud.all=Mock(return_value=[])
        cloud.request=Mock(return_value={'record':{'record_id':'recStored'}})
        for _ in range(2):self.assertEqual(cloud.upsert('stable-check-key',rows),'recStored')
        call=cloud.request.call_args
        self.assertEqual(uuid.UUID(call.args[3]['client_token']).version,4)
        self.assertEqual(cloud.request.call_args_list[0].args[3],call.args[3])
        fields=call.args[2]['fields']
        self.assertEqual(fields['楼栋'],'A')
        self.assertEqual(fields['提交人员'],'人员一')
        self.assertEqual(fields['检查状态'],'已提交')
        self.assertEqual(fields['排班人员'],'人员一、人员二')
        self.assertIn('检查配电设备',fields['检查清单'])
        self.assertIn('指示灯异常',fields['检查明细'])
        self.assertIn('计划检查时间',fields)
        self.assertIn('检查截止时间',fields)


class AgentSenderTests(unittest.TestCase):
    def test_agent_app_scoped_identity_and_receipt_are_used_not_login_app(self):
        client=SimpleNamespace(post=AsyncMock(return_value=SimpleNamespace(status_code=200,json=lambda:{'code':0,'data':{'message_id':'om_delivered'}})))
        bot=SimpleNamespace(config={'app_id':'cli_agent'},messenger=SimpleNamespace(client=AsyncMock(return_value=client),headers=AsyncMock(return_value={'Authorization':'fixture'})),
            handover_cards=SimpleNamespace(recipient_id=Mock(return_value=('open_id','ou_agentPerson'))))
        runtime=SimpleNamespace(feishu_assistant_channel=bot,auth_manager=SimpleNamespace(directory_person=Mock(return_value={'id':'recOne','selectable':True})))
        sender=CheckSender(runtime)
        async def run():
            sender.loop=asyncio.get_running_loop()
            return await asyncio.to_thread(sender,'ou_one',{'elements':[]},'stable-reminder')
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard',return_value={'real_write_allowed':True}):
            receipt=asyncio.run(run())
        self.assertTrue(receipt[0])
        self.assertEqual(receipt[2][0]['sender_app_id'],'cli_agent')
        self.assertEqual(client.post.call_args.kwargs['json']['receive_id'],'ou_agentPerson')
        self.assertEqual(client.post.call_args.kwargs['params'],{'receive_id_type':'open_id'})

    def test_missing_agent_or_message_receipt_is_not_reported_as_sent(self):
        sender=CheckSender(SimpleNamespace())
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard',return_value={'real_write_allowed':True}):
            with self.assertRaises(CheckError):asyncio.run(sender.send('ou_one',{},'stable-reminder'))
        self.assertFalse(sender('ou_one',{},'stable-reminder')[0])

    def test_success_code_without_a_valid_message_id_is_not_delivery_success(self):
        client=SimpleNamespace(post=AsyncMock())
        bot=SimpleNamespace(config={'app_id':'cli_agent'},messenger=SimpleNamespace(client=AsyncMock(return_value=client),headers=AsyncMock(return_value={})),
            handover_cards=SimpleNamespace(recipient_id=Mock(return_value=('union_id','on_person'))))
        sender=CheckSender(SimpleNamespace(feishu_assistant_channel=bot,
            auth_manager=SimpleNamespace(directory_person=Mock(return_value={'id':'recOne','selectable':True}))))
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard',return_value={'real_write_allowed':True}):
            for mid in (None,'',True,'not-a-message-id'):
                with self.subTest(mid=mid):
                    client.post.return_value=SimpleNamespace(status_code=200,json=lambda:{'code':0,'data':{'message_id':mid}})
                    with self.assertRaises(CheckError):asyncio.run(sender.send('ou_one',{},'stable-reminder'))


if __name__=='__main__':unittest.main()

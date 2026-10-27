"""Durable schedules and semantic receipts, with no model or message network calls."""
import asyncio
import copy
import datetime as dt
import json
import socket
import sys
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fastapi import Request
from lan_bitable_template_portal.assistant_schedules import ScheduledTasks, TZ, local_time, next_occurrence, validate_spec
from lan_bitable_template_portal.assistant_schedule_runtime import AssistantScheduler
from lan_bitable_template_portal.state_store import LanPortalStateStore
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_schedules import handle

NOW = local_time('2026-10-27T10:00:00+08:00').timestamp()
ACTOR = {'id': 'ou_alice', 'name': '测试甲', 'scopes': ['A'], 'modules': ['assistant', 'knowledge'], 'is_admin': False}
CONVERSATION = 'a' * 32


def spec(**changes):
    return {'title': '测试提醒', 'instruction': '检查已领取任务的处理情况。', 'kind': 'reminder',
            'start_at': '2026-10-27T11:00:00+08:00', 'repeat': 'once', 'interval_minutes': 0,
            'weekdays': [], 'day_of_month': 0, 'until': '', **changes}


def context(actor=ACTOR, topic=''):
    request = Request({'type': 'http', 'method': 'POST', 'path': '/', 'headers': [], 'state': {}})
    if topic:
        request.state.feishu_message_route = {'chat_id': 'oc_chat', 'chat_type': 'group', 'message_id': 'om_original',
            'root_id': 'om_root', 'thread_id': 'omt_topic', 'sender_id': actor['id'], 'app_id': 'cli_test'}
    return {'request': request, 'actor': actor, 'feishu_topic': {'id': topic} if topic else None}


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch('time.time', return_value=NOW + 3600))
        root = self.enterContext(tempfile.TemporaryDirectory())
        self.store = LanPortalStateStore(Path(root) / 'state.sqlite3')
        self.tasks = ScheduledTasks(self.store)
        self.enterContext(patch.object(socket.socket, 'connect', side_effect=AssertionError('network forbidden')))

    def create(self, value=None, actor=ACTOR, topic='', operation='operation_create'):
        return self.tasks.dispatch(context(actor, topic), actor, {'action': 'create', 'spec': value or spec(),
            'operation_id': operation, 'conversation_id': CONVERSATION}, NOW)['task']

    def change(self, task, action, **extra):
        return self.tasks.dispatch(context(), ACTOR, {'action': action, 'id': task['id'],
            'revision': task['revision'], 'operation_id': 'operation_' + action,
            'conversation_id': CONVERSATION, **extra}, NOW)['task']

    def test_duplicate_operation_and_restart_keep_one_task_and_receipt(self):
        task = self.create()
        self.tasks = ScheduledTasks(self.store)
        self.assertEqual(self.create(), task)
        self.assertEqual(len(self.tasks.dispatch(context(), ACTOR, {'action': 'list', 'conversation_id': CONVERSATION}, NOW)['tasks']), 1)
        with self.assertRaises(AssistantError):
            self.create(spec(title='不同任务'))

    def test_same_account_private_and_topics_are_isolated(self):
        self.create()
        group_actor = {**ACTOR, 'channel': 'feishu:oc_chat'}
        task = self.create(actor=group_actor, topic='topic-one', operation='operation_topic')
        other = self.tasks.dispatch(context(group_actor, 'topic-two'), group_actor, {'action': 'list', 'conversation_id': CONVERSATION}, NOW)
        self.assertEqual(other['tasks'], [])
        for actor, ctx in [({**ACTOR, 'id': 'ou_bob'}, context({**ACTOR, 'id': 'ou_bob'})), (ACTOR, context())]:
            with self.assertRaises(AssistantError):
                self.tasks.dispatch(ctx, actor, {'action': 'cancel', 'id': task['id'], 'revision': task['revision'],
                    'operation_id': 'operation_cancel', 'conversation_id': CONVERSATION}, NOW)

    def test_schedule_cannot_choose_another_chat_or_recurse(self):
        ctx = context()
        ctx['request'].state.assistant_schedule_id = 'existing'
        with self.assertRaises(AssistantError):
            self.tasks.dispatch(ctx, ACTOR, {'action': 'create', 'spec': spec(), 'operation_id': 'operation_recursive', 'conversation_id': CONVERSATION}, NOW)
        with self.assertRaises(AssistantError):
            self.tasks.dispatch(context(), ACTOR, {'action': 'create', 'spec': spec(), 'recipient': 'ou_bob',
                'operation_id': 'operation_recipient', 'conversation_id': CONVERSATION}, NOW)

    def test_claim_exact_time_multiple_workers_restart_and_one_off(self):
        task = self.create()
        due = task['next_at']
        self.assertIsNone(self.tasks.claim('early', due - 1))
        with ThreadPoolExecutor(max_workers=4) as pool:
            claims = list(pool.map(lambda worker: ScheduledTasks(self.store).claim(worker, due), ['a', 'b', 'c', 'd']))
        claimed = [value for value in claims if value]
        self.assertEqual(len(claimed), 1)
        job = claimed[0][1]
        self.assertIsNone(self.tasks.claim('restart', due + 119))
        retried = self.tasks.claim('restart', due + 121)[1]
        self.assertEqual(retried['id'], job['id'])
        self.tasks.finish(retried, retried['claim_token'])
        self.assertIsNone(self.tasks.claim('again', due + 122))
        self.assertEqual(self.store.get_document('assistant_schedules', 'task:' + task['id'])['status'], 'completed')

    def test_periodic_missed_runs_coalesce_and_do_not_overlap(self):
        task = self.create(spec(repeat='interval', interval_minutes=120))
        late = task['next_at'] + 24 * 3600 + 60
        first = self.tasks.claim('worker', late)
        self.assertEqual(first[0]['next_at'], task['next_at'] + 26 * 3600)
        self.assertIsNone(self.tasks.claim('other', late + 1))
        self.tasks.finish(first[1], first[1]['claim_token'])
        self.assertIsNone(self.tasks.claim('again', late + 1))
        self.assertIsNotNone(self.tasks.claim('again', first[0]['next_at']))

    def test_pause_cancel_and_edit_do_not_send_old_jobs(self):
        for action in ('pause', 'cancel', 'update'):
            with self.subTest(action=action):
                task = self.create(operation='operation_' + action)
                _, job = self.tasks.claim('worker', task['next_at'])
                payload = {'spec': spec(start_at='2026-10-28T11:00:00+08:00')} if action == 'update' else {}
                changed = self.tasks.dispatch(context(), ACTOR, {'action': action, 'id': task['id'],
                    'revision': task['revision'], 'operation_id': 'change_' + action, 'conversation_id': CONVERSATION, **payload}, NOW)['task']
                with self.assertRaises(AssistantError):
                    self.tasks.checkpoint(job, job['claim_token'], reply={'text': 'stale'})
                self.assertIsNone(self.tasks.claim('restart', task['next_at'] + 121))
                self.assertEqual(changed['revision'], 2)

    def test_failed_delivery_resume_reuses_original_reply(self):
        task = self.create()
        _, job = self.tasks.claim('worker', task['next_at'])
        self.tasks.checkpoint(job, job['claim_token'], phase='ready', reply={'text': '已核实结果', 'sent_parts': ['answer:0']})
        self.tasks.finish(job, job['claim_token'], error='权限待核对', permanent=True)
        saved = self.store.get_document('assistant_schedules', 'task:' + task['id'])
        self.change(saved, 'resume')
        _, retry = self.tasks.claim('restored', task['next_at'] + 300)
        self.assertEqual(retry['id'], job['id'])
        self.assertEqual(retry['reply']['sent_parts'], ['answer:0'])

    def test_failed_answer_is_not_marked_success(self):
        task = self.create()
        _, job = self.tasks.claim('worker', task['next_at'])
        self.tasks.checkpoint(job, job['claim_token'], answer_status='failed', answer_error='模型暂不可用')
        self.tasks.finish(job, job['claim_token'])
        saved = self.store.get_document('assistant_schedules', 'task:' + task['id'])
        self.assertEqual(saved['status'], 'failed')
        self.assertEqual(saved['last_error'], '模型暂不可用')

    def test_deadline_passed_during_shutdown_is_not_delivered(self):
        task = self.create(spec(repeat='daily', until='2026-10-28T11:00:00+08:00'))
        self.assertIsNone(self.tasks.claim('worker', local_time('2026-10-29T11:00:00+08:00').timestamp()))
        self.assertEqual(self.store.get_document('assistant_schedules', 'task:' + task['id'])['last_status'], 'expired')

    def test_calendar_rules_and_invalid_specs(self):
        instruction='查询当前测试任务的进度，并把进度情况整理成回复发回本会话。'
        self.assertEqual(validate_spec(spec(instruction=instruction),NOW)['instruction'],instruction)
        monthly = validate_spec(spec(start_at='2026-10-31T11:00:00+08:00', repeat='monthly', day_of_month=31), NOW)
        following = next_occurrence(monthly, local_time(monthly['start_at']).timestamp())
        self.assertEqual(dt.datetime.fromtimestamp(following, TZ).isoformat(), '2026-12-31T11:00:00+08:00')
        last_day = {**monthly, 'day_of_month': -1}
        self.assertEqual(dt.datetime.fromtimestamp(next_occurrence(last_day, following), TZ).day, 31)
        weekdays = validate_spec(spec(start_at='2026-10-30T11:00:00+08:00', repeat='weekly', weekdays=[0,1,2,3,4]), NOW)
        self.assertEqual(dt.datetime.fromtimestamp(next_occurrence(weekdays, local_time(weekdays['start_at']).timestamp()), TZ).weekday(), 0)
        for changes in ({'start_at':'2026-10-27T09:00:00+08:00'}, {'start_at':'2026-10-28T10:00'},
                        {'repeat':'interval','interval_minutes':0}, {'repeat':'weekly','weekdays':[]},
                        {'repeat':'monthly','day_of_month':31}, {'weekdays':[True]}, {'kind':'shell'}):
            with self.subTest(changes=changes), self.assertRaises(AssistantError):
                validate_spec(spec(**changes), NOW)


class SemanticTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.root = self.enterContext(tempfile.TemporaryDirectory())
        self.store = ScheduledTasks(LanPortalStateStore(Path(self.root) / 'state.sqlite3'))
        self.actor = copy.deepcopy(ACTOR)
        self.context = context(self.actor)
        self.model = SimpleNamespace(complete=Mock(), settings=lambda: {'models': []})
        async def dispatch(action, payload):
            self.assertEqual(action, 'assistant_schedule')
            return self.store.dispatch(self.context, self.actor, payload, NOW)
        self.bridge = SimpleNamespace(acall=AsyncMock(side_effect=dispatch))
        self.engine = SimpleNamespace(portal=SimpleNamespace(catalog=SimpleNamespace(bridge=self.bridge)),
            assistant=SimpleNamespace(model_for=lambda actor:self.model, _state=lambda actor:{'id':CONVERSATION}))
        self.turn = {'question':'明天上午十一点提醒我检查报告', 'operation_id':'operation_semantic',
                     '_profile':{'id':'model'}, '_reference_time':'2026-10-27T10:00:00+08:00'}
        self.emit = AsyncMock()
        self.authorize = AsyncMock(side_effect=lambda:copy.deepcopy(self.actor))

    async def test_semantic_create_has_receipt_and_duplicate_does_not_call_model(self):
        self.model.complete.return_value = json.dumps({'action':'create','schedule':spec(start_at='2026-10-28T11:00:00+08:00')})
        first = await handle(self.engine,self.actor,self.turn,[],self.emit,self.authorize)
        self.assertIn('定时任务已保存',first['answer'])
        self.assertIn('2026-10-28 11:00:00',first['answer'])
        self.model.complete.side_effect=AssertionError('duplicate must reuse receipt')
        again = await handle(self.engine,self.actor,self.turn,[],self.emit,self.authorize)
        self.assertIn('无需重复',again['answer'])
        self.assertEqual(len(self.store.dispatch(self.context,self.actor,{'action':'list','conversation_id':CONVERSATION},NOW)['tasks']),1)

    async def test_ambiguous_request_does_not_create_and_model_error_never_claims_success(self):
        self.model.complete.return_value=json.dumps({'action':'clarify','clarification':'请说明具体时刻。'})
        result=await handle(self.engine,self.actor,self.turn,[],self.emit,self.authorize)
        self.assertIn('尚未创建',result['answer'])
        self.model.complete.return_value='not JSON'
        with self.assertRaises(AssistantError):
            await handle(self.engine,self.actor,self.turn,[],self.emit,self.authorize)
        self.assertEqual(self.store.dispatch(self.context,self.actor,{'action':'list','conversation_id':CONVERSATION},NOW)['tasks'],[])

    async def test_forged_owner_destination_and_unauthorized_task_id_are_rejected(self):
        for decision in ({'action':'create','schedule':spec(),'owner':'ou_bob'}, {'action':'cancel','id':'0'*32},
                         {'action':'create','schedule':{**spec(),'recipient':'oc_other'}}):
            self.model.complete.return_value=json.dumps(decision)
            with self.assertRaises(AssistantError):
                await handle(self.engine,self.actor,self.turn,[],self.emit,self.authorize)
        self.actor['_scheduled_task']='existing'
        with self.assertRaises(AssistantError):
            await handle(self.engine,self.actor,self.turn,[],self.emit,self.authorize)

    def test_scheduled_execution_does_not_replace_human_web_or_topic_lane(self):
        from openclaw_service.assistant.lighthouse_context import execution_key
        for origin in ({**ACTOR,'_execution_lane':'web:browserfixture01'},
                       {**ACTOR,'channel':'feishu:oc_chat','_feishu_topic':{'id':'oc_chat:om_root'}}):
            keys={execution_key(origin),execution_key({**origin,'_scheduled_task':'a'*32}),
                  execution_key({**origin,'_scheduled_task':'b'*32})}
            self.assertEqual(len(keys),3)


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(patch('time.time', return_value=NOW + 3600))
        root=self.enterContext(tempfile.TemporaryDirectory())
        self.store=LanPortalStateStore(Path(root)/'state.sqlite3')
        self.runtime=SimpleNamespace(state_store=self.store)
        self.worker=AssistantScheduler(SimpleNamespace(),self.runtime,AsyncMock())
        self.task=self.worker.tasks.dispatch(context(),ACTOR,{'action':'create','conversation_id':CONVERSATION,
            'operation_id':'operation_runtime','spec':spec(kind='task')},NOW)['task']
        self.task,self.job=self.worker.tasks.claim(self.worker.worker,self.task['next_at'])
        self.request=context()['request']
        self.request.state.assistant_schedule={'id':self.task['id'],'revision':1,'mode':'task'}
        self.worker.request_context=AsyncMock(return_value=(self.request,ACTOR,None))
        self.turn={'operation_id':'schedule_'+self.job['id'],'status':'completed','answer':'已核实的结果','sources':[]}

    async def test_web_result_is_stored_in_original_conversation_without_feishu(self):
        self.worker.call=AsyncMock(side_effect=[{'conversation_id':CONVERSATION,'turns':[]},
            {'conversation_id':CONVERSATION,'turns':[self.turn]}])
        await self.worker.execute(self.task,self.job)
        self.assertEqual(self.store.get_document('assistant_schedules','task:'+self.task['id'])['status'],'completed')
        self.assertEqual(self.worker.call.call_args_list[1].args[3]['operation_id'],self.turn['operation_id'])

    async def test_lost_chat_response_recovers_without_resubmitting(self):
        async def reply(*args,**kwargs):
            index=self.worker.call.await_count
            if index == 1:return {'conversation_id':CONVERSATION,'turns':[]}
            if index == 2:raise OSError('response lost')
            self.assertEqual(args[2],'conversation')
            saved=self.store.get_document('assistant_schedules','run:'+self.job['id'])
            self.assertEqual(saved['status'],'running')
            self.assertEqual(saved['claim_token'],self.job['claim_token'])
            self.worker.validate({**self.request.state.assistant_schedule,'run_id':self.job['id'],
                                  'claim_token':self.job['claim_token']})
            return {'conversation_id':CONVERSATION,'turns':[self.turn]}
        self.worker.call=AsyncMock(side_effect=reply)
        sleep=asyncio.sleep
        async def fast_sleep(seconds):await sleep(0 if seconds == 5 else seconds)
        with patch('lan_bitable_template_portal.assistant_schedule_runtime.asyncio.sleep',new=fast_sleep):
            await self.worker.execute(self.task,self.job)
        self.assertEqual(self.worker.call.await_count,3)
        self.assertEqual(self.store.get_document('assistant_schedules','task:'+self.task['id'])['status'],'completed')

    async def test_cleared_conversation_and_revoked_account_pause_without_sending(self):
        self.worker.call=AsyncMock(return_value={'conversation_id':'b'*32,'turns':[]})
        await self.worker.execute(self.task,self.job)
        self.assertEqual(self.store.get_document('assistant_schedules','task:'+self.task['id'])['status'],'paused')
        self.worker.call.assert_awaited_once()

    async def test_feishu_reply_retries_delivery_only_and_keeps_topic(self):
        message={'message_id':'schedule_'+self.job['id'],'reply_to_message_id':'om_original',
            'root_id':'om_root','chat_id':'oc_chat','chat_type':'group','app_id':'cli_test','sender_id':ACTOR['id']}
        self.worker.request_context=AsyncMock(return_value=(self.request,ACTOR,message))
        posts=[]
        async def send(msg,reply,checkpoint,authorize):
            await authorize()
            if not reply.get('sent_parts'):
                posts.append((msg['root_id'],msg['message_id']))
                reply['sent_parts']=['answer:0']
                await checkpoint()
                raise OSError('network lost after receipt')
        bot=SimpleNamespace(messenger=SimpleNamespace(send=AsyncMock(side_effect=send)),
            _turn_reply=AsyncMock(return_value={'text':'已核实','actor_id':ACTOR['id']}),
            _validate_delivery=AsyncMock(),inbox=SimpleNamespace(set=Mock(),remember_public_reply=Mock()))
        self.runtime.feishu_assistant_channel=bot
        self.worker.call=AsyncMock(return_value={'conversation_id':CONVERSATION,'turns':[self.turn]})
        await self.worker.execute(self.task,self.job)
        self.task,self.job=self.worker.tasks.claim(self.worker.worker,time.time()+61)
        await self.worker.execute(self.task,self.job)
        self.assertEqual(posts,[('om_root','schedule_'+self.job['id'])])
        bot._turn_reply.assert_awaited_once()
        self.assertEqual(self.store.get_document('assistant_schedules','task:'+self.task['id'])['status'],'completed')

    async def test_unknown_delivery_past_dedupe_window_does_not_resend(self):
        self.worker.tasks.checkpoint(self.job,self.job['claim_token'],reply={'text':'pending'},delivery_started=time.time()-3601)
        self.worker.call=AsyncMock(return_value={'conversation_id':CONVERSATION,'turns':[]})
        self.worker.request_context=AsyncMock(return_value=(self.request,ACTOR,{'message_id':'schedule_'+self.job['id']}))
        self.runtime.feishu_assistant_channel=SimpleNamespace(messenger=SimpleNamespace(send=AsyncMock()))
        await self.worker.execute(self.task,self.job)
        self.runtime.feishu_assistant_channel.messenger.send.assert_not_awaited()
        self.assertEqual(self.store.get_document('assistant_schedules','task:'+self.task['id'])['status'],'paused')

    async def test_cached_attachment_delivery_checks_access_again(self):
        reply={'text':'资料已整理','actor_id':ACTOR['id'],'conversation_id':CONVERSATION,
               'files':[{'id':'f'*32,'file_key':'cached_key','name':'report.txt'}]}
        self.worker.tasks.checkpoint(self.job,self.job['claim_token'],reply=reply)
        message={'message_id':'schedule_'+self.job['id']}
        self.worker.request_context=AsyncMock(return_value=(self.request,ACTOR,message))
        self.worker.call=AsyncMock(return_value={'conversation_id':CONVERSATION,'turns':[]})
        bot=SimpleNamespace(_validate_delivery=AsyncMock(),
            _prepare_reply_files=AsyncMock(side_effect=AssistantError('附件权限已撤销。',403)),
            messenger=SimpleNamespace(send=AsyncMock()))
        self.runtime.feishu_assistant_channel=bot
        await self.worker.execute(self.task,self.job)
        bot._prepare_reply_files.assert_awaited_once()
        bot.messenger.send.assert_not_awaited()
        self.assertEqual(self.store.get_document('assistant_schedules','task:'+self.task['id'])['status'],'paused')

    async def test_lost_heartbeat_cancels_waiting_execution(self):
        import asyncio
        sleep=asyncio.sleep
        async def fast_sleep(seconds):
            await sleep(0 if seconds == 30 else seconds)
        async def wait_forever(*_,**__):
            await asyncio.Event().wait()
        self.worker.call=AsyncMock(side_effect=wait_forever)
        with patch('lan_bitable_template_portal.assistant_schedule_runtime.asyncio.sleep',new=fast_sleep), \
                patch.object(self.worker.tasks,'checkpoint',side_effect=AssistantError('领取已失效。',409)):
            with self.assertRaises(asyncio.CancelledError):
                await self.worker.execute(self.task,self.job)
        self.assertEqual(self.store.get_document('assistant_schedules','run:'+self.job['id'])['status'],'pending')

    def test_authority_rejects_replaced_claim(self):
        schedule={**self.request.state.assistant_schedule,'run_id':self.job['id'],
                  'claim_token':self.job['claim_token']}
        self.worker.validate(schedule)
        current=dict(self.job,claim_token='replaced')
        self.store.put_document('assistant_schedules','run:'+self.job['id'],current)
        with self.assertRaises(AssistantError):self.worker.validate(schedule)

    async def test_new_requests_use_separate_lane_and_current_authority(self):
        from lan_bitable_template_portal.lighthouse_bridge import PortalAuthority
        original={**ACTOR,'learning_scopes':[]}
        auth=PortalAuthority.__new__(PortalAuthority)
        auth.client=SimpleNamespace(lease='lease')
        auth.controller=object()
        auth.runtime=SimpleNamespace(assistant_scheduler=self.worker)
        auth.contexts={'ctx':{'expires':time.monotonic()+100,'lease':'lease','request':self.request,
            'actor':original,'schedule':self.request.state.assistant_schedule}}
        with patch('lan_bitable_template_portal.lighthouse_bridge.actor_for',AsyncMock(return_value=original)):
            _,actor=await auth.authorize('ctx')
            self.assertEqual(actor['_execution_lane'],'schedule:'+self.task['id'])
            self.assertEqual(actor['_scheduled_task'],self.task['id'])
            self.assertEqual(actor['_scheduled_mode'],'task')
        changed={**original,'scopes':[]}
        with patch('lan_bitable_template_portal.lighthouse_bridge.actor_for',AsyncMock(return_value=changed)):
            with self.assertRaises(AssistantError):await auth.authorize('ctx')

    async def test_background_business_commit_is_blocked_before_native_write(self):
        from lan_bitable_template_portal.lighthouse_bridge import PortalAuthority
        authority=PortalAuthority.__new__(PortalAuthority)
        catalog=SimpleNamespace(get=Mock(return_value={'method':'POST'}),
            validate_operation=Mock(return_value=({'api_id':'POST /api/notices','method':'POST','path':'/api/notices'},[])),
            invoke=AsyncMock())
        authority.get_catalog=lambda:catalog
        with self.assertRaisesRegex(AssistantError,'必须由用户确认'):
            await authority.invoke({'schedule':{'id':self.task['id']}},ACTOR,{'operation':{'api_id':'POST /api/notices'}})
        catalog.invoke.assert_not_awaited()

    async def test_literal_reminder_uses_no_model_or_business_tools(self):
        from openclaw_service.assistant.lighthouse_model import LighthouseModel
        actor={**ACTOR,'_scheduled_task':self.task['id'],'_scheduled_mode':'reminder'}
        emit=AsyncMock()
        result=await LighthouseModel.answer(SimpleNamespace(),actor,{'question':'【定时任务】记得核对任务'},[],None,
            emit,AsyncMock(return_value=actor),{})
        self.assertEqual(result['answer'],'【定时任务】记得核对任务')
        self.assertEqual(result['sources'],[])


if __name__ == '__main__':
    unittest.main()

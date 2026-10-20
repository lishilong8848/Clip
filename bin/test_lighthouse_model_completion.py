"""Answer completion regressions using scripted gateways and isolated tool data."""
import asyncio
import copy
import json
import sys
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pydantic_ai import ModelRetry, Tool
from pydantic_ai.models.function import FunctionModel
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_openclaw import OpenClawToolAgent
from openclaw_service.assistant.lighthouse_model import LighthouseModel
from openclaw_service.assistant.lighthouse_queries import PortalOperation
from bin import test_lighthouse_model_failover as failover
from bin import test_lighthouse_openclaw as fixtures


PROCESS = '我先查询当前维修记录。'
FINAL = 'D楼当前共有1条维修记录。'
PARALLEL_QUERY_INTRO = ('我直接拉上周 5 个楼栋的学练历史，每楼一页 40 条记录'
                       '(period=week,date 范围 2026-10-12 至 2026-10-18)。并行发起:')


class ModelCompletionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.fixture = failover.ModelFailoverTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)

    async def test_missing_deliverable_is_completed_from_receipts_without_repeating_tools(self):
        agent, effects, attempts = self.fixture.agent, [], []
        agent._buffered_reply = True
        partial, complete = '共有2人。', '共有2人：测试甲、测试乙。'
        async def read():
            effects.append('read')
            return {'ok': True, 'data': {'participants': [{'name': '测试甲'}, {'name': '测试乙'}]}}
        agent.tools['lighthouse_query'] = Tool(read)
        async def validate(answer):
            if answer == partial:
                raise ModelRetry('请补充原回执中已经提供的两个人名。')
            return answer
        agent.validators.append(validate)
        async def hook(client):
            submissions = [params for method, params in client.calls if method == 'agent']
            if not effects:
                await self.fixture.engine.bridge.call(agent.token, {'session_key': agent.session_key,
                    'run_id': agent.run_id, 'tool': 'lighthouse_query', 'call_id': 'first-read', 'params': {}})
            if len(submissions) == 2:
                attempts.append(submissions[-1])
                client.wait = {'status': 'ok', 'terminalReply': {'disposition': 'visible', 'text': complete}}
                with self.assertRaises(AssistantError):
                    await self.fixture.engine.bridge.call(agent.token, {'session_key': agent.session_key,
                        'run_id': agent.run_id, 'tool': 'lighthouse_query', 'call_id': 'must-not-requery', 'params': {}})
        self.fixture.scripts['current'] = {'hook': hook, 'wait': {'status': 'ok',
            'terminalReply': {'disposition': 'visible', 'text': partial}}, 'events': [
            {'event': 'agent', 'payload': {'runId': 'run-1', 'stream': 'assistant', 'data': {'delta': partial}}}]}
        events = await self.fixture.run_agent()
        self.assertEqual(events[-1].result.output, complete)
        self.assertEqual(effects, ['read'])
        self.assertEqual(len(attempts), 1)
        self.assertIn('测试乙', attempts[0]['message'])
        visible = ''.join(event.part.content if event.event_kind == 'part_start' else event.delta.content_delta
                          for event in events if event.event_kind in {'part_start', 'part_delta'})
        self.assertEqual(visible, complete)
        self.assertEqual(agent.calls, 1)
        self.assertFalse(agent._evidence_only)

    async def test_suppressed_terminal_reply_uses_existing_backup_chain(self):
        self.fixture.scripts['current'] = {'wait': {
            'status': 'ok', 'terminalReply': {'disposition': 'suppressed'}}}
        events = await self.fixture.run_agent()
        self.assertEqual(events[-1].result.output, 'fixture reply')
        self.assertEqual(self.fixture.selected, ['current', 'backup'])

    async def test_completed_empty_reply_uses_existing_backup_chain(self):
        self.fixture.scripts['current'] = {'accept': {'status': 'ok'}, 'wait': {
            'status': 'ok', 'terminalReply': {'disposition': 'visible', 'text': ''}}}
        events = await self.fixture.run_agent()
        self.assertEqual(events[-1].result.output, 'fixture reply')
        self.assertEqual(self.fixture.selected, ['current', 'backup'])

    async def test_evidence_continuation_blocks_new_tool_calls_and_keeps_receipts(self):
        agent = self.fixture.agent
        effects, submissions, rejected = [], [], []

        async def submit():
            effects.append('accepted')
            return {'ok': True, 'job_id': 'original-job', 'count': 1}

        agent.tools['lighthouse_submit'] = Tool(submit)

        async def validate(answer):
            if answer == PROCESS:
                raise ModelRetry('请根据已取得的工具证据给出结果，不要只说明将要查询。')
            return answer

        agent.validators.append(validate)

        async def client_for(actor, item):
            client = fixtures.FakeGatewayClient('ws://127.0.0.1:19999', 'fixture-token')
            original = client.request

            async def request(method, params=None, timeout=None):
                if method == 'agent':
                    submissions.append(copy.deepcopy(params))
                    client.wait = {'status': 'ok', 'terminalReply': {
                        'disposition': 'visible', 'text': PROCESS if len(submissions) == 1 else FINAL}}
                return await original(method, params, timeout)

            client.request = request
            original_event = client.next_event

            async def next_event(timeout):
                payload = {'session_key': agent.session_key, 'run_id': agent.run_id,
                           'tool': 'lighthouse_submit', 'call_id': 'original-call', 'params': {}}
                if not effects:
                    await self.fixture.engine.bridge.call(agent.token, payload)
                elif len(submissions) == 2 and not rejected:
                    receipt = await self.fixture.engine.bridge.call(agent.token, payload)
                    self.assertEqual(receipt['job_id'], 'original-job')
                    with self.assertRaises(AssistantError):
                        await self.fixture.engine.bridge.call(agent.token, {**payload, 'call_id': 'new-call'})
                    rejected.append(True)
                return await original_event(timeout)

            client.next_event = next_event
            self.fixture.clients.append(client)
            return client

        self.fixture.engine.gateway_client = client_for
        events = await self.fixture.run_agent()
        self.assertEqual(events[-1].result.output, FINAL)
        self.assertEqual(effects, ['accepted'])
        self.assertEqual(rejected, [True])
        self.assertEqual(agent.calls, 1)
        self.assertEqual(len(submissions), 2)
        self.assertEqual(submissions[0]['sessionKey'], submissions[1]['sessionKey'])
        self.assertIn('original-job', submissions[1]['message'])
        self.assertIn('original question', submissions[1]['message'])
        self.assertFalse(self.fixture.engine.bridge.active)
        self.assertFalse(agent._evidence_only)

    def incomplete_script(self, *, unknown=False, stall=False, final=PROCESS):
        agent, effects = self.fixture.agent, []

        async def submit():
            effects.append('submitted')
            if unknown:
                raise asyncio.TimeoutError()
            return {'ok': True, 'job_id': 'original-job'}

        agent.tools['lighthouse_submit'] = Tool(submit)

        async def validate(answer):
            if answer == PROCESS:
                raise ModelRetry('请根据已有回执完成回答。')
            return answer

        agent.validators.append(validate)

        async def hook(client):
            if not effects:
                await self.fixture.engine.bridge.call(agent.token, {
                    'session_key': agent.session_key, 'run_id': agent.run_id,
                    'tool': 'lighthouse_submit', 'call_id': 'original-call', 'params': {}})
            if sum(method == 'agent' for method, _ in client.calls) == 2:
                if stall:
                    await asyncio.Event().wait()
                client.wait = {'status': 'ok', 'terminalReply': {'disposition': 'visible', 'text': final}}

        self.fixture.scripts['current'] = {'hook': hook, 'wait': {
            'status': 'ok', 'terminalReply': {'disposition': 'visible', 'text': PROCESS}}}
        return effects

    async def test_incomplete_continuation_stops_after_one_retry_without_success(self):
        effects = self.incomplete_script()
        self.fixture.scripts['backup'] = {'wait': {'status': 'ok', 'terminalReply': {
            'disposition': 'visible', 'text': PROCESS}}}
        events = []
        with self.assertRaises(AssistantError):
            async with self.fixture.agent.run_stream_events(['original question'], message_history=[]) as stream:
                async for event in stream:
                    events.append(event)
        self.assertEqual(effects, ['submitted'])
        self.assertEqual(sum(method == 'agent' for method, _ in self.fixture.clients[0].calls), 2)
        self.assertFalse(any(event.event_kind == 'agent_run_result' for event in events))
        self.assertEqual(self.fixture.selected, ['current', 'backup'])
        self.assertEqual(sum(method == 'agent' for client in self.fixture.clients for method, _ in client.calls), 4)
        self.assertFalse(self.fixture.engine.bridge.active)
        self.assertFalse(self.fixture.agent._evidence_only)

    async def test_unknown_tool_receipt_is_preserved_in_honest_continuation(self):
        answer = '原操作超时，结果未知，请核对原任务，不能确认是否成功。'
        effects = self.incomplete_script(unknown=True, final=answer)
        events = await self.fixture.run_agent()
        self.assertIn('未知', events[-1].result.output)
        self.assertNotIn('已完成', events[-1].result.output)
        self.assertEqual(effects, ['submitted'])
        submissions = [params for method, params in self.fixture.clients[0].calls if method == 'agent']
        self.assertEqual(len(submissions), 2)
        self.assertIn('结果未知', submissions[1]['message'])
        self.assertEqual(self.fixture.selected, ['current'])

    async def test_continuation_obeys_overall_timeout_and_cleans_up(self):
        effects = self.incomplete_script(stall=True)
        original_timeout = asyncio.timeout
        with patch('openclaw_service.assistant.lighthouse_openclaw.asyncio.timeout',
                   side_effect=lambda _: original_timeout(.05)):
            with self.assertRaises(AssistantError) as raised:
                await self.fixture.run_agent()
        self.assertEqual(raised.exception.category, 'timeout')
        self.assertEqual(effects, ['submitted'])
        self.assertEqual(self.fixture.selected, ['current'])
        self.assertFalse(self.fixture.engine.bridge.active)
        self.assertFalse(self.fixture.items[0]['busy'])
        self.assertFalse(self.fixture.agent._evidence_only)

    async def test_general_incomplete_replies_retry_then_use_existing_backup(self):
        for answer in ('👍', 'NO_REPLY', 'no_reply', '我先整理', PARALLEL_QUERY_INTRO):
            with self.subTest(answer=answer):
                self.fixture.setUp()
                self.fixture.agent.turn['question'] = '现在什么模式'
                if answer == PARALLEL_QUERY_INTRO:
                    chunks = ['我', '直接', '拉', answer[4:].split('。')[0], '。', '并', '行', '发', '起', ':']
                else:
                    chunks = ['我', '先', '整理'] if answer == '我先整理' else ['no', '_reply'] if answer == 'no_reply' else [answer]
                self.fixture.scripts['current'] = {'wait': {'status': 'ok', 'terminalReply': {
                    'disposition': 'visible', 'text': answer}}, 'events': [
                        {'event': 'agent', 'payload': {'runId': 'run-1', 'stream': 'assistant',
                            'data': {'delta': chunk}}} for chunk in chunks]}
                events = await self.fixture.run_agent(['现在什么模式'])
                self.assertEqual(events[-1].result.output, 'fixture reply')
                self.assertEqual(self.fixture.selected, ['current', 'backup'])
                submissions = [params for client in self.fixture.clients for method, params in client.calls if method == 'agent']
                self.assertEqual(len(submissions), 3)
                self.assertEqual(submissions[1]['idempotencyKey'], submissions[0]['idempotencyKey'] + ':validation')
                text = ''.join(event.part.content if event.event_kind == 'part_start' else event.delta.content_delta
                               for event in events if event.event_kind in {'part_start', 'part_delta'})
                self.assertEqual(text, 'fixture reply')
                self.assertEqual(self.fixture.agent.calls, 0)

    async def test_all_incomplete_models_fail_with_bounded_attempts_and_no_success(self):
        for identity in ('current', 'backup'):
            self.fixture.scripts[identity] = {'wait': {'status': 'ok', 'terminalReply': {
                'disposition': 'visible', 'text': 'NO_REPLY'}}}
        events = []
        with self.assertRaises(AssistantError):
            async with self.fixture.agent.run_stream_events(['现在什么模式'], message_history=[]) as stream:
                async for event in stream:
                    events.append(event)
        self.assertEqual(self.fixture.selected, ['current', 'backup'])
        self.assertEqual(sum(method == 'agent' for client in self.fixture.clients for method, _ in client.calls), 4)
        self.assertFalse(any(event.event_kind in {'part_start', 'part_delta', 'agent_run_result'} for event in events))

    async def test_explicit_emoji_request_and_substantive_short_answers_do_not_retry(self):
        for question, answer in (('只用一个表情回复我', '👍'), ('请只回复👍', '👍'),
                                 ('简短回答现在什么模式', '标准模式'), ('能帮忙吗，简短回复', '可以')):
            with self.subTest(question=question):
                self.fixture.setUp()
                self.fixture.agent.turn['question'] = question
                self.fixture.scripts['current'] = {'wait': {'status': 'ok', 'terminalReply': {
                    'disposition': 'visible', 'text': answer}}}
                events = await self.fixture.run_agent([question])
                self.assertEqual(events[-1].result.output, answer)
                self.assertEqual(self.fixture.selected, ['current'])
                self.assertEqual(sum(method == 'agent' for client in self.fixture.clients for method, _ in client.calls), 1)

    async def test_meaningful_text_after_withheld_prefix_streams_once(self):
        answer = '我可以帮你整理资料。'
        self.fixture.scripts['current'] = {'wait': {'status': 'ok', 'terminalReply': {
            'disposition': 'visible', 'text': answer}}, 'events': [
                {'event': 'agent', 'payload': {'runId': 'run-1', 'stream': 'assistant', 'data': {'delta': chunk}}}
                for chunk in ('我', '可以帮你', '整理资料。')]}
        events = await self.fixture.run_agent(['现在什么模式'])
        self.assertEqual(events[-1].result.output, answer)
        self.assertEqual(''.join(event.delta.content_delta for event in events if event.event_kind == 'part_delta'), answer)

    async def test_incomplete_terminal_after_real_output_cannot_retry_or_claim_success(self):
        self.fixture.agent._live_text_only = True
        self.fixture.scripts['current'] = {'wait': {'status': 'ok', 'terminalReply': {
            'disposition': 'visible', 'text': 'NO_REPLY'}}, 'events': [
                {'event': 'agent', 'payload': {'runId': 'run-1', 'stream': 'assistant',
                    'data': {'delta': '我可以帮你整理资料。'}}}]}
        events = []
        with self.assertRaises(AssistantError):
            async with self.fixture.agent.run_stream_events(['现在什么模式'], message_history=[]) as stream:
                async for event in stream:
                    events.append(event)
        self.assertEqual(self.fixture.selected, ['current'])
        self.assertEqual(sum(method == 'agent' for client in self.fixture.clients for method, _ in client.calls), 1)
        self.assertFalse(any(event.event_kind == 'agent_run_result' for event in events))

    async def test_evidence_cancellation_clears_completion_mode(self):
        effects = self.incomplete_script(stall=True)
        entered = asyncio.Event()
        original = self.fixture.scripts['current']['hook']

        async def hook(client):
            if sum(method == 'agent' for method, _ in client.calls) == 2:
                entered.set()
            await original(client)

        self.fixture.scripts['current']['hook'] = hook
        task = asyncio.create_task(self.fixture.run_agent())
        await asyncio.wait_for(entered.wait(), .5)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(effects, ['submitted'])
        self.assertFalse(self.fixture.agent._evidence_only)
        self.assertFalse(self.fixture.engine.bridge.active)

    async def test_reusing_agent_after_completion_allows_next_turn_tools(self):
        effects = self.incomplete_script(final='原任务已接收，编号original-job。')
        await self.fixture.run_agent()
        self.assertFalse(self.fixture.agent._evidence_only)
        agent, reads = self.fixture.agent, []
        agent.turn['run_id'] = 'next-turn'

        async def read_next():
            reads.append('next-turn')
            return {'ok': True, 'value': 'new evidence'}

        agent.tools['lighthouse_read_next'] = Tool(read_next)

        async def hook(client):
            await self.fixture.engine.bridge.call(agent.token, {'session_key': agent.session_key,
                'run_id': agent.run_id, 'tool': 'lighthouse_read_next', 'call_id': 'next-call', 'params': {}})

        self.fixture.scripts['current'] = {'hook': hook}
        events = await self.fixture.run_agent(['下一轮的新问题'])
        self.assertEqual(events[-1].result.output, 'fixture reply')
        self.assertEqual(reads, ['next-turn'])
        self.assertEqual(effects, ['submitted'])
        self.assertEqual(agent.calls, 2)
        self.assertIn('original-call', agent.results)
        self.assertFalse(agent._evidence_only)

    async def test_saved_read_and_accepted_write_receipts_can_use_backup_without_replay(self):
        for name, unknown in (('query', False), ('submit', False), ('submit', True)):
            with self.subTest(tool=name, unknown=unknown):
                self.fixture.setUp()
                agent, effects, rejected = self.fixture.agent, [], []
                original_session = agent.session_key
                agent.turn['question'] = '原问题'

                async def operation():
                    effects.append('accepted')
                    if unknown:
                        raise asyncio.TimeoutError()
                    return {'ok': True, 'job_id': 'saved-receipt', 'count': 1, 'api_key': 'private-fixture-key'}

                agent.tools['lighthouse_' + name] = Tool(operation)

                async def first(client):
                    await self.fixture.engine.bridge.call(agent.token, {'session_key': agent.session_key,
                        'run_id': agent.run_id, 'tool': 'lighthouse_' + name, 'call_id': 'saved-call', 'params': {}})

                async def backup(client):
                    payload = {'session_key': agent.session_key, 'run_id': agent.run_id,
                               'tool': 'lighthouse_' + name, 'call_id': 'new-write', 'params': {}}
                    with self.assertRaises(AssistantError):
                        await self.fixture.engine.bridge.call(agent.token, payload)
                    rejected.append(True)

                self.fixture.fail(hook=first)
                self.fixture.scripts['backup'] = {'hook': backup, 'wait': {'status': 'ok', 'terminalReply': {
                    'disposition': 'visible', 'text': '操作已完成。' if unknown else '已按原回执核对，数量为1。'}}}
                events = await self.fixture.run_agent(['原问题\n不可复制到备用模型的额外资料'])
                self.assertEqual(self.fixture.selected, ['current', 'backup'])
                self.assertEqual(effects, ['accepted'])
                self.assertTrue(rejected)
                self.assertEqual(agent.calls, 1)
                self.assertIn('saved-call', agent.results)
                self.assertFalse(agent._evidence_only)
                self.assertEqual(agent.session_key, original_session)
                requests = [params for client in self.fixture.clients for method, params in client.calls if method == 'agent']
                self.assertNotEqual(requests[0]['sessionKey'], requests[1]['sessionKey'])
                self.assertNotIn('private-fixture-key', requests[1]['message'])
                self.assertNotIn('不可复制到备用模型的额外资料', requests[1]['message'])
                if unknown:
                    self.assertIn('未知', events[-1].result.output)
                    self.assertNotIn('已完成', events[-1].result.output)
                else:
                    self.assertIn('saved-receipt', requests[1]['message'])
                    self.assertIn('数量为1', events[-1].result.output)

    def test_evidence_continuation_preserves_partial_and_pagination_markers(self):
        data = {'participants': [{'name': '姓名' + str(index)} for index in range(55)],
                'complete': False, 'truncated': True, 'names_complete': False, 'has_more': True,
                'next_offset': 55, 'next_page': 2, 'summary': {'participants': 100}}
        self.fixture.agent.results['partial-list'] = ('signature', {'ok': True, 'data': data})
        message = self.fixture.agent._complete_from_evidence('原问题', '仅依据回执整理。')
        snapshot = json.loads(message[message.index('[{'):])[0]['data']
        self.assertEqual(snapshot, data)
        self.assertIn('不得称为完整', message)

    async def test_missing_receipt_and_published_text_still_block_tool_fallback(self):
        for missing in (False, True):
            with self.subTest(missing=missing):
                self.fixture.setUp()
                agent = self.fixture.agent
                agent._live_text_only = True
                agent.calls = 1
                if not missing:
                    agent.results['receipt'] = ('signature', {'ok': True, 'job_id': 'accepted'})
                self.fixture.fail()
                if not missing:
                    self.fixture.scripts['current']['events'].insert(0, {'event': 'agent', 'payload': {
                        'runId': 'run-1', 'stream': 'assistant', 'data': {'delta': '已输出的实际内容'}}})
                with self.assertRaises(AssistantError):
                    await self.fixture.run_agent()
                self.assertEqual(self.fixture.selected, ['current'])

    async def test_tool_fallback_remains_bounded_to_four_models(self):
        profiles = [failover.profile('current'), *[failover.profile('backup' + str(index)) for index in range(4)]]
        self.fixture.configure(self.fixture.actor['id'], profiles)
        agent, effects = self.fixture.agent, []

        async def query():
            effects.append('read')
            return {'ok': True, 'count': 1}

        agent.tools['lighthouse_query'] = Tool(query)

        async def hook(client):
            await self.fixture.engine.bridge.call(agent.token, {'session_key': agent.session_key,
                'run_id': agent.run_id, 'tool': 'lighthouse_query', 'call_id': 'original-read', 'params': {}})

        for item in profiles:
            self.fixture.fail(item['id'])
        self.fixture.scripts['current']['hook'] = hook
        with self.assertRaises(AssistantError):
            await self.fixture.run_agent()
        self.assertEqual(self.fixture.selected, ['current', 'backup0', 'backup1', 'backup2'])
        self.assertEqual(effects, ['read'])
        self.assertFalse(agent._evidence_only)
        self.assertNotIn(':evidence:', agent.session_key)
        self.assertFalse(self.fixture.engine.bridge.active)

    async def test_tool_fallback_keeps_one_global_timeout(self):
        agent, effects = self.fixture.agent, []

        async def query():
            effects.append('read')
            return {'ok': True, 'count': 1}

        agent.tools['lighthouse_query'] = Tool(query)

        async def first(client):
            await self.fixture.engine.bridge.call(agent.token, {'session_key': agent.session_key,
                'run_id': agent.run_id, 'tool': 'lighthouse_query', 'call_id': 'original-read', 'params': {}})

        async def stall(client):
            await asyncio.Event().wait()

        self.fixture.fail(hook=first)
        self.fixture.scripts['backup'] = {'hook': stall}
        original_timeout = asyncio.timeout
        with patch('openclaw_service.assistant.lighthouse_openclaw.asyncio.timeout',
                   side_effect=lambda _: original_timeout(.05)) as timeout:
            with self.assertRaises(AssistantError) as raised:
                await self.fixture.run_agent()
        timeout.assert_called_once_with(240)
        self.assertEqual(raised.exception.category, 'timeout')
        self.assertEqual(self.fixture.selected, ['current', 'backup'])
        self.assertEqual(effects, ['read'])
        self.assertTrue(all(not item['busy'] for item in self.fixture.items))
        self.assertFalse(agent._evidence_only)
        self.assertNotIn(':evidence:', agent.session_key)


class ModelOutputValidationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fixture = fixtures.AgentFactoryHookTests()
        await self.fixture.asyncSetUp()
        self.addCleanup(self.fixture.tmp.cleanup)
        fixtures.FakeGatewayClient._reset()
        self.addCleanup(fixtures.FakeGatewayClient._reset)
        self.agent = fixtures.FakeAgent(None)
        await self.fixture._run_answer(lambda *_args, **_kwargs: self.agent, '查询D楼维修记录有几条', [])
        await self.agent.tools['query'](PortalOperation(
            api_id='GET /api/repair-management/records', params={'scope': 'D'}))

    async def test_tool_evidence_does_not_turn_process_text_into_a_complete_answer(self):
        for answer in (PROCESS, '我先查询当前记录。\n正在整理查询资料。', '👍', 'NO_REPLY'):
            with self.subTest(answer=answer):
                with self.assertRaises(ModelRetry):
                    await self.agent.validators[0](answer)

    async def test_actual_answer_and_honest_limit_are_accepted(self):
        for answer in (FINAL, '我先查询当前记录。\nD楼当前共有1条维修记录。',
                       '本次查询超时，暂无法核实实际数量。'):
            with self.subTest(answer=answer):
                self.assertEqual(await self.agent.validators[0](answer), answer)

    async def test_real_parallel_query_preamble_requires_a_result(self):
        for answer in (PARALLEL_QUERY_INTRO, PARALLEL_QUERY_INTRO[:-1] + '：'):
            with self.subTest(answer=answer):
                with self.assertRaises(ModelRetry):
                    await self.agent.validators[0](answer)
        completed = PARALLEL_QUERY_INTRO + '\n实际查询结果：D楼当前共有1条维修记录。[1]'
        self.assertEqual(await self.agent.validators[0](completed), completed)

    async def test_general_chat_without_sources_rejects_incomplete_replies(self):
        agent = fixtures.FakeAgent(None)
        await self.fixture._run_answer(lambda *_args, **_kwargs: agent, '现在什么模式', [])
        for answer in ('👍', 'NO_REPLY', 'HEARTBEAT_OK', '我先整理'):
            with self.subTest(answer=answer):
                with self.assertRaises(ModelRetry):
                    await agent.validators[0](answer)

    async def test_sdk_general_retry_does_not_concatenate_rejected_placeholder(self):
        for placeholder in ('👍', 'NO_REPLY', '我先整理。', PARALLEL_QUERY_INTRO):
            with self.subTest(placeholder=placeholder):
                answers, calls, emitted = iter((placeholder, '标准模式。')), [], []

                async def stream(messages, info):
                    calls.append(1)
                    yield next(answers)

                @asynccontextmanager
                async def model(*_):
                    yield FunctionModel(stream_function=stream)

                async def emit(kind, value):
                    if kind == 'text':
                        emitted.append(value['delta'])

                engine = LighthouseModel(self.fixture.portal, model_factory=model)
                self.addAsyncCleanup(engine.close)
                result = await engine.answer(self.fixture.actor, {'question': '现在什么模式', 'file_ids': [],
                    '_profile': {'id': 'fixture', 'name': 'Fixture', 'model': 'fixture'}}, [], self.fixture.request,
                    emit, self.fixture.authorize, {})
                self.assertEqual(result['answer'], '标准模式。')
                self.assertEqual(''.join(emitted), '标准模式。')
                self.assertEqual(len(calls), 2)

    async def test_general_chat_respects_explicit_format_without_blanket_short_reply_exemption(self):
        for question, answer, accepted in (
                ('只用一个表情回复我', '👍', True), ('请用一个表情回应', '👍', True),
                ('只要一个表情', '👍', True), ('发个表情就好', '👍', True), ('只回复。', '。', True),
                ('请只回复👍', '👍', True), ('只回复“我先整理”', '我先整理', True),
                ('请原样输出NO_REPLY', 'NO_REPLY', True), ('简短回答现在什么模式', '标准模式', True),
                ('简短回答现在什么模式', '👍', False), ('只用一个表情回复我', 'NO_REPLY', False),
                ('不要只回表情，告诉我现在什么模式', '👍', False),
                ('不要回复“我先整理”，告诉我现在什么模式', '我先整理', False)):
            with self.subTest(question=question, answer=answer):
                agent = fixtures.FakeAgent(None)
                await self.fixture._run_answer(lambda *_args, **_kwargs: agent, question, [])
                if accepted:
                    self.assertEqual(await agent.validators[0](answer), answer)
                else:
                    with self.assertRaises(ModelRetry):
                        await agent.validators[0](answer)

    async def test_real_model_query_backup_reads_tail_from_authorized_snapshot_only(self):
        await self.real_query_snapshot_backup(revoke=False)

    async def test_backup_snapshot_read_rechecks_current_authorization(self):
        await self.real_query_snapshot_backup(revoke=True)

    async def test_backup_snapshot_tail_does_not_claim_unfetched_api_pages_are_complete(self):
        await self.real_query_snapshot_backup(revoke=False, partial=True)

    async def real_query_snapshot_backup(self, *, revoke, partial=False):
        flow = failover.ModelFailoverTests()
        flow.setUp()
        self.addCleanup(flow.tearDown)
        self.fixture.actor['learning_scopes'] = ['D']
        flow.actor = self.fixture.actor
        flow.configure(flow.actor['id'], flow.profiles)
        reads, receipts, tails = [], [], []
        participants = [{'person_id': 'p-' + str(index), 'name': '测试姓名' + str(index), 'scopes': ['D'],
                         'answered': 1, 'phone': '13800138000', 'password': 'fixture-password'}
                        for index in range(1, 56)]

        @self.fixture.app.get('/api/learning/profile')
        async def participation(scope: str, view: str):
            reads.append((scope, view))
            return {'ok': True, 'data': {'kind': 'learning_participation', 'scope': scope,
                'complete': not partial, 'truncated': False, 'names_complete': True, 'has_more': partial,
                'next_offset': 55 if partial else None, 'summary': {'participants': 100 if partial else 55}, 'participants': participants}}

        self.fixture.portal.catalog = fixtures.PortalAPICatalog(self.fixture.app)

        def factory(_model, *, actor, turn, emit, **kwargs):
            agent = OpenClawToolAgent(flow.engine, actor, turn, emit, flow.profiles[0], **kwargs)
            flow.agent = agent
            agent._acquire_runtime = AsyncMock(side_effect=flow.acquire)
            return agent

        async def first(client):
            agent = flow.agent
            receipt = await flow.engine.bridge.call(agent.token, {'session_key': agent.session_key,
                'run_id': agent.run_id, 'tool': 'lighthouse_query', 'call_id': 'original-query',
                'params': {'operation': {'api_id': 'GET /api/learning/profile',
                                         'params': {'scope': 'D', 'view': 'participation'}}}})
            self.assertTrue(receipt['ok'], receipt)
            self.assertTrue(receipt['sample_limited'])
            self.assertEqual(len(receipt['data']['participants']), 40)
            self.assertEqual(receipt['data']['participants'][-1]['name'], '测试姓名40')
            self.assertEqual(receipt['data']['has_more'], partial)
            if partial:
                self.assertEqual(receipt['query_state'], 'partial')
            receipts.append(receipt)

        async def backup(client):
            agent, receipt = flow.agent, receipts[0]
            message = next(params['message'] for method, params in client.calls if method == 'agent')
            self.assertIn(receipt['query_ref'], message)
            self.assertNotIn('测试姓名55', message)
            self.assertIn('read_query', message)
            for tool, params in (('lighthouse_query', {'operation': {'api_id': 'GET /api/learning/profile',
                                    'params': {'scope': 'D', 'view': 'participation'}}}),
                                 ('lighthouse_prepare_business', {}),
                                 ('lighthouse_call_extension', {'server_id': 'fixture-unconfigured', 'tool': 'fixture', 'arguments': {}})):
                self.assertIn(tool, agent.tools)
                with self.assertRaisesRegex(AssistantError, '仅允许read_query'):
                    await flow.engine.bridge.call(agent.token, {'session_key': agent.session_key,
                        'run_id': agent.run_id, 'tool': tool, 'call_id': 'forbidden-' + tool, 'params': params})
            if revoke:
                self.fixture.actor = {**self.fixture.actor, 'scopes': []}
            tail = await flow.engine.bridge.call(agent.token, {'session_key': agent.session_key,
                'run_id': agent.run_id, 'tool': 'lighthouse_read_query', 'call_id': 'read-tail',
                'params': {'query_ref': receipt['query_ref'], 'path': ['participants'], 'offset': 40, 'limit': 20}})
            tails.append(tail)
            if revoke:
                self.assertFalse(tail['ok'])
                self.assertEqual(tail['status'], 403)
                self.assertNotIn('测试姓名55', json.dumps(tail, ensure_ascii=False))
                client.wait = {'status': 'ok', 'terminalReply': {'disposition': 'visible', 'text': '已查全55人，末位测试姓名55。'}}
                return
            self.assertTrue(tail['ok'], tail)
            self.assertEqual(tail['original_query_ref'], receipt['query_ref'])
            self.assertEqual(tail['data']['loaded_count'], 55)
            self.assertTrue(tail['data']['snapshot_only'])
            self.assertIsNone(tail['next_offset'])
            names = [row['name'] for row in receipt['data']['participants'] + tail['data']['items']]
            self.assertEqual(names, [row['name'] for row in participants])
            for value in (message, json.dumps(tail, ensure_ascii=False)):
                for sensitive in ('13800138000', 'fixture-password', '"phone"', '"password"'):
                    self.assertNotIn(sensitive, value)
            client.wait = {'status': 'ok', 'terminalReply': {'disposition': 'visible',
                'text': '快照中共55人，末位为' + names[-1] + '。'
                    + ('原接口仍有下一页，完整名单尚未查全。' if partial else '')}}

        flow.fail(hook=first)
        flow.scripts['backup'] = {'hook': backup}
        self.fixture.emit_log.clear()
        if revoke:
            with self.assertRaisesRegex(AssistantError, '权限已变化') as caught:
                await self.fixture._run_answer(factory, '查询D楼人员明细', [])
            self.assertEqual(caught.exception.status, 403)
        else:
            await self.fixture._run_answer(factory, '查询D楼人员明细', [])
        self.assertEqual(flow.selected, ['current', 'backup'])
        self.assertEqual(reads, [('D', 'participation')])
        self.assertEqual(len(tails), 1)
        self.assertEqual(flow.agent.calls, 2)
        self.assertEqual(self.fixture.writes, [])
        text = ''.join(value['delta'] for kind, value in self.fixture.emit_log if kind == 'text')
        if revoke:
            self.assertEqual(text, '')
            self.assertNotIn('测试姓名55', text)
            self.assertNotIn('已查全', text)
        else:
            self.assertIn('快照中共55人,末位为测试姓名55。', text)
            if partial:
                self.assertIn('原接口仍有下一页', text)
                self.assertIn('完整名单尚未查全', text)

    async def test_real_query_evidence_completes_once_and_only_final_text_is_emitted(self):
        engine = fixtures._fake_engine()
        engine.keep_gateway = AsyncMock()
        agents = []
        submissions = []
        self.fixture.reads.clear()
        self.fixture.emit_log.clear()

        def factory(_model, *, actor, turn, emit, **kwargs):
            agent = OpenClawToolAgent(engine, actor, turn, emit, turn['_profile'], **kwargs)
            agent._acquire_runtime = AsyncMock(return_value={'port': 19999, 'token': 'fixture-token'})
            agents.append(agent)
            return agent

        async def client_for(_actor, _item):
            agent = agents[-1]
            client = fixtures.FakeGatewayClient('ws://127.0.0.1:19999', 'fixture-token')
            original = client.request

            async def request(method, params=None, timeout=None):
                if method == 'agent':
                    submissions.append(copy.deepcopy(params))
                    client.wait = {'status': 'ok', 'terminalReply': {
                        'disposition': 'visible', 'text': PROCESS if len(submissions) == 1 else FINAL}}
                return await original(method, params, timeout)

            client.request = request
            original_event = client.next_event

            async def next_event(timeout):
                if not agent.calls:
                    await engine.bridge.call(agent.token, {'session_key': agent.session_key,
                        'run_id': agent.run_id, 'tool': 'lighthouse_query', 'call_id': 'query-once',
                        'params': {'operation': {'api_id': 'GET /api/repair-management/records', 'params': {'scope': 'D'}}}})
                return await original_event(timeout)

            client.next_event = next_event
            return client

        engine.gateway_client = client_for
        await self.fixture._run_answer(factory, '查询D楼维修记录有几条', [])
        self.assertEqual(self.fixture.reads, ['D'])
        self.assertEqual(len(submissions), 2)
        self.assertIn('r1', submissions[1]['message'])
        self.assertEqual([value['delta'] for kind, value in self.fixture.emit_log if kind == 'text'], [FINAL])
        self.assertEqual(agents[0].calls, 1)
        self.assertFalse(engine.bridge.active)


if __name__ == '__main__':
    unittest.main()

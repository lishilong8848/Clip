"""Native final-answer protocol regressions; scripted transports, no services."""
import asyncio
import copy
import unittest
from unittest.mock import AsyncMock, Mock, patch

from pydantic_ai import Tool

from bin.test_lighthouse_openclaw import FakeGatewayClient, _recovery_agent
from openclaw_service.assistant import lighthouse_openclaw as native
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_gateway import GatewayClient
from openclaw_service.assistant.lighthouse_runtime import agent_id
from openclaw_service.assistant.lighthouse_context import execution_key


PROCESS = '我直接拉上周5栋的答题记录，并行发起查询。'
FINAL = '上周5栋共14人参与答题，完成210题，正确率92%。'


def assistant(text, *, phase=None, seq=1):
    return {'event': 'agent', 'payload': {'runId': 'run-final', 'seq': seq,
        'stream': 'assistant', 'data': {'text': text, **({'phase': phase} if phase else {})}}}


def end(seq=2):
    return {'event': 'agent', 'payload': {'runId': 'run-final', 'seq': seq,
        'stream': 'lifecycle', 'data': {'phase': 'end'}}}


def chat_final(text=FINAL, *, seq=3, **values):
    return {'event': 'chat', 'payload': {'runId': 'run-final', 'state': 'final', 'seq': seq,
        'message': {'role': 'assistant', 'content': [{'type': 'text', 'text': text}]}, **values}}


def visible(text=FINAL, **values):
    return {'status': 'ok', 'terminalReply': {'disposition': 'visible', 'text': text}, **values}


class ScriptedGateway(FakeGatewayClient):
    def __init__(self, *, frames=(), outcomes=(), history=None):
        super().__init__('ws://127.0.0.1:19999', 'fixture-token', run_id='run-final',
                         events=copy.deepcopy(frames), history=history or {'messages': []})
        self.outcomes = list(copy.deepcopy(outcomes)) or [visible()]

    async def request(self, method, params=None, timeout=None):
        if method == 'agent.wait':
            self.calls.append((method, copy.deepcopy(params)))
            value = self.outcomes.pop(0) if len(self.outcomes) > 1 else self.outcomes[0]
            if isinstance(value, Exception):
                raise value
            return copy.deepcopy(value)
        return await super().request(method, params, timeout)

    async def next_event(self, timeout):
        if self.events:
            return await super().next_event(timeout)
        await asyncio.sleep(.002)
        raise asyncio.TimeoutError()


class NativeFinalAnswerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        FakeGatewayClient._reset()
        self.agent = _recovery_agent(turn={'operation_id': 'run-final'})
        # Production grace permits native terminal persistence to settle; tests use milliseconds.
        self.enterContext(patch.object(native.OpenClawToolAgent, 'FINAL_REPLY_GRACE', .04, create=True))
        self.enterContext(patch.object(native.OpenClawToolAgent, 'FINAL_REPLY_POLL', .005, create=True))
        self.enterContext(patch.object(native.OpenClawToolAgent, 'RUN_STATUS_POLL', .01, create=True))

    def tearDown(self):
        FakeGatewayClient._reset()

    async def collect(self, client):
        self.agent.client = client
        async with asyncio.timeout(.5):
            return [event async for event in self.agent._events(client, ['上周哪些人员参与答题？'], [])]

    def assert_single_submission(self, client):
        submissions = [params for method, params in client.calls if method == 'agent']
        self.assertEqual(len(submissions), 1)
        self.assertNotIn(':validation', submissions[0]['idempotencyKey'])

    async def test_generic_terminal_failure_uses_only_same_run_error_and_keeps_native_details_private(self):
        for identity, expected in (({}, 'model_context'), ({'runId': 'foreign'}, 'model_protocol'),
                ({'sessionKey': 'foreign'}, 'model_protocol'), ({'agentId': 'foreign'}, 'model_protocol')):
            with self.subTest(identity=identity):
                self.agent.run_id = 'own-run'
                client = self.agent.client = GatewayClient('ws://127.0.0.1:19999', 'fixture-token')
                client.select_run('own-run')
                client._handle_event({'event': 'agent', 'payload': {'runId': 'own-run',
                    'sessionKey': self.agent.session_key, 'agentId': agent_id(execution_key(self.agent.actor)),
                    'stream': 'lifecycle', 'data': {'phase': 'error', 'error': 'Context overflow: private provider body'}, **identity}})
                with self.assertRaises(AssistantError) as raised:
                    self.agent._terminal_answer({'runId': 'own-run', 'status': 'error', 'error': 'failed'})
                self.assertEqual(raised.exception.category, expected)
                self.assertNotIn('private provider body', str(raised.exception))
                self.assertEqual(self.agent.run_id, '')
                await client.close()
                self.assertFalse(client.terminal_errors('own-run'))

    async def test_specific_terminal_error_wins_over_event_hint_and_pending_error_cannot_finish(self):
        self.agent.run_id = 'own-run'
        client = self.agent.client = GatewayClient('ws://127.0.0.1:19999', 'fixture-token')
        client.select_run('own-run')
        client._handle_event({'event': 'chat', 'payload': {'runId': 'own-run', 'state': 'error',
            'errorMessage': 'Model provider authentication failed'}})
        self.assertIsNone(self.agent._terminal_answer({'status': 'error', 'error': 'failed', 'pendingError': True}))
        self.assertEqual(self.agent.run_id, 'own-run')
        with self.assertRaises(AssistantError) as raised:
            self.agent._terminal_answer({'status': 'error', 'error': {'message': 'Model provider unavailable'}})
        self.assertEqual(raised.exception.category, 'model_connection')
        await client.close()

    async def test_generic_native_lifecycle_summary_does_not_hide_specific_chat_error(self):
        self.agent.run_id = 'own-run'
        client = self.agent.client = GatewayClient('ws://127.0.0.1:19999', 'fixture-token')
        client.select_run('own-run')
        client._handle_event({'event': 'agent', 'payload': {'runId': 'own-run', 'stream': 'lifecycle',
            'data': {'phase': 'error', 'error': 'All model fallback candidates failed'}}})
        client._handle_event({'event': 'chat', 'payload': {'runId': 'own-run', 'state': 'error',
            'errorMessage': 'Context overflow: private provider body'}})
        with self.assertRaises(AssistantError) as raised:
            self.agent._terminal_answer({'status': 'error', 'error': 'Agent run failed'})
        self.assertEqual(raised.exception.category, 'model_context')
        await client.close()

    async def test_process_snapshot_is_not_final_when_terminal_reply_is_suppressed(self):
        client = ScriptedGateway(frames=[assistant(PROCESS), end()],
            outcomes=[{'status': 'ok', 'terminalReply': {'disposition': 'suppressed'}}])
        with self.assertRaises(AssistantError):
            await self.collect(client)
        self.assert_single_submission(client)

    async def test_lifecycle_end_without_reply_waits_for_chat_final(self):
        client = ScriptedGateway(frames=[assistant(PROCESS), end(), chat_final()], outcomes=[{'status': 'ok'}])
        events = await self.collect(client)
        self.assertEqual(events[-1].result.output, FINAL)
        self.assert_single_submission(client)

    async def test_chat_final_without_lifecycle_end_is_observed_and_completed(self):
        client = ScriptedGateway(frames=[assistant('上周5栋共'), chat_final()], outcomes=[visible()])
        events = await self.collect(client)
        self.assertEqual(events[-1].result.output, FINAL)
        self.assert_single_submission(client)

    async def test_pending_wait_is_observed_without_new_agent_or_model(self):
        client = ScriptedGateway(frames=[assistant(PROCESS), end(), chat_final()],
            outcomes=[{'status': 'timeout', 'pendingError': True}, visible()])
        events = await self.collect(client)
        self.assertEqual(events[-1].result.output, FINAL)
        self.assert_single_submission(client)
        self.assertFalse(getattr(self.agent, '_terminal_model_failure', False))

    async def test_chat_error_waits_for_confirmed_outcome_without_publishing_preview(self):
        self.agent._buffered_reply = True
        client = ScriptedGateway(frames=[assistant(PROCESS),
            {'event': 'chat', 'payload': {'runId': 'run-final', 'state': 'error'}}],
            outcomes=[{'status': 'timeout', 'pendingError': True}, visible()])
        events = await self.collect(client)
        self.assertEqual(events[-1].result.output, FINAL)
        self.assertNotIn(PROCESS, str(events))
        self.assert_single_submission(client)
        self.assertEqual(sum(method == 'agent.wait' for method, _ in client.calls), 2)
        self.assertFalse(getattr(self.agent, '_terminal_model_failure', False))

    async def test_yielded_tool_turn_cannot_be_final(self):
        self.agent.calls = 1
        client = ScriptedGateway(frames=[assistant(PROCESS), end(), chat_final()],
            outcomes=[visible(PROCESS, yielded=True), visible()])
        events = await self.collect(client)
        self.assertEqual(events[-1].result.output, FINAL)
        self.assert_single_submission(client)

    async def test_missing_terminal_reply_after_tool_never_reuses_process_or_history(self):
        self.agent.calls = 1
        client = ScriptedGateway(frames=[assistant(PROCESS), end()], outcomes=[{'status': 'ok'}],
            history={'messages': [{'runId': 'another-run', 'role': 'assistant', 'content': FINAL}],
                     'inFlightRun': {'runId': 'another-run', 'text': FINAL}})
        with self.assertRaises(AssistantError):
            await self.collect(client)
        self.assert_single_submission(client)
        self.assertFalse(getattr(self.agent, '_terminal_model_failure', False))

    async def test_chat_and_agent_sequence_numbers_are_not_one_counter(self):
        client = ScriptedGateway(frames=[assistant(PROCESS), end(seq=2), chat_final(seq=2)],
            outcomes=[{'status': 'ok'}])
        events = await self.collect(client)
        self.assertEqual(events[-1].result.output, FINAL)
        self.assert_single_submission(client)

    async def test_foreign_chat_session_cannot_supply_missing_reply(self):
        client = ScriptedGateway(frames=[assistant(PROCESS), end(),
            chat_final(sessionKey='another-person-session')], outcomes=[{'status': 'ok'}])
        with self.assertRaises(AssistantError):
            await self.collect(client)
        self.assert_single_submission(client)

    async def test_tool_call_message_is_not_a_chat_final_answer(self):
        frame = chat_final()
        frame['payload']['message']['content'].append({'type': 'toolCall', 'id': 'unresolved', 'name': 'query'})
        client = ScriptedGateway(frames=[assistant(PROCESS), end(), frame], outcomes=[{'status': 'ok'}])
        with self.assertRaises(AssistantError):
            await self.collect(client)
        self.assert_single_submission(client)

    async def test_terminal_snapshot_replaces_partial_answer_without_concatenation(self):
        client = ScriptedGateway(frames=[assistant('上周5栋共'), end()], outcomes=[visible()])
        events = await self.collect(client)
        self.assertEqual(events[-1].result.output, FINAL)
        self.assert_single_submission(client)

    async def test_final_text_is_not_classified_by_its_wording(self):
        text = PROCESS + '以上是已核实的最终答复。'
        client = ScriptedGateway(frames=[end()], outcomes=[visible(text)])
        self.assertEqual((await self.collect(client))[-1].result.output, text)

    async def test_dropped_terminal_events_use_original_run_observer(self):
        client = ScriptedGateway(frames=[assistant('上周5栋共')], outcomes=[visible()])
        events = await self.collect(client)
        self.assertEqual(events[-1].result.output, FINAL)
        self.assert_single_submission(client)
        observers = [params for method, params in client.calls if method == 'agent.wait']
        self.assertEqual(observers[0], {'runId': client.run_id, 'timeoutMs': 0})

    async def test_explicit_nonfinal_terminal_metadata_waits_for_complete_answer(self):
        for metadata in ({'phase': 'commentary'}, {'phase': 'analysis'}, {'stopReason': 'toolUse'},
                         {'stopReason': 'length'}, {'pendingToolCalls': ['pending']}):
            with self.subTest(metadata=metadata):
                candidate = visible(PROCESS)
                candidate['terminalReply'].update(metadata)
                client = ScriptedGateway(frames=[assistant(PROCESS), end(), chat_final()],
                                         outcomes=[candidate, visible()])
                events = await self.collect(client)
                self.assertEqual(events[-1].result.output, FINAL)
                self.assert_single_submission(client)

    async def test_chat_final_does_not_override_unknown_or_failed_run(self):
        for outcome in ({'status': 'timeout', 'pendingError': True}, {'status': 'in_progress'},
                        {'status': 'error', 'error': 'Model provider unavailable'}):
            with self.subTest(outcome=outcome):
                client = ScriptedGateway(frames=[chat_final()], outcomes=[outcome])
                events = []
                self.agent.client = client
                with self.assertRaises(AssistantError):
                    async with asyncio.timeout(.5):
                        async for event in self.agent._events(client, ['question'], []):
                            events.append(event)
                self.assertFalse(any(event.event_kind == 'agent_run_result' for event in events))
                self.assert_single_submission(client)

    async def test_foreign_run_or_agent_cannot_supply_answer(self):
        for route in ({'runId': 'another-run'}, {'agentId': 'another-agent'}):
            with self.subTest(route=route):
                client = ScriptedGateway(frames=[assistant(PROCESS), end(), chat_final(**route)],
                                         outcomes=[{'status': 'ok'}])
                with self.assertRaises(AssistantError):
                    await self.collect(client)
                self.assert_single_submission(client)

    async def test_chat_final_waiting_or_truncated_metadata_is_not_discarded(self):
        for metadata in ({'yielded': True}, {'stopReason': 'toolUse'}, {'stopReason': 'length'},
                         {'pendingToolCalls': ['pending']}, {'aborted': True}):
            with self.subTest(metadata=metadata):
                client = ScriptedGateway(frames=[assistant(PROCESS), end(), chat_final(**metadata)],
                                         outcomes=[{'status': 'ok'}])
                with self.assertRaises(AssistantError):
                    await self.collect(client)
                self.assert_single_submission(client)

    async def test_foreign_wait_result_cannot_commit_even_with_owned_chat_final(self):
        client = ScriptedGateway(frames=[chat_final()], outcomes=[visible(runId='another-run')])
        with self.assertRaises(AssistantError):
            await self.collect(client)
        self.assert_single_submission(client)

    async def test_recovery_provisional_history_waits_for_owned_final_without_resubmit(self):
        first = ScriptedGateway(frames=[assistant(PROCESS), OSError('disconnect')])
        resumed = []
        async def keep_gateway(*_):
            pass
        self.agent.engine.keep_gateway = keep_gateway
        self.agent.item_url = first.url
        def reconnect(*_):
            client = ScriptedGateway(frames=[chat_final()], outcomes=[{'status': 'in_progress'}, visible()],
                history={'inFlightRun': {'runId': self.agent.run_id, 'text': PROCESS}})
            client.events[0]['payload']['runId'] = self.agent.run_id
            resumed.append(client)
            return client
        with patch.object(native, 'GatewayClient', side_effect=reconnect):
            events = await self.collect(first)
        self.assertEqual(events[-1].result.output, FINAL)
        self.assert_single_submission(first)
        self.assertEqual(len(resumed), 1)
        self.assertFalse(any(method == 'agent' for method, _ in resumed[0].calls))
        self.assertTrue(first.closed)

    async def test_tool_receipts_remain_idempotent_and_incomplete_run_cannot_fail_over(self):
        for outcome, completed in (('success', True), ('success', False), ('failure', False), ('unknown', False)):
            with self.subTest(outcome=outcome, completed=completed):
                agent = _recovery_agent(turn={'operation_id': 'run-final'})
                effects = []
                async def submit():
                    effects.append(outcome)
                    if outcome == 'failure':
                        raise AssistantError('business operation rejected', 409)
                    if outcome == 'unknown':
                        raise asyncio.TimeoutError()
                    return {'ok': True, 'job_id': 'original-task'}
                agent.tools['lighthouse_submit'] = Tool(submit)
                client = ScriptedGateway(frames=[assistant(PROCESS), end(), *([chat_final()] if completed else [])],
                    outcomes=[{'status': 'ok'}, visible()] if completed else [{'status': 'ok'}])
                original = client.next_event
                async def next_event(timeout):
                    if not effects:
                        payload = {'session_key': agent.session_key, 'run_id': agent.run_id,
                                   'tool': 'lighthouse_submit', 'call_id': 'one-operation', 'params': {}}
                        first = await agent.engine.bridge.call(agent.token, payload)
                        self.assertEqual(await agent.engine.bridge.call(agent.token, payload), first)
                    return await original(timeout)
                client.next_event = next_event
                agent._acquire_runtime = AsyncMock(return_value=agent.item)
                agent.engine.gateway_client = AsyncMock(return_value=client)
                agent.engine.keep_gateway = AsyncMock()
                agent._next_profile = Mock(side_effect=AssertionError('Unknown result cannot fail over'))
                events = []
                async def consume():
                    async with agent.run_stream_events(['question'], message_history=[]) as stream:
                        async for event in stream:
                            events.append(event)
                if completed:
                    await consume()
                else:
                    with self.assertRaises(AssistantError):
                        await consume()
                self.assertEqual(effects, [outcome])
                self.assertEqual(agent.calls, 1)
                self.assertIn('one-operation', agent.results)
                self.assert_single_submission(client)
                agent._next_profile.assert_not_called()
                if completed:
                    self.assertEqual(events[-1].result.output, FINAL)
                    self.assertIn('function_tool_result', [event.event_kind for event in events[:-1]])
                else:
                    self.assertFalse(any(event.event_kind == 'agent_run_result' for event in events))
                self.assertFalse(agent.engine.bridge.active)


if __name__ == '__main__':
    unittest.main()

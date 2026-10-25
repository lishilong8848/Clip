"""Model failover stays inside one authorized turn; all transports are fakes."""
import asyncio
import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pydantic_ai import BinaryContent, Tool
from pydantic_ai.exceptions import ModelRetry
from openclaw_service.assistant import lighthouse_openclaw as native
from openclaw_service.assistant.lighthouse_ai import AssistantError, CustomModel, NAMESPACE
from openclaw_service.assistant.lighthouse_gateway import GatewayError
from openclaw_service.assistant.lighthouse_stream import failure_detail
from openclaw_service.assistant.lighthouse_runtime import account_key, agent_id, build_configuration, model_provider_id
from bin.test_lighthouse_openclaw import FakeGatewayClient, Store, _fake_engine


def profile(identity, **options):
    return {'id': identity, 'name': identity, 'model': identity, 'endpoint': 'https://fixture.invalid/v1/chat/completions',
            'key_cipher': 'fixture-cipher-' + identity, **options}


class ModelFailoverTests(unittest.IsolatedAsyncioTestCase):
    def tearDown(self):
        FakeGatewayClient._reset()

    def setUp(self):
        FakeGatewayClient._reset()
        self.store = Store(Path(tempfile.gettempdir()) / 'unused-failover-fixture.sqlite3')
        self.profiles = [profile('current'), profile('backup')]
        self.actor = {'id': 'person-a', 'scopes': ['A']}
        self.configure(self.actor['id'], self.profiles)
        self.store.put_document(NAMESPACE, 'model', {'enabled': True, 'models': [], 'image_input_default_version': 1})
        self.model = CustomModel(self.store, client=object(), protect=lambda value: value, unprotect=lambda value: value)
        self.engine = _fake_engine()
        self.engine.assistant.model_for = lambda actor: self.model.for_actor(actor['id'])
        self.engine.keep_gateway = AsyncMock()
        self.engine.gateway_client = self.client_for
        self.emitted, self.clients, self.items, self.selected, self.scripts = [], [], [], [], {}
        self.agent = native.OpenClawToolAgent(self.engine, self.actor,
            {'operation_id': 'original-operation', 'run_id': 'original-run', 'file_ids': ['original-file']},
            self.emit, self.profiles[0], instructions='Original authorization and business policy')
        self.agent._acquire_runtime = AsyncMock(side_effect=self.acquire)

    def configure(self, owner, profiles, *, enabled=True):
        self.store.put_document(NAMESPACE, 'model:' + owner, {
            'enabled': enabled, 'models': profiles, 'active_model_id': profiles[0]['id'] if profiles else '',
            'shared_models_linked': True, 'image_input_default_version': 1})

    async def emit(self, kind, value):
        self.emitted.append((kind, copy.deepcopy(value)))

    async def acquire(self):
        self.assertFalse(self.engine.bridge.active, 'Previous run callback still registered')
        item = {'port': 19999, 'token': 'loopback-fixture', 'busy': True}
        self.items.append(item)
        self.selected.append(self.agent.profile['id'])
        return item

    async def client_for(self, actor, item):
        self.assertEqual(actor, self.actor)
        script = self.scripts.get(self.agent.profile['id'], {})
        client = FakeGatewayClient('ws://127.0.0.1:19999', 'loopback-fixture',
            events=script.get('events'), fail=script.get('fail'), wait=script.get('wait'))
        if script.get('hook'):
            original = client.next_event
            async def next_event(timeout):
                await script['hook'](client)
                return await original(timeout)
            client.next_event = next_event
        if script.get('accept'):
            original_request = client.request
            async def request(method, params=None, timeout=None):
                result = await original_request(method, params, timeout)
                return {**result, **script['accept']} if method == 'agent' else result
            client.request = request
        self.clients.append(client)
        return client

    def fail_model(self, identity='current', message='Model provider unavailable', **options):
        self.scripts[identity] = {'events': [{'event': 'agent', 'payload': {'runId': 'run-1',
            'stream': 'lifecycle', 'data': {'phase': 'error', 'error': message}}}],
            'wait': {'status': 'error', 'error': message}, **options}

    async def run_agent(self, prompt=None, history=None):
        async with self.agent.run_stream_events(prompt or ['original question'], message_history=history or []) as events:
            return [event async for event in events]

    async def test_current_first_and_no_fallback_on_success(self):
        events = await self.run_agent()
        self.assertEqual(events[-1].result.output, 'fixture reply')
        self.assertEqual(self.selected, ['current'])
        self.assertFalse(self.engine.bridge.active)
        self.assertFalse(self.items[0]['busy'])

    async def test_output_truncation_retries_in_fresh_session_without_replaying_tools(self):
        self.fail_model(message='Model output token limit reached')
        original = self.agent.session_key
        events = await self.run_agent()
        self.assertEqual(events[-1].result.output, 'fixture reply')
        self.assertEqual(self.selected, ['current', 'backup'])
        requests = [p for c in self.clients for method, p in c.calls if method == 'agent']
        self.assertEqual(requests[0]['sessionKey'], original)
        self.assertTrue(requests[1]['sessionKey'].startswith(original + ':output:'))
        self.assertTrue(self.agent.profile['_output_recovery'])
        self.assertEqual(self.agent.calls, 0)
        self.assertEqual(self.agent.session_key, original)

    def test_confirmed_length_error_is_not_an_unclassified_protocol_error(self):
        self.agent.run_id = 'original-native-run'
        with self.assertRaises(AssistantError) as raised:
            self.agent._terminal_answer({'status': 'error', 'error': 'failed', 'stopReason': 'length'})
        self.assertEqual(raised.exception.category, 'model_output')

    async def test_large_read_receipts_are_bounded_without_requery_or_losing_snapshots(self):
        from openclaw_service.assistant.lighthouse_context import bound_model_payload
        rows = [{'title': '设备检查事项' * 8, 'state': '待核对', 'record_id': f'r{i}'} for i in range(400)]
        receipt = {'ok': True, 'query_ref': 'query_saved', 'source_number': 1,
                   'data': {'records': rows, 'complete': False, 'has_more': True, 'next_offset': 400}}
        reads, replies = [], []
        async def query():
            reads.append('read')
            return copy.deepcopy(receipt)
        self.agent.tools['lighthouse_query'] = Tool(query)
        async def hook(client):
            if reads:
                return
            payload = {'session_key': self.agent.session_key, 'run_id': self.agent.run_id,
                       'tool': 'lighthouse_query', 'call_id': 'large-read', 'params': {}}
            replies.append(await self.engine.bridge.call(self.agent.token, payload))
            replies.append(await self.engine.bridge.call(self.agent.token, payload))
        self.fail_model(message='Context overflow', hook=hook)
        self.agent._auto_tools = True
        await self.run_agent()
        self.assertEqual(reads, ['read'])
        self.assertEqual(replies[0], replies[1])
        self.assertLess(len(json.dumps(replies[0], ensure_ascii=False).encode()), 8000)
        self.assertEqual(replies[0]['query_ref'], 'query_saved')
        self.assertTrue(replies[0]['receipt_excerpt_only'])
        self.assertEqual(replies[0]['source_query_state'], 'partial')
        self.assertEqual(self.agent.results['large-read'][1], receipt)
        # Twenty-four completed reads must not be copied wholesale into the recovery prompt.
        self.agent.results = {f'call-{i}': ('sig', {**copy.deepcopy(receipt), 'query_ref': f'query_saved_{i}'}) for i in range(24)}
        originals = copy.deepcopy(self.agent.results)
        message = self.agent._complete_from_evidence('按原日期和楼栋核对资料', '补查后回答')
        self.assertLess(len(message.encode()), 30000)
        bound_model_payload({'model': 'WanWu/Deepseek-Auto', 'messages': [{'role': 'user', 'content': message}]},
                            {'model': 'WanWu/Deepseek-Auto'})
        self.assertEqual(self.agent.results, originals)
        for i in range(24):
            self.assertIn(f'"query_ref": "query_saved_{i}"', message)

    def test_receipt_compaction_keeps_small_and_unaddressable_results_unchanged(self):
        small = {'ok': False, 'query_ref': 'query_short', 'data': {'complete': False}, 'error': '查询未完成'}
        large = {'ok': True, 'job_id': 'accepted-job', 'data': [{'title': '已接收事项' * 1000}] * 10}
        for value in (small, large, {**large, 'query_ref': ''}):
            self.assertEqual(native.model_read_receipt(value), value)

    async def test_context_failure_recovers_without_reusing_overflowed_native_history(self):
        self.fail_model(message='Context length exceeded')
        original_session = self.agent.session_key
        original_client = self.client_for
        async def client_for(actor, item):
            client = await original_client(actor, item)
            if self.agent.session_key == original_session:
                client.history = {'messages': [{'role': 'assistant', 'content': 'old native tool history'}]}
                client.wait = {'status': 'error', 'error': 'Context length exceeded'}
            return client
        self.engine.gateway_client = client_for
        history = [SimpleNamespace(parts=[SimpleNamespace(content='已授权范围A，2026-10-25白班', part_kind='user-prompt')])]
        saved_history = copy.deepcopy(history)
        events = await self.run_agent(['继续核对当前问题'], history)
        self.assertEqual(events[-1].result.output, 'fixture reply')
        requests = [params for client in self.clients for method, params in client.calls if method == 'agent']
        self.assertEqual(len(requests), 2)
        self.assertNotEqual(requests[0]['sessionKey'], requests[1]['sessionKey'])
        self.assertIn('继续核对当前问题', requests[1]['message'])
        self.assertIn('已授权范围A，2026-10-25白班', requests[1]['message'])
        self.assertEqual(history, saved_history)
        self.assertEqual(self.agent.session_key, original_session)
        self.assertFalse(any(method in {'sessions.delete', 'chat.clear', 'sessions.reset'}
                             for client in self.clients for method, _ in client.calls))

    async def test_native_history_budget_reserves_current_request_policy_and_tool_schemas(self):
        self.agent.profile['context_window'] = 32000
        self.agent.instructions = 'POLICY_MUST_REMAIN ' * 500
        history = [SimpleNamespace(parts=[SimpleNamespace(content='OLD_DETAIL ' * 2500, part_kind=kind)])
                   for kind in ('user-prompt', 'text')]
        original = copy.deepcopy(history)
        question = 'CURRENT_SCOPE_D 2026-10-25 只核对此条件'
        await self.run_agent([question], history)
        request = next(params for client in self.clients for method, params in client.calls if method == 'agent')
        self.assertIn(question, request['message'])
        self.assertIn('已压缩', request['message'])
        self.assertNotIn('OLD_DETAIL', request['message'])
        self.assertEqual(request['extraSystemPrompt'], self.agent.instructions)
        self.assertEqual(request['promptMode'], 'minimal')
        self.assertEqual(history, original)
        self.assertLess(len(request['message'].encode()), 4000)

    async def test_rehydrated_history_fits_without_truncating_current_request_or_saved_history(self):
        from openclaw_service.assistant.lighthouse_context import bound_model_payload
        history = [SimpleNamespace(parts=[SimpleNamespace(content='历史资料' * 1000, part_kind=kind)])
                   for _ in range(10) for kind in ('user-prompt', 'text')]
        saved_history = copy.deepcopy(history)
        question = '只核对A楼2026-10-25白班，保留原筛选并给出依据。'
        await self.run_agent([question], history)
        request = next(params for method, params in self.clients[0].calls if method == 'agent')
        bounded = bound_model_payload({'messages': [{'role': 'user', 'content': request['message']}]},
                                      {'context_window': 149952})
        self.assertEqual(bounded['messages'][0]['content'], request['message'])
        self.assertTrue(request['message'].endswith(question))
        self.assertIn('部分历史对话已省略', request['message'])
        self.assertEqual(history, saved_history)

    async def test_topic_read_validation_exhaustion_uses_backup_and_original_receipts(self):
        self.actor.update(channel='feishu:oc_fixture',
                          _feishu_topic={'id': 'oc_fixture:om_topic', 'context': 'Only public topic context'})
        self.agent = native.OpenClawToolAgent(self.engine, self.actor,
            {'operation_id': 'original-operation', 'run_id': 'original-run'}, self.emit, self.profiles[0],
            instructions='Original authorization and business policy')
        self.agent._acquire_runtime = AsyncMock(side_effect=self.acquire)
        self.agent._auto_tools = True
        self.agent._buffered_reply = True
        reads = []
        async def query(page: int):
            reads.append(page)
            return {'ok': True, 'query_ref': 'snapshot-' + str(page),
                    'data': {'page': page, 'scope': 'A', 'has_more': page == 1}}
        self.agent.tools['lighthouse_query'] = Tool(query)
        async def validate(answer):
            if self.agent.profile['id'] == 'current':
                raise ModelRetry('Read the missing second page before claiming completeness')
            return answer
        self.agent.validators.append(validate)
        async def hook(client):
            page = 1 if self.agent.profile['id'] == 'current' else 2
            if page not in reads:
                await self.engine.bridge.call(self.agent.token, {'session_key': self.agent.session_key,
                    'run_id': self.agent.run_id, 'tool': 'lighthouse_query', 'call_id': 'page-' + str(page),
                    'params': {'page': page}})
        for identity, answer in (('current', 'Unverified complete list'), ('backup', 'Verified complete answer')):
            self.scripts[identity] = {'hook': hook, 'events': [], 'wait': {'status': 'ok',
                'terminalReply': {'disposition': 'visible', 'text': answer}}}
        events = await self.run_agent(['Original topic question, A building'])
        self.assertEqual(events[-1].result.output, 'Verified complete answer')
        self.assertEqual(self.selected, ['current', 'backup'])
        self.assertEqual(reads, [1, 2])
        backup = next(params for method, params in self.clients[-1].calls if method == 'agent')
        self.assertIn('Original topic question', backup['message'])
        self.assertIn('snapshot-1', backup['message'])
        self.assertIn(':evidence:', backup['sessionKey'])
        self.assertTrue(backup['idempotencyKey'].endswith(':fallback:1'))
        self.assertEqual(self.agent.health.snapshot(self.agent._health_key(self.profiles[0])), {})

    async def test_confirmed_mutation_validation_failure_does_not_start_another_model(self):
        effects = []
        async def submit():
            effects.append('submitted')
            return {'ok': True, 'job_id': 'original-job'}
        self.agent.tools['lighthouse_submit'] = Tool(submit)
        async def hook(client):
            if not effects:
                await self.engine.bridge.call(self.agent.token, {'session_key': self.agent.session_key,
                    'run_id': self.agent.run_id, 'tool': 'lighthouse_submit', 'call_id': 'original', 'params': {}})
        async def reject(answer):
            raise ModelRetry('Business outcome does not match the candidate')
        self.agent.validators.append(reject)
        self.scripts['current'] = {'hook': hook}
        with self.assertRaises(AssistantError):
            await self.run_agent()
        self.assertEqual(effects, ['submitted'])
        self.assertEqual(self.selected, ['current'])
        self.assertEqual(self.agent.health.snapshot(self.agent._health_key(self.profiles[0])), {})

    async def test_repeated_invalid_answers_have_a_bounded_model_chain(self):
        self.configure(self.actor['id'], [self.profiles[0], *[profile('backup' + str(i)) for i in range(5)]])
        async def reject(answer):
            raise ModelRetry('Missing evidence')
        self.agent.validators.append(reject)
        with self.assertRaises(AssistantError) as raised:
            await self.run_agent()
        self.assertEqual(raised.exception.category, 'model_validation')
        self.assertEqual(self.selected, ['current', 'backup0', 'backup1', 'backup2'])
        self.assertFalse(self.engine.bridge.active)

    async def test_terminal_failures_switch_once_and_keep_turn_session_history_and_images(self):
        for message in ('Model provider unavailable', 'Model provider timed out', 'Model provider authentication failed',
                        'Model provider rate limit exceeded', 'Model provider model not found', 'private provider failure'):
            with self.subTest(message=message):
                self.setUp()
                self.profiles = [profile('current', image_input=True), profile('backup', image_input=True)]
                self.configure(self.actor['id'], self.profiles)
                self.agent.profile = self.profiles[0]
                self.fail_model(message=message)
                prompt = ['original question', BinaryContent(data=b'fixture-image', media_type='image/png')]
                history = [SimpleNamespace(parts=[SimpleNamespace(content='authorized prior context', part_kind='user-prompt')])]
                original_turn = copy.deepcopy(self.agent.turn)
                with self.assertLogs(native.__name__, level='WARNING') as logs:
                    events = await self.run_agent(prompt, history)
                self.assertEqual(events[-1].result.output, 'fixture reply')
                self.assertEqual(self.selected, ['current', 'backup'])
                requests = [params for client in self.clients for method, params in client.calls if method == 'agent']
                self.assertEqual(len(requests), 2)
                self.assertTrue(all(request['promptMode'] == 'minimal' for request in requests))
                for field in ('message', 'sessionKey', 'attachments', 'extraSystemPrompt'):
                    self.assertEqual(requests[0][field], requests[1][field])
                self.assertIn('authorized prior context', requests[1]['message'])
                self.assertEqual(requests[1]['idempotencyKey'], requests[0]['idempotencyKey'] + ':fallback:1')
                self.assertEqual(self.agent.turn, original_turn)
                self.assertEqual(self.model.for_actor(self.actor['id']).settings()['active_model_id'], 'current')
                self.assertNotIn('private provider failure', ''.join(logs.output))
                self.assertTrue(all(not item['busy'] for item in self.items))
                self.assertFalse(self.engine.bridge.active)

    async def test_only_current_account_configured_compatible_models_are_candidates(self):
        profiles = [*self.profiles[:1], profile('missing', key_cipher=''), profile('no-tools', tool_calls=False),
                    profile('vision-enabled', image_input=True), profile('backup')]
        self.configure(self.actor['id'], profiles)
        self.configure('person-b', [profile('other-person-private')])
        self.fail_model()
        await self.run_agent()
        self.assertEqual(self.selected, ['current', 'backup'])
        self.assertNotIn('other-person-private', self.selected)

    async def test_disabled_assistant_does_not_select_a_backup(self):
        self.configure(self.actor['id'], self.profiles, enabled=False)
        self.fail_model()
        with self.assertRaises(AssistantError):
            await self.run_agent()
        self.assertEqual(self.selected, ['current'])

    async def test_duplicate_model_aliases_not_retried_and_chain_is_bounded(self):
        aliases = [self.profiles[0], {**self.profiles[0], 'id': 'duplicate'},
                   *[profile('backup' + str(index)) for index in range(5)]]
        self.configure(self.actor['id'], aliases)
        for item in aliases:
            self.fail_model(item['id'])
        with self.assertRaises(AssistantError):
            await self.run_agent()
        self.assertEqual(self.selected, ['current', 'backup0', 'backup1', 'backup2'])

    async def test_all_models_failed_retains_sanitized_diagnostic_not_success(self):
        for item in self.profiles:
            self.fail_model(item['id'], 'provider private secret and personal-data')
        with self.assertRaises(AssistantError) as raised:
            await self.run_agent()
        category, message = failure_detail(raised.exception)
        self.assertEqual(category, 'model_protocol')
        self.assertIn('模型运行未完成', message)
        self.assertNotIn('private', message)
        self.assertEqual(self.selected, ['current', 'backup'])

    async def test_unloaded_shared_tools_are_not_reported_as_four_bad_models(self):
        self.fail_model(message='No callable tools remain after resolving explicit tool allowlist (private fixture)')
        with self.assertRaises(AssistantError) as raised:
            await self.run_agent()
        self.assertEqual(raised.exception.category, 'runtime_tools')
        self.assertIn('查询工具', str(raised.exception))
        self.assertNotIn('private fixture', str(raised.exception))
        self.assertEqual(self.selected, ['current'])

    async def test_generic_failed_status_uses_native_cause_for_fallback_and_diagnostics(self):
        for summary, error, category, backups in (
                ('failed', 'Model provider unavailable: private-body', 'model_connection', ['current', 'backup']),
                ('failed', 'No callable tools remain: private-body', 'runtime_tools', ['current']),
                ("⚠️ Agent couldn't generate a response. Please try again.",
                 'Context overflow: private-body', 'model_context', ['current', 'backup']),
                ("⚠️ Agent couldn't generate a response. Please try again.",
                 'No callable tools remain: private-body', 'runtime_tools', ['current'])):
            with self.subTest(error=error):
                self.setUp()
                self.fail_model(message=summary)
                original_client = self.client_for
                async def client_for(actor, item):
                    client = await original_client(actor, item)
                    if self.agent.profile['id'] == 'current':
                        client.terminal_errors = lambda run: ({'runId': run,
                            'sessionKey': self.agent.session_key, 'error': error},)
                    return client
                self.engine.gateway_client = client_for
                with patch('openclaw_service.diagnostics.capture') as capture:
                    if category == 'runtime_tools':
                        with self.assertRaises(AssistantError) as raised:
                            await self.run_agent()
                        self.assertEqual(raised.exception.category, category)
                    else:
                        events = await self.run_agent()
                        self.assertEqual(events[-1].result.output, 'fixture reply')
                self.assertEqual(capture.call_args.kwargs['category'], category)
                self.assertEqual(capture.call_args.kwargs['native'], error)
                self.assertEqual(self.selected, backups)
                self.assertNotIn('private-body', str(self.emitted))

    async def test_terminal_acceptance_preserves_result_meta_error_instead_of_failed_summary(self):
        self.scripts['current'] = {'accept': {'status': 'error', 'summary': 'failed',
            'result': {'meta': {'error': 'No callable tools remain: private-body'}}}}
        with self.assertRaises(AssistantError) as raised:
            await self.run_agent()
        self.assertEqual(raised.exception.category, 'runtime_tools')
        self.assertEqual(self.selected, ['current'])
        self.assertNotIn('private-body', str(raised.exception))

    async def test_pending_acceptance_observes_original_run_without_starting_backup(self):
        self.scripts['current'] = {'accept': {'status': 'error', 'summary': 'failed',
            'result': {'meta': {'pendingError': True}}},
            'wait': {'status': 'ok', 'terminalReply': {'disposition': 'visible', 'text': 'original completed answer'}}}
        events = await self.run_agent()
        self.assertEqual(events[-1].result.output, 'original completed answer')
        self.assertEqual(self.selected, ['current'])
        self.assertEqual(sum(method == 'agent' for client in self.clients for method, _ in client.calls), 1)

    async def test_unsettled_acceptance_cannot_authorize_replay(self):
        for flags in ({'pendingToolCalls': ['original-write']}, {'aborted': True}, {'stopReason': 'toolUse'}):
            with self.subTest(flags=flags):
                self.setUp()
                self.scripts['current'] = {'accept': {'status': 'error', 'summary': 'failed', 'result': {'meta': flags}},
                    'wait': {'status': 'error', 'error': 'failed', **flags}}
                with patch.object(native.OpenClawToolAgent, 'FINAL_REPLY_GRACE', .03), \
                        patch.object(native.OpenClawToolAgent, 'FINAL_REPLY_POLL', .005):
                    with self.assertRaises(AssistantError):
                        await self.run_agent()
                self.assertEqual(self.selected, ['current'])
                self.assertEqual(sum(method == 'agent' for client in self.clients for method, _ in client.calls), 1)

    async def test_tool_success_failure_and_unknown_result_never_replayed(self):
        for result in ('success', 'failure', 'unknown'):
            with self.subTest(result=result):
                self.setUp()
                calls = []
                async def tool():
                    calls.append(result)
                    if result == 'failure':
                        raise AssistantError('Original business failure', 503)
                    if result == 'unknown':
                        raise asyncio.TimeoutError()
                    return {'ok': True, 'job_id': 'original-business-task'}
                self.agent.tools['lighthouse_submit'] = Tool(tool)
                async def hook(client):
                    await self.engine.bridge.call(self.agent.token, {'session_key': self.agent.session_key,
                        'run_id': self.agent.run_id, 'tool': 'lighthouse_submit', 'call_id': 'first-call', 'params': {}})
                self.fail_model(hook=hook)
                events = await self.run_agent()
                self.assertEqual(calls, [result])
                self.assertEqual(self.selected, ['current', 'backup'])
                self.assertIn('first-call', self.agent.results)
                if result != 'success':
                    self.assertIn('未知', events[-1].result.output)

    async def test_partial_reply_does_not_mix_in_another_model_answer(self):
        self.agent._live_text_only = True
        self.fail_model()
        self.scripts['current']['events'].insert(0, {'event': 'agent', 'payload': {'runId': 'run-1',
            'stream': 'assistant', 'data': {'delta': 'partial original answer'}}})
        events = []
        with self.assertRaises(AssistantError):
            async with self.agent.run_stream_events(['hello'], message_history=[]) as stream:
                async for event in stream:
                    events.append(event)
        self.assertEqual(events[0].delta.content_delta, 'partial original answer')
        self.assertEqual(self.selected, ['current'])

    async def test_confirmed_readonly_run_without_terminal_body_uses_backup_not_prefix(self):
        self.agent._auto_tools = True
        self.agent._buffered_reply = True
        reads = []
        async def query():
            reads.append('original')
            return {'ok': True, 'query_ref': 'original-snapshot', 'data': {'total': 2}}
        self.agent.tools['lighthouse_query'] = Tool(query)
        async def hook(client):
            if not reads:
                await self.engine.bridge.call(self.agent.token, {'session_key': self.agent.session_key,
                    'run_id': self.agent.run_id, 'tool': 'lighthouse_query', 'call_id': 'original-read', 'params': {}})
        self.scripts['current'] = {'hook': hook, 'wait': {'status': 'ok'}, 'events': [
            {'event': 'agent', 'payload': {'runId': 'run-1', 'stream': 'assistant', 'data': {'text': '今天'}}},
            {'event': 'agent', 'payload': {'runId': 'run-1', 'stream': 'lifecycle', 'data': {'phase': 'end'}}}]}
        with patch.object(native.OpenClawToolAgent, 'FINAL_REPLY_GRACE', .02), \
                patch.object(native.OpenClawToolAgent, 'FINAL_REPLY_POLL', .005), \
                patch.object(native.OpenClawToolAgent, 'RUN_STATUS_POLL', .005):
            events = await self.run_agent(['Original business question'])
        self.assertEqual(self.selected, ['current', 'backup'])
        self.assertEqual(reads, ['original'])
        self.assertEqual(events[-1].result.output, 'fixture reply')
        self.assertNotIn('今天', str(events))
        request = next(params for method, params in self.clients[-1].calls if method == 'agent')
        self.assertIn('original-snapshot', request['message'])
        self.assertIn('Original business question', request['message'])

    async def test_chat_error_observes_terminal_failure_and_reuses_read_receipts(self):
        self.agent._auto_tools = True
        self.agent._buffered_reply = True
        reads = []
        async def query():
            reads.append('original')
            return {'ok': True, 'query_ref': 'original-snapshot', 'data': {'total': 2}}
        self.agent.tools['lighthouse_query'] = Tool(query)
        async def hook(client):
            if not reads:
                await self.engine.bridge.call(self.agent.token, {'session_key': self.agent.session_key,
                    'run_id': self.agent.run_id, 'tool': 'lighthouse_query', 'call_id': 'original-read', 'params': {}})
        self.fail_model(hook=hook, wait={'status': 'error', 'error': 'Model provider unavailable'})
        self.scripts['current']['events'].insert(0, {'event': 'chat', 'payload': {
            'runId': 'run-1', 'state': 'error', 'errorMessage': 'private provider detail'}})
        with self.assertLogs(native.__name__, level='WARNING') as logs:
            events = await self.run_agent(['Original business question'])
        self.assertEqual(self.selected, ['current', 'backup'])
        self.assertEqual(reads, ['original'])
        self.assertEqual(events[-1].result.output, 'fixture reply')
        self.assertTrue(any(method == 'agent.wait' for method, _ in self.clients[0].calls))
        request = next(params for method, params in self.clients[-1].calls if method == 'agent')
        self.assertIn('original-snapshot', request['message'])
        self.assertIn('Original business question', request['message'])
        self.assertNotIn('private provider detail', ''.join(logs.output))

    async def test_chat_error_unknown_run_or_abort_cannot_start_backup(self):
        for state, outcome, missing_receipt in (
                ('error', {'status': 'in_progress'}, False),
                ('error', {'status': 'error', 'pendingError': True}, False),
                ('error', {'status': 'error', 'pendingToolCalls': ['pending']}, False),
                ('error', {'status': 'error', 'aborted': True}, False),
                ('error', {'status': 'error', 'stopReason': 'aborted'}, False),
                ('error', {'status': 'error', 'stopReason': 'toolUse'}, False),
                ('error', {'status': 'error', 'terminalReply': {'aborted': True}}, False),
                ('error', {'status': 'error', 'runId': 'another-run'}, False),
                ('error', {'status': 'error'}, True),
                ('aborted', {'status': 'error'}, False)):
            with self.subTest(state=state, outcome=outcome, missing_receipt=missing_receipt):
                self.setUp()
                if missing_receipt:
                    self.agent.calls = 1
                self.scripts['current'] = {'wait': outcome, 'events': [
                    {'event': 'chat', 'payload': {'runId': 'run-1', 'state': state}},
                    {'event': 'agent', 'payload': {'runId': 'run-1', 'stream': 'lifecycle',
                        'data': {'phase': 'error', 'error': 'Model provider unavailable'}}}]}
                with patch.object(native.OpenClawToolAgent, 'FINAL_REPLY_GRACE', .02), \
                        patch.object(native.OpenClawToolAgent, 'FINAL_REPLY_POLL', .005):
                    with self.assertRaises(AssistantError):
                        await self.run_agent()
                self.assertEqual(self.selected, ['current'])
                self.assertEqual(sum(method == 'agent' for client in self.clients for method, _ in client.calls), 1)
                self.assertFalse(self.engine.bridge.active)

    async def test_empty_final_after_confirmed_reads_uses_backup_but_writes_do_not(self):
        for readonly in (True, False):
            with self.subTest(readonly=readonly):
                self.setUp()
                self.agent._auto_tools = True
                self.agent._buffered_reply = True
                effects = []
                async def tool():
                    effects.append('original')
                    return {'ok': True, 'query_ref': 'original-receipt'}
                name = 'lighthouse_query' if readonly else 'lighthouse_submit'
                self.agent.tools[name] = Tool(tool)
                async def hook(client):
                    if not effects:
                        await self.engine.bridge.call(self.agent.token, {'session_key': self.agent.session_key,
                            'run_id': self.agent.run_id, 'tool': name, 'call_id': 'original-call', 'params': {}})
                self.scripts['current'] = {'hook': hook, 'wait': {'status': 'ok',
                    'terminalReply': {'disposition': 'visible', 'text': ''}}}
                if readonly:
                    events = await self.run_agent()
                    self.assertEqual(events[-1].result.output, 'fixture reply')
                    self.assertEqual(self.selected, ['current', 'backup'])
                    request = next(params for method, params in self.clients[-1].calls if method == 'agent')
                    self.assertIn('original-receipt', request['message'])
                else:
                    with self.assertRaises(AssistantError):
                        await self.run_agent()
                    self.assertEqual(self.selected, ['current'])
                self.assertEqual(effects, ['original'])
                self.assertFalse(self.engine.bridge.active)

    async def test_unknown_pending_or_mutating_terminal_cannot_be_replayed(self):
        for outcome, mutation in (({'status': 'in_progress'}, False),
                ({'status': 'ok', 'pendingError': True}, False),
                ({'status': 'ok', 'pendingToolCalls': ['pending']}, False),
                ({'status': 'ok', 'stopReason': 'toolUse'}, False),
                ({'status': 'ok', 'aborted': True}, False), ({'status': 'ok'}, True)):
            with self.subTest(outcome=outcome, mutation=mutation):
                self.setUp()
                self.agent._auto_tools = True
                self.agent._buffered_reply = True
                effects = []
                async def submit():
                    effects.append('original')
                    return {'ok': True, 'job_id': 'original-task'}
                self.agent.tools['lighthouse_submit'] = Tool(submit)
                async def hook(client):
                    if mutation and not effects:
                        await self.engine.bridge.call(self.agent.token, {'session_key': self.agent.session_key,
                            'run_id': self.agent.run_id, 'tool': 'lighthouse_submit', 'call_id': 'original-write', 'params': {}})
                self.scripts['current'] = {'hook': hook, 'wait': outcome, 'events': [
                    {'event': 'agent', 'payload': {'runId': 'run-1', 'stream': 'lifecycle', 'data': {'phase': 'end'}}}]}
                with patch.object(native.OpenClawToolAgent, 'FINAL_REPLY_GRACE', .02), \
                        patch.object(native.OpenClawToolAgent, 'FINAL_REPLY_POLL', .005), \
                        patch.object(native.OpenClawToolAgent, 'RUN_STATUS_POLL', .005):
                    with self.assertRaises(AssistantError):
                        await self.run_agent()
                self.assertEqual(self.selected, ['current'])
                self.assertEqual(effects, ['original'] if mutation else [])

    async def test_uncertain_connection_result_only_observes_original_run(self):
        self.scripts['current'] = {'fail': {'agent': GatewayError('DISCONNECTED')}}
        with patch.object(native, 'GatewayClient', FakeGatewayClient):
            events = await self.run_agent()
        self.assertEqual(events[-1].result.output, 'fixture reply')
        self.assertEqual(self.selected, ['current'])
        calls = [method for client in FakeGatewayClient.instances for method, _ in client.calls]
        self.assertEqual(calls.count('agent'), 1)
        self.assertIn('agent.wait', calls)

    async def test_unconfirmed_connection_failure_does_not_start_backup(self):
        self.scripts['current'] = {'fail': {'agent': GatewayError('DISCONNECTED')}}
        FakeGatewayClient.class_fail = {'agent.wait': GatewayError('DISCONNECTED')}
        with patch.object(native, 'GatewayClient', FakeGatewayClient):
            with self.assertRaises(GatewayError):
                await self.run_agent()
        self.assertEqual(self.selected, ['current'])

    async def test_terminal_acceptance_and_recovery_errors_can_switch_safely(self):
        for recovered in (False, True):
            with self.subTest(recovered=recovered):
                self.setUp()
                if recovered:
                    self.scripts['current'] = {'fail': {'agent': GatewayError('DISCONNECTED')}}
                    FakeGatewayClient.class_wait = {'status': 'error', 'error': 'Model provider unavailable'}
                else:
                    self.scripts['current'] = {'accept': {'status': 'error', 'summary': 'Model provider unavailable'}}
                self.scripts['backup'] = {'wait': {'status': 'ok', 'terminalReply': {'disposition': 'visible', 'text': 'backup answer'}}}
                with patch.object(native, 'GatewayClient', FakeGatewayClient):
                    events = await self.run_agent()
                self.assertEqual(events[-1].result.output, 'backup answer')
                self.assertEqual(self.selected, ['current', 'backup'])

    async def test_business_validation_and_empty_reply_after_tool_do_not_resubmit(self):
        for empty in (False, True):
            with self.subTest(empty=empty):
                self.setUp()
                self.agent.calls = 1
                self.scripts['current'] = {'wait': {'status': 'ok', 'terminalReply': {
                    'disposition': 'visible', 'text': '' if empty else 'unverified business success'}}}
                async def reject(answer):
                    raise ModelRetry('No reliable business evidence')
                self.agent.validators.append(reject)
                with self.assertRaises(AssistantError):
                    await self.run_agent()
                self.assertEqual(self.selected, ['current'])
                self.assertEqual(sum(method == 'agent' for client in self.clients for method, _ in client.calls), 1)

    async def test_overall_timeout_and_cancellation_clean_up_without_fallback(self):
        original_timeout = asyncio.timeout
        for cancelled in (False, True):
            with self.subTest(cancelled=cancelled):
                self.setUp()
                entered = asyncio.Event()
                async def stall(client):
                    entered.set()
                    await asyncio.Event().wait()
                self.scripts['current'] = {'hook': stall}
                with patch.object(native.asyncio, 'timeout', side_effect=lambda _: original_timeout(.05 if not cancelled else 5)):
                    task = asyncio.create_task(self.run_agent())
                    await entered.wait()
                    if cancelled:
                        task.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await task
                    else:
                        with self.assertRaises(AssistantError) as error:
                            await task
                        self.assertEqual(failure_detail(error.exception)[0], 'timeout')
                self.assertEqual(self.selected, ['current'])
                self.assertFalse(self.engine.bridge.active)
                self.assertFalse(self.items[0]['busy'])

    async def test_late_callback_from_failed_attempt_is_rejected(self):
        self.fail_model()
        async def late_callback(client):
            old_id = next(params['idempotencyKey'] for method, params in self.clients[0].calls if method == 'agent')
            with self.assertRaisesRegex(AssistantError, '运行编号已失效'):
                await self.engine.bridge.call(self.agent.token, {'session_key': self.agent.session_key,
                    'run_id': old_id, 'tool': 'lighthouse_probe', 'call_id': 'late', 'params': {}})
        self.scripts['backup'] = {'hook': late_callback}
        await self.run_agent()
        self.assertEqual(self.agent.calls, 0)

    async def test_resident_run_is_released_before_alternative_acquisition(self):
        self.agent.resident = True
        self.engine.manager.begin_run = AsyncMock()
        self.engine.manager.release = AsyncMock()
        self.fail_model()
        await self.run_agent()
        self.assertEqual(self.engine.manager.begin_run.await_count, 2)
        self.assertEqual(self.engine.manager.release.await_count, 2)
        self.assertTrue(self.agent.profile['_fallback_model'])

    def private_channel(self, directory):
        from lan_bitable_template_portal.feishu_assistant import FeishuAssistant, Inbox, parse_message
        from bin.test_feishu_assistant import event
        runtime = SimpleNamespace(state_store=Store(Path(directory) / 'fixture.sqlite3'),
            auth_manager=SimpleNamespace(_open_id_explicitly_disabled=lambda _: False))
        channel = FeishuAssistant(SimpleNamespace(bound_port=18766, preferred_port=18766), runtime, AsyncMock())
        channel.inbox = Inbox(Path(directory) / 'inbox.sqlite3')
        message = parse_message(event(chat_type='p2p', content='original question'), 'cliTest', 'ouBot')
        channel.inbox.add(message)
        channel.messenger = SimpleNamespace(react=AsyncMock(), send=AsyncMock())
        async def answer(_):
            events = await self.run_agent()
            return {'text': events[-1].result.output}
        channel.answer = AsyncMock(side_effect=answer)
        return channel, message

    async def test_private_message_receives_final_reply_on_model_success_or_failure(self):
        for failed in (False, True):
            with self.subTest(failed=failed), tempfile.TemporaryDirectory() as directory:
                self.setUp()
                self.fail_model()
                if failed:
                    self.fail_model('backup', 'private upstream secret')
                channel, message = self.private_channel(directory)
                await channel._drain(message['union_id'])
                channel.messenger.react.assert_awaited_once()
                channel.messenger.send.assert_awaited_once()
                text = channel.messenger.send.await_args.args[1]['text']
                self.assertIn('这次未能完成回答' if failed else 'fixture reply', text)
                if failed:
                    self.assertIn('诊断编号', text)
                self.assertNotIn('private upstream secret', text)
                self.assertEqual(channel.inbox.owners(), [])

    async def test_private_message_delivery_retry_never_reexecutes_model_or_tools(self):
        with tempfile.TemporaryDirectory() as directory:
            self.fail_model()
            channel, message = self.private_channel(directory)
            channel.messenger.send.side_effect = TimeoutError()
            await channel._drain(message['union_id'])
            with channel.inbox.connect() as db:
                saved = db.execute('SELECT state,reply FROM messages WHERE id=?', (message['message_id'],)).fetchone()
            self.assertEqual(saved[0], 'reply')
            self.assertIn('fixture reply', saved[1])
            channel.inbox.set(message['message_id'], 'reply', delay=0)
            channel.messenger.send.side_effect = None
            await channel._drain(message['union_id'])
            channel.answer.assert_awaited_once()
            channel.messenger.react.assert_awaited_once()
            self.assertEqual(self.selected, ['current', 'backup'])
            self.assertEqual(channel.inbox.owners(), [])


class FallbackRoutingTests(unittest.TestCase):
    def test_native_auth_cooldown_is_not_shared_between_alternatives(self):
        root = Path(tempfile.gettempdir()) / 'unused-model-route-fixture'
        profiles = [profile('current'), profile('alternative', _fallback_model=True),
                    profile('third', _fallback_model=True)]
        accounts = {account_key(str(index)): {'profile': value, 'agent_id': agent_id(str(index)),
            'config_fingerprint': 'fixture', 'root': root / str(index)} for index, value in enumerate(profiles)}
        config = build_configuration(root, accounts, 19999, model_url='http://127.0.0.1:19999/models', plugin=root / 'plugin')
        routes = [config['agents']['entries'][agent_id(str(index))]['model'] for index in range(3)]
        self.assertTrue(routes[0].startswith('lighthouse/'))
        self.assertEqual(len({route.split('/')[0] for route in routes}), 3)
        self.assertEqual(model_provider_id(profiles[1]), model_provider_id(copy.deepcopy(profiles[1])))
        self.assertNotEqual(model_provider_id(profiles[1]), model_provider_id({**profiles[1], 'key_cipher': 'changed'}))
        for index, route in enumerate(routes):
            provider, alias = route.split('/')
            self.assertEqual(config['agents']['entries'][agent_id(str(index))]['modelPolicy']['allow'], [route])
            self.assertEqual(config['models']['providers'][provider]['models'][0]['id'], alias)
        rendered = str(config)
        self.assertNotIn('fixture-cipher-', rendered)
        self.assertNotIn('https://fixture.invalid', rendered)
        self.assertEqual(config['session']['reset']['mode'], 'none')


if __name__ == '__main__':
    unittest.main()

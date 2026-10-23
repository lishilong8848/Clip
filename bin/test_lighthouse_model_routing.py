"""Bounded routing and health regressions; every gateway/model transport is a fake."""
import asyncio
import copy
import sys
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pydantic_ai import ModelRetry
from openclaw_service.assistant import lighthouse_openclaw as native
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_gateway import GatewayError
from bin import test_lighthouse_model_failover as fixtures


class ModelHealthTests(unittest.TestCase):
    def setUp(self):
        self.now = 100.0
        self.health = native._ModelHealth()
        self.health._clock = lambda: self.now
        self.key = ('account-hash', 'configuration-hash')

    def test_monotonic_finite_backoff_and_successful_recovery(self):
        with patch.object(native.time, 'time', side_effect=AssertionError('No wall clock')):
            for _ in range(20):
                self.health.record(self.key, category='model_connection')
            failed = self.health.snapshot(self.key)
            self.assertEqual(failed['cooldown_until'], self.now + 120)
            self.assertEqual(failed['failures'], 5)
            self.now = failed['cooldown_until']
            self.assertLessEqual(self.health.snapshot(self.key)['cooldown_until'], self.now)
            self.health.record(self.key, latency=8)
            self.health.record(self.key, latency=4)
        recovered = self.health.snapshot(self.key)
        self.assertEqual(recovered['failures'], 0)
        self.assertEqual(recovered['cooldown_until'], 0)
        self.assertEqual(recovered['category'], '')
        self.assertEqual(recovered['latency'], 7)

    def test_configuration_faults_have_finite_separate_backoff(self):
        self.health.record(self.key, category='model_auth')
        self.assertEqual(self.health.snapshot(self.key)['cooldown_until'], self.now + 60)
        for _ in range(20):
            self.health.record(self.key, category='model_request')
        self.assertEqual(self.health.snapshot(self.key)['cooldown_until'], self.now + 300)

    def test_business_permission_context_and_unknown_errors_do_not_poison_health(self):
        for category in ('forbidden', 'validation', 'model_context', 'vision_unsupported', 'model_protocol'):
            self.health.record(self.key, category=category)
        self.assertEqual(self.health.snapshot(self.key), {})

    def test_tracking_is_bounded_expires_and_does_not_expose_mutable_entries(self):
        for index in range(self.health.MAX_ENTRIES + 10):
            self.health.record(('account', str(index)), latency=1)
        self.assertEqual(len(self.health._entries), self.health.MAX_ENTRIES)
        self.assertEqual(self.health.snapshot(('account', '0')), {})
        key = next(iter(self.health._entries))
        self.health.snapshot(key)['latency'] = 999
        self.assertEqual(self.health.snapshot(key)['latency'], 1)
        self.now += self.health.TTL
        self.assertEqual(self.health.snapshot(key), {})
        self.assertEqual(len(self.health._entries), 0)

    def test_concurrent_readers_and_writers_share_bounded_health(self):
        def observe(worker):
            for index in range(100):
                key = ('account', str(worker * 100 + index))
                self.health.record(key, category='model_busy')
                self.health.snapshot(key)
                self.health.record(key, latency=worker + 1)
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(observe, range(8)))
        self.assertLessEqual(len(self.health._entries), self.health.MAX_ENTRIES)
        self.assertTrue(all(entry['failures'] == 0 for entry in self.health._entries.values()))


class ModelRoutingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.fixture = fixtures.ModelFailoverTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.enterContext(patch.object(native, 'GatewayClient', fixtures.FakeGatewayClient))
        self.agent = self.fixture.agent
        self.agent.turn['file_ids'] = []
        self.agent.turn['question'] = 'hello'
        self.now = 100.0
        self.agent.health._clock = lambda: self.now

    def profiles(self, *alternatives):
        profiles = [fixtures.profile('current'), *alternatives]
        self.fixture.configure(self.fixture.actor['id'], profiles)
        self.agent.profile = self.fixture.model.for_actor(self.fixture.actor['id']).profile('current')
        return profiles

    def health_key(self, identity):
        return self.agent._health_key(self.fixture.model.for_actor(self.fixture.actor['id']).profile(identity))

    def next_profile(self):
        return self.agent._next_profile({self.agent._profile_key(self.agent.profile)})

    async def test_explicit_initial_model_is_preserved_even_during_cooldown(self):
        self.profiles(fixtures.profile('reasoner', reasoning=True))
        self.agent.turn['question'] = '分析并比较维修趋势'
        original = copy.deepcopy(self.agent.turn)
        session = self.agent.session_key
        key = self.health_key('current')
        self.agent.health.record(key, category='model_auth')
        events = await self.fixture.run_agent()
        self.assertEqual(self.fixture.selected, ['current'])
        self.assertEqual(events[-1].result.output, 'fixture reply')
        self.assertEqual(self.agent.turn, original)
        self.assertEqual(self.agent.session_key, session)
        health = self.agent.health.snapshot(key)
        self.assertEqual(health['failures'], 0)
        self.assertGreaterEqual(health['latency'], 0)

    def test_vision_fallback_cannot_select_known_ocr_only_models_with_stale_flags(self):
        self.profiles(
            fixtures.profile('glm', model='WanWu/GLM-Auto', image_input=True),
            fixtures.profile('qwen', model='WanWu/Qwen-Auto', image_input=True),
            fixtures.profile('deepseek', model='WanWu/Deepseek-Auto', image_input=True),
            fixtures.profile('vision', model='WanWu/MiniMax-Auto', image_input=True))
        self.agent.profile['image_input'] = True
        self.assertEqual(self.next_profile()['id'], 'vision')

    async def test_automatic_initial_task_route_acquires_once_and_preserves_turn_and_session(self):
        self.profiles(fixtures.profile('plain'), fixtures.profile('reasoner', reasoning=True))
        self.agent.profile = {**self.agent.profile, '_automatic_selection': True}
        self.agent.turn.update(question='Analyze and compare repair trends', _profile=self.agent.profile)
        original_turn, original_actor = copy.deepcopy(self.agent.turn), copy.deepcopy(self.agent.actor)
        session = self.agent.session_key
        await self.fixture.run_agent()
        self.assertEqual(self.fixture.selected, ['reasoner'])
        self.agent._acquire_runtime.assert_awaited_once()
        self.assertEqual(self.agent.session_key, session)
        self.assertEqual(self.agent.turn, original_turn)
        self.assertEqual(self.agent.actor, original_actor)
        self.assertEqual(self.fixture.model.for_actor(self.fixture.actor['id']).settings()['active_model_id'], 'current')
        request = next(params for method, params in self.fixture.clients[0].calls if method == 'agent')
        self.assertEqual(request['sessionKey'], session)
        self.assertNotIn(':fallback:', request['idempotencyKey'])
        self.assertEqual(request['thinking'], 'xhigh')
        self.assertTrue(self.agent.profile['_fallback_model'])

    async def test_automatic_initial_route_skips_cooling_default(self):
        self.agent.profile = {**self.agent.profile, '_automatic_selection': True}
        self.agent.health.record(self.health_key('current'), category='model_connection')
        await self.fixture.run_agent()
        self.assertEqual(self.fixture.selected, ['backup'])
        self.agent._acquire_runtime.assert_awaited_once()
        self.assertEqual(self.agent.health.snapshot(self.health_key('current'))['failures'], 1)

    async def test_automatic_initial_route_cannot_start_any_cooling_model(self):
        self.agent.profile = {**self.agent.profile, '_automatic_selection': True}
        for identity in ('current', 'backup'):
            self.agent.health.record(self.health_key(identity), category='model_busy')
        with self.assertRaises(AssistantError) as raised:
            await self.fixture.run_agent()
        self.assertEqual(raised.exception.category, 'model_busy')
        self.agent._acquire_runtime.assert_not_awaited()
        self.assertEqual(self.fixture.selected, [])
        self.assertFalse(self.fixture.engine.bridge.active)

    async def test_automatic_initial_route_keeps_default_on_equal_scores_regardless_of_config_order(self):
        key = 'model:' + self.fixture.actor['id']
        saved = self.fixture.store.get_document(fixtures.NAMESPACE, key)
        saved['models'].reverse()
        self.fixture.store.put_document(fixtures.NAMESPACE, key, saved)
        self.agent.profile = {**self.agent.profile, '_automatic_selection': True}
        await self.fixture.run_agent()
        self.assertEqual(self.fixture.selected, ['current'])
        self.agent._acquire_runtime.assert_awaited_once()

    async def test_initial_routing_requires_boolean_true_not_truthy_or_legacy_values(self):
        original = dict(self.agent.profile)
        for flag in (False, 1, 'true', None):
            with self.subTest(flag=flag):
                self.fixture.selected.clear()
                self.agent.profile = {**original, '_automatic_selection': flag}
                self.agent.health.record(self.health_key('current'), category='model_auth')
                await self.fixture.run_agent()
                self.assertEqual(self.fixture.selected, ['current'])

    async def test_automatic_initial_route_cannot_enable_vision_or_tools(self):
        for capabilities, unsafe in (({'image_input': False}, {'image_input': True}),
                                     ({'tool_calls': False}, {'tool_calls': True})):
            with self.subTest(capabilities=capabilities):
                profiles = [fixtures.profile('current', **capabilities),
                            fixtures.profile('unsafe', reasoning=True, **unsafe)]
                self.fixture.configure(self.fixture.actor['id'], profiles)
                self.fixture.selected.clear()
                self.agent.profile = {**profiles[0], '_automatic_selection': True}
                self.agent.turn['question'] = 'Analyze and compare repair trends'
                await self.fixture.run_agent()
                self.assertEqual(self.fixture.selected, ['unsafe'] if 'image_input' in capabilities else ['current'])
                for capability, enabled in capabilities.items():
                    self.assertEqual(self.agent.profile.get(capability), enabled)

    async def test_automatic_attachment_route_uses_existing_context_settings(self):
        self.profiles(fixtures.profile('large', context_window=128000))
        self.agent.profile = {**self.agent.profile, '_automatic_selection': True}
        self.agent.turn['file_ids'] = ['authorized-file']
        await self.fixture.run_agent()
        self.assertEqual(self.fixture.selected, ['large'])
        self.agent._acquire_runtime.assert_awaited_once()

    async def test_automatic_initial_route_retains_existing_failure_fallback(self):
        self.profiles(fixtures.profile('reasoner', reasoning=True))
        self.agent.profile = {**self.agent.profile, '_automatic_selection': True}
        self.agent.turn['question'] = 'Analyze and compare repair trends'
        self.fixture.fail('reasoner')
        await self.fixture.run_agent()
        self.assertEqual(self.fixture.selected, ['reasoner', 'current'])
        requests = [params for client in self.fixture.clients for method, params in client.calls if method == 'agent']
        self.assertEqual(requests[1]['sessionKey'], requests[0]['sessionKey'])
        self.assertEqual(requests[1]['idempotencyKey'], requests[0]['idempotencyKey'] + ':fallback:1')

    async def test_task_routing_uses_only_configured_reasoning_and_does_not_change_selection(self):
        self.profiles(fixtures.profile('plain'), fixtures.profile('reasoner', reasoning=True))
        self.agent.turn['question'] = 'Analyze and compare repair trends'
        self.fixture.fail()
        await self.fixture.run_agent()
        self.assertEqual(self.fixture.selected, ['current', 'reasoner'])
        settings = self.fixture.model.for_actor(self.fixture.actor['id']).settings()
        self.assertEqual(settings['active_model_id'], 'current')
        self.assertEqual(self.agent.health.snapshot(self.health_key('current'))['category'], 'model_connection')
        self.assertEqual(self.agent.health.snapshot(self.health_key('reasoner'))['failures'], 0)

    def test_simple_tasks_prefer_configured_nonreasoning_alternative(self):
        self.profiles(fixtures.profile('reasoner', reasoning=True), fixtures.profile('plain'))
        self.assertEqual(self.next_profile()['id'], 'plain')

    def test_task_matching_examines_only_bounded_question_text(self):
        self.profiles(fixtures.profile('plain'), fixtures.profile('reasoner', reasoning=True))
        self.agent.turn['question'] = 'x' * 2000 + ' analyze and compare'
        self.assertEqual(self.next_profile()['id'], 'plain')

    async def test_context_failure_prefers_larger_existing_context_without_cooling_provider(self):
        self.profiles(fixtures.profile('small', context_window=32000),
                      fixtures.profile('large', context_window=128000))
        self.fixture.fail(message='Context length exceeded')
        await self.fixture.run_agent()
        self.assertEqual(self.fixture.selected, ['current', 'large'])
        self.assertEqual(self.agent.health.snapshot(self.health_key('current')), {})

    def test_attachments_prefer_larger_context_and_bad_context_config_is_skipped(self):
        self.profiles(fixtures.profile('bad', context_window='invalid'),
                      fixtures.profile('small', context_window=32000),
                      fixtures.profile('large', context_window=128000))
        self.agent.turn['file_ids'] = ['authorized-file']
        self.assertEqual(self.next_profile()['id'], 'large')

    def test_recent_success_latency_breaks_otherwise_equal_candidate_ties(self):
        self.profiles(fixtures.profile('slower'), fixtures.profile('faster'))
        self.agent.health.record(self.health_key('slower'), latency=10)
        self.agent.health.record(self.health_key('faster'), latency=2)
        self.assertEqual(self.next_profile()['id'], 'faster')

    def test_equal_candidates_keep_configuration_order(self):
        self.profiles(fixtures.profile('first'), fixtures.profile('second'))
        self.assertEqual(self.next_profile()['id'], 'first')

    async def test_cooling_candidates_are_skipped_and_all_cooling_ends_the_chain(self):
        self.profiles(fixtures.profile('backup'), fixtures.profile('third'))
        self.agent.health.record(self.health_key('backup'), category='model_busy')
        self.fixture.fail()
        self.fixture.fail('third')
        with self.assertRaises(AssistantError):
            await self.fixture.run_agent()
        self.assertEqual(self.fixture.selected, ['current', 'third'])
        self.agent.profile = self.fixture.profiles[0]
        self.assertIsNone(self.next_profile())
        self.now += 15
        self.assertEqual(self.next_profile()['id'], 'backup')

    def test_configuration_fingerprint_releases_old_cooldown_and_aliases_share_health(self):
        original = fixtures.profile('backup')
        self.profiles(original)
        key = self.health_key('backup')
        self.agent.health.record(key, category='model_auth')
        self.assertIsNone(self.next_profile())
        self.assertEqual(key, self.agent._health_key({**original, 'id': 'alias', 'name': 'alias'}))
        for field, value in (('key_cipher', 'changed-fixture'), ('endpoint', 'https://changed.invalid/v1/chat/completions'),
                             ('model', 'different-model'), ('reasoning', True), ('context_window', 128000)):
            with self.subTest(field=field):
                changed = {**original, field: value}
                self.profiles(changed)
                self.assertNotEqual(key, self.health_key('backup'))
                self.assertEqual(self.next_profile()['id'], 'backup')

    def test_health_is_isolated_by_account_even_for_same_shared_configuration(self):
        self.profiles(fixtures.profile('backup'))
        self.agent.health.record(self.health_key('backup'), category='model_auth')
        other = native.OpenClawToolAgent(self.fixture.engine, {'id': 'other-account', 'scopes': ['A']},
                                        {}, self.fixture.emit, self.agent.profile, instructions='policy')
        self.assertIs(other.health, self.agent.health)
        key = other._health_key(fixtures.profile('backup'))
        self.assertNotEqual(key, self.health_key('backup'))
        self.assertEqual(other.health.snapshot(key), {})
        self.assertNotIn('fixture-cipher', str(self.agent.health._entries))
        self.assertNotIn('fixture.invalid', str(self.agent.health._entries))

    def test_capabilities_and_account_configuration_are_hard_candidate_filters(self):
        self.profiles(fixtures.profile('vision', image_input=True), fixtures.profile('no-tools', tool_calls=False),
                      fixtures.profile('missing', key_cipher=''), fixtures.profile('backup'))
        self.fixture.configure('other-account', [fixtures.profile('other-private')])
        self.assertEqual(self.next_profile()['id'], 'backup')
        profiles = [fixtures.profile('current', tool_calls=False), fixtures.profile('enabled-tools'),
                    fixtures.profile('safe', tool_calls=False)]
        self.fixture.configure(self.fixture.actor['id'], profiles)
        self.agent.profile = profiles[0]
        self.assertEqual(self.next_profile()['id'], 'safe')

    async def test_visible_provider_failure_records_health_without_replaying(self):
        self.agent._live_text_only = True
        self.fixture.fail()
        self.fixture.scripts['current']['events'].insert(0, {'event': 'agent', 'payload': {
            'runId': 'run-1', 'stream': 'assistant', 'data': {'delta': 'visible original answer'}}})
        with self.assertRaises(AssistantError):
            await self.fixture.run_agent()
        self.assertEqual(self.fixture.selected, ['current'])
        self.assertEqual(self.agent.health.snapshot(self.health_key('current'))['category'], 'model_connection')

    async def test_visible_output_validation_failure_never_retries_or_penalizes_provider(self):
        self.agent._live_text_only = True
        self.fixture.scripts['current'] = {'events': [{'event': 'agent', 'payload': {
            'runId': 'run-1', 'stream': 'assistant', 'data': {'delta': 'visible unverified answer'}}}]}
        async def reject(_):
            raise ModelRetry('Business evidence missing')
        self.agent.validators.append(reject)
        with self.assertRaises(AssistantError):
            await self.fixture.run_agent()
        self.assertEqual(self.fixture.selected, ['current'])
        submissions = [method for client in self.fixture.clients for method, _ in client.calls if method == 'agent']
        self.assertEqual(len(submissions), 1)
        self.assertEqual(self.agent.health.snapshot(self.health_key('current')), {})

    async def test_unpublished_terminal_reply_is_validated_before_publishing(self):
        validations = []
        async def validate(answer):
            validations.append(answer)
            if len(validations) == 1:
                raise ModelRetry('Check business evidence')
            return 'verified final answer'
        self.agent.validators.append(validate)
        events = await self.fixture.run_agent()
        texts = [event.delta.content_delta if event.event_kind == 'part_delta' else event.part.content
                 for event in events if event.event_kind in {'part_start', 'part_delta'}]
        self.assertEqual(texts, ['verified final answer'])
        self.assertEqual(len(validations), 2)

    async def test_permission_validation_failure_does_not_start_or_cool_another_model(self):
        async def deny(_):
            raise AssistantError('No permission for this business', 403)
        self.agent.validators.append(deny)
        with self.assertRaises(AssistantError) as raised:
            await self.fixture.run_agent()
        self.assertEqual(raised.exception.status, 403)
        self.assertEqual(self.fixture.selected, ['current'])
        self.assertEqual(self.agent.health.snapshot(self.health_key('current')), {})

    async def test_partial_tool_receipts_cannot_trigger_validation_retry_or_fallback(self):
        self.agent.calls = 2
        self.agent.results['confirmed'] = ('signature', {'ok': True, 'job_id': 'existing-job'})
        self.fixture.scripts['current'] = {'wait': {'status': 'ok', 'terminalReply': {
            'disposition': 'visible', 'text': 'NO_REPLY'}}}
        with self.assertRaises(AssistantError):
            await self.fixture.run_agent()
        self.assertEqual(self.fixture.selected, ['current'])
        submissions = [method for client in self.fixture.clients for method, _ in client.calls if method == 'agent']
        self.assertEqual(len(submissions), 1)
        self.assertFalse(self.agent._evidence_only)
        self.assertFalse(self.fixture.engine.bridge.active)
        self.assertEqual(self.agent.health.snapshot(self.health_key('current')), {})

    async def test_observer_recovery_records_success_without_resubmission(self):
        self.agent.health.record(self.health_key('current'), category='model_connection')
        self.fixture.scripts['current'] = {'fail': {'agent': GatewayError('DISCONNECTED')}}
        await self.fixture.run_agent()
        self.assertEqual(self.fixture.selected, ['current'])
        self.assertEqual(self.agent.health.snapshot(self.health_key('current'))['failures'], 0)
        methods = [method for client in fixtures.FakeGatewayClient.instances for method, _ in client.calls]
        self.assertEqual(methods.count('agent'), 1)

    async def test_unknown_connection_outcome_does_not_poison_provider_or_replay(self):
        self.fixture.scripts['current'] = {'fail': {'agent': GatewayError('DISCONNECTED')}}
        fixtures.FakeGatewayClient.class_fail = {'agent.wait': GatewayError('DISCONNECTED')}
        with self.assertRaises(GatewayError):
            await self.fixture.run_agent()
        self.assertEqual(self.fixture.selected, ['current'])
        self.assertEqual(self.agent.health.snapshot(self.health_key('current')), {})

    async def test_cancellation_is_not_a_provider_failure(self):
        entered = asyncio.Event()
        async def stall(_):
            entered.set()
            await asyncio.Event().wait()
        self.fixture.scripts['current'] = {'hook': stall}
        task = asyncio.create_task(self.fixture.run_agent())
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.fixture.selected, ['current'])
        self.assertEqual(self.agent.health.snapshot(self.health_key('current')), {})


if __name__ == '__main__':
    unittest.main()

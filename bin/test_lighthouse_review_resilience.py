"""Offline reviewer fallback, isolation, evidence coverage, and permission checks."""
import asyncio
import copy
import json
import socket
import sys
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from openclaw_service.assistant.lighthouse_ai import AssistantError, PRIVATE_REPLY
from openclaw_service.assistant.lighthouse_context import update_task_context
from openclaw_service.assistant.lighthouse_model_health import model_health, model_health_key
from openclaw_service.assistant.lighthouse_queries import query_statistics
from openclaw_service.assistant.lighthouse_review import AnswerReviewer, READ_REPAIR_TOOLS, UNVERIFIED_REPLY, _compact_evidence


class ReviewResilienceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.actor = {'id': 'fixture-owner', 'scopes': ['D'], 'modules': ['assistant', 'learning']}
        self.profiles = {name: {'id': name, 'model': name, 'key_cipher': 'fixture-secret',
                               'endpoint': 'https://fixture.invalid/chat/completions', 'reasoning': True}
                         for name in ('writer', 'first', 'second', 'third')}
        self.model = SimpleNamespace(
            settings=lambda: {'enabled': True, 'models': [{'id': name, 'configured': True} for name in self.profiles]},
            profile=Mock(side_effect=lambda name: copy.deepcopy(self.profiles[name])), complete=Mock())
        self.assistant = SimpleNamespace(model_for=Mock(return_value=self.model))
        self.reviewer = AnswerReviewer(self.assistant)
        self.turn = {'question': 'Summarize the authorized records.', '_profile': self.profiles['writer']}
        self.result = {'answer': 'One record. [1]', 'sources': [self.source()]}
        self.authorize = AsyncMock(side_effect=lambda: copy.deepcopy(self.actor))
        self.passed = json.dumps({'passed': True, 'issues': []})
        self.rejected = json.dumps({'passed': False, 'issues': ['Unsupported answer']})
        self.model.complete.return_value = self.passed
        network = patch.object(socket.socket, 'connect', side_effect=AssertionError('No live network permitted'))
        network.start()
        self.addCleanup(network.stop)

    @staticmethod
    def source(number=1, data=None):
        return {'number': number, 'query_ref': f'query-{number}', 'title': f'Source {number}',
                'available': True, 'query_conditions': {'scope': 'D'},
                'data': data if data is not None else {'items': [{'name': 'fixture'}], 'complete': True}}

    async def run_review(self, **kwargs):
        return await self.reviewer.run(self.actor, self.turn, [], self.result, self.authorize, **kwargs)

    def packet(self):
        return json.loads(self.model.complete.call_args_list[0].args[0][1]['content'])

    async def test_four_models_two_fail_third_healthy_and_health_contains_no_content(self):
        self.model.complete.side_effect = [TimeoutError('private provider text'), OSError('fixture-secret'), self.passed]
        result = await self.run_review()
        self.assertEqual(result['answer_review']['status'], 'passed')
        self.assertEqual([call.kwargs['profile']['id'] for call in self.model.complete.call_args_list],
                         ['first', 'second', 'third'])
        health = model_health(self.assistant)
        self.assertEqual(health.snapshot(model_health_key(self.actor, self.profiles['first']))['category'], 'timeout')
        self.assertEqual(health.snapshot(model_health_key(self.actor, self.profiles['second']))['category'], 'model_connection')
        self.assertGreaterEqual(health.snapshot(model_health_key(self.actor, self.profiles['third']))['latency'], 0)
        self.assertNotIn('fixture-secret', str(health._entries))
        self.assertNotIn('private provider text', str(health._entries))

    async def test_unusable_configuration_is_skipped_individually(self):
        self.model.profile.side_effect = [AssistantError('missing configuration', 503), None, self.profiles['third']]
        result = await self.run_review()
        self.assertEqual(result['answer_review']['status'], 'passed')
        self.assertEqual(self.model.complete.call_args.kwargs['profile']['id'], 'third')

    async def test_fourth_writer_is_reached_if_all_independent_profiles_fail(self):
        self.model.complete.side_effect = [TimeoutError(), TimeoutError(), TimeoutError(), self.passed]
        result = await self.run_review()
        self.assertEqual(result['answer_review']['status'], 'passed')
        self.assertEqual(self.model.complete.call_args.kwargs['profile']['id'], 'writer')

    async def test_health_sorts_healthy_reviewers_by_latency_before_writer(self):
        health = model_health(self.assistant)
        for name, latency in [('first', 5), ('second', 3), ('third', 1), ('writer', .01)]:
            health.record(model_health_key(self.actor, self.profiles[name]), latency=latency)
        health.record(model_health_key(self.actor, self.profiles['third']), category='timeout')
        await self.run_review()
        self.assertEqual(self.model.complete.call_args.kwargs['profile']['id'], 'second')
        self.assertFalse(self.model.complete.call_args.kwargs['profile']['reasoning'])
        self.assertTrue(self.profiles['second']['reasoning'])

    async def test_healthy_writer_precedes_cooling_reviewers(self):
        health = model_health(self.assistant)
        for name in ('first', 'second', 'third'):
            health.record(model_health_key(self.actor, self.profiles[name]), category='timeout')
        await self.run_review()
        self.assertEqual(self.model.complete.call_args.kwargs['profile']['id'], 'writer')

    async def test_four_users_and_topics_review_independently_without_250ms_drop(self):
        barrier = threading.Barrier(4)
        packets = []
        def complete(messages, **kwargs):
            packets.append(json.loads(messages[1]['content']))
            barrier.wait(timeout=3)
            time.sleep(.35)
            return self.passed
        self.model.complete.side_effect = complete
        calls = []
        for index in range(4):
            actor = {**self.actor, 'id': f'owner-{index}', '_feishu_topic': {'id': f'topic-{index}', 'context': f'context-{index}'}}
            turn = {**self.turn, 'question': f'question-{index}'}
            result = {**self.result, 'answer': f'answer-{index}'}
            calls.append(self.reviewer.run(actor, turn, [], result, AsyncMock(return_value=actor)))
        results = await asyncio.gather(*calls)
        self.assertEqual([result['answer_review']['status'] for result in results], ['passed'] * 4)
        self.assertEqual(self.model.complete.call_count, 4)
        self.assertEqual({(p['question'], p['answer'], p['topic_context']) for p in packets},
                         {(f'question-{i}', f'answer-{i}', f'context-{i}') for i in range(4)})

    def large_source(self):
        rows = [{'name': f'entry-{i}', 'person': {'id': f'person-{i % 3}'}, 'score': i,
                 'description': 'unrelated detail ' * 120} for i in range(60)]
        rows[-1]['name'] = 'TARGET'
        return self.source(data={'items': rows, 'complete': True, 'total': 60})

    async def test_oversize_evidence_has_exact_aggregates_relevant_rows_and_explicit_limits(self):
        self.result = {'answer': 'TARGET has score 59. [1]', 'sources': [self.large_source()]}
        self.turn['question'] = 'Give TARGET score, distinct person IDs and total score.'
        before = copy.deepcopy(self.result)
        result = await self.run_review()
        self.assertEqual(self.result, before)
        packet = self.packet()
        self.assertLessEqual(len(self.model.complete.call_args.args[0][1]['content'].encode()), 48000)
        source = packet['evidence'][0]
        self.assertTrue(source['excerpt_only'])
        self.assertIn('TARGET', [row['name'] for row in source['data']['items']])
        coverage = source['coverage']['lists'][0]
        self.assertEqual((coverage['loaded_count'], coverage['shown_count'], coverage['omitted_count']), (60, 8, 52))
        self.assertIn(59, coverage['row_indices'])
        receipts = {(s['operation'], tuple(s['field'])): s for s in source['statistics']}
        self.assertEqual(receipts['count', ()]['value'], 60)
        self.assertEqual(receipts['distinct', ('person', 'id')]['value'], 3)
        self.assertEqual(receipts['sum', ('score',)]['value'], 1770)
        self.assertEqual(receipts['mean', ('score',)]['value'], 29.5)
        self.assertEqual(receipts['group_count', ('person', 'id')]['groups'],
                         [{'value': f'person-{i}', 'count': 20} for i in range(3)])
        self.assertEqual(result['answer_review']['status'], 'limited')
        self.assertFalse(result['answer_review']['coverage']['full_detail'])
        self.assertIn('逐条明细未核验', result['answer'])

    async def test_partial_and_missing_aggregate_inputs_do_not_become_complete(self):
        self.turn['question'] = 'Total score and missing scores?'
        source = self.large_source()
        source['data'].update(complete=False, has_more=True, total=90)
        del source['data']['items'][-1]['score']
        self.result['sources'] = [source]
        await self.run_review()
        statistics = self.packet()['evidence'][0]['statistics']
        self.assertTrue(all(not receipt['complete'] for receipt in statistics))
        total = next(s for s in statistics if s['operation'] == 'sum' and s['field'] == ['score'])
        self.assertEqual((total['value'], total['missing_count'], total['loaded_count']), (1711, 1, 60))

    def test_large_nested_data_has_bounded_statistics_and_explicit_discovery_gaps(self):
        row = {'score': 2, **{f'unrelated-{i}': i for i in range(100)}, 'nested': [[{}] * 1000] * 20}
        source = {**self.source(data={f'list-{i}': [row] * 300 for i in range(30)}), 'state': 'found'}
        with patch('openclaw_service.assistant.lighthouse_review.query_statistics', wraps=query_statistics) as aggregate:
            compact = _compact_evidence(source, {'score'})
        self.assertLessEqual(aggregate.call_count, 16)
        self.assertLessEqual(len(compact['coverage']['lists']), 4)
        self.assertGreater(compact['coverage']['omitted_branch_count'], 0)
        self.assertEqual(compact['coverage']['lists'][0]['relevance_scanned_count'], 128)
        self.assertTrue(compact['coverage']['lists'][0]['field_discovery_limited'])
        self.assertTrue(all(not stat['field'] or stat['field'] == ['score'] for stat in compact['statistics']))
        self.assertTrue({'plan_task', 'query_progress'} <= READ_REPAIR_TOOLS)

    async def test_goals_prioritize_early_reference_and_keep_all_deliverable_descriptions(self):
        self.result = {'answer': 'A supported partial result.', 'sources': [
            self.source(i, {'text': f'source-{i} ' * 700}) for i in range(1, 21)]}
        self.turn['_answer_goals'] = [{'description': f'Deliverable {i}', 'kind': 'data',
                                     'status': 'partial', 'query_refs': ['query-1'], 'limitation': 'Partial detail'}
                                    for i in range(8)]
        update_task_context(self.turn, self.actor, {'scopes': ['D'], 'shift': 'night', 'deliverables': [
            {'id': f'goal-{i}', 'description': f'Required deliverable {i}', 'kind': 'data'} for i in range(8)]})
        await self.run_review()
        packet = self.packet()
        self.assertEqual(packet['evidence'][0]['query_ref'], 'query-1')
        self.assertEqual(len(packet['response_goals']), 8)
        self.assertIn('Required deliverable 7', packet['task_contract'])
        self.assertIn('night', packet['task_contract'])
        self.assertEqual(len(packet['coverage']['sources']), 20)
        omitted = sum(not source['included'] for source in packet['coverage']['sources'])
        self.assertGreater(omitted, 0)
        self.assertEqual(packet['omitted_source_count'], omitted)

    async def test_other_owner_contract_and_history_are_excluded(self):
        update_task_context(self.turn, self.actor, {'scopes': ['D'], 'shift': 'private-contract'})
        self.turn['_task_context']['owner'] = 'other-owner'
        await self.reviewer.run(self.actor, self.turn,
            [{'question': 'private-history', 'scopes': ['A'], 'answer': 'hidden'}], self.result, self.authorize)
        self.assertNotIn('task_contract', self.packet())
        self.assertEqual(self.packet()['history'], [])

    async def test_authorization_changes_block_fallback_and_configuration_denial_propagates(self):
        for changed in ({**self.actor, 'id': 'other'}, {**self.actor, 'scopes': []}, {**self.actor, 'modules': []}):
            with self.subTest(changed=changed):
                self.model.complete.reset_mock(side_effect=True)
                self.model.complete.side_effect = TimeoutError()
                self.authorize.side_effect = [copy.deepcopy(self.actor), copy.deepcopy(self.actor), changed]
                with self.assertRaises(AssistantError) as raised:
                    await self.run_review()
                self.assertEqual(raised.exception.status, 403)
                self.assertEqual(self.model.complete.call_count, 1)
        self.authorize.side_effect = lambda: copy.deepcopy(self.actor)
        self.model.profile.side_effect = AssistantError('denied', 403)
        with self.assertRaises(AssistantError):
            await self.run_review()

    async def test_known_rejected_answer_does_not_escape_failed_rewrite(self):
        self.model.complete.side_effect = [self.rejected, TimeoutError()]
        result = await self.run_review()
        self.assertEqual(result['answer'], UNVERIFIED_REPLY)
        self.assertEqual(result['answer_review']['status'], 'needs_review')

    async def test_no_tools_business_replay_auth_expansion_or_credential_in_packet(self):
        before = copy.deepcopy((self.actor, self.turn, self.result))
        self.model.complete.side_effect = [self.rejected, 'Only one supported record.', self.passed]
        result = await self.run_review()
        self.assertEqual(result['answer_review']['status'], 'corrected')
        self.assertEqual((self.actor, self.turn, self.result), before)
        for call in self.model.complete.call_args_list:
            self.assertEqual(set(call.kwargs), {'profile', 'structured', 'max_tokens', 'timeout'})
            self.assertEqual([message['role'] for message in call.args[0]], ['system', 'user'])
            self.assertNotIn('fixture-secret', str(call.args))
        for call in self.assistant.model_for.call_args_list:
            self.assertEqual(call.args[0], self.actor)
        self.model.complete.reset_mock()
        for result in ({**self.result, 'plan': {'id': 'already-submitted'}},
                       {**self.result, 'clarification': {'options': []}}, {**self.result, 'answer': PRIVATE_REPLY}):
            checked = await self.reviewer.run(self.actor, self.turn, [], result, self.authorize)
            self.assertEqual(checked['answer'], result['answer'])
        self.model.complete.assert_not_called()

    async def test_total_review_deadline_stops_fallback(self):
        real_timeout = asyncio.timeout_at
        self.model.complete.side_effect = lambda *args, **kwargs: (time.sleep(.2), self.passed)[1]
        def deadline(when):
            now = asyncio.get_running_loop().time()
            self.assertTrue(44 < when - now <= 45)
            return real_timeout(now + .05)
        with patch('openclaw_service.assistant.lighthouse_review.asyncio.timeout_at', side_effect=deadline):
            result = await self.run_review()
        self.assertEqual(result['answer_review']['status'], 'unavailable')
        self.assertEqual(self.model.complete.call_count, 1)


if __name__ == '__main__':
    unittest.main()

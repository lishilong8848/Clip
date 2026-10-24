"""Evaluator checks use synthetic APIs and prohibit outbound socket connections."""
import asyncio
import copy
import json
import os
import socket
import sys
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tools import evaluate_lighthouse_intelligence as evaluator
from tools import evaluate_lighthouse_reviews as reviews


class EngineEvaluationTests(unittest.TestCase):
    def setUp(self):
        connect = socket.socket.connect

        def local_only(sock, address):
            # Windows asyncio implements its wake-up socketpair over loopback.
            if not isinstance(address, tuple) or address[0] not in ('127.0.0.1', '::1'):
                raise AssertionError('Network forbidden')
            return connect(sock, address)

        self.network = patch.object(socket.socket, 'connect', local_only)
        self.network.start()
        self.addCleanup(self.network.stop)
        self.calls = [{'scope': 'D', 'page': 1, 'page_size': 20},
                      {'scope': 'D', 'page': 2, 'page_size': 2}]
        self.result = {'answer': evaluator.COMPLETE, 'sources': [
            {'scopes': ['D'], 'data': {'complete': True, 'items': copy.deepcopy(evaluator.ROWS)}}]}

    def test_offline_selfcheck_six_passes_without_claiming_model_quality(self):
        results = asyncio.run(evaluator.evaluate(evaluator.fixture_factory, evaluator.PROFILE))
        self.assertEqual(len(results), 6)
        for case in results:
            self.assertTrue(case['execution_ok'], case)
            self.assertTrue(case['mechanical_checks_passed'], case)
            self.assertEqual(case['read_calls'], 2)
            self.assertEqual(case['semantic_status'], 'not_applicable')
            self.assertIsNone(case['answer_review'])
        with patch('builtins.print'), patch.object(sys, 'argv', ['evaluate']):
            self.assertEqual(evaluator.main(), 0)

    def test_contradiction_missing_reads_wrong_scope_and_generic_failure_cannot_pass(self):
        self.assertTrue(all(evaluator.case_checks(self.result, self.calls, False).values()))
        for answer in ('共3人：测试甲、测试乙。', '共2人：测试甲、测试乙。合计3人。',
                       '读取失败，请稍后重试。'):
            self.result['answer'] = answer
            self.assertFalse(all(evaluator.case_checks(self.result, self.calls, False).values()), answer)
        self.result['answer'] = evaluator.COMPLETE
        for calls in ([], self.calls[:1], [{**call, 'scope': 'E'} for call in self.calls]):
            self.assertFalse(all(evaluator.case_checks(self.result, calls, False).values()))
        self.result['sources'] = []
        self.assertFalse(all(evaluator.case_checks(self.result, self.calls, False).values()))

    def test_expected_partial_is_successful_handling_not_a_complete_answer(self):
        self.result.update(answer=evaluator.PARTIAL, sources=[
            {'scopes': ['D'], 'data': {'complete': False, 'items': copy.deepcopy(evaluator.ROWS[:2])}}])
        self.assertTrue(all(evaluator.case_checks(self.result, self.calls, True).values()))
        self.result['answer'] = evaluator.COMPLETE
        self.assertFalse(all(evaluator.case_checks(self.result, self.calls, True).values()))
        self.result['answer'] = '发生故障，请稍后重试。'
        self.assertFalse(all(evaluator.case_checks(self.result, self.calls, True).values()))

    def test_execution_error_is_distinct_and_does_not_expose_exception_body(self):
        async def broken(*args, **kwargs):
            raise OSError('private-token /private/path')
        with patch.object(evaluator.IntelligenceEvaluationTests, 'evaluate', broken), \
                patch('builtins.print') as output, patch.object(sys, 'argv', ['evaluate']):
            self.assertEqual(evaluator.main(), 1)
        report = output.call_args.args[0]
        self.assertNotIn('private', report)
        for case in json.loads(report)[0]['results']:
            self.assertFalse(case['execution_ok'])
            self.assertFalse(case['mechanical_checks_passed'])
            self.assertEqual(case['error'], 'OSError')

    def test_live_mode_passes_env_key_and_still_requires_manual_review(self):
        opened = []

        @asynccontextmanager
        async def configured(custom, profile):
            opened.append((custom.unprotect(profile['key_cipher']), profile['model']))
            async with evaluator.fixture_factory() as model:
                yield model

        with patch.dict(os.environ, {'LIGHTHOUSE_EVAL_API_KEY': 'fixture-secret'}), \
                patch.object(evaluator, 'configured_model', configured), patch('builtins.print') as output, \
                patch.object(sys, 'argv', ['evaluate', '--live', '--endpoint',
                    'https://fixture.example/v1/chat/completions', '--model', 'test-model']):
            self.assertEqual(evaluator.main(), 0)
        self.assertEqual(opened, [('fixture-secret', 'test-model')] * 6)
        report = output.call_args.args[0]
        self.assertNotIn('fixture-secret', report)
        for case in json.loads(report)[0]['results']:
            self.assertTrue(case['execution_ok'])
            self.assertTrue(case['mechanical_checks_passed'])
            self.assertEqual(case['semantic_status'], 'needs_manual_review')

    def test_review_fixture_mode_never_reads_model_settings_or_claims_accuracy(self):
        with patch('tools.evaluate_lighthouse_models.load_shared_models', side_effect=AssertionError('No configuration reads')), \
                patch('builtins.print') as output, patch.object(sys, 'argv', ['review-evaluation']):
            self.assertEqual(reviews.main(), 0)
        report = json.loads(output.call_args.args[0])
        self.assertEqual(report['mode'], 'offline_fixture')
        self.assertEqual(report['model_calls'], 0)
        self.assertEqual(report['semantic_status'], 'not_measured')
        self.assertEqual(report['pairs'], 10)

    def test_review_false_acceptance_and_unavailable_are_not_counted_as_correct(self):
        for status in ('passed', 'limited'):
            self.assertEqual(reviews.outcome(status, False), 'false_acceptance')
            self.assertEqual(reviews.outcome(status, True), 'accepted_correct')
        self.assertEqual(reviews.outcome('needs_repair', True), 'false_rejection')
        self.assertEqual(reviews.outcome('needs_repair', False), 'rejected_incorrect')
        for status in (None, 'unavailable', 'needs_review', 'original_flow', 'unknown'):
            self.assertEqual(reviews.outcome(status, False), 'unavailable')

    def test_cabinet_review_fixtures_use_native_source_identity(self):
        profile = {'id': 'fixture', 'model': 'fixture', 'key_cipher': 'fixture-secret',
                   'endpoint': 'https://fixture.invalid/chat/completions'}
        model = SimpleNamespace(complete=Mock(return_value='{"passed":true,"issues":[]}'))
        cases = tuple(case for case in reviews.CASES if case[0].startswith('cabinet_'))
        rows = asyncio.run(reviews.evaluate(model, [profile], cases=cases))
        self.assertEqual(len(rows), 6)
        for call in model.complete.call_args_list:
            source = json.loads(call.args[0][1]['content'])['evidence'][0]
            self.assertEqual(source['field_contract']['state_labels']['off'], '未上电/已下电')
            self.assertNotIn('field_contract', source['data'])
        self.assertEqual([row['outcome'] for row in rows].count('false_acceptance'), 3)

    def test_review_evaluation_uses_real_reviewer_and_does_not_hide_invalid_output(self):
        profile = {'id': 'fixture-profile', 'model': 'fixture', 'key_cipher': 'fixture-secret',
                   'endpoint': 'https://fixture.invalid/chat/completions'}
        model = SimpleNamespace(complete=Mock(return_value='{"passed":true,"issues":[]}'))
        rows = asyncio.run(reviews.evaluate(model, [profile], cases=reviews.CASES[:1]))
        self.assertEqual([row['outcome'] for row in rows], ['accepted_correct', 'false_acceptance'])
        self.assertEqual(model.complete.call_count, 2)
        self.assertNotIn('fixture-secret', json.dumps(rows))
        self.assertNotIn('fixture.invalid', json.dumps(rows))
        packet = json.loads(model.complete.call_args.args[0][1]['content'])
        self.assertEqual(packet['evidence'][0]['data'], reviews.CASES[0][2])
        model.complete.return_value = 'invalid review output'
        rows = asyncio.run(reviews.evaluate(model, [profile], cases=reviews.CASES[:1]))
        self.assertEqual([row['outcome'] for row in rows], ['unavailable', 'unavailable'])


if __name__ == '__main__':
    unittest.main()

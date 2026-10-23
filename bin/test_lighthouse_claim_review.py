"""Offline claim grounding and repair contracts; semantic verdicts are model fixtures."""
import copy
import json
import socket
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_review import AnswerReviewer, REVIEW, REWRITE, UNVERIFIED_REPLY


class ClaimReviewTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.actor = {'id': 'claim-owner', 'scopes': ['D'], 'modules': ['assistant', 'knowledge']}
        self.profile = {'id': 'writer', 'model': 'writer', 'key_cipher': 'fixture-key',
                        'endpoint': 'https://fixture.invalid/chat/completions'}
        self.model = SimpleNamespace(
            settings=lambda: {'enabled': True, 'models': [{'id': 'checker', 'configured': True}]},
            profile=lambda _: {**self.profile, 'id': 'checker', 'model': 'checker'}, complete=Mock())
        self.assistant = SimpleNamespace(model_for=Mock(return_value=self.model))
        self.reviewer = AnswerReviewer(self.assistant)
        self.authorize = AsyncMock(side_effect=lambda: copy.deepcopy(self.actor))
        self.turn = {'question': 'Give the roster and count for this shift.', '_profile': self.profile}
        self.good = 'Night roster: Ava and Bo. [1]'
        self.bad = 'There are three people. [1]'
        self.result = {'answer': self.good + '\n' + self.bad, 'sources': [self.source(1,
            'Night roster: Ava and Bo. Two people, three attendance records.')],
            'goals': [{'goal_id': 'roster', 'status': 'complete'}, {'goal_id': 'count', 'status': 'partial'}]}
        self.support = self.claim(self.good, 'support', [(1, 'Night roster: Ava and Bo.')])
        self.mismatch = self.claim(self.bad, 'mismatch', [(1, 'Two people, three attendance records.')],
                                   'Count distinct people, not attendance records.')
        self.passed = self.verdict(True)
        network = patch.object(socket.socket, 'connect', side_effect=AssertionError('No live network permitted'))
        network.start()
        self.addCleanup(network.stop)

    @staticmethod
    def source(number, text, **kwargs):
        return {'number': number, 'title': 'Fixture source', 'query_ref': f'query-{number}',
                'available': True, 'query_conditions': {'building': 'D', 'shift': 'night'},
                'data': {'text': text, 'complete': True}, **kwargs}

    @staticmethod
    def claim(text, status, evidence=(), issue=''):
        return {'claim': text, 'status': status,
                'evidence': [{'source_number': number, 'quote': quote, 'kind': 'original'}
                             for number, quote in evidence], 'issue': issue}

    @staticmethod
    def verdict(passed, claims=None, issues=None):
        value = {'passed': passed, 'issues': issues if issues is not None else ([] if passed else ['Fix the count.'])}
        if claims is not None:
            value['claim_assessments'] = claims
        return json.dumps(value)

    async def run_review(self, **kwargs):
        return await self.reviewer.run(self.actor, self.turn, [], self.result, self.authorize, **kwargs)

    def packet(self, index=0):
        return json.loads(self.model.complete.call_args_list[index].args[0][1]['content'])

    async def test_legacy_verdict_and_result_fields_remain_compatible(self):
        self.result['answer'] = self.good
        self.model.complete.return_value = self.passed
        checked = await self.run_review()
        self.assertEqual(checked['answer_review'], {'status': 'passed', 'repairs': 0})
        self.assertEqual(checked['answer'], self.good)
        self.assertEqual(checked['goals'], self.result['goals'])

    async def test_incorrect_count_vs_roster_is_corrected_without_discarding_good_conclusion(self):
        fixed = self.good + '\nThere are two people. [1]'
        before = copy.deepcopy(self.result)
        self.model.complete.side_effect = [self.verdict(False, [self.support, self.mismatch]), fixed, self.passed]
        checked = await self.run_review()
        self.assertEqual(checked['answer'], fixed)
        self.assertEqual(checked['answer_review']['status'], 'corrected')
        feedback = self.packet(1)
        self.assertEqual(feedback['preserve_claims'], [self.good])
        self.assertEqual(feedback['response_goals'], before['goals'])
        self.assertEqual(feedback['claim_assessments'][1], self.mismatch)
        self.assertEqual(self.result, before)
        self.assertEqual(checked['goals'], before['goals'])
        self.assertNotIn(self.mismatch['issue'], checked['answer'])

    async def test_irrelevant_building_equipment_and_threshold_require_local_repair(self):
        for sentence, evidence, issue in (
            ('Use this procedure in building D. [1]', 'This procedure applies only in building A.', 'Building does not match.'),
            ('Use this procedure on pump P2. [1]', 'This procedure applies only to pump P1.', 'Equipment does not match.'),
            ('Start at pressure 10. [1]', 'Start only when pressure exceeds 10 and approval is recorded.',
             'The threshold is strict and approval is required.'),
        ):
            with self.subTest(sentence=sentence):
                self.result.update(answer=sentence, sources=[self.source(1, evidence, kind='company_knowledge')])
                assessment = self.claim(sentence, 'mismatch', [(1, evidence)], issue)
                self.model.complete.return_value = self.verdict(False, [assessment], [issue])
                self.model.complete.reset_mock()
                checked = await self.run_review(defer_repair=True)
                self.assertEqual(checked['answer_review']['status'], 'needs_repair')
                self.assertEqual(checked['answer_review']['claim_assessments'], [
                    {'status': 'mismatch', 'source_numbers': [1], 'evidence_kinds': ['original']}])
                self.model.complete.assert_called_once()
                self.assertEqual(self.packet()['evidence'][0]['conditions']['building'], 'D')

    async def test_version_conflict_cannot_be_passed_just_because_one_date_is_newer(self):
        self.result.update(answer='The newer threshold 12 is effective. [2]', sources=[
            self.source(1, 'Approved 2025-01: threshold 10.', kind='company_knowledge'),
            self.source(2, 'Draft 2026-01: threshold 12; approval pending.', kind='company_knowledge')])
        conflict = self.claim(self.result['answer'], 'conflict', [
            (1, 'Approved 2025-01: threshold 10.'), (2, 'Draft 2026-01: threshold 12; approval pending.')],
            'No approved replacement or effective-date evidence resolves the conflict.')
        self.model.complete.return_value = self.verdict(True, [conflict])
        checked = await self.run_review(defer_repair=True)
        self.assertEqual(checked['answer_review']['status'], 'needs_repair')
        self.assertIn(conflict['issue'], checked['answer_review']['issues'])
        self.assertIn('不能只按日期最新', REVIEW)
        self.assertIn('批准、生效、替代关系及适用范围', REVIEW)

    async def test_missing_basis_can_have_no_evidence_and_preserves_other_goal(self):
        missing = self.claim(self.bad, 'missing', issue='The current shift count was not provided.')
        self.model.complete.return_value = self.verdict(False, [self.support, missing])
        checked = await self.run_review(defer_repair=True)
        self.assertEqual(checked['answer_review']['preserved_claim_count'], 1)
        self.assertNotIn('preserve_claims', checked['answer_review'])
        self.assertEqual(checked['goals'], self.result['goals'])

    async def test_failed_or_destructive_rewrite_retains_only_verified_claims(self):
        for rewrite in (TimeoutError(), 'There are two people. [1]'):
            with self.subTest(rewrite=rewrite):
                self.model.complete.reset_mock(side_effect=True)
                self.model.complete.side_effect = [self.verdict(False, [self.support, self.mismatch]), rewrite]
                checked = await self.run_review()
                self.assertEqual(checked['answer_review']['status'], 'needs_review')
                self.assertTrue(checked['answer'].startswith(self.good))
                self.assertNotIn(self.bad, checked['answer'])
                self.assertNotIn(self.mismatch['issue'], checked['answer'])
                self.assertEqual(checked['goals'], self.result['goals'])
                self.assertEqual(self.model.complete.call_count, 2)

    async def test_invalid_claims_and_fabricated_quotes_do_not_become_verified(self):
        cases = [
            self.claim(self.good, 'support', [(99, 'Night roster: Ava and Bo.')]),
            self.claim(self.good, 'support', [(1, 'Night roster: Ava and Cy.')]),
            self.claim('An invented candidate sentence.', 'support', [(1, 'Night roster: Ava and Bo.')]),
            self.claim(self.good, 'support'),
            self.claim(self.good, 'support', [(1, 'Night roster: Ava and Bo.')], 'A contradiction'),
            self.claim(self.good, 'conflict', [(1, 'Night roster: Ava and Bo.')], 'Only one piece of evidence'),
            self.claim(self.good, 'mismatch', [(1, 'Night roster: Ava and Bo.')]),
        ]
        for assessment in cases:
            with self.subTest(assessment=assessment):
                self.model.complete.reset_mock(side_effect=True)
                self.model.complete.return_value = self.verdict(True, [assessment])
                checked = await self.run_review(defer_repair=True)
                self.assertEqual(checked['answer_review']['status'], 'unavailable')
                self.assertNotIn('claim_assessments', checked['answer_review'])

    async def test_company_quotes_must_come_from_visible_body_not_metadata(self):
        self.result['answer'] = 'Threshold is 10. [1]'
        for data, quote in (({'name': 'Threshold is 10.', 'text': 'No threshold here.'}, 'Threshold is 10.'),
                            ({'text': 'visible ' * 900 + 'HIDDEN_TAIL'}, 'HIDDEN_TAIL'),
                            ({'text': 'visible ' * 3000}, '[内容过长,未发送]'),
                            ({'text': 'Alpha condition. Beta action.'}, 'Alpha condition. ... Beta action.')):
            with self.subTest(quote=quote):
                self.result['sources'] = [self.source(1, '', kind='company_knowledge', data=data)]
                self.model.complete.return_value = self.verdict(True, [self.claim(self.result['answer'], 'support', [(1, quote)])])
                self.model.complete.reset_mock()
                checked = await self.run_review()
                self.assertEqual(checked['answer_review']['status'], 'unavailable')
                self.assertFalse(self.packet()['coverage']['full_detail'])

    async def test_unavailable_or_duplicate_sources_cannot_ground_a_claim(self):
        for sources in ([self.source(1, 'Night roster: Ava and Bo.', available=False)],
                        [self.source(1, 'Night roster: Ava and Bo.'), self.source(1, 'Conflicting duplicate')]):
            with self.subTest(sources=sources):
                self.result['sources'] = sources
                self.model.complete.return_value = self.verdict(True, [self.support])
                checked = await self.run_review()
                self.assertEqual(checked['answer_review']['status'], 'unavailable')

    async def test_false_citation_is_rejected_even_when_reviewer_passes_or_is_offline(self):
        for number in ('9999', '9' * 5000):
            self.result['answer'] = f'A supported result. [{number}]'
            self.model.complete.return_value = self.passed
            checked = await self.run_review(defer_repair=True)
            self.assertEqual(checked['answer_review']['status'], 'needs_repair')
        self.model.complete.side_effect = TimeoutError()
        checked = await self.run_review()
        self.assertEqual(checked['answer'], UNVERIFIED_REPLY)
        self.assertNotIn('[9999]', checked['answer'])

    async def test_narrow_company_answer_has_coverage_metadata_without_blanket_disclaimer(self):
        self.result['answer'] = 'Threshold is 10. [1]'
        self.result['sources'] = [self.source(1, '[Approved] Threshold is 10.', kind='company_knowledge',
            coverage={'kind': 'chunk', 'full_document': False}, visual_evidence={'read': False, 'pages': [2]})]
        self.result['knowledge_retrieval'] = {'repair_rounds': 1, 'searches': 2, 'sources': 1, 'complete': True}
        assessment = self.claim(self.result['answer'], 'support', [(1, '[Approved] Threshold is 10.')])
        for verdict in (self.passed, self.verdict(True, [assessment])):
            with self.subTest(verdict=verdict):
                self.model.complete.return_value = verdict
                checked = await self.run_review()
                self.assertEqual(checked['answer'], self.result['answer'])
                self.assertFalse(checked['answer_review']['coverage']['full_detail'])
                self.assertEqual(checked['answer_review']['status'], 'limited')
                self.assertTrue(self.packet()['knowledge_retrieval']['complete'])
                source = self.packet()['evidence'][0]
                self.assertEqual(source['source_coverage'], {'kind': 'chunk', 'full_document': False})
                self.assertEqual(source['visual_evidence'], {'read': False, 'pages': [2]})

    async def test_small_partial_source_does_not_claim_full_coverage(self):
        self.result['sources'][0]['data'].update(complete=False, has_more=True)
        self.result['answer'] = 'Known people include Ava and Bo. [1]'
        self.model.complete.return_value = self.passed
        checked = await self.run_review()
        self.assertEqual(checked['answer_review']['status'], 'limited')
        self.assertFalse(checked['answer_review']['coverage']['full_detail'])
        self.assertEqual(checked['answer'], self.result['answer'])

    async def test_visual_and_ocr_claims_use_typed_evidence_without_requiring_body_text(self):
        for kind, source_kind, text, answer in (
            ('visual', 'model_visual_interpretation', 'Arrows connect pump P1 to valve V2.',
             'Image observation on page 2: pump P1 connects to valve V2. [1]'),
            ('ocr', 'ocr_transcription', 'Rated pressure: 10 bar',
             'OCR on page 2 reads a rated pressure of 10 bar. [1]'),
        ):
            with self.subTest(kind=kind):
                self.result.update(answer=answer, sources=[self.source(1, '', kind='company_knowledge', data={
                    'text': '', 'source_version': 2, 'coverage': {'full_document': False},
                    'visual_evidence': [{'kind': source_kind, 'text': text, 'visual_understanding': kind == 'visual',
                                         'location': 'page 2', 'document_id': 'fixture-doc', 'version': 2, 'hash': 'fixture-hash'}]})])
                assessment = self.claim(answer, 'support', [(1, text)])
                assessment['evidence'][0]['kind'] = kind
                self.model.complete.return_value = self.verdict(True, [assessment])
                self.model.complete.reset_mock()
                checked = await self.run_review()
                self.assertEqual(checked['answer'], answer)
                self.assertEqual(checked['answer_review']['status'], 'limited')
                self.assertEqual(checked['answer_review']['claim_assessments'][0]['evidence_kinds'], [kind])
                source = self.packet()['evidence'][0]
                self.assertEqual(source['data']['source_version'], 2)
                self.assertEqual(source['source_coverage'], {'full_document': False})
                self.assertNotIn(text, json.dumps(checked['answer_review']))

    async def test_visual_observation_cannot_masquerade_as_original_or_ocr(self):
        self.result.update(answer='The image shows pump P1. [1]', sources=[self.source(1, '', kind='company_knowledge',
            data={'text': '', 'visual_evidence': [{'kind': 'model_visual_interpretation',
                  'visual_understanding': True, 'text': 'A pump marked P1 is visible.'}]})])
        for kind in ('original', 'ocr'):
            with self.subTest(kind=kind):
                assessment = self.claim(self.result['answer'], 'support', [(1, 'A pump marked P1 is visible.')])
                assessment['evidence'][0]['kind'] = kind
                self.model.complete.return_value = self.verdict(True, [assessment])
                checked = await self.run_review()
                self.assertEqual(checked['answer_review']['status'], 'unavailable')
        self.result['sources'][0]['data']['visual_evidence'][0]['visual_understanding'] = False
        assessment['evidence'][0]['kind'] = 'visual'
        self.model.complete.return_value = self.verdict(True, [assessment])
        checked = await self.run_review()
        self.assertEqual(checked['answer_review']['status'], 'unavailable')

    async def test_raw_company_quotes_are_transient_in_success_repair_and_failure(self):
        quote = 'Fixture rule: only approved changes may be scheduled.'
        good = 'Approval is required before scheduling. [1]'
        bad = 'An unapproved change can also be scheduled. [1]'
        self.result.update(answer=good + '\n' + bad, sources=[self.source(1, quote, kind='company_knowledge')])
        support = self.claim(good, 'support', [(1, quote)])
        mismatch = self.claim(bad, 'mismatch', [(1, quote)], 'The approval condition was dropped.')
        for mode in ('passed', 'deferred', 'failed', 'corrected'):
            with self.subTest(mode=mode):
                self.model.complete.reset_mock(side_effect=True)
                rejection = self.verdict(False, [support, mismatch])
                if mode == 'passed':
                    self.result['answer'] = good
                    self.model.complete.return_value = self.verdict(True, [support])
                else:
                    self.result['answer'] = good + '\n' + bad
                    self.model.complete.side_effect = ([rejection] if mode == 'deferred' else
                        [rejection, TimeoutError()] if mode == 'failed' else
                        [rejection, good, self.verdict(True, [support])])
                checked = await self.run_review(defer_repair=mode == 'deferred')
                metadata = json.dumps(checked['answer_review'])
                self.assertNotIn(quote, metadata)
                self.assertNotIn(good, metadata)
                self.assertNotIn('"quote"', metadata)
                self.assertNotIn('preserve_claims', metadata)
                # The parent replaces transient source bodies with the public metadata before storage.
                checked['sources'] = [{'number': 1, 'kind': 'company_knowledge', 'data': {'version': 1}}]
                self.assertNotIn(quote, json.dumps(checked))

    async def test_unavailable_fallback_sanitizes_original_answer(self):
        self.result['answer'] = self.good + '\napi_key: not-a-real-secret'
        self.model.complete.side_effect = TimeoutError()
        checked = await self.run_review()
        self.assertTrue(checked['answer'].startswith(self.good))
        self.assertNotIn('not-a-real-secret', checked['answer'])
        self.assertNotIn('not-a-real-secret', str(self.model.complete.call_args_list))
        self.model.complete.reset_mock(side_effect=True)
        self.model.complete.return_value = self.verdict(False)
        checked = await self.run_review(defer_repair=True)
        self.assertNotIn('not-a-real-secret', checked['answer'])

    async def test_company_history_profile_and_permissions_stay_isolated_without_tools(self):
        actor = {**self.actor, '_feishu_topic': {'id': 'topic-one', 'context': 'Public topic text'}}
        authorize = AsyncMock(return_value=actor)
        history = [{'question': 'private other topic', 'answer': 'private answer', 'feishu_topic': 'topic-two', 'scopes': ['D']},
                   {'question': 'private scope', 'answer': 'private answer', 'feishu_topic': 'topic-one', 'scopes': ['A']},
                   {'question': 'private module', 'answer': 'private answer', 'feishu_topic': 'topic-one',
                    'scopes': ['D'], 'access_modules': ['payroll']}]
        self.model.complete.side_effect = [self.verdict(False, [self.support, self.mismatch]),
                                           self.good + '\nThere are two people. [1]', self.passed]
        await self.reviewer.run(actor, self.turn, history, self.result, authorize)
        self.assertEqual(self.packet()['history'], [])
        for call in self.model.complete.call_args_list:
            self.assertEqual(set(call.kwargs), {'profile', 'structured', 'max_tokens', 'timeout'})
            self.assertNotIn('fixture-key', str(call.args))
            self.assertNotIn('private answer', str(call.args))
        for call in self.assistant.model_for.call_args_list:
            self.assertEqual(call.args[0], actor)
        self.assertIn('不执行/发送/提交任何业务', REWRITE)

    async def test_permission_revoked_after_rejection_blocks_preserved_fallback(self):
        self.model.complete.return_value = self.verdict(False, [self.support, self.mismatch])
        self.authorize.side_effect = [copy.deepcopy(self.actor)] * 3 + [{**self.actor, 'modules': []}]
        with self.assertRaises(AssistantError) as error:
            await self.run_review()
        self.assertEqual(error.exception.status, 403)


if __name__ == '__main__':
    unittest.main()

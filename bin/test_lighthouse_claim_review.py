"""Offline claim grounding and repair contracts; semantic verdicts are model fixtures."""
import copy
import datetime as dt
from decimal import Inexact, localcontext
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
from openclaw_service.assistant.lighthouse_review import _retains_claim, _citation_numbers, _supported_reply


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
        self.result = {'answer': self.good + '\n\n' + self.bad, 'sources': [self.source(1,
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

    async def test_claim_line_ids_preserve_markdown_and_resolve_only_current_candidate(self):
        self.result['answer'] = '**Two people.**\n\n| Ava | Bo |'
        self.result['sources'] = [self.source(1, 'Two people: Ava and Bo.')]
        self.model.complete.return_value = self.verdict(True, [{
            'claim_id': 1, 'status': 'support', 'evidence': [
                {'source_number': 1, 'quote': 'Two people'}], 'issue': ''}, {
            'claim_id': 3, 'status': 'support', 'evidence': [
                {'source_number': 1, 'quote': 'Ava and Bo'}], 'issue': ''}])
        checked = await self.run_review()
        self.assertEqual(checked['answer_review']['status'], 'passed')
        self.assertEqual(checked['answer'], self.result['answer'])
        self.assertEqual(self.packet()['answer_lines'], {'1': '**Two people.**', '3': '| Ava | Bo |'})
        self.assertNotIn('claim_id', json.dumps(checked))
        self.assertNotIn('answer_lines', json.dumps(checked))

    async def test_claim_line_ids_cannot_guess_blank_missing_or_altered_sentences(self):
        self.result['answer'] = '**Two people.**\n\n| Ava | Bo |'
        self.result['sources'] = [self.source(1, 'Two people: Ava and Bo.')]
        valid = {'claim_id': 1, 'status': 'support', 'evidence': [
            {'source_number': 1, 'quote': 'Two people'}], 'issue': ''}
        for change in ({'claim_id': 2}, {'claim_id': 4}, {'claim_id': 0}, {'claim_id': -1},
                       {'claim_id': True}, {'claim_id': '1'}, {'claim': 'Three people.'},
                       {'evidence': [{'source_number': 99, 'quote': 'Two people'}]}):
            with self.subTest(change=change):
                self.model.complete.return_value = self.verdict(True, [{**valid, **change}])
                self.assertEqual((await self.run_review())['answer_review']['status'], 'unavailable')

    async def test_repair_resolves_ids_then_regenerates_line_map_without_leaking_labels(self):
        self.result['answer'] = self.good + '\n\n' + self.bad
        positive = {**self.support, 'claim_id': 1}
        negative = {**self.mismatch, 'claim_id': 3}
        positive.pop('claim'); negative.pop('claim')
        fixed = self.good + '\nThere are two people. [1]'
        self.model.complete.side_effect = [self.verdict(False, [positive, negative]), fixed,
            self.verdict(True, [{'claim_id': 2, 'status': 'support', 'evidence': [
                {'source_number': 1, 'quote': 'Two people'}], 'issue': ''}])]
        checked = await self.run_review()
        self.assertEqual(checked['answer_review']['status'], 'corrected')
        self.assertEqual(checked['answer'], fixed)
        self.assertEqual(self.packet(1)['preserve_claims'], [self.good])
        self.assertEqual(self.packet(1)['claim_assessments'][0]['claim'], self.good)
        self.assertEqual(self.packet(2)['answer_lines']['2'], 'There are two people. [1]')
        self.assertNotIn('3', self.packet(2)['answer_lines'])

    async def test_line_ids_never_truncate_long_lines_or_expand_the_review_budget(self):
        claim = self.claim('First known line.', 'support', [(1, 'First known line.')])
        self.result['sources'] = [self.source(1, 'First known line.')]
        self.result['answer'] = 'context ' * 160 + '\nFirst known line.'
        self.model.complete.return_value = self.verdict(True, [{**claim, 'claim_id': 2}])
        checked = await self.run_review()
        self.assertEqual(checked['answer_review']['status'], 'passed')
        self.assertNotIn('1', self.packet()['answer_lines'])
        self.assertEqual(self.packet()['answer'], self.result['answer'])
        self.result['answer'] = '\n'.join(['First known line.'] * 1100)
        self.model.complete.reset_mock()
        self.model.complete.return_value = self.verdict(True, [claim])
        checked = await self.run_review()
        self.assertEqual(checked['answer_review']['status'], 'passed')
        self.assertEqual(checked['answer'], self.result['answer'])
        self.assertFalse('answer_lines' in self.packet())
        self.assertLessEqual(len(self.model.complete.call_args.args[0][1]['content'].encode('utf-8')), 48000)
        self.model.complete.return_value = self.verdict(True, [{**claim, 'claim_id': 1}])
        self.assertEqual((await self.run_review())['answer_review']['status'], 'unavailable')

    async def test_rewrite_feedback_is_bounded_and_does_not_repeat_line_maps(self):
        lines = [f'Fact {i}: ' + 'verified detail ' * 50 for i in range(8)]
        self.result.update(answer='\n'.join(lines), sources=[self.source(1, '', data={
            'facts': lines, 'padding': [{'note': 'unrelated data ' * 30, 'id': i} for i in range(70)]})])
        claims = [self.claim(line, 'support', [(1, f'Fact {i}:')]) for i, line in enumerate(lines[:-1])]
        claims.append(self.claim(lines[-1], 'mismatch', [(1, 'Fact 7:')], 'Correct the last fact.'))
        fixed = '\n'.join([*lines[:-1], 'Corrected last fact.'])
        self.model.complete.side_effect = [self.verdict(False, claims), fixed, self.passed]
        checked = await self.run_review()
        self.assertEqual(checked['answer_review']['status'], 'corrected')
        self.assertEqual(checked['answer'], fixed)
        self.assertFalse('answer_lines' in self.packet(1))
        self.assertEqual(self.packet(1)['preserve_claims'], lines[:-1])
        self.assertTrue(all(len(call.args[0][1]['content'].encode('utf-8')) <= 48000
                            for call in self.model.complete.call_args_list))

    async def test_rewrite_preserves_verified_json_text_and_rechecks_permission_after_compaction(self):
        from openclaw_service.assistant import lighthouse_review as review
        for literal in ('{"total":2}', '["A","B"]'):
            with self.subTest(literal=literal):
                self.model.complete.reset_mock(side_effect=True)
                self.result.update(answer=literal + '\nWrong sentence.', sources=[self.source(1, '', data={'total': 2})])
                claims = [self.claim(literal, 'support', [(1, '2')]),
                          self.claim('Wrong sentence.', 'mismatch', [(1, '2')], 'Fix the sentence.')]
                self.model.complete.side_effect = [self.verdict(False, claims), literal + '\nFixed sentence.', self.passed]
                checked = await self.run_review()
                self.assertEqual(checked['answer_review']['status'], 'corrected')
                self.assertEqual(self.packet(1)['preserve_claims'], [literal])
                self.assertEqual(self.packet(1)['claim_assessments'][0]['claim'], literal)
        original_packet = review._review_packet
        count = 0
        def compact(packet):
            nonlocal count
            count += 1
            value = original_packet(packet)
            if count == 2:
                self.actor = {**self.actor, 'scopes': []}
            return value
        self.model.complete.reset_mock(side_effect=True)
        self.model.complete.return_value = self.verdict(False, claims)
        with patch.object(review, '_review_packet', compact), self.assertRaises(AssistantError) as raised:
            await self.run_review()
        self.assertEqual(raised.exception.status, 403)
        self.model.complete.assert_called_once()

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

    async def test_structured_business_quotes_match_fields_in_one_visible_object(self):
        self.result.update(answer='Two tasks; Ava starts at 08:30. [1]', sources=[self.source(1, '', data={
            'total': 2, 'complete': True, 'items': [
                {'name': 'Ava', 'time': '08:30', 'enabled': True, 'count': 1},
                {'name': 'Bo', 'time': '09:00', 'enabled': False, 'count': 2}]
        })])
        for quote in ('"total": 2', '{"time":"08:30","name":"Ava"}',
                      '"name": "Bo", "count": 2'):
            with self.subTest(quote=quote):
                assessment = self.claim(self.result['answer'], 'support', [(1, quote)])
                self.model.complete.return_value = self.verdict(True, [assessment])
                checked = await self.run_review()
                self.assertEqual(checked['answer_review']['status'], 'passed')
                self.assertEqual(checked['answer'], self.result['answer'])
        for quote in ('"name":"Ava","time":"09:00"', '"total":3',
                      '"total":999,"total":2', '{}', '"missing":null',
                      '"name":"Ava","enabled":1', '"name":"Ava","count":true',
                      '"name":"Ava","count":1.0', '"total":NaN'):
            with self.subTest(rejected=quote):
                assessment = self.claim(self.result['answer'], 'support', [(1, quote)])
                self.model.complete.return_value = self.verdict(True, [assessment])
                checked = await self.run_review()
                self.assertEqual(checked['answer_review']['status'], 'unavailable')

    async def test_structured_quotes_cannot_replace_company_body_or_ocr(self):
        self.result.update(answer='Two items. [1]', sources=[self.source(1, '', kind='company_knowledge',
            data={'text': 'No count in the document.', 'total': 2, 'visual_evidence': []})])
        for kind in ('original', 'ocr', 'visual'):
            assessment = self.claim(self.result['answer'], 'support', [(1, '"total":2')])
            assessment['evidence'][0]['kind'] = kind
            self.model.complete.return_value = self.verdict(True, [assessment])
            checked = await self.run_review()
            self.assertEqual(checked['answer_review']['status'], 'unavailable')

    async def test_structured_quotes_cannot_turn_hidden_markers_into_evidence(self):
        self.result['answer'] = 'Only the visible field is verified.'
        for marker in ('[敏感信息已隐藏]', '[凭证已隐藏]', '[内容过长,未发送]', '[复核列表预算限制,未核验]'):
            for value in (marker, {'nested': [marker]}):
                self.result['sources'] = [self.source(1, '', data={'note': value, 'visible': 'Known text.'})]
                for escaped in (False, True):
                    with self.subTest(marker=marker, value=value, escaped=escaped):
                        quote = json.dumps({'note': value}, ensure_ascii=escaped)
                        self.model.complete.return_value = self.verdict(True, [self.claim(
                            self.result['answer'], 'support', [(1, quote)])])
                        self.assertEqual((await self.run_review())['answer_review']['status'], 'unavailable')
                self.model.complete.return_value = self.verdict(True, [self.claim(
                    self.result['answer'], 'support', [(1, '"visible":"Known text."')])])
                self.assertEqual((await self.run_review())['answer_review']['status'], 'passed')

    async def test_business_path_evidence_matches_exact_visible_values_without_json_in_strings(self):
        self.result.update(answer='Two people; Ava has no pending tasks.', sources=[self.source(1, '', data={
            'total': 2, 'complete': True, 'scopes': ['D', 'E'], 'counts': {'active': 1, 'closed': 2}, 'items': [
                {'name': 'Ava', 'pending': None, 'enabled': False, 'count': 1.0}, {'name': 'Bo', 'count': 1}]})])
        for path, value in ((['total'], 2), (['items', '0', 'name'], 'Ava'),
                            (['items', '0', 'pending'], None), (['items', '0', 'enabled'], False),
                            (['items', '0', 'count'], 1.0), (['scopes'], ['D', 'E']),
                            (['counts'], {'closed': 2, 'active': 1})):
            with self.subTest(path=path):
                claim = self.claim(self.result['answer'], 'support')
                claim['evidence'] = [{'source_number': 1, 'path': path, 'value': value, 'kind': 'original'}]
                self.model.complete.return_value = self.verdict(True, [claim])
                checked = await self.run_review()
                self.assertEqual(checked['answer_review']['status'], 'passed')
                self.assertNotIn('path', json.dumps(checked['answer_review']))
                self.assertEqual(checked['answer'], self.result['answer'])

    async def test_business_path_evidence_rejects_wrong_record_types_and_unknown_fields(self):
        self.result.update(answer='Ava has two tasks.', sources=[self.source(1, '', data={
            'total': 2, 'items': [{'name': 'Ava', 'count': 1, 'enabled': True, 'nullable': None},
                                {'name': 'Bo', 'count': 2}], 'query_guidance': 'not an external rule'})])
        valid = {'source_number': 1, 'path': ['items', '0', 'count'], 'value': 1}
        for changes in ({'value': 2}, {'value': True}, {'value': 1.0}, {'value': '1'}, {'value': None},
                        {'value': float('nan')}, {'value': float('inf')}, {'value': {'count': 1}},
                        {'path': ['items', '1', 'name'], 'value': 'Ava'},
                        {'path': ['items', '-1', 'count'], 'value': 2}, {'path': ['items', '2', 'count']},
                        {'path': ['items', 0, 'count']}, {'path': []}, {'path': None},
                        {'path': ['items', '０', 'count']}, {'path': ['x' * 201]},
                        {'path': ['items'] * 13}, {'path': ['items', '0', 'missing'], 'value': None},
                        {'path': ['query_guidance'], 'value': 'source rule not returned'},
                        {'quote': 'Ava'}, {'kind': 'visual'}, {'kind': 'ocr'}, {'source_number': 9}):
            with self.subTest(changes=changes):
                claim = self.claim(self.result['answer'], 'support')
                claim['evidence'] = [{**valid, **changes}]
                self.model.complete.return_value = self.verdict(True, [claim])
                checked = await self.run_review()
                self.assertEqual(checked['answer_review']['status'], 'unavailable')
        claim['evidence'] = [{'source_number': 1, 'path': ['items', '0', 'nullable']}]
        self.model.complete.return_value = self.verdict(True, [claim])
        self.assertEqual((await self.run_review())['answer_review']['status'], 'unavailable')

    async def test_path_evidence_cannot_use_company_metadata_or_hidden_excerpt_markers(self):
        self.result['answer'] = 'The threshold is two.'
        claim = self.claim(self.result['answer'], 'support')
        for source, path, value in (
            (self.source(1, 'Threshold two.', kind='company_knowledge', data={'total': 2, 'text': 'Threshold two.'}), ['total'], 2),
            (self.source(1, '', data={'text': 'visible ' * 3000}), ['text'], '[内容过长,未发送]'),
            (self.source(1, '', data={'total': 2, '_private': 'hidden'}), ['_private'], 'hidden'),
            (self.source(1, '', data={'visual_evidence': [{'text': 'two'}]}), ['visual_evidence', '0', 'text'], 'two'),
        ):
            with self.subTest(path=path):
                self.result['sources'] = [source]
                claim['evidence'] = [{'source_number': 1, 'path': path, 'value': value}]
                self.model.complete.return_value = self.verdict(True, [claim])
                self.assertEqual((await self.run_review())['answer_review']['status'], 'unavailable')

        for marker in ('[敏感信息已隐藏]', '[凭证已隐藏]', '[内容过长,未发送]',
                       '[原资料后续未纳入复核节选]', '[复核列表预算限制,未核验]',
                       '[复核节选预算限制,未核验]', '[节选深度限制]', 'x' * 4001):
            with self.subTest(marker=marker):
                self.result['sources'] = [self.source(1, '', data={'note': marker})]
                claim['evidence'] = [{'source_number': 1, 'path': ['note'], 'value': marker}]
                self.model.complete.return_value = self.verdict(True, [claim])
                self.assertEqual((await self.run_review())['answer_review']['status'], 'unavailable')

        for actual, value in ((['D', 'E'], ['E', 'D']), ({'count': 1}, {'count': True}),
                              ({'count': 1}, {'count': 1.0}), ({'a': 1, 'b': 2}, {'a': 1}),
                              (['[敏感信息已隐藏]'], ['[敏感信息已隐藏]'])):
            self.result['sources'] = [self.source(1, '', data={'field': actual})]
            claim['evidence'] = [{'source_number': 1, 'path': ['field'], 'value': value}]
            self.model.complete.return_value = self.verdict(True, [claim])
            self.assertEqual((await self.run_review())['answer_review']['status'], 'unavailable')

    async def test_path_grounding_preserves_source_uniqueness_and_negative_verdict(self):
        good = self.source(1, '', data={'total': 2})
        part = {'source_number': 1, 'path': ['total'], 'value': 2, 'kind': 'original'}
        claim = self.claim(self.result['answer'], 'support')
        claim['evidence'] = [part]
        for sources in ([{**good, 'available': False}], [good, copy.deepcopy(good)]):
            self.result['sources'] = sources
            self.model.complete.return_value = self.verdict(True, [claim])
            self.assertEqual((await self.run_review())['answer_review']['status'], 'unavailable')
        self.result['sources'] = [good]
        conflict = {**claim, 'status': 'conflict', 'issue': 'conflicting counts', 'evidence': [part, part]}
        self.model.complete.return_value = self.verdict(True, [conflict])
        self.assertEqual((await self.run_review())['answer_review']['status'], 'unavailable')
        negative = {**claim, 'status': 'mismatch', 'issue': 'Fix the count.',
                    'evidence': [{**part, 'value': 999}]}
        self.model.complete.return_value = self.verdict(False, [negative], ['Fix the count.'])
        checked = await self.run_review(defer_repair=True)
        self.assertEqual(checked['answer_review']['status'], 'needs_repair')
        self.assertNotIn('claim_assessments', checked['answer_review'])

    async def test_format_recovery_gives_next_reviewer_only_fixed_hint_and_original_packet(self):
        valid = self.claim(self.good, 'support', [(1, 'Night roster: Ava and Bo.')])
        invalid = {**valid, 'claim': 'MODEL_PRIVATE_REPLY_NOT_A_CANDIDATE'}
        for first, hint in (('{"passed":true,UNTRUSTED', '合法JSON'),
                            (self.verdict(True, [invalid]), '逐字复制')):
            with self.subTest(hint=hint):
                self.result['answer'] = self.good
                self.model.complete.reset_mock(side_effect=True)
                self.model.complete.side_effect = [first, self.verdict(True, [valid])]
                checked = await self.run_review()
                self.assertEqual(checked['answer_review']['status'], 'passed')
                self.assertEqual(self.model.complete.call_count, 2)
                calls = self.model.complete.call_args_list
                second = calls[1].args[0]
                self.assertIn('格式恢复提示', second[0]['content'])
                self.assertIn(hint, second[0]['content'])
                self.assertNotIn('UNTRUSTED', str(second))
                self.assertNotIn('MODEL_PRIVATE_REPLY', str(second))
                self.assertEqual(calls[0].args[0][1]['content'], second[1]['content'])
                self.assertEqual(checked['answer'], self.good)

    async def test_path_review_cannot_deliver_after_authorization_is_revoked(self):
        self.result.update(answer='Two tasks.', sources=[self.source(1, '', data={'total': 2})])
        claim = self.claim(self.result['answer'], 'support')
        claim['evidence'] = [{'source_number': 1, 'path': ['total'], 'value': 2}]
        def complete(*args, **kwargs):
            self.actor = {**self.actor, 'scopes': []}
            return self.verdict(True, [claim])
        self.model.complete.side_effect = complete
        with self.assertRaises(AssistantError) as raised:
            await self.run_review()
        self.assertEqual(raised.exception.status, 403)

    async def test_numeric_quotes_cannot_match_part_of_a_different_number(self):
        self.result['answer'] = 'The visible amount is two. [1]'
        for value, quote in ((12, '2'), (-2, '2'), (2.5, '2'), ('Total: 12.', '2'),
                             ('Value: -2', '2'), ('Value: 2e3', '2'), ('Total: 12 tasks.', '2 tasks'),
                             ('Threshold: 20', 'Threshold: 2'), ('{"total":20}', '"total":2')):
            with self.subTest(value=value, quote=quote):
                self.result['sources'] = [self.source(1, '', data={'value': value})]
                self.model.complete.return_value = self.verdict(True, [self.claim(
                    self.result['answer'], 'support', [(1, quote)])])
                checked = await self.run_review()
                self.assertEqual(checked['answer_review']['status'], 'unavailable')
        for value in (2, 'Total: 2.', '共2人'):
            self.result['sources'] = [self.source(1, '', data={'value': value})]
            self.model.complete.return_value = self.verdict(True, [self.claim(
                self.result['answer'], 'support', [(1, '2')])])
            checked = await self.run_review()
            self.assertEqual(checked['answer_review']['status'], 'passed')

    async def test_arithmetic_contradiction_overrules_positive_review_and_preserves_only_correct_fragment(self):
        good, bad, fixed = 'Volume: 1200 L.', 'Converted: 12 m3.', 'Converted: 1.2 m3.'
        self.result.update(answer=good + ' ' + bad, sources=[self.source(1, '', data={
            'volume_liters': 1200, 'liters_per_cubic_meter': 1000})])
        evidence = [{'source_number': 1, 'path': [key], 'value': value} for key, value in
                    (('volume_liters', 1200), ('liters_per_cubic_meter', 1000))]
        positive = {**self.claim(good, 'support'), 'evidence': evidence[:1]}
        incorrect = {**self.claim(bad, 'support'), 'evidence': evidence, 'calculations': [
            {'expression': '1200/1000', 'stated_result': '12'}]}
        corrected = {**incorrect, 'claim': fixed, 'calculations': [
            {'expression': '1200/1000', 'stated_result': '1.2'}]}
        self.model.complete.side_effect = [self.verdict(True, [positive, incorrect]), good + ' ' + fixed,
                                          self.verdict(True, [positive, corrected])]
        checked = await self.run_review()
        self.assertEqual(checked['answer_review']['status'], 'corrected')
        self.assertEqual(checked['answer'], good + ' ' + fixed)
        feedback = self.packet(1)
        self.assertEqual(feedback['preserve_claims'], [good])
        self.assertEqual(feedback['claim_assessments'][1]['status'], 'mismatch')
        self.assertNotIn('calculations', json.dumps(checked))
        self.assertNotIn('1200/1000', json.dumps(checked['answer_review']))

    async def test_rewrite_can_rejoin_verified_prose_but_still_checks_the_new_answer(self):
        good, bad, fixed = '用了1200升水。', '换算为12立方米。', '用了1200升水,换算为1.2立方米。'
        self.result.update(answer=good + '\n' + bad, sources=[self.source(1, '', data={
            'liters': 1200, 'conversion': 1000})])
        evidence = [{'source_number': 1, 'path': ['liters'], 'value': 1200},
                    {'source_number': 1, 'path': ['conversion'], 'value': 1000}]
        positive = {**self.claim(good, 'support'), 'evidence': evidence[:1]}
        negative = {**self.claim(bad, 'mismatch', issue='Incorrect conversion.'), 'evidence': evidence}
        verified = {**self.claim(fixed, 'support'), 'evidence': evidence,
                    'calculations': [{'expression': '1200/1000', 'stated_result': '1.2'}]}
        self.model.complete.side_effect = [self.verdict(False, [positive, negative]), fixed, self.verdict(True, [verified])]
        checked = await self.run_review()
        self.assertEqual(checked['answer_review']['status'], 'corrected')
        self.assertEqual(checked['answer'], fixed)
        self.assertEqual(self.model.complete.call_count, 3)
        self.assertEqual(self.packet(2)['answer'], fixed)
        for claim, answer in (('Total: 1.', 'Total: 1.2.'), ('Total: 1.', 'Total: 1,200.'),
                              ('Total: 1.', 'Total: 1 200.'), ('Total: 1.', 'Total: 1\u00a0200.'),
                              ('Total: 1.', 'Total: 1\u202f200.'), ('Total: 1.', 'Total: 1  200.'),
                              ('200 units.', '1,200 units.'), ('200 units.', '1 200 units.'),
                              ('200 units.', '1  200 units.'), ('200 units.', '1\t\t200 units.'),
                              ('200 units.', '1,  200 units.'),
                              ('1 person.', '11 person.'), ('1 person.', '-1 person.'),
                              ('Used 1.2 L.', 'Used 12 L.'), ('Used 1200 L.', 'Used 1200 m3.'),
                              ('Original code: x=2/1.', 'Original code: x=1/2.')):
            self.assertFalse(_retains_claim(answer, claim), (claim, answer))
        for claim, answer in (('Two people.', 'Two people, one task.'), ('量为1.2。', '量为1.2，已确认。'),
                              ('共有2人。', '共有2人,1人已完成。'), ('用了1200升水。', '用了1200升水,1.2立方米。'),
                              ('Value: 2.5.', 'Value: 2.5; confirmed.'), ('Ava and Bo.', 'Ava and Bo.')):
            self.assertTrue(_retains_claim(answer, claim), (claim, answer))

    async def test_display_rounding_and_negative_verdict_is_never_promoted(self):
        self.result.update(answer='Rate: 66.67%.', sources=[self.source(1, '', data={'done': 2, 'total': 3})])
        claim = {**self.claim(self.result['answer'], 'support'), 'evidence': [
            {'source_number': 1, 'path': ['done'], 'value': 2},
            {'source_number': 1, 'path': ['total'], 'value': 3}],
            'calculations': [{'expression': '2/3*100', 'stated_result': '66.67'}]}
        self.model.complete.return_value = self.verdict(True, [claim])
        self.assertEqual((await self.run_review())['answer_review']['status'], 'passed')
        claim['calculations'][0]['decimal_places'] = 2
        self.model.complete.return_value = self.verdict(True, [claim])
        self.assertEqual((await self.run_review())['answer_review']['status'], 'passed')
        claim.update(status='missing', issue='The sample does not cover all requested buildings.')
        self.model.complete.return_value = self.verdict(False, [claim], [claim['issue']])
        checked = await self.run_review(defer_repair=True)
        self.assertEqual(checked['answer_review']['status'], 'needs_repair')
        self.assertEqual(checked['answer_review']['claim_assessments'][0]['status'], 'missing')
        self.result['answer'] = 'Rate: 66.68%.'
        claim.update(claim=self.result['answer'], status='support', issue='')
        claim['calculations'][0]['stated_result'] = '66.68'
        self.model.complete.return_value = self.verdict(True, [claim])
        self.assertEqual((await self.run_review(defer_repair=True))['answer_review']['status'], 'needs_repair')

    async def test_calculations_cannot_invent_numbers_run_code_or_replace_source_evidence(self):
        self.result.update(answer='Converted: 12 m3.', sources=[self.source(1, '', data={'liters': 1200})])
        claim = {**self.claim(self.result['answer'], 'support'), 'evidence': [
            {'source_number': 1, 'path': ['liters'], 'value': 1200}]}
        for check in ({'expression': '12', 'stated_result': '1.2'},
                      {'expression': '12', 'stated_result': '2'},
                      {'expression': '__import__("os").system("echo unsafe")', 'stated_result': '12'},
                      {'expression': '1/0', 'stated_result': '12'},
                      {'expression': '9**99', 'stated_result': '12'},
                      {'expression': '12', 'stated_result': True},
                      {'expression': '12', 'stated_result': 'NaN'},
                      {'expression': '12', 'stated_result': '12', 'decimal_places': True},
                      {'expression': '12', 'stated_result': '12', 'decimal_places': -1},
                      {'expression': '12', 'stated_result': '12', 'decimal_places': 13}):
            with self.subTest(check=check):
                self.model.complete.return_value = self.verdict(True, [{**claim, 'calculations': [check]}])
                self.assertEqual((await self.run_review())['answer_review']['status'], 'unavailable')
        for literal in ('1e999999999999999999999', '1e101', '1e-101'):
            self.result['answer'] = literal
            self.model.complete.return_value = self.verdict(True, [{**claim, 'claim': literal,
                'calculations': [{'expression': '1', 'stated_result': literal}]}])
            self.assertEqual((await self.run_review())['answer_review']['status'], 'unavailable')
        self.result['answer'] = claim['claim']
        self.model.complete.return_value = self.verdict(True, [{**claim, 'evidence': [],
            'calculations': [{'expression': '6*2', 'stated_result': '12'}]}])
        self.assertEqual((await self.run_review())['answer_review']['status'], 'unavailable')

    async def test_numeric_json_results_and_invalid_arithmetic_keep_existing_review_contract(self):
        self.result.update(answer='Converted: 1.2 m3.', sources=[self.source(1, '', data={'liters': 1200})])
        claim = {**self.claim(self.result['answer'], 'support'), 'evidence': [
            {'source_number': 1, 'path': ['liters'], 'value': 1200}],
            'calculations': [{'expression': '1200/1000', 'stated_result': 1.2}]}
        self.model.complete.return_value = self.verdict(True, [claim])
        self.assertEqual((await self.run_review())['answer_review']['status'], 'passed')
        claim.update(status='mismatch', issue='Wrong applicable unit.')
        claim['calculations'][0]['stated_result'] = '1.2 m3'
        self.model.complete.return_value = self.verdict(False, [claim], ['Wrong applicable unit.'])
        checked = await self.run_review(defer_repair=True)
        self.assertEqual(checked['answer_review']['status'], 'needs_repair')
        self.assertEqual(checked['answer_review']['issues'], ['Wrong applicable unit.'])
        self.assertNotIn('claim_assessments', checked['answer_review'])

    async def test_arithmetic_mismatch_cannot_be_lost_through_overlapping_positive_claims(self):
        self.result.update(answer='Converted: 12 m3.', sources=[self.source(1, '', data={'liters': 1200})])
        claim = {**self.claim(self.result['answer'], 'support'), 'evidence': [
            {'source_number': 1, 'path': ['liters'], 'value': 1200}]}
        fragment = {**claim, 'claim': '12 m3.', 'calculations': [
            {'expression': '1200/1000', 'stated_result': '12'}]}
        self.model.complete.return_value = self.verdict(True, [claim, fragment])
        checked = await self.run_review(defer_repair=True)
        self.assertEqual(checked['answer_review']['status'], 'needs_repair')
        self.assertTrue(all(item['status'] == 'mismatch' for item in checked['answer_review']['claim_assessments']))
        self.assertNotIn('preserved_claim_count', checked['answer_review'])
        good = 'Volume: 1200 L.'
        self.result['answer'] = good + ' ' + self.result['answer']
        parent = {**claim, 'claim': self.result['answer']}
        self.model.complete.return_value = self.verdict(True, [parent, {**claim, 'claim': good}, fragment])
        checked = await self.run_review(defer_repair=True)
        self.assertEqual(checked['answer_review']['status'], 'needs_repair')
        self.assertEqual(checked['answer_review']['preserved_claim_count'], 1)

    async def test_invalid_calculation_envelopes_cannot_erase_a_negative_verdict(self):
        for envelope in (None, '1/0', {}, ['1/0'], [1], [True], [{}] * 4):
            self.model.complete.return_value = self.verdict(False, [{**self.mismatch, 'calculations': envelope}], ['Fix the count.'])
            checked = await self.run_review(defer_repair=True)
            self.assertEqual(checked['answer_review']['status'], 'needs_repair')
            self.assertEqual(checked['answer_review']['issues'], ['Fix the count.'])
            self.assertNotIn('claim_assessments', checked['answer_review'])
            self.model.complete.return_value = self.verdict(True, [{**self.support, 'calculations': envelope}])
            self.assertEqual((await self.run_review(defer_repair=True))['answer_review']['status'], 'unavailable')

    async def test_arithmetic_format_recovery_uses_fixed_hint_without_forwarding_failed_reply(self):
        self.result.update(answer='Volume: 1.2 m3.', sources=[self.source(1, '', data={'liters': 1200})])
        claim = {**self.claim(self.result['answer'], 'support'), 'evidence': [
            {'source_number': 1, 'path': ['liters'], 'value': 1200}],
            'calculations': [{'expression': '1200/1000', 'stated_result': '1.2'}]}
        invalid = {**claim, 'calculations': [{'expression': '1200/1000', 'stated_result': 'UNTRUSTED_VALUE'}]}
        self.model.complete.side_effect = [self.verdict(True, [invalid]), self.verdict(True, [claim])]
        checked = await self.run_review()
        self.assertEqual(checked['answer_review']['status'], 'passed')
        second = self.model.complete.call_args_list[1].args[0]
        self.assertIn('calculations最多3个对象', second[0]['content'])
        self.assertNotIn('UNTRUSTED_VALUE', str(second))
        self.assertEqual(self.packet(0), self.packet(1))

    async def test_arithmetic_does_not_match_grouped_number_fragments_or_inherit_decimal_traps(self):
        self.result['sources'] = [self.source(1, '', data={'quantity': 1200})]
        for separator in (',', '_', ' ', '\u00a0', '\u202f', '\u2009'):
            self.result['answer'] = f'Total: 1{separator}200 units.'
            for value in ('1', '200'):
                claim = {**self.claim(self.result['answer'], 'support'), 'evidence': [
                    {'source_number': 1, 'path': ['quantity'], 'value': 1200}],
                    'calculations': [{'expression': value, 'stated_result': value}]}
                self.model.complete.return_value = self.verdict(True, [claim])
                self.assertEqual((await self.run_review())['answer_review']['status'], 'unavailable')
        self.result.update(answer='Rounded: 1.24.', sources=[self.source(1, '', data={'value': 1.235})])
        claim = {**self.claim(self.result['answer'], 'support'), 'evidence': [
            {'source_number': 1, 'path': ['value'], 'value': 1.235}],
            'calculations': [{'expression': '1.235', 'stated_result': '1.24', 'decimal_places': 2}]}
        with localcontext() as context:
            context.traps[Inexact] = True
            self.model.complete.return_value = self.verdict(True, [claim])
            self.assertEqual((await self.run_review())['answer_review']['status'], 'passed')
            claim.update(status='mismatch', issue='Wrong applicable unit.')
            self.model.complete.return_value = self.verdict(False, [claim], ['Wrong applicable unit.'])
            self.assertEqual((await self.run_review(defer_repair=True))['answer_review']['status'], 'needs_repair')

    async def test_dates_code_and_user_formula_quotes_are_not_automatically_arithmetic_claims(self):
        answer = '2026/10/24\n```python\nratio = 2/1\n```\nUser example: 3/8.'
        self.result.update(answer=answer, sources=[self.source(1, answer)])
        self.model.complete.return_value = self.verdict(True, [self.claim(answer, 'support', [(1, answer)])])
        checked = await self.run_review()
        self.assertEqual(checked['answer_review']['status'], 'passed')
        self.assertEqual(checked['answer'], answer)

    async def test_reviewer_uses_native_learning_state_and_count_meanings(self):
        self.turn['question'] = '今天学练题单还剩几题？'
        self.result.update(answer='已答3题，还剩7题。', sources=[self.source(1, '', data={
            'items': [{'status': 'pending', 'stats': {'total': 10, 'answered': 3}}], 'total': 1})])
        self.model.complete.return_value = self.passed
        await self.run_review()
        meanings = self.packet()['business_meanings']
        self.assertIn('pending表示尚未完成答题', meanings)
        self.assertIn('不是待发布', meanings)
        self.assertIn('题单列表total是题单条数', meanings)
        self.assertIn('不能从答题状态推断', meanings)

    async def test_reviewer_rebuilds_cabinet_field_meaning_from_identity_not_source_text(self):
        forged = {'subject': 'forged', 'state_labels': {'off': 'server shutdown'}}
        self.result.update(answer='Three cabinets have off status.', sources=[self.source(1, '',
            api_id='GET /api/cabinet-power/overview', field_contract=forged,
            data={'counts': {'off': 3}, 'field_contract': forged, 'source_kind': 'untrusted'})])
        original = copy.deepcopy(self.result)
        self.model.complete.return_value = self.passed
        await self.run_review()
        evidence = self.packet()['evidence'][0]
        self.assertEqual(evidence['field_contract']['state_labels']['off'], '未上电/已下电')
        self.assertIn('不证明服务器', evidence['query_guidance'])
        self.assertEqual(evidence['data']['field_contract'], forged)
        self.assertEqual(self.result, original)
        for source in ({**original['sources'][0], 'api_id': 'GET /api/cabinet-power/batches'},
                       {**original['sources'][0], 'api_id': 'GET /api/unknown/status', 'source_kind': 'local_cabinet_inventory'},
                       {**original['sources'][0], 'api_id': None, 'source_kind': 'untrusted'}):
            self.result['sources'] = [source]
            await self.run_review()
            self.assertNotIn('field_contract', self.packet(-1)['evidence'][0])
        self.result['sources'] = [{**original['sources'][0], 'api_id': None,
            'source_kind': 'local_cabinet_inventory'}]
        await self.run_review()
        self.assertEqual(self.packet(-1)['evidence'][0]['field_contract']['state_fields'], ['state'])

    async def test_reviewer_receives_shared_shift_meaning_and_turn_clock_not_answer_clock(self):
        self.turn.update(question='昨天夜班有多少人参与答题？', _reference_time='2026-10-24T23:59:59+08:00')
        self.result['sources'][0]['api_id'] = 'GET /api/learning/completion'
        self.result['answer'] = '2026-10-23夜班，在D楼范围内有两人参与。'
        self.result['query_context'] = {'reference_time': '2099-01-01', 'available_scopes': ['E']}
        self.model.complete.return_value = self.passed
        await self.run_review()
        packet = self.packet()
        self.assertEqual(packet['query_context'], {
            'reference_time': '2026-10-24T23:59:59+08:00', 'available_scopes': ['D']})
        self.assertIn('shift只筛选', packet['evidence'][0]['query_guidance'])
        self.assertIn('不裁剪tasks', packet['evidence'][0]['query_guidance'])
        self.assertIn('按person_id去重', packet['evidence'][0]['query_guidance'])
        self.assertIn('按原问题及明确限定的范围核对', REVIEW)

    async def test_review_does_not_apply_shift_rules_to_submission_participants(self):
        self.turn['question'] = '本周哪些人参与答题？'
        self.result['sources'][0]['data'] = {'kind': 'learning_participation', 'complete': True,
            'participants': [{'person_id': 'review-only', 'answered': 0, 'practice': 1, 'answer_attempts': 1}],
            'summary': {'participants': 1}}
        self.model.complete.return_value = self.passed
        await self.run_review()
        evidence = self.packet()['evidence'][0]
        self.assertIn('answered=0但practice>0', evidence['query_guidance'])
        self.assertIn('不能排除', evidence['query_guidance'])
        self.assertNotIn('已答大于0才算该班次参与', json.dumps(self.packet(), ensure_ascii=False))

    async def test_engine_keeps_reference_clock_when_generation_crosses_midnight(self):
        from openclaw_service.assistant import lighthouse_model

        before = dt.datetime(2026, 10, 24, 23, 59, 59, tzinfo=dt.timezone(dt.timedelta(hours=8)))
        clock = Mock(wraps=dt.datetime)
        clock.now.return_value = before
        engine = lighthouse_model.LighthouseModel(SimpleNamespace(assistant=self.assistant), answer_reviewer=self.reviewer)
        async def candidate(actor, turn, *args):
            self.assertEqual(turn['_reference_time'], before.isoformat())
            clock.now.return_value = before + dt.timedelta(seconds=5)
            return copy.deepcopy(self.result)
        engine._answer_candidate = candidate
        self.model.complete.return_value = self.passed
        self.turn['_reference_time'] = '1999-01-01T00:00:00+08:00'
        with patch.object(lighthouse_model, 'dt', SimpleNamespace(
            datetime=clock, timezone=dt.timezone, timedelta=dt.timedelta)), patch(
            'openclaw_service.assistant.lighthouse_query_intent.resolve_query_intent',
            new=AsyncMock(return_value={'kind': 'business', 'query': self.turn['question']})):
            await engine.answer(self.actor, self.turn, [], None, AsyncMock(), self.authorize, {})
        self.assertEqual(self.packet()['query_context']['reference_time'], before.isoformat())
        clock.now.assert_called_once()

    async def test_negative_verdict_with_bad_quotes_still_requests_repair_without_verifying_claims(self):
        invalid = self.claim(self.bad, 'mismatch', [(1, 'count: 2')], 'The count conflicts with the roster.')
        self.model.complete.return_value = self.verdict(False, [self.support, invalid])
        checked = await self.run_review(defer_repair=True)
        self.assertEqual(checked['answer_review']['status'], 'needs_repair')
        self.assertNotIn('claim_assessments', checked['answer_review'])
        self.assertNotIn('preserved_claim_count', checked['answer_review'])
        self.assertTrue(checked['answer_review']['issues'])
        self.model.complete.assert_called_once()

    async def test_negative_bad_quote_verdict_cannot_fall_back_to_rejected_answer(self):
        invalid = self.claim(self.bad, 'mismatch', [(99, 'invented quote')], 'The count conflicts with the roster.')
        fixed = self.good + '\nThere are two people. [1]'
        for rewritten, expected in ((fixed, 'corrected'), (TimeoutError(), 'needs_review')):
            with self.subTest(expected=expected):
                self.model.complete.reset_mock(side_effect=True)
                self.model.complete.side_effect = [self.verdict(False, [invalid]), rewritten, self.passed]
                checked = await self.run_review()
                self.assertEqual(checked['answer_review']['status'], expected)
                self.assertNotIn(self.bad, checked['answer'])
                self.assertNotIn('invented quote', str(checked))
                if expected == 'corrected':
                    self.assertEqual(checked['answer'], fixed)
                else:
                    self.assertEqual(checked['answer'], UNVERIFIED_REPLY)

    async def test_partial_validation_keeps_only_independent_facts_in_both_channels(self):
        invalid = self.claim(self.bad, 'support', [(1, 'An invented quote.')])
        before = copy.deepcopy(self.result)
        for channel in ('', 'feishu:fixture'):
            with self.subTest(channel=channel):
                self.actor['channel'] = channel
                self.model.complete.reset_mock(side_effect=True)
                self.model.complete.return_value = self.verdict(True, [self.support, invalid])
                checked = await self.run_review()
                self.assertEqual(checked['answer_review']['status'], 'needs_review')
                self.assertEqual(checked['answer_review']['repairs'], 0)
                self.assertEqual(checked['answer_review']['preserved_claim_count'], 1)
                self.assertIn(self.good, checked['answer'])
                self.assertNotIn(self.bad, checked['answer'])
                self.assertNotIn('invented quote', str(checked))
                self.assertEqual(self.model.complete.call_count, 2)
                self.assertTrue(all('preserve_claims' not in json.loads(call.args[0][1]['content'])
                                    for call in self.model.complete.call_args_list))
                self.assertEqual(self.result, before)

    async def test_partial_claims_are_not_forced_into_rewrite_and_complete_review_can_pass(self):
        invalid = self.claim(self.bad, 'mismatch', [(1, 'Invalid quote.')], 'Wrong count.')
        fixed = 'Ava and Bo are the two people on the night roster. [1]'
        self.model.complete.side_effect = [self.verdict(False, [self.support, invalid]), fixed, self.passed]
        checked = await self.run_review()
        self.assertEqual(self.packet(1)['preserve_claims'], [])
        self.assertEqual(self.packet(1)['claim_assessments'], [])
        self.assertEqual(checked['answer'], fixed)
        self.assertEqual(checked['answer_review']['status'], 'corrected')

    async def test_invalid_negative_quote_revokes_old_preserved_sentence(self):
        fixed = self.good + '\nThere are two people. [1]'
        count = self.claim('There are two people. [1]', 'support', [(1, 'Two people')])
        invalid_negative = self.claim(self.good, 'mismatch', [(99, 'Invalid quote.')], 'Roster not established.')
        self.model.complete.side_effect = [self.verdict(False, [self.support, self.mismatch]), fixed,
            self.verdict(False, [invalid_negative, count]), TimeoutError()]
        checked = await self.run_review()
        self.assertEqual(checked['answer_review']['status'], 'needs_review')
        self.assertNotIn(self.good, checked['answer'])
        self.assertEqual(checked['answer'], UNVERIFIED_REPLY)
        self.assertEqual(self.packet(3)['preserve_claims'], [])

    async def test_unknown_negative_and_unanchored_later_claim_cannot_resurrect_old_support(self):
        fixed = self.good + '\nThere are two people. [1]'
        self.model.complete.side_effect = [self.verdict(False, [self.support, self.mismatch]), fixed,
                                          self.verdict(False), TimeoutError()]
        checked = await self.run_review()
        self.assertEqual(checked['answer'], UNVERIFIED_REPLY)
        invalid = self.claim(self.bad, 'support', [(1, 'Invalid quote.')])
        unanchored = {**invalid, 'claim': 'Not in the candidate.'}
        self.model.complete.reset_mock(side_effect=True)
        self.model.complete.side_effect = [self.verdict(True, [self.support, invalid]),
                                          self.verdict(True, [self.support, unanchored])]
        checked = await self.run_review()
        self.assertEqual(checked['answer'], UNVERIFIED_REPLY)
        self.assertNotIn('preserved_claim_count', checked['answer_review'])

    async def test_partial_pool_is_bound_to_the_current_candidate_not_the_old_rewrite(self):
        invalid = self.claim(self.bad, 'mismatch', [(1, 'Invalid quote.')], 'Wrong count.')
        rewritten = 'The count is two. [1]'
        unverified = self.claim(rewritten, 'support', [(1, 'Invalid quote.')])
        self.model.complete.side_effect = [self.verdict(False, [self.support, invalid]), rewritten,
                                          self.verdict(True, [unverified]), self.verdict(True, [unverified])]
        checked = await self.run_review()
        self.assertEqual(checked['answer'], UNVERIFIED_REPLY)
        self.assertNotIn(self.good, checked['answer'])

    async def test_validated_arithmetic_rejection_survives_other_bad_quotes_and_provider_fallback(self):
        good, bad, extra = 'Volume: 1200 L.', 'Converted: 12 m3.', 'Extra advice.'
        self.result.update(answer='\n'.join((good, bad, extra)), sources=[self.source(1, '', data={'liters': 1200})])
        evidence = [{'source_number': 1, 'path': ['liters'], 'value': 1200}]
        support = {**self.claim(good, 'support'), 'evidence': evidence}
        wrong_math = {**self.claim(bad, 'support'), 'evidence': evidence,
                      'calculations': [{'expression': '1200/1000', 'stated_result': '12'}]}
        malformed = self.claim(extra, 'support')
        self.model.complete.side_effect = [self.verdict(True, [support, wrong_math, malformed]), self.passed]
        checked = await self.run_review(defer_repair=True)
        self.assertEqual(checked['answer_review']['status'], 'needs_repair')
        self.assertIn('核算结果为1.2', str(checked['answer_review']['issues']))
        self.assertEqual(self.model.complete.call_count, 2)

    async def test_partial_recovery_reauthorizes_before_return(self):
        invalid = self.claim(self.bad, 'support', [(1, 'Invalid quote.')])
        count = 0
        def complete(*args, **kwargs):
            nonlocal count
            count += 1
            if count == 2:
                self.actor['scopes'] = []
            return self.verdict(True, [self.support, invalid])
        self.model.complete.side_effect = complete
        actor = copy.deepcopy(self.actor)
        with self.assertRaises(AssistantError) as error:
            await self.reviewer.run(actor, self.turn, [], self.result, self.authorize)
        self.assertEqual(error.exception.status, 403)

    async def test_unchanged_rejected_arithmetic_survives_unrelated_or_punctuation_rewrites(self):
        good, bad = 'Volume: 1200 L.', 'Converted: 12 m3.'
        self.result.update(answer=good + '\n\n' + bad + '\n\nExtra advice.',
                           sources=[self.source(1, '', data={'liters': 1200})])
        evidence = [{'source_number': 1, 'path': ['liters'], 'value': 1200}]
        support = {**self.claim(good, 'support'), 'evidence': evidence}
        wrong = {**self.claim(bad, 'support'), 'evidence': evidence,
                 'calculations': [{'expression': '1200/1000', 'stated_result': '12'}]}
        for unchanged in (bad, bad.rstrip('.') + ', still current.'):
            with self.subTest(unchanged=unchanged):
                self.model.complete.reset_mock(side_effect=True)
                self.model.complete.side_effect = [self.verdict(True, [support, wrong]),
                    good + '\n\n' + unchanged + '\n\nRevised advice.', self.passed, TimeoutError()]
                checked = await self.run_review()
                self.assertEqual(checked['answer_review']['status'], 'needs_review')
                self.assertNotIn('Converted: 12 m3', checked['answer'])
                self.assertIn(good, checked['answer'])
                self.assertIn('mismatch', [item['status'] for item in checked['answer_review']['claim_assessments']])
                self.assertIn('核算结果为1.2', str(self.packet(3)['issues']))

    async def test_schema_failure_clears_old_partial_and_preserved_but_transport_failure_does_not(self):
        invalid = self.claim(self.bad, 'support', [(1, 'Invalid quote.')])
        broken = self.verdict(False, [self.claim(self.good, 'mismatch', [(1, 123)], 'Roster unsupported.')])
        self.model.complete.side_effect = [self.verdict(True, [self.support, invalid]), broken]
        self.assertEqual((await self.run_review())['answer'], UNVERIFIED_REPLY)
        self.model.complete.reset_mock(side_effect=True)
        self.model.complete.side_effect = [self.verdict(False, [self.support, self.mismatch]),
            self.good + '\nThere are two people. [1]', broken, TimeoutError()]
        self.assertEqual((await self.run_review())['answer'], UNVERIFIED_REPLY)
        self.model.complete.reset_mock(side_effect=True)
        self.model.complete.side_effect = [self.verdict(True, [self.support, invalid]), TimeoutError()]
        checked = await self.run_review()
        self.assertEqual(checked['answer_review']['status'], 'needs_review')
        self.assertIn(self.good, checked['answer'])
        self.assertNotIn(self.bad, checked['answer'])

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

    async def test_code_and_visible_array_paths_are_not_source_numbers(self):
        self.result['sources'] = [self.source(1, '', data={'items': [{'name': 'Ava'}]})]
        self.model.complete.return_value = self.passed
        for answer in ('The first record is items[0].', 'Field items[0].name is Ava. [1]',
                       'Code: `unread[99]` and ``print(`[99]`)``. [1]',
                       '```python\nunread[99]\n```\nKnown record. [1]',
                       '~~~~python\nunread[99]\n~~~\n[88]\n~~~~\nKnown record. [1]',
                       '    unread[99]\n\nKnown record. [1]'):
            with self.subTest(answer=answer):
                self.result['answer'] = answer
                self.model.complete.reset_mock()
                checked = await self.run_review(defer_repair=True)
                self.assertEqual(checked['answer_review']['status'], 'passed')
                self.assertEqual(checked['answer'], answer)
                self.model.complete.assert_called_once()
        for answer in ('items[0] [99]', 'Chinese中文说明[99]', 'English text[99]', '[1][99]',
                       '`items[0]` [99]', '```python\nitems[99]\n```\n[99]',
                       'Unclosed ` example [99]', 'items[9]', '[000]', '[0001][99]'):
            with self.subTest(answer=answer):
                self.result['answer'] = answer
                self.assertEqual((await self.run_review(defer_repair=True))['answer_review']['status'], 'needs_repair')

    def test_literal_array_paths_do_not_hide_real_citations_or_replace_supported_refs(self):
        sources = [self.source(1, '', data={'items': [{'name': 'Ava'}, {'name': 'Bo'}]})]
        for text, expected in ((r'Escaped \[99\] and [0001]', ['1']),
                               ('items[0] items[1] [1][99]', ['1', '99']),
                               ('items[999] [000]', ['999', '0']),
                               ('items[0] [' + '9' * 5000 + ']', ['9' * 5000]),
                               ('``[99]`` [1] ` unclosed [99]', ['1', '99'])):
            with self.subTest(text=text[:80]):
                self.assertEqual(list(_citation_numbers(text, sources)), expected)
        self.assertEqual(list(_citation_numbers('items[0]', [{**sources[0], 'available': False}])), ['0'])
        for marker in ('available', 'snapshot_exists', 'source_snapshot_ready'):
            with self.subTest(marker=marker):
                unavailable = [{**sources[0], 'data': {**sources[0]['data'], marker: False}}]
                self.assertEqual(list(_citation_numbers('items[0]', unavailable)), ['0'])
        for text, expected in (('Known fact [99](https://fixture.invalid/reference)', ['99']),
                               ('[99]\n\n[99]: https://fixture.invalid/reference', ['99']),
                               ('[**99**](https://fixture.invalid/reference)', ['99']),
                               ('[0001](https://fixture.invalid/[99])', ['1']),
                               ('[docs](https://fixture.invalid/[99])', []),
                               ('[`99`](https://fixture.invalid/reference)', [])):
            with self.subTest(link=text):
                self.assertEqual(list(_citation_numbers(text, sources)), expected)
        claim = {'claim': 'The next record is items[1].', 'evidence': [{'source_number': 1}]}
        self.assertTrue(_supported_reply([claim], sources, claim['claim']).startswith(claim['claim'] + ' [1]'))

    def test_partial_reply_does_not_extract_negations_conditions_or_number_suffixes(self):
        for answer, fragment in (('The operation is not approved.', 'approved.'),
                                 ('Only if authorized, the operation may proceed.', 'the operation may proceed.'),
                                 ('Count: 1200 units.', '200'),
                                 ('Authorization is required.\n\nThe operation may proceed.', 'The operation may proceed.')):
            with self.subTest(answer=answer):
                self.assertEqual(_supported_reply([{'claim': fragment, 'evidence': [{'source_number': 1}]}],
                                                  self.result['sources'], answer), UNVERIFIED_REPLY)

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

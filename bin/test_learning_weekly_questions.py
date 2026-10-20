"""Question-level weekly rankings, using only isolated submitted answers."""
import copy
import datetime as dt
import json
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from bin import test_learning as support
from bin.test_learning import CURRENT, learning
from lan_bitable_template_portal import learning_engagement as engagement
from lan_bitable_template_portal.learning_weekly_cards import WeeklyCards, render


class WeeklyQuestionTests(unittest.TestCase):
    question = support.LearningTests.question

    def setUp(self):
        support.LearningTests.setUp(self)
        guard = patch.dict(sys.modules, {'lan_bitable_template_portal.portal_service': SimpleNamespace(BUILDING_OPEN_ID_MAP={})})
        guard.start(); self.addCleanup(guard.stop)

    def submit(self, qid, correct, *, scope='A', invalid=False, review=False, kind='single', day='2026-09-28'):
        identity = 'sample:' + str(len(self.service._all('paper')))
        q = self.question(qid, kind=kind, stem='Same title', invalid=invalid)
        with self.service.transaction() as conn:
            self.service._put('person', identity, {'id': identity, 'name': identity, 'active': True,
                'scopes': [scope], 'login_ids': []}, conn, False)
            self.service._put('paper', identity, {'id': identity, 'person_id': identity, 'scope': scope,
                'date': day, 'questions': [q]}, conn, False)
            self.service._put('record', identity, {'id': identity, 'entries': {q['id']: {
                'attempt': {'correct': correct, 'submitted_at': day + 'T14:00:00+08:00'},
                'needs_review': review,
                'practice': [{'correct': False, 'submitted_at': day + 'T15:00:00+08:00'}]}}}, conn, False)
        return q['id']

    def ranking(self, scope=None):
        with self.service.transaction() as conn:
            return engagement.weekly_questions(self.service, '2026-09-28', '2026-10-04', conn, scope)

    def test_true_rates_and_real_ids_not_titles_or_families(self):
        first = self.submit('a', False)
        self.submit('a', True)
        second = self.submit('b', False)
        third = self.submit('c', False)
        fourth = self.submit('d', False)
        ranking = self.ranking()
        self.assertEqual([r['question_id'] for r in ranking], [second, third, fourth])
        self.assertEqual([(r['wrong'], r['total']) for r in ranking], [(1, 1)] * 3)
        self.submit('d', True)
        self.assertEqual(self.ranking()[2]['question_id'], first)
        self.assertEqual((self.ranking()[2]['wrong'], self.ranking()[2]['total']), (1, 2))

    def test_invalid_review_interview_ungraded_and_other_weeks_excluded(self):
        self.submit('valid', False)
        for changes in ({'invalid': True}, {'review': True}, {'kind': 'interview'}, {'day': '2026-10-05'}):
            self.submit('excluded', False, **changes)
        self.submit('ungraded', None)
        self.assertEqual([r['question_id'] for r in self.ranking()], ['written:valid'])
        self.assertEqual((self.ranking()[0]['wrong'], self.ranking()[0]['total']), (1, 1))

    def test_scope_filtered_before_ranking_and_no_fabricated_empty_statistics(self):
        self.submit('A', False)
        self.submit('B', False, scope='B')
        self.assertEqual([r['question_id'] for r in self.ranking('A')], ['written:A'])
        self.assertEqual(self.ranking('H'), [])
        text = json.dumps(engagement.weekly_question_elements({'audience': 'group', 'questions': []}), ensure_ascii=False)
        self.assertIn('暂无', text)
        self.assertNotIn('0%', text)

    def test_up_to_three_includes_real_zero_error_samples_and_ties_use_sample_count(self):
        first = self.submit('a', True)
        second = self.submit('b', True)
        self.submit('b', True)
        self.assertEqual([(q['question_id'], q['wrong'], q['total']) for q in self.ranking()],
                         [(second, 0, 2), (first, 0, 1)])

    def test_group_snapshot_and_navigation_keep_personal_and_supervisor_views(self):
        self.submit('A', False)
        self.submit('B', False, scope='B')
        engagement.enqueue_weekly(self.service, CURRENT + dt.timedelta(days=7), set())
        group = self.service._get('engagement', 'week:2026-09-28:summary:group')
        supervisor = self.service._get('engagement', 'week:2026-09-28:summary:A')
        self.assertNotIn('questions', supervisor)
        self.assertEqual([r['question_id'] for r in group['question_buildings']['A']], ['written:A'])
        group['questions'][0]['stem'] = 'GLOBAL-QUESTION'
        group['question_buildings']['A'][0]['stem'] = 'ONLY-A-QUESTION'
        group['question_buildings']['B'][0]['stem'] = 'ONLY-B-QUESTION'
        ticket = {'id': 'fixture', 'token': 'fixture', 'report': group}
        overview = json.dumps(render(ticket, 'http://fixture'), ensure_ascii=False)
        self.assertIn('GLOBAL-QUESTION', overview)
        a_view = json.dumps(render(ticket, 'http://fixture', scope='A'), ensure_ascii=False)
        self.assertIn('ONLY-A-QUESTION', a_view)
        self.assertNotIn('ONLY-B-QUESTION', a_view)
        self.assertNotIn('GLOBAL-QUESTION', a_view)
        self.assertIn('个人画像', a_view)
        portrait = json.dumps(render(ticket, 'http://fixture', scope='A', person_id=group['buildings']['A'][0]['person_id']), ensure_ascii=False)
        self.assertNotIn('ONLY-A-QUESTION', portrait)
        private = copy.deepcopy(ticket)
        private['report']['scope'] = 'A'
        self.assertNotIn('ONLY-A-QUESTION', json.dumps(render(private, 'http://fixture'), ensure_ascii=False))

    def test_old_queued_report_backfills_once_and_truncates_large_question(self):
        self.submit('a', False)
        engagement.enqueue_weekly(self.service, CURRENT + dt.timedelta(days=7), set())
        report = self.service._get('engagement', 'week:2026-09-28:summary:group')
        report.pop('questions'); report.pop('question_buildings')
        cards = WeeklyCards(None, lambda: self.service)
        ticket = cards.prepare(report, 'oc_fixture', 'cli_fixture')
        self.assertEqual(len(ticket['report']['questions']), 1)
        question = ticket['report']['questions'][0]
        question.update(stem='Long' * 5000, options=[{'text': 'Option' * 5000}] * 26)
        elements = engagement.weekly_question_elements(ticket['report'])
        text = json.dumps(elements, ensure_ascii=False)
        self.assertLess(len(text), 1500)
        self.assertIn('首答样本 1 次', text)
        self.assertIn('100.0%', text)


if __name__ == '__main__':
    unittest.main()

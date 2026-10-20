"""Isolated reminder, weekly card and historical mistake regressions."""
import copy
import datetime as dt
import json
import sys
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from types import SimpleNamespace
from unittest.mock import patch

from bin import test_learning_personal as personal_tests
from bin.test_learning import CURRENT, learning
from lan_bitable_template_portal import learning_engagement as engagement

REAL_THREAD_START = threading.Thread.start


class EngagementTests(unittest.TestCase):
    seed = personal_tests.PersonalLearningTests.seed
    question = personal_tests.PersonalLearningTests.question
    dispatch = personal_tests.PersonalLearningTests.dispatch
    assert_status = personal_tests.PersonalLearningTests.assert_status
    prepare = personal_tests.PersonalLearningTests.prepare
    self_actor = personal_tests.PersonalLearningTests.self_actor
    duty = personal_tests.PersonalLearningTests.duty
    claim = personal_tests.PersonalLearningTests.claim
    answer = personal_tests.PersonalLearningTests.answer

    def setUp(self):
        personal_tests.PersonalLearningTests.setUp(self)
        fake_portal = SimpleNamespace(BUILDING_OPEN_ID_MAP={'A': 'ou_dutyA'}, LI_SHILONG_OPEN_ID='ou_summary')
        guard = patch.dict(sys.modules, {'lan_bitable_template_portal.portal_service': fake_portal})
        guard.start(); self.addCleanup(guard.stop)
        self.service._portal_url = lambda: 'http://portal.test:18766'
        self.prepare(count=20)
        with self.service.transaction() as conn:
            self.service._put('settings', 'main', {**learning.DEFAULT_SETTINGS, 'weekly_report_enabled': False}, conn, False)
            for p in self.service._all('person', conn):
                p['login_ids'] = ['ou_' + p['id']]
                self.service._put('person', p['id'], p, conn, False)
        self.personal_hr = {p['id']: {'scopes': p['scopes'], 'selectable': True,
            'position': '值班员', 'login_ids': p['login_ids']} for p in self.service._all('person')}
        self.hr = {'people': copy.deepcopy(self.personal_hr)}
        self.service._login_people_reader = lambda: self.hr

    def start(self, paper):
        return self.dispatch('paper.start', {'id': paper['id'], 'person_id': paper['person_id']}, self.self_actor(paper['person_id']))

    def practice_timer(self):
        return self.dispatch('paper.claim', {'person_id': 'p1', 'mode': 'practice',
            'operation_id': 'timer-fixture'}, self.self_actor())

    def send(self, current):
        if not callable(getattr(self.service, '_weekly_card_sender', None)):
            self.service._weekly_card_sender = lambda job, target, identity: self.service._send(target, engagement.make_card(job, self.service.portal_url()), identity)
        with self.service._notification_lock:
            engagement.send_due(self.service, current)

    def weekly(self, current=CURRENT + dt.timedelta(days=7)):
        with self.service.transaction() as conn:
            self.service._put('settings', 'main', {**learning.DEFAULT_SETTINGS, 'timeout_reminder_enabled': False}, conn, False)
        self.send(current)
        self.send(current)  # Drain the second bounded outbox page.

    def retry(self, paper, q, correct=True):
        return self.dispatch('paper.answer', {'id': paper['id'], 'question_id': q['id'],
            'option_ids': ['o0' if correct else 'o1'], 'operation_id': 'repeat-' + str(len(self.service._get('record', paper['id'])['entries'][q['id']].get('practice', []))),
            'practice': True}, self.self_actor(paper['person_id']))

    def mistakes(self, who='p1', **query):
        return self.dispatch('review', actor=self.self_actor(who), query={'person_id': who, 'kind': 'mistakes', **query})

    def test_view_claim_does_not_start_and_start_is_idempotent(self):
        paper = self.claim()
        self.dispatch('paper.get', {'id': paper['id']}, self.self_actor())
        self.assertFalse(self.service._all('engagement'))
        first = self.start(paper)
        with patch.object(learning, 'now', return_value=CURRENT + dt.timedelta(hours=1)):
            self.assertEqual(self.start(paper)['started_at'], first['started_at'])
        self.assertEqual(self.service._get('engagement', 'timeout:' + paper['id'])['due_at'], (CURRENT + dt.timedelta(hours=2)).timestamp())
        self.assertEqual(first['version'], paper['version'])

    def test_timeout_once_with_partial_submission_restart_and_lost_response(self):
        paper = self.practice_timer(); self.answer(paper)
        self.send(CURRENT + dt.timedelta(hours=1, minutes=59))
        self.assertFalse(self.sender.calls)
        self.sender.fail_once = True
        self.send(CURRENT + dt.timedelta(hours=2))
        self.assertEqual(self.sender.calls[0][0], 'ou_p1')
        self.assertIn('14题未提交', json.dumps(self.sender.calls[0][1], ensure_ascii=False))
        self.send(CURRENT + dt.timedelta(hours=2, minutes=1))
        self.assertEqual(len(self.sender.calls), 1)
        self.service = learning.LearningService(self.root, self.cloud, self.sender, get_portal_url=lambda: 'http://portal.test:18766', get_login_people=lambda: self.hr)
        self.send(CURRENT + dt.timedelta(hours=2, minutes=6))
        self.send(CURRENT + dt.timedelta(days=1))
        self.assertEqual(len(self.sender.calls), 2)
        self.assertEqual(self.sender.calls[0][2], self.sender.calls[1][2])
        self.assertIn('paper_id=practice%3A', json.dumps(self.sender.calls[1][1]))

    def test_finished_or_deleted_paper_never_reminds(self):
        first = self.claim(); self.start(first)
        with self.service.transaction() as conn:
            paper = self.service._get('paper', first['id'], conn)
            paper['questions'] = [q for q in paper['questions'] if q['type'] != 'interview'][:1]
            self.service._put('paper', first['id'], paper, conn)
        self.answer(first)
        second = self.claim('p2'); self.start(second)
        with self.service.transaction() as conn:
            p = self.service._get('paper', second['id'], conn); p['deleted_at'] = learning.stamp()
            self.service._put('paper', p['id'], p, conn)
        self.send(CURRENT + dt.timedelta(hours=3))
        self.assertFalse(self.sender.calls)

    def test_start_requires_owner_and_matching_person(self):
        paper = self.claim()
        self.assert_status(403, self.dispatch, 'paper.start', {'id': paper['id'], 'person_id': 'p1'}, self.self_actor('p2'))
        self.assert_status(403, self.dispatch, 'paper.start', {'id': paper['id'], 'person_id': 'p1'}, self.duty('A'))
        self.assert_status(409, self.dispatch, 'paper.start', {'id': paper['id'], 'person_id': 'p2'}, self.self_actor())
        self.assertFalse(self.service._all('engagement'))

    def test_explicit_self_practice_starts_timer(self):
        paper = self.dispatch('paper.claim', {'person_id': 'p1', 'mode': 'practice', 'operation_id': 'round1'}, self.self_actor())
        self.assertTrue(paper['started_at'])
        self.send(CURRENT + dt.timedelta(hours=2))
        self.assertEqual(len(self.sender.calls), 1)

    def test_cross_day_and_timezone_deadline(self):
        current = CURRENT.replace(hour=23)
        with patch.object(learning, 'now', return_value=current):
            paper = self.practice_timer()
        self.send((current + dt.timedelta(hours=2)).astimezone(dt.timezone.utc))
        self.assertEqual(len(self.sender.calls), 1)

    def test_no_shared_ambiguous_or_departed_recipient(self):
        people = [dict(id='a', active=True, login_ids=['ou_same']), dict(id='b', active=True, login_ids=['ou_same']),
            dict(id='c', active=True, login_ids=['ou_dutyA']), dict(id='d', active=False, login_ids=['ou_d']),
            dict(id='e', active=True, login_ids=['staff:recE', 'ou_e'])]
        self.assertEqual(engagement.recipients(people), {'e': 'ou_e'})

    def test_weekly_personal_and_summary_rates_practice_and_empty_people(self):
        paper = self.claim(); result = self.answer(paper, wrong=True)
        q = next(q for q in result['questions'] if q.get('attempt'))
        self.retry(paper, q)
        second = self.claim('p2'); self.answer(second)
        self.weekly()
        personal = [card for _, card, key in self.sender.calls if ':summary:' not in key]
        self.assertEqual(len(personal), 6)
        summary = self.service._get('engagement', 'week:2026-09-28:summary:group')
        self.assertEqual(summary['participants'], 2)
        a, b = summary['items']
        self.assertEqual((a['answered'], a['practice'], a['accuracy'], a['error_rate']), (1, 1, 0, 100))
        self.assertEqual((b['accuracy'], b['error_rate']), (100, 0))
        empty = next(card for recipient, card, _ in self.sender.calls if recipient == 'ou_p3')
        self.assertIn('正确率 —', json.dumps(empty, ensure_ascii=False))
        self.assertFalse(any(target == 'ou_summary' for target, _, _ in self.sender.calls))
        self.assertEqual(sum(target == engagement.WEEKLY_CHAT_ID for target, _, _ in self.sender.calls), 1)
        before = len(self.sender.calls)
        self.weekly(CURRENT + dt.timedelta(days=9))
        self.assertEqual(len(self.sender.calls), before)

    def test_week_boundary_and_no_old_backfill(self):
        self.weekly(CURRENT.replace(hour=0, minute=0))
        self.assertIsNotNone(self.service._get('engagement', 'week:2026-09-21:summary:group'))
        self.weekly(dt.datetime(2027, 1, 5, 9, tzinfo=learning.TZ))
        report = self.service._get('engagement', 'week:2026-12-28:summary:group')
        self.assertEqual(report['end'], '2027-01-03')
        self.assertEqual(len([j for j in self.service._all('engagement') if j['kind'] == 'weekly_batch']), 2)

    def test_finished_week_sends_at_monday_midnight_beijing(self):
        paper = self.claim()
        self.answer(paper)
        sunday = dt.datetime(2026, 10, 4, 23, 59, 59, tzinfo=learning.TZ)
        self.weekly(sunday)
        self.assertIsNone(self.service._get('engagement', 'week:2026-09-28:summary:group'))
        self.weekly((sunday + dt.timedelta(seconds=1)).astimezone(dt.timezone.utc))
        summary = self.service._get('engagement', 'week:2026-09-28:summary:group')
        self.assertEqual((summary['date'], summary['end'], summary['items'][0]['answered']), ('2026-09-28', '2026-10-04', 1))
        self.assertEqual(summary['status'], 'sent')

    def test_personal_reports_only_duty_roles_but_summary_keeps_every_participant(self):
        self.hr['people']['p1']['position'] = '工程师'
        self.hr['people']['p2']['position'] = '值班长'
        self.hr['people']['p3']['position'] = '经理'
        for pid in ('p1', 'p2', 'p3'):
            self.answer(self.claim(pid))
        self.weekly()
        personal = {target for target, _, key in self.sender.calls if ':summary:' not in key}
        self.assertEqual(personal, {'ou_p2', 'ou_p4', 'ou_p5', 'ou_p6'})
        group = self.service._get('engagement', 'week:2026-09-28:summary:group')
        self.assertEqual({p['person_id'] for p in group['items']}, {'p1', 'p2', 'p3'})

    def test_pending_timeout_and_weekly_recheck_changed_role_before_retry(self):
        paper = self.practice_timer()
        self.sender.fail_once = True
        self.send(CURRENT + dt.timedelta(hours=2))
        self.hr['people']['p1']['position'] = '工程师'
        self.send(CURRENT + dt.timedelta(hours=2, minutes=6))
        self.assertEqual(self.service._get('engagement', 'timeout:' + paper['id'])['status'], 'skipped_role')
        self.assertEqual(len(self.sender.calls), 1)
        self.hr['people']['p1']['position'] = '值班员'
        self.weekly()
        with self.service.transaction() as conn:
            job = self.service._get('engagement', 'week:2026-09-28:p1', conn)
            job.update(status='pending', sent_to=[])
            self.service._put('engagement', job['id'], job, conn)
        self.hr['people']['p1']['position'] = '工程师'
        self.sender.calls.clear()
        self.send(CURRENT + dt.timedelta(days=7, minutes=6))
        self.assertEqual(self.service._get('engagement', job['id'])['status'], 'skipped_role')
        self.assertNotIn('ou_p1', [target for target, _, _ in self.sender.calls])

    def test_weekly_later_practice_counts_in_its_week_and_invalid_excluded(self):
        p = self.claim(); result = self.answer(p, wrong=True)
        q = next(q for q in result['questions'] if q.get('attempt'))
        with patch.object(learning, 'now', return_value=CURRENT + dt.timedelta(days=8)):
            self.retry(p, q)
        with closing(self.service._connect()) as conn:
            result = engagement.weekly_stats(self.service, '2026-10-05', '2026-10-11', conn)['p1']
        self.assertEqual((result['answered'], result['practice'], result['accuracy']), (0, 1, None))
        with self.service.transaction() as conn:
            raw = self.service._get('paper', p['id'], conn)
            next(question for question in raw['questions'] if question['id'] == q['id'])['invalid'] = True
            self.service._put('paper', p['id'], raw, conn)
            self.assertEqual(engagement.weekly_stats(self.service, '2026-09-28', '2026-10-11', conn), {})

    def test_weekly_cloud_restore_does_not_resend(self):
        self.weekly()
        self.service.sync_pending(limit=1000, force=True)
        restored = learning.LearningService(self.root / 'restored', self.cloud, self.sender, get_portal_url=lambda: 'http://portal.test:18766')
        restored.restore()
        count = len(self.sender.calls)
        engagement.send_due(restored, CURRENT + dt.timedelta(days=7, hours=1))
        self.assertEqual(len(self.sender.calls), count)

    def test_mistakes_include_practice_wrong_and_retain_after_correct_redo(self):
        paper = self.claim(); result = self.answer(paper)
        q = next(q for q in result['questions'] if q.get('attempt'))
        self.retry(paper, q, False); self.retry(paper, q, True)
        result = self.mistakes()
        self.assertEqual(result['total'], 1)
        self.assertEqual(result['items'][0]['mistake_count'], 1)
        self.assertTrue(result['items'][0]['question']['attempt']['correct'])
        self.assertEqual(self.mistakes('p2')['total'], 0)
        self.assert_status(403, self.dispatch, 'review', actor=self.self_actor('p2'), query={'kind': 'mistakes', 'person_id': 'p1'})

    def test_mistakes_dedup_pagination_and_regrading(self):
        paper = self.claim(); self.answer(paper, True)
        with self.service.transaction() as conn:
            raw = self.service._get('paper', paper['id'], conn)
            rec = self.service._get('record', paper['id'], conn)
            duplicate = copy.deepcopy(raw); duplicate.update(id='practice:duplicate', mode='practice')
            self.service._put('paper', duplicate['id'], duplicate, conn)
            self.service._put('record', duplicate['id'], {**rec, 'id': duplicate['id']}, conn)
        result = self.mistakes(page_size=1, page=9)
        self.assertEqual((result['total'], result['page'], result['items'][0]['mistake_count']), (1, 1, 2))
        self.assertEqual(self.mistakes(search='unmatched')['total'], 0)
        with self.service.transaction() as conn:
            for p in self.service._documents('paper', person_id='p1', conn=conn):
                for q in p['questions']: q['correct_option_ids'] = ['o1']
                rec = self.service._get('record', p['id'], conn)
                self.service._reconcile_grades(p, rec)
                self.service._put('paper', p['id'], p, conn)
                self.service._put('record', p['id'], rec, conn)
        self.assertEqual(self.mistakes()['total'], 0)

    def test_worker_notifier_runs_despite_cloud_failure(self):
        paper = self.practice_timer()
        with patch.object(self.service, '_tick', side_effect=OSError('offline')):
            with self.assertRaises(OSError): self.service.tick(CURRENT + dt.timedelta(hours=2))
        self.assertEqual(len(self.sender.calls), 1)

    def test_concurrent_start_and_remind_do_not_duplicate(self):
        paper = self.practice_timer()
        with patch('threading.Thread.start', REAL_THREAD_START):
            with ThreadPoolExecutor(max_workers=4) as pool:
                list(pool.map(lambda _: self.start(paper), range(8)))
                list(pool.map(lambda _: self.send(CURRENT + dt.timedelta(hours=2)), range(8)))
        self.assertEqual(len(self.sender.calls), 1)
        self.assertEqual(len(self.service._all('engagement')), 1)

    def test_missing_identity_waits_then_sends_without_changing_job_id(self):
        paper = self.practice_timer()
        with self.service.transaction() as conn:
            person = self.service._get('person', 'p1', conn)
            self.service._put('person', 'p1', {**person, 'login_ids': []}, conn, False)
        self.send(CURRENT + dt.timedelta(hours=2))
        self.assertFalse(self.sender.calls)
        with self.service.transaction() as conn:
            self.service._put('person', 'p1', person, conn, False)
        self.send(CURRENT + dt.timedelta(hours=2, minutes=6))
        self.assertEqual(self.sender.calls[0][2], 'timeout:' + paper['id'])

    def test_completed_during_failed_delivery_cancels_retry(self):
        paper = self.practice_timer()
        self.sender.fail_once = True
        self.send(CURRENT + dt.timedelta(hours=2))
        with self.service.transaction() as conn:
            raw = self.service._get('paper', paper['id'], conn)
            raw['questions'] = raw['questions'][:1]
            self.service._put('paper', raw['id'], raw, conn)
        self.answer(paper)
        self.send(CURRENT + dt.timedelta(hours=2, minutes=6))
        self.assertEqual(len(self.sender.calls), 1)
        self.assertEqual(self.service._get('engagement', 'timeout:' + paper['id'])['status'], 'skipped_completed')

    def test_weekly_large_summary_keeps_every_participant(self):
        with self.service.transaction() as conn:
            rows = {f'x{n}': {'person_id': f'x{n}', 'scopes': ['A'], 'answered': 1, 'choices': 1,
                'correct': 1, 'wrong': 0, 'interviews': 0, 'practice': 0, 'accuracy': 100, 'error_rate': 0} for n in range(65)}
        with patch.object(engagement, 'weekly_stats', return_value=rows):
            self.weekly()
        parts = [job for job in self.service._all('engagement') if job['kind'] == 'weekly_summary' and job.get('audience') == 'group']
        self.assertEqual([len(part['items']) for part in parts], [65])
        self.assertEqual(sum(len(part['items']) for part in parts), 65)
        card = engagement.make_card(parts[0], 'http://portal.test')
        self.assertLess(len(json.dumps(card, ensure_ascii=False).encode()), 18000)
        table = next(element for element in card['elements'] if element['tag'] == 'table')
        self.assertEqual(len(table['rows']), 65)
        self.assertEqual(table['page_size'], 10)
        self.assertEqual(len([call for call in self.sender.calls if call[0] == engagement.WEEKLY_CHAT_ID]), 1)

    def test_summary_equal_person_average_and_compact_native_chart(self):
        job = {'kind': 'weekly_summary', 'date': '2026-10-05', 'end': '2026-10-11', 'items': [
            {'name': '第一人', 'answered': 10, 'choices': 10, 'correct': 9, 'practice': 5, 'accuracy': 1},
            {'name': '第二人', 'answered': 1, 'choices': 1, 'correct': 1, 'practice': 0, 'accuracy': 1},
            {'name': '仅问答', 'answered': 4, 'choices': 0, 'correct': 0, 'practice': 2, 'accuracy': None},
        ]}
        card = engagement.make_card(job, 'http://portal.test')
        metrics = [field['text']['content'] for field in card['elements'][1]['fields']]
        self.assertEqual(metrics, ['**参与人数**\n3 人', '**首次答题总数**\n15 题', '**人均答题数**\n5.0 题', '**平均正确率**\n95%'])
        self.assertIn('选择题答对 10 题', card['elements'][2]['text']['content'])
        self.assertNotIn('错误率', json.dumps(card, ensure_ascii=False))
        chart = next(element for element in card['elements'] if element['tag'] == 'chart')
        values = chart['chart_spec']['data']['values']
        self.assertEqual(len(values), 101)
        self.assertEqual({row['range']: row['count'] for row in values if row['count']}, {'90%': 1, '100%': 1})
        self.assertEqual(chart['chart_spec']['type'], 'line')
        self.assertEqual(chart['chart_spec']['line']['style']['curveType'], 'monotone')
        self.assertEqual(chart['height'], '260px')
        self.assertEqual(chart['chart_spec']['axes'][1]['tick']['tickStep'], 1)
        self.assertTrue(chart['chart_spec']['axes'][1]['label']['autoHide'])
        self.assertEqual((chart['chart_spec']['dataZoom'][0]['start'], chart['chart_spec']['dataZoom'][0]['end']), (0, 1))
        self.assertEqual(card['elements'][-1]['actions'][0]['url'], 'http://portal.test/learning?view=overview&from=2026-10-05&to=2026-10-11')
        table = next(element for element in card['elements'] if element['tag'] == 'table')
        self.assertEqual([column['display_name'] for column in table['columns']], ['姓名', '答题数量', '答对数量', '正确率', '评分'])
        self.assertEqual(table['rows'][0], {'name': '第一人', 'correct': 9, 'answered': 10, 'accuracy': '90%', 'score': '90'})
        self.assertEqual(table['rows'][2]['accuracy'], '—')
        self.assertEqual(table['rows'][2]['score'], '暂无评分')
        self.assertLess(card['elements'].index(chart), card['elements'].index(table))

    def test_summary_auto_scores_and_sorts_before_paginating_without_mutating_source(self):
        items = [
            {'name': '全对', 'answered': 3, 'choices': 3, 'correct': 3, 'score': 0},
            {'name': '仅问答', 'answered': 8, 'choices': 0, 'correct': 0, 'score': 100},
            {'name': '全错', 'answered': 5, 'choices': 5, 'correct': 0},
            {'name': '部分正确', 'answered': 3, 'choices': 3, 'correct': 2},
        ]
        card = engagement.summary_card({'date': '2026-10-05', 'end': '2026-10-11', 'items': items}, 'http://portal.test')
        table = next(e for e in card['elements'] if e['tag'] == 'table')
        self.assertEqual([r['name'] for r in table['rows']], ['全错', '部分正确', '全对', '仅问答'])
        self.assertEqual([r['score'] for r in table['rows']], ['0', '66.7', '100', '暂无评分'])
        self.assertEqual(items[0]['score'], 0)

    def test_summary_empty_and_ungraded_never_shows_fake_zero_accuracy(self):
        for items in ([], [{'answered': 3, 'practice': 1, 'choices': 0, 'correct': 0}]):
            card = engagement.make_card({'kind': 'weekly_summary', 'date': '2026-10-05', 'end': '2026-10-11', 'items': items}, 'http://portal.test')
            self.assertEqual(card['elements'][1]['fields'][3]['text']['content'], '**平均正确率**\n—')
            self.assertNotIn('错误率', json.dumps(card, ensure_ascii=False))
            self.assertEqual(sum(e['tag'] == 'chart' for e in card['elements']), 0)

    def test_summary_distribution_uses_unrounded_rates_and_boundary_counts(self):
        scores = [(0, 1), (29, 100), (59, 100), (3, 5), (799, 1000), (4, 5), (9999, 10000), (1, 1)]
        items = [{'answered': total, 'practice': 0, 'choices': total, 'correct': correct} for correct, total in scores]
        card = engagement.make_card({'kind': 'weekly_summary', 'date': '2026-10-05', 'end': '2026-10-11', 'items': items}, 'http://portal.test')
        chart = next(e for e in card['elements'] if e['tag'] == 'chart')
        values = chart['chart_spec']['data']['values']
        self.assertEqual([r['range'] for r in values], [f'{percent}%' for percent in range(101)])
        self.assertEqual({r['range']: r['count'] for r in values if r['count']},
            {'0%': 1, '29%': 1, '59%': 1, '60%': 1, '79%': 1, '80%': 1, '99%': 1, '100%': 1})
        self.assertEqual(sum(r['count'] for r in values), len(items))

    def test_weekly_paper_count_deduplicates_first_answers_and_practice(self):
        paper = self.claim(); result = self.answer(paper)
        q = next(q for q in result['questions'] if q.get('attempt'))
        self.retry(paper, q); self.retry(paper, q)
        with closing(self.service._connect()) as conn:
            row = engagement.weekly_stats(self.service, '2026-09-28', '2026-10-04', conn)['p1']
        self.assertEqual((row['papers'], row['answered'], row['practice']), (1, 1, 2))
        second = self.dispatch('paper.claim', {'person_id': 'p1', 'mode': 'practice', 'operation_id': 'second-round'}, self.self_actor())
        self.answer(second)
        with closing(self.service._connect()) as conn:
            row = engagement.weekly_stats(self.service, '2026-09-28', '2026-10-04', conn)['p1']
        self.assertEqual((row['papers'], row['answered'], row['practice']), (2, 2, 2))
        with patch.object(learning, 'now', return_value=CURRENT + dt.timedelta(days=8)):
            self.retry(paper, q)
        with closing(self.service._connect()) as conn:
            row = engagement.weekly_stats(self.service, '2026-10-05', '2026-10-11', conn)['p1']
        self.assertEqual((row['papers'], row['answered'], row['practice']), (1, 0, 1))

    def test_legacy_chunk_summary_does_not_misrepresent_global_metrics(self):
        job = {'kind': 'weekly_summary', 'date': '2026-10-05', 'end': '2026-10-11', 'part': 2, 'parts': 3,
            'participants': 65, 'items': [{'answered': 5, 'choices': 5, 'correct': 4, 'practice': 0}]}
        card = engagement.make_card(job, 'http://portal.test')
        self.assertIn('以上仅统计本组 1 人', json.dumps(card, ensure_ascii=False))

    def test_migration_backfills_historical_mistakes_and_backup(self):
        paper = self.claim(); self.answer(paper, True)
        with self.service.transaction() as conn:
            conn.execute('UPDATE learning_results SET wrong_count=0,last_wrong_at=\'\'')
            conn.execute('PRAGMA user_version=3')
        self.service = learning.LearningService(self.root, self.cloud, self.sender)
        self.assertEqual(self.mistakes()['total'], 1)
        self.assertTrue((self.root / 'backups' / 'learning-before-weekly.sqlite3').is_file())

    def test_disabled_flags_cancel_pending_messages(self):
        paper = self.claim(); self.start(paper)
        with self.service.transaction() as conn:
            self.service._put('settings', 'main', {**learning.DEFAULT_SETTINGS, 'timeout_reminder_enabled': False, 'weekly_report_enabled': False}, conn, False)
        self.send(CURRENT + dt.timedelta(hours=3))
        self.assertFalse(self.sender.calls)
        self.assertEqual(self.service._get('engagement', 'timeout:' + paper['id'])['status'], 'cancelled')

    def test_restore_repairs_missing_timer_and_partial_weekly_batch(self):
        paper = self.claim(); self.start(paper)
        self.service.sync_pending(limit=1000, force=True)
        self.cloud.entities.pop(('engagement', 'timeout:' + paper['id']))
        restored = learning.LearningService(self.root / 'timer-restore', self.cloud, self.sender)
        restored.restore()
        self.assertEqual(restored._get('engagement', 'timeout:' + paper['id'])['due_at'], (CURRENT + dt.timedelta(hours=2)).timestamp())
        self.weekly()
        count = len(self.sender.calls)
        with self.service.transaction() as conn:
            conn.execute("DELETE FROM documents WHERE kind='engagement' AND key='week:2026-09-28'")
        self.weekly()
        self.assertEqual(len(self.sender.calls), count, 'partial cloud recovery must preserve already sent personal cards')

    def test_card_accepts_legacy_attempt_without_needs_review_flag(self):
        paper = {'id': 'legacy-personal', 'date': '2026-10-13', 'questions': [{'id': 'q1'}, {'id': 'q2'}]}
        record = {'entries': {'q1': {'attempt': {'correct': True}}}}
        card = engagement.make_card({'kind': 'timeout'}, 'http://portal.test', paper, record)
        self.assertIn('还有1题未提交', json.dumps(card, ensure_ascii=False))

    def manager_directory(self, rows):
        self.hr = {'people': {**copy.deepcopy(self.personal_hr), **{rid: {'scopes': ['A'], 'selectable': True, 'inactive': False,
            'position': '主管', 'login_ids': ['ou_' + rid], **values} for rid, values in rows.items()}}}
        self.service._login_people_reader = lambda: self.hr

    def test_supervisors_use_local_roles_unique_identity_and_own_scope(self):
        self.manager_directory({'a': {}, 'b': {'scopes': ['B'], 'position': '', 'assignments': ['主管']},
            'h': {'scopes': ['H'], 'position': '经理'}, 'h_supervisor': {'scopes': ['H']}, 'engineer': {'position': '工程师'},
            'other_manager': {'scopes': ['E'], 'position': '经理'}, 'left': {'inactive': True},
            'disabled': {'selectable': False}, 'ambiguous': {'scopes': ['A', 'B']},
            'station': {'scopes': ['110']}, 'shared': {'login_ids': ['ou_dutyA']},
            'duplicate1': {'login_ids': ['ou_duplicate']}, 'duplicate2': {'login_ids': ['ou_duplicate']}})
        self.assertEqual(engagement.supervisors(self.service), {'A': ['ou_a'], 'B': ['ou_b']})
        self.hr = {}
        self.assertEqual(engagement.supervisors(self.service), {})

    def test_building_report_filters_first_and_practice_before_person_aggregation(self):
        paper = self.claim(); result = self.answer(paper, wrong=True)
        q = next(q for q in result['questions'] if q.get('attempt'))
        self.retry(paper, q)
        with self.service.transaction() as conn:
            raw = self.service._get('paper', paper['id'], conn)
            record = self.service._get('record', paper['id'], conn)
            other = {**raw, 'id': 'moved-person:B', 'scope': 'B', 'mode': 'practice'}
            self.service._put('paper', other['id'], other, conn)
            self.service._put('record', other['id'], {**record, 'id': other['id']}, conn)
        self.manager_directory({'supervisorA': {}, 'supervisorB': {'scopes': ['B']}})
        self.weekly()
        a = self.service._get('engagement', 'week:2026-09-28:summary:A')
        b = self.service._get('engagement', 'week:2026-09-28:summary:B')
        full = self.service._get('engagement', 'week:2026-09-28:summary:group')
        for job, scope in ((a, 'A'), (b, 'B')):
            item = job['items'][0]
            self.assertEqual((item['answered'], item['practice'], item['scopes']), (1, 1, [scope]))
            card = engagement.make_card(job, 'http://portal.test')
            self.assertEqual(card['header']['title']['content'], scope + '楼 · 每周学练汇总')
            self.assertIn('&scope=' + scope, card['elements'][-1]['actions'][0]['url'])
        self.assertEqual((full['items'][0]['answered'], full['items'][0]['practice']), (2, 2))
        calls = {recipient: card for recipient, card, _ in self.sender.calls}
        self.assertEqual(calls[engagement.WEEKLY_CHAT_ID], engagement.make_card(full, 'http://portal.test:18766'))
        self.assertNotIn('ou_summary', calls)
        self.assertEqual(calls['ou_supervisorA']['header']['title']['content'], 'A楼 · 每周学练汇总')

    def test_supervisor_retry_keeps_receipts_and_rechecks_role_after_restart(self):
        self.manager_directory({'first': {}, 'second': {}, 'third': {}})
        actual_send = self.service._send
        def fail_two(target, card, key):
            if target in {'ou_second', 'ou_third'}:
                self.sender.calls.append((target, card, key))
                return False
            return actual_send(target, card, key)
        self.service._send = fail_two
        self.weekly()
        key = 'week:2026-09-28:summary:A'
        self.assertEqual(self.service._get('engagement', key)['sent_to'], ['ou_first'])
        self.hr['people']['third']['position'] = '工程师'
        self.service = learning.LearningService(self.root, self.cloud, self.sender,
            get_portal_url=lambda: 'http://portal.test:18766', get_login_people=lambda: self.hr)
        self.send(CURRENT + dt.timedelta(days=7, minutes=6))
        self.assertEqual(self.service._get('engagement', key)['status'], 'sent')
        self.assertEqual(sum(target == 'ou_first' for target, _, _ in self.sender.calls), 1)
        self.assertEqual(sum(target == 'ou_third' for target, _, _ in self.sender.calls), 1)
        second_keys = [identity for target, _, identity in self.sender.calls if target == 'ou_second']
        self.assertEqual(len(second_keys), 2)
        self.assertEqual(second_keys[0], second_keys[1])

    def test_unavailable_directory_does_not_block_group_and_recovers_locally(self):
        self.service._login_people_reader = lambda: (_ for _ in ()).throw(OSError('local busy'))
        self.weekly()
        self.assertIn(engagement.WEEKLY_CHAT_ID, [target for target, _, _ in self.sender.calls])
        self.assertFalse(any(':summary:' not in key for _, _, key in self.sender.calls))
        self.assertEqual(self.service._get('engagement', 'week:2026-09-28:summary:A')['status'], 'pending')
        self.manager_directory({'found': {}})
        self.send(CURRENT + dt.timedelta(days=7, minutes=6))
        self.assertEqual(sum(target == 'ou_found' for target, _, _ in self.sender.calls), 1)
        self.assertEqual(sum(target == engagement.WEEKLY_CHAT_ID for target, _, _ in self.sender.calls), 1)
        self.assertIn('ou_p1', [target for target, _, _ in self.sender.calls])

    def test_personal_pie_uses_first_choices_only_and_handles_zero_scores(self):
        for correct, wrong in ((7, 3), (10, 0), (0, 10)):
            job = {'kind': 'weekly', 'name': '本人', 'date': '2026-10-05', 'end': '2026-10-11',
                'stats': {'answered': 12, 'choices': 10, 'correct': correct, 'wrong': wrong,
                    'interviews': 2, 'practice': 99, 'days': 4, 'accuracy': correct * 10, 'error_rate': wrong * 10}}
            card = engagement.make_card(job, 'http://portal.test')
            charts = [e for e in card['elements'] if e['tag'] == 'chart']
            self.assertEqual(len(charts), 1)
            self.assertEqual(charts[0]['chart_spec']['type'], 'pie')
            self.assertEqual(charts[0]['chart_spec']['data']['values'],
                [{'type': '正确', 'count': correct}, {'type': '错误', 'count': wrong}])
            self.assertIn('学习 4 天', json.dumps(card, ensure_ascii=False))
            self.assertIn('tab=review', card['elements'][-1]['actions'][0]['url'])
        for stats in ({}, {'answered': 2, 'interviews': 2, 'choices': 0}):
            job['stats'] = stats
            card = engagement.make_card(job, 'http://portal.test')
            self.assertFalse(any(e['tag'] == 'chart' for e in card['elements']))
            self.assertIn('正确率 —', json.dumps(card, ensure_ascii=False))

    def test_group_failure_retries_only_group_with_same_identity(self):
        actual_send = self.service._send
        def fail_group(target, card, key):
            if target == engagement.WEEKLY_CHAT_ID:
                self.sender.calls.append((target, card, key))
                raise OSError('response lost')
            return actual_send(target, card, key)
        self.service._send = fail_group
        self.weekly()
        count = len(self.sender.calls)
        key = 'week:2026-09-28:summary:group'
        self.assertEqual(self.service._get('engagement', key)['status'], 'pending')
        self.service = learning.LearningService(self.root, self.cloud, self.sender, get_portal_url=lambda: 'http://portal.test:18766')
        self.send(CURRENT + dt.timedelta(days=7, minutes=6))
        self.assertEqual(len(self.sender.calls), count + 1)
        self.assertEqual(self.sender.calls[-1][0], engagement.WEEKLY_CHAT_ID)
        self.assertEqual([k for target, _, k in self.sender.calls if target == engagement.WEEKLY_CHAT_ID], [key, key])
        self.assertEqual(self.service._get('engagement', key)['status'], 'sent')

    def test_card_error_does_not_stop_other_recipients(self):
        self.weekly()
        with self.service.transaction() as conn:
            broken = self.service._get('engagement', 'week:2026-09-28:p1', conn)
            broken.update(status='pending', sent_to=[], stats={'choices': 1})
            self.service._put('engagement', broken['id'], broken, conn)
            group = self.service._get('engagement', 'week:2026-09-28:summary:group', conn)
            group.update(status='pending', sent_to=[])
            self.service._put('engagement', group['id'], group, conn)
        self.send(CURRENT + dt.timedelta(days=7, minutes=6))
        self.assertEqual(self.service._get('engagement', broken['id'])['status'], 'pending')
        self.assertEqual(self.service._get('engagement', group['id'])['status'], 'sent')

    def test_summary_audiences_exclude_h_and_li_but_group_keeps_h_data(self):
        self.manager_directory({**{scope: {'scopes': [scope]} for scope in 'ABCDE'},
            'H': {'scopes': ['H'], 'position': '经理'}})
        paper = self.claim(); self.answer(paper)
        with self.service.transaction() as conn:
            raw = self.service._get('paper', paper['id'], conn)
            self.service._put('paper', paper['id'], {**raw, 'scope': 'H'}, conn)
            person = self.service._get('person', 'p1', conn)
            self.service._put('person', 'p1', {**person, 'scopes': ['H']}, conn)
        self.weekly()
        reports = [job for job in self.service._all('engagement') if job['kind'] == 'weekly_summary']
        self.assertEqual({job['audience'] for job in reports}, {'A', 'B', 'C', 'D', 'E', 'group'})
        sent = {target for target, _, key in self.sender.calls if ':summary:' in key}
        self.assertEqual(sent, {'ou_A', 'ou_B', 'ou_C', 'ou_D', 'ou_E', engagement.WEEKLY_CHAT_ID})
        group = next(job for job in reports if job['audience'] == 'group')
        self.assertEqual((group['items'][0]['scopes'], group['items'][0]['answered']), (['H'], 1))
        self.assertTrue(all(not job['items'] for job in reports if job['audience'] != 'group'))
        self.assertTrue(any(target == 'ou_p1' and ':summary:' not in key for target, _, key in self.sender.calls))

    def test_weekly_automatic_delivery_uses_interactive_cards_only_for_the_configured_audiences(self):
        self.manager_directory({**{scope: {'scopes': [scope]} for scope in 'ABCDE'},
                                'H': {'scopes': ['H'], 'position': '主管'}})
        self.answer(self.claim())
        deliveries = []
        def interactive(job, target, identity):
            deliveries.append((copy.deepcopy(job), target, identity))
            return True
        self.service._weekly_card_sender = interactive
        self.weekly()
        self.assertEqual({target for _, target, _ in deliveries},
                         {engagement.WEEKLY_CHAT_ID, 'ou_A', 'ou_B', 'ou_C', 'ou_D', 'ou_E'})
        self.assertEqual(len(deliveries), 6)
        for job, target, _ in deliveries:
            self.assertEqual(job['kind'], 'weekly_summary')
            if target == engagement.WEEKLY_CHAT_ID:
                self.assertEqual(set(job['buildings']), set('ABCDEH'))
            else:
                self.assertEqual(set(job['buildings']), {job['scope']})
                self.assertEqual(target, 'ou_' + job['scope'])
        self.assertFalse(any(':summary:' in identity for _, _, identity in self.sender.calls))
        self.send(CURRENT + dt.timedelta(days=7, minutes=6))
        self.assertEqual(len(deliveries), 6, 'already delivered weekly cards must not be resent')

    def test_missing_interactive_sender_keeps_report_pending_instead_of_using_old_app(self):
        self.weekly()
        key = 'week:2026-09-28:summary:group'
        with self.service.transaction() as conn:
            job = self.service._get('engagement', key, conn)
            job.update(status='pending', sent_to=[], due_at=0)
            self.service._put('engagement', key, job, conn)
        self.service._weekly_card_sender = None
        before = len(self.sender.calls)
        engagement.send_due(self.service, CURRENT + dt.timedelta(days=7, minutes=6))
        job = self.service._get('engagement', key)
        self.assertEqual(job['status'], 'pending')
        self.assertIn('智能体周报发送器尚未就绪', job['error'])
        self.assertEqual(len(self.sender.calls), before)

    def test_interactive_sender_error_is_preserved_and_recovery_reuses_same_task(self):
        calls = []
        def failed(job, target, identity):
            calls.append(identity)
            raise learning.LearningError('飞书智能体拒绝周报（HTTP 400，code=230002）。请将智能体加入指定群。', 503)
        self.service._weekly_card_sender = failed
        self.weekly()
        key = 'week:2026-09-28:summary:group'
        job = self.service._get('engagement', key)
        self.assertEqual(job['status'], 'pending')
        self.assertIn('230002', job['error'])
        self.assertIn('230002', self.service._last_error)
        self.service._weekly_card_sender = lambda job, target, identity: calls.append(identity) or True
        self.send(CURRENT + dt.timedelta(days=7, minutes=6))
        self.assertEqual(self.service._get('engagement', key)['status'], 'sent')
        self.assertEqual(calls, [key, key])

    def test_retired_pending_summary_recipients_cancel_after_restart(self):
        self.weekly()
        with self.service.transaction() as conn:
            group = self.service._get('engagement', 'week:2026-09-28:summary:group', conn)
            for audience, scope in (('H', 'H'), ('1', ''), ('legacy', '')):
                key = 'week:2026-09-28:summary:' + audience
                job = {**group, 'id': key, 'audience': audience, 'scope': scope, 'status': 'pending', 'sent_to': []}
                if audience == 'legacy':
                    del job['audience']
                self.service._put('engagement', key, job, conn)
        before = len(self.sender.calls)
        self.service = learning.LearningService(self.root, self.cloud, self.sender, get_portal_url=lambda: 'http://portal.test:18766')
        self.send(CURRENT + dt.timedelta(days=7, minutes=6))
        self.assertEqual(len(self.sender.calls), before)
        for audience in ('H', '1', 'legacy'):
            self.assertEqual(self.service._get('engagement', 'week:2026-09-28:summary:' + audience)['status'], 'cancelled')


if __name__ == '__main__':
    unittest.main()

"""Synthetic SQLite only: daily scope, delivery evidence, and worker race regressions."""
import copy
import datetime as dt
import json
import sys
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

from bin import test_learning as support
from lan_bitable_template_portal import learning as core, learning_completion as completion
from lan_bitable_template_portal import learning_daily_report as daily, learning_engagement as engagement
from lan_bitable_template_portal.learning_reminders import ROSTER_VERSION
from lan_bitable_template_portal.pre_shift_checks import windows

DATE = '2026-10-26'
NOW = dt.datetime(2026, 10, 27, 9, tzinfo=core.TZ)
REAL_THREAD_START = threading.Thread.start
REAL_SEND = daily._send


class DailyReportTests(unittest.TestCase):
    question = support.LearningTests.question

    def setUp(self):
        support.LearningTests.setUp(self)
        self.clock = NOW
        self.directory = {}
        for target, kwargs in (
            ('lan_bitable_template_portal.learning.now', {'side_effect': lambda: self.clock}),
            ('lan_bitable_template_portal.learning_shift_storage.current_directory',
             {'side_effect': lambda service: (self.directory, True, 'cli_current')}),
            ('lan_bitable_template_portal.learning_daily_report._send', {'side_effect': self.accept}),
        ):
            guard = patch(target, **kwargs)
            value = guard.start(); self.addCleanup(guard.stop)
            if target.endswith('._send'):
                self.send_mock = value
        guard = patch.dict(sys.modules, {'lan_bitable_template_portal.portal_service': SimpleNamespace(
            BUILDING_OPEN_ID_MAP={'A': 'ou_shared'}, external_real_write_guard=lambda: {'real_write_allowed': True})})
        guard.start(); self.addCleanup(guard.stop)
        self.service._portal_url = lambda: 'https://portal.invalid'
        self.service._restored = True
        self.roster = []
        self.put('settings', {**core.DEFAULT_SETTINGS, 'id': 'main', 'enabled': True,
                             'weekly_report_enabled': False, 'timeout_reminder_enabled': False})
        self.put('publication', {'id': self.service._publication_key(DATE), 'date': DATE,
            'created_at': DATE + 'T13:00:00+08:00', 'reserve_ids': ['reserve']})
        self.put('reserve', {'id': 'reserve', 'date': DATE, 'scope': 'A',
                            'questions': [self.question(n) for n in range(15)]})
        self.member()

    def put(self, kind, value, dirty=False):
        with self.service.transaction() as conn:
            self.service._put(kind, value['id'], value, conn, dirty)
        return value

    def delete(self, kind, identity):
        with self.service.transaction() as conn:
            conn.execute('DELETE FROM documents WHERE kind=? AND key=?', (kind, identity))

    def save_roster(self):
        self.put('local', {'id': 'shift_roster:' + DATE, 'date': DATE, 'version': ROSTER_VERSION,
                           'people': self.roster, 'issues': []})

    def member(self, pid='p1', shift='白', *, scope='A', paper=True):
        person = self.put('person', {'id': pid, 'name': '示例' + pid, 'active': True,
                                    'scopes': [scope], 'login_ids': ['ou_' + pid]})
        self.directory[pid] = {'name': person['name'], 'scopes': [scope], 'selectable': True,
                               'position': '值班员', 'login_ids': person['login_ids']}
        self.roster.append({'person_id': pid, 'name': person['name'], 'scope': scope,
                            'shift': shift, 'open_id': 'ou_' + pid})
        self.save_roster()
        if paper:
            self.put('paper', {'id': 'paper:' + pid, 'person_id': pid, 'person': person,
                'date': DATE, 'scope': scope, 'shift': shift, 'mode': 'daily',
                'created_at': DATE + ('T20:00:00+08:00' if shift == '夜' else 'T13:00:00+08:00'),
                'questions': [self.question(n) for n in range(2)]})
        self.notice(pid, shift)
        return person

    def notice(self, pid='p1', shift='白', status='sent', **changes):
        return self.put('notification', {'id': f'shift:{DATE}:{shift}:ou_{pid}', 'kind': 'shift',
            'date': DATE, 'shift': shift, 'open_id': 'ou_' + pid, 'status': status,
            'delivery': [{'open_id': 'ou_' + pid, 'ok': True, 'message_id': 'om_notice',
                          'sender_app_id': 'cli_current'}], **changes})

    def answer(self, pid='p1', count=2, correct=0):
        paper = self.service._get('paper', 'paper:' + pid)
        return self.put('record', {'id': paper['id'], 'person_id': pid, 'date': DATE, 'scope': paper['scope'],
            'entries': {q['id']: {'attempt': {'correct': index < correct, 'submitted_at': self.clock.isoformat()}}
                        for index, q in enumerate(paper['questions'][:count])}})

    def report(self):
        with self.service.transaction() as conn:
            return daily.collect(self.service, DATE, self.clock, conn, self.directory, 'cli_current')

    def accept(self, card, identity):
        return True, '', [{'chat_id': engagement.WEEKLY_CHAT_ID, 'ok': True,
            'message_uuid': identity, 'message_id': 'om_report', 'sender_app_id': 'cli_current'}]

    def reject(self, card, identity):
        return False, 'offline rejection', [{'chat_id': engagement.WEEKLY_CHAT_ID, 'ok': False,
            'message_uuid': identity, 'failure_kind': 'not_sent'}]

    def job(self):
        return self.service._get('engagement', 'daily-report:' + DATE)

    def run_due(self, current=NOW):
        self.clock = current
        daily.send_due(self.service, current)

    def restart(self):
        self.service = core.LearningService(self.root, self.cloud, self.sender,
                                           get_portal_url=lambda: 'https://portal.invalid')
        self.service._restored = True

    def test_partial_and_cross_midnight_completed_use_daily_not_score(self):
        self.member('night', '夜'); self.answer('night')
        self.answer(count=1)
        report = self.report()
        self.assertEqual([(r['person_id'], r['answered'], r['assigned']) for r in report['unfinished']], [('p1', 1, 2)])
        self.assertFalse(report['unverified'])

    def test_no_publication_or_dirty_publication_never_accuses(self):
        original = self.service._get('publication', self.service._publication_key(DATE))
        for value in (None, original):
            with self.subTest(value=value is not None):
                if value is None:
                    self.delete('publication', original['id'])
                else:
                    self.put('publication', value, dirty=True)
                self.assertFalse(self.report()['unfinished'])
                self.assertTrue(self.report()['unverified'])

    def test_failed_unknown_old_app_and_missing_delivery_are_separate(self):
        cases = [('pending', [{'ok': False, 'failure_kind': 'not_sent', 'open_id': 'ou_p1'}], 'failed'),
                 ('pending', [{'ok': False, 'failure_kind': 'uncertain', 'open_id': 'ou_p1'}], 'unverified'),
                 ('sent', [{'ok': True, 'message_id': 'om_old', 'sender_app_id': 'cli_old', 'open_id': 'ou_p1'}], 'unverified'),
                 ('sent', [], 'unverified'), ('cancelled', [], 'failed')]
        for status, delivery, key in cases:
            with self.subTest(status=status, delivery=delivery):
                self.notice(status=status, delivery=delivery)
                report = self.report()
                self.assertFalse(report['unfinished'])
                self.assertEqual(len(report[key]), 1)
                card = json.dumps(daily.render_card(report, 'https://portal.invalid'), ensure_ascii=False)
                self.assertNotIn('<font color=', card)

    def test_duplicate_roster_counts_once_duplicate_identity_counts_unknown(self):
        self.roster.append(copy.deepcopy(self.roster[0])); self.save_roster()
        self.assertEqual(len(self.report()['unfinished']), 1)
        person = self.service._get('person', 'p1')
        self.put('person', {**person, 'id': 'duplicate'})
        self.assertFalse(self.report()['unfinished'])
        self.assertEqual(len(self.report()['unverified']), 1)

    def test_current_app_alias_preserves_canonical_person_and_completion(self):
        person = self.service._get('person', 'p1')
        self.put('person', {**person, 'aliases': ['directory:p1']})
        self.directory['p1']['login_ids'] = ['ou_current']
        self.roster[0]['open_id'] = 'ou_current'; self.save_roster()
        self.notice(open_id='ou_current', delivery=[{'open_id': 'ou_current', 'ok': True,
                    'message_id': 'om_current', 'sender_app_id': 'cli_current'}])
        self.assertEqual([r['person_id'] for r in self.report()['unfinished']], ['p1'])
        self.answer(); self.assertFalse(self.report()['unfinished'])
        self.assertEqual(self.service._get('person', 'p1')['login_ids'], ['ou_p1'])
        self.put('person', {**person, 'id': 'duplicate', 'aliases': ['directory:p1']})
        self.assertTrue(self.report()['unverified'])

    def test_cross_shift_explicit_paper_not_reused_for_other_shift(self):
        self.roster.append({**self.roster[0], 'shift': '夜'}); self.save_roster()
        self.notice(shift='夜')
        self.assertEqual([r['shift'] for r in self.report()['unfinished']], ['白'])
        self.assertEqual([r['shift'] for r in self.report()['unverified']], ['夜'])
        self.answer()
        self.assertFalse(self.report()['unfinished'])
        self.assertTrue(self.report()['unverified'])

    def test_legacy_paper_without_shift_is_ambiguous_across_shifts(self):
        self.roster.append({**self.roster[0], 'shift': '夜'}); self.save_roster()
        paper = self.service._get('paper', 'paper:p1'); paper.pop('shift')
        self.put('paper', paper)
        self.assertFalse(self.report()['unfinished'])
        self.assertEqual(len(self.report()['unverified']), 2)

    def test_long_day_practice_sharing_unassigned_and_deleted_are_excluded(self):
        self.answer()
        self.member('long'); self.roster[-1]['duty_kind'] = '长白'
        self.member('practice', paper=False)
        self.roster.pop(); self.save_roster()
        self.put('paper', {'id': 'practice', 'mode': 'practice', 'person_id': 'practice', 'date': DATE,
                          'scope': 'A', 'questions': [self.question(2)]})
        self.put('sharing_attempt', {'id': 'sharing', 'person_id': 'p1', 'date': DATE})
        self.put('sharing_batch', {'id': 'sharing', 'date': DATE, 'status': 'published'})
        self.member('deleted')
        self.put('paper', {**self.service._get('paper', 'paper:deleted'), 'deleted_at': DATE})
        report = self.report()
        self.assertFalse(any(report[key] for key in ('unfinished', 'failed', 'unverified')))

    def test_missing_yesterday_roster_does_not_read_today_or_return_zero(self):
        self.delete('local', 'shift_roster:' + DATE)
        self.put('local', {'id': 'shift_roster', 'date': '2026-10-27', 'version': ROSTER_VERSION, 'people': []})
        self.service._shift_roster_reader = Mock(side_effect=AssertionError('never read live roster'))
        report = self.report()
        self.assertTrue(report['unverified']); self.assertFalse(report['unfinished'])
        self.service._shift_roster_reader.assert_not_called()

    def test_empty_roster_is_zero_but_roster_issue_is_not(self):
        self.roster = []; self.save_roster()
        self.run_due(); self.assertEqual(self.job()['status'], 'skipped_completed')
        self.send_mock.assert_not_called()
        self.delete('engagement', self.job()['id'])
        self.put('local', {'id': 'shift_roster:' + DATE, 'date': DATE, 'version': ROSTER_VERSION,
                          'people': [], 'issues': [{'scope': 'B', 'reason': '名单读取不全'}]})
        self.run_due(); self.assertEqual(self.job()['status'], 'sent')
        self.assertTrue(self.job()['report']['unverified'])

    def test_legacy_record_without_date_still_counts_submitted_answers(self):
        record = self.answer(); record.pop('date')
        self.put('record', record)
        self.assertFalse(self.report()['unfinished'])

    def test_unclaimed_uses_frozen_reserve_not_todays_bank(self):
        self.delete('paper', 'paper:p1')
        self.assertEqual(self.report()['unfinished'][0]['assigned'], sum(core.QUOTAS.values()))
        self.put('reserve', {'id': 'reserve', 'date': DATE, 'scope': 'A', 'questions': []})
        for n in range(20):
            self.put('question', self.question(n))
        self.assertFalse(self.report()['unfinished']); self.assertTrue(self.report()['unverified'])
        self.delete('reserve', 'reserve')
        self.assertTrue(self.report()['unverified'])

    def test_saved_completed_projection_cannot_mask_missing_source_cache(self):
        self.answer()
        row = completion.project(self.service, DATE, current=NOW)[0]
        self.assertTrue(completion.valid(row))
        self.put(completion.KIND, row)
        self.delete('record', 'paper:p1')
        self.assertFalse(self.report()['unfinished']); self.assertTrue(self.report()['unverified'])
        self.delete('paper', 'paper:p1')
        self.assertFalse(self.report()['unfinished']); self.assertTrue(self.report()['unverified'])

    def test_restored_gate_disabled_gate_and_before_09_even_in_utc(self):
        self.run_due(NOW - dt.timedelta(seconds=1))
        self.assertIsNone(self.job())
        self.service._restored = False
        self.run_due(); self.assertIsNone(self.job())
        self.service._restored = True
        self.put('settings', {**self.service.settings(), 'id': 'main', 'enabled': False})
        self.run_due(); self.assertIsNone(self.job()); self.send_mock.assert_not_called()
        self.put('settings', {**self.service.settings(), 'id': 'main', 'enabled': True})
        self.run_due(NOW.astimezone(dt.timezone.utc)); self.assertEqual(self.job()['status'], 'sent')

    def test_shift_end_exactly_09_due_later_end_not_accused(self):
        self.member('night', '夜')
        for minutes, expected in ((0, True), (1, False)):
            with self.subTest(minutes=minutes), patch('lan_bitable_template_portal.pre_shift_checks.windows',
                    side_effect=lambda date, shift: (windows(date, shift)[0],
                        NOW + dt.timedelta(minutes=minutes) if shift == '夜' else windows(date, shift)[1])):
                report = self.report()
                self.assertEqual(any(r['person_id'] == 'night' for r in report['unfinished']), expected)

    def test_restart_after_09_catches_once_and_no_backlog_before_09(self):
        old = {'id': 'daily-report:2026-10-25', 'kind': daily.KIND, 'date': '2026-10-25', 'status': 'pending', 'due_at': 0}
        self.put('engagement', old)
        self.restart(); self.run_due(NOW.replace(hour=0))
        self.assertEqual(self.service._get('engagement', old['id'])['status'], 'expired')
        self.send_mock.assert_not_called()
        self.run_due(NOW.replace(hour=11)); self.restart(); self.run_due(NOW.replace(hour=12))
        self.assertEqual(self.send_mock.call_count, 1)
        self.assertEqual(self.job()['status'], 'sent')

    def test_all_completed_sends_green_summary_once_with_people_and_accuracy(self):
        self.member('night', '夜')
        self.answer(correct=2); self.answer('night', correct=1)
        self.run_due()
        self.assertEqual(self.job()['status'], 'sent')
        card = self.send_mock.call_args.args[0]
        text = json.dumps(card, ensure_ascii=False)
        self.assertEqual(card['header']['template'], 'green')
        self.assertIn('全部答题完成', text)
        self.assertIn('答题人员 2 人', text)
        self.assertIn('75.0%', text)
        self.assertIn('3/4题', text)
        self.assertNotIn('<font color="red">', text)
        self.restart(); self.run_due(NOW + dt.timedelta(hours=1))
        self.assertEqual(self.send_mock.call_count, 1)

    def test_completed_with_zero_correct_is_not_missing_accuracy(self):
        self.answer(); self.run_due()
        self.assertEqual(self.job()['status'], 'sent')
        self.assertIn('0.0%', json.dumps(self.send_mock.call_args.args[0], ensure_ascii=False))

    def test_completed_subjective_questions_do_not_invent_accuracy(self):
        paper = self.service._get('paper', 'paper:p1')
        paper['questions'] = [self.question(n, kind='interview') for n in range(2)]
        self.put('paper', paper); self.answer(correct=2); self.run_due()
        text = json.dumps(self.send_mock.call_args.args[0], ensure_ascii=False)
        self.assertIn('全部答题完成', text)
        self.assertIn('暂无可统计客观题', text)
        self.assertNotIn('100.0%', text)

    def test_completed_metrics_use_first_answers_and_exclude_review_and_practice(self):
        record = self.answer(correct=1)
        for entry in record['entries'].values():
            entry['practice'] = [{'correct': True, 'submitted_at': NOW.isoformat()}]
        self.put('record', record)
        self.put('paper', {'id': 'practice', 'person_id': 'p1', 'date': DATE, 'scope': 'A',
                          'mode': 'practice', 'questions': [self.question(9)]})
        self.put('record', {'id': 'practice', 'entries': {'written:9': {
            'attempt': {'correct': True, 'submitted_at': NOW.isoformat()}}}})
        self.run_due()
        text = json.dumps(self.send_mock.call_args.args[0], ensure_ascii=False)
        self.assertIn('50.0%', text)
        self.assertIn('1/2题', text)

    def test_completed_roster_duplicates_do_not_inflate_people_or_accuracy(self):
        self.roster.append(copy.deepcopy(self.roster[0])); self.save_roster()
        self.answer(correct=1)
        report = self.report()
        report['completed'].append(copy.deepcopy(report['completed'][0]))
        card = daily.render_card(report, 'https://portal.invalid')
        text = json.dumps(card, ensure_ascii=False)
        self.assertIn('答题人员 1 人', text)
        self.assertIn('1/2题', text)

    def test_accuracy_weights_question_counts_and_people_are_unique_across_shifts(self):
        self.answer(correct=1)
        report = self.report()
        report['completed'].append({**report['completed'][0], 'shift': '夜',
                                   'answered': 3, 'assigned': 3, 'correct': 3, 'objective_answered': 3})
        card = daily.render_card(report, 'https://portal.invalid')
        text = json.dumps(card, ensure_ascii=False)
        self.assertIn('答题人员 1 人', text)
        self.assertIn('完成 2 人班', text)
        self.assertIn('80.0%（4/5题）', text)

    def test_completed_people_with_unverified_or_failed_duties_never_report_all_done(self):
        self.answer(correct=1); self.member('night', '夜')
        for status, delivery in (('pending', []), ('sent', [])):
            with self.subTest(status=status):
                self.notice('night', '夜', status=status, delivery=delivery)
                report = self.report()
                card = daily.render_card(report, 'https://portal.invalid')
                self.assertEqual(card['header']['template'], 'orange')
                self.assertNotIn('全部答题完成', json.dumps(card, ensure_ascii=False))

    def test_failure_rechecks_completed_and_never_retries_early(self):
        self.send_mock.side_effect = self.reject
        self.run_due(); self.assertEqual(self.job()['status'], 'pending')
        self.run_due(NOW + dt.timedelta(minutes=1)); self.assertEqual(self.send_mock.call_count, 1)
        self.send_mock.side_effect = self.accept
        self.answer(); self.run_due(NOW + dt.timedelta(minutes=5))
        self.assertEqual(self.job()['status'], 'sent'); self.assertEqual(self.send_mock.call_count, 2)
        self.assertIn('全部答题完成', json.dumps(self.send_mock.call_args.args[0], ensure_ascii=False))

    def test_definite_failure_retries_new_verified_payload_only(self):
        self.send_mock.side_effect = self.reject
        self.run_due(); self.answer(count=1)
        self.send_mock.side_effect = self.accept
        self.run_due(NOW + dt.timedelta(minutes=5))
        first, second = self.send_mock.call_args_list
        self.assertNotEqual(first.args[1], second.args[1])
        self.assertIn('1/2题', json.dumps(second.args[0], ensure_ascii=False))
        self.assertEqual(self.job()['status'], 'sent')

    def test_unknown_malformed_boolean_and_wrong_receipts_never_replay(self):
        for response in (True, (True, ''), (False, '', []),
                         (True, '', [{'chat_id': 'oc_wrong', 'ok': True, 'message_id': 'om_wrong'}])):
            with self.subTest(response=response):
                self.delete('engagement', 'daily-report:' + DATE)
                self.send_mock.reset_mock(); self.send_mock.side_effect = lambda *args: response
                self.run_due(); self.assertEqual(self.job()['status'], 'needs_review')
                self.assertIn('未确认', self.service._last_error)
                self.restart(); self.run_due(NOW + dt.timedelta(hours=2))
                self.assertEqual(self.send_mock.call_count, 1)

    def test_blocked_fingerprint_conflict_and_wrong_app_are_not_safe_rejections(self):
        def blocked(card, identity):
            return False, '', [{'chat_id': engagement.WEEKLY_CHAT_ID, 'ok': False,
                'message_uuid': identity, 'failure_kind': 'blocked'}]
        def wrong_app(card, identity):
            ok, message, rows = self.accept(card, identity)
            rows[0]['sender_app_id'] = 'cli_previous'
            return ok, message, rows
        def contradictory(card, identity):
            _, message, rows = self.accept(card, identity)
            return False, message, rows
        def contradictory_rejection(card, identity):
            _, message, rows = self.reject(card, identity)
            return True, message, rows
        for sender in (blocked, wrong_app, contradictory, contradictory_rejection):
            with self.subTest(sender=sender.__name__):
                self.delete('engagement', 'daily-report:' + DATE)
                self.send_mock.reset_mock(); self.send_mock.side_effect = sender
                self.run_due(); self.assertEqual(self.job()['status'], 'needs_review')
                self.run_due(NOW + dt.timedelta(hours=2)); self.assertEqual(self.send_mock.call_count, 1)

    def test_prep_exception_retries_without_claiming_an_unknown_delivery(self):
        with patch.object(daily, 'render_card', side_effect=ValueError('synthetic render error')):
            self.run_due()
        self.assertEqual(self.job()['status'], 'pending')
        self.assertFalse(self.job().get('attempted_at')); self.send_mock.assert_not_called()
        self.run_due(NOW + dt.timedelta(minutes=5)); self.assertEqual(self.job()['status'], 'sent')

    def test_send_exception_and_lost_local_receipt_do_not_replay(self):
        self.send_mock.side_effect = TimeoutError('synthetic')
        self.run_due(); self.assertEqual(self.job()['status'], 'needs_review')
        self.restart(); self.run_due(NOW + dt.timedelta(hours=2))
        self.assertEqual(self.send_mock.call_count, 1)
        self.delete('engagement', self.job()['id']); self.send_mock.side_effect = self.accept
        original = self.service._put
        def lose_receipt(kind, key, value, *args, **kwargs):
            if kind == 'engagement' and value.get('kind') == daily.KIND and value.get('status') == 'sent':
                raise OSError('synthetic disk failure after delivery')
            return original(kind, key, value, *args, **kwargs)
        with patch.object(self.service, '_put', side_effect=lose_receipt):
            self.run_due()
        self.assertEqual(self.job()['status'], 'needs_review')
        self.restart(); self.run_due(NOW + dt.timedelta(hours=2)); self.assertEqual(self.send_mock.call_count, 2)

    def test_parallel_instances_share_one_durable_claim(self):
        other = core.LearningService(self.root, self.cloud, self.sender, get_portal_url=self.service._portal_url)
        other._restored = True
        with patch('threading.Thread.start', REAL_THREAD_START), ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda i: daily.send_due(self.service if i % 2 else other, NOW), range(8)))
        self.assertEqual(self.send_mock.call_count, 1); self.assertEqual(self.job()['status'], 'sent')

    def test_exact_lease_expiry_recovers_unattempted_but_not_attempted(self):
        for attempted, count, status in ((None, 1, 'sent'), (NOW.timestamp(), 0, 'needs_review')):
            with self.subTest(attempted=attempted):
                self.put('engagement', {'id': 'daily-report:' + DATE, 'kind': daily.KIND, 'date': DATE,
                    'status': 'pending', 'due_at': NOW.timestamp(), 'attempt': 0,
                    'sending_until': NOW.timestamp() + daily.LEASE_SECONDS, 'attempted_at': attempted, 'lease': 'old'})
                self.send_mock.reset_mock()
                self.run_due(NOW + dt.timedelta(seconds=daily.LEASE_SECONDS - 1))
                self.send_mock.assert_not_called()
                self.run_due(NOW + dt.timedelta(seconds=daily.LEASE_SECONDS))
                self.assertEqual(self.send_mock.call_count, count); self.assertEqual(self.job()['status'], status)

    def test_inflight_lease_expiry_and_late_success_do_not_send_twice(self):
        def late(card, identity):
            self.clock = NOW + dt.timedelta(seconds=daily.LEASE_SECONDS)
            daily.send_due(self.service, self.clock)
            self.assertEqual(self.job()['status'], 'needs_review')
            return self.accept(card, identity)
        self.send_mock.side_effect = late
        self.run_due(); self.assertEqual(self.send_mock.call_count, 1)
        self.assertEqual(self.job()['status'], 'sent')

    def test_cancel_and_stop_during_preparation_prevent_send(self):
        def cancel():
            self.put('engagement', {**self.job(), 'status': 'cancelled'})
            return 'https://portal.invalid'
        self.service._portal_url = cancel
        self.run_due(); self.assertEqual(self.job()['status'], 'cancelled'); self.send_mock.assert_not_called()
        self.restart(); self.run_due(); self.send_mock.assert_not_called()

    def test_stop_or_disable_after_first_snapshot_never_sends(self):
        original = daily.collect
        for stop in (False, True):
            with self.subTest(stop=stop):
                self.delete('engagement', 'daily-report:' + DATE)
                self.service._stop.clear()
                self.put('settings', {**self.service.settings(), 'id': 'main', 'enabled': True})
                def change(*args):
                    result = original(*args)
                    if stop:
                        self.service._stop.set()
                    else:
                        self.service._put('settings', 'main', {**self.service.settings(), 'enabled': False}, args[3], False)
                    return result
                with patch.object(daily, 'collect', side_effect=change):
                    self.run_due()
                self.send_mock.assert_not_called()
                self.assertEqual(self.job()['status'], 'pending')
                self.assertFalse(self.job().get('attempted_at'))

    def test_stale_worker_after_lease_takeover_cannot_overwrite_success(self):
        other = core.LearningService(self.root, self.cloud, self.sender, get_portal_url=lambda: 'https://portal.invalid')
        other._restored = True
        def slow_url():
            self.clock = NOW + dt.timedelta(seconds=daily.LEASE_SECONDS)
            daily.send_due(other, self.clock)
            return 'https://portal.invalid'
        self.service._portal_url = slow_url
        self.run_due()
        self.assertEqual(self.send_mock.call_count, 1)
        self.assertEqual(self.job()['status'], 'sent')

    def test_no_business_database_write_lock_is_held_during_delivery(self):
        other = core.LearningService(self.root, self.cloud, self.sender)
        def unlocked(card, identity):
            with other.transaction() as conn:
                other._put('local', 'concurrent_answer', {'ok': True}, conn, False)
            return self.accept(card, identity)
        self.send_mock.side_effect = unlocked
        self.run_due(); self.assertEqual(self.job()['status'], 'sent')
        self.assertTrue(other._get('local', 'concurrent_answer')['ok'])

    def test_rollover_only_parses_pending_old_daily_reports(self):
        self.put('engagement', {'id': 'old-weekly', 'kind': 'weekly', 'status': 'pending', 'date': '2026-10-01'})
        self.put('engagement', {'id': 'daily-report:2026-10-25', 'kind': daily.KIND,
            'date': '2026-10-25', 'status': 'sending', 'attempted_at': NOW.timestamp() - 10000})
        original = self.service._all
        def bounded(kind, *args):
            self.assertNotEqual(kind, 'engagement', 'daily scheduler must not decode all historical engagements')
            return original(kind, *args)
        with patch.object(self.service, '_all', side_effect=bounded):
            self.run_due(NOW.replace(hour=0))
        self.send_mock.assert_not_called()
        self.assertEqual(self.service._get('engagement', 'old-weekly')['status'], 'pending')
        self.assertEqual(self.service._get('engagement', 'daily-report:2026-10-25')['status'], 'needs_review')

    def test_late_callback_cannot_resurrect_cancelled_job(self):
        def cancelled(card, identity):
            self.put('engagement', {**self.job(), 'status': 'cancelled'})
            return self.accept(card, identity)
        self.send_mock.side_effect = cancelled
        self.run_due(); self.assertEqual(self.job()['status'], 'cancelled')
        self.assertEqual(self.job()['delivery'][0]['message_id'], 'om_report')
        self.restart(); self.run_due(NOW + dt.timedelta(minutes=5)); self.assertEqual(self.send_mock.call_count, 1)

    def test_completion_between_first_snapshot_and_send_is_rechecked(self):
        original = daily.collect
        calls = []
        def changed(*args):
            # Simulate a committed answer between the two report transactions.
            if calls:
                conn = args[3]
                paper = self.service._get('paper', 'paper:p1', conn)
                self.service._put('record', paper['id'], {'id': paper['id'], 'date': DATE, 'person_id': 'p1',
                    'entries': {q['id']: {'attempt': {'correct': True, 'submitted_at': NOW.isoformat()}}
                                for q in paper['questions']}}, conn, False)
            calls.append(True)
            return original(*args)
        with patch.object(daily, 'collect', side_effect=changed):
            self.run_due()
        self.assertEqual(len(calls), 2); self.send_mock.assert_called_once()
        self.assertEqual(self.job()['status'], 'sent')
        self.assertIn('全部答题完成', json.dumps(self.send_mock.call_args.args[0], ensure_ascii=False))

    def test_midnight_during_preparation_expires_original_day(self):
        def rollover():
            self.clock = NOW + dt.timedelta(days=1)
            return 'https://portal.invalid'
        self.service._portal_url = rollover
        self.run_due(); self.send_mock.assert_not_called()
        self.assertEqual(self.job()['status'], 'expired')

    def test_public_dispatcher_keeps_qt_receipts_and_no_callback_permissions(self):
        card = daily.render_card(self.report(), 'https://portal.invalid')
        dispatch = Mock(return_value=self.accept(card, 'uuid'))
        # Importing the production config would initialize real log/data paths.
        with patch.dict(sys.modules, {'upload_event_module.services.robot_webhook':
                                     SimpleNamespace(_deliver_many=dispatch)}):
            self.assertEqual(REAL_SEND(card, 'uuid')[2][0]['sender_app_id'], 'cli_current')
        dispatch.assert_called_once_with('interactive', card, [engagement.WEEKLY_CHAT_ID], 'chat_id', 'uuid')
        button = card['elements'][-1]['actions'][0]
        self.assertIn('/learning?', button['url']); self.assertNotIn('value', button)

    def test_mock_or_blocked_mode_never_reaches_public_dispatcher(self):
        portal = sys.modules['lan_bitable_template_portal.portal_service']
        for guard in ({'real_write_allowed': False}, {'real_write_allowed': True, 'mock_external': True}):
            dispatch = Mock(side_effect=AssertionError('real send forbidden'))
            with self.subTest(guard=guard), patch.object(portal, 'external_real_write_guard', return_value=guard), \
                 patch.dict(sys.modules, {'upload_event_module.services.robot_webhook': SimpleNamespace(_deliver_many=dispatch)}):
                result = REAL_SEND(daily.render_card(self.report(), 'https://portal.invalid'), 'uuid')
                self.assertFalse(result[0]); self.assertEqual(result[2][0]['failure_kind'], 'not_sent')
                self.assertEqual(result[2][0]['chat_id'], engagement.WEEKLY_CHAT_ID)
                self.assertEqual(result[2][0]['message_uuid'], 'uuid')
                dispatch.assert_not_called()

    def test_stop_while_rendering_does_not_send(self):
        original = daily.render_card
        def stop(report, url):
            self.service._stop.set()
            return original(report, url)
        with patch.object(daily, 'render_card', side_effect=stop):
            self.run_due()
        self.send_mock.assert_not_called(); self.assertEqual(self.job()['status'], 'pending')
        self.assertFalse(self.job().get('attempted_at'))

    def test_worker_hook_excludes_daily_from_legacy_dispatch(self):
        with patch.object(engagement, 'duty_recipients', return_value=set()), \
             patch.object(engagement, 'repeated_reminders', return_value=({}, set())), \
             patch.object(daily, 'send_due') as hook:
            self.put('engagement', {'id': 'daily-report:' + DATE, 'kind': daily.KIND,
                'date': DATE, 'status': 'pending', 'due_at': 0})
            engagement.send_due(self.service, NOW)
        hook.assert_called_once_with(self.service, NOW)
        self.assertEqual(self.job()['status'], 'pending'); self.assertFalse(self.sender.calls)

    def test_restore_preserves_old_kinds_and_completed_daily_receipt(self):
        self.run_due()
        self.cloud.entities['engagement', self.job()['id']] = copy.deepcopy(self.job())
        self.cloud.entities['engagement', 'old-week'] = {'id': 'old-week', 'kind': 'weekly_batch', 'status': 'prepared'}
        self.delete('engagement', self.job()['id'])
        self.restart(); self.service.restore(); self.run_due()
        self.assertEqual(self.send_mock.call_count, 1)
        self.assertEqual(self.service._get('engagement', 'old-week')['kind'], 'weekly_batch')

    def test_red_render_deduplicates_person_shifts_and_separates_unknown(self):
        report = self.report(); row = report['unfinished'][0]
        report['unfinished'] += [dict(row), {**row, 'shift': '夜'}]
        report['failed'] = [{'name': '发送失败人员', 'reason': '投递失败'}]
        report['example'] = True
        card = daily.render_card(report, 'https://portal.invalid/')
        text = json.dumps(card, ensure_ascii=False)
        self.assertEqual(card['header']['template'], 'orange')
        self.assertIn('2 人班 · 去重 1 人', text)
        self.assertIn('不代表实际进度', text)
        self.assertEqual(text.count('<font color='), 2)
        failure = next(e for e in card['elements'] if '发送失败人员' in str(e))
        self.assertEqual(failure['text']['tag'], 'plain_text')
        query = parse_qs(urlsplit(card['elements'][-1]['actions'][0]['url']).query)
        self.assertEqual(query, {'tab': ['completion']})
        self.assertEqual(card['elements'][-1]['actions'][0]['text']['content'], '查看学练完成情况')


if __name__ == '__main__':
    unittest.main()

"""Offline shift-bound reminder periods and durable delivery deduplication."""
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
from bin.test_learning import learning
from lan_bitable_template_portal import learning_engagement as engagement
from lan_bitable_template_portal.pre_shift_checks import windows

REAL_THREAD_START = threading.Thread.start
DAY = '2026-10-10'


class RepeatedReminderTests(unittest.TestCase):
    question = support.LearningTests.question

    def setUp(self):
        support.LearningTests.setUp(self)
        fake_portal = SimpleNamespace(BUILDING_OPEN_ID_MAP={'A': 'ou_shared'})
        guard = patch.dict(sys.modules, {'lan_bitable_template_portal.portal_service': fake_portal})
        guard.start(); self.addCleanup(guard.stop)
        self.hr = {'people': {'r1': {'name': 'Worker', 'scopes': ['A'], 'selectable': True,
            'position': '值班员', 'login_ids': ['ou_person']}}}
        self.roster = [{'name': 'Worker', 'scope': 'A', 'shift': '白', 'open_id': 'ou_person'}]
        self.reader = Mock(side_effect=lambda day: copy.deepcopy(self.roster) if day == DAY else [])
        self.service._login_people_reader = lambda: self.hr
        self.service._shift_roster_reader = self.reader
        self.service._portal_url = lambda: 'http://portal.test:18766'
        self.service._restored = True
        with self.service.transaction() as conn:
            self.service._put('question', 'written:1', self.question(1), conn, False)
            self.service._put('settings', 'main', {**learning.DEFAULT_SETTINGS, 'enabled': True,
                'weekly_report_enabled': False}, conn, False)
            self.service._put('person', 'canonical', {'id': 'canonical', 'name': 'Worker', 'active': True,
                'scopes': ['A'], 'aliases': ['directory:r1'], 'login_ids': ['ou_person']}, conn, False)
            self.service._put('reserve', 'reserve', {'id': 'reserve', 'scope': 'A', 'date': DAY,
                'questions': [self.question(1)]}, conn, False)
            self.service._put('publication', 'personal:' + DAY, {'date': DAY,
                'created_at': DAY + 'T13:00:00+08:00', 'reserve_ids': ['reserve'], 'notify': True}, conn, False)

    def at(self, hour, minute=0, day=10):
        return dt.datetime(2026, 10, day, hour, minute, tzinfo=learning.TZ)

    def send(self, current):
        with self.service._notification_lock:
            engagement.send_due(self.service, current)

    def claim(self, *, started=False, complete=False):
        paper = {'id': 'daily', 'person_id': 'canonical', 'scope': 'A', 'date': DAY,
                 'questions': [self.question(1)]}
        if started:
            paper['started_at'] = DAY + 'T13:00:00+08:00'
        with self.service.transaction() as conn:
            self.service._put('paper', paper['id'], paper, conn, False)
            if complete:
                self.service._put('record', paper['id'], {'id': paper['id'], 'entries': {'written:1': {
                    'attempt': {'correct': True, 'submitted_at': DAY + 'T14:00:00+08:00'}}}}, conn, False)
        return paper

    def test_unclaimed_day_reminds_at_15_17_and_stops_at_existing_shift_end(self):
        for current in (self.at(13), self.at(14, 59), self.at(15), self.at(15), self.at(17), self.at(18), self.at(19)):
            self.send(current)
        self.assertEqual(len(self.sender.calls), 2)
        self.assertIn('尚未开始', json.dumps(self.sender.calls[0][1], ensure_ascii=False))
        self.assertEqual([c[0] for c in self.sender.calls], ['ou_person'] * 2)
        self.assertNotEqual(self.sender.calls[0][2], self.sender.calls[1][2])
        self.reader.assert_called_once_with(DAY)

    def test_started_unsubmitted_and_claimed_unstarted_are_both_covered(self):
        self.claim()
        self.send(self.at(15))
        self.assertIn('1题未提交', json.dumps(self.sender.calls[0][1], ensure_ascii=False))
        self.claim(started=True)
        self.send(self.at(17))
        self.assertEqual(len(self.sender.calls), 2)

    def test_night_crosses_midnight_uses_roster_date_and_authoritative_window(self):
        self.roster[0]['shift'] = '夜'
        for hour, day in ((20, 10), (22, 10), (0, 11), (2, 11), (4, 11), (6, 11), (8, 11), (9, 11)):
            self.send(self.at(hour, day=day).astimezone(dt.timezone.utc))
        deadline = windows(DAY, '夜')[1]
        expected = list(range(22, 24, 2)) + list(range(0, deadline.hour, 2))
        self.assertEqual(len(self.sender.calls), len(expected))
        self.assertTrue(all('2026-10-10:夜:A:canonical:' in c[2] for c in self.sender.calls))
        self.reader.assert_called_once_with(DAY)

    def test_finished_deleted_no_publication_no_scope_paper_or_off_duty_stop(self):
        self.claim(complete=True)
        self.send(self.at(15))
        self.assertFalse(self.sender.calls)
        with self.service.transaction() as conn:
            paper = self.service._get('paper', 'daily', conn)
            paper['deleted_at'] = DAY
            self.service._put('paper', 'daily', paper, conn, False)
        self.send(self.at(17))
        self.assertFalse(self.sender.calls)

    def test_no_applicable_published_questions_or_roster_identity_never_sends(self):
        with self.service.transaction() as conn:
            reserve = self.service._get('reserve', 'reserve', conn)
            reserve['scope'] = 'B'
            self.service._put('reserve', 'reserve', reserve, conn, False)
        self.send(self.at(15))
        self.assertFalse(self.sender.calls)
        self.assertFalse(self.service._all('engagement'))
        with self.service.transaction() as conn:
            reserve['scope'] = 'A'
            self.service._put('reserve', 'reserve', reserve, conn, False)
        self.hr['people']['r1']['inactive'] = True
        self.send(self.at(17))
        self.assertFalse(self.sender.calls)

    def test_restart_and_parallel_schedulers_send_only_one_per_period(self):
        other = learning.LearningService(self.root, self.cloud, self.sender,
            get_login_people=lambda: self.hr, get_shift_roster=self.reader, get_portal_url=self.service._portal_url)
        other._restored = True
        with patch('threading.Thread.start', REAL_THREAD_START), ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda n: engagement.send_due(self.service if n % 2 else other, self.at(15)), range(8)))
        self.assertEqual(len(self.sender.calls), 1)
        self.service = other
        self.send(self.at(15))
        self.send(self.at(17))
        self.assertEqual(len(self.sender.calls), 2)

    def test_failed_send_retries_same_period_id_and_completed_paper_stops_retry(self):
        self.claim(started=True)
        self.sender.fail_once = True
        self.send(self.at(15))
        self.send(self.at(15, 1))
        self.assertEqual(len(self.sender.calls), 1)
        self.send(self.at(15, 6))
        self.assertEqual(self.sender.calls[0][2], self.sender.calls[1][2])
        self.claim(complete=True)
        self.send(self.at(17))
        self.assertEqual(len(self.sender.calls), 2)

    def test_old_delivered_period_and_original_history_are_preserved(self):
        paper = self.claim(started=True)
        old = {'id': 'timeout:daily', 'kind': 'timeout', 'paper_id': paper['id'], 'person_id': 'canonical',
               'date': DAY, 'status': 'sent', 'due_at': self.at(15).timestamp(),
               'sent_at': self.at(15).isoformat(), 'sent_to': ['ou_person']}
        with self.service.transaction() as conn:
            self.service._put('engagement', old['id'], old, conn, False)
        before = self.service._get('engagement', old['id'])
        self.send(self.at(15))
        self.assertFalse(self.sender.calls)
        self.send(self.at(17))
        self.assertEqual(len(self.sender.calls), 1)
        self.assertEqual(self.service._get('engagement', old['id']), before)

    def test_missing_roster_retains_pending_job_and_shared_accounts_never_receive(self):
        self.send(self.at(15))
        self.service._shift_roster_reader = None
        self.send(self.at(17))
        self.assertEqual(len(self.sender.calls), 1)
        self.hr['people']['r1']['login_ids'] = ['ou_shared']
        self.service._shift_roster_reader = self.reader
        self.send(self.at(17))
        self.assertEqual(len(self.sender.calls), 1)

    def test_new_app_roster_uses_canonical_alias_without_replacing_old_logins(self):
        self.claim(complete=True)
        self.hr['people']['r1']['login_ids'] = ['ou_newApp']
        self.roster[0]['open_id'] = 'ou_newApp'
        self.send(self.at(15))
        self.assertFalse(self.sender.calls, 'completion belongs to the existing canonical person')
        with self.service.transaction() as conn:
            conn.execute("DELETE FROM documents WHERE kind='record' AND key='daily'")
            paper = self.service._get('paper', 'daily', conn)
            self.service._put('paper', 'daily', paper, conn, False)
        self.send(self.at(17))
        self.assertEqual(len(self.sender.calls), 1)
        self.assertEqual(self.sender.calls[0][0], 'ou_newApp')
        self.assertIn(':canonical:', self.sender.calls[0][2])
        self.assertEqual(self.service._get('person', 'canonical')['login_ids'], ['ou_person'])
        self.assertEqual(len(self.service._all('person')), 1)

    def test_duplicate_alias_or_roster_name_does_not_guess_a_person(self):
        with self.service.transaction() as conn:
            other = {**self.service._get('person', 'canonical', conn), 'id': 'other'}
            self.service._put('person', 'other', other, conn, False)
        self.send(self.at(15))
        self.assertFalse(self.sender.calls)
        self.assertFalse(self.service._all('engagement'))

    def test_new_app_shared_duty_mapping_is_never_a_personal_recipient(self):
        from lan_bitable_template_portal.feishu_identity import TARGET_DUTY_OPEN_IDS
        shared = TARGET_DUTY_OPEN_IDS['A']
        self.hr['people']['r1']['login_ids'] = [shared]
        self.roster[0]['open_id'] = shared
        self.send(self.at(15))
        self.assertFalse(self.sender.calls)
        self.assertFalse(self.service._all('engagement'))

    def test_fresh_roster_alias_can_keep_trusted_old_hr_principal(self):
        self.claim(complete=True)
        self.roster[0]['open_id'] = 'ou_newApp'
        self.hr['people']['r1']['feishu_open_ids'] = {'current_app': 'ou_newApp'}
        self.send(self.at(15))
        self.assertFalse(self.sender.calls)
        self.assertEqual(len(self.service._all('person')), 1)

    def test_completed_after_failed_delivery_stops_retry_in_same_period(self):
        self.claim()
        self.sender.fail_once = True
        self.send(self.at(15))
        self.claim(complete=True)
        self.send(self.at(15, 6))
        self.assertEqual(len(self.sender.calls), 1)
        job = self.service._all('engagement')[0]
        self.assertEqual(job['status'], 'skipped_completed')

    def test_late_publication_and_configured_time_never_start_early(self):
        with self.service.transaction() as conn:
            publication = self.service._get('publication', 'personal:' + DAY, conn)
            publication['created_at'] = DAY + 'T14:00:00+08:00'
            self.service._put('publication', 'personal:' + DAY, publication, conn, False)
        for hour in (13, 15, 16, 17, 18):
            self.send(self.at(hour))
        self.assertEqual(len(self.sender.calls), 1)
        self.assertEqual(self.service._all('engagement')[0]['due_at'], self.at(16).timestamp() + 300)

    def test_pending_old_daily_timer_cannot_send_before_or_after_shift(self):
        paper = self.claim(started=True)
        old = {'id': 'timeout:daily', 'kind': 'timeout', 'paper_id': paper['id'], 'person_id': 'canonical',
               'date': DAY, 'status': 'pending', 'due_at': self.at(11).timestamp()}
        with self.service.transaction() as conn:
            self.service._put('engagement', old['id'], old, conn, False)
        self.send(self.at(11))
        self.send(self.at(18))
        self.assertFalse(self.sender.calls)
        self.assertEqual(self.service._get('engagement', old['id'])['status'], 'superseded')

    def test_missing_publication_and_next_day_do_not_reuse_old_questions(self):
        with self.service.transaction() as conn:
            conn.execute("DELETE FROM documents WHERE kind='publication'")
        self.send(self.at(15))
        self.send(self.at(15, day=11))
        self.assertFalse(self.sender.calls)
        self.reader.assert_not_called()

    def test_withdrawn_or_consumed_questions_do_not_remind_unclaimed_learners(self):
        with self.service.transaction() as conn:
            q = self.service._get('question', 'written:1', conn)
            q['status'] = 'draft'
            self.service._put('question', q['id'], q, conn, False)
        self.send(self.at(15))
        self.assertFalse(self.sender.calls)
        with self.service.transaction() as conn:
            q['status'] = 'published'
            self.service._put('question', q['id'], q, conn, False)
            conn.execute('INSERT INTO learning_usage VALUES(?,?,?,?,?)', ('old', q['family_id'], 'canonical', 'A', '2026-10-09'))
        self.send(self.at(17))
        self.assertFalse(self.sender.calls)

    def test_same_name_people_are_isolated_by_canonical_id_scope_and_roster_date(self):
        self.hr['people']['r2'] = {**self.hr['people']['r1'], 'scopes': ['B'], 'login_ids': ['ou_other']}
        self.roster.append({**self.roster[0], 'scope': 'B', 'shift': '夜', 'open_id': 'ou_other'})
        with self.service.transaction() as conn:
            person = {**self.service._get('person', 'canonical', conn), 'id': 'canonicalB',
                      'aliases': ['directory:r2'], 'scopes': ['B'], 'login_ids': ['ou_other']}
            self.service._put('person', 'canonicalB', person, conn, False)
            reserve = {**self.service._get('reserve', 'reserve', conn), 'id': 'reserveB', 'scope': 'B'}
            self.service._put('reserve', 'reserveB', reserve, conn, False)
            publication = self.service._get('publication', 'personal:' + DAY, conn)
            publication['reserve_ids'].append('reserveB')
            self.service._put('publication', 'personal:' + DAY, publication, conn, False)
        self.send(self.at(15))
        self.assertEqual([c[0] for c in self.sender.calls], ['ou_person'])
        self.send(self.at(22))
        self.assertEqual([c[0] for c in self.sender.calls], ['ou_person', 'ou_other'])
        self.send(self.at(0, day=11))
        self.assertEqual([c[0] for c in self.sender.calls], ['ou_person', 'ou_other', 'ou_other'])
        self.assertIn('2026-10-10:夜:B:canonicalB', self.sender.calls[-1][2])

    def test_expired_pending_period_is_not_backfilled_and_crash_lease_is_retried(self):
        engagement.repeated_reminders(self.service, self.at(15))
        job = self.service._all('engagement')[0]
        with self.service.transaction() as conn:
            job['sending_until'] = self.at(15, 1).timestamp()
            self.service._put('engagement', job['id'], job, conn, False)
        self.send(self.at(15))
        self.assertFalse(self.sender.calls)
        self.send(self.at(15, 2))
        self.assertEqual(len(self.sender.calls), 1)
        self.assertEqual(self.sender.calls[0][2], job['id'])
        engagement.repeated_reminders(self.service, self.at(17))
        self.send(self.at(18))
        self.assertEqual(len(self.sender.calls), 1)
        self.assertIn('expired', [j['status'] for j in self.service._all('engagement')])

    def test_unclaimed_night_link_claims_and_reopens_roster_day_after_midnight(self):
        self.roster[0]['shift'] = '夜'
        current = self.at(0, day=11)
        self.send(current)
        url = self.sender.calls[-1][1]['elements'][-1]['actions'][0]['url']
        query = parse_qs(urlsplit(url).query)
        self.assertEqual((query['date'], query['shift']), ([DAY], ['夜']))
        actor = {'id': 'personnel_r1', 'person_id': 'canonical', 'can_answer': True,
                 'shared_account': False, 'scope': '', 'is_admin': False}
        payload = {'person_id': 'canonical', 'scope': 'A', 'date': query['date'][0], 'shift': query['shift'][0]}
        with self.service.transaction() as conn:
            self.service._put('publication', 'personal:2026-10-11', {'date': '2026-10-11',
                'created_at': '2026-10-11T00:00:00+08:00', 'reserve_ids': [], 'notify': True}, conn, False)
        with patch.object(learning, 'now', return_value=current):
            claimed = self.service.dispatch('paper.claim', payload, actor, {})
            again = self.service.dispatch('paper.claim', payload, actor, {})
            opened = self.service.dispatch('paper.get', {'id': claimed['id']}, actor, {})
        self.assertEqual(claimed['date'], DAY)
        self.assertEqual(claimed['id'], 'personal:' + DAY + ':canonical')
        self.assertEqual(again['id'], claimed['id'])
        self.assertEqual(opened['date'], DAY)
        self.assertEqual(len(self.service._all('paper')), 1)
        with patch.object(learning, 'now', return_value=windows(DAY, '夜')[1]):
            with self.assertRaisesRegex(learning.LearningError, '班次已结束'):
                self.service.dispatch('paper.claim', payload, actor, {})

    def test_night_claim_rejects_wrong_roster_scope_person_and_invalid_window(self):
        self.roster[0]['shift'] = '夜'
        actor = {'id': 'personnel_r1', 'person_id': 'canonical', 'can_answer': True,
                 'shared_account': False, 'scope': '', 'is_admin': False}
        base = {'person_id': 'canonical', 'scope': 'A', 'date': DAY, 'shift': '夜'}
        for changed in ({'scope': 'B'}, {'person_id': 'other'}, {'shift': '白'}, {'shift': 'invalid'},
                        {'date': '2026-10-12'}, {'date': '20261010'}, {'date': DAY, 'shift': ''}):
            with self.subTest(changed=changed), patch.object(learning, 'now', return_value=self.at(0, day=11)):
                with self.assertRaises(learning.LearningError):
                    self.service.dispatch('paper.claim', {**base, **changed}, actor, {})
        self.roster[0]['scope'] = 'B'
        with patch.object(learning, 'now', return_value=self.at(0, day=11)):
            with self.assertRaisesRegex(learning.LearningError, '本人当前值班'):
                self.service.dispatch('paper.claim', base, actor, {})
        self.assertFalse(self.service._all('paper'))

    def test_cold_secondary_service_cannot_cancel_or_delay_restored_workers_period(self):
        engagement.repeated_reminders(self.service, self.at(15))
        before = self.service._all('engagement')
        cold = learning.LearningService(self.root, self.cloud, self.sender,
            get_login_people=lambda: self.hr, get_shift_roster=self.reader, get_portal_url=self.service._portal_url)
        engagement.send_due(cold, self.at(15))
        self.assertEqual(cold._all('engagement'), before)
        self.assertFalse(self.sender.calls)
        self.send(self.at(15))
        self.assertEqual(len(self.sender.calls), 1)

    def test_night_claim_rechecks_shift_end_after_roster_lookup(self):
        from lan_bitable_template_portal import learning_reminders as reminders
        self.roster[0]['shift'] = '夜'
        deadline = windows(DAY, '夜')[1]
        current = [deadline - dt.timedelta(seconds=1)]
        lookup = reminders.shift_learners
        def delayed_lookup(*args):
            result = lookup(*args)
            current[0] = deadline
            return result
        actor = {'id': 'personnel_r1', 'person_id': 'canonical', 'can_answer': True,
                 'shared_account': False, 'scope': '', 'is_admin': False}
        with patch.object(learning, 'now', side_effect=lambda: current[0]), \
             patch.object(reminders, 'shift_learners', side_effect=delayed_lookup):
            with self.assertRaisesRegex(learning.LearningError, '班次已结束'):
                self.service.dispatch('paper.claim', {'date': DAY, 'shift': '夜'}, actor, {})
        self.assertFalse(self.service._all('paper'))


if __name__ == '__main__':
    unittest.main()

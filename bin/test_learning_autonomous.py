"""Offline regression tests for personal autonomous papers and daily coexistence."""
import csv
import datetime as dt
import io
import sqlite3
import threading
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

from bin import test_learning as base
from bin import test_learning_self_service as support
from lan_bitable_template_portal import learning

# The autonomous harness freezes threading.Thread.start to forbid worker threads.
# The concurrency test temporarily restores the genuine implementation, which we
# capture at import time (i.e. before the per-test setUp patch is installed).
_REAL_THREAD_START = threading.Thread.start


class AutonomousLearningTests(unittest.TestCase):
    # Reuse the exact offline harness + helpers from the existing self-service
    # tests without inheriting the whole LearningTests class.
    setUp = support.SelfServiceTests.setUp
    seed = support.SelfServiceTests.seed
    dispatch = support.SelfServiceTests.dispatch
    question = support.SelfServiceTests.question
    actor = support.SelfServiceTests.actor
    seed_service = support.SelfServiceTests.seed_service
    restart = base.LearningTests.restart
    self_actor = base.LearningTests.self_actor

    # ------------------------------------------------------------------ helpers
    def _seed_demo(self, written=0, duty=0, professional=0, supplemental=0,
                   people=None):
        """Seed four banks + the two persons, then mark the service restored.

        No publication is created here: practice claims explicitly do not need
        one, while the daily tests call Publish() themselves.
        """
        self.seed(self.question(i, 'written', 'single') for i in range(written))
        self.seed(self.question(i, 'duty', 'interview') for i in range(duty))
        self.seed(self.question(i, 'professional', 'interview') for i in range(professional))
        self.seed(self.question(i, 'supplemental', 'single') for i in range(supplemental))
        people = people or [
            support._person('p1', '人员1', 'A', 'ou_p1'),
            support._person('p2', '人员2', 'B', 'ou_p2'),
        ]
        with self.service.transaction() as conn:
            for row in people:
                self.service._put('person', row['id'], row, conn)
        self.service._restored = True

    def _claim_practice(self, actor, operation_id, person_id=None):
        return self.dispatch('paper.claim', {
            'mode': 'practice',
            'operation_id': operation_id,
            'person_id': person_id or actor['person_id'],
        }, actor, {})

    def _claim_practice_on(self, actor, operation_id, offset, person_id=None):
        """Claim a practice round as seen from CURRENT + offset days."""
        dt_day = base.CURRENT + dt.timedelta(days=offset)
        with patch('lan_bitable_template_portal.learning.now', return_value=dt_day):
            return self._claim_practice(actor, operation_id, person_id)

    def _usage_count(self):
        conn = self.service._connect()
        try:
            return conn.execute('SELECT COUNT(*) FROM learning_usage').fetchone()[0]
        finally:
            conn.close()

    def _paper_families(self, paper):
        """Family ids from the stored paper (public paper omits family_id)."""
        stored = self.service._get('paper', paper['id'])
        return {q['family_id'] for q in stored['questions']}

    # ------------------------------------------------------------------ rules
    def test_practice_round_fifteen_distinct_and_same_op_reuses_paper(self):
        # Enough inventory for two disjoint practice rounds by one person.
        self._seed_demo(written=20, duty=3, professional=3, supplemental=12)
        p1 = self.actor(id='oid-p1', person_id='p1', can_answer=True)

        first = self._claim_practice(p1, 'op1')
        self.assertEqual(first['mode'], 'practice')
        self.assertEqual(len(first['questions']), 15)
        self.assertEqual(Counter(q['bank'] for q in first['questions']),
                         {'written': 8, 'duty': 1, 'professional': 1, 'supplemental': 5})
        first_families = self._paper_families(first)
        self.assertEqual(len(first_families), 15, 'same round must never repeat a family')

        # Same operation id + same person + same day reuses the same paper.
        retry = self._claim_practice(p1, 'op1')
        self.assertEqual(retry['id'], first['id'])

        # A different operation creates a new, disjoint practice paper.
        second = self._claim_practice(p1, 'op2')
        self.assertNotEqual(second['id'], first['id'])
        second_families = self._paper_families(second)
        self.assertEqual(len(second_families), 15)
        self.assertTrue(first_families.isdisjoint(second_families),
                        'new practice round must exclude the previous 7-day usage')

    def test_different_people_can_fully_overlap_quota_inventory(self):
        # Exactly the daily quota per bank in the whole inventory (8+1+1+5).
        self._seed_demo(written=8, duty=1, professional=1, supplemental=5)
        p1 = self.actor(id='oid-p1', person_id='p1', can_answer=True)
        p2 = self.actor(id='oid-p2', person_id='p2', can_answer=True)

        paper_a = self._claim_practice(p1, 'op-p1')
        paper_b = self._claim_practice(p2, 'op-p2')

        qa = {q['id'] for q in paper_a['questions']}
        qb = {q['id'] for q in paper_b['questions']}
        self.assertEqual(len(qa), 15)
        self.assertEqual(qa, qb, 'other people may legitimately repeat the same set')

    def test_seven_day_daily_and_practice_mutual_exclusion(self):
        self._seed_demo(written=20, duty=3, professional=3, supplemental=12)
        self.service.publish(base.DAY)
        p1 = self.actor(id='oid-p1', person_id='p1', can_answer=True)

        daily = self.dispatch('paper.claim', {'person_id': 'p1'}, p1)
        daily_families = self._paper_families(daily)
        self.assertEqual(len(daily_families), 15)

        practice = self._claim_practice(p1, 'op-mix')
        practice_families = self._paper_families(practice)
        self.assertEqual(len(practice_families), 15)
        self.assertTrue(daily_families.isdisjoint(practice_families),
                        'practice must not reuse any daily family within the rolling 7 days')

        # The daily unique index still keeps exactly one daily paper per day.
        again = self.dispatch('paper.claim', {'person_id': 'p1'}, p1)
        self.assertEqual(again['id'], daily['id'])
        practice_ids = [p['id'] for p in self.service._all('paper')
                        if p.get('mode') == 'practice' and p['person_id'] == 'p1']
        self.assertEqual(len(practice_ids), 1)

    def test_insufficient_dedup_returns_400_and_writes_nothing(self):
        # 8+1+1+4 = only 14 usable families for p1 -> shortage in supplemental.
        self._seed_demo(written=8, duty=1, professional=1, supplemental=4)
        p1 = self.actor(id='oid-p1', person_id='p1', can_answer=True)

        with self.assertRaises(learning.LearningError) as ctx:
            self._claim_practice(p1, 'op-insuff')
        self.assertEqual(ctx.exception.status, 400)
        self.assertIn('不足15道', str(ctx.exception))
        # Nothing durable is written: no paper, no learning usage, no sync marker.
        self.assertEqual(self.service._all('paper'), [])
        self.assertEqual(self._usage_count(), 0)
        self.assertIsNone(self.service._get('local', 'self_practice_sync'))

    # ------------------------------------------------- rate/fixture lifecycle
    def test_restore_new_machine_same_op_reuse_and_daily_continues(self):
        self._seed_demo(written=20, duty=3, professional=3, supplemental=12)
        self.service.publish(base.DAY)
        p1 = self.actor(id='oid-p1', person_id='p1', can_answer=True)

        daily = self.dispatch('paper.claim', {'person_id': 'p1'}, p1)
        practice = self._claim_practice(p1, 'op-restore')
        q0 = practice['questions'][0]
        self.dispatch('paper.answer', {
            'id': practice['id'], 'person_id': 'p1', 'question_id': q0['id'],
            'option_ids': ['o0'], 'operation_id': 'a-restore'}, p1)

        # Upload everything (including the seeded person rows) to the shared fake
        # cloud through the existing sync path so a new machine can resolve identity.
        self.service.sync_pending(force=True)

        # A fresh machine using the same cloud.
        node2_root = Path(self.temp.name) / 'node2'
        svc2 = learning.LearningService(node2_root, self.cloud, self.sender)
        svc2.restore()
        self.assertTrue(svc2._restored)

        self.assertIsNotNone(svc2._get('paper', daily['id']))
        self.assertIsNotNone(svc2._get('paper', practice['id']))
        self.assertIsNotNone(svc2._get('record', practice['id']))

        # Same operation id on the fresh machine reuses the restored practice paper.
        restored_daily = svc2.dispatch('paper.claim', {'person_id': 'p1'}, p1, {})
        restored_same_op = svc2.dispatch('paper.claim', {
            'mode': 'practice', 'operation_id': 'op-restore', 'person_id': 'p1'}, p1, {})

        self.assertEqual(restored_daily['id'], daily['id'])
        self.assertEqual(restored_same_op['id'], practice['id'])
        # Answer op was restored and stays a single first attempt (idempotent).
        entries = svc2._get('record', practice['id'])['entries']
        self.assertEqual(entries[q0['id']]['attempt']['operation_id'], 'a-restore')
        self.assertEqual(entries[q0['id']].get('practice') or [], [])
        # Idempotent resubmission on the fresh machine returns the paper untouched.
        retried = svc2.dispatch('paper.answer', {
            'id': practice['id'], 'person_id': 'p1', 'question_id': q0['id'],
            'option_ids': ['o0'], 'operation_id': 'a-restore'}, p1, {})
        self.assertEqual(retried['id'], practice['id'])
        self.assertEqual(len(svc2._get('record', practice['id'])['entries'][q0['id']].get('practice') or []), 0)

    # -------------------------------------------------------------- auth/perms
    def test_admin_cannot_claim_for_other_and_duty_cannot_submit_practice(self):
        self._seed_demo(written=20, duty=3, professional=3, supplemental=12)
        admin = self.actor(id='admin', is_admin=True, person_id='p1', can_answer=True)
        own = self._claim_practice(admin, 'op-admin')
        self.assertEqual(own['person_id'], 'p1')

        with self.assertRaises(learning.LearningError) as c1:
            self._claim_practice(admin, 'op-admin-other', person_id='p2')
        self.assertEqual(c1.exception.status, 403)

        duty = self.actor(id='duty-A', shared_account=True, scope='A')
        with self.assertRaises(learning.LearningError) as c2:
            self._claim_practice(duty, 'op-duty')
        self.assertEqual(c2.exception.status, 403, 'duty accounts are read-only')

    # ------------------------------------------------------------ views/stats
    def test_list_papers_today_only_daily_mode_practice_only_history_both(self):
        self._seed_demo(written=20, duty=3, professional=3, supplemental=12)
        self.service.publish(base.DAY)
        p1 = self.actor(id='oid-p1', person_id='p1', can_answer=True)

        daily = self.dispatch('paper.claim', {'person_id': 'p1'}, p1)
        self._claim_practice(p1, 'op-list')

        today = self.service.list_papers(p1, {'today': '1'})
        self.assertEqual(today['total'], 1)
        self.assertEqual(today['items'][0]['id'], daily['id'])
        self.assertEqual(today['items'][0]['mode'], 'daily')

        practice_only = self.service.list_papers(p1, {'mode': 'practice'})
        self.assertEqual(practice_only['total'], 1)
        self.assertEqual(practice_only['items'][0]['mode'], 'practice')

        history = self.service.list_papers(p1, {})
        self.assertEqual({item['mode'] for item in history['items']}, {'daily', 'practice'})

    def test_latest_practice_in_same_second_is_resumed(self):
        self._seed_demo(written=20, duty=3, professional=3, supplemental=12)
        actor = self.actor(id='oid-p1', person_id='p1', can_answer=True)
        papers = []
        for n in (1, 2):
            with patch.object(learning, 'now', return_value=base.CURRENT + dt.timedelta(microseconds=n)):
                papers.append(self._claim_practice(actor, 'latest-' + str(n)))
        self.assertLess(papers[0]['created_at'], papers[1]['created_at'])
        resumed = self.service.list_papers(actor, {'today': '1', 'mode': 'practice'})
        self.assertEqual(resumed['items'][0]['id'], papers[1]['id'])

    def test_today_summary_assigned_excludes_practice_but_portrait_counts_it(self):
        self._seed_demo(written=20, duty=3, professional=3, supplemental=12)
        self.service.publish(base.DAY)
        p1 = self.actor(id='oid-p1', person_id='p1', can_answer=True)

        daily = self.dispatch('paper.claim', {'person_id': 'p1'}, p1)
        practice = self._claim_practice(p1, 'op-prof')
        self.dispatch('paper.answer', {
            'id': daily['id'], 'person_id': 'p1', 'question_id': daily['questions'][0]['id'],
            'option_ids': ['o0'], 'operation_id': 'd-ans'}, p1)
        self.dispatch('paper.answer', {
            'id': practice['id'], 'person_id': 'p1', 'question_id': practice['questions'][0]['id'],
            'option_ids': ['o0'], 'operation_id': 'p-ans'}, p1)

        prof = self.service.profile(p1, {})
        # today_summary.assigned only counts the daily paper's questions.
        self.assertEqual(prof['today_summary']['assigned'], 15)
        # The personal portrait counts self-study answers too.
        self.assertEqual(prof['summary']['answered'], 2)
        self.assertEqual(prof['summary']['attempt_count'], 2)

    # ------------------------------------------------------------- sync/notice
    def test_practice_sync_is_silent_without_notifications_when_auto_publish_off(self):
        self._seed_demo(written=20, duty=3, professional=3, supplemental=12)
        p1 = self.actor(id='oid-p1', person_id='p1', can_answer=True)
        paper = self._claim_practice(p1, 'op-sync')
        self.dispatch('paper.answer', {
            'id': paper['id'], 'person_id': 'p1', 'question_id': paper['questions'][0]['id'],
            'option_ids': ['o0'], 'operation_id': 'sync-ans'}, p1)

        self.assertFalse(self.service.settings()['enabled'], 'auto publish is off by default')
        # The existing tick path keeps manual mode alive because a self-practice
        # sync marker was recorded, then syncs the practice paper + record.
        self.service.tick()

        self.assertIn(('paper', paper['id']), self.cloud.entities)
        self.assertIn(('record', paper['id']), self.cloud.entities)
        self.assertFalse(self.sender.calls, 'practice must never emit a notification')
        self.assertEqual(self.service._all('notification'), [])
        self.assertEqual((self.service._get('local', 'self_practice_sync') or {}).get('enabled'), True)

    # ------------------------------------------------------ schema migration
    def test_schema_upgrade_preserves_old_daily_records_and_allows_practice(self):
        self._seed_demo(written=20, duty=3, professional=3, supplemental=12)
        self.service.publish(base.DAY)
        p1 = self.actor(id='oid-p1', person_id='p1', can_answer=True)
        daily = self.dispatch('paper.claim', {'person_id': 'p1'}, p1)

        # Simulate the pre-practice schema: user_version 2 with the old unique
        # index that did NOT exclude practice papers.
        db = self.root / 'learning.sqlite3'
        conn = sqlite3.connect(db)
        try:
            conn.execute('PRAGMA user_version=2')
            conn.execute('DROP INDEX IF EXISTS learning_paper_identity')
            conn.execute("CREATE UNIQUE INDEX learning_paper_identity "
                         "ON documents(person_id,day) WHERE kind='paper' AND person_id<>''")
            conn.commit()
        finally:
            conn.close()

        # Re-opening runs the v3 migration: drop old index, recreate with the
        # practice-excluding predicate. The existing daily record must survive.
        self.restart()
        self.assertIsNotNone(self.service._get('paper', daily['id']),
                             'old daily paper must survive the unique-index upgrade')

        self.service._restored = True
        practice = self._claim_practice(p1, 'op-upgrade')
        self.assertEqual(practice['mode'], 'practice')
        self.assertNotEqual(practice['id'], daily['id'])

        # Both may now coexist for the same person+day under the new index.
        self.assertEqual(self.service.dispatch('paper.claim', {'person_id': 'p1'}, p1, {})['id'],
                         daily['id'])

    # ---------------------------------------------- rolling 7-day precedence
    def test_rolling_six_day_practice_exclusion_and_seventh_day_expiry(self):
        # Inventory sized so day0 consumes 15 families, day+6 needs the remaining
        # 15, and on day+7 only the day0 set is still un-excluded within the
        # rolling window (forcing exact reuse and proving seventh-day expiry).
        self._seed_demo(written=16, duty=2, professional=2, supplemental=10)
        p1 = self.actor(id='oid-p1', person_id='p1', can_answer=True)

        p0 = self._claim_practice_on(p1, 'op-day0', offset=0)
        f0 = self._paper_families(p0)
        self.assertEqual(len(f0), 15)

        # day+6 is still inside the rolling 7-day window (day0..day+6) so every
        # family used on day0 must be excluded.
        p6 = self._claim_practice_on(p1, 'op-day6', offset=6)
        f6 = self._paper_families(p6)
        self.assertEqual(len(f6), 15)
        self.assertTrue(f0.isdisjoint(f6),
                        'day+6 must still exclude day0 practice families')

        # day+7 falls outside the window (day+1..day+7); the day0 set becomes
        # available again and the exact-quota inventory forces a full reuse.
        p7 = self._claim_practice_on(p1, 'op-day7', offset=7)
        self.assertEqual(self._paper_families(p7), f0,
                         'day+7 must make day0 practice families usable again')

    def test_practice_first_then_daily_mutual_exclusion(self):
        self._seed_demo(written=20, duty=3, professional=3, supplemental=12)
        self.service.publish(base.DAY)
        p1 = self.actor(id='oid-p1', person_id='p1', can_answer=True)

        practice = self._claim_practice(p1, 'op-first')
        practice_families = self._paper_families(practice)
        self.assertEqual(len(practice_families), 15)

        daily = self.dispatch('paper.claim', {'person_id': 'p1'}, p1)
        daily_families = self._paper_families(daily)
        self.assertEqual(len(daily_families), 15)
        self.assertTrue(daily_families.isdisjoint(practice_families),
                        'daily must not reuse practice families within the rolling 7 days')

    # ------------------------------------------ restore + silent persistence
    def test_restore_then_new_answer_with_auto_publish_off_persists_silently(self):
        self._seed_demo(written=20, duty=3, professional=3, supplemental=12)
        p1 = self.actor(id='oid-p1', person_id='p1', can_answer=True)
        paper = self._claim_practice(p1, 'op-restore-ans')
        self.service.sync_pending(force=True)

        # Fresh machine restores the practice paper from the shared fake cloud.
        svc2 = learning.LearningService(
            Path(self.temp.name) / 'node-ans', self.cloud, self.sender)
        svc2.restore()
        self.assertTrue(svc2._restored)

        # Auto-publish is off by default; submit a brand-new answer on the fresh
        # machine, then let tick persist it through the manual-mode sync path.
        self.assertFalse(svc2.settings()['enabled'])
        q0 = paper['questions'][0]
        version = (svc2._get('record', paper['id']) or {}).get('version', 0)
        svc2.dispatch('paper.answer', {
            'id': paper['id'], 'person_id': 'p1', 'question_id': q0['id'],
            'version': version, 'option_ids': ['o0'], 'operation_id': 'r2-new-answer'}, p1, {})
        self.assertEqual((svc2._get('local', 'self_practice_sync') or {}).get('enabled'),
                         True, 'a practice mutation must set the local sync marker')

        svc2.tick()

        stored = self.cloud.entities.get(('record', paper['id']))
        self.assertIsNotNone(stored, 'the new answer must be synced to the cloud')
        self.assertEqual(stored['entries'][q0['id']]['attempt']['operation_id'],
                         'r2-new-answer')
        self.assertFalse(self.sender.calls, 'practice must never emit a notification')
        self.assertEqual(svc2._all('notification'), [])
        self.assertEqual((svc2._get('local', 'self_practice_sync') or {}).get('enabled'), True)

    # ------------------------------------------------- concurrency/atomicity
    def test_concurrent_same_operation_claims_yield_one_paper_and_one_round(self):
        self._seed_demo(written=20, duty=3, professional=3, supplemental=12)
        p1 = self.actor(id='oid-p1', person_id='p1', can_answer=True)
        svc2 = learning.LearningService(self.root, self.cloud, self.sender)
        svc2._restored = True

        barrier = threading.Barrier(2)
        ids, errors, guard = [], [], threading.Lock()

        def worker(service):
            barrier.wait()
            try:
                paper = service.dispatch('paper.claim', {
                    'mode': 'practice', 'operation_id': 'op-concurrent',
                    'person_id': 'p1'}, p1, {})
            except Exception as exc:
                with guard:
                    errors.append(repr(exc))
                return
            with guard:
                ids.append(paper['id'])

        with patch('threading.Thread.start', new=_REAL_THREAD_START):
            threads = [threading.Thread(target=worker, args=(svc,))
                       for svc in (self.service, svc2)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        self.assertEqual(errors, [], errors)
        self.assertTrue(ids, 'both workers must complete')
        self.assertEqual(ids, [ids[0]] * 2,
                         'concurrent same-op claims must resolve to one paper id')
        practice_papers = [p for p in self.service._all('paper')
                           if p.get('mode') == 'practice' and p.get('person_id') == 'p1']
        self.assertEqual(len(practice_papers), 1,
                         'concurrent same-op claim must not create duplicate papers')
        self.assertEqual(self._usage_count(), 15,
                         'one claimed round indexes exactly 15 usage rows')

    # ------------------------------------------------------------ export CSV
    def test_export_csv_final_column_labels_practice_vs_daily(self):
        self._seed_demo(written=20, duty=3, professional=3, supplemental=12)
        self.service.publish(base.DAY)
        p1 = self.actor(id='oid-p1', person_id='p1', can_answer=True)
        self.dispatch('paper.claim', {'person_id': 'p1'}, p1)
        self._claim_practice(p1, 'op-export')

        admin = self.actor(id='admin', is_admin=True, person_id='p1', can_answer=True)
        data, _, _ = self.service.export('results', {'scope': 'A'}, admin)
        rows = list(csv.reader(io.StringIO(data.decode('utf-8-sig'))))

        before = ['日期', '楼栋', '姓名', '工号', '人员标识', '题库', '题目',
                  '首次正确', '使用提示', '自评', '提交时间', '无效题', '更正说明', '操作账号']
        self.assertEqual(rows[0][:len(before)], before,
                         'existing CSV columns must keep their order')
        self.assertEqual(rows[0][-1], '题单类型',
                         'the final CSV column must label the paper type')

        labels = [row[-1] for row in rows[1:] if len(row) >= len(before) + 1]
        self.assertEqual(set(labels), {'自主练习', '每日题单'},
                         'both daily and practice rows must be distinguishable')
        self.assertEqual(labels.count('自主练习'), 15)
        self.assertEqual(labels.count('每日题单'), 15)


if __name__ == '__main__':
    unittest.main()

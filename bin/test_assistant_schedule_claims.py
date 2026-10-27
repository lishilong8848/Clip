"""Offline persistence regressions: no scheduler, model, application or delivery."""
import copy
import datetime as dt
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
import socket
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal import assistant_schedules as schedules
from lan_bitable_template_portal.state_store import LanPortalStateStore


BASE = schedules.local_time('2026-10-27T10:00:00+08:00').timestamp()
ACTOR = {'id': 'ou_claimfixture', 'name': '测试人员', 'channel': ''}
CONVERSATION = 'a' * 32


def iso(offset):
    return dt.datetime.fromtimestamp(BASE + offset, schedules.TZ).isoformat(timespec='seconds')


class ClaimTests(unittest.TestCase):
    def setUp(self):
        directory = self.enterContext(tempfile.TemporaryDirectory(prefix='assistant_claims_'))
        self.path = Path(directory) / 'state.sqlite3'
        self.tasks = schedules.ScheduledTasks(LanPortalStateStore(self.path))
        self.now = BASE
        self.enterContext(patch.object(schedules.time, 'time', side_effect=lambda: self.now))
        self.enterContext(patch.object(socket.socket, 'connect', side_effect=AssertionError('network forbidden')))
        self.context = {'request': SimpleNamespace(state=SimpleNamespace(), headers={}), 'actor': ACTOR}
        self.sequence = 0

    def create(self, *, offset=60, **changes):
        self.sequence += 1
        spec = {'title': '领取回归', 'instruction': '核对任务。', 'kind': 'reminder',
                'start_at': iso(offset), 'repeat': 'once', **changes}
        return self.tasks.dispatch(self.context, ACTOR, {'action': 'create', 'conversation_id': CONVERSATION,
            'operation_id': f'create_fixture_{self.sequence}', 'spec': spec}, BASE)['task']

    def job(self, job):
        return self.tasks.store.get_document(schedules.NAMESPACE, 'run:' + job['id'])

    def task(self, task):
        return self.tasks.store.get_document(schedules.NAMESPACE, 'task:' + task['id'])

    def change(self, task, action, **payload):
        self.sequence += 1
        return self.tasks.dispatch(self.context, ACTOR, {'action': action, 'id': task['id'],
            'revision': task['revision'], 'conversation_id': CONVERSATION,
            'operation_id': f'change_fixture_{self.sequence}', **payload}, self.now)['task']

    def claim(self, *, offset=60, worker='process-one'):
        self.now = BASE + offset
        return self.tasks.claim(worker)

    def assert_stale(self, task, job, token):
        saved_task, saved_job = self.task(task), self.job(job)
        with self.assertRaises(schedules.AssistantError) as raised:
            self.tasks.checkpoint(job, token, reply={'text': 'stale overwrite'})
        self.assertEqual(raised.exception.status, 409)
        self.assertIsNone(self.tasks.finish(job, token, error='stale failure', permanent=True))
        self.assertEqual(self.task(task), saved_task)
        self.assertEqual(self.job(job), saved_job)

    def test_process_name_is_not_a_claim_credential(self):
        task = self.create()
        _, job = self.claim()
        self.assertRegex(job['claim_token'], r'^[a-f0-9]{32}$')
        self.assertEqual(job['lease'], 'process-one')
        self.assertNotEqual(job['claim_token'], job['lease'])
        for wrong in ('process-one', '', None, 'f' * 32):
            with self.subTest(token=wrong):
                self.assert_stale(task, job, wrong)

    def test_heartbeat_renews_only_a_live_claim(self):
        self.create()
        _, job = self.claim()
        token = job['claim_token']
        self.now += 30
        self.tasks.checkpoint(job, token, phase='ready', reply={'text': 'durable'})
        self.assertEqual(job['claim_token'], token)
        self.assertEqual(job['lease_until'], self.now + schedules.LEASE_SECONDS)
        self.assertEqual(self.job(job)['reply'], {'text': 'durable'})
        self.assertIsNone(self.tasks.claim('other', job['lease_until'] - 1))

    def test_expired_claim_cannot_renew_or_finish_without_a_takeover(self):
        task = self.create()
        _, job = self.claim()
        self.now = job['lease_until']
        self.assert_stale(task, job, job['claim_token'])

    def test_same_process_takeover_fences_old_checkpoint_and_finish(self):
        task = self.create()
        _, old = self.claim()
        old_token = old['claim_token']
        self.now = old['lease_until']
        _, new = self.tasks.claim('process-one')
        self.assertEqual(new['id'], old['id'])
        self.assertNotEqual(new['claim_token'], old_token)
        self.tasks.checkpoint(new, new['claim_token'], reply={'text': 'new claimant'})
        self.assert_stale(task, old, old_token)
        self.tasks.finish(new, new['claim_token'])
        self.assertEqual(self.task(task)['status'], 'completed')
        self.assertEqual(self.job(new)['reply'], {'text': 'new claimant'})

    def test_four_independent_stores_claim_once_and_take_over_once(self):
        task = self.create()
        instances = [schedules.ScheduledTasks(LanPortalStateStore(self.path)) for _ in range(4)]
        self.now = BASE + 60
        def compete():
            barrier = threading.Barrier(4, timeout=10)
            def take(index):
                barrier.wait()
                return instances[index].claim('same-process')
            with ThreadPoolExecutor(max_workers=4) as pool:
                return [result for result in pool.map(take, range(4)) if result is not None]
        first = compete()
        self.assertEqual(len(first), 1)
        old = first[0][1]
        self.now = old['lease_until']
        second = compete()
        self.assertEqual(len(second), 1)
        new = second[0][1]
        self.assertEqual(new['id'], old['id'])
        self.assertNotEqual(new['claim_token'], old['claim_token'])
        self.assert_stale(task, old, old['claim_token'])
        self.tasks.finish(new, new['claim_token'])
        self.assertIsNone(self.tasks.claim('again'))

    def test_retry_after_partial_delivery_preserves_receipts_and_rotates_token(self):
        self.create()
        _, old = self.claim()
        reply = {'text': 'saved answer', 'sent_parts': ['answer:0'],
                 'files': [{'id': 'f' * 32, 'file_key': 'fixture-only'}]}
        self.tasks.checkpoint(old, old['claim_token'], reply=reply, phase='ready', delivery_started=self.now)
        self.tasks.finish(old, old['claim_token'], error='temporary failure')
        saved = self.job(old)
        self.assertEqual(saved['status'], 'pending')
        self.assertEqual(saved['claim_token'], '')
        self.assertIsNone(self.tasks.claim('restart', saved['retry_at'] - 1))
        self.now = saved['retry_at']
        _, new = schedules.ScheduledTasks(LanPortalStateStore(self.path)).claim('restart')
        self.assertNotEqual(new['claim_token'], old['claim_token'])
        self.assertEqual(new['id'], old['id'])
        self.assertEqual(new['reply'], reply)
        self.assertEqual(new['delivery_started'], BASE + 60)

    def test_legacy_run_without_token_requires_fresh_claim(self):
        task = self.create()
        _, old = self.claim()
        saved = self.job(old)
        del saved['claim_token']
        with self.tasks.transaction() as conn:
            self.tasks.write(conn, 'run:' + saved['id'], saved)
        self.assert_stale(task, old, 'process-one')
        self.now = saved['lease_until']
        _, new = self.tasks.claim('restart')
        self.assertRegex(new['claim_token'], r'^[a-f0-9]{32}$')
        self.tasks.checkpoint(new, new['claim_token'], phase='ready')

    def test_finished_claim_cannot_be_reopened_by_a_late_heartbeat(self):
        task = self.create(repeat='interval', interval_minutes=1)
        _, job = self.claim()
        self.tasks.finish(job, job['claim_token'])
        self.assertEqual(self.task(task)['status'], 'active')
        self.assertEqual(self.job(job)['claim_token'], '')
        self.assert_stale(task, job, job['claim_token'])

    def test_checkpoint_cannot_replace_ownership_or_terminal_state(self):
        self.create()
        _, job = self.claim()
        before = self.job(job)
        for field in ('id', 'task_id', 'revision', 'due_at', 'status', 'lease', 'claim_token', 'lease_until', 'retry_at'):
            with self.subTest(field=field), self.assertRaises(schedules.AssistantError):
                self.tasks.checkpoint(job, job['claim_token'], **{field: 'forged'})
        self.assertEqual(self.job(job), before)

    def test_pause_cancel_update_and_resume_fence_old_claims(self):
        for action in ('pause', 'cancel', 'update'):
            with self.subTest(action=action):
                task = self.create()
                _, old = self.claim()
                extra = {'spec': {**task['spec'], 'start_at': iso(300)}} if action == 'update' else {}
                changed = self.change(task, action, **extra)
                self.assert_stale(task, old, old['claim_token'])
                if action == 'pause':
                    self.change(changed, 'resume')
                    _, new = self.tasks.claim('process-one')
                    self.assertNotEqual(new['claim_token'], old['claim_token'])
                    self.assertEqual(new['id'], old['id'])
                    self.assert_stale(task, old, old['claim_token'])
                    self.tasks.finish(new, new['claim_token'])

    def test_ninth_due_task_is_not_starved_by_eight_active_runs(self):
        for pending in (False, True):
            with self.subTest(pending=pending):
                tasks = schedules.ScheduledTasks(LanPortalStateStore(self.path.with_name(f'nine-{pending}.sqlite3')))
                self.tasks = tasks
                for _ in range(8):
                    self.create(repeat='interval', interval_minutes=1)
                ninth = self.create(offset=130)
                self.now = BASE + 60
                jobs = [tasks.claim('worker')[1] for _ in range(8)]
                self.now = BASE + 119
                for job in jobs:
                    if pending:
                        tasks.finish(job, job['claim_token'], error='outage')
                    else:
                        tasks.checkpoint(job, job['claim_token'])
                self.now = BASE + 130
                claimed = tasks.claim('available-worker')
                self.assertIsNotNone(claimed)
                self.assertEqual(claimed[0]['id'], ninth['id'])

    def test_new_revision_not_blocked_by_old_live_run(self):
        task = self.create(repeat='interval', interval_minutes=1)
        _, old = self.claim()
        changed = self.change(task, 'update', spec={**task['spec'], 'start_at': iso(90)})
        new_task, new = self.claim(offset=90)
        self.assertEqual(new_task['id'], changed['id'])
        self.assertEqual(new['revision'], 2)
        self.assertNotEqual(new['id'], old['id'])
        self.assert_stale(task, old, old['claim_token'])

    def test_new_claim_after_deadline_expires_without_creating_a_run(self):
        task = self.create(repeat='daily', until=iso(80))
        self.assertIsNone(self.claim(offset=81))
        saved = self.task(task)
        self.assertEqual((saved['status'], saved['last_status'], saved['next_at']), ('completed', 'expired', None))
        self.assertNotIn('active_run', saved)

    def test_pending_retry_and_expired_lease_do_not_run_after_deadline(self):
        for pending in (False, True):
            with self.subTest(pending=pending):
                task = self.create(repeat='daily', until=iso(80))
                _, job = self.claim()
                reply = {'text': 'keep audit trail', 'sent_parts': ['answer:0']}
                self.tasks.checkpoint(job, job['claim_token'], reply=reply)
                if pending:
                    self.tasks.finish(job, job['claim_token'], error='outage')
                self.now = BASE + 181
                self.assertIsNone(self.tasks.claim('restart'))
                saved = self.job(job)
                self.assertEqual(saved['status'], 'expired')
                self.assertEqual(saved['claim_token'], '')
                self.assertEqual(saved['reply'], reply)
                self.assertEqual(self.task(task)['last_status'], 'expired')

    def test_deadline_crossing_blocks_checkpoint_and_finish_marks_expired(self):
        task = self.create(repeat='daily', until=iso(80))
        _, job = self.claim()
        self.now = BASE + 81
        with self.assertRaises(schedules.AssistantError) as raised:
            self.tasks.checkpoint(job, job['claim_token'], phase='sending')
        self.assertEqual(raised.exception.status, 409)
        self.tasks.finish(job, job['claim_token'], error='expired', permanent=True)
        self.assertEqual(self.job(job)['status'], 'expired')
        self.assertEqual(self.task(task)['status'], 'completed')

    def test_deadline_is_inclusive_but_lease_boundary_is_exclusive(self):
        task = self.create(until=iso(60))
        _, job = self.claim()
        self.tasks.checkpoint(job, job['claim_token'])
        self.tasks.finish(job, job['claim_token'])
        self.assertEqual(self.task(task)['last_status'], 'completed')

    def test_failed_persistence_rolls_back_claim_and_checkpoint(self):
        task = self.create()
        self.now = BASE + 60
        original = self.tasks.write
        def fail_task(conn, key, value):
            original(conn, key, value)
            if key.startswith('task:'):
                raise OSError('fixture disk failure')
        with patch.object(self.tasks, 'write', side_effect=fail_task), self.assertRaises(OSError):
            self.tasks.claim('worker')
        self.assertNotIn('active_run', self.task(task))
        _, job = self.tasks.claim('worker')
        before = copy.deepcopy(job)
        def fail_write(conn, key, value):
            original(conn, key, value)
            raise OSError('fixture checkpoint failure')
        with patch.object(self.tasks, 'write', side_effect=fail_write), self.assertRaises(OSError):
            self.tasks.checkpoint(job, job['claim_token'], reply={'text': 'not durable'})
        self.assertEqual(job, before)
        self.assertEqual(self.job(job), before)

    def test_failed_transaction_exit_does_not_advance_in_memory_checkpoint(self):
        self.create()
        _, job = self.claim()
        before = copy.deepcopy(job)
        transaction = self.tasks.transaction
        @contextmanager
        def fail_at_commit():
            with transaction() as conn:
                yield conn
                raise OSError('fixture commit failure')
        with patch.object(self.tasks, 'transaction', fail_at_commit), self.assertRaises(OSError):
            self.tasks.checkpoint(job, job['claim_token'], reply={'text': 'not committed'})
        self.assertEqual(job, before)
        self.assertEqual(self.job(job), before)


if __name__ == '__main__':
    unittest.main()

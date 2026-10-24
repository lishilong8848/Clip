"""Offline diagnostic persistence tests; every database is temporary."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service.diagnostic_store import DiagnosticStore, LeaseLostError, batch_digest


class DiagnosticStoreTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.now = 1_000.0
        clock = patch('openclaw_service.diagnostic_store.time.time', side_effect=lambda: self.now)
        clock.start()
        self.addCleanup(clock.stop)
        self.store = DiagnosticStore(self.root)
        self.event = {'module': 'assistant', 'stage': 'answer', 'version': '1', 'run_id': 'a' * 8,
                      'category': 'network', 'error_type': 'OSError', 'http_status': 503,
                      'frames': ['model.py:answer:12'], 'native_reason': 'timeout',
                      'native_fingerprint': 'b' * 64}

    def counts(self):
        with closing(sqlite3.connect(self.store.db_path)) as conn, conn:
            return tuple(conn.execute('SELECT COUNT(*) FROM ' + name).fetchone()[0]
                         for name in ('batches', 'events'))

    def due_batch(self, *, lease_seconds=300):
        identity = self.store.append(self.event)
        self.now += 60
        batch = self.store.claim(lease_seconds=lease_seconds)
        self.assertEqual(batch['id'], identity)
        return batch

    def test_idle_has_no_batches_or_events_and_no_busy_polling(self):
        self.assertIsNone(self.store.claim())
        self.assertEqual(self.store.next_delay(), 60.0)
        self.assertEqual(self.counts(), (0, 0))
        self.assertIsNone(self.store.get_batch('missing'))
        with closing(sqlite3.connect(self.store.db_path)) as conn, conn:
            self.assertEqual(conn.execute('SELECT key FROM meta').fetchall(), [('machine_id',)])

    def test_sixty_seconds_boundary_and_flat_payload(self):
        identity = self.store.append(self.event)
        self.assertEqual(self.store.next_delay(now=1_030), 30)
        self.assertIsNone(self.store.claim(now=1_059.999))
        self.assertAlmostEqual(self.store.next_delay(now=1_059.999), .001)
        self.assertEqual(self.store.next_delay(now=1_060), 0)
        self.now = 1_060
        batch = self.store.claim(lease_seconds=1800)
        self.assertEqual((batch['id'], batch['started_at'], batch['ended_at'], batch['error_count']),
                         (identity, 1_000, 1_060, 1))
        self.assertEqual(batch['events'], [{**self.event, 'count': 1, 'first_at': 1_000, 'last_at': 1_000}])
        self.assertEqual((batch['attempts'], batch['lease_until']), (1, 2_860))
        self.assertEqual(self.store.next_delay(), 60)
        self.assertEqual(self.store.next_delay(now=2_859), 1)
        payload = {key: batch[key] for key in ('id', 'machine_id', 'started_at', 'ended_at', 'error_count', 'events')}
        digest = hashlib.sha256(json.dumps(payload, ensure_ascii=True, sort_keys=True,
                                           separators=(',', ':'), allow_nan=False).encode()).hexdigest()
        self.assertEqual(batch['payload_hash'], digest)
        self.assertEqual(batch_digest(batch), digest)
        self.assertEqual(batch_digest({**batch, 'attempts': 10, 'file_token': 'changed', 'lease_token': 'changed'}), digest)
        for field, value in (('id', 'other'), ('machine_id', 'other'), ('started_at', 0),
                             ('ended_at', 0), ('error_count', 2), ('events', [])):
            self.assertNotEqual(batch_digest({**batch, field: value}), digest)

    def test_duplicate_aggregation_ignores_only_top_level_occurrence_times(self):
        first = self.store.append({**self.event, 'timestamp': 'untrusted', 'at': float('nan')})
        second = self.store.append({**dict(reversed(list(self.event.items()))), 'occurred_at': -1}, now=1_005)
        self.assertEqual(first, second)
        self.store.append({**self.event, 'run_id': 'c' * 8}, now=1_006)
        self.store.append({**self.event, 'detail': {'at': 1}}, now=1_007)
        self.store.append({**self.event, 'detail': {'at': 2}}, now=1_008)
        self.store.append({**self.event, 'frames': ['model.py:answer:13']}, now=1_009)
        batch = self.store.claim(now=1_060)
        self.assertEqual(batch['error_count'], 6)
        self.assertEqual(len(batch['events']), 5)
        self.assertEqual(batch['events'][0], {**self.event, 'count': 2, 'first_at': 1_000, 'last_at': 1_005})

    def test_append_rolls_expired_open_batch_without_waiting_for_uploader(self):
        first = self.store.append(self.event)
        second = self.store.append(self.event, now=1_060)
        self.assertNotEqual(first, second)
        old = self.store.get_batch(first)
        self.assertEqual((old['state'], old['error_count'], old['ended_at']), ('pending', 1, 1_060))
        self.assertEqual(self.store.claim(now=1_061)['id'], first)
        self.assertIsNone(self.store.claim(now=1_119))
        self.assertEqual(self.store.claim(now=1_120)['id'], second)
        self.assertEqual(self.store.get_batch(first)['payload_hash'], old['payload_hash'])

    def test_frozen_batch_is_immutable_while_new_events_open_another(self):
        first = self.due_batch()
        second = self.store.append(self.event)
        self.assertNotEqual(first['id'], second)
        self.assertEqual(self.store.get_batch(first['id']), first)
        self.assertEqual(self.store.get_batch(second)['state'], 'open')

    def test_restart_keeps_machine_open_deadline_batch_hash_lease_and_checkpoint(self):
        identity = self.store.append(self.event)
        restarted = DiagnosticStore(self.root)
        self.assertEqual(restarted.machine_id, self.store.machine_id)
        self.assertTrue(identity.startswith(self.store.machine_id + ':'))
        self.assertEqual(restarted.next_delay(now=1_030), 30)
        self.now = 1_060
        batch = restarted.claim()
        restarted.checkpoint(batch['id'], batch['lease_token'], file_token='file_one', auth_fingerprint='auth_one')
        again = DiagnosticStore(self.root)
        saved = again.get_batch(identity)
        for field in ('id', 'machine_id', 'payload_hash', 'lease_token', 'attempts', 'events'):
            self.assertEqual(saved[field], batch[field])
        self.assertEqual((saved['file_token'], saved['auth_fingerprint']), ('file_one', 'auth_one'))
        self.assertIsNone(again.claim())
        again.complete(identity, batch['lease_token'], 'rec_original')
        done = DiagnosticStore(self.root).get_batch(identity)
        self.assertEqual((done['state'], done['record_id'], done['file_token']), ('done', 'rec_original', 'file_one'))
        self.assertEqual(again.next_delay(), 60)
        self.assertIsNone(again.claim(now=1_000_000))

    def test_concurrent_instances_append_without_lost_counts_and_claim_once(self):
        root = self.root / 'concurrent'
        with ThreadPoolExecutor(2) as pool:
            stores = list(pool.map(lambda _: DiagnosticStore(root), range(2)))
        self.assertEqual(stores[0].machine_id, stores[1].machine_id)
        barrier = threading.Barrier(2)
        def append(store):
            barrier.wait(timeout=5)
            return [store.append(self.event, now=1_000) for _ in range(30)]
        with ThreadPoolExecutor(2) as pool:
            identities = list(pool.map(append, stores))
        self.assertEqual(len(set(identities[0] + identities[1])), 1)
        def claim(store):
            barrier.wait(timeout=5)
            return store.claim(now=1_060)
        with ThreadPoolExecutor(2) as pool:
            claimed = [batch for batch in pool.map(claim, stores) if batch is not None]
        self.assertEqual(len(claimed), 1)
        self.assertEqual((claimed[0]['error_count'], claimed[0]['events'][0]['count']), (60, 60))

    def test_retry_precedes_due_open_and_preserves_payload_and_attachment(self):
        batch = self.due_batch()
        self.store.checkpoint(batch['id'], batch['lease_token'], file_token='file_one', auth_fingerprint='auth_one')
        self.store.fail(batch['id'], batch['lease_token'], 'NETWORK')
        self.now += 1
        new = self.store.append(self.event)
        self.assertIsNone(self.store.claim(now=1_119))
        self.assertEqual(self.store.next_delay(now=1_119), 1)
        self.now = 1_121
        retry = self.store.claim()
        self.assertEqual(retry['id'], batch['id'])
        self.assertEqual(retry['payload_hash'], batch['payload_hash'])
        self.assertEqual((retry['attempts'], retry['file_token'], retry['auth_fingerprint']), (2, 'file_one', 'auth_one'))
        self.assertNotEqual(retry['lease_token'], batch['lease_token'])
        self.assertEqual(self.store.claim()['id'], new)

    def test_backoff_grows_from_sixty_to_nine_hundred_without_deleting(self):
        batch = self.due_batch()
        for delay in (60, 120, 240, 480, 900, 900):
            with self.subTest(delay=delay, attempts=batch['attempts']):
                self.store.fail(batch['id'], batch['lease_token'], 'NETWORK', now=self.now)
                saved = self.store.get_batch(batch['id'])
                self.assertEqual((saved['state'], saved['next_at'], saved['last_error']),
                                 ('pending', self.now + delay, 'NETWORK'))
                self.assertEqual(self.store.next_delay(), 60)
                self.assertIsNone(self.store.claim(now=self.now + delay - .001))
                self.now += delay
                batch = self.store.claim()
                self.assertEqual(batch['payload_hash'], saved['payload_hash'])
        self.assertEqual(self.counts(), (1, 1))

    def test_expired_lease_is_fenced_before_and_after_reclaim(self):
        batch = self.due_batch(lease_seconds=10)
        self.now += 10
        def stale_writes():
            with self.assertRaises(LeaseLostError):
                self.store.checkpoint(batch['id'], batch['lease_token'], file_token='old', auth_fingerprint='old')
            with self.assertRaises(LeaseLostError):
                self.store.complete(batch['id'], batch['lease_token'], 'rec_stale')
            with self.assertRaises(LeaseLostError):
                self.store.fail(batch['id'], batch['lease_token'], 'STALE')
        stale_writes()
        self.assertEqual(self.store.next_delay(), 0)
        other = DiagnosticStore(self.root)
        retry = other.claim()
        self.assertEqual((retry['id'], retry['payload_hash']), (batch['id'], batch['payload_hash']))
        self.assertNotEqual(retry['lease_token'], batch['lease_token'])
        other.checkpoint(retry['id'], retry['lease_token'], file_token='new', auth_fingerprint='new')
        stale_writes()
        other.complete(retry['id'], retry['lease_token'], 'rec_current')
        stale_writes()
        saved = self.store.get_batch(batch['id'])
        self.assertEqual((saved['record_id'], saved['file_token'], saved['auth_fingerprint']),
                         ('rec_current', 'new', 'new'))

    def test_checkpoint_can_invalidate_attachment_on_auth_rotation_but_requires_live_lease(self):
        batch = self.due_batch()
        self.store.checkpoint(batch['id'], batch['lease_token'], file_token='file', auth_fingerprint='auth')
        self.store.checkpoint(batch['id'], batch['lease_token'], file_token='', auth_fingerprint='auth_new')
        saved = self.store.get_batch(batch['id'])
        self.assertEqual((saved['file_token'], saved['auth_fingerprint']), ('', 'auth_new'))
        self.store.checkpoint(batch['id'], batch['lease_token'], file_token='', auth_fingerprint='')
        self.assertEqual(self.store.get_batch(batch['id'])['file_token'], '')
        self.store.fail(batch['id'], batch['lease_token'], 'AUTH_CHANGED')
        with self.assertRaises(LeaseLostError):
            self.store.checkpoint(batch['id'], batch['lease_token'], file_token='file', auth_fingerprint='auth')

    def test_invalid_input_is_rejected_and_never_becomes_success(self):
        for event in (None, [], {'count': 2}, {'value': float('nan')}, {'value': float('inf')}, {'value': object()}):
            with self.subTest(event_type=type(event).__name__), self.assertRaises((ValueError, TypeError)):
                self.store.append(event)
        for value in (float('nan'), float('inf'), -1, True, '1000'):
            with self.assertRaises(ValueError):
                self.store.append(self.event, now=value)
        self.assertEqual(self.counts(), (0, 0))
        batch = self.due_batch()
        for code in ('', 'request with user text', 'x\nsecret', 'x' * 81):
            with self.assertRaises(ValueError):
                self.store.fail(batch['id'], batch['lease_token'], code)
        with self.assertRaises(ValueError):
            self.store.complete(batch['id'], batch['lease_token'], '')
        self.assertEqual(self.store.get_batch(batch['id'])['state'], 'leased')

    def test_sqlite_failure_rolls_back_batch_and_event_and_surfaces(self):
        with closing(sqlite3.connect(self.store.db_path)) as conn, conn:
            conn.execute("""CREATE TRIGGER fail_event AFTER INSERT ON events
                            BEGIN SELECT RAISE(ABORT, 'injected disk failure'); END""")
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'injected disk failure'):
            self.store.append(self.event)
        self.assertEqual(self.counts(), (0, 0))
        with closing(sqlite3.connect(self.store.db_path)) as conn, conn:
            conn.execute('DROP TRIGGER fail_event')
        identity = self.store.append(self.event)
        self.assertEqual(self.store.get_batch(identity)['error_count'], 1)
        with patch('openclaw_service.diagnostic_store.sqlite3.connect', side_effect=sqlite3.OperationalError('disk unavailable')):
            with self.assertRaises(sqlite3.OperationalError):
                self.store.claim(now=1_060)
        self.assertEqual(self.store.get_batch(identity)['state'], 'open')

    def test_append_lock_timeout_is_bounded_and_does_not_create_partial_batch(self):
        with closing(sqlite3.connect(self.store.db_path, isolation_level=None)) as conn:
            conn.execute('BEGIN IMMEDIATE')
            started = time.monotonic()
            with self.assertRaises(sqlite3.OperationalError):
                self.store.append(self.event)
            self.assertLess(time.monotonic() - started, 1.5)
            conn.rollback()
        self.assertEqual(self.counts(), (0, 0))


if __name__ == '__main__':
    unittest.main()

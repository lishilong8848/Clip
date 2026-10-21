"""Isolated person/shift snapshot regression; never sends real messages."""
import copy
import datetime as dt
import threading
import unittest
from tempfile import TemporaryDirectory
from unittest.mock import patch

from bin import test_learning as support
from lan_bitable_template_portal import learning as core, learning_shift_storage as storage

DATE = '2026-10-20'
CURRENT = dt.datetime.fromisoformat(DATE + 'T15:00:00+08:00')


class Cloud:
    shift_storage_enabled = True

    def current_app_id(self):
        return 'cli_test'

    def __init__(self):
        self.entities, self.calls = {}, []
        self.fail = False
        self.on_write = None

    def upsert_entity(self, kind, key, payload, operation_id, **kwargs):
        self.calls.append((kind, key, copy.deepcopy(payload), kwargs))
        self.entities[kind, key] = copy.deepcopy(payload)
        if self.on_write:
            callback, self.on_write = self.on_write, None
            callback()
        if self.fail:
            raise OSError('synthetic uncertain write')
        return 'recSynthetic'


class ShiftStorageTests(unittest.TestCase):
    def setUp(self):
        self.temp = self.enterContext(TemporaryDirectory())
        self.clock = self.enterContext(patch.object(core, 'now', return_value=CURRENT))
        self.cloud = Cloud()
        self.directory = {'one': {'login_ids': ['ou_one']}, 'two': {'login_ids': ['ou_two']}}
        self.service = core.LearningService(self.temp, cloud=self.cloud,
            get_login_people=self.login_directory)
        for rid in self.directory:
            self.put('person', {'id': 'directory:' + rid, 'name': rid, 'scopes': ['A'],
                               'login_ids': self.directory[rid]['login_ids'], 'active': True}, dirty=False)

    def login_directory(self):
        return {'people': self.directory, 'schema': 4, 'app_id': 'cli_test'}

    def put(self, kind, value, dirty=True):
        with self.service.transaction() as conn:
            self.service._put(kind, value['id'], value, conn, dirty)
        return value

    def paper(self, identity='paper', pid='directory:one', shift='白', date=DATE, mode='daily'):
        question = {'id': 'q1', 'family_id': 'family', 'type': 'single', 'bank': 'written', 'stem': 'Test'}
        return self.put('paper', {'id': identity, 'person_id': pid, 'person': self.service._get('person', pid),
            'date': date, 'scope': 'A', 'shift': shift, 'mode': mode, 'questions': [question],
            'created_at': date + ('T20:00:00+08:00' if shift == '夜' else 'T13:00:00+08:00')})

    def record(self, paper='paper', pid='directory:one', date=DATE, practice=()):
        return self.put('record', {'id': paper, 'person_id': pid, 'date': date, 'scope': 'A',
            'entries': {'q1': {'attempt': {'correct': True, 'submitted_at': date + 'T14:00:00+08:00',
                'answer_view_snapshot': {'version': 1, 'answer_before_submit': False}}, 'practice': list(practice)}}})

    def roster(self):
        from lan_bitable_template_portal.learning_reminders import ROSTER_VERSION
        self.put('local', {'id': 'shift_roster:' + DATE, 'date': DATE, 'version': ROSTER_VERSION,
                          'people': [{'person_id': 'directory:one', 'scope': 'A', 'shift': s} for s in ('白', '夜')]}, False)

    def bundles(self):
        return {k: v for (kind, k), v in self.cloud.entities.items() if kind == 'shift_person'}

    def test_same_person_same_shift_updates_one_row_without_losing_snapshots(self):
        self.paper(); self.record()
        self.put('notification', {'id': 'notice', 'open_id': 'ou_one', 'date': DATE, 'shift': '白', 'status': 'sent'})
        self.assertEqual(self.service.sync_pending()['pending_errors'], 0)
        self.assertEqual(len(self.bundles()), 1)
        bundle = next(iter(self.bundles().values()))
        self.assertEqual({e['kind'] for e in bundle['entities']}, {'paper', 'record', 'notification'})
        self.assertEqual((bundle['summary']['assigned'], bundle['summary']['answered']), (1, 1))
        self.assertEqual(self.cloud.calls[-1][3]['display_fields']['人员'], [{'id': 'ou_one'}])
        calls = len(self.cloud.calls)
        self.service.sync_pending()
        self.assertEqual(len(self.cloud.calls), calls)
        self.record(practice=[{'correct': False, 'submitted_at': DATE + 'T15:00:00+08:00'}])
        self.service.sync_pending()
        self.assertEqual(len(self.bundles()), 1)
        self.assertEqual(len(next(v for v in self.bundles().values() if v['shift'] == '白')['entities']), 3)

    def test_people_and_shifts_and_dates_are_separate(self):
        self.paper()
        self.paper('second', 'directory:two')
        self.put('notification', {'id': 'night', 'person_id': 'directory:one', 'date': DATE, 'shift': '夜'})
        self.paper('tomorrow', date='2026-10-21')
        self.service.sync_pending()
        self.assertEqual({(v['date'], v['shift'], v['person_id']) for v in self.bundles().values()}, {
            (DATE, '白', 'directory:one'), (DATE, '白', 'directory:two'),
            (DATE, '夜', 'directory:one'), ('2026-10-21', '白', 'directory:one')})

    def test_repeated_quizzes_and_review_same_shift_remain_one_row(self):
        self.roster()
        self.paper(); self.record()
        self.paper('extra', mode='practice')
        self.record('extra', practice=[{'correct': False, 'submitted_at': DATE + 'T15:00:00+08:00'}])
        self.service.sync_pending()
        self.assertEqual(len(self.bundles()), 1)
        value = next(iter(self.bundles().values()))
        self.assertEqual((value['summary']['practice_questions'], value['summary']['review_count']), (1, 1))
        self.assertEqual(len(value['entities']), 4)

    def test_night_after_midnight_stays_with_roster_day(self):
        self.roster(); self.paper(shift='夜')
        self.record(practice=[{'correct': False, 'submitted_at': '2026-10-21T03:00:00+08:00'}])
        self.service.sync_pending()
        self.assertEqual(len(self.bundles()), 1)
        value = next(iter(self.bundles().values()))
        self.assertEqual((value['date'], value['shift'], value['summary']['review_count']), (DATE, '夜', 1))

    def test_unknown_commit_retries_same_identity_and_keeps_dirty(self):
        self.paper(); self.cloud.fail = True
        self.assertEqual(self.service.sync_pending()['pending_errors'], 1)
        self.assertTrue(self.service._get('paper', 'paper')['_dirty'])
        self.cloud.fail = False
        self.service.sync_pending(force=True)
        self.assertEqual(len(self.bundles()), 1)
        self.assertFalse(self.service._get('paper', 'paper')['_dirty'])

    def test_edit_during_write_is_not_acknowledged_or_overwritten(self):
        paper = self.paper()
        self.cloud.on_write = lambda: self.put('paper', {**paper, 'started_at': DATE + 'T14:00:00+08:00'})
        self.service.sync_pending()
        self.assertTrue(self.service._get('paper', 'paper')['_dirty'])
        self.service.sync_pending()
        self.assertFalse(self.service._get('paper', 'paper')['_dirty'])
        self.assertEqual(len(self.bundles()), 1)
        self.assertIn('started_at', next(iter(self.bundles().values()))['entities'][0]['payload'])

    def test_restart_reuses_ack_and_preserves_global_records(self):
        self.paper()
        self.put('publication', {'id': DATE, 'date': DATE, 'reserve_ids': [], 'paper_ids': ['paper']})
        self.service.sync_pending()
        count = len(self.cloud.calls)
        restored = core.LearningService(self.temp, cloud=self.cloud, get_login_people=self.login_directory)
        restored.sync_pending()
        self.assertEqual(len(self.cloud.calls), count)
        self.assertIn(('publication', DATE), self.cloud.entities)

    def test_unresolved_or_duplicate_identity_does_not_guess_or_fall_back(self):
        self.put('person', {'id': 'directory:two', 'name': 'one', 'login_ids': ['ou_one'], 'scopes': ['A']}, False)
        self.put('notification', {'id': 'notice', 'open_id': 'ou_one', 'date': DATE, 'shift': '白'})
        self.assertEqual(self.service.sync_pending()['pending_errors'], 1)
        self.assertEqual(self.cloud.calls, [])
        self.assertTrue(self.service._get('notification', 'notice')['_dirty'])

    def test_current_directory_identity_is_used_not_old_app_id(self):
        self.paper()
        self.directory['one']['login_ids'] = ['ou_newapp']
        self.service.sync_pending()
        self.assertEqual(self.cloud.calls[-1][3]['display_fields']['人员'], [{'id': 'ou_newapp'}])
        self.directory['two']['login_ids'] = ['ou_newapp']
        self.service.sync_pending()
        self.assertEqual(self.cloud.calls[-1][3]['display_fields']['人员'], [])

    def test_old_app_directory_cannot_supply_person_field_or_enrich_history(self):
        self.paper()
        self.service._login_people_reader = lambda: {**self.login_directory(), 'app_id': 'cli_previous'}
        self.service.sync_pending()
        self.assertNotIn('人员', self.cloud.calls[-1][3]['display_fields'])
        self.assertEqual(self.cloud.calls[-1][3]['expected_app_id'], 'cli_test')
        self.cloud.list_entity_records = lambda: self.fail('old-app directory must not start historical enrichment')
        storage.backfill(self.service)

    def test_clean_history_is_not_copied_into_new_rows(self):
        paper = self.paper()
        self.put('paper', paper, False)
        self.service.sync_pending()
        self.assertEqual(self.cloud.calls, [])

    def test_first_write_identity_is_frozen_even_if_roster_changes_after_timeout(self):
        self.roster()
        self.paper(shift='白')
        self.cloud.fail = True
        self.service.sync_pending()
        self.put('local', {'id': 'shift_roster:' + DATE, 'date': DATE,
                          'people': [{'person_id': 'directory:one', 'scope': 'A', 'shift': '夜'}]}, False)
        self.cloud.fail = False
        self.service.sync_pending(force=True)
        self.assertEqual(len(self.bundles()), 1)
        self.assertEqual(next(iter(self.bundles().values()))['shift'], '白')

    def test_frozen_record_rejects_a_changed_person_without_overwriting(self):
        paper = self.paper()
        self.service.sync_pending()
        count = len(self.cloud.calls)
        self.put('paper', {**paper, 'person_id': 'directory:two'})
        self.assertEqual(self.service.sync_pending()['pending_errors'], 1)
        self.assertEqual(len(self.cloud.calls), count)
        self.assertTrue(self.service._get('paper', 'paper')['_dirty'])

    def test_two_shifts_in_daily_projection_get_two_person_rows_not_one_class_row(self):
        self.roster()
        from lan_bitable_template_portal import learning_completion as completion
        for value in completion.project(self.service, DATE, current=CURRENT):
            self.put('daily_progress', value)
        self.service.sync_pending()
        self.service.sync_pending()
        self.assertEqual({(v['shift'], v['person_id']) for v in self.bundles().values()},
                         {('白', 'directory:one'), ('夜', 'directory:one')})
        self.assertEqual(sum(e['kind'] == 'daily_progress' for v in self.bundles().values() for e in v['entities']), 1)

    def test_historical_enrichment_resumes_without_new_or_deleted_records(self):
        paper = self.paper()
        records = [{'record_id': 'recOld', 'kind': 'paper', 'key': 'paper', 'payload': paper, 'payload_sha256': 'hash'},
                   {'record_id': 'recSystem', 'kind': 'settings', 'key': 'main', 'payload': {'enabled': True}, 'payload_sha256': 'settings'}]
        self.cloud.list_entity_records = lambda: records
        patched = []
        self.cloud.enrich_entity_record = lambda *args, **kwargs: patched.append((args, kwargs))
        storage.backfill(self.service, limit=1)
        self.assertEqual(len(patched), 1)
        restored = core.LearningService(self.temp, cloud=self.cloud, get_login_people=self.login_directory)
        storage.backfill(restored, limit=1)
        storage.backfill(restored, limit=1)
        self.assertEqual({p[0][0] for p in patched}, {'recOld', 'recSystem'})
        self.assertEqual(len(patched), 2)
        self.assertEqual(self.cloud.calls, [])
        self.assertTrue(restored._get('local', 'shift_history_fields')['complete'])

    def test_backfill_conflict_reloads_only_unfinished_records(self):
        records = [{'record_id': rid, 'kind': 'settings', 'key': rid,
                    'payload': {'enabled': True}, 'payload_sha256': 'first'} for rid in ('recA', 'recB')]
        self.cloud.list_entity_records = lambda: copy.deepcopy(records)
        done = []
        class Conflict(Exception):
            conflict = True
        def enrich(rid, kind, key, fields, **kwargs):
            if rid == 'recB' and kwargs['expected_payload_sha256'] == 'first':
                raise Conflict('synthetic changed record')
            done.append(rid)
        self.cloud.enrich_entity_record = enrich
        with self.assertRaises(Conflict):
            storage.backfill(self.service)
        self.assertEqual(done, ['recA'])
        records[1]['payload_sha256'] = 'second'
        with patch.object(storage.time, 'time', return_value=10**12):
            storage.backfill(self.service)
        self.assertEqual(done, ['recA', 'recB'])
        self.assertTrue(self.service._get('local', 'shift_history_fields')['complete'])

    def test_missing_backfill_record_does_not_block_remaining_jobs(self):
        records = [{'record_id': rid, 'kind': 'settings', 'key': rid,
                    'payload': {'enabled': True}, 'payload_sha256': 'hash'} for rid in ('recA', 'recZ')]
        self.cloud.list_entity_records = lambda: copy.deepcopy(records)
        done = []
        def enrich(rid, *args, **kwargs):
            if rid == 'recA':
                raise OSError('deleted externally')
            done.append(rid)
        self.cloud.enrich_entity_record = enrich
        with self.assertRaises(OSError):
            storage.backfill(self.service)
        records.pop(0)
        with patch.object(storage.time, 'time', return_value=10**12):
            storage.backfill(self.service)
        self.assertEqual(done, ['recZ'])
        self.assertEqual(self.service._get('local', 'shift_history_fields')['missing_ids'], ['recA'])
        self.assertTrue(self.service._get('local', 'shift_history_fields')['complete'])

    def test_restore_rejects_different_local_owner_atomically(self):
        paper = self.paper()
        self.cloud.load_entities = lambda: [{'kind': 'paper', 'key': 'paper',
            'payload': {**paper, 'person_id': 'directory:two'}, 'storage_owner': [DATE, '白', 'directory:two']}]
        with self.assertRaisesRegex(core.LearningError, '归属.*冲突'):
            self.service.restore()
        self.assertEqual(self.service._get('paper', 'paper')['person_id'], 'directory:one')
        self.assertIsNone(self.service._get('local', 'shift_owner:paper:paper'))

    def test_new_record_cannot_recover_beside_another_persons_existing_paper(self):
        paper = self.paper()
        for existing_record in (False, True):
            with self.subTest(existing_record=existing_record):
                record = {'id': 'paper', 'person_id': 'directory:two', 'date': DATE, 'scope': 'A', 'entries': {}}
                if existing_record:
                    self.put('record', record)
                self.cloud.load_entities = lambda: [
                    {'kind': 'record', 'key': 'paper', 'payload': record, 'storage_owner': [DATE, '白', 'directory:two']},
                    {'kind': 'paper', 'key': 'paper', 'payload': {**paper, 'person_id': 'directory:two'}}]
                with self.assertRaisesRegex(core.LearningError, '关联题单归属冲突'):
                    self.service.restore()
                self.assertEqual(self.service._get('paper', 'paper')['person_id'], 'directory:one')
                self.assertIsNone(self.service._get('local', 'shift_owner:record:paper'))

    def test_frozen_record_still_checks_linked_paper_owner(self):
        paper = self.paper()
        self.put('paper', paper, False)
        self.record(pid='directory:two')
        self.put('local', {'id': 'shift_owner:record:paper', 'owner': [DATE, '白', 'directory:two']}, False)
        self.assertEqual(self.service.sync_pending()['pending_errors'], 1)
        self.assertEqual(self.cloud.calls, [])
        self.assertTrue(self.service._get('record', 'paper')['_dirty'])

    def test_restore_timeout_does_not_copy_old_paper_into_bundle(self):
        paper = self.paper()
        paper['started_at'] = DATE + 'T13:00:00+08:00'
        self.put('paper', paper, False)
        self.cloud.load_entities = lambda: [{'kind': 'paper', 'key': 'paper', 'payload': paper}]
        self.service.restore()
        self.service.sync_pending()
        self.assertTrue(self.service._get('local', 'shift_legacy:paper:paper')['legacy'])
        self.assertEqual(self.bundles(), {})
        self.assertEqual(self.service._get('paper', 'paper')['id'], 'paper')

    def test_two_sync_callers_never_create_parallel_writes(self):
        self.paper()
        started, release = threading.Event(), threading.Event()
        self.cloud.on_write = lambda: (started.set(), release.wait(5))
        first = threading.Thread(target=self.service.sync_pending)
        second = threading.Thread(target=self.service.sync_pending)
        first.start()
        self.assertTrue(started.wait(5))
        second.start(); release.set()
        first.join(5); second.join(5)
        self.assertFalse(first.is_alive() or second.is_alive())
        self.assertEqual(len(self.cloud.calls), 1)


class ShiftCloudIntegrationTests(unittest.TestCase):
    from bin import test_learning_shift_cloud as fixture
    new_remote = fixture.support.LearningCloudTests.new_remote
    put = ShiftStorageTests.put
    login_directory = ShiftStorageTests.login_directory

    def setUp(self):
        self.fixture.ShiftCloudTests.setUp(self)
        self.manager._credentials.return_value = ('cli_test', 'synthetic-secret')
        self.temp = self.enterContext(TemporaryDirectory())
        self.enterContext(patch.object(core, 'now', return_value=CURRENT))
        self.directory = {'one': {'login_ids': ['ou_one']}}
        self.service = core.LearningService(self.temp, cloud=self.remote,
            get_login_people=self.login_directory)

    def test_actual_adapter_sync_and_empty_database_restore(self):
        pid = 'directory:one'
        person = self.put('person', {'id': pid, 'name': '合成人员', 'scopes': ['A'], 'active': True,
                                    'login_ids': ['ou_one']})
        question = support.LearningTests.question(self, 1)
        self.put('paper', {'id': 'paper', 'person_id': pid, 'person': person, 'date': DATE,
                          'scope': 'A', 'shift': '白', 'questions': [question], 'created_at': DATE + 'T13:00:00+08:00'})
        self.put('record', {'id': 'paper', 'person_id': pid, 'date': DATE, 'scope': 'A', 'version': 1,
            'entries': {question['id']: {'attempt': {'correct': True, 'option_ids': ['o0'], 'missed': [], 'wrong': [],
                'submitted_at': DATE + 'T14:00:00+08:00'}, 'practice': []}}})
        self.put('notification', {'id': 'night', 'person_id': pid, 'date': DATE, 'shift': '夜', 'status': 'sent'})
        self.assertEqual(self.service.sync_pending()['pending_errors'], 0, self.service._last_error)
        rows = self.fake.records[self.table]
        self.assertEqual(sum(r['fields']['类别'] == '班次人员汇总' for r in rows), 2)
        with TemporaryDirectory() as directory:
            restored = core.LearningService(directory, cloud=self.remote,
                get_login_people=self.login_directory)
            restored.restore()
            self.assertEqual(restored._get('record', 'paper')['entries'], self.service._get('record', 'paper')['entries'])
            self.assertEqual(restored._get('notification', 'night')['status'], 'sent')
            self.assertEqual(restored._get('local', 'shift_owner:paper:paper')['owner'], [DATE, '白', pid])
            restored.sync_pending()
            self.assertEqual(len(self.fake.records[self.table]), len(rows))


if __name__ == '__main__':
    unittest.main()

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
        self.directory = {rid: {'name': rid, 'login_ids': ['ou_' + rid], 'selectable': True} for rid in ('one', 'two')}
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
        storage.backfill(self.service)
        self.assertEqual(done, ['recA'])
        self.assertEqual(storage.history_issues(self.service)[0]['record_id'], 'recB')
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
        storage.backfill(self.service)
        self.assertEqual(done, ['recZ'])
        records.pop(0)
        with patch.object(storage.time, 'time', return_value=10**12):
            storage.backfill(self.service)
        self.assertEqual(done, ['recZ'])
        self.assertEqual(self.service._get('local', 'shift_history_fields')['missing_ids'], ['recA'])
        self.assertTrue(self.service._get('local', 'shift_history_fields')['complete'])

    def historical(self, name='one', rid='recOld', pid='legacy-id'):
        return {'record_id': rid, 'kind': 'paper', 'key': rid, 'display_name': name,
                'payload': {'id': rid, 'person_id': pid, 'person': {'id': pid, 'name': name},
                            'date': DATE, 'shift': '白', 'scope': 'A', 'questions': []}, 'payload_sha256': rid}

    def history_run(self, records):
        self.cloud.list_entity_records = lambda: copy.deepcopy(records)
        writes = []
        self.cloud.enrich_entity_record = lambda *args, **kwargs: writes.append((args, kwargs))
        storage.backfill(self.service)
        return writes

    def test_history_exact_name_uses_new_app_identity_without_changing_business_id(self):
        record = self.historical('one')
        self.directory['one']['login_ids'] = ['ou_currentApp']
        writes = self.history_run([record])
        self.assertEqual(writes[0][0][3]['人员'], [{'id': 'ou_currentApp'}])
        self.assertEqual(writes[0][1]['expected_app_id'], 'cli_test')
        self.assertEqual(writes[0][1]['expected_display_name'], 'one')
        self.assertEqual(record['payload']['person_id'], 'legacy-id')
        self.assertFalse(storage.history_issues(self.service))
        self.assertEqual(self.cloud.calls, [])

    def test_history_duplicate_does_not_guess_by_building_or_block_unique_person(self):
        self.directory['duplicate'] = {'name': 'one', 'login_ids': ['ou_duplicate'], 'scopes': ['B']}
        writes = self.history_run([self.historical(), self.historical('two', 'recOther')])
        self.assertNotIn('人员', writes[0][0][3])
        self.assertEqual(writes[1][0][3]['人员'], [{'id': 'ou_two'}])
        issue = storage.history_issues(self.service)[0]
        self.assertIn('重名', issue['reason'])
        self.assertEqual(len(issue['candidates']), 2)
        self.assertNotIn('ou_', str(issue))
        self.assertEqual(self.service._get('local', 'shift_history_fields')['remaining'], 1)
        storage.backfill(self.service)
        self.assertEqual(len(writes), 2, 'No repeated remote writes for unresolved names')
        self.paper('new-normal')
        self.assertEqual(self.service.sync_pending()['pending_errors'], 0)
        self.assertEqual(len(self.bundles()), 1)

    def test_history_no_fuzzy_or_inactive_or_conflicting_or_shared_identity(self):
        for label, name, person, rows, expected in [
            ('missing', 'unknown', {}, self.directory, '未找到完全同名'),
            ('partial', 'on', {}, self.directory, '未找到完全同名'),
            ('case', 'One', {}, self.directory, '未找到完全同名'),
            ('no-name', '', {}, self.directory, '没有可核验'),
            ('changed', 'two', {'id': 'directory:one'}, self.directory, '不一致'),
            ('inactive', 'one', {}, {'one': {**self.directory['one'], 'inactive': True}}, '停用'),
            ('unselectable', 'one', {}, {'one': {**self.directory['one'], 'selectable': False}}, '待核对'),
            ('no-oid', 'one', {}, {'one': {'name': 'one', 'login_ids': []}}, 'openid'),
            ('shared-oid', 'one', {}, {**self.directory, 'extra': {'name': 'else', 'login_ids': ['ou_one']}}, 'openid'),
        ]:
            with self.subTest(label=label):
                oid, reason, _ = storage.resolve_history_identity({'required': True, 'person': person, 'names': [name] if name else []}, rows)
                self.assertEqual(oid, '')
                self.assertIn(expected, reason)

    def test_history_original_conflicting_names_and_operator_title_are_not_recipients(self):
        record = self.historical()
        record['payload']['person']['name'] = 'two'
        writes = self.history_run([record])
        self.assertNotIn('人员', writes[0][0][3])
        self.assertIn('姓名不一致', storage.history_issues(self.service)[0]['reason'])
        state = storage.Snapshot({})
        for kind in ('settings', 'question', 'publication', 'sharing_batch'):
            self.assertFalse(storage.history_identity(state, {'kind': kind, 'key': 'x',
                'payload': {'name': 'one', 'operator_name': 'one'}})['required'])

    def test_history_linked_paper_and_original_visible_name_can_resolve(self):
        record = self.historical()
        state = storage.Snapshot({('paper', record['key']): {'payload': record['payload']}})
        identity = storage.history_identity(state, {'kind': 'record', 'key': record['key'], 'payload': {'entries': {}}})
        self.assertEqual(storage.resolve_history_identity(identity, self.directory)[0], 'ou_one')
        record['payload']['person'] = {}
        self.assertEqual(self.history_run([record])[0][0][3]['人员'], [{'id': 'ou_one'}])

    def test_history_retry_after_restart_and_directory_repair_only_updates_pending_row(self):
        records = [self.historical('missing'), self.historical('two', 'recOther')]
        writes = self.history_run(records)
        restored = core.LearningService(self.temp, cloud=self.cloud, get_login_people=self.login_directory)
        self.directory['added'] = {'name': 'missing', 'login_ids': ['ou_added'], 'selectable': True}
        with patch.object(storage.time, 'time', return_value=10**12):
            storage.backfill(restored)
        self.assertEqual(len(writes), 3)
        self.assertEqual(writes[-1][0][0], 'recOld')
        self.assertEqual(writes[-1][0][3]['人员'], [{'id': 'ou_added'}])
        self.assertTrue(restored._get('local', 'shift_history_fields')['complete'])
        self.assertFalse(storage.history_issues(restored))

    def test_history_v1_completed_is_rechecked_after_upgrade(self):
        self.put('local', {'id': 'shift_history_fields', 'version': 1, 'complete': True}, False)
        writes = self.history_run([self.historical()])
        self.assertEqual(len(writes), 1)
        self.assertEqual(self.service._get('local', 'shift_history_fields')['version'], storage.HISTORY_VERSION)

    def test_unresolved_rows_never_starve_later_rows_even_across_retry_windows(self):
        records = [self.historical('missing', 'recA'), self.historical('two', 'recZ')]
        self.cloud.list_entity_records = lambda: copy.deepcopy(records)
        writes = []
        self.cloud.enrich_entity_record = lambda *args, **kwargs: writes.append(args)
        with patch.object(storage.time, 'time', return_value=1000):
            storage.backfill(self.service, limit=1)
        with patch.object(storage.time, 'time', return_value=1400):
            storage.backfill(self.service, limit=1)
        self.assertEqual([row[0] for row in writes], ['recA', 'recZ'])
        self.assertEqual(writes[-1][3]['人员'], [{'id': 'ou_two'}])

    def test_history_report_admin_only_and_export_escapes_formula(self):
        self.history_run([self.historical('=missing')])
        admin = {'id': 'admin', 'is_admin': True}
        ordinary = {'id': 'ordinary', 'person_id': 'directory:one', 'scope': 'A', 'can_answer': True}
        self.assertIn('history_issues', self.service.bootstrap('', admin)['sync'])
        self.assertNotIn('history_issues', self.service.bootstrap('A', ordinary)['sync'])
        self.assertNotIn('history_issues', self.service.sync_status())
        with self.assertRaises(core.LearningError) as caught:
            self.service.export('history_identity', {}, ordinary)
        self.assertEqual(caught.exception.status, 403)
        data, filename, mime = self.service.export('history_identity', {}, admin)
        self.assertIn("'=missing", data.decode('utf-8-sig'))
        self.assertIn('待核对', filename)
        self.assertIn('text/csv', mime)

    def test_legacy_dirty_sync_preserves_name_backfill_and_never_clears_unresolved_personnel(self):
        value = self.historical()['payload']
        self.put('paper', value)
        self.put('local', {'id': 'shift_legacy:paper:' + value['id'], 'legacy': True}, False)
        self.service.sync_pending()
        self.assertEqual(self.cloud.calls[-1][3]['display_fields']['人员'], [{'id': 'ou_one'}])
        self.directory['duplicate'] = {'name': 'one', 'login_ids': ['ou_duplicate']}
        self.put('paper', {**value, 'status': 'completed'})
        self.service.sync_pending()
        self.assertNotIn('人员', self.cloud.calls[-1][3]['display_fields'])
        self.assertEqual(self.cloud.calls[-1][2]['status'], 'completed')
        self.assertFalse(self.service._get('paper', value['id'])['_dirty'])

    def test_history_rejects_cross_person_link_even_with_identical_names(self):
        self.directory['two']['name'] = 'one'
        paper = self.historical('one', 'recPaper', 'directory:two')
        record = {'record_id': 'recAnswer', 'kind': 'record', 'key': 'recPaper', 'payload_sha256': 'hash',
                  'payload': {'id': 'recPaper', 'person_id': 'directory:one', 'entries': {}}}
        writes = self.history_run([paper, record])
        answer = next(args for args, _ in writes if args[0] == 'recAnswer')
        self.assertNotIn('人员', answer[3])
        self.assertIn('人员标识不一致', storage.history_issues(self.service)[0]['reason'])

    def test_history_rejects_conflicting_embedded_person_id(self):
        record = self.historical(pid='directory:one')
        record['payload']['person']['id'] = 'directory:two'
        writes = self.history_run([record])
        self.assertNotIn('人员', writes[0][0][3])
        self.assertIn('人员标识不一致', storage.history_issues(self.service)[0]['reason'])

    def test_history_numeric_date_does_not_stop_later_valid_record(self):
        bad = self.historical(rid='recBad')
        bad['payload']['date'] = 1792512000000
        writes = self.history_run([bad, self.historical('two', 'recGood')])
        self.assertEqual({args[0] for args, _ in writes}, {'recBad', 'recGood'})
        self.assertEqual(next(args for args, _ in writes if args[0] == 'recGood')[3]['人员'], [{'id': 'ou_two'}])

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

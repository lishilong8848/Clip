"""Offline personnel-only Bitable, restart and verified-cleanup regressions."""
import copy
import datetime as dt
import hashlib
import json
import unittest
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from bin import test_learning as support
from bin import test_learning_shift_cloud as shift_support
from lan_bitable_template_portal import learning as core, learning_cloud as cloud
from lan_bitable_template_portal import learning_personnel_storage as storage

DATE = '2026-10-20'
CURRENT = dt.datetime.fromisoformat(DATE + 'T15:00:00+08:00')


class PersonnelCloud(support.FakeCloud):
    shift_storage_enabled = True
    personnel_only_storage = True

    def __init__(self):
        super().__init__()
        self.app_id = 'cli_test'
        self.fields = {}
        self.deleted = []
        self.fail_delete_after_commit = False
        self.fail_delete_before_commit = False

    def current_app_id(self):
        return self.app_id

    def ensure_personnel_schema(self):
        pass

    def list_entity_records(self, *, include_fields=False):
        rows = []
        for (kind, key), payload in self.entities.items():
            fields = {'类别': cloud.ENTITY_KIND_LABELS[kind], '业务键': key,
                      '内容': json.dumps(payload, ensure_ascii=False), **self.fields.get((kind, key), {})}
            row = {'record_id': 'rec' + core.digest([kind, key])[:20], 'kind': kind, 'key': key,
                   'payload': copy.deepcopy(payload), 'payload_sha256': core.digest(payload),
                   'display_name': fields.get('姓名', '')}
            if include_fields:
                row['fields'] = copy.deepcopy(fields)
            rows.append(row)
        return rows

    def load_entities(self, records=None):
        return cloud.LearningCloud.load_entities(self, records)

    def upsert_entity(self, kind, key, payload, operation_id, **kwargs):
        self.fields[kind, key] = copy.deepcopy(kwargs.get('display_fields', {}))
        return super().upsert_entity(kind, key, payload, operation_id)

    def delete_archived_entity(self, row, **kwargs):
        if self.fail_delete_before_commit:
            raise OSError('synthetic delete failed')
        current = next(r for r in self.list_entity_records(include_fields=True) if r['record_id'] == row['record_id'])
        if current != row:
            raise cloud.LearningCloudConflict('synthetic changed row')
        self.deleted.append(row['record_id'])
        del self.entities[row['kind'], row['key']]
        if self.fail_delete_after_commit:
            raise cloud.LearningCloudWriteUncertain('synthetic lost delete acknowledgement')
        return {'deleted': True, 'record_id': row['record_id']}

    def enrich_entity_record(self, record_id, kind, key, fields, **kwargs):
        self.fields.setdefault((kind, key), {}).update(copy.deepcopy(fields))


class PersonnelOnlyTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(TemporaryDirectory()))
        self.enterContext(patch.object(core, 'now', return_value=CURRENT))
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('offline tests only')))
        self.cloud = PersonnelCloud()
        self.sender = support.FakeSender()
        self.directory = {'schema': 4, 'app_id': 'cli_test', 'people': {
            pid: {'name': pid, 'login_ids': ['ou_' + pid], 'selectable': True}
            for pid in ('one', 'two')}}
        self.service = self.restart()
        for pid in ('one', 'two'):
            self.put('person', 'directory:' + pid, {'id': 'directory:' + pid, 'name': pid, 'active': True,
                     'scopes': ['A'], 'login_ids': ['ou_' + pid]})

    def restart(self):
        self.service = core.LearningService(self.root, self.cloud, self.sender, get_login_people=lambda: self.directory)
        return self.service

    def put(self, kind, key, value, dirty=True):
        with self.service.transaction() as conn:
            self.service._put(kind, key, value, conn, dirty)
        return value

    def paper(self, key='paper', pid='directory:one', date=DATE, shift='白', mode='daily'):
        question = support.LearningTests.question(self, 1)
        return self.put('paper', key, {'id': key, 'person_id': pid,
            'person': self.service._get('person', pid), 'date': date, 'scope': 'A', 'shift': shift,
            'mode': mode, 'questions': [question], 'created_at': date + 'T13:00:00+08:00'})

    def answer(self, paper='paper', practice=()):
        return self.put('record', paper, {'id': paper, 'version': 1, 'person_id': 'directory:one', 'scope': 'A', 'date': DATE,
            'entries': {'written:1': {'attempt': {'option_ids': ['o0'], 'correct': True,
                'submitted_at': DATE + 'T14:00:00+08:00', 'answer_view_snapshot': {
                    'version': 1, 'answer_before_submit': False}}, 'practice': list(practice)}}})

    def summaries(self):
        return {key: value for (kind, key), value in self.cloud.entities.items() if kind == 'shift_person'}

    def test_only_personnel_rows_are_written_and_internal_state_stays_local(self):
        self.paper(); self.answer()
        for kind in ('settings', 'publication', 'audit', 'issue', 'sharing_batch'):
            self.put(kind, 'system', {'id': 'system', 'secret_fixture': kind})
        self.assertEqual(self.service.sync_pending()['pending_errors'], 0)
        self.assertEqual(len(self.summaries()), 1)
        summary = next(iter(self.summaries().values()))
        self.assertEqual(summary['entities'], [])
        self.assertNotIn('tasks', summary['summary'])
        self.assertEqual((summary['summary']['assigned'], summary['summary']['answered']), (1, 1))
        self.assertEqual({kind for kind, _ in self.cloud.entities}, {'shift_person'})
        for kind in ('settings', 'publication', 'audit', 'issue', 'sharing_batch'):
            self.assertEqual(self.service._get(kind, 'system')['secret_fixture'], kind)
            self.assertFalse(self.service._get(kind, 'system')['_dirty'])
        self.assertEqual(self.sender.calls, [])

    def test_each_person_date_and_shift_has_its_own_row(self):
        self.paper()
        self.paper('other-person', pid='directory:two')
        self.paper('tomorrow', date='2026-10-21')
        self.put('notification', 'night', {'id': 'night', 'person_id': 'directory:one', 'date': DATE, 'shift': '夜'})
        self.service.sync_pending()
        self.assertEqual({(s['date'], s['shift'], s['person_id']) for s in self.summaries().values()}, {
            (DATE, '白', 'directory:one'), (DATE, '白', 'directory:two'),
            (DATE, '夜', 'directory:one'), ('2026-10-21', '白', 'directory:one')})

    def test_repeated_quizzes_update_one_row_and_preserve_local_answers(self):
        self.paper(); self.answer()
        self.service.sync_pending()
        self.answer(practice=[{'option_ids': ['o1'], 'correct': False, 'submitted_at': DATE + 'T15:00:00+08:00'}])
        self.service.sync_pending()
        self.assertEqual(len(self.summaries()), 1)
        self.assertEqual(len(self.service._get('record', 'paper')['entries']['written:1']['practice']), 1)

    def test_delivery_failure_and_success_update_visible_status_without_changing_completion(self):
        self.paper()
        notice = {'id': 'notice', 'paper_id': 'paper', 'person_id': 'directory:one',
                  'date': DATE, 'shift': '白', 'status': 'failed', 'error': 'synthetic rejection'}
        self.put('notification', 'notice', notice)
        self.service.sync_pending()
        identity = next(iter(self.summaries()))
        self.assertEqual(self.cloud.fields['shift_person', identity]['题单发送状态'], '发送失败')
        self.assertEqual(self.cloud.fields['shift_person', identity]['已答题数'], 0)
        self.put('notification', 'notice', {**notice, 'status': 'sent', 'error': ''})
        self.service.sync_pending()
        self.assertEqual(self.cloud.fields['shift_person', identity]['题单发送状态'], '已发送')
        self.assertEqual(len(self.summaries()), 1)

    def test_lost_summary_acknowledgement_retries_same_row_without_resending_messages(self):
        self.paper()
        self.cloud.fail_after_commit = 'upsert_entity'
        self.assertEqual(self.service.sync_pending()['pending_errors'], 1)
        self.assertTrue(self.service._get('paper', 'paper')['_dirty'])
        self.assertEqual(self.service.sync_pending(force=True)['pending_errors'], 0)
        self.assertEqual(len(self.summaries()), 1)
        self.assertFalse(self.service._get('paper', 'paper')['_dirty'])
        self.assertEqual(self.sender.calls, [])

    def test_write_race_leaves_new_revision_pending(self):
        value = self.paper()
        self.cloud.on_upsert = lambda: self.put('paper', 'paper', {**value, 'started_at': DATE + 'T14:00:00+08:00'})
        self.service.sync_pending()
        self.assertTrue(self.service._get('paper', 'paper')['_dirty'])
        self.service.sync_pending()
        self.assertFalse(self.service._get('paper', 'paper')['_dirty'])

    def test_restart_keeps_local_config_answers_and_receipts_without_cloud_restore(self):
        self.paper(); self.answer()
        self.put('settings', 'main', {'enabled': True})
        self.put('notification', 'sent', {'id': 'sent', 'person_id': 'directory:one', 'date': DATE,
                 'shift': '白', 'status': 'sent', 'delivery': [{'message_id': 'om_fixture'}]})
        self.service.sync_pending()
        self.restart()
        with patch.object(self.cloud, 'load_entities', side_effect=AssertionError('local data must not depend on cloud internals')):
            self.service.restore()
        self.assertTrue(self.service.settings()['enabled'])
        self.assertEqual(self.service._get('notification', 'sent')['delivery'][0]['message_id'], 'om_fixture')
        self.assertIsNotNone(self.service._get('record', 'paper'))
        self.assertEqual(self.sender.calls, [])

    def test_legacy_records_are_backed_up_with_files_before_cleanup(self):
        paper = self.paper()
        self.cloud.entities['paper', 'paper'] = paper
        self.cloud.entities['settings', 'main'] = {'enabled': True}
        self.cloud.fields['settings', 'main'] = {'附件': [{'file_token': 'fixture_file'}]}
        self.cloud.files['fixture_file'] = b'original attachment'
        self.service.sync_pending()
        storage.cleanup(self.service)
        self.assertEqual({kind for kind, _ in self.cloud.entities}, {'paper', 'shift_person'})
        self.assertEqual(self.cloud.fields['paper', 'paper']['人员'], [{'id': 'ou_one'}])
        self.assertTrue((self.root / 'backups/learning-before-personnel-only.sqlite3').is_file())
        with closing(self.service._connect()) as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM learning_cloud_archive WHERE removed=1').fetchone()[0], 1)
            row = conn.execute('SELECT sha256,content FROM learning_cloud_files').fetchone()
            self.assertEqual(row['sha256'], hashlib.sha256(row['content']).hexdigest())
            self.assertEqual(row['content'], b'original attachment')
        self.assertIsNotNone(self.service._get('paper', 'paper'))

    def test_new_column_is_captured_in_backup_before_system_record_cleanup(self):
        self.cloud.entities['settings', 'main'] = {'enabled': True}

        def provision():
            self.cloud.fields['settings', 'main'] = {'题单发送状态': ''}

        with patch.object(self.cloud, 'ensure_personnel_schema', side_effect=provision):
            self.service.sync_pending()
        storage.cleanup(self.service)
        self.assertNotIn(('settings', 'main'), self.cloud.entities)

    def test_failed_summary_does_not_delete_personal_history_but_other_records_can_finish(self):
        paper = self.paper()
        self.cloud.entities['paper', 'paper'] = paper
        self.cloud.entities['settings', 'main'] = {'enabled': True}
        self.cloud.fail_after_commit = 'upsert_entity'
        self.service.sync_pending()
        storage.cleanup(self.service)
        self.assertIn(('paper', 'paper'), self.cloud.entities)
        self.assertNotIn(('settings', 'main'), self.cloud.entities)

    def test_missing_attachment_does_not_delete_record_or_block_other_records(self):
        self.cloud.entities['settings', 'broken'] = {'id': 'broken'}
        self.cloud.fields['settings', 'broken'] = {'附件': [{'file_token': 'missing'}]}
        self.cloud.entities['settings', 'good'] = {'id': 'good'}
        self.service.sync_pending()
        storage.cleanup(self.service)
        self.assertIn(('settings', 'broken'), self.cloud.entities)
        self.assertNotIn(('settings', 'good'), self.cloud.entities)

    def test_archived_attachment_remains_readable_offline_with_original_access_checks(self):
        value = {'id': 'file', 'name': 'fixture.txt', 'kind': 'material', 'file_token': 'original_file'}
        self.cloud.entities['attachment', 'file'] = value
        self.cloud.files['original_file'] = b'original content'
        self.service.sync_pending()
        storage.cleanup(self.service)
        self.assertNotIn(('attachment', 'file'), self.cloud.entities)
        with patch.object(self.cloud, 'download_attachment', side_effect=AssertionError('must use verified local archive')):
            with self.assertRaises(core.LearningError) as denied:
                self.service.attachment('file', support.ACTOR)
            self.assertEqual(denied.exception.status, 403)
            path, _, _ = self.service.attachment('file', {'is_admin': True})
            self.assertEqual(path.read_bytes(), b'original content')

    def test_existing_bundle_is_not_stripped_until_its_attachment_is_local(self):
        paper = self.paper()
        identity = '班次人员:' + DATE + ':白:directory:one'
        original = {'schema_version': 1, 'date': DATE, 'shift': '白', 'person_id': 'directory:one',
                    'entities': [{'kind': 'paper', 'key': 'paper', 'payload': paper}],
                    'summary': {'date': DATE, 'shift': '白', 'person_id': 'directory:one'}}
        self.cloud.entities['shift_person', identity] = copy.deepcopy(original)
        self.cloud.fields['shift_person', identity] = {'附件': [{'file_token': 'original_file'}]}
        self.paper('other', pid='directory:two')
        self.assertEqual(self.service.sync_pending()['pending_errors'], 1)
        self.assertEqual(self.cloud.entities['shift_person', identity], original)
        self.assertTrue(self.service._get('paper', 'paper')['_dirty'])
        self.assertTrue(any(s['person_id'] == 'directory:two' for s in self.summaries().values()))
        self.cloud.files['original_file'] = b'original binary'
        self.assertEqual(self.service.sync_pending(force=True)['pending_errors'], 0)
        self.assertEqual(self.cloud.entities['shift_person', identity]['entities'], [])

    def test_cleanup_cursor_does_not_starve_records_after_a_failure(self):
        for key in ('one', 'two', 'three'):
            self.cloud.entities['settings', key] = {'id': key}
            self.cloud.fields['settings', key] = {'附件': [{'file_token': 'missing_' + key}]}
        self.service.sync_pending()
        seen = set()
        for attempt in range(3):
            with patch.object(storage.time, 'time', return_value=10**12 + attempt * 120):
                storage.cleanup(self.service, limit=1)
            seen.add(self.service._get('local', storage.PROGRESS_KEY)['cursor'])
        self.assertEqual(len(seen), 3)
        self.assertEqual(len(self.cloud.entities), 3)

    def test_legacy_reminders_without_shift_owner_are_archived_without_deleting_local_tasks(self):
        self.cloud.entities['notification','orphan']={'id':'orphan','open_id':'ou_unknown','status':'sent'}
        self.cloud.entities['engagement','old-weekly']={'id':'old-weekly','kind':'weekly','status':'sent'}
        storage.prepare(self.service)
        self.service._last_error=storage.CLEANUP_ERROR_PREFIX+'旧归属错误'
        before={key:self.service._get(*key) for key in self.cloud.entities}
        storage.cleanup(self.service)
        self.assertEqual(self.cloud.entities,{})
        self.assertEqual(len(self.cloud.deleted),2)
        self.assertEqual(self.service._last_error,'')
        for key,value in before.items(): self.assertEqual(self.service._get(*key),value)
        with closing(self.service._connect()) as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM learning_cloud_archive WHERE removed=1 AND files_ready=1').fetchone()[0],2)

    def test_legacy_cleanup_keeps_pending_local_work_and_does_not_wait_for_new_summary(self):
        paper=self.paper()
        self.cloud.entities['notification','pending']={'id':'pending','paper_id':paper['id'],'status':'pending','attempts':2}
        storage.prepare(self.service)
        storage.cleanup(self.service)
        self.assertNotIn(('notification','pending'),self.cloud.entities)
        self.assertEqual(self.service._get('notification','pending')['status'],'pending')
        self.assertEqual(self.service._get('notification','pending')['attempts'],2)
        self.assertEqual(self.service._get('paper',paper['id'])['questions'],paper['questions'])
        self.assertEqual(self.sender.calls,[])

    def test_corrupt_backup_or_changed_cloud_row_is_never_deleted(self):
        for changed_cloud in (False, True):
            with self.subTest(changed_cloud=changed_cloud):
                self.cloud = PersonnelCloud()
                self.service = core.LearningService(self.root / str(changed_cloud), self.cloud,
                    get_login_people=lambda: self.directory)
                key = str(changed_cloud)
                self.cloud.entities['settings', key] = {'id': key}
                with self.service.transaction() as conn:
                    self.service._put('local', storage.PROGRESS_KEY, {}, conn, False)
                storage.prepare(self.service)
                if changed_cloud:
                    self.cloud.entities['settings', key]['updated'] = True
                else:
                    with self.service.transaction() as conn:
                        conn.execute("UPDATE learning_cloud_archive SET document='{}'")
                storage.cleanup(self.service)
                self.assertIn(('settings', key), self.cloud.entities)

    def test_failed_backup_prevents_cloud_mutation_and_can_resume(self):
        self.paper()
        with patch.object(storage, '_backup_database', side_effect=OSError('synthetic disk full')), self.assertLogs(level='WARNING'):
            self.assertEqual(self.service.sync_pending()['pending_errors'], 1)
        self.assertEqual(self.cloud.entities, {})
        self.assertTrue(self.service._get('paper', 'paper')['_dirty'])
        self.assertEqual(self.service.sync_pending()['pending_errors'], 0)
        self.assertEqual(len(self.summaries()), 1)

    def test_lost_delete_acknowledgement_and_restart_do_not_delete_twice(self):
        self.cloud.entities['settings', 'main'] = {'enabled': True}
        self.service.sync_pending()
        self.cloud.fail_delete_after_commit = True
        storage.cleanup(self.service)
        self.assertEqual(len(self.cloud.deleted), 1)
        self.assertEqual(self.service.sync_status(include_history_issues=True)['history_issue_count'], 1)
        with patch.object(self.cloud, 'list_entity_records', side_effect=OSError('offline')):
            with patch.object(storage.time, 'time', return_value=10**12), self.assertRaises(OSError):
                storage.cleanup(self.service)
        self.assertEqual(self.service.sync_status(include_history_issues=True)['history_issue_count'], 1)
        self.restart()
        with patch.object(storage.time, 'time', return_value=10**12):
            storage.cleanup(self.service)
        self.assertTrue(self.service._get('local', storage.PROGRESS_KEY)['complete'])
        self.assertEqual(len(self.cloud.deleted), 1)
        self.assertEqual(self.service.sync_status(include_history_issues=True)['history_issue_count'], 0)

    def test_delete_failure_preserves_original_and_retry_after_restart_works(self):
        self.cloud.entities['settings', 'main'] = {'enabled': True}
        self.service.sync_pending()
        self.cloud.fail_delete_before_commit = True
        storage.cleanup(self.service)
        self.assertIn(('settings', 'main'), self.cloud.entities)
        self.cloud.fail_delete_before_commit = False
        self.restart()
        with patch.object(storage.time, 'time', return_value=10**12):
            storage.cleanup(self.service)
        self.assertNotIn(('settings', 'main'), self.cloud.entities)

    def test_question_bank_edit_keeps_original_save_path_without_attachment_entity_rows(self):
        question = support.LearningTests.question(self, 1)
        self.put('question', question['id'], question)
        self.service.sync_pending()
        self.assertIn(question['id'], self.cloud.saved_questions)
        self.assertNotIn(('question', question['id']), self.cloud.entities)

    def test_disabling_schedule_is_saved_locally_without_cloud_setup(self):
        self.put('settings', 'main', {'enabled': False})
        self.service.tick(CURRENT)
        self.assertFalse(self.service._get('settings', 'main')['_dirty'])
        self.assertEqual(self.cloud.calls, [])
        self.assertEqual(self.cloud.entities, {})

    def test_packager_includes_new_module_and_excludes_local_database_and_backups(self):
        import package_portable as packaging
        self.assertIn('lan_bitable_template_portal.learning_personnel_storage', packaging.SMOKE_IMPORT_MODULES)
        paths = {p.relative_to(packaging.PROJECT_ROOT).as_posix()
                 for p in packaging._iter_project_files(packaging.PROJECT_ROOT)}
        self.assertIn('bin/lan_bitable_template_portal/learning_personnel_storage.py', paths)
        for relative in ('bin/data/learning/learning.sqlite3',
                         'bin/data/learning/backups/learning-before-personnel-only.sqlite3'):
            self.assertTrue(packaging._is_runtime_data_path(packaging.PROJECT_ROOT / relative))
            self.assertNotIn(relative, paths)


class PersonnelCloudProtocolTests(unittest.TestCase):
    setUp = shift_support.ShiftCloudTests.setUp
    new_remote = shift_support.ShiftCloudTests.new_remote
    bundle = shift_support.ShiftCloudTests.bundle
    display = shift_support.ShiftCloudTests.display
    row = shift_support.ShiftCloudTests.row
    upsert = shift_support.ShiftCloudTests.upsert

    def test_summary_is_chinese_inline_without_internal_data_or_gzip_attachment(self):
        payload = self.bundle(entities=[])
        saved = self.upsert(payload, display_fields=self.display())
        fields = saved['fields']
        self.assertEqual(fields['类别'], '班次人员汇总')
        visible = json.loads(fields['内容'])
        self.assertEqual(set(visible), {'人员班次答题情况'})
        self.assertTrue(all(any('\u4e00' <= c <= '\u9fff' for c in name) for name in visible['人员班次答题情况']))
        self.assertEqual(fields['附件'], [])
        self.assertEqual(self.remote.list_entity_records()[0]['payload'], payload)
        self.assertEqual(self.remote.load_entities(), [])
        self.assertEqual(self.fake.media, {})

    def test_delivery_column_is_provisioned_once_and_wrong_type_is_rejected(self):
        self.remote.ensure_personnel_schema()
        writes = len(self.fake.mutations())
        self.assertEqual(writes, 1)
        self.remote.ensure_personnel_schema()
        self.assertEqual(len(self.fake.mutations()), writes)
        field = next(f for f in self.fake.fields[self.table] if f['field_name'] == '题单发送状态')
        field['type'] = 2
        with self.assertRaises(cloud.LearningCloudError):
            self.remote.ensure_personnel_schema()
        self.assertEqual(len(self.fake.mutations()), writes)

    def test_changed_bundle_is_not_overwritten_against_stale_archive(self):
        payload = self.bundle()
        original = self.row('shift_person', '班次人员:2026-10-21:白:stable-person', payload, compressed=True)
        with self.assertRaises(cloud.LearningCloudConflict):
            self.upsert(self.bundle(entities=[]), display_fields=self.display(), expected_previous_sha256='0' * 64)
        self.assertEqual(len(self.fake.records[self.table]), 1)
        self.assertEqual(self.fake.records[self.table][0], original)
        self.assertFalse(self.fake.mutations())

    def test_new_answer_can_update_an_already_converted_summary(self):
        payload = self.bundle()
        original = self.row('shift_person', '班次人员:2026-10-21:白:stable-person', payload, compressed=True)
        checksum = cloud._payload_digest(payload)
        self.upsert(self.bundle(entities=[]), display_fields=self.display(), expected_previous_sha256=checksum)
        updated = self.bundle(entities=[], summary={'answered': 2})
        saved = self.upsert(updated, display_fields=self.display(已答题数=2), expected_previous_sha256=checksum)
        self.assertEqual(saved['record_id'], original['record_id'])
        self.assertEqual(self.remote.list_entity_records()[0]['payload']['summary']['answered'], 2)

    def test_archived_delete_checks_full_fields_and_confirms_deleted_id(self):
        self.enterContext(patch('upload_event_module.services.feishu_token_manager.token_manager._credentials',
                                return_value=('cli_current', 'synthetic-secret')))
        original = self.row('settings', 'main', {'enabled': True}, fields={'custom': 'preserve'})
        archived = self.remote.list_entity_records(include_fields=True)[0]
        original['fields']['custom'] = 'changed'
        with self.assertRaises(cloud.LearningCloudConflict):
            self.remote.delete_archived_entity(archived, expected_app_id='cli_current')
        self.assertFalse(any(call[0] == 'DELETE' for call in self.fake.calls))

    def test_delete_requires_confirmed_matching_receipt(self):
        self.enterContext(patch('upload_event_module.services.feishu_token_manager.token_manager._credentials',
                                return_value=('cli_current', 'synthetic-secret')))
        self.row('settings', 'main', {'enabled': True})
        archived = self.remote.list_entity_records(include_fields=True)[0]
        response = {'record_id': archived['record_id'], 'deleted': False}

        def intercept(request):
            if request.method == 'DELETE':
                return self.fake.response(response)

        self.fake.before = intercept
        with self.assertRaises(cloud.LearningCloudWriteUncertain):
            self.remote.delete_archived_entity(archived, expected_app_id='cli_current')
        response['deleted'] = True
        self.assertTrue(self.remote.delete_archived_entity(archived, expected_app_id='cli_current')['deleted'])

    def test_cleanup_ignores_expiring_attachment_links_but_checks_file_identity(self):
        self.enterContext(patch('upload_event_module.services.feishu_token_manager.token_manager._credentials',
                                return_value=('cli_current', 'synthetic-secret')))
        for field in ('file_token', 'name', 'size', None):
            with self.subTest(field=field):
                original = self.row('attachment', str(field), {'id': str(field)}, fields={'附件': [{
                    'file_token': 'fileOriginal', 'name': 'backup.json', 'size': 12,
                    'url': 'https://example.test/old', 'tmp_url': 'https://example.test/old-temp'}]})
                archived = next(row for row in self.remote.list_entity_records(include_fields=True) if row['key'] == str(field))
                attachment = original['fields']['附件'][0]
                attachment.update(url='https://example.test/new', tmp_url='https://example.test/new-temp')
                if field:
                    attachment[field] = 24 if field == 'size' else 'changed'
                    with self.assertRaises(cloud.LearningCloudConflict):
                        self.remote.delete_archived_entity(archived, expected_app_id='cli_current')
                    self.assertFalse(any(call[0] == 'DELETE' for call in self.fake.calls))
                else:
                    self.fake.before = lambda request: self.fake.response({'record_id': archived['record_id'],
                        'deleted': True}) if request.method == 'DELETE' else None
                    self.assertTrue(self.remote.delete_archived_entity(archived, expected_app_id='cli_current')['deleted'])

    def test_personal_history_cannot_be_deleted_by_system_cleanup(self):
        for kind in ('paper', 'record', 'sharing_attempt', 'daily_progress', 'shift_person'):
            with self.subTest(kind=kind), self.assertRaisesRegex(cloud.LearningCloudError, '人员答题历史'):
                self.remote.delete_archived_entity({'record_id': 'recProtected', 'kind': kind}, expected_app_id='cli_current')
        self.assertFalse(self.fake.calls)

    def test_feishu_json_error_and_empty_download_are_not_valid_backups(self):
        for response in (shift_support.httpx.Response(200, json={'code': 1254302, 'msg': 'not permitted'}),
                         shift_support.httpx.Response(200, content=b'')):
            self.fake.before = lambda request, response=response: response
            with self.assertRaises(cloud.LearningCloudError):
                self.remote.download_attachment('fileFixture', 1024)
        content = b'{"code":123,"note":"legitimate text attachment"}'
        self.fake.before = lambda request: shift_support.httpx.Response(200, content=content,
            headers={'content-type': 'text/plain'})
        self.assertEqual(self.remote.download_attachment('fileFixture', 1024), content)


if __name__ == '__main__':
    unittest.main()

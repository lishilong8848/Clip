"""Isolated scheduling and evidence regressions against the production modules."""

import datetime as dt
import json
import pathlib
import shutil
import sqlite3
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock, patch

import PIL.Image

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from lan_bitable_template_portal import pre_shift_checks as checks
from lan_bitable_template_portal import pre_shift_cloud as cloudmod
from test_pre_shift_checks import receipt

TZ = checks.TZ
DAY = '2026-10-14'


def at(value):
    return dt.datetime.fromisoformat(value).replace(tzinfo=TZ)


def make_png(path):
    tmp_path = str(path) + '.png'
    PIL.Image.new('RGB', (2, 2)).save(tmp_path)
    shutil.copy2(tmp_path, path)


class FailingCommitConnection(sqlite3.Connection):
    def commit(self):
        raise sqlite3.OperationalError('disk io')


class TrackingConnection(sqlite3.Connection):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.update_tasks = 0

    def execute(self, sql, *args, **kwargs):
        if isinstance(sql, str) and sql.strip().upper().startswith('UPDATE tasks'):
            self.update_tasks += 1
        return super().execute(sql, *args, **kwargs)


def factory_connect(real_connect, conn_class):
    def connect(*a, **k):
        k['factory'] = conn_class
        return real_connect(*a, **k)
    return connect


def roster_people(scope='A'):
    return [{'scope': scope, 'shift': shift, 'open_id': f'ou_{scope}_{i}', 'name': f'p{i}'}
            for shift in ('白', '夜') for i in (1, 2)]


class Base(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.mkdtemp()
        self.admin = {'id': 'ou_admin', 'name': '管理员', 'level': 'L1',
                      'scope_rules': ['ALL'], 'category': 'manager'}
        self.worker = {'id': 'ou_A_1', 'name': '值班员一', 'level': 'L4',
                       'scope_rules': ['A'], 'home_scope': 'A', 'category': 'operator'}
        self.peer = {**self.worker, 'id': 'ou_A_2', 'name': '值班员二', 'category': 'shift_leader'}
        roster_calls = {'n': 0}
        def roster(day):
            roster_calls['n'] += 1
            return {'people': roster_people(), 'warnings': {}}
        self.roster = roster
        self.roster_calls = roster_calls
        self.sender = Mock(side_effect=receipt)
        self.cloud = Mock()
        self.cloud.upsert.return_value = 'rec_test'
        self.service = checks.CheckService(self.temp, roster=self.roster, cloud=self.cloud,
                                           sender=self.sender, portal_url=lambda: 'http://portal')
        self.now_patch = patch.object(checks, 'now', return_value=at(DAY + 'T09:00:00'))
        self.now_patch.start()
        self.addCleanup(self.now_patch.stop)
        self.addCleanup(shutil.rmtree, self.temp, True)

    def save(self, scope, version, items, at_time=None):
        current = at_time or at(DAY + 'T09:00:00')
        with patch.object(checks, 'now', return_value=current):
            return self.service.save_definition(self.admin, scope, version, items)

    def prepare(self, current):
        self.service.prepare(current)


class SchedulingTests(Base):
    def test_invalid_legacy_frequency_does_not_block_other_items(self):
        self.save('A', 0, [{'id': 'valid', 'text': '正常检查'}])
        with self.service.connection(True) as conn:
            stored = self.service.definition('A', conn)['items']
            stored.append({'id': 'bad', 'text': '旧配置损坏', 'frequency': {'kind': 'unknown'}})
            conn.execute('UPDATE definitions SET items=? WHERE scope=?', (json.dumps(stored), 'A'))
        self.prepare(at(DAY + 'T09:00:00'))
        tasks = self.service.state(self.worker, 'A', at(DAY + 'T09:00:00'))['tasks']
        self.assertTrue(tasks)
        self.assertTrue(all([i['id'] for i in task['items']] == ['valid'] for task in tasks))

    def test_per_item_groups_and_nonstandard_id(self):
        items = [
            {'id': 'legacy', 'text': '传统'},
            {'id': 'lunch', 'text': '午间', 'frequency': {'kind': 'daily', 'time': '11:00'}},
        ]
        self.save('A', 0, items, at(DAY + 'T09:00:00'))
        self.prepare(at(DAY + 'T11:05:00'))
        tasks = {t['id']: t for t in self.service.state(self.worker, 'A', at(DAY + 'T11:05:00'))['tasks']}
        self.assertIn(f'{DAY}:A:白', tasks, 'legacy white standard id must remain day:scope:shift')
        self.assertIn(f'{DAY}:A:白:1100', tasks, 'nonstandard due needs an HHmm suffix')
        self.assertEqual([i['id'] for i in tasks[f'{DAY}:A:白']['items']], ['legacy'])
        self.assertEqual([i['id'] for i in tasks[f'{DAY}:A:白:1100']['items']], ['lunch'])
        # Files/handover state fields are exposed.
        t = tasks[f'{DAY}:A:白']
        self.assertEqual(t['files'], [])
        self.assertIsNone(t['handover_photo'])
        self.assertTrue(t['can_upload_handover'])

    def test_edit_clear_hides_pending_and_submitted_kept(self):
        self.save('A', 0, [{'id': 'x', 'text': 'X'}], at(DAY + 'T09:00:00'))
        self.prepare(at(DAY + 'T09:00:00'))
        current = at(DAY + 'T09:00:00')
        self.assertEqual(len(self.service.state(self.worker, 'A', current)['tasks']), 2, 'white and upcoming night are both visible')
        self.save('A', 1, [], at(DAY + 'T09:00:00'))
        self.assertEqual(self.service.state(self.worker, 'A', current)['tasks'], [])

    def test_text_edit_preserves_frequency_anchor(self):
        items = [{'id': 'a', 'text': '原始', 'frequency': {'kind': 'temporary', 'cadence': 'daily',
                                                          'limit_type': 'days', 'limit': 3, 'time': '09:00',
                                                          'start_date': '2026-10-10'}}]
        saved = self.save('A', 0, items, at(DAY + 'T09:00:00'))
        self.assertEqual(saved['items'][0]['frequency']['start_date'], '2026-10-10')
        again = self.save('A', 1, [{'id': 'a', 'text': '改文案'}],
                          at(DAY + 'T09:00:00'))
        self.assertEqual(again['items'][0]['frequency']['start_date'], '2026-10-10')

    def test_late_boot_midnight_uses_prior_roster(self):
        self.save('A', 0, [{'id': 'x', 'text': 'X'}], at(DAY + 'T09:00:00'))
        current = at('2026-10-15T06:00:00')
        self.prepare(current)
        tasks = self.service.state(self.worker, 'A', current)['tasks']
        night = [t for t in tasks if t['id'] == f'{DAY}:A:夜']
        self.assertEqual(len(night), 1, 'previous-night task must remain open at 06:00')

    def test_month_end_schedules(self):
        self.save('A', 0, [{'id': 'm', 'text': '月末', 'frequency': {'kind': 'month_end', 'time': '10:00',
                                                                    'start_date': '2026-01-01'}}],
                  at('2026-02-28T10:30:00'))
        self.prepare(at('2026-02-28T10:30:00'))
        tasks = self.service.state(self.worker, 'A', at('2026-02-28T10:30:00'))['tasks']
        self.assertTrue(any(t['shift'] == '白' and any(i['id'] == 'm' for i in t['items']) for t in tasks))

    def test_finite_temporary_task_expiry_and_restart_stability(self):
        items = [{'id': 'tmp', 'text': '临时', 'frequency': {'kind': 'temporary', 'cadence': 'daily',
                                                            'limit_type': 'occurrences', 'limit': 2, 'time': '09:00'}}]
        self.save('A', 0, items, at('2026-10-14T09:00:00'))
        self.prepare(at('2026-10-14T09:00:00'))
        self.assertEqual(len(self.service.state(self.worker, 'A', at('2026-10-14T09:00:00'))['tasks']), 1)  # daily cadence -> one white task
        # Reconstruction on restart is stable.
        fresh = checks.CheckService(self.temp, roster=self.roster, cloud=self.cloud,
                                    sender=self.sender, portal_url=lambda: 'http://portal')
        with patch.object(checks, 'now', return_value=at('2026-10-14T09:00:00')):
            fresh.prepare(at('2026-10-14T09:00:00'))
        ids = sorted(t['id'] for t in fresh.state(self.worker, 'A', at('2026-10-14T09:00:00'))['tasks'])
        # The temporary daily occurrence limit=2 spans two nominal days; day1 occurs once (white at 09:00).
        self.assertEqual(ids, [f'{DAY}:A:白:0900'])


class AttachmentTests(Base):
    def _white_id(self):
        return f'{DAY}:A:白'

    def test_early_and_late_evidence_keeps_building_and_file_permissions(self):
        self.save('A', 0, [{'id': 'x', 'text': 'X'}], at(DAY + 'T09:00:00'))
        self.prepare(at(DAY + 'T09:00:00'))
        future = f'{DAY}:A:夜'
        early = at(DAY + 'T09:00:00')
        session = self.service.stage_upload(self.worker, future, 'x', 'item', '提前检查.txt', early)
        pathlib.Path(session['path']).write_bytes(b'early evidence')
        self.service.commit_upload(self.worker, session, None, early)
        photo = self.service.stage_upload(self.peer, future, '', 'handover', '提前交接.png', early)
        make_png(photo['path'])
        self.service.commit_upload(self.peer, photo, None, early)
        late = at('2026-10-16T09:00:00')
        session = self.service.stage_upload(self.peer, self._white_id(), 'x', 'item', '补做检查.txt', late)
        pathlib.Path(session['path']).write_bytes(b'late evidence')
        self.service.commit_upload(self.peer, session, None, late)
        row = next(t for t in self.service.state(self.worker, 'A', late)['tasks'] if t['id'] == self._white_id())
        self.assertEqual([f['name'] for f in row['files']], ['补做检查.txt'])
        with self.assertRaises(checks.CheckError):
            self.service.stage_upload({**self.peer, 'home_scope': 'B'}, self._white_id(), 'x', 'item', '越权.txt', late)

    def test_item_file_upload_ownership_and_versioned_evidence(self):
        self.save('A', 0, [{'id': 'x', 'text': 'X'}], at(DAY + 'T09:00:00'))
        self.prepare(at(DAY + 'T09:00:00'))
        white = self._white_id()
        session = self.service.stage_upload(self.worker, white, 'x', 'item', 'note.png')
        make_png(session['path'])
        size = pathlib.Path(session['path']).stat().st_size
        uploaded = self.service.commit_upload(self.worker, session, size)
        state = self.service.state(self.worker, 'A', at(DAY + 'T09:00:00'))
        task = next(t for t in state['tasks'] if t['id'] == white)
        self.assertEqual([f['id'] for f in task['files']], [uploaded['id']])
        self.assertTrue(task['files'][0]['can_delete'])
        # Cross-scope cannot view/delete.
        other = {**self.worker, 'id': 'ou_other', 'home_scope': 'B', 'scope_rules': ['B']}
        with self.assertRaises(checks.CheckError) as exc:
            self.service.get_file(other, uploaded['id'])
        self.assertEqual(exc.exception.status, 403)
        # Another roster person cannot delete pending upload.
        with self.assertRaises(checks.CheckError):
            self.service.delete_file(self.peer, uploaded['id'], at(DAY + 'T09:00:00'))
        # Uploader can delete before submit (within shift window).
        self.service.delete_file(self.worker, uploaded['id'], at(DAY + 'T09:00:00'))
        # Re-upload for the submitted evidence assertions below.
        session = self.service.stage_upload(self.worker, white, 'x', 'item', 'note.png')
        make_png(session['path'])
        size = pathlib.Path(session['path']).stat().st_size
        uploaded = self.service.commit_upload(self.worker, session, size)
        # Submitted evidence requires an explicit revision to change.
        self.service.submit(self.worker, white, 1, [{'id': 'x', 'status': 'normal', 'note': ''}], at(DAY + 'T09:10:00'))
        state = self.service.state(self.worker, 'A', at(DAY + 'T09:10:00'))
        task = next(t for t in state['tasks'] if t['id'] == white)
        self.assertEqual([f['name'] for f in task['files']], ['note.png'])
        self.assertTrue(task['files'][0]['can_delete'])
        with self.assertRaises(checks.CheckError) as exc:
            self.service.delete_file(self.worker, uploaded['id'], at(DAY + 'T09:10:00'))
        self.assertEqual(exc.exception.status, 409)
        original = pathlib.Path(self.service.get_file(self.worker, uploaded['id'])['path']).read_bytes()
        deleted = self.service.delete_file(self.peer, uploaded['id'], at(DAY + 'T09:10:00'),
                                           task_id=white, revision=task['revision'])
        self.assertEqual(deleted['revision'], task['revision'] + 1)
        self.assertEqual(self.service.task(self.worker, white)['files'], [])
        self.assertEqual(pathlib.Path(self.service.get_file(self.worker, uploaded['id'])['path']).read_bytes(), original)

    def test_reject_bad_path_size_and_signature(self):
        self.save('A', 0, [{'id': 'x', 'text': 'X'}], at(DAY + 'T09:00:00'))
        self.prepare(at(DAY + 'T09:00:00'))
        white = self._white_id()
        # Traversal / weird names sanitized to plain filename; user path never used.
        session = self.service.stage_upload(self.worker, white, 'x', 'item', '../../evil.png')
        self.assertEqual(session['name'], 'evil.png')
        self.service.abort_upload(session)
        # Unsupported extension.
        with self.assertRaises(checks.CheckError):
            self.service.stage_upload(self.worker, white, 'x', 'item', 'evil.exe')
        # Handover only accepts images.
        with self.assertRaises(checks.CheckError):
            self.service.stage_upload(self.worker, white, '', 'handover', 'hand.pdf')
        # Empty file.
        session = self.service.stage_upload(self.worker, white, 'x', 'item', 'empty.png')
        with self.assertRaises(checks.CheckError):
            self.service.commit_upload(self.worker, session, 0)
        # Signature mismatch: html bytes in a .png.
        session = self.service.stage_upload(self.worker, white, 'x', 'item', 'fake.png')
        pathlib.Path(session['path']).write_bytes(b'<html>not an image</html>')
        with self.assertRaises(checks.CheckError):
            self.service.commit_upload(self.worker, session, len(b'<html>not an image</html>'))
        # Oversize per-file bound is enforced against the actual staged size.
        session = self.service.stage_upload(self.worker, white, 'x', 'item', 'big.txt')
        pathlib.Path(session['path']).write_bytes(b'a' * (21 * 1024 * 1024))
        actual = pathlib.Path(session['path']).stat().st_size
        with self.assertRaises(checks.CheckError) as exc:
            self.service.commit_upload(self.worker, session, actual)
        self.assertEqual(exc.exception.status, 400)
        # Stage size must match the reported streamed size.
        session = self.service.stage_upload(self.worker, white, 'x', 'item', 'size.txt')
        pathlib.Path(session['path']).write_bytes(b'abc')
        reported = pathlib.Path(session['path']).stat().st_size
        with self.assertRaises(checks.CheckError) as exc:
            self.service.commit_upload(self.worker, session, reported + 1)
        self.assertEqual(exc.exception.status, 409)

    def test_parallel_submit_one_wins_and_one_handover(self):
        self.save('A', 0, [{'id': 'x', 'text': 'X'}], at(DAY + 'T09:00:00'))
        self.prepare(at(DAY + 'T09:00:00'))
        white = self._white_id()
        barrier = threading.Barrier(2)
        def submit(actor):
            barrier.wait()
            return self.service.submit(actor, white, 1, [{'id': 'x', 'status': 'normal', 'note': ''}], at(DAY + 'T09:05:00'))
        with ThreadPoolExecutor(2) as pool:
            results = list(pool.map(submit, [self.worker, self.peer]))
        self.assertEqual(sum(not r['already_completed'] for r in results), 1)

        # Two competing handover uploads: only one wins.
        revision = self.service.task(self.worker, white)['revision']
        s1 = self.service.stage_upload(self.worker, white, '', 'handover', 'h1.png', revision=revision)
        make_png(s1['path'])
        size1 = pathlib.Path(s1['path']).stat().st_size
        s2 = self.service.stage_upload(self.peer, white, '', 'handover', 'h2.png', revision=revision)
        make_png(s2['path'])
        size2 = pathlib.Path(s2['path']).stat().st_size
        winner = self.service.commit_upload(self.worker, s1, size1)
        with self.assertRaises(checks.CheckError) as exc:
            self.service.commit_upload(self.peer, s2, size2)
        self.assertEqual(exc.exception.status, 409)
        state = self.service.state(self.worker, 'A', at(DAY + 'T09:05:00'))
        task = next(t for t in state['tasks'] if t['id'] == white)
        self.assertEqual(task['handover_photo']['id'], winner['id'])

    def test_shared_photo_after_submit_bumps_outbox(self):
        self.save('A', 0, [{'id': 'x', 'text': 'X'}], at(DAY + 'T09:00:00'))
        self.prepare(at(DAY + 'T09:00:00'))
        white = self._white_id()
        self.service.submit(self.worker, white, 1, [{'id': 'x', 'status': 'normal', 'note': ''}], at(DAY + 'T09:05:00'))
        with self.service.connection() as conn:
            outbox = conn.execute('SELECT revision, synced FROM outbox').fetchall()
        self.assertEqual(len(outbox), 1)
        before = outbox[0]['revision']
        revision = self.service.task(self.worker, white)['revision']
        session = self.service.stage_upload(self.worker, white, '', 'handover', 'h.png', revision=revision)
        make_png(session['path'])
        size = pathlib.Path(session['path']).stat().st_size
        self.service.commit_upload(self.worker, session, size)
        with self.service.connection() as conn:
            after = conn.execute('SELECT revision FROM outbox').fetchone()['revision']
        self.assertEqual(after, before + 1, 'shared photo after submit must bump outbox revision')

    def test_no_duplicate_token_upload_after_record_failure(self):
        uploads = Mock(return_value='tok_one')
        stub_service = SimpleNamespace(_upload_bitable_file=uploads)
        cloud = cloudmod.CheckCloud(stub_service)
        cloud.schema_ready = True
        cloud._guard_real_write = lambda: None
        persisted = {}
        cloud.on_file_token = lambda file_id, token: persisted.__setitem__(file_id, token)
        task = {
            'day': DAY, 'scope': 'A', 'shift': '白', 'submitted': '2026-10-14T09:10:00+08:00',
            'actor_name': 'a', 'actor_id': 'ou_x', 'definition_version': 1,
            'answers': json.dumps([{'id': 'x', 'text': 'X', 'status': 'normal', 'note': ''}]),
            '_files': [{'file_id': 'f1', 'task_id': 't1', 'item_id': 'x', 'kind': 'item',
                        'name': 'a.png', 'size': 10, 'is_image': True, 'path': '/tmp/a.png', 'token': ''}],
        }
        calls = {'n': 0}
        def fake_request(method, path, body=None, params=None):
            if path == 'records/rec_existing':
                if calls['n'] == 0:
                    calls['n'] += 1
                    raise TimeoutError('transient')
                return {}
            raise AssertionError(path)
        cloud.request = fake_request
        with self.assertRaises(TimeoutError):
            cloud.upsert('key', [task], 'rec_existing')
        self.assertEqual(persisted.get('f1'), 'tok_one')
        # Retry with persisted token reuses it; no duplicate upload.
        task2 = dict(task)
        task2['_files'] = [dict(task['_files'][0], token='tok_one')]
        cloud.upsert('key', [task2], 'rec_existing')
        uploads.assert_called_once()


class CloudFieldTests(Base):
    def test_upsert_adds_attachment_fields_with_dedup_handover(self):
        cloud = cloudmod.CheckCloud(None)
        cloud.schema_ready = True
        cloud._guard_real_write = lambda: None
        uploaded_refs = {
            'f1': {'token': 'TOK1', 'name': 'a.png', 'is_image': True, 'item_id': 'x'},
            'h1': {'token': 'TOKH', 'name': 'h.png', 'is_image': True, 'item_id': '', 'kind': 'handover'},
        }
        calls = {'uploaded': set()}
        def fake_upload(file_path=None, file_name=None, app_token=None):
            calls['uploaded'].add(file_name)
            return {'a.png': 'TOK1', 'h.png': 'TOKH'}[file_name]
        class DummyService:
            _upload_bitable_file = staticmethod(fake_upload)
        cloud.service = DummyService()
        cloud.on_file_token = lambda *_: None
        tasks = [{
            'day': DAY, 'scope': 'A', 'shift': '白', 'submitted': '2026-10-14T09:10:00+08:00',
            'actor_name': 'a', 'actor_id': 'ou_x', 'definition_version': 1,
            'answers': json.dumps([{'id': 'x', 'text': 'X', 'status': 'normal', 'note': ''}]),
            '_files': [
                {'file_id': 'f1', 'item_id': 'x', 'kind': 'item', 'name': 'a.png', 'is_image': True,
                 'path': '/tmp/a.png', 'token': ''},
                {'file_id': 'h1', 'item_id': '', 'kind': 'handover', 'name': 'h.png', 'is_image': True,
                 'shift': '白', 'path': '/tmp/h.png', 'token': ''},
            ],
        }, {
            'day': DAY, 'scope': 'A', 'shift': '白', 'submitted': '2026-10-14T09:10:00+08:00',
            'actor_name': 'a', 'actor_id': 'ou_x', 'definition_version': 1,
            'answers': json.dumps([{'id': 'x', 'text': 'X', 'status': 'normal', 'note': ''}]),
            '_files': [
                {'file_id': 'h1', 'item_id': '', 'kind': 'handover', 'name': 'h.png', 'is_image': True,
                 'shift': '白', 'path': '/tmp/h.png', 'token': ''},
            ],
        }]
        captured = {}
        def fake_request(method, path, body=None, params=None):
            if path == 'records/rec_existing':
                captured['fields'] = body.get('fields', {})
                return {}
            raise AssertionError(path)
        cloud.request = fake_request
        cloud.upsert('key', tasks, 'rec_existing')
        self.assertEqual(captured['fields']['检查附件'], [{'file_token': 'TOK1'}])
        self.assertEqual(len(captured['fields']['交接班照片']), 1, 'handover must be deduplicated')
        self.assertEqual(captured['fields']['交接班照片'], [{'file_token': 'TOKH'}])
        self.assertIn('（附件：a.png）', captured['fields']['检查明细'])


class RegressionTests(Base):
    def _white_id(self):
        return f'{DAY}:A:白'

    def _upload_image(self, white, item_id, name, kind='item', actor=None):
        actor = actor or self.worker
        task = self.service.task(actor, white)
        session = self.service.stage_upload(actor, white, item_id, kind, name,
                                            revision=task['revision'] if task['submitted'] else None)
        make_png(session['path'])
        size = pathlib.Path(session['path']).stat().st_size
        return self.service.commit_upload(actor, session, size), session

    def test_handover_after_submit_reaches_cloud(self):
        self.save('A', 0, [{'id': 'x', 'text': 'X'}], at(DAY + 'T09:00:00'))
        self.prepare(at(DAY + 'T09:00:00'))
        white = self._white_id()
        self.service.submit(self.worker, white, 1, [{'id': 'x', 'status': 'normal', 'note': ''}],
                            at(DAY + 'T09:05:00'))
        meta, _ = self._upload_image(white, '', 'h.png', kind='handover')
        self.service.sync(at(DAY + 'T09:10:00'))
        self.cloud.upsert.assert_called_once()
        tasks = self.cloud.upsert.call_args.args[1]
        handover_refs = [r for t in tasks for r in t['_files'] if r['kind'] == 'handover']
        self.assertEqual([r['file_id'] for r in handover_refs], [meta['id']],
                         'post-submit handover must reach cloud sync')

    def test_second_group_same_shift_shares_same_handover_once(self):
        items = [{'id': 'x', 'text': 'X'},
                 {'id': 'lunch', 'text': '午间', 'frequency': {'kind': 'daily', 'time': '11:00'}}]
        self.save('A', 0, items, at(DAY + 'T09:00:00'))
        self.prepare(at(DAY + 'T11:05:00'))
        white_std = f'{DAY}:A:白'
        white_lunch = f'{DAY}:A:白:1100'
        self.service.submit(self.worker, white_std, 1,
                            [{'id': 'x', 'status': 'normal', 'note': ''}], at(DAY + 'T11:10:00'))
        self.service.submit(self.worker, white_lunch, 1,
                            [{'id': 'lunch', 'status': 'normal', 'note': ''}], at(DAY + 'T11:10:00'))
        meta, _ = self._upload_image(white_std, '', 'h.png', kind='handover')
        with self.service.connection() as conn:
            task_std = conn.execute('SELECT * FROM tasks WHERE id=?', (white_std,)).fetchone()
            task_lunch = conn.execute('SELECT * FROM tasks WHERE id=?', (white_lunch,)).fetchone()
            f_std = self.service._internal_files(conn, dict(task_std))
            f_lunch = self.service._internal_files(conn, dict(task_lunch))
        h_std = [r for r in f_std if r['kind'] == 'handover']
        h_lunch = [r for r in f_lunch if r['kind'] == 'handover']
        self.assertEqual(h_std[0]['file_id'], meta['id'])
        self.assertEqual(h_lunch[0]['file_id'], meta['id'])
        self.assertEqual(len({r['file_id'] for r in h_std + h_lunch}), 1,
                         'same shared handover file id across groups')

    def test_missing_local_evidence_sets_retry(self):
        self.save('A', 0, [{'id': 'x', 'text': 'X'}], at(DAY + 'T09:00:00'))
        self.prepare(at(DAY + 'T09:00:00'))
        white = self._white_id()
        meta, _ = self._upload_image(white, 'x', 'note.png')
        with self.service.connection() as conn:
            row = conn.execute('SELECT * FROM files WHERE id=?', (meta['id'],)).fetchone()
        (self.service.files_dir / row['disk_name']).unlink()
        self.service.submit(self.worker, white, 1, [{'id': 'x', 'status': 'normal', 'note': ''}],
                            at(DAY + 'T09:10:00'))
        self.cloud.upsert.reset_mock()
        self.service.sync(at(DAY + 'T09:10:00'))
        self.cloud.upsert.assert_not_called()
        with self.service.connection() as conn:
            outbox = conn.execute('SELECT retry_at, synced, error FROM outbox').fetchone()
        self.assertGreater(outbox['retry_at'], at(DAY + 'T09:10:00').timestamp())
        self.assertIn('附件缺失', outbox['error'])
        self.assertEqual(outbox['synced'], 0)

    def test_clean_restart_0110_creates_prior_night_task(self):
        self.save('A', 0, [{'id': 'x', 'text': 'X', 'frequency': {'kind': 'daily', 'time': '01:00'}}],
                  at(DAY + 'T09:00:00'))
        requested = []
        def roster(day):
            requested.append(day)
            return {'people': roster_people(), 'warnings': {}}
        self.service.roster = roster
        current = at('2026-10-15T01:10:00')
        self.service.prepare(current)
        tasks = self.service.state(self.worker, 'A', current)['tasks']
        self.assertTrue(any(t['day'] == DAY and t['shift'] == '夜' for t in tasks),
                        '01:00 daily maps to previous roster night')
        self.assertIn(DAY, requested)

    def test_image_extension_mismatch_rejected(self):
        self.save('A', 0, [{'id': 'x', 'text': 'X'}], at(DAY + 'T09:00:00'))
        self.prepare(at(DAY + 'T09:00:00'))
        white = self._white_id()
        session = self.service.stage_upload(self.worker, white, 'x', 'item', 'fake.jpeg')
        make_png(session['path'])  # PNG content but .jpeg extension
        with self.assertRaises(checks.CheckError):
            self.service.commit_upload(self.worker, session,
                                       pathlib.Path(session['path']).stat().st_size)

    def test_document_signature_mismatch_rejected(self):
        self.save('A', 0, [{'id': 'x', 'text': 'X'}], at(DAY + 'T09:00:00'))
        self.prepare(at(DAY + 'T09:00:00'))
        white = self._white_id()
        for name, payload in [('fake.pdf', b'MZ\x90\x00executable'),
                              ('fake.doc', b'NOT-OLE'),
                              ('fake.docx', b'not-a-zip'),
                              ('fake.txt', b'\xff\xfe\x00garbage')]:
            session = self.service.stage_upload(self.worker, white, 'x', 'item', name)
            pathlib.Path(session['path']).write_bytes(payload)
            with self.assertRaises(checks.CheckError, msg=name):
                self.service.commit_upload(self.worker, session, len(payload))

    def test_sqlite_failure_removes_new_final_file(self):
        self.save('A', 0, [{'id': 'x', 'text': 'X'}], at(DAY + 'T09:00:00'))
        self.prepare(at(DAY + 'T09:00:00'))
        white = self._white_id()
        session = self.service.stage_upload(self.worker, white, 'x', 'item', 'note.png')
        make_png(session['path'])
        size = pathlib.Path(session['path']).stat().st_size
        before = {p.name for p in self.service.files_dir.iterdir()}
        with patch.object(checks.sqlite3, 'connect',
                          side_effect=factory_connect(checks.sqlite3.connect, FailingCommitConnection)):
            with self.assertRaises(sqlite3.OperationalError):
                self.service.commit_upload(self.worker, session, size)
        after = {p.name for p in self.service.files_dir.iterdir()}
        self.assertEqual(after, before, 'failed commit must leave no orphan final file')

    def test_delete_after_commit_failure_keeps_file(self):
        self.save('A', 0, [{'id': 'x', 'text': 'X'}], at(DAY + 'T09:00:00'))
        self.prepare(at(DAY + 'T09:00:00'))
        white = self._white_id()
        meta, _ = self._upload_image(white, 'x', 'note.png')
        with self.service.connection() as conn:
            row = dict(conn.execute('SELECT * FROM files WHERE id=?', (meta['id'],)).fetchone())
        with patch.object(checks.sqlite3, 'connect',
                          side_effect=factory_connect(checks.sqlite3.connect, FailingCommitConnection)):
            with self.assertRaises(sqlite3.OperationalError):
                self.service.delete_file(self.worker, meta['id'], at(DAY + 'T09:00:00'))
        with self.service.connection() as conn:
            self.assertIsNotNone(conn.execute('SELECT id FROM files WHERE id=?', (meta['id'],)).fetchone())
        self.assertTrue((self.service.files_dir / row['disk_name']).exists(),
                        'file must not be unlinked when DB commit fails')

    def test_definition_version_change_during_upload_409(self):
        self.save('A', 0, [{'id': 'x', 'text': 'X'}], at(DAY + 'T09:00:00'))
        self.prepare(at(DAY + 'T09:00:00'))
        white = self._white_id()
        session = self.service.stage_upload(self.worker, white, 'x', 'item', 'note.png')
        make_png(session['path'])
        size = pathlib.Path(session['path']).stat().st_size
        self.save('A', 1, [{'id': 'new', 'text': 'N'}], at(DAY + 'T09:00:00'))
        with self.assertRaises(checks.CheckError) as exc:
            self.service.commit_upload(self.worker, session, size)
        self.assertEqual(exc.exception.status, 409)

    def test_removed_item_file_never_synced(self):
        items = [{'id': 'x', 'text': 'X'}, {'id': 'y', 'text': 'Y'}]
        self.save('A', 0, items, at(DAY + 'T09:00:00'))
        self.prepare(at(DAY + 'T09:00:00'))
        white = self._white_id()
        meta, _ = self._upload_image(white, 'y', 'y.png')
        self.save('A', 1, [{'id': 'x', 'text': 'X'}], at(DAY + 'T09:00:00'))
        with self.service.connection() as conn:
            task = dict(conn.execute('SELECT * FROM tasks WHERE id=?', (white,)).fetchone())
            version = task['definition_version']
        self.service.submit(self.worker, white, version, [{'id': 'x', 'status': 'normal', 'note': ''}],
                            at(DAY + 'T09:10:00'))
        with self.service.connection() as conn:
            task = dict(conn.execute('SELECT * FROM tasks WHERE id=?', (white,)).fetchone())
            files = self.service._internal_files(conn, task)
        self.assertFalse(any(r['item_id'] == 'y' for r in files),
                         'removed definition item must never sync as orphan attachment')

    def test_handover_can_delete_flag_matches_uploader(self):
        self.save('A', 0, [{'id': 'x', 'text': 'X'}], at(DAY + 'T09:00:00'))
        self.prepare(at(DAY + 'T09:00:00'))
        white = self._white_id()
        self._upload_image(white, '', 'h.png', kind='handover')
        uploader_state = self.service.state(self.worker, 'A', at(DAY + 'T09:00:00'))
        peer_state = self.service.state(self.peer, 'A', at(DAY + 'T09:00:00'))
        uploader_task = next(t for t in uploader_state['tasks'] if t['id'] == white)
        peer_task = next(t for t in peer_state['tasks'] if t['id'] == white)
        self.assertTrue(uploader_task['handover_photo']['can_delete'])
        self.assertFalse(peer_task['handover_photo']['can_delete'])

    def test_retry_same_item_upload_returns_existing_without_duplicate(self):
        self.save('A', 0, [{'id': 'x', 'text': 'X'}], at(DAY + 'T09:00:00'))
        self.prepare(at(DAY + 'T09:00:00'))
        white = self._white_id()
        meta1, _ = self._upload_image(white, 'x', 'note.png')
        meta2, _ = self._upload_image(white, 'x', 'note.png')
        self.assertEqual(meta1['id'], meta2['id'], 'same content retry must return original file')
        with self.service.connection() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM files WHERE task_id=? AND item_id=? AND kind='item'",
                (white, 'x')).fetchone()[0]
        self.assertEqual(count, 1)

    def test_handover_retry_same_uploader_same_hash_returns_existing_and_conflicts_otherwise(self):
        self.save('A', 0, [{'id': 'x', 'text': 'X'}], at(DAY + 'T09:00:00'))
        self.prepare(at(DAY + 'T09:00:00'))
        white = self._white_id()
        meta1, _ = self._upload_image(white, '', 'h.png', kind='handover')
        meta2, _ = self._upload_image(white, '', 'h.png', kind='handover')
        self.assertEqual(meta1['id'], meta2['id'], 'same uploader+hash retry returns existing handover')
        # different photo by same uploader -> conflict
        session = self.service.stage_upload(self.worker, white, '', 'handover', 'h2.png')
        PIL.Image.new('RGB', (3, 3), (1, 2, 3)).save(str(session['path']) + '.png')
        shutil.copy2(str(session['path']) + '.png', session['path'])
        with self.assertRaises(checks.CheckError) as exc:
            self.service.commit_upload(self.worker, session, pathlib.Path(session['path']).stat().st_size)
        self.assertEqual(exc.exception.status, 409)
        # different uploader of same photo -> still conflict
        session = self.service.stage_upload(self.peer, white, '', 'handover', 'h.png')
        make_png(session['path'])
        with self.assertRaises(checks.CheckError) as exc:
            self.service.commit_upload(self.peer, session, pathlib.Path(session['path']).stat().st_size)
        self.assertEqual(exc.exception.status, 409)

    def test_image_verify_no_global_mutation_and_overlimit_before_decode(self):
        self.save('A', 0, [{'id': 'x', 'text': 'X'}], at(DAY + 'T09:00:00'))
        self.prepare(at(DAY + 'T09:00:00'))
        white = self._white_id()
        original = PIL.Image.MAX_IMAGE_PIXELS
        # Overlimit image is rejected before verify/load (no decode), via a fake open
        # that exposes a >25M-pixel header but never actually decodes pixels.
        big = Mock()
        big.format = 'PNG'
        big.width = 6000
        big.height = 5000  # 30_000_000 pixels > 25_000_000
        big.load = Mock()
        big.verify = Mock()
        big.__enter__ = Mock(return_value=big)
        big.__exit__ = Mock(return_value=False)
        with patch.object(PIL.Image, 'open', return_value=big):
            with self.assertRaises(checks.CheckError) as exc:
                self.service._verify_image('oversize.png', '.png')
        self.assertIn('像素过大', str(exc.exception))
        big.verify.assert_not_called()
        big.load.assert_not_called()
        self.assertEqual(PIL.Image.MAX_IMAGE_PIXELS, original)
        # Concurrent valid image verifications never mutate the shared Pillow global.
        def run_verify(i):
            p = pathlib.Path(self.temp) / f'ok_{i}.png'
            make_png(p)
            self.service._verify_image(p, '.png')
        with ThreadPoolExecutor(4) as pool:
            list(pool.map(run_verify, range(8)))
        self.assertEqual(PIL.Image.MAX_IMAGE_PIXELS, original)

    def test_handover_actor_change_rejected(self):
        self.save('A', 0, [{'id': 'x', 'text': 'X'}], at(DAY + 'T09:00:00'))
        self.prepare(at(DAY + 'T09:00:00'))
        white = self._white_id()
        session = self.service.stage_upload(self.worker, white, '', 'handover', 'h.png')
        make_png(session['path'])
        size = pathlib.Path(session['path']).stat().st_size
        other = {**self.worker, 'id': 'ou_A_other', 'name': '另一个人'}
        with self.assertRaises(checks.CheckError) as exc:
            self.service.commit_upload(other, session, size)
        self.assertEqual(exc.exception.status, 409)

    def test_route_always_aborts_staging_after_successful_dedup(self):
        """The upload route finally must always abort staging; a deduped success
        leaves an unused staging file that must not leak. 'abort' must never remove
        the already-committed final file."""
        self.save('A', 0, [{'id': 'x', 'text': 'X'}], at(DAY + 'T09:00:00'))
        self.prepare(at(DAY + 'T09:00:00'))
        white = self._white_id()
        meta1, _ = self._upload_image(white, 'x', 'note.png')
        # Second identical upload returns existing ref without moving staging.
        session = self.service.stage_upload(self.worker, white, 'x', 'item', 'note.png')
        make_png(session['path'])
        size = pathlib.Path(session['path']).stat().st_size
        result = self.service.commit_upload(self.worker, session, size)
        self.assertEqual(result['id'], meta1['id'])
        # The route finally now always invokes abort_upload, even on committed success.
        self.service.abort_upload(session)
        leftover = [p for p in self.service.staging_dir.iterdir() if p.name.endswith('.upload')]
        self.assertEqual(leftover, [], 'success dedup must leave zero staging files')
        with self.service.connection() as conn:
            row = conn.execute('SELECT disk_name FROM files WHERE id=?', (meta1['id'],)).fetchone()
        self.assertTrue((self.service.files_dir / row['disk_name']).exists(),
                        'abort must not delete the committed final file')

    def test_staged_hash_read_error_propagates(self):
        """SHA-256 read errors must propagate instead of silently hashing to empty."""
        self.save('A', 0, [{'id': 'x', 'text': 'X'}], at(DAY + 'T09:00:00'))
        self.prepare(at(DAY + 'T09:00:00'))
        white = self._white_id()
        session = self.service.stage_upload(self.worker, white, 'x', 'item', 'note.png')
        make_png(session['path'])
        size = pathlib.Path(session['path']).stat().st_size
        with patch.object(checks.hashlib, 'file_digest', side_effect=OSError('boom')):
            with self.assertRaises(OSError):
                self.service.commit_upload(self.worker, session, size)

    def test_reconcile_skips_unchanged_update(self):
        self.save('A', 0, [{'id': 'x', 'text': 'X'}], at(DAY + 'T09:00:00'))
        self.prepare(at(DAY + 'T09:00:00'))
        class Counter(TrackingConnection):
            total = 0

            def execute(self, sql, *args, **kwargs):
                if isinstance(sql, str) and sql.strip().upper().startswith('UPDATE tasks'):
                    Counter.total += 1
                return super().execute(sql, *args, **kwargs)

        with patch.object(checks.sqlite3, 'connect',
                          side_effect=factory_connect(checks.sqlite3.connect, Counter)):
            self.prepare(at(DAY + 'T09:00:00'))
        self.assertEqual(Counter.total, 0, 'unchanged task must not be UPDATEed')


if __name__ == '__main__':
    unittest.main()

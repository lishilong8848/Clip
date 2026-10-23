"""Submitted checklist edits use temporary SQLite/files and fake delivery only."""
import asyncio
import copy
import datetime as dt
import io
import json
import os
import sqlite3
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import httpx
from fastapi import FastAPI
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal import pre_shift_checks as checks, pre_shift_routes
from bin.test_pre_shift_extensions import Base, DAY, at


class SubmittedEditTests(Base):
    def setUp(self):
        super().setUp()
        self.enterContext(patch('socket.create_connection', side_effect=AssertionError('Network forbidden')))
        self.enterContext(patch('openclaw_service.assistant.lighthouse_files.recognize_text', return_value=''))
        self.current = at(DAY + 'T09:00:00')
        self.save('A', 0, [{'id': 'first', 'text': 'First item'}, {'id': 'last', 'text': 'Last item'}])
        self.prepare(self.current)
        self.task_id = DAY + ':A:\u767d'
        self.answers = [{'id': key, 'status': 'normal', 'note': ''} for key in ('first', 'last')]

    def snapshot(self, task_id=None, actor=None):
        return self.service.task(actor or self.worker, task_id or self.task_id, self.current)

    def submit_task(self, task_id=None, actor=None):
        identity = task_id or self.task_id
        task = self.snapshot(identity, actor)
        return self.service.submit(actor or self.worker, identity, task['definition_version'], self.answers, self.current)

    def changed_answers(self, note='Changed observation'):
        return [self.answers[0], {'id': 'last', 'status': 'abnormal', 'note': note}]

    def image(self, color='red'):
        stream = io.BytesIO()
        Image.new('RGB', (20, 20), color).save(stream, format='PNG')
        return stream.getvalue()

    def staged(self, *, kind='item', item_id='last', task_id=None, revision=None, replace_file_id=None, content=None, actor=None):
        actor = actor or self.worker
        session = self.service.stage_upload(actor, task_id or self.task_id, item_id if kind == 'item' else '',
            kind, 'fixture.png', self.current, revision=revision, replace_file_id=replace_file_id)
        Path(session['path']).write_bytes(content or self.image())
        session['photo_capture'] = {'watermark': {'enabled': False}}
        return session

    def upload(self, **kwargs):
        session = self.staged(**kwargs)
        return self.service.commit_upload(kwargs.get('actor') or self.worker, session, None, self.current)

    def clone_task(self, identity, **changes):
        with self.service.connection(True) as conn:
            row = dict(conn.execute('SELECT * FROM tasks WHERE id=?', (self.task_id,)).fetchone())
            row.update(id=identity, submitted='', actor_id='', actor_name='', cloud_key='', answers='', evidence='',
                       revision=1, updated_by='', updated_by_name='', updated_at='')
            row.update(changes)
            conn.execute('INSERT INTO tasks(' + ','.join(row) + ') VALUES(' + ','.join('?' for _ in row) + ')', list(row.values()))
        return identity

    def outbox(self):
        with self.service.connection() as conn:
            return [dict(row) for row in conn.execute('SELECT * FROM outbox ORDER BY id')]

    def test_answer_edit_preserves_submitter_identity_first_writer_and_reminders(self):
        self.submit_task()
        original = self.snapshot()
        result = self.service.edit(self.peer, self.task_id, 1, self.changed_answers(), self.current)
        self.assertEqual(result['revision'], 2)
        for key in ('submitted', 'actor_id', 'actor_name', 'cloud_key'):
            self.assertEqual(result[key], original[key])
        self.assertEqual(result['updated_by'], self.peer['id'])
        self.assertEqual(result['updated_by_name'], self.peer['name'])
        self.assertTrue(result['can_edit'])
        self.assertEqual(self.service.edit(self.peer, self.task_id, 1, self.changed_answers(), self.current), result)
        with self.assertRaises(checks.CheckError) as stale:
            self.service.edit(self.worker, self.task_id, 1, self.answers, self.current)
        self.assertEqual(stale.exception.status, 409)
        self.assertTrue(self.submit_task(actor=self.peer)['already_completed'])
        self.assertEqual(self.snapshot()['answers'], result['answers'])
        self.service.notify(self.current + dt.timedelta(hours=2))
        self.sender.assert_not_called()
        self.assertEqual(len(self.outbox()), 1)
        self.assertEqual(self.outbox()[0]['revision'], 2)
        with self.service.connection() as conn:
            audit = conn.execute('SELECT * FROM task_changes').fetchall()
            self.assertEqual(len(audit), 1)
            self.assertEqual(json.loads(audit[0]['before_json'])['answers'], original['answers'])
        history = self.service.history(self.worker, 'A', current=self.current)['items'][0]
        self.assertEqual((history['revision'], history['can_edit']), (2, True))

    def test_final_item_photo_after_submit_is_published_and_removed_without_destroying_original(self):
        self.submit_task()
        uploaded = self.upload(revision=1)
        self.assertEqual(uploaded['revision'], 2)
        self.assertEqual(uploaded['item_id'], 'last')
        path = Path(self.service.get_file(self.worker, uploaded['id'])['path'])
        original_bytes = path.read_bytes()
        self.assertEqual([f['id'] for f in self.snapshot()['files']], [uploaded['id']])
        self.service.sync(self.current)
        self.assertEqual(self.cloud.upsert.call_args.args[1][0]['_files'][0]['file_id'], uploaded['id'])
        removed = self.service.delete_file(self.peer, uploaded['id'], self.current, revision=2, task_id=self.task_id)
        self.assertEqual(removed['revision'], 3)
        self.assertEqual(self.snapshot()['files'], [])
        self.assertEqual(path.read_bytes(), original_bytes)
        self.assertEqual(self.service.delete_file(self.peer, uploaded['id'], self.current,
            revision=2, task_id=self.task_id), removed)
        with self.service.connection() as conn:
            self.assertEqual(conn.execute('SELECT archived FROM files').fetchone()[0], 1)
            audit = json.loads(conn.execute('SELECT before_json FROM task_changes WHERE revision=2').fetchone()[0])
            self.assertEqual(audit['evidence']['item_files'][0]['id'], uploaded['id'])
        self.service.sync(self.current)
        self.assertEqual(self.cloud.upsert.call_args.args[1][0]['_files'], [])
        self.assertEqual(self.cloud.upsert.call_args.args[2], 'rec_test')

    def test_stale_upload_retry_requires_identical_content_name_item_and_replacement(self):
        self.submit_task()
        first = self.upload(revision=1)
        replay = self.upload(revision=1)
        self.assertEqual((replay['id'], replay['revision']), (first['id'], 2))
        for options in ({'content': self.image('blue')}, {'item_id': 'first'}):
            with self.subTest(options=list(options)), self.assertRaises(checks.CheckError) as error:
                self.upload(revision=1, **options)
            self.assertEqual(error.exception.status, 409)
        session = self.staged(revision=1)
        session['name'] = 'different.png'
        with self.assertRaises(checks.CheckError):
            self.service.commit_upload(self.worker, session, None, self.current)
        self.service.edit(self.peer, self.task_id, 2, self.changed_answers(), self.current)
        with self.assertRaises(checks.CheckError):
            self.upload(revision=1)
        with self.service.connection() as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM files').fetchone()[0], 1)

    def test_shared_handover_replacement_updates_only_exact_shift_and_overview(self):
        extra = self.clone_task(self.task_id + ':1100', due=self.current.timestamp() + 7200)
        next_day = self.clone_task('next-day', day='2026-10-15')
        night = DAY + ':A:\u591c'
        for identity in (self.task_id, extra, next_day, night):
            self.submit_task(identity)
        item = self.upload(task_id=extra, revision=1, item_id='first', content=self.image('blue'))
        before = {identity: self.snapshot(identity)['revision'] for identity in (self.task_id, extra, next_day, night)}
        old = self.upload(kind='handover', revision=before[self.task_id])
        original = Path(self.service.get_file(self.worker, old['id'])['path']).read_bytes()
        for identity in (self.task_id, extra):
            self.assertEqual(self.snapshot(identity)['revision'], before[identity] + 1)
        replacement = self.upload(kind='handover', revision=old['revision'], replace_file_id=old['id'], content=self.image('green'))
        for identity in (self.task_id, extra):
            self.assertEqual(self.snapshot(identity)['handover_photo']['id'], replacement['id'])
            self.assertEqual(self.snapshot(identity)['revision'], before[identity] + 2)
        for identity in (next_day, night):
            self.assertIsNone(self.snapshot(identity)['handover_photo'])
            self.assertEqual(self.snapshot(identity)['revision'], before[identity])
        self.assertEqual(self.snapshot(extra)['files'][0]['id'], item['id'])
        self.assertEqual(Path(self.service.get_file(self.worker, old['id'])['path']).read_bytes(), original)
        overview = self.service.state(self.admin, 'A', self.current)['overview']['rows']
        self.assertEqual(next(row for row in overview if row['id'] == self.task_id)['handover_photo']['url'], replacement['url'])
        self.assertEqual(self.upload(kind='handover', revision=old['revision'], replace_file_id=old['id'],
            content=self.image('green'))['id'], replacement['id'])
        with self.assertRaises(checks.CheckError):
            self.upload(kind='handover', revision=old['revision'], replace_file_id=replacement['id'], content=self.image('green'))
        with self.assertRaises(checks.CheckError):
            self.service.delete_file(self.worker, replacement['id'], self.current, revision=replacement['revision'])
        self.service.delete_file(self.worker, replacement['id'], self.current,
            revision=self.snapshot(extra)['revision'], task_id=extra)
        self.assertIsNone(self.snapshot()['handover_photo'])
        self.assertIsNone(self.snapshot(extra)['handover_photo'])
        self.assertEqual(self.snapshot(extra)['files'][0]['id'], item['id'])
        self.service.sync(self.current)
        latest = [task for call in self.cloud.upsert.call_args_list for task in call.args[1]]
        self.assertFalse(any(ref['kind'] == 'handover' for task in latest for ref in task['_files']))

    def test_preview_retains_revision_and_replacement_and_rejects_intervening_edit(self):
        self.submit_task()
        old = self.upload(kind='handover', revision=1)
        session = self.staged(kind='handover', revision=2, replace_file_id=old['id'], content=self.image('blue'))
        preview = self.service.retain_photo_preview(self.worker, session, Path(session['path']).stat().st_size, self.current)
        self.assertEqual(preview['revision'], 2)
        self.service.edit(self.peer, self.task_id, 2, self.changed_answers(), self.current)
        with self.assertRaises(checks.CheckError) as error:
            self.service.commit_photo_preview(self.worker, preview['preview_id'], {'enabled': False}, False, self.current)
        self.assertEqual(error.exception.status, 409)
        self.assertEqual(self.snapshot()['handover_photo']['id'], old['id'])
        session = self.staged(kind='handover', revision=3, replace_file_id=old['id'], content=self.image('green'))
        preview = self.service.retain_photo_preview(self.worker, session, Path(session['path']).stat().st_size, self.current)
        with patch.object(self.service, '_complete_photo_preview', side_effect=sqlite3.OperationalError('lost receipt')):
            result = self.service.commit_photo_preview(self.worker, preview['preview_id'], {'enabled': False}, False, self.current)
        replay = self.service.commit_photo_preview(self.worker, preview['preview_id'], {'enabled': False}, False, self.current)
        self.assertEqual((replay['id'], replay['revision']), (result['id'], 4))
        self.assertEqual(self.snapshot()['handover_photo']['id'], replay['id'])

    def test_invalid_replacement_and_lease_or_commit_failure_leave_old_photo_active(self):
        self.submit_task()
        old = self.upload(kind='handover', revision=1)
        for failure in ('mime', 'lease', 'transaction'):
            with self.subTest(failure=failure):
                session = self.staged(kind='handover', revision=2, replace_file_id=old['id'], content=self.image('blue'))
                if failure == 'mime':
                    Path(session['path']).write_bytes(b'not an image')
                if failure == 'lease':
                    with self.service.connection(True) as conn:
                        conn.execute('UPDATE photo_deliveries SET lease=? WHERE file_id=?', (self.current.timestamp() + 60, old['id']))
                if failure == 'transaction':
                    with patch.object(self.service, '_publish_change', side_effect=sqlite3.OperationalError('disk fixture')):
                        with self.assertRaises(sqlite3.OperationalError):
                            self.service.commit_upload(self.worker, session, None, self.current)
                else:
                    with self.assertRaises(checks.CheckError):
                        self.service.commit_upload(self.worker, session, None, self.current)
                with self.service.connection(True) as conn:
                    conn.execute('UPDATE photo_deliveries SET lease=0')
                    self.assertEqual(conn.execute('SELECT COUNT(*) FROM files').fetchone()[0], 1)
                self.assertEqual(self.snapshot()['revision'], 2)
                self.assertEqual(self.snapshot()['handover_photo']['id'], old['id'])

    def test_permissions_validation_caps_and_parallel_edits(self):
        self.submit_task()
        outsider = {**self.worker, 'home_scope': 'B', 'scope_rules': ['B']}
        for operation in (lambda: self.snapshot(actor=outsider),
                lambda: self.service.edit(outsider, self.task_id, 1, self.answers, self.current),
                lambda: self.staged(actor=outsider, revision=1)):
            with self.assertRaises(checks.CheckError) as error:
                operation()
            self.assertEqual(error.exception.status, 403)
        for bad in (True, 0, '1', None, 10**100):
            with self.assertRaises(checks.CheckError):
                self.service.edit(self.worker, self.task_id, bad, self.answers, self.current)
        barrier = threading.Barrier(2)
        def edit(index):
            barrier.wait()
            try:
                return self.service.edit(self.worker, self.task_id, 1, self.changed_answers(str(index)), self.current)['revision']
            except checks.CheckError as exc:
                return exc.status
        with ThreadPoolExecutor(2) as pool:
            self.assertEqual(sorted(pool.map(edit, (0, 1))), [2, 409])
        with patch.object(checks, 'MAX_FILES_PER_ITEM', 1):
            self.upload(revision=2)
            with self.assertRaises(checks.CheckError):
                self.upload(revision=3, content=self.image('blue'))
        self.assertEqual(self.snapshot()['revision'], 3)

    def test_sync_claim_and_edit_during_upload_never_mark_new_revision_synced(self):
        self.submit_task()
        old = self.upload(revision=1)
        entered, release = threading.Event(), threading.Event()
        def slow(key, tasks, record_id):
            entered.set()
            self.assertTrue(release.wait(10))
            self.assertTrue(Path(tasks[0]['_files'][0]['path']).is_file())
            return 'rec_original'
        self.cloud.upsert.side_effect = slow
        with ThreadPoolExecutor(2) as pool:
            first = pool.submit(self.service.sync, self.current)
            self.assertTrue(entered.wait(10))
            try:
                self.service.delete_file(self.worker, old['id'], self.current, revision=2, task_id=self.task_id)
                other = checks.CheckService(self.temp, cloud=self.cloud)
                other.sync(self.current)
                self.assertEqual(self.cloud.upsert.call_count, 1)
            finally:
                release.set()
            first.result(timeout=10)
        row = self.outbox()[0]
        self.assertEqual((row['revision'], row['synced']), (3, 2))
        self.assertEqual(row['record_id'], 'rec_original')
        self.cloud.upsert.side_effect = None
        self.cloud.upsert.return_value = 'rec_original'
        self.service.sync(self.current)
        self.assertEqual(self.cloud.upsert.call_args.args[2], 'rec_original')
        self.assertEqual(self.cloud.upsert.call_args.args[1][0]['_files'], [])
        self.assertEqual(self.outbox()[0]['synced'], 3)

    def test_snapshot_and_upload_response_keep_their_own_revision_during_concurrent_edit(self):
        self.submit_task()
        old = self.upload(kind='handover', revision=1)
        original = self.service._state_handover
        changed = []
        def overlap(conn, actor, task, current):
            if not changed:
                changed.append(self.upload(kind='handover', revision=2, replace_file_id=old['id'], content=self.image('blue')))
            return original(conn, actor, task, current)
        with patch.object(self.service, '_state_handover', side_effect=overlap):
            snapshot = self.snapshot()
        self.assertEqual((snapshot['revision'], snapshot['handover_photo']['id']), (2, old['id']))
        self.assertEqual(self.snapshot()['revision'], 3)
        connection = self.service.connection
        raced = []
        @contextmanager
        def commit_then_edit(write=False):
            with connection(write) as conn:
                yield conn
            if write and not raced:
                raced.append(True)
                self.service.edit(self.peer, self.task_id, 4, self.changed_answers(), self.current)
        with patch.object(self.service, 'connection', side_effect=commit_then_edit):
            uploaded = self.upload(revision=3)
        self.assertEqual(uploaded['revision'], 4)
        self.assertEqual(self.snapshot()['revision'], 5)

    def test_expired_cloud_worker_finishing_last_requeues_latest_snapshot(self):
        self.submit_task()
        old = self.upload(revision=1)
        entered, release = threading.Event(), threading.Event()
        published = []
        lock = threading.Lock()
        calls = []
        def publish(key, tasks, record_id):
            with lock:
                number = len(calls)
                calls.append((key, record_id))
            if number == 0:
                entered.set()
                self.assertTrue(release.wait(10))
            published.append([ref['file_id'] for task in tasks for ref in task['_files']])
            return 'rec_original'
        self.cloud.upsert.side_effect = publish
        with ThreadPoolExecutor(2) as pool:
            first = pool.submit(self.service.sync, self.current)
            self.assertTrue(entered.wait(10))
            try:
                self.service.delete_file(self.worker, old['id'], self.current, revision=2, task_id=self.task_id)
                self.service.sync(self.current + dt.timedelta(seconds=301))
            finally:
                release.set()
            first.result(timeout=10)
        self.assertEqual(published, [[], [old['id']]])
        self.assertGreater(self.outbox()[0]['revision'], self.outbox()[0]['synced'])
        self.service.sync(self.current + dt.timedelta(seconds=302))
        self.assertEqual(published[-1], [])
        self.assertEqual(len({key for key, _ in calls}), 1)
        self.assertEqual(calls[-1][1], 'rec_original')

    def test_expired_cloud_worker_write_then_response_loss_requeues_latest_snapshot(self):
        self.submit_task()
        old = self.upload(revision=1)
        entered, release = threading.Event(), threading.Event()
        published, calls = [], []
        def publish(key, tasks, record_id):
            files = [ref['file_id'] for task in tasks for ref in task['_files']]
            calls.append((key, record_id))
            if files:
                entered.set()
                self.assertTrue(release.wait(10))
            published.append(files)
            if files:
                raise OSError('response lost after cloud write')
            return 'rec_original'
        self.cloud.upsert.side_effect = publish
        takeover = self.current + dt.timedelta(seconds=301)
        with ThreadPoolExecutor(2) as pool:
            first = pool.submit(self.service.sync, self.current)
            self.assertTrue(entered.wait(10))
            try:
                self.service.delete_file(self.worker, old['id'], self.current, revision=2, task_id=self.task_id)
                self.service.sync(takeover)
                self.assertEqual((self.outbox()[0]['revision'], self.outbox()[0]['synced']), (3, 3))
                self.service.wake.clear()
            finally:
                release.set()
            first.result(timeout=10)
        self.assertEqual(published, [[], [old['id']]])
        row = self.outbox()[0]
        self.assertGreater(row['revision'], row['synced'])
        self.assertEqual((row['record_id'], row['retry_at']), ('rec_original', 0))
        self.assertTrue(self.service.wake.is_set())
        self.service.sync(takeover)
        self.assertEqual(published, [[], [old['id']], []])
        self.assertEqual(len({key for key, _ in calls}), 1)
        self.assertEqual(calls[-1][1], 'rec_original')
        row = self.outbox()[0]
        self.assertEqual(row['revision'], row['synced'])
        self.assertEqual(row['record_id'], 'rec_original')

    def test_old_schema_migrates_in_fresh_process_and_restarts_without_losing_edits(self):
        self.submit_task()
        with self.service.connection(True) as conn:
            conn.execute('DROP TABLE task_changes')
            conn.execute('DROP INDEX one_handover')
            conn.execute("CREATE UNIQUE INDEX one_handover ON files(day,scope,shift) WHERE kind='handover'")
            conn.execute('ALTER TABLE files DROP COLUMN archived')
            for field in ('revision', 'updated_by', 'updated_by_name', 'updated_at'):
                conn.execute('ALTER TABLE tasks DROP COLUMN ' + field)
            for field in ('lease', 'lease_token'):
                conn.execute('ALTER TABLE outbox DROP COLUMN ' + field)
        code = ('import json,sys\nfrom lan_bitable_template_portal.pre_shift_checks import CheckService\n'
                's=CheckService(sys.argv[1])\n'
                'with s.connection() as c:\n print(json.dumps(dict(c.execute("SELECT * FROM tasks WHERE submitted!=?",("",)).fetchone())))')
        result = subprocess.run([sys.executable, '-B', '-c', code, self.temp],
            cwd=Path(__file__).resolve().parent, capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stderr)
        migrated = json.loads(result.stdout)
        self.assertEqual(migrated['revision'], 1)
        self.assertEqual(migrated['updated_by'], self.worker['id'])
        changed = self.service.edit(self.peer, self.task_id, 1, self.changed_answers(), self.current)
        restarted = checks.CheckService(self.temp)
        self.assertEqual(restarted.task(self.worker, self.task_id, self.current), changed)

    def test_routes_expose_revision_and_enforce_body_origin_and_shared_context(self):
        self.submit_task()
        user = {**self.worker, 'open_id': self.worker['id'], 'modules': ['maintenance']}
        async def read_json(request, **kwargs):
            return await request.json()
        controller = SimpleNamespace(_current_session=lambda _: {'user': user},
            _request_base_url=lambda _: 'http://fixture', _read_json_request=read_json)
        runtime = SimpleNamespace(service=Mock(), auth_manager=SimpleNamespace(public_user=lambda _: user))
        app = FastAPI()
        with patch.object(pre_shift_routes, 'CheckService', return_value=self.service), \
                patch('lan_bitable_template_portal.pre_shift_location.PhotoLocation.resolve', return_value=''):
            pre_shift_routes.install_pre_shift_routes(app, controller, runtime)
            async def exercise():
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://fixture') as client:
                    url = '/api/pre-shift-checks/tasks/' + self.task_id
                    task = (await client.get(url)).json()['data']
                    self.assertEqual(task['revision'], 1)
                    self.assertTrue(task['can_edit'])
                    body = {'revision': 1, 'answers': self.changed_answers()}
                    self.assertEqual((await client.put(url, json=body, headers={'Origin': 'https://other.invalid'})).status_code, 403)
                    self.assertEqual((await client.put(url, json={**body, 'actor_name': 'forged'})).status_code, 400)
                    result = await client.put(url, json=body)
                    self.assertEqual(result.status_code, 200, result.text)
                    self.assertEqual(result.json()['data']['revision'], 2)
                    params = {'task_id': self.task_id, 'kind': 'item', 'item_id': 'last', 'name': 'fixture.png', 'revision': 2}
                    result = await client.post('/api/pre-shift-checks/files', params=params, content=self.image())
                    self.assertEqual(result.status_code, 200, result.text)
                    uploaded = result.json()['data']
                    self.assertEqual(uploaded['revision'], 3)
                    result = await client.delete(uploaded['url'], params={'task_id': self.task_id, 'revision': 3})
                    self.assertEqual(result.json()['data']['revision'], 4)
                    self.assertEqual((await client.put(url, json={'revision': 3, 'answers': self.answers})).status_code, 409)
                    from urllib.parse import quote
                    headers = {'X-Photo-Context': quote(json.dumps({'watermark': {'enabled': False}}))}
                    handover = {'task_id': self.task_id, 'kind': 'handover', 'name': 'fixture.png', 'revision': 4}
                    response = await client.post('/api/pre-shift-checks/files', params=handover, content=self.image(), headers=headers)
                    self.assertEqual(response.status_code, 200, response.text)
                    photo = response.json()['data']
                    self.assertEqual(photo['revision'], 5)
                    params = {**handover, 'revision': 5, 'replace_file_id': photo['id'], 'retain': 1}
                    response = await client.post('/api/pre-shift-checks/photo-preview', params=params,
                        content=self.image('blue'), headers=headers)
                    self.assertEqual(response.status_code, 200, response.text)
                    preview = response.json()['data']
                    self.assertEqual(preview['revision'], 5)
                    confirmation = {'preview_id': preview['preview_id'], 'watermark': {'enabled': False}, 'remember_style': False}
                    updated = await client.put(url, json={'revision': 5, 'answers': self.answers})
                    self.assertEqual(updated.json()['data']['revision'], 6)
                    self.assertEqual((await client.post('/api/pre-shift-checks/files', json=confirmation)).status_code, 409)
                    response = await client.post('/api/pre-shift-checks/photo-preview', params={**params, 'revision': 6},
                        content=self.image('green'), headers=headers)
                    self.assertEqual(response.status_code, 200, response.text)
                    confirmation['preview_id'] = response.json()['data']['preview_id']
                    response = await client.post('/api/pre-shift-checks/files', json=confirmation)
                    self.assertEqual(response.status_code, 200, response.text)
                    self.assertEqual(response.json()['data']['revision'], 7)
                    replay = await client.post('/api/pre-shift-checks/files', json=confirmation)
                    self.assertEqual(replay.json()['data']['id'], response.json()['data']['id'])
            asyncio.run(exercise())


if __name__ == '__main__':
    import unittest
    unittest.main()

"""Offline attachment context, local preparation and shared-index regressions."""
import copy
from concurrent.futures import ThreadPoolExecutor
import io
import json
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from bin.test_document_library import actor, configuration
from bin.test_document_library_sync import SyncFixture
from bin.test_document_library_remote import RemoteTestCase, RECORD_ID, FILE_TOKEN, AssistantError as RemoteError
from bin.test_lighthouse_knowledge import FakeEmbedder, FakeMemoryIndex
from lan_bitable_template_portal.document_library import DocumentLibraryService
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_knowledge import KnowledgeBase


def response(status=200, content=b'original file bytes', headers=None, fail=None):
    def chunks(size):
        for offset in range(0, len(content), size):
            yield content[offset:offset + size]
        if fail:
            raise fail
    return SimpleNamespace(status_code=status, headers=headers or {}, iter_bytes=chunks)


class AttachmentContextTests(RemoteTestCase):
    def test_advanced_permission_context_and_cdn_redirect_drop_bearer_and_extra(self):
        client = self.use_client()
        client.download_responses = [response(302, headers={'location': 'https://docs.feishucdn.com/signed?key=opaque'}), response()]
        self.assertEqual(self.remote.download(FILE_TOKEN, record_id=RECORD_ID, field_id='fldFiles'), b'original file bytes')
        first, second = self.calls
        self.assertEqual(json.loads(first['params']['extra']), {'bitablePerm': {
            'tableId': self.remote.table_id, 'attachments': {'fldFiles': {RECORD_ID: [FILE_TOKEN]}}}})
        self.assertTrue(first['headers']['Authorization'])
        self.assertNotIn('Authorization', second['headers'])
        self.assertIsNone(second['params'])
        self.assertFalse(second['follow_redirects'])
        self.assertEqual(self.guard_calls, 0)

    def test_invalid_context_is_rejected_before_network(self):
        for record, field in ((None, 'fldFiles'), (RECORD_ID, None), ('rec/evil', 'fldFiles'), (RECORD_ID, 'fld?evil')):
            with self.assertRaises(RemoteError) as caught:
                self.remote.download(FILE_TOKEN, record_id=record, field_id=field)
            self.assertEqual(caught.exception.category, 'invalid_attachment_context')
        self.assertEqual(self.calls, [])

    def test_http_401_refreshes_once_and_403_or_429_do_not_blind_retry(self):
        client = self.use_client()
        client.download_responses = [response(401, b'{}'), response()]
        self.remote.download(FILE_TOKEN, record_id=RECORD_ID, field_id='fldFiles')
        self.assertEqual(self.manager.forced, [False, True])
        self.assertEqual(self.calls[0]['params'], self.calls[1]['params'])
        for status, code in ((403, 'unauthorized'), (429, 'rate_limit')):
            before = len(self.calls)
            client.download_responses = [response(status, b'{"code":0}')]
            with self.assertRaises(RemoteError) as caught:
                self.remote.download(FILE_TOKEN)
            self.assertEqual(caught.exception.category, code)
            self.assertEqual(len(self.calls), before + 1)
        before = len(self.calls)
        client.download_responses = [response(401, b'{}'), response(401, b'{}')]
        with self.assertRaises(RemoteError) as caught:
            self.remote.download(FILE_TOKEN)
        self.assertEqual(caught.exception.category, 'token_rejected')
        self.assertEqual(len(self.calls), before + 2)

    def test_bad_codes_and_unsafe_redirects_fail_closed(self):
        client = self.use_client()
        for value in ([], {}, '99991663'):
            client.download_responses = [response(200, json.dumps({'code': value}).encode(), {'content-type': 'application/json'})]
            with self.assertRaises(RemoteError) as caught:
                self.remote.download(FILE_TOKEN)
            self.assertEqual(caught.exception.category, 'bad_response')
        for url in ('http://docs.feishu.cn/a', 'https://127.0.0.1/a', 'https://feishu.cn.evil.test/a',
                    'https://user:secret@docs.feishu.cn/a', 'https://docs.feishu.cn:8080/a',
                    'https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal', '/other-api', 'https://[::1]/a'):
            before = len(self.calls)
            client.download_responses = [response(302, headers={'location': url})]
            with self.assertRaises(RemoteError) as caught:
                self.remote.download(FILE_TOKEN)
            self.assertEqual(caught.exception.category, 'unsafe_redirect')
            self.assertEqual(len(self.calls), before + 1)

    def test_encoded_content_length_and_truncated_response(self):
        client = self.use_client()
        client.download_responses = [response(content=b'decoded-body', headers={'content-encoding': 'gzip', 'content-length': '8'})]
        self.assertEqual(self.remote.download(FILE_TOKEN), b'decoded-body')
        client.download_responses = [response(content=b'short', headers={'content-length': '99'})]
        with self.assertRaises(RemoteError) as caught:
            self.remote.download(FILE_TOKEN)
        self.assertEqual(caught.exception.category, 'bad_response')

    def test_stream_interruption_removes_part_and_checks_rotation_after_bytes(self):
        client = self.use_client()
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / 'fixture.part'
            client.download_responses = [response(fail=RuntimeError('URL-secret must not be exposed'))]
            with self.assertRaises(RemoteError) as caught:
                self.remote.download_to_file(FILE_TOKEN, target)
            self.assertEqual(caught.exception.category, 'transport')
            self.assertNotIn('URL-secret', str(caught.exception))
            self.assertFalse(target.exists())
            def changed(size):
                yield b'bytes'
                self.manager.credentials = ('cli_rotated', 'synthetic-rotated-secret')
            client.download_responses = [SimpleNamespace(status_code=200, headers={}, iter_bytes=changed)]
            with self.assertRaises(RemoteError) as caught:
                self.remote.download_to_file(FILE_TOKEN, target)
            self.assertEqual(caught.exception.category, 'credential_changed')
            self.assertFalse(target.exists())


class LocalPreparationTests(SyncFixture):
    def setUp(self):
        super().setUp()
        self.kb = KnowledgeBase(Path(self.temp) / 'lighthouse_openclaw/knowledge_base',
            embedder=FakeEmbedder(), memory_index_factory=FakeMemoryIndex, start_worker=False)
        self.addCleanup(self.kb.close)
        self.worker = DocumentLibraryService(self.temp, remote_factory=lambda _: self.remote, knowledge=self.kb, clock=lambda: self.now)

    def tearDown(self):
        self.assertEqual((self.remote.creates, self.remote.updates, self.remote.uploads), (0, 0, 0))

    def drain(self):
        for _ in range(100):
            prepared = self.worker.prepare_pending()
            worked = self.worker.process_one()
            indexed = False if prepared else self.kb.process_one()
            if not prepared and not worked and not indexed:
                return
        self.fail('fixture queue did not settle')

    def test_all_fields_and_unsupported_originals_cache_before_index_without_ai_worker(self):
        self.remote.files['tokenZipFixture'] = b'archive bytes'
        self.remote.rows['recOne']['fields']['Files'].append({'file_token': 'tokenZipFixture', 'name': 'archive.zip'})
        self.save()
        self.assertTrue(self.service.prepare_pending())
        self.assertEqual(self.remote.downloads, 3)
        with self.kb.connect(readonly=True) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM documents').fetchone()[0], 0)
        first = self.service.list_records(actor(), 'manuals', {})['items'][0]
        self.assertTrue(all(item['cached'] for item in first['knowledge_items']))
        archive = next(item for item in first['knowledge_items'] if item['name'] == 'archive.zip')
        self.assertEqual(archive['status'], 'unsupported')
        self.assertIn('原件已缓存', archive['error'])
        self.assertIn('暂不支持', archive['error'])
        self.drain()
        with patch('openclaw_service.assistant.lighthouse_knowledge.KnowledgeBase', side_effect=AssertionError('No extra worker')):
            first = self.service.list_records(actor(), 'manuals', {})['items'][0]
        self.assertEqual(next(item for item in first['knowledge_items'] if item['kind'] == 'record')['status'], 'ready')
        self.assertEqual(self.remote.downloads, 3)
        self.assertFalse(self.kb.list(self.admin, {})['items'])

    def test_cached_text_indexes_before_other_attachments_finish_downloading(self):
        self.save()
        # A busy download queue must not keep already cached record text queued forever.
        original = self.remote.download_to_file
        def unavailable(token, target, **context):
            if token == 'tokenFixtureTwo':
                raise AssistantError('fixture offline', 503, category='transport')
            return original(token, target, **context)
        with patch.object(self.remote, 'download_to_file', side_effect=unavailable):
            for _ in range(12):
                self.worker.prepare_pending(limit=1)
                self.kb.process_one()
        first = self.service.list_records(actor(), 'manuals', {})['items'][0]
        self.assertTrue(first['knowledge_summary']['vectorized'])
        self.assertEqual(first['knowledge_status'], 'ready')
        second = self.service.list_records(actor(('B',)), 'manuals', {})['items'][0]
        self.assertFalse(second['knowledge_summary']['vectorized'])
        self.assertEqual(next(item for item in second['knowledge_items'] if item['kind'] == 'attachment')['status'], 'retrying')

    def test_attachment_permission_failure_isolated_then_retry_preserves_cached_files(self):
        self.save()
        original = self.remote.download_to_file
        contexts = []
        def fail_one(token, target, **context):
            contexts.append((token, context))
            if token == 'tokenFixtureOne':
                Path(target).write_bytes(b'partial')
                raise AssistantError('secret provider data', 403, category='unauthorized')
            return original(token, target, **context)
        with patch.object(self.remote, 'download_to_file', side_effect=fail_one):
            self.drain()
        row = self.service.list_records(actor(), 'manuals', {})['items'][0]
        item = next(item for item in row['knowledge_items'] if item['kind'] != 'record')
        self.assertEqual((item['status'], item['stage'], item['cached'], item['error_code']), ('retrying', 'prepare', False, 'unauthorized'))
        self.assertNotIn('secret', json.dumps(row))
        self.assertFalse(list((self.service.root / 'files').glob('*.part')))
        self.assertEqual(contexts[0][1], {'record_id': 'recOne', 'field_id': 'fldFiles'})
        other = self.service.list_records(actor(('B',)), 'manuals', {})['items'][0]
        self.assertEqual(other['knowledge_status'], 'ready')
        self.now += 10
        self.drain()
        self.assertEqual(self.remote.downloads, 2)
        self.assertEqual(self.service.list_records(actor(), 'manuals', {})['items'][0]['knowledge_status'], 'ready')

    def test_failed_index_submission_reuses_cache_after_restart(self):
        self.save()
        original = self.kb.upsert_source
        def fail_attachment(key, source, name, content):
            if name == 'alpha.txt':
                raise RuntimeError('secret path and credentials')
            return original(key, source, name, content)
        with patch.object(self.kb, 'upsert_source', side_effect=fail_attachment):
            self.drain()
        first = self.service.list_records(actor(), 'manuals', {})['items'][0]
        item = next(item for item in first['knowledge_items'] if item['name'] == 'alpha.txt')
        self.assertEqual((item['status'], item['stage'], item['cached']), ('retrying', 'index', True))
        self.assertTrue(item['error'])
        self.assertNotIn('secret', json.dumps(first))
        before = self.remote.downloads
        self.now += 10
        self.worker = DocumentLibraryService(self.temp, remote_factory=lambda _: self.remote, knowledge=self.kb, clock=lambda: self.now)
        self.drain()
        self.assertEqual(self.remote.downloads, before)
        self.assertEqual(self.service.list_records(actor(), 'manuals', {})['items'][0]['knowledge_status'], 'ready')

    def test_unreadable_index_configuration_does_not_hide_cached_records(self):
        self.save()
        self.drain()
        for payload in ('null', '[]', '1', 'not-json'):
            with self.subTest(payload=payload):
                with self.kb.connect() as db:
                    db.execute("UPDATE config SET payload=? WHERE key='embedding'", (payload,))
                row = self.service.list_records(actor(), 'manuals', {})['items'][0]
                self.assertEqual(row['knowledge_status'], 'unavailable')
                self.assertTrue(row['fields'])
                self.assertTrue(all(item['cached'] for item in row['knowledge_items']))
                self.assertTrue(all(item['status'] == 'unavailable' for item in row['knowledge_items']))

    def test_existing_worker_prepares_each_original_before_index_and_stops_between_items(self):
        self.save()
        def index():
            with self.kb.connect(readonly=True) as db:
                if not db.execute('SELECT 1 FROM documents WHERE pending_version IS NOT NULL').fetchone():
                    return False
            with self.worker.connect(readonly=True) as db:
                tasks = db.execute("SELECT payload FROM ingestion WHERE status='synced'").fetchall()
            self.assertTrue(tasks)
            for task in tasks:
                payload = json.loads(task[0])
                if payload.get('kind') != 'record':
                    self.assertTrue(self.worker._cache_size(payload['file_token']))
            self.kb._stop.set()
            return True
        with patch.object(self.kb, 'process_one', side_effect=index) as run, patch(
                'upload_event_module.services.process_lifetime.lower_current_thread_priority'):
            self.kb._run()
        self.assertTrue(run.called)

    def test_continuous_preparation_does_not_starve_queued_manual_document(self):
        manual = self.kb.upload(self.admin, 'manual-fixture.txt', b'Manual document remains available')
        prepared = 0
        def prepare(*, limit):
            nonlocal prepared
            self.assertEqual(limit, 1)
            prepared += 1
            self.assertLessEqual(prepared, 4, 'continuous source preparation starved manual indexing')
            return True
        original = self.kb.process_one
        def index():
            worked = original()
            with self.kb.connect(readonly=True) as db:
                state = db.execute('SELECT status FROM documents WHERE id=?', (manual['id'],)).fetchone()[0]
            self.assertEqual(state, 'ready')
            self.kb._stop.set()
            return worked
        with patch.object(self.worker, 'prepare_pending', side_effect=prepare), patch.object(self.kb, 'process_one', side_effect=index), patch(
                'upload_event_module.services.process_lifetime.lower_current_thread_priority'):
            self.kb._run()
        self.assertEqual(prepared, 4)

    def test_shared_download_clock_limits_both_processes_and_cache_hits_skip_slots(self):
        self.save()
        starts = []
        original = self.remote.download_to_file
        def observed(*args, **kwargs):
            starts.append(self.now)
            return original(*args, **kwargs)
        with patch.object(self.remote, 'download_to_file', side_effect=observed):
            source = self.service._table('manuals')
            self.service._content_path(source, 'tokenFixtureOne', record_id='recOne', field_id='fldFiles')
            self.worker._content_path(source, 'tokenFixtureTwo', record_id='recTwo', field_id='fldFiles')
            self.worker._content_path(source, 'tokenFixtureOne', record_id='recOne', field_id='fldFiles')
        self.assertEqual(len(starts), 2)
        self.assertGreaterEqual(starts[1] - starts[0], .25)


class CacheRecoveryRoutesTests(SyncFixture):
    def setUp(self):
        super().setUp()
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from lan_bitable_template_portal.document_library_routes import install_document_library_routes
        self.current_actor = self.admin
        self.auth_calls = 0
        self.revoke_response = False
        async def authorize(request):
            self.auth_calls += 1
            if self.revoke_response and self.auth_calls == 2:
                return actor(('H',))
            return copy.deepcopy(self.current_actor)
        app = FastAPI()
        install_document_library_routes(app, authorize, lambda: self.service)
        self.client = self.enterContext(TestClient(app))
        self.url = '/api/document-library/tables/manuals/records'
        for target in ('socket.socket.connect', 'socket.socket.connect_ex', 'socket.getaddrinfo'):
            self.enterContext(patch(target, side_effect=AssertionError('No network')))

    def test_completed_scan_migrates_old_retry_and_read_wakes_preparation_without_ai(self):
        self.save()
        self.service.process_sync('manuals')
        with self.service.connect() as db:
            db.execute("UPDATE ingestion SET status='retrying',next_attempt=999999")
            for column in ('stage', 'cached', 'cache_size', 'error', 'error_code'):
                db.execute('ALTER TABLE ingestion DROP COLUMN ' + column)
        self.service = self.reopen()
        ready = threading.Event()
        original = self.service._content_path
        def cached(*args, **kwargs):
            result = original(*args, **kwargs)
            if self.remote.downloads == 2:
                ready.set()
            return result
        with patch.object(self.service, '_content_path', side_effect=cached), patch.object(self.remote, 'records', side_effect=AssertionError('No rescan')):
            result = self.client.get(self.url)
            self.assertEqual(result.status_code, 200, result.text)
            self.assertTrue(ready.wait(3))
        self.assertIsNone(self.service.knowledge)
        self.assertTrue(self.service._cache_size('tokenFixtureOne'))
        self.assertTrue(self.service._cache_size('tokenFixtureTwo'))

    def test_denied_and_response_revocation_do_not_start_original_downloads(self):
        self.save()
        self.service.process_sync('manuals')
        self.current_actor = actor(('H',))
        with patch.object(self.remote, 'download_to_file') as download:
            self.assertEqual(self.client.get(self.url).status_code, 404)
            self.current_actor = self.admin
            self.auth_calls, self.revoke_response = 0, True
            self.assertEqual(self.client.get(self.url).status_code, 403)
            download.assert_not_called()

    def test_slow_original_does_not_hold_first_records_response(self):
        self.save()
        entered, release = threading.Event(), threading.Event()
        original = self.remote.download_to_file
        def slow(*args, **kwargs):
            entered.set()
            self.assertTrue(release.wait(5))
            return original(*args, **kwargs)
        with patch.object(self.remote, 'download_to_file', side_effect=slow):
            with ThreadPoolExecutor(max_workers=1) as pool:
                result = pool.submit(self.client.get, self.url)
                try:
                    self.assertTrue(entered.wait(2))
                    page = result.result(3)
                    self.assertEqual(page.status_code, 200, page.text)
                    self.assertEqual(len(page.json()['data']['items']), 2)
                finally:
                    release.set()


if __name__ == '__main__':
    unittest.main()

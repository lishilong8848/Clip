"""First-sync recovery using temporary SQLite, fake providers and no AI worker."""
import copy
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from bin.test_document_library import Remote, actor, configuration
from lan_bitable_template_portal.document_library import DocumentLibraryService
from openclaw_service.assistant.lighthouse_ai import AssistantError


class SyncFixture(unittest.TestCase):
    def setUp(self):
        self.temp = self.enterContext(tempfile.TemporaryDirectory())
        self.enterContext(patch.dict(os.environ, {'CLIPFLOW_DATA_DIR': self.temp}))
        from upload_event_module import logger
        trace = getattr(logger, '_crash_trace_file', None)
        if trace and Path(trace.name).resolve().is_relative_to(Path(self.temp).resolve()):
            def close_logging():
                import faulthandler
                import logging
                logger._stop_logging_listener()
                logging.shutdown()
                faulthandler.disable()
                trace.close()
                logger._crash_trace_file = None
            self.addCleanup(close_logging)
        self.enterContext(patch('lan_bitable_template_portal.document_library_remote.DocumentLibraryRemote',
            side_effect=AssertionError('No real provider')))
        self.now = 1000.0
        self.enterContext(patch.object(DocumentLibraryService, '_sleep_download', side_effect=lambda delay: setattr(self, 'now', self.now + delay)))
        self.remote = Remote()
        self.service = self.reopen()
        self.admin = actor(('A', 'B'), admin=True)

    def tearDown(self):
        self.assertEqual((self.remote.creates, self.remote.updates, self.remote.uploads), (0, 0, 0))
        self.assertLessEqual(self.remote.downloads, len(self.remote.files))

    def reopen(self):
        return DocumentLibraryService(self.temp, remote_factory=lambda _: self.remote, clock=lambda: self.now)

    def save(self):
        self.service.save_table(self.admin, configuration())

    def job(self):
        with self.service.connect(readonly=True) as db:
            rows = db.execute('SELECT * FROM sync_jobs').fetchall()
        self.assertEqual(len(rows), 1)
        return dict(rows[0])

    def assert_pending(self, code='document_library_sync_pending'):
        with self.assertRaises(AssistantError) as caught:
            self.service.list_records(actor(), 'manuals', {})
        self.assertEqual(caught.exception.category, code)
        return str(caught.exception)


class FirstSyncTests(SyncFixture):
    def test_snapshot_and_completion_are_read_from_the_same_transaction(self):
        self.save()
        with self.service.connect() as db:
            db.execute('PRAGMA journal_mode=WAL')
        original = self.service.connect
        published = False
        def publish_between_reads(statement):
            nonlocal published
            if not published and statement.startswith('SELECT 1 FROM sync_jobs'):
                published = True
                self.service.process_sync('manuals')
        @contextmanager
        def observe(*, readonly=False):
            with original(readonly=readonly) as db:
                if readonly:
                    db.set_trace_callback(publish_between_reads)
                yield db
        with patch.object(self.service, 'connect', side_effect=observe):
            self.assert_pending()
        self.assertTrue(published)
        self.assertEqual(len(self.service.list_records(self.admin, 'manuals', {})['items']), 2)

    def test_concurrent_upgrade_adds_error_columns_without_losing_pending_job(self):
        self.save()
        identity = self.job()['id']
        with self.service.connect() as db:
            db.execute('ALTER TABLE sync_jobs DROP COLUMN error')
            db.execute('ALTER TABLE sync_jobs DROP COLUMN error_code')
        with ThreadPoolExecutor(max_workers=4) as pool:
            services = list(pool.map(lambda _: self.reopen(), range(4)))
        self.assertTrue(all(service.db_path == self.service.db_path for service in services))
        job = self.job()
        self.assertEqual(job['id'], identity)
        self.assertEqual(job['status'], 'queued')
        self.assertEqual(job['error'], '')
        self.assertEqual(job['error_code'], '')

    def test_existing_job_wakes_worker_and_completed_initial_is_not_enqueued_again(self):
        self.save()
        identity = self.job()['id']
        wake = Mock()
        self.service.knowledge = SimpleNamespace(_wake=wake)
        self.assertEqual(self.service.ensure_initial_sync(actor(), 'manuals')['job_id'], identity)
        wake.set.assert_called_once_with()
        self.service.knowledge = None
        self.service.process_sync('manuals')
        self.assertIsNone(self.service.ensure_initial_sync(actor(), 'manuals'))
        self.assertEqual(self.job()['id'], identity)

    def test_initial_scan_uses_existing_queue_without_knowledge_worker(self):
        self.save()
        self.assert_pending()
        self.assertTrue(self.service.process_sync('manuals'))
        result = self.service.list_records(actor(), 'manuals', {})
        self.assertEqual([row['record_id'] for row in result['items']], ['recOne'])
        self.assertEqual(result['sync_status'], 'completed')
        self.assertFalse(self.service.process_sync('manuals'))
        self.assertIsNone(self.service.knowledge)

    def test_confirmed_empty_is_distinct_from_pending_or_malformed(self):
        self.save()
        self.assert_pending()
        for page in ({}, {'items': [], 'has_more': None}, {'items': [{}], 'has_more': False}):
            with patch.object(self.remote, 'records', return_value=page):
                self.service.request_sync(actor(), 'manuals')
                self.service.process_sync('manuals')
            self.assertIn('返回不完整', self.assert_pending('document_library_sync_failed'))
        self.remote.rows.clear()
        self.service.request_sync(actor(), 'manuals')
        self.service.process_sync('manuals')
        self.assertEqual(self.service.list_records(actor(), 'manuals', {})['items'], [])
        self.assertEqual(self.job()['error'], '')

    def test_error_survives_restart_retry_reuses_id_and_does_not_echo_private_details(self):
        self.save()
        with patch.object(self.remote, 'records', side_effect=OSError('https://secret.example?token=do-not-expose')):
            self.service.process_sync('manuals')
        failed = self.job()
        self.assertEqual(failed['status'], 'retrying')
        self.service = self.reopen()
        self.assertIn('网络', self.assert_pending('document_library_sync_failed'))
        self.assertNotIn('secret', json.dumps(self.job()))
        self.service.ensure_initial_sync(actor(), 'manuals')
        self.assertFalse(self.service.process_sync('manuals'))
        retry = self.service.request_sync(actor(), 'manuals')
        self.assertEqual(retry, {'job_id': failed['id'], 'status': 'queued'})
        self.assertTrue(self.service.process_sync('manuals'))
        self.assertEqual(self.job()['status'], 'completed')
        self.assertEqual(self.service.sync_status(actor(), 'manuals', failed['id'])['error'], '')

    def test_first_partial_page_is_not_a_complete_list_and_retry_resumes_cursor(self):
        self.save()
        rows = list(self.remote.rows.values())
        with patch.object(self.remote, 'records', return_value={'items': [rows[0]], 'has_more': True, 'page_token': 'next'}):
            self.service.process_sync('manuals')
        partial = self.service.list_records(actor(), 'manuals', {})
        self.assertFalse(partial['cache']['complete'])
        self.assertEqual([r['record_id'] for r in partial['items']], ['recOne'])
        with patch.object(self.remote, 'records', side_effect=AssistantError('denied code=1254302', 403, category='unauthorized')):
            self.service.process_sync('manuals')
        self.assertIn('1254302', self.service.list_records(actor(), 'manuals', {})['cache']['error'])
        identity = self.job()['id']
        self.service = self.reopen()
        self.service.request_sync(actor(), 'manuals')
        with patch.object(self.remote, 'records', return_value={'items': [rows[1]], 'has_more': False, 'page_token': ''}) as records:
            self.service.process_sync('manuals')
        records.assert_called_once_with(cursor='next', page_size=100)
        self.assertEqual(self.job()['id'], identity)
        self.assertEqual(len(self.service.list_records(self.admin, 'manuals', {})['items']), 2)
        with self.service.connect(readonly=True) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM versions').fetchone()[0], 2)

    def test_restart_keeps_active_lease_then_recovers_expired_lease(self):
        self.save()
        claimed = self.service._claim('sync_jobs')
        self.service = self.reopen()
        self.assertEqual(self.service.request_sync(actor(), 'manuals')['status'], 'running')
        self.assertFalse(self.service.process_sync('manuals'))
        self.now += 181
        self.assertTrue(self.service.process_sync('manuals'))
        self.service._retry('sync_jobs', claimed, OSError('late failure'))
        self.assertEqual(self.job()['status'], 'completed')

    def test_missing_enqueue_recovered_and_unauthorized_read_cannot_start_or_inspect(self):
        self.save()
        with self.service.connect() as db:
            db.execute('DELETE FROM sync_jobs')
        for call in (self.service.ensure_initial_sync, self.service.request_sync):
            with self.assertRaises(AssistantError) as caught:
                call(actor(('H',)), 'manuals')
            self.assertEqual(caught.exception.status, 404)
        with self.assertRaises(AssistantError) as caught:
            self.service.list_records(actor(('H',)), 'manuals', {})
        self.assertEqual(caught.exception.status, 404)
        self.service.ensure_initial_sync(actor(), 'manuals')
        self.assertTrue(self.service.process_sync('manuals'))
        self.assertEqual(self.job()['status'], 'completed')


class FirstSyncRoutesTests(SyncFixture):
    def setUp(self):
        super().setUp()
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from lan_bitable_template_portal.document_library_routes import install_document_library_routes
        self.current_actor = self.admin
        async def authorize(request):
            return copy.deepcopy(self.current_actor)
        app = FastAPI()
        install_document_library_routes(app, authorize, lambda: self.service)
        self.client = self.enterContext(TestClient(app))
        self.prefix = '/api/document-library/tables/manuals'
        for name in ('socket.socket.connect', 'socket.socket.connect_ex', 'socket.getaddrinfo'):
            self.enterContext(patch(name, side_effect=AssertionError('No live network')))

    def test_save_starts_background_scan_and_concurrent_reads_share_it(self):
        entered, release = threading.Event(), threading.Event()
        original = self.remote.records
        def blocked(**kwargs):
            entered.set()
            self.assertTrue(release.wait(5), 'test did not release fake remote')
            return original(**kwargs)
        with patch.object(self.remote, 'records', side_effect=blocked) as scan:
            try:
                saved = self.client.post('/api/document-library/tables', json=configuration())
                self.assertEqual(saved.status_code, 200, saved.text)
                self.assertTrue(entered.wait(3), 'save did not trigger first sync')
                with ThreadPoolExecutor(max_workers=2) as pool:
                    first = pool.submit(self.client.get, self.prefix + '/records')
                    second = pool.submit(self.client.get, self.prefix + '/records')
                    release.set()
                    for future in (first, second):
                        response = future.result(5)
                        self.assertEqual(response.status_code, 200, response.text)
                        data = response.json()['data']
                        self.assertIn(len(data['items']), (1, 2))
                        if data['cache']['complete']:
                            self.assertEqual(len(data['items']), 2)
            finally:
                release.set()
        self.assertEqual(scan.call_count, 1)
        self.assertEqual(self.job()['status'], 'completed')
        self.assertEqual(len(self.client.get(self.prefix + '/records').json()['data']['items']), 2)

    def test_failure_retry_response_and_no_duplicate_jobs(self):
        self.save()
        self.remote.fail_scan = True
        response = self.client.get(self.prefix + '/records')
        self.assertEqual(response.status_code, 503, response.text)
        self.assertEqual(response.json()['code'], 'document_library_sync_failed')
        self.assertIn('网络', response.json()['error'])
        identity = self.job()['id']
        self.remote.fail_scan = False
        retry = self.client.post(self.prefix + '/sync', json={})
        self.assertEqual(retry.json()['data']['job_id'], identity)
        response = self.client.get(self.prefix + '/records')
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.job()['id'], identity)

    def test_bounded_pending_response_continues_same_background_work(self):
        entered, release = threading.Event(), threading.Event()
        self.save()
        original = self.remote.records
        def slow(**kwargs):
            entered.set()
            self.assertTrue(release.wait(5))
            return original(**kwargs)
        with patch.object(self.remote, 'records', side_effect=slow) as scan, patch(
                'lan_bitable_template_portal.document_library_routes._INITIAL_WAIT_SECONDS', 0):
            try:
                response = self.client.get(self.prefix + '/records')
                self.assertEqual(response.status_code, 503)
                self.assertEqual(response.json()['code'], 'document_library_sync_pending')
                identity = self.job()['id']
                # A pending response may precede worker dispatch under a busy executor.
                self.assertTrue(entered.wait(3), 'queued first sync did not start')
            finally:
                release.set()
        response = self.client.get(self.prefix + '/records')
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.job()['id'], identity)
        self.assertEqual(scan.call_count, 1)

    def test_revocation_during_background_read_is_not_exposed(self):
        self.save()
        original = self.remote.records
        def revoke(**kwargs):
            self.current_actor = actor(('H',))
            return original(**kwargs)
        with patch.object(self.remote, 'records', side_effect=revoke):
            response = self.client.get(self.prefix + '/records')
        self.assertEqual(response.status_code, 403)
        self.assertNotIn('Alpha policy', response.text)


class FirstSyncFrontendTests(unittest.TestCase):
    def test_component_pending_retry_cancel_and_stale_table_guards(self):
        frontend = Path(__file__).parent / 'lan_bitable_template_portal/frontend'
        node = shutil.which('node')
        if not node:
            self.skipTest('Node is not installed')
        # Compile the complete real SFC; its imports are isolated by the source fixture.
        result = subprocess.run(
            [node, 'scripts/check-document-library-maintenance.mjs', '--source-check'],
            cwd=frontend, text=True, encoding='utf-8', capture_output=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('first-sync partial/pending keeps rows', result.stdout)
        self.assertIn('cancelled reads, stale table responses', result.stdout)


if __name__ == '__main__':
    unittest.main()

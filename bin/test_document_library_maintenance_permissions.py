"""Maintenance permission races through the production bridge, entirely offline."""
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from contextlib import contextmanager
import copy
import json
from pathlib import Path
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

from fastapi import FastAPI
from fastapi.testclient import TestClient

from bin import test_document_library as fixtures
from lan_bitable_template_portal.document_library import DocumentLibraryService
from lan_bitable_template_portal.document_library_portal import install_document_library_portal
from lan_bitable_template_portal.portal_auth import PortalAuthManager


class MaintenancePermissionTests(fixtures.LibraryFixture):
    def setUp(self):
        self.enterContext(patch('openclaw_service.assistant.lighthouse_knowledge._load_local', return_value=None))
        super().setUp()
        self.profile = {'role': 'building', 'modules': ['knowledge'], 'scopes': ['A']}
        self.session = {'user': {'open_id': fixtures.actor()['id'], 'name': 'Fixture reader'}}
        auth = SimpleNamespace(
            account_profile=lambda identity: copy.deepcopy(self.profile),
            scopes_for_open_id=lambda identity: list(self.profile['scopes']),
            _scopes_from_raw=lambda values: values or [])
        auth.is_admin = lambda session: PortalAuthManager.is_admin(auth, session)
        auth.session_scopes = lambda session: PortalAuthManager.session_scopes(auth, session)
        controller = SimpleNamespace(
            _current_session=lambda request: copy.deepcopy(self.session),
            _request_base_url=lambda request: str(request.base_url).rstrip('/'))
        runtime = SimpleNamespace(auth_manager=auth,
            state_store=SimpleNamespace(db_path=Path(self.temp) / 'portal.sqlite3'))

        def build(root, **kwargs):
            self.bridge = DocumentLibraryService(root, remote_factory=lambda _: self.remote,
                knowledge=self.kb, clock=lambda: self.now, **kwargs)
            return self.bridge

        self.enterContext(patch('lan_bitable_template_portal.document_library_portal.DocumentLibraryService', side_effect=build))
        self.enterContext(patch('lan_bitable_template_portal.document_library_remote.DocumentLibraryRemote',
            side_effect=AssertionError('No real provider')))
        app = FastAPI()
        install_document_library_portal(app, controller, runtime)
        self.client = self.enterContext(TestClient(app, base_url='https://portal.example'))
        for target in ('socket.socket.connect', 'socket.socket.connect_ex', 'socket.getaddrinfo'):
            self.enterContext(patch(target, side_effect=AssertionError('No network')))
        self.url = '/api/document-library/tables/manuals'
        self.origin = {'origin': 'https://portal.example'}
        self.assertEqual(self.client.get('/api/document-library/tables').status_code, 200)

    def tearDown(self):
        self.assertEqual((self.remote.creates, self.remote.updates, self.remote.uploads), (0, 0, 0))

    def store_snapshot(self):
        with self.service.connect(readonly=True) as db:
            return {table: [tuple(row) for row in db.execute('SELECT * FROM ' + table + ' ORDER BY rowid')]
                for table in ('tables', 'records', 'versions', 'ingestion', 'maintenance', 'sync_jobs', 'schema_cache')}

    def assert_revocation_while_locked(self, method, suffix, revoke):
        request = {'operation_id': str(uuid.uuid4()), 'expected_revision': self.service._table('manuals')['revision']}
        if suffix == '/reindex':
            request['record_ids'] = ['recOne']
        before = self.store_snapshot()
        attempting = threading.Event()
        connect = self.bridge.connect

        @contextmanager
        def observe(*, readonly=False):
            with connect(readonly=readonly) as db:
                if not readonly:
                    db.set_trace_callback(lambda sql: attempting.set() if sql.strip().upper() == 'BEGIN IMMEDIATE' else None)
                yield db

        with patch.object(self.bridge, 'connect', side_effect=observe), ThreadPoolExecutor(1) as pool:
            with self.service.connect() as held:
                held.execute('BEGIN IMMEDIATE')
                future = pool.submit(self.client.request, method, self.url + suffix, headers=self.origin, json=request)
                self.assertTrue(attempting.wait(5), 'Request never attempted the write transaction')
                # The actual SQLite lock, not a mocked wait, must keep the request unfinished.
                with self.assertRaises(TimeoutError):
                    future.result(timeout=0.05)
                revoke()
            response = future.result(timeout=10)
        self.assertEqual(response.status_code, 403, response.text)
        self.assertEqual(response.headers['cache-control'], 'no-store')
        self.assertEqual(self.store_snapshot(), before, 'Denied request changed the store or queued work')

    def assert_response_rejected(self, suffix, change, expected):
        check = self.bridge.check_response
        captured = []

        def changed(actor, key, result, **kwargs):
            captured.append(copy.deepcopy(result))
            change()
            return check(actor, key, result, **kwargs)

        with patch.object(self.bridge, 'check_response', side_effect=changed):
            response = self.client.get(self.url + suffix)
        self.assertEqual(len(captured), 1, 'Request failed before the response boundary')
        self.assertIn('Alpha policy', json.dumps(captured[0]))
        self.assertEqual(response.status_code, expected, response.text)
        self.assertEqual(response.headers['cache-control'], 'no-store')
        self.assertFalse(response.json()['ok'])
        self.assertNotIn('Alpha policy', response.text)
        self.assertNotIn('data', response.json())

    def refresh_record(self, **fields):
        self.remote.rows['recOne']['fields'].update(fields)
        return self.service.record(self.admin, 'manuals', 'recOne')

    def mark_pending(self):
        with self.service.connect() as db:
            db.execute("UPDATE records SET pending_operation='fixture-awaiting-confirmation' WHERE table_key='manuals' AND id='recOne'")

    def retire_record(self):
        self.service._retire('manuals', 'recOne', config_revision=self.service._table('manuals')['revision'])

    def test_remove_rechecks_admin_after_real_write_lock_wait(self):
        self.profile['role'] = 'admin'
        self.assert_revocation_while_locked('DELETE', '', lambda: self.profile.update(role='building'))

    def test_remove_rechecks_module_grant_after_real_write_lock_wait(self):
        self.profile['role'] = 'admin'
        self.assert_revocation_while_locked('DELETE', '', lambda: self.profile.update(modules=[]))

    def test_reindex_rechecks_scope_grant_after_real_write_lock_wait(self):
        self.assert_revocation_while_locked('POST', '/reindex', lambda: self.profile.update(scopes=[]))

    def test_reindex_rechecks_module_grant_after_real_write_lock_wait(self):
        self.assert_revocation_while_locked('POST', '/reindex', lambda: self.profile.update(modules=[]))

    def test_cached_detail_rejects_background_cross_scope_move(self):
        self.assert_response_rejected('/records/recOne', lambda: self.refresh_record(Building='B'), 404)
        self.assertEqual(self.client.get(self.url + '/records/recOne').status_code, 404)

    def test_cached_list_rejects_background_cross_scope_move(self):
        self.assert_response_rejected('/records', lambda: self.refresh_record(Building='B'), 404)

    def test_cached_detail_rejects_deleted_record(self):
        self.assert_response_rejected('/records/recOne', self.retire_record, 404)

    def test_cached_detail_rejects_pending_confirmation(self):
        self.assert_response_rejected('/records/recOne', self.mark_pending, 409)

    def test_cached_detail_rejects_changed_record_version(self):
        self.assert_response_rejected('/records/recOne', lambda: self.refresh_record(Number=5), 409)

    def test_cached_detail_rejects_stale_cached_config_revision(self):
        def stale_config():
            with self.service.connect() as db:
                row = self.service._cached('manuals', 'recOne', db)
                payload = json.loads(row['payload'])
                payload['config_revision'] = 'fixture-stale-config'
                db.execute("UPDATE records SET payload=? WHERE table_key='manuals' AND id='recOne'", (json.dumps(payload),))
        self.assert_response_rejected('/records/recOne', stale_config, 409)

    def test_cached_detail_rejects_changed_table_configuration(self):
        def reconfigure():
            revision = self.service._table('manuals')['revision']
            self.service.save_table(self.admin, fixtures.configuration(name='Renamed fixture', expected_revision=revision), key='manuals')
        self.assert_response_rejected('/records/recOne', reconfigure, 409)

    def test_historical_versions_remain_readable_when_record_revision_differs(self):
        original = self.service._cached('manuals', 'recOne')['revision']
        current = self.refresh_record(Number=5)
        self.assertNotEqual(current['revision'], original)
        response = self.client.get(self.url + '/records/recOne/versions')
        self.assertEqual(response.status_code, 200, response.text)
        versions = response.json()['data']['items']
        self.assertEqual({item['number'] for item in versions}, {1, 2})
        self.assertEqual({item['revision'] for item in versions}, {original, current['revision']})
        self.assertEqual({item['fields']['fldNumber'] for item in versions}, {4, 5})

    def test_historical_versions_still_filter_historical_scope(self):
        self.refresh_record(Building='B', Title='Restricted historical revision')
        self.refresh_record(Building='A', Title='Alpha policy updated')
        response = self.client.get(self.url + '/records/recOne/versions')
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual({item['number'] for item in response.json()['data']['items']}, {1, 3})
        self.assertNotIn('Restricted historical revision', response.text)
        self.profile['scopes'] = ['A', 'B']
        allowed = self.client.get(self.url + '/records/recOne/versions')
        self.assertEqual(allowed.status_code, 200, allowed.text)
        self.assertEqual({item['number'] for item in allowed.json()['data']['items']}, {1, 2, 3})

    def test_historical_response_rechecks_current_scope(self):
        self.assert_response_rejected('/records/recOne/versions', lambda: self.refresh_record(Building='B'), 404)

    def test_historical_response_rejects_deleted_record(self):
        self.assert_response_rejected('/records/recOne/versions', self.retire_record, 404)

    def test_historical_response_rejects_pending_confirmation(self):
        self.assert_response_rejected('/records/recOne/versions', self.mark_pending, 409)


if __name__ == '__main__':
    unittest.main()

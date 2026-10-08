"""Visitor auth and cabinet API boundaries, with no real cloud access."""
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.cabinet_guest import install_cabinet_guest_access
from lan_bitable_template_portal.cabinet_power_routes import install_cabinet_power_routes
from lan_bitable_template_portal.portal_auth import AUTH_COOKIE_NAME, PortalAuthManager


class CabinetGuestTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        with patch('lan_bitable_template_portal.portal_auth.get_data_file_path', side_effect=lambda n: str(self.root / n)):
            self.auth = PortalAuthManager()
        self.addCleanup(self.auth._cleanup_executor.shutdown, wait=True)
        self.runtime = SimpleNamespace(auth_manager=self.auth, state_store=Mock())
        self.service = Mock()
        self.service.local.version.return_value = 1
        self.service.bootstrap.return_value = {'buildings': [{'scope': s, 'status': 'succeeded'} for s in 'ABCDE']}
        self.service.overview.side_effect = lambda scope, *_: {'scope': scope, 'counts': {'total': 1}}
        self.service.racks.return_value = {'items': [{'room': '201', 'rack': 'A01'}]}
        self.service.layout.return_value = {'cells': []}
        self.service.operations.return_value = {'items': [], 'total': 0}
        self.service.export_history.return_value = {'items': [], 'total': 0}
        self.service.job.return_value = {'job_id': 'job1', 'kind': 'export', 'status': 'pending', 'scope': 'E'}
        self.service.job_status.return_value = {'job_id': 'job1', 'kind': 'export', 'status': 'succeeded', 'scope': 'E'}
        self.export = self.root / 'E.xlsm'
        self.export.write_bytes(b'export-fixture')
        self.service.read.return_value = {'scope': 'E', 'path': str(self.export), 'filename': self.export.name}

        async def read_json(request, **_kwargs):
            return await request.json()

        self.controller = SimpleNamespace(
            _current_session=lambda r: self.auth.get_session(r.cookies.get(AUTH_COOKIE_NAME, '')),
            _auth_required_response=lambda: JSONResponse({'ok': False}, status_code=401),
            _read_json_request=read_json,
            _json_ok=lambda r, s, d: JSONResponse({'ok': True, 'data': d}),
            _portal_error_response=lambda e, **kw: JSONResponse({'ok': False, 'error': str(e)}, status_code=500),
        )
        self.app = FastAPI()
        with patch('lan_bitable_template_portal.cabinet_power_routes.CabinetPowerService', return_value=self.service):
            install_cabinet_power_routes(self.app, self.controller, self.runtime)

        @self.app.get('/api/auth/status')
        async def status(request: Request):
            return self.auth.public_status(self.controller._current_session(request))

        @self.app.post('/api/auth/logout')
        async def logout(request: Request):
            self.auth.clear_session(request.cookies.get(AUTH_COOKIE_NAME, ''))
            return JSONResponse({'ok': True}, headers={'Set-Cookie': self.auth.clear_cookie_header()})

        @self.app.get('/api/repair-management')
        async def private_business():
            raise AssertionError('guest must not enter another business handler')

        install_cabinet_guest_access(self.app, self.controller, self.runtime)
        self.client = TestClient(self.app, follow_redirects=False)
        self.addCleanup(self.client.close)

    def login(self):
        response = self.client.post('/api/auth/guest')
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers['location'], '/cabinet-power')
        return self.client.cookies[AUTH_COOKIE_NAME]

    def test_guest_session_is_persistent_isolated_and_revocable(self):
        with patch.object(self.auth, '_exchange_login_code', side_effect=AssertionError('guest needs no Feishu')):
            sid = self.login()
        self.assertIn('HttpOnly', self.auth.cookie_header(sid))
        original = self.auth.get_session(sid)
        self.auth._sessions.clear()
        with patch.object(self.auth, 'scopes_for_open_id', side_effect=AssertionError('no employee permission lookup')):
            restored = self.auth.get_session(sid)
        self.assertEqual(restored['user'], original['user'])
        self.assertEqual(restored['allowed_scopes'], list('ABCDE'))
        self.assertFalse(self.auth.is_admin(restored))
        status = self.client.get('/api/auth/status').json()
        self.assertEqual(status['user']['role'], 'guest')
        self.assertEqual({s['value'] for s in status['scope_options']}, set('ABCDE'))
        self.assertEqual(self.login(), sid)
        another = self.auth.get_session(self.auth.create_guest_session())
        self.assertNotEqual(another['user']['open_id'], original['user']['open_id'])
        self.assertEqual(self.client.post('/api/auth/logout').status_code, 200)
        self.assertIsNone(self.auth.get_session(sid))
        self.assertEqual(self.client.get('/api/cabinet-power/overview?scope=E').status_code, 401)

    def test_reads_use_existing_data_without_starting_bootstrap_or_writes(self):
        self.login()
        for path in ('buildings', 'overview', 'rooms', 'racks', 'operations', 'rooms/201/layout', 'export-history'):
            with self.subTest(path=path):
                response = self.client.get('/api/cabinet-power/' + path, params={'scope': 'E'})
                self.assertEqual(response.status_code, 200, response.text)
        self.service.bootstrap.assert_called_once()
        self.assertEqual(len(self.service.bootstrap.call_args.args), 1)
        self.service.job.assert_not_called()
        self.service.save_operation.assert_not_called()
        self.service.start_export_batch.assert_not_called()
        self.service.local.version.return_value = 0
        self.service.overview.reset_mock()
        self.assertEqual(self.client.get('/api/cabinet-power/overview?scope=E').status_code, 409)
        self.service.overview.assert_not_called()

    def test_guest_can_export_one_building_download_and_not_inject_snapshot(self):
        self.login()
        response = self.client.post('/api/cabinet-power/exports', json={
            'scope': 'E', 'batch_id': 'single_fixture', 'snapshot': {'injected': True}, 'upload': True,
        })
        self.assertEqual(response.status_code, 200, response.text)
        scope, kind, owner, payload = self.service.job.call_args.args
        self.assertEqual((scope, kind), ('E', 'export'))
        self.assertTrue(owner.startswith('guest_'))
        self.assertEqual(payload, {'scope': 'E', 'batch_id': 'guest-current-export'})
        self.assertEqual(self.client.get('/api/cabinet-power/jobs/job1').status_code, 200)
        result = self.client.get('/api/cabinet-power/exports/export1/download')
        self.assertEqual(result.content, b'export-fixture')
        self.service.upload_export.assert_not_called()
        self.service.start_export_batch.assert_not_called()
        for scope in ('ALL', 'H', '', 'AE'):
            with self.subTest(scope=scope):
                self.assertEqual(self.client.post('/api/cabinet-power/exports', json={'scope': scope}).status_code, 403)

    def test_guest_denied_all_mutations_other_businesses_and_batch_workflows(self):
        self.login()
        cases = [
            ('POST', 'refresh'), ('POST', 'bootstrap'), ('GET', 'bootstrap'),
            ('POST', 'exports/export1/upload'), ('POST', 'exports/export1/cleanup'),
            ('POST', 'export-batches'), ('GET', 'export-batches/b1'), ('POST', 'export-batches/b1/resume'),
            ('GET', 'export-schedule'), ('PUT', 'export-schedule'), ('GET', 'storage'), ('POST', 'storage'),
            ('POST', 'operations'), ('PATCH', 'operations/rec1'), ('PATCH', 'rack-power'),
            ('GET', 'writes'), ('POST', 'writes/op1/resume'), ('POST', 'writes/op1/reconcile'),
            ('GET', 'batches'), ('POST', 'batches'), ('POST', 'batches/recognize'),
            ('DELETE', 'batches/b1'), ('POST', 'batches/b1/confirm'), ('POST', 'batches/b1/rollback'),
        ]
        self.service.reset_mock()
        for method, suffix in cases:
            with self.subTest(method=method, path=suffix):
                result = self.client.request(method, '/api/cabinet-power/' + suffix, json={'scope': 'E'})
                self.assertEqual(result.status_code, 403, result.text)
        for path in ('/api/repair-management', '/api/scope-overview', '/api/assistant/pending',
                     '/api/auth/permissions', '/api/learning/bootstrap', '/api/local/action', '/api/cabinet-powerX/exports'):
            self.assertEqual(self.client.get(path).status_code, 403, path)
        self.assertEqual(self.service.mock_calls, [])
        for path in ('/', '/repair-management', '/cabinet-power/batches', '/life-guide'):
            result = self.client.get(path)
            self.assertEqual(result.status_code, 303)
            self.assertEqual(result.headers['location'], '/cabinet-power')

    def test_guest_cannot_poll_refresh_jobs(self):
        self.login()
        self.service.job_status.return_value = {'kind': 'refresh', 'scope': 'E'}
        self.assertEqual(self.client.get('/api/cabinet-power/jobs/job1').status_code, 403)

    def test_guest_token_cannot_drop_prefix_to_bypass_permissions(self):
        sid = self.login()
        response = self.client.get('/api/cabinet-power/overview?scope=E',
                                   headers={'Cookie': AUTH_COOKIE_NAME + '=' + sid.removeprefix('guest_')})
        self.assertEqual(response.status_code, 401)

    def test_guest_entry_does_not_replace_employee_login(self):
        self.auth.upsert_permission_user(open_id='ou_fixture_admin', name='Fixture Admin', role='admin',
                                         scopes=['ALL'], enabled=True, updated_by='fixture')
        self.auth._sessions['employee-session'] = {
            'role': 'admin', 'allowed_scopes': ['ALL'], 'expires_at': 9999999999,
            'user': {'open_id': 'ou_fixture_admin', 'name': 'Fixture Admin'},
        }
        with patch.object(self.auth, 'create_guest_session') as create:
            response = self.client.post('/api/auth/guest', headers={'Cookie': AUTH_COOKIE_NAME + '=employee-session'})
        self.assertEqual(response.status_code, 303)
        self.assertNotIn('set-cookie', response.headers)
        create.assert_not_called()
        self.assertTrue(self.auth.is_admin(self.auth.get_session('employee-session')))


if __name__ == '__main__':
    unittest.main()

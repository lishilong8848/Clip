"""Exercise real routes without starting schedulers or accessing production stores."""
import sys
import re
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent))
from clipflow_backend.main import FastAPIPortalController, PortalRuntime
from lan_bitable_template_portal.portal_auth import PortalAuthManager, AUTH_COOKIE_NAME
from lan_bitable_template_portal.portal_service import MaintenancePortalService
from lan_bitable_template_portal.state_store import LanPortalStateStore


class PortalAccessBoundaries(unittest.TestCase):
    def setUp(self):
        self.session = None
        self.auth = PortalAuthManager.__new__(PortalAuthManager)
        self.auth.get_session = lambda _: self.session
        self.state = Mock()
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state.db_path = Path(self.temp.name) / 'fixture.sqlite3'
        self.state.get_backend_runtime.return_value = {'fixture_private_runtime': 'not public'}
        service = MaintenancePortalService.__new__(MaintenancePortalService)
        service._state_store = LanPortalStateStore(self.state.db_path)
        self.addCleanup(service._state_store.shutdown_write_worker)
        service._signature_management = Mock()
        self.runtime = type('FixtureRuntime', (PortalRuntime,), {
            'auth_manager': self.auth, 'state_store': self.state, 'service': service,
            '_polling_work_order_manager': None})
        self.runtime_patch = patch('clipflow_backend.main.PortalRuntime', self.runtime)
        self.runtime_patch.start()
        self.addCleanup(self.runtime_patch.stop)
        with patch('clipflow_backend.main.LanPortalStateStore', return_value=self.state), patch(
                'clipflow_backend.main.DrillManagementService', return_value=Mock()):
            self.controller = FastAPIPortalController()
        self.addCleanup(self.controller._background_executor.shutdown, wait=True, cancel_futures=True)
        self.addCleanup(self.controller._drill_executor.shutdown, wait=True, cancel_futures=True)
        self.controller._consume_rate_limit = Mock(return_value=(True, 0))
        self.app = self.controller._build_app()
        self.client = TestClient(self.app, client=('198.51.100.14', 4321), follow_redirects=False)
        self.addCleanup(self.client.close)

    def login(self, role='building', scopes=None):
        self.session = {'role': role, 'allowed_scopes': scopes or ['D'],
                        'user': {'open_id': 'ou_fixture', 'name': '测试人员'}}
        self.client.cookies.set(AUTH_COOKIE_NAME, 'fixture')

    def test_cached_auth_status_cannot_restore_revoked_session(self):
        self.login()
        first = self.client.get('/api/auth/status?next=/learning').json()['data']
        self.assertTrue(first['logged_in'])
        self.session = None
        response = self.client.get('/api/auth/status?next=/learning')
        current = response.json()['data']
        self.assertFalse(current['logged_in'])
        self.assertIsNone(current['user'])
        self.assertEqual(current['allowed_scopes'], [])
        self.assertIn('no-store', response.headers.get('cache-control', ''))

    def test_auth_status_immediately_reflects_permission_change(self):
        self.login('admin', ['ALL'])
        self.assertEqual(self.client.get('/api/auth/status?next=/').json()['data']['user']['role'], 'admin')
        self.login('building', ['D'])
        data = self.client.get('/api/auth/status?next=/').json()['data']
        self.assertEqual(data['user']['role'], 'building')
        self.assertEqual(data['allowed_scopes'], ['D'])

    def test_rate_limit_never_returns_old_authentication_payload(self):
        self.controller._read_cache_put(('auth_status', 'fixture', '/'),
            {'ok': True, 'data': {'logged_in': True, 'user': {'name': '旧身份'}}})
        self.client.cookies.set(AUTH_COOKIE_NAME, 'fixture')
        self.controller._consume_rate_limit.return_value = (False, 1)
        response = self.client.get('/api/auth/status?next=/')
        self.assertEqual(response.status_code, 429)
        self.assertNotIn('旧身份', response.text)

    def test_remote_health_requires_admin_even_when_cached(self):
        self.controller._read_cache_put(('health',), {'data': {'fixture_private_runtime': 'not public'}})
        self.assertEqual(self.client.get('/api/health').status_code, 401)
        self.login()
        self.assertEqual(self.client.get('/api/health').status_code, 403)
        self.login('admin')
        self.assertEqual(self.client.get('/api/health').status_code, 200)
        self.session = None
        self.controller._consume_rate_limit.return_value = (False, 1)
        response = self.client.get('/api/health')
        self.assertNotIn('fixture_private_runtime', response.text)
        self.assertNotEqual(response.status_code, 200)

    def test_public_probe_never_reads_or_returns_runtime_details(self):
        self.controller._read_cache_put(('health',), {'data': {'fixture_private_runtime': 'not public'}})
        response = self.client.get('/api/health?probe=1')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['service'], 'clipflow_backend')
        self.assertNotIn('data', response.json())
        self.state.get_backend_runtime.assert_not_called()
        self.controller._consume_rate_limit.return_value = (False, 1)
        self.assertNotIn('fixture_private_runtime', self.client.get('/api/health?probe=1').text)

    def test_local_startup_keeps_detailed_health_without_browser_login(self):
        client = TestClient(self.app, client=('127.0.0.1', 4321))
        self.addCleanup(client.close)
        response = client.get('/api/health')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['data']['runtime'], {'fixture_private_runtime': 'not public'})

    def test_registered_business_api_routes_deny_anonymous_requests(self):
        checked = 0
        public_auth = {'/api/auth/login', '/api/auth/feishu/callback', '/api/auth/logout',
                       '/api/auth/status', '/api/auth/guest', '/api/auth/password/people',
                       '/api/auth/password/login', '/api/auth/password/reset'}
        for route in self.app.routes:
            path = getattr(route, 'path', '')
            if not path.startswith('/api/') or path in public_auth:
                continue
            path = re.sub(r'\{[^}]+\}', 'fixture', path)
            for method in sorted(getattr(route, 'methods', set()) - {'HEAD', 'OPTIONS'}):
                with self.subTest(method=method, path=path):
                    response = self.client.request(method, path, json={} if method != 'GET' else None)
                    if 'text/event-stream' in response.headers.get('content-type', ''):
                        frames = [json.loads(line[6:]) for line in response.text.splitlines() if line.startswith('data: ')]
                        self.assertEqual(len(frames), 1)
                        self.assertTrue(frames[0]['auth_required'])
                        self.assertFalse(frames[0]['ok'])
                        self.assertLessEqual(set(frames[0]), {'ok', 'error', 'auth_required'})
                        checked += 1
                        continue
                    self.assertIn(response.status_code, (400, 401, 403, 404, 405, 410, 422),
                                  f'{method} {path}: {response.status_code} {response.text[:180]}')
                    checked += 1
        self.assertGreater(checked, 350)

    def test_signature_management_aliases_do_not_expose_people_or_handwriting(self):
        for method, operations in (('GET', ('people', 'duplicates', 'preview')),
                                   ('POST', ('refresh', 'requests', 'temporary', 'merge', 'associate', 'migrate'))):
            for operation in operations:
                with self.subTest(method=method, operation=operation):
                    response = self.client.request(method, '/api/signatures/management/' + operation, json={})
                    self.assertEqual(response.status_code, 401)
        self.assertEqual(self.runtime.service._signature_management.mock_calls, [])

    def test_notice_panel_requires_login_before_loading_manager(self):
        response = self.client.get('/api/assistant/notice-panel')
        self.assertEqual(response.status_code, 401)
        self.assertTrue(response.json()['auth_required'])
        self.assertFalse(hasattr(self.controller, '_notice_panel'))

    def test_invalid_work_order_parameters_are_client_errors(self):
        for action in ('confirm', 'rollback', 'activate', 'release'):
            with self.subTest(action=action):
                response = self.client.post('/api/polling-work-orders/' + action, json={})
                self.assertEqual(response.status_code, 400)


if __name__ == '__main__':
    unittest.main()

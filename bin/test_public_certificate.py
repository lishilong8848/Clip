"""Isolated certificate status, authorization and renewal-hook tests; no real issuance."""
import base64
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
from public_access import certificate as cert

NOW = 1792300000
ORIGIN = 'https://dt.sjhl.online'


def metadata():
    return {'version': 1, 'domain': 'dt.sjhl.online', 'checked_at': NOW, 'issuer': "CN=YE1,O=Let's Encrypt,C=US",
            'not_before': NOW - 86400, 'not_after': NOW + 87 * 86400, 'fingerprint': 'public',
            'private_key': 'must-not-pass', 'renewal': {'enabled': True, 'last_check_ok': True,
            'serving_current': True, 'next_check': 'Mon 2026-10-19 04:20:00 CST', 'next_renewal_at': NOW + 57 * 86400,
            'token': 'must-not-pass'}}


class CertificateTests(unittest.TestCase):
    def setUp(self):
        cert._saved.clear()
        self.addCleanup(cert._saved.clear)
        self.clock = patch.object(cert.time, 'time', return_value=NOW).start()
        self.addCleanup(patch.stopall)
        self.settings = SimpleNamespace(origin=ORIGIN, token='fixture-not-a-real-secret')
        self.read = lambda: cert.certificate_status(self.settings)

    def test_validation_whitelists_public_fields_and_rejects_untrusted_data(self):
        value = cert.validate(metadata(), 'dt.sjhl.online', NOW)
        self.assertNotIn('private_key', value)
        self.assertNotIn('token', value['renewal'])
        for changes in ({'domain': 'another.example'}, {'checked_at': NOW + 301}, {'not_after': float('nan')},
                        {'not_before': True}, {'not_after': 0}, {'renewal': {}}, {'issuer': 'x' * 513}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                cert.validate({**metadata(), **changes}, 'dt.sjhl.online', NOW)
        self.assertEqual(cert.present(value, NOW)['remaining_days'], 87)
        self.assertEqual(cert.present(value, value['not_after'])['state'], 'expired')
        self.assertEqual(cert.present(value, NOW + 37 * 3600)['state'], 'stale')
        value['renewal']['last_check_ok'] = False
        self.assertEqual(cert.present(value, NOW)['state'], 'warning')

    def test_cache_failure_retains_stale_data_and_recovers_without_false_success(self):
        with patch.object(cert, 'fetch_status', return_value=metadata()) as fetch:
            self.assertEqual(self.read()['state'], 'healthy')
            self.assertEqual(self.read()['state'], 'healthy')
            self.assertEqual(fetch.call_count, 1)
            self.assertEqual(fetch.call_args.args[0], ORIGIN)
            self.clock.return_value = NOW + 301
            fetch.side_effect = TimeoutError()
            self.assertEqual(self.read()['state'], 'stale')
            self.clock.return_value = NOW + 302
            self.assertEqual(self.read()['state'], 'stale')
            self.assertEqual(fetch.call_count, 2)
            self.clock.return_value = NOW + 362
            fetch.side_effect = None
            self.assertEqual(self.read()['state'], 'healthy')

    def test_concurrent_cold_read_uses_one_request_and_reports_loading(self):
        entered, release = threading.Event(), threading.Event()
        results = []
        def fetch(*_args):
            entered.set()
            release.wait(3)
            return metadata()
        with patch.object(cert, 'fetch_status', side_effect=fetch) as get:
            thread = threading.Thread(target=lambda: results.append(self.read()))
            thread.start()
            try:
                self.assertTrue(entered.wait(2))
                for _ in range(20):
                    self.assertEqual(self.read()['state'], 'preparing')
            finally:
                release.set()
                thread.join(3)
            get.assert_called_once()
            self.assertEqual(results[0]['state'], 'healthy')

    def test_failure_is_backed_off_and_never_claims_renewal_enabled(self):
        with patch.object(cert, 'fetch_status', side_effect=TimeoutError()) as fetch:
            self.assertEqual(self.read(), {'state': 'unavailable'})
            self.assertEqual(self.read(), {'state': 'unavailable'})
            fetch.assert_called_once()

    def test_missing_formal_host_credential_never_makes_an_anonymous_probe(self):
        with patch.object(cert, 'fetch_status') as fetch:
            self.assertEqual(cert.certificate_status(None), {'state': 'unavailable'})
            self.assertEqual(cert.certificate_status(SimpleNamespace(token='')), {'state': 'unavailable'})
            fetch.assert_not_called()

    def test_http_transport_bounds_content_and_rejects_redirect_or_bad_json(self):
        response = {'status': 200, 'content': json.dumps(metadata())}
        client = httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(response['status'], content=response['content'])))
        with patch('httpx.Client', return_value=client), patch('upload_event_module.services.http_client.verified_tls_context'):
            self.assertEqual(cert.fetch_status(ORIGIN, 'fixture-access-key')['domain'], 'dt.sjhl.online')
        for status, content in ((302, ''), (200, 'x' * 9000), (200, 'not json')):
            response.update(status=status, content=content)
            client = httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(response['status'], content=response['content'])))
            with patch('httpx.Client', return_value=client), patch('upload_event_module.services.http_client.verified_tls_context'), self.assertRaises(Exception):
                cert.fetch_status(ORIGIN, 'fixture-access-key')

    def test_route_checks_live_trusted_level_and_rejects_anonymous_guest_and_revoked(self):
        app = FastAPI()
        controller = SimpleNamespace(_current_session=Mock(return_value=None), _auth_required_response=lambda:
                                     JSONResponse({'ok': False}, status_code=401))
        runtime = SimpleNamespace(auth_manager=SimpleNamespace(public_user=Mock(return_value={})))
        cert.install_certificate_routes(app, controller, runtime)
        with TestClient(app) as client, patch.object(cert, 'certificate_status', return_value={'state': 'healthy'}) as read:
            self.assertEqual(client.get('/api/auth/certificate').status_code, 401)
            for level, role in [('L0', 'admin'), ('L1', 'admin'), ('L1', 'admin')]:
                controller._current_session.return_value = {'role': 'admin', 'user': {'open_id': 'fixture'}}
                runtime.auth_manager.public_user.return_value = {'role': role, 'level': level, 'category': 'duty', 'home_scope': 'H'}
                response = client.get('/api/auth/certificate')
                self.assertEqual(response.status_code, 200)
                self.assertIn('no-store', response.headers['cache-control'])
            read.reset_mock()
            for level, role in [('L2', 'admin'), ('L3', 'admin'), ('L4', 'building'), ('L0', 'building')]:
                runtime.auth_manager.public_user.return_value = {'level': level, 'role': role}
                self.assertEqual(client.get('/api/auth/certificate?level=L0').status_code, 403)
            runtime.auth_manager.public_user.return_value = {'level': 'L0', 'role': 'admin'}
            for role in ('guest', 'temporary'):
                controller._current_session.return_value = {'role': role}
                self.assertEqual(client.get('/api/auth/certificate').status_code, 403)
            read.assert_not_called()
            controller._current_session.return_value = {'role': 'admin'}
            runtime.auth_manager.public_user.return_value = {'role': 'admin', 'level': 'L1'}
            with patch.object(cert, 'certificate_status', side_effect=OSError('secret/path/token')):
                response = client.get('/api/auth/certificate')
                self.assertEqual(response.json()['data'], {'state': 'unavailable'})
                self.assertNotIn('token', response.text)


spec = importlib.util.spec_from_file_location('certificate_server_fixture', Path(__file__).parent / 'public_access/server/certificate-status.py')
server = importlib.util.module_from_spec(spec)
spec.loader.exec_module(server)


class CertificateServerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        path = self.base / 'acme-certs/dt.sjhl.online_ecc/dt.sjhl.online.conf'
        path.parent.mkdir(parents=True)
        command = base64.b64encode(b'nginx -t && nginx -s reload').decode()
        path.write_text("Le_NextRenewTime='1797185923'\nLe_ReloadCmd='__ACME_BASE64__START_" + command + "__ACME_BASE64__END_'\nACCOUNT_KEY='PRIVATE_DO_NOT_PUBLISH'\n")
        patch.object(server, 'BASE', self.base).start()
        patch.object(server, 'STATUS', self.base / 'status/certificate.json').start()
        patch.dict(server.os.environ, {'SERVICE_RESULT': 'success'}).start()
        patch.object(server.os, 'fchmod', create=True).start()
        self.addCleanup(patch.stopall)
        def output(*args):
            if args[:2] == ('openssl', 'x509') and '-dates' in args:
                return "subject=CN=dt.sjhl.online\nissuer=CN=YE1,O=Let's Encrypt,C=US\nnotBefore=Oct 15 06:25:18 2026 GMT\nnotAfter=Jan 13 06:25:17 2027 GMT\nsha256 Fingerprint=AA:BB"
            if '-checkhost' in args:
                return 'Hostname dt.sjhl.online does match certificate'
            if args[:2] == ('systemctl', 'show'):
                return 'ActiveState=active\nUnitFileState=enabled\nNextElapseUSecRealtime=Mon 2026-10-19 04:20:00 CST'
            return ''
        self.commands = patch.object(server, 'output', side_effect=output).start()
        self.live = patch.object(server, 'live_fingerprint', return_value='aabb').start()

    def test_current_certificate_does_not_reload_or_expose_private_configuration(self):
        value = server.collect()
        self.assertTrue(value['renewal']['enabled'])
        self.assertTrue(value['renewal']['serving_current'])
        self.assertNotIn('PRIVATE_DO_NOT_PUBLISH', json.dumps(value))
        self.assertFalse(any(call.args[0] == 'nginx' for call in self.commands.call_args_list))

    def test_missed_reload_is_repaired_by_config_check_then_graceful_reload_only(self):
        self.live.side_effect = ['old', 'aabb']
        with patch.object(server.time, 'sleep'):
            self.assertTrue(server.collect()['renewal']['serving_current'])
        self.assertEqual([call.args for call in self.commands.call_args_list if call.args[0] == 'nginx'],
                         [('nginx', '-t'), ('nginx', '-s', 'reload')])

    def test_failed_check_preserves_certificate_and_writes_failure_not_success(self):
        prior = server.collect()
        server.STATUS.parent.mkdir()
        server.STATUS.write_text(json.dumps(prior))
        with patch.object(server, 'collect', side_effect=OSError('do-not-expose-secret')):
            server.main()
        result = json.loads(server.STATUS.read_text())
        self.assertEqual(result['not_after'], prior['not_after'])
        self.assertFalse(result['renewal']['last_check_ok'])
        self.assertNotIn('do-not-expose-secret', server.STATUS.read_text())

    def test_renewal_failure_does_not_try_to_reload(self):
        self.live.return_value = 'old'
        with patch.dict(server.os.environ, {'SERVICE_RESULT': 'exit-code'}):
            value = server.collect()
        self.assertFalse(value['renewal']['last_check_ok'])
        self.assertFalse(any(call.args[0] == 'nginx' for call in self.commands.call_args_list))

    def test_wrong_hostname_is_rejected_before_reload(self):
        old_output = self.commands.side_effect
        self.commands.side_effect = lambda *args: 'Hostname does NOT match certificate' if '-checkhost' in args else old_output(*args)
        with self.assertRaises(ValueError):
            server.collect()
        self.assertFalse(any(call.args[0] == 'nginx' for call in self.commands.call_args_list))

    def test_packaging_and_service_register_status_without_new_client_startup_work(self):
        root = Path(__file__).resolve().parents[1]
        service = (root / 'bin/public_access/server/clipflow-cert-renew.service').read_text()
        self.assertIn('ExecStopPost=/usr/bin/python3', service)
        self.assertNotIn('--force', service)
        nginx = (root / 'bin/public_access/server/dt-https.conf').read_text()
        self.assertIn('location = /.well-known/clipflow-certificate.json', nginx)
        self.assertIn('bin.test_public_certificate', (root / 'package_portable.py').read_text())


if __name__ == '__main__':
    unittest.main()

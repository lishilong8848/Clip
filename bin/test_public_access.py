"""Public tunnel isolation tests: no real server, credentials or business writes."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch
import zipfile

BIN = Path(__file__).resolve().parent
if str(BIN) not in sys.path:
    sys.path.insert(0, str(BIN))

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from public_access import attach
from public_access.config import Settings, atomic_json
from public_access.distribution import create_archive, install, installed, previous, checksum
from public_access.ingress import PublicGuard, StreamingIngress, MARKER
from public_access.supervisor import PublicAccess, BACKOFF

SETTINGS = Settings(enabled=True, public_url='https://portal.example.com',
                    server_addr='192.0.2.2', token='test-only-token')
SECRET = 'test-ingress-marker'


def make_app():
    app = FastAPI()

    @app.get('/')
    async def home():
        return {'ok': True}

    @app.api_route('/api/workbench', methods=['GET', 'POST'])
    async def workbench(request: Request):
        return {'client': request.client.host, 'scheme': request.url.scheme,
                'cookie': request.headers.get('cookie', ''), 'query': request.url.query,
                'body': (await request.body()).decode(), 'marker': request.headers.get(MARKER.decode())}

    @app.get('/api/auth/status')
    async def status(request: Request):
        return JSONResponse({'ok': True}, headers={'Set-Cookie': 'session=alice; HttpOnly; SameSite=Lax'})

    @app.post('/api/auth/password/login')
    async def login():
        return {'ok': True}

    @app.get('/api/polling-work-orders/session')
    @app.get('/api/signatures/people')
    async def signed(request: Request):
        return JSONResponse({'ok': request.query_params.get('token') == 'valid'},
                            status_code=200 if request.query_params.get('token') == 'valid' else 403)

    @app.get('/api/assistant/stream')
    async def stream():
        async def content():
            yield b'data: one\n\n'
            yield b'data: two\n\n'
        return StreamingResponse(content(), media_type='text/event-stream')

    @app.api_route('/api/qt/events', methods=['GET', 'POST'])
    @app.post('/api/backend/shutdown')
    @app.post('/api/auth/guest')
    @app.get('/api/unreviewed-module')
    async def private():
        return {'should_not_be_public': True}
    return app


class GuardTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.session = {'user': {'open_id': 'alice'}}
        self.reader = Mock(side_effect=lambda request: self.session)
        self.guard = PublicGuard(make_app(), SECRET, SETTINGS, self.reader)
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.guard, client=('127.0.0.1', 1)),
                                        base_url=SETTINGS.public_url, headers={MARKER.decode(): SECRET})

    async def asyncTearDown(self):
        await self.client.aclose()

    async def test_public_request_cannot_be_treated_as_local(self):
        response = await self.client.get('/api/workbench', headers={'X-Forwarded-For': '127.0.0.1'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['client'], '192.0.2.1')
        self.assertEqual(response.json()['scheme'], 'https')
        self.assertIsNone(response.json()['marker'])

    async def test_internal_routes_denied_even_for_logged_in_admin(self):
        for path in ('/api/qt/events', '/api/backend/shutdown'):
            response = await self.client.post(path, headers={'Origin': SETTINGS.public_url})
            self.assertEqual(response.status_code, 403, path)
        self.assertEqual((await self.client.get('/api/unreviewed-module')).status_code, 403)
        self.assertEqual((await self.client.get('/openapi.json')).status_code, 403)

    async def test_anonymous_guest_entry_retains_same_origin_requirement(self):
        self.session = None
        self.assertEqual((await self.client.post('/api/auth/guest', headers={'Origin': SETTINGS.public_url})).status_code, 200)
        self.assertEqual((await self.client.post('/api/auth/guest')).status_code, 403)
        self.assertEqual((await self.client.post('/api/auth/guest', headers={'Origin': 'https://evil.example'})).status_code, 403)

    async def test_lan_request_bypasses_only_public_policy(self):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=self.guard), base_url='http://test') as lan:
            response = await lan.get('/api/qt/events')
            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.json()['should_not_be_public'])

    async def test_forged_marker_host_and_cross_origin_denied(self):
        self.assertEqual((await self.client.get('/', headers={MARKER.decode(): 'wrong'})).status_code, 403)
        self.assertEqual((await self.client.get('/', headers={'Host': 'evil.example'})).status_code, 400)
        self.assertEqual((await self.client.post('/api/workbench', headers={'Origin': 'https://evil.example'})).status_code, 403)
        self.assertEqual((await self.client.post('/api/workbench')).status_code, 403)
        self.assertEqual((await self.client.post('/api/workbench', headers={'Origin': SETTINGS.public_url})).status_code, 200)

    async def test_anonymous_and_guest_cannot_read_business(self):
        self.session = None
        self.assertEqual((await self.client.get('/api/workbench')).status_code, 401)
        self.assertEqual((await self.client.get('/api/auth/status')).status_code, 200)
        self.session = {'role': 'guest'}
        self.assertEqual((await self.client.get('/api/workbench')).status_code, 403)

    async def test_signed_link_still_verifies_original_token(self):
        self.session = None
        self.assertEqual((await self.client.get('/api/polling-work-orders/session')).status_code, 403)
        self.assertEqual((await self.client.get('/api/polling-work-orders/session?token=valid')).status_code, 200)
        self.assertEqual((await self.client.get('/api/signatures/people')).status_code, 403)
        self.assertEqual((await self.client.get('/api/signatures/people?record_id=rec-one&token=valid')).status_code, 200)

    async def test_public_health_is_minimal_and_cookie_is_secure(self):
        self.session = None
        self.assertEqual((await self.client.get('/api/health')).json(), {'ok': True, 'service': 'clipflow_backend'})
        self.assertIn('; Secure', (await self.client.get('/api/auth/status')).headers['set-cookie'])

    async def test_auth_limit_does_not_block_business_reads(self):
        self.guard.auth_times = [__import__('time').monotonic()] * 30
        self.assertEqual((await self.client.post('/api/auth/password/login', headers={'Origin': SETTINGS.public_url})).status_code, 429)
        self.assertEqual((await self.client.get('/api/workbench')).status_code, 200)

    async def test_encoded_traversal_is_denied(self):
        for path in ('/api/qt%2fevents', '/api/%252e%252e/qt/events', '/api/qt%5cevents'):
            self.assertEqual((await self.client.get(path)).status_code, 403, path)


class ProxyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        guard = PublicGuard(make_app(), SECRET, SETTINGS, lambda r: {'user': {'open_id': 'alice'}})
        upstream = httpx.AsyncClient(transport=httpx.ASGITransport(app=guard, client=('127.0.0.1', 1)))
        self.proxy = StreamingIngress('http://127.0.0.1:18766', SECRET, SETTINGS, upstream)
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.proxy), base_url=SETTINGS.public_url)

    async def asyncTearDown(self):
        await self.client.aclose()
        await self.proxy.close()

    async def test_round_trip_upload_and_query(self):
        response = await self.client.post('/api/workbench?scope=E&name=%E6%B5%8B%E8%AF%95',
            content=b'upload data', headers={'Origin': SETTINGS.public_url, 'Cookie': 'session=bob'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['body'], 'upload data')
        self.assertIn('scope=E', response.json()['query'])
        self.assertEqual(response.json()['cookie'], 'session=bob')

    async def test_shared_upstream_cookie_jar_never_leaks_between_people(self):
        await self.client.get('/api/auth/status')
        self.client.cookies.clear()
        response = await self.client.get('/api/workbench')
        self.assertEqual(response.json()['cookie'], '')

    async def test_sse_and_public_marker_spoof(self):
        response = await self.client.get('/api/assistant/stream', headers={MARKER.decode(): 'attacker', 'X-Forwarded-For': '127.0.0.1'})
        self.assertEqual(response.text, 'data: one\n\ndata: two\n\n')
        self.assertEqual(self.proxy.active, 0)
        self.assertEqual((await self.client.get('/api/qt/events')).status_code, 403)

    async def test_oversize_host_and_concurrency(self):
        self.assertEqual((await self.client.get('/', headers={'Host': 'evil.example'})).status_code, 400)
        self.assertEqual((await self.client.post('/api/workbench', headers={'Content-Length': str(301 * 1024**2)})).status_code, 413)
        self.proxy.active = SETTINGS.max_requests
        self.assertEqual((await self.client.get('/')).status_code, 503)

    async def test_network_error_does_not_replay_submission(self):
        calls = []
        async def fail(request):
            calls.append(request)
            raise httpx.ReadTimeout('test timeout')
        await self.proxy.client.aclose()
        self.proxy.client = httpx.AsyncClient(transport=httpx.MockTransport(fail))
        response = await self.client.post('/api/workbench', content=b'one request', headers={'Origin': SETTINGS.public_url})
        self.assertEqual(response.status_code, 502)
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.proxy.active, 0)


class ConfigAndDistributionTests(unittest.TestCase):
    def test_update_bundles_health_runtime_and_retains_local_incident_state(self):
        import shutil
        import package_portable as package
        from frontend_assets import patch_deletions
        runtime = [path.relative_to(package.PROJECT_ROOT) for path in (BIN / 'public_access').iterdir()
                   if path.suffix in {'.py', '.json'}]
        runtime += [Path(name) for name in (
            'bin/tools/check_public_access.py', 'bin/clipflow_backend/main.py',
            'bin/lan_bitable_template_portal/portal_service.py',
            'bin/refactored_main.py', '启动程序.bat')]
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project, installed = root / 'project', root / 'installed'
            for relative in runtime:
                target = project / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(package.PROJECT_ROOT / relative, target)
            shutil.copytree(project, installed)
            state = Path('bin/data/public_access/health.json')
            atomic_json(project / state, {'incident_id': 'developer-must-not-ship'})
            atomic_json(installed / state, {'incident_id': 'ongoing-outage', 'notified': ['fixture-li', 'fixture-h']})
            before = (installed / state).read_bytes()
            with patch.object(package, 'PROJECT_ROOT', project), patch.object(package, 'BUILD_DIR', root / 'build'):
                package.build_patch(root / 'unused', installed, 'health-update')
            payload = root / 'build/health-update_patch_only'
            meta = json.loads((payload / 'bin/patch_meta.json').read_text(encoding='utf-8'))
            self.assertTrue(meta['restart_required'])
            self.assertFalse((payload / state).exists())
            for relative in runtime:
                self.assertEqual((payload / relative).read_bytes(), (project / relative).read_bytes())
                self.assertEqual(meta['file_sha256'][relative.as_posix()], checksum(payload / relative))
            removed = patch_deletions(installed, payload, [])
            self.assertNotIn(state, removed)
            shutil.copytree(payload, installed, dirs_exist_ok=True)
            self.assertEqual((installed / state).read_bytes(), before)

    def test_full_public_runtime_patch_requires_restart_even_with_current_baseline(self):
        import package_portable as package
        for mode in ('added', 'changed', 'deleted', 'unchanged'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                project, baseline = root / 'project', root / 'baseline'
                for folder in (project, baseline):
                    (folder / 'bin/public_access').mkdir(parents=True)
                relative = Path('bin/public_access/supervisor.py')
                if mode != 'deleted':
                    (project / relative).write_text('VERSION = 2\n', encoding='utf-8')
                if mode != 'added':
                    (baseline / relative).write_text(
                        'VERSION = 2\n' if mode == 'unchanged' else 'VERSION = 1\n', encoding='utf-8')
                with patch.object(package, 'PROJECT_ROOT', project), \
                     patch.object(package, 'BUILD_DIR', root / 'build'), \
                     patch.object(package, '_include_frontend_generation', return_value=0), \
                     patch.object(package, '_include_assistant_skills', return_value=0):
                    package.build_patch(root / 'dist', baseline, 'fixture')
                meta = json.loads((root / 'build/fixture_patch_only/bin/patch_meta.json').read_text(encoding='utf-8'))
                self.assertFalse(meta['ui_changed'])
                self.assertTrue(meta['restart_required'])

    def test_disabled_does_not_start_or_download(self):
        app = make_app()
        with patch('public_access.config.DISABLED') as disabled, patch('public_access.provisioning.read_deployment') as read, \
                patch('public_access.supervisor.PublicAccess') as manager:
            disabled.is_file.return_value = True
            self.assertEqual(attach(app, Mock()), (app, None))
            manager.assert_not_called()
            read.assert_not_called()

    def test_invalid_config_cannot_break_lan_startup(self):
        app = make_app()
        with patch('public_access.config.DISABLED') as disabled, \
                patch('public_access.provisioning.read_deployment', side_effect=ValueError('invalid')):
            disabled.is_file.return_value = False
            self.assertEqual(attach(app, Mock()), (app, None))

    def test_generated_config_cannot_forward_to_portal_directly(self):
        config = SETTINGS.frpc_config()
        self.assertFalse(config['loginFailExit'])
        self.assertTrue(config['transport']['tls']['enable'])
        self.assertEqual(config['proxies'][0]['localPort'], 18767)
        self.assertNotIn('includes', config)

    def test_config_requires_https_and_bounded_parallelism(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'config.json'
            self.assertFalse(Settings.read(path).enabled)
            for value in ({'enabled': 'true'}, {'max_requests': 1000}, {'enabled': True, 'public_url': 'http://example.com'}):
                atomic_json(path, value)
                with self.assertRaises(ValueError):
                    Settings.read(path)

    def make_package(self, folder):
        source = folder / 'source'
        source.mkdir()
        (source / 'frpc.exe').write_bytes(b'MZ-test-binary')
        (source / 'LICENSE').write_text('license', encoding='utf-8')
        (source / 'frpc.toml').write_text('auth.token="never-package"', encoding='utf-8')
        (source / 'frps.exe').write_bytes(b'server-not-needed')
        output = folder / 'package.zip'
        spec = create_archive(source, output, '0.71.0')
        for part in spec['parts']:
            part.update(record_id='rec-test', file_token='file-test')
        atomic_json(folder / 'spec.json', spec)
        return spec, output

    def test_package_contains_no_config_and_installed_version_is_reused(self):
        with tempfile.TemporaryDirectory() as temp:
            folder = Path(temp)
            spec, archive = self.make_package(folder)
            with zipfile.ZipFile(archive) as bundle:
                self.assertEqual(set(bundle.namelist()), {'frpc.exe', 'LICENSE'})
            mirror = Mock()
            mirror.download_part.side_effect = lambda part, dest: Path(dest).write_bytes((folder / part['name']).read_bytes())
            factory = Mock(return_value=mirror)
            binary, actual = install(folder / 'runtime', spec_path=folder / 'spec.json', mirror_factory=factory)
            self.assertEqual(checksum(binary), spec['files']['frpc.exe'])
            self.assertEqual(install(folder / 'runtime', spec_path=folder / 'spec.json', mirror_factory=factory), (binary, actual))
            self.assertEqual(factory.call_count, 1)
            atomic_json(folder / 'runtime/working.json', actual)
            self.assertEqual(previous(folder / 'runtime'), (binary, actual))

    def test_corrupt_download_and_cancellation_never_install(self):
        with tempfile.TemporaryDirectory() as temp:
            folder = Path(temp)
            spec, _ = self.make_package(folder)
            mirror = Mock()
            mirror.download_part.side_effect = lambda part, dest: Path(dest).write_bytes(b'corrupt')
            with self.assertRaises(ValueError):
                install(folder / 'runtime', spec_path=folder / 'spec.json', mirror_factory=lambda: mirror)
            self.assertIsNone(installed(folder / 'runtime', spec))
            with self.assertRaises(InterruptedError):
                install(folder / 'runtime', spec_path=folder / 'spec.json', mirror_factory=lambda: mirror, cancelled=lambda: True)
            self.assertFalse((folder / 'runtime/working.json').exists())

    def test_release_iterator_excludes_secrets_and_large_runtime(self):
        import package_portable as package
        for name in ('bin/data/public_access/config.json', 'bin/data/public_access/frpc.generated.json',
                     'bin/data/public_access/frpc/versions/abc/frpc.exe'):
            self.assertTrue(package._is_runtime_data_path(package.PROJECT_ROOT / name, package.PROJECT_ROOT))

    def test_supervisor_cancellation_does_not_spawn_client(self):
        with tempfile.TemporaryDirectory() as temp, socket.socket() as reserve:
            reserve.bind(('127.0.0.1', 0))
            port = reserve.getsockname()[1]
            reserve.close()
            manager = PublicAccess(replace(SETTINGS, ingress_port=port), 'http://127.0.0.1:18766', SECRET, data=Path(temp))
            def cancelled(*args, **kwargs):
                manager.stop()
                raise InterruptedError()
            with patch('public_access.supervisor.previous', return_value=(None, None)), \
                    patch('public_access.supervisor.install', side_effect=cancelled), \
                    patch('public_access.supervisor.subprocess.Popen') as popen:
                manager._run()
                popen.assert_not_called()
            self.assertEqual(manager.state, 'stopped')

    def test_first_component_download_failure_retries_without_starting_a_tunnel(self):
        with tempfile.TemporaryDirectory() as temp, socket.socket() as reserve:
            reserve.bind(('127.0.0.1', 0))
            port = reserve.getsockname()[1]
            reserve.close()
            manager = PublicAccess(replace(SETTINGS, ingress_port=port),
                'http://127.0.0.1:18766', SECRET, data=Path(temp))
            attempts = []
            def install(*args, **kwargs):
                attempts.append(1)
                if len(attempts) == 1:
                    raise OSError('offline')
                manager.stop()
                return Path(temp) / 'frpc.exe', {'version': 'fixture'}
            with patch('public_access.supervisor.previous', return_value=(None, None)), \
                    patch('public_access.supervisor.install', side_effect=install), \
                    patch.object(manager.stop_event, 'wait', return_value=False) as wait, \
                    patch('public_access.supervisor.subprocess.Popen') as spawn:
                manager._run()
                self.assertEqual(len(attempts), 2)
                wait.assert_called_once_with(5)
                spawn.assert_not_called()
                self.assertEqual(manager.state, 'stopped')

    def test_background_start_is_single_and_stop_only_terminates_owned_child(self):
        manager = PublicAccess(SETTINGS, 'http://127.0.0.1:18766', SECRET)
        with patch('public_access.supervisor.threading.Thread') as thread:
            manager.start()
            manager.start()
            self.assertEqual(thread.call_count, 1)
        manager.child = Mock()
        manager.stop()
        manager.child.terminate.assert_called_once()
        self.assertEqual(BACKOFF, (5, 15, 30, 60))

    def test_health_monitor_survives_frpc_worker_failure_until_portal_stop(self):
        with tempfile.TemporaryDirectory() as temp, socket.socket() as reserve:
            reserve.bind(('127.0.0.1', 0))
            port = reserve.getsockname()[1]
            reserve.close()
            settings = replace(SETTINGS, ingress_port=port)
            manager = PublicAccess(settings, 'http://127.0.0.1:18766', SECRET, data=Path(temp),
                deployment={'deployment_host': '192.168.224.130'})
            with patch('public_access.health.Monitor') as monitor, \
                    patch('public_access.provisioning.provision', return_value=settings), \
                    patch('public_access.supervisor.previous', return_value=(None, None)), \
                    patch('public_access.supervisor.install', return_value=(Path(temp) / 'frpc.exe', {})), \
                    patch('public_access.supervisor.verify_binary', side_effect=ValueError('invalid binary')):
                manager.start()
                manager.thread.join(4)
                self.assertFalse(manager.thread.is_alive())
                self.assertEqual(manager.state, 'failed')
                monitor.return_value.start.assert_called_once()
                monitor.return_value.stop.assert_not_called()
                manager.stop()
                monitor.return_value.stop.assert_called_once()

    def test_background_stop_reaps_child_and_closes_ingress(self):
        real_popen = subprocess.Popen
        children = []
        with tempfile.TemporaryDirectory() as temp, socket.socket() as reserve:
            reserve.bind(('127.0.0.1', 0))
            port = reserve.getsockname()[1]
            reserve.close()
            manager = PublicAccess(replace(SETTINGS, ingress_port=port),
                                   'http://127.0.0.1:18766', SECRET, data=Path(temp))

            def spawn(args, **kwargs):
                child = real_popen([sys.executable, '-I', '-S', '-u', '-c',
                    "import time; print('start proxy success [clipflow-public]', flush=True); time.sleep(30)"], **kwargs)
                children.append(child)
                return child

            # Verify lifecycle, without background I/O starvation from parallel test processes.
            with patch('public_access.supervisor.previous', return_value=(None, None)), \
                 patch('public_access.supervisor.install', return_value=(Path(temp) / 'frpc.exe', {'version': 'fixture'})), \
                 patch('public_access.supervisor.verify_binary'), \
                 patch('public_access.supervisor.subprocess.Popen', side_effect=spawn), \
                 patch('upload_event_module.services.process_lifetime.lower_current_thread_priority'), \
                 patch('upload_event_module.services.process_lifetime.register_child_process', return_value=True):
                try:
                    manager.start()
                    worker = manager.thread
                    manager.start()
                    self.assertIs(manager.thread, worker)
                    deadline = time.monotonic() + 8
                    while manager.state != 'connected' and worker.is_alive() and time.monotonic() < deadline:
                        time.sleep(.02)
                    self.assertEqual(manager.state, 'connected')
                    self.assertEqual(len(children), 1)
                    self.assertIsNone(children[0].poll())
                    manager.stop()
                    worker.join(8)
                    self.assertFalse(worker.is_alive())
                    self.assertIsNotNone(children[0].poll())
                    self.assertEqual(manager.state, 'stopped')
                    with socket.socket() as probe:
                        probe.settimeout(.2)
                        self.assertNotEqual(probe.connect_ex(('127.0.0.1', port)), 0)
                finally:
                    manager.stop()
                    if manager.thread:
                        manager.thread.join(8)
                    for child in children:
                        if child.poll() is None:
                            child.kill()
                            child.wait(timeout=3)

    def test_occupied_ingress_port_does_not_download_or_spawn(self):
        with tempfile.TemporaryDirectory() as temp, socket.socket() as occupied:
            occupied.bind(('127.0.0.1', 0))
            occupied.listen()
            manager = PublicAccess(replace(SETTINGS, ingress_port=occupied.getsockname()[1]),
                                   'http://127.0.0.1:18766', SECRET, data=Path(temp))
            config = Path(temp) / 'frpc.generated.json'
            config.write_text('owned by the running instance', encoding='utf-8')
            with patch('public_access.supervisor.install') as installer, \
                    patch('public_access.supervisor.subprocess.Popen') as popen:
                manager._run()
                installer.assert_not_called()
                popen.assert_not_called()
            self.assertEqual(manager.state, 'failed')
            self.assertGreater(occupied.getsockname()[1], 0)
            self.assertEqual(config.read_text(encoding='utf-8'), 'owned by the running instance')

    def test_corrupt_executable_is_not_reused(self):
        with tempfile.TemporaryDirectory() as temp:
            folder = Path(temp)
            spec, _ = self.make_package(folder)
            mirror = Mock()
            mirror.download_part.side_effect = lambda part, dest: Path(dest).write_bytes((folder / part['name']).read_bytes())
            binary, _ = install(folder / 'runtime', spec_path=folder / 'spec.json', mirror_factory=lambda: mirror)
            binary.write_bytes(b'changed-binary')
            self.assertIsNone(installed(folder / 'runtime', spec))
            binary, _ = install(folder / 'runtime', spec_path=folder / 'spec.json', mirror_factory=lambda: mirror)
            self.assertEqual(checksum(binary), spec['files']['frpc.exe'])


class ProvisioningTests(unittest.TestCase):
    def profile(self):
        from openclaw_service.assistant.lighthouse_distribution import BASE, TABLE
        value = {'format': 1, 'deployment_host': '192.168.224.130', 'settings': asdict(SETTINGS)}
        content = json.dumps(value, sort_keys=True).encode('utf-8')
        digest = hashlib.sha256(content).hexdigest()
        public = asdict(SETTINGS)
        public.pop('token')
        return {'format': 1, 'deployment_host': value['deployment_host'], 'settings': public,
                'credential': {'format': 1, 'component': 'public_access_profile', 'version': digest,
                    'platform': 'windows_amd64', 'base': BASE, 'table': TABLE,
                    'sha256': digest, 'size': len(content), 'parts': [{'index': 1,
                        'size': len(content), 'sha256': digest, 'record_id': 'rec-fixture', 'file_token': 'fixture'}]}}, content

    def test_manifest_validates_and_contains_no_token(self):
        from public_access.provisioning import read_deployment
        profile, _ = self.profile()
        with tempfile.TemporaryDirectory() as temp:
            file = Path(temp) / 'deployment.json'
            atomic_json(file, profile)
            loaded, settings = read_deployment(file)
            self.assertEqual(loaded, profile)
            self.assertEqual(settings.token, '')
            self.assertTrue(settings.enabled)
            for invalid in ({**profile, 'deployment_host': '127.0.0.1'},
                            {**profile, 'settings': {**profile['settings'], 'token': 'do-not-ship'}},
                            {**profile, 'credential': {**profile['credential'], 'size': 8193}}):
                atomic_json(file, invalid)
                with self.assertRaises(Exception):
                    read_deployment(file)

    def test_local_interface_binding_never_connects_to_remote_host(self):
        from public_access.provisioning import local_host
        with patch('public_access.provisioning.socket.socket') as factory:
            probe = factory.return_value.__enter__.return_value
            probe.bind.side_effect = OSError('not assigned locally')
            self.assertFalse(local_host('192.168.224.130'))
            probe.bind.assert_called_once_with(('192.168.224.130', 0))
            probe.connect.assert_not_called()
            probe.bind.side_effect = None
            self.assertTrue(local_host('192.168.224.130'))

    def test_other_hosts_ignore_even_an_enabled_copied_config(self):
        from public_access.provisioning import provision
        profile, _ = self.profile()
        app = make_app()
        with patch('public_access.config.DISABLED') as disabled, \
                patch('public_access.provisioning.read_deployment', return_value=(profile, SETTINGS)), \
                patch('public_access.provisioning.local_host', return_value=False), \
                patch('public_access.config.Settings.read', return_value=SETTINGS), \
                patch('public_access.supervisor.PublicAccess') as manager:
            disabled.is_file.return_value = False
            self.assertEqual(attach(app, Mock()), (app, None))
            manager.assert_not_called()
            factory = Mock()
            with self.assertRaises(ValueError):
                provision(profile, mirror_factory=factory)
            factory.assert_not_called()

    def test_formal_host_attaches_without_a_cloud_request_or_reading_old_config(self):
        profile, _ = self.profile()
        app = make_app()
        controller = Mock(bound_port=18766)
        controller.get_url.return_value = 'http://127.0.0.1:18766'
        with patch('public_access.config.DISABLED') as disabled, \
                patch('public_access.provisioning.read_deployment', return_value=(profile, SETTINGS)), \
                patch('public_access.provisioning.local_host', return_value=True), \
                patch('public_access.config.Settings.read') as old_config, \
                patch('public_access.provisioning.provision') as download:
            disabled.is_file.return_value = False
            guard, manager = attach(app, controller)
            self.assertIsInstance(guard, PublicGuard)
            self.assertIsNone(manager.thread)
            self.assertEqual(manager.deployment, profile)
            old_config.assert_not_called()
            download.assert_not_called()

    @unittest.skipUnless(os.name == 'nt', 'Windows DPAPI cache')
    def test_empty_host_downloads_once_then_uses_encrypted_verified_cache(self):
        from public_access.provisioning import provision
        profile, content = self.profile()
        mirror = Mock()
        mirror.download_part.side_effect = lambda part, path: path.write_bytes(content)
        factory = Mock(return_value=mirror)
        with tempfile.TemporaryDirectory() as temp, patch('public_access.provisioning.local_host', return_value=True):
            root = Path(temp) / 'private'
            self.assertEqual(provision(profile, root, mirror_factory=factory), SETTINGS)
            self.assertEqual(provision(profile, root, mirror_factory=factory), SETTINGS)
            factory.assert_called_once()
            mirror.close.assert_called_once()
            saved = (root / 'deployment-credential.json').read_text(encoding='utf-8')
            self.assertNotIn(SETTINGS.token, saved)
            self.assertEqual(list(root.glob('credential-*')), [])
            # Updates preserve the runtime cache; a changed profile must not reuse old credentials.
            changed = json.loads(json.dumps(profile))
            changed['credential']['sha256'] = '0' * 64
            mirror.download_part.side_effect = OSError('offline')
            with self.assertRaises(OSError):
                provision(changed, root, mirror_factory=factory)
            self.assertEqual((root / 'deployment-credential.json').read_text(encoding='utf-8'), saved)

    def test_corrupt_or_wrong_host_payload_is_never_cached(self):
        from public_access.provisioning import provision, decode_credential
        profile, content = self.profile()
        with self.assertRaises(ValueError):
            decode_credential(content + b'changed', profile)
        wrong = json.loads(content)
        wrong['deployment_host'] = '192.168.224.122'
        raw = json.dumps(wrong).encode()
        changed = {**profile, 'credential': {**profile['credential'], 'sha256': hashlib.sha256(raw).hexdigest(), 'size': len(raw)}}
        with self.assertRaises(ValueError):
            decode_credential(raw, changed)
        mirror = Mock()
        mirror.download_part.side_effect = lambda part, path: path.write_bytes(b'corrupt')
        with tempfile.TemporaryDirectory() as temp, patch('public_access.provisioning.local_host', return_value=True), \
                patch('openclaw_service.protocol.protect_state_directory'):
            with self.assertRaises(ValueError):
                provision(profile, Path(temp), mirror_factory=lambda: mirror)
            self.assertFalse((Path(temp) / 'deployment-credential.json').exists())
            self.assertEqual(list(Path(temp).glob('credential-*')), [])
            mirror.close.assert_called_once()

    def test_cancelled_provision_does_not_download(self):
        from public_access.provisioning import provision
        profile, _ = self.profile()
        factory = Mock()
        with patch('public_access.provisioning.local_host', return_value=True), self.assertRaises(InterruptedError):
            provision(profile, mirror_factory=factory, cancelled=lambda: True)
        factory.assert_not_called()

    def test_provision_failure_waits_and_shutdown_does_not_spawn(self):
        profile, _ = self.profile()
        with tempfile.TemporaryDirectory() as temp, socket.socket() as reserve:
            reserve.bind(('127.0.0.1', 0))
            port = reserve.getsockname()[1]
            reserve.close()
            manager = PublicAccess(replace(SETTINGS, ingress_port=port),
                'http://127.0.0.1:18766', SECRET, data=Path(temp), deployment=profile)
            with patch('public_access.provisioning.provision', side_effect=OSError('offline')), \
                    patch('public_access.supervisor.install') as installer, \
                    patch('public_access.supervisor.subprocess.Popen') as spawn:
                try:
                    manager.start()
                    deadline = time.monotonic() + 3
                    while manager.state != 'provision_retry' and time.monotonic() < deadline:
                        time.sleep(.01)
                    self.assertEqual(manager.state, 'provision_retry')
                    manager.stop()
                    manager.thread.join(3)
                    self.assertFalse(manager.thread.is_alive())
                    self.assertEqual(manager.state, 'stopped')
                    installer.assert_not_called()
                    spawn.assert_not_called()
                finally:
                    manager.stop()
                    manager.thread.join(3)

    def test_publisher_reuses_mirror_and_keeps_secret_out_of_manifest_and_fields(self):
        from bin.test_lighthouse_distribution import FakeFeishuClient
        from openclaw_service.assistant.lighthouse_distribution import FeishuRuntimeMirror
        from tools import publish_public_access_profile as publisher
        client = FakeFeishuClient()
        uploaded = []
        def upload(*args, **kwargs):
            uploaded.append(Path(kwargs['file_path']).read_bytes())
            return {'code': 0, 'data': {'file_token': 'fileFixture'}}
        client.request_file_json = upload
        mirror = FeishuRuntimeMirror(client=client, headers={})
        with tempfile.TemporaryDirectory() as temp, patch('openclaw_service.protocol.protect_state_directory'):
            root = Path(temp)
            config, output = root / 'config.json', root / 'deployment.json'
            atomic_json(config, asdict(SETTINGS))
            def download(part, target):
                target.write_bytes(uploaded[0])
            mirror.download_part = download
            with patch.object(publisher, 'DATA', root / 'private'):
                first = publisher.publish('192.168.224.130', config_path=config, output=output, mirror_factory=lambda: mirror)
                self.assertNotIn(SETTINGS.token, output.read_text(encoding='utf-8'))
                second = publisher.publish('192.168.224.130', config_path=config, output=output, mirror_factory=lambda: mirror)
                self.assertEqual(first, second)
                self.assertEqual(len(uploaded), 1)
                self.assertEqual(len(client.records), 1)
                self.assertNotIn(SETTINGS.token, json.dumps(client.records))
                self.assertIn(SETTINGS.token, uploaded[0].decode())
                self.assertEqual(list((root / 'private').glob('publish-profile-*')), [])
                previous = output.read_bytes()
                mirror.download_part = lambda part, target: target.write_bytes(b'corrupt')
                with self.assertRaises(ValueError):
                    publisher.publish('192.168.224.130', config_path=config, output=output, mirror_factory=lambda: mirror)
                self.assertEqual(output.read_bytes(), previous)

    def test_update_excludes_credential_cache_and_includes_public_manifest(self):
        import package_portable as package
        self.assertIn(Path('bin/public_access/deployment.json'), package.FORCE_PATCH_INCLUDE_FILES)
        for name in ('deployment-credential.json', 'profile-upload-journal.json', 'autostart-disabled.json', 'health.json'):
            self.assertTrue(package._is_runtime_data_path(
                package.PROJECT_ROOT / 'bin/data/public_access' / name, package.PROJECT_ROOT))
        self.assertFalse(package._is_development_only_path(
            package.PROJECT_ROOT / 'bin/tools/check_public_access.py', package.PROJECT_ROOT))

    def test_disable_then_enable_does_not_require_a_plaintext_local_config(self):
        from tools import configure_public_access as configure
        profile, _ = self.profile()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with patch.object(configure, 'DISABLED', root / 'disabled.json'), \
                    patch.object(configure, 'CONFIG', root / 'missing-config.json'), \
                    patch('public_access.provisioning.read_deployment', return_value=(profile, SETTINGS)), \
                    patch.object(sys, 'argv', ['configure', '--disable']):
                configure.main()
                self.assertTrue(configure.DISABLED.is_file())
                with patch.object(sys, 'argv', ['configure', '--enable', '--confirm-server-ready']):
                    configure.main()
                self.assertFalse(configure.DISABLED.exists())
                self.assertFalse(configure.CONFIG.exists())


class PublicDomainCutoverTests(unittest.TestCase):
    def test_retired_domain_closes_business_entry_without_redirect(self):
        server = BIN / 'public_access/server'
        for name in ('dengta-http.conf', 'dengta-https.conf'):
            source = (server / name).read_text(encoding='utf-8')
            self.assertIn('server_name dengta.sjhl.online;', source)
            self.assertIn('location / { return 410; }', source)
            self.assertNotRegex(source, r'return\s+30[1278]\s')
        active = (server / 'dt-https.conf').read_text(encoding='utf-8')
        self.assertIn('server_name dt.sjhl.online;', active)
        self.assertIn('proxy_pass http://127.0.0.1:6100;', active)

    def test_cutover_waits_rolls_back_and_finishes_once(self):
        bash = shutil.which('bash') or ('C:/Program Files/Git/bin/bash.exe' if os.name == 'nt' else '')
        if not bash or not Path(bash).is_file():
            self.skipTest('Bash is required for the isolated server-script check.')
        source = (BIN / 'public_access/server/cutover-to-dt.sh').read_text(encoding='utf-8')
        for scenario in ('offline', 'wrong_service', 'nginx_failure', 'ready'):
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                base, sites = root / 'public', root / 'sites'
                (base / 'deployment').mkdir(parents=True)
                sites.mkdir()
                for domain in ('dt', 'dengta'):
                    (sites / f'{domain}.sjhl.online.conf').write_text(f'old-{domain}')
                    (base / 'deployment' / f'{domain}-https.conf').write_text(f'new-{domain}')
                calls = root / 'systemctl.log'
                failed = root / 'nginx-failed'
                script = source.replace('base=/opt/clipflow-public', 'base=' + shlex.quote(base.as_posix())).replace(
                    'sites=/www/server/panel/vhost/nginx', 'sites=' + shlex.quote(sites.as_posix()))
                fake_commands = '''
PATH=/usr/bin:/bin:$PATH
flock() { return 0; }
openssl() { return 0; }
curl() {
    [[ "$SCENARIO" != offline ]] || return 22
    if [[ "$SCENARIO" == wrong_service ]]; then printf wrong; else printf '%s' '{"ok":true,"service":"clipflow_backend"}'; fi
}
nginx() {
    if [[ "$SCENARIO" == nginx_failure && "$1" == -t && ! -f "$FAILED" ]]; then
        touch "$FAILED"
        return 1
    fi
    return 0
}
systemctl() { printf '%s\\n' "$*" >> "$CALLS"; }
'''
                env = {**os.environ, 'SCENARIO': scenario, 'FAILED': failed.as_posix(), 'CALLS': calls.as_posix()}
                result = subprocess.run([bash, '-c', fake_commands + script], env=env, capture_output=True, text=True, timeout=15)
                self.assertEqual(result.returncode, 1 if scenario == 'nginx_failure' else 0, result.stderr)
                for domain in ('dt', 'dengta'):
                    self.assertEqual((sites / f'{domain}.sjhl.online.conf').read_text(),
                        ('new-' if scenario == 'ready' else 'old-') + domain)
                self.assertEqual((base / 'domain-dt-active').exists(), scenario == 'ready')
                self.assertEqual(calls.exists(), scenario == 'ready')
                if scenario == 'ready':
                    self.assertIn('disable --now clipflow-domain-cutover.timer', calls.read_text())
                    env['SCENARIO'] = 'offline'
                    again = subprocess.run([bash, '-c', fake_commands + script], env=env, capture_output=True, text=True, timeout=15)
                    self.assertEqual(again.returncode, 0, again.stderr)
                    self.assertEqual(len(list((base / 'backups').iterdir())), 1)


class LoopbackFlowTests(unittest.TestCase):
    """Real loopback HTTP, using only an isolated in-memory app and ephemeral ports."""
    def test_stream_parallel_people_and_public_outage_leave_lan_working(self):
        import uvicorn
        import time
        app = make_app()
        release = threading.Event()

        @app.get('/api/assistant/live')
        async def live():
            async def content():
                yield b'data: first\n\n'
                while not release.is_set():
                    await asyncio.sleep(.01)
                yield b'data: last\n\n'
            return StreamingResponse(content(), media_type='text/event-stream')

        guard = PublicGuard(app, SECRET, SETTINGS, lambda r: {'user': {'open_id': r.headers.get('cookie', '')}})
        servers, sockets, threads = [], [], []

        def start_server(application):
            sock = socket.socket()
            sock.bind(('127.0.0.1', 0))
            sockets.append(sock)
            server = uvicorn.Server(uvicorn.Config(application, lifespan='off', log_level='error',
                access_log=False, log_config=None, ws='none', timeout_graceful_shutdown=1))
            servers.append(server)
            async def serve():
                try:
                    await server.serve(sockets=[sock])
                finally:
                    if isinstance(application, StreamingIngress):
                        await application.close()
            thread = threading.Thread(target=lambda: asyncio.run(serve()), daemon=True)
            threads.append(thread)
            thread.start()
            deadline = time.monotonic() + 5
            while not server.started:
                if time.monotonic() > deadline or not thread.is_alive():
                    self.fail('Isolated loopback server did not start.')
                time.sleep(.01)
            return 'http://127.0.0.1:' + str(sock.getsockname()[1])

        proxy = None
        try:
            backend_url = start_server(guard)
            proxy = StreamingIngress(backend_url, SECRET, SETTINGS)
            public_url = start_server(proxy)
            with httpx.Client(base_url=public_url, trust_env=False, timeout=3,
                              headers={'Host': 'portal.example.com'}) as browser:
                with browser.stream('GET', '/api/assistant/live') as response:
                    chunks = response.iter_raw()
                    first = next(chunks)
                    self.assertIn(b'data: first', first)
                    self.assertFalse(release.is_set())
                    release.set()
                    self.assertIn(b'data: last', b''.join(chunks))
                def ask(index):
                    response = browser.get('/api/workbench', headers={'Cookie': 'user=' + str(index)})
                    return index, response.json()['cookie']
                with ThreadPoolExecutor(max_workers=8) as pool:
                    for index, cookie in pool.map(ask, range(20)):
                        self.assertEqual(cookie, 'user=' + str(index))
                self.assertEqual(browser.get('/api/qt/events').status_code, 403)
            servers[-1].should_exit = True
            threads[-1].join(timeout=3)
            self.assertFalse(threads[-1].is_alive())
            with httpx.Client(trust_env=False, timeout=2) as lan:
                self.assertEqual(lan.get(backend_url + '/api/qt/events').status_code, 200)
        finally:
            release.set()
            for server in servers:
                server.should_exit = True
            for thread in threads:
                thread.join(timeout=3)
            for sock in sockets:
                sock.close()


if __name__ == '__main__':
    unittest.main()

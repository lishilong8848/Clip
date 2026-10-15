"""Public tunnel isolation tests: no real server, credentials or business writes."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
from pathlib import Path
import socket
import sys
import tempfile
import threading
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
        for path in ('/api/qt/events', '/api/backend/shutdown', '/api/auth/guest'):
            response = await self.client.post(path, headers={'Origin': SETTINGS.public_url})
            self.assertEqual(response.status_code, 403, path)
        self.assertEqual((await self.client.get('/api/unreviewed-module')).status_code, 403)
        self.assertEqual((await self.client.get('/openapi.json')).status_code, 403)

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
    def test_disabled_does_not_start_or_download(self):
        app = make_app()
        with patch('public_access.config.Settings.read', return_value=Settings()), patch('public_access.supervisor.PublicAccess') as manager:
            self.assertEqual(attach(app, Mock()), (app, None))
            manager.assert_not_called()

    def test_invalid_config_cannot_break_lan_startup(self):
        app = make_app()
        with patch('public_access.config.Settings.read', side_effect=ValueError('invalid')):
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

    def test_occupied_ingress_port_does_not_download_or_spawn(self):
        with tempfile.TemporaryDirectory() as temp, socket.socket() as occupied:
            occupied.bind(('127.0.0.1', 0))
            occupied.listen()
            manager = PublicAccess(replace(SETTINGS, ingress_port=occupied.getsockname()[1]),
                                   'http://127.0.0.1:18766', SECRET, data=Path(temp))
            with patch('public_access.supervisor.install') as installer, \
                    patch('public_access.supervisor.subprocess.Popen') as popen:
                manager._run()
                installer.assert_not_called()
                popen.assert_not_called()
            self.assertEqual(manager.state, 'failed')
            self.assertGreater(occupied.getsockname()[1], 0)

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

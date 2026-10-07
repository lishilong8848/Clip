"""Shared gateway lifecycle, per-agent configuration and private model broker tests."""
import asyncio
import json
import os
import socket
import subprocess
import sys
import tempfile
import ssl
import threading
import time
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import httpx
from fastapi import FastAPI
from lan_bitable_template_portal import lighthouse_runtime as lrt
from lan_bitable_template_portal.lighthouse_ai import AssistantError
from lan_bitable_template_portal.lighthouse_runtime import OpenClawRuntime, account_key, install_model_route
from upload_event_module.services import http_client as tls_policy


class _FakeProcess:
    def __init__(self, pid=1234):
        self.pid, self._poll, self.stdout = pid, None, None
        self.terminate_count = self.kill_count = self.wait_count = 0
    def poll(self): return self._poll
    def terminate(self):
        self.terminate_count += 1
        self._poll = 0
    def kill(self):
        self.kill_count += 1
        self._poll = 0
    def wait(self, timeout=None):
        self.wait_count += 1
        self._poll = 0
        return 0


def _profile():
    return {'model': 'fixture-model', 'name': 'Fixture', 'endpoint': 'https://provider.example/v1/chat/completions',
            'key_cipher': 'cipher-wrapper-secret', 'vision_verified': True, 'context_window': 32000}


def _actor(account='u-default', scopes=('A',)):
    return {'id': account, 'scopes': list(scopes)}


def _model():
    return Mock(unprotect=Mock(return_value='plaintext-model-key-should-never-reach-config'))


class NativeWindowsLoaderTests(unittest.TestCase):
    def test_readonly_windows_metadata_preserves_identity_listeners_scope_and_fallback(self):
        if os.name != 'nt':
            self.skipTest('Windows metadata adapter')
        try:
            node, entry = lrt.runtime_files()
        except AssistantError:
            self.skipTest('Verified Node runtime is not installed')
        import datetime as dt
        import win32api
        import win32process
        handle = win32api.OpenProcess(0x1000, False, os.getpid())
        try:
            created = win32process.GetProcessTimes(handle)['CreationTime'].astimezone(dt.timezone.utc)
        finally:
            handle.Close()
        elapsed = created - dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
        expected = elapsed.days * 86400000 + elapsed.seconds * 1000 + elapsed.microseconds // 1000
        with ExitStack() as stack:
            listener = stack.enter_context(socket.socket())
            listener.bind(('127.0.0.1', 0))
            listener.listen()
            ports = [listener.getsockname()[1]]
            if socket.has_ipv6:
                ipv6 = stack.enter_context(socket.socket(socket.AF_INET6))
                ipv6.bind(('::1', 0))
                ipv6.listen()
                ports.append(ipv6.getsockname()[1])
            root = Path(stack.enter_context(tempfile.TemporaryDirectory()))
            script = root / 'metadata.mjs'
            script.write_text('''import assert from 'node:assert/strict';
import cp from 'node:child_process';
import {pathToFileURL} from 'node:url';
import {resolve} from 'node:path';
const nativeSpawn = cp.spawnSync;
let failNative = false, malformed = false, failNetstat = false;
let helpers = 0, netstats = 0, originals = 0;
const fallback = '2000-01-01T00:00:00.123+00:00';
const powershell = file => String(file).toLowerCase().endsWith('powershell.exe');
cp.execFileSync = function(file, args, options) {
  if (powershell(file)) { originals++; assert.ok(options.timeout > 0 && options.timeout <= 1000); return fallback; }
  throw new Error('Unrelated exec was unexpectedly intercepted');
};
cp.spawnSync = function(file, args, options) {
  if (file === process.env.LIGHTHOUSE_PYTHON) {
    helpers++;
    assert.deepEqual(args.slice(0, 4), ['-I', '-S', '-B', '-c']);
    assert.match(args.at(-1), /^[1-9]\\d*$/);
    assert.equal(options.shell, false);
    assert.ok(options.timeout > 0 && options.timeout <= 1000);
    if (failNative) return {status:1, stdout:''};
    if (malformed) return {status:0, stdout:'unverified timestamp'};
  }
  if (String(file).toLowerCase().endsWith('netstat.exe')) {
    netstats++;
    assert.deepEqual(args, ['-ano']);
    if (failNetstat) return {status:1, stdout:''};
  }
  if (powershell(file)) {
    originals++;
    assert.ok(options.timeout > 0 && options.timeout <= 1000);
    const stdout = args.at(-1).includes('Get-NetTCPConnection') ? process.env.OWNER_PID : fallback;
    return {status:0, stdout};
  }
  return nativeSpawn.call(this, file, args, options);
};
await import(process.env.PRELOAD);
const sdk = file => pathToFileURL(resolve(process.env.LIGHTHOUSE_SDK_ROOT, 'dist', file)).href;
const {t: start} = await import(sdk('pid-alive-BcyyC-CC.js'));
const {a: otherStart, n: owners} = await import(sdk('windows-port-pids-CgzBZ5yT.js'));
const pid = Number(process.env.OWNER_PID), expected = Number(process.env.EXPECTED_MS);
assert.equal(start(pid), expected, 'PID identity lost milliseconds');
assert.equal(otherStart(pid,1000), expected, 'Gateway/file-lock identities disagree');
assert.equal(start(0), null);
for (const port of JSON.parse(process.env.LISTEN_PORTS)) {
  const result = owners(port,1000);
  assert.equal(result.ok, true);
  assert.ok(result.pids.includes(pid), 'Live IPv4/IPv6 listener owner missing');
}
assert.equal(originals, 0, 'Native metadata fell back unexpectedly');
assert.equal(netstats, JSON.parse(process.env.LISTEN_PORTS).length);
const previousHelpers = helpers, previousNetstats = netstats;
cp.execFileSync('powershell.exe', ['-NoProfile','-NonInteractive','-Command', `(Get-Process -Id ${pid}).StartTime.ToString('o')`], {encoding:'utf8', timeout:1000});
cp.spawnSync('powershell.exe', ['-NoProfile','-Command','Write-Output fixture'], {encoding:'utf8', timeout:1000});
assert.equal(helpers, previousHelpers, 'Unowned command gained native access');
assert.equal(netstats, previousNetstats);
assert.equal(originals, 2);
failNative = true;
assert.equal(start(pid), Date.parse(fallback));
assert.equal(otherStart(pid,1000), Date.parse(fallback));
failNative = false; malformed = true;
assert.equal(start(pid), Date.parse(fallback), 'Malformed helper output bypassed original query');
failNetstat = true;
assert.deepEqual(owners(4321,1000), {ok:true,pids:[pid]}, 'Failed netstat became a false empty list');
assert.equal(originals, 6);
console.log('Readonly identity/listener scope and fallback OK');
''', encoding='utf-8')
            result = subprocess.run([str(node), str(script)], env={**os.environ,
                'LIGHTHOUSE_SDK_ROOT': str(entry.parent), 'LIGHTHOUSE_PYTHON': sys.executable,
                'OWNER_PID': str(os.getpid()), 'EXPECTED_MS': str(expected), 'LISTEN_PORTS': json.dumps(ports),
                'PRELOAD': (Path(lrt.__file__).parent / 'openclaw/native-paths.mjs').resolve().as_uri()},
                capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('Readonly identity/listener scope and fallback OK', result.stdout)

    def test_sdk_esm_fast_path_preserves_other_resolution_and_worker_scope(self):
        if os.name != 'nt':
            self.skipTest('Windows compatibility adapter')
        try:
            node, _ = lrt.runtime_files()
        except AssistantError:
            self.skipTest('Verified Node runtime is not installed')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sdk = root / 'SDK space'
            inner = sdk / 'dist/inner'
            inner.mkdir(parents=True)
            (sdk / 'dist/package.json').write_text('{"type":"module"}', encoding='utf-8')
            (sdk / 'dist/inside.js').write_text('export const value = 17;', encoding='utf-8')
            (inner / 'importer.js').write_text('export {value} from "../inside.js";', encoding='utf-8')
            (inner / 'missing.js').write_text('import "./absent.js";', encoding='utf-8')
            (sdk / 'inside.cjs').write_text('module.exports = 23;', encoding='utf-8')
            (inner / 'worker.js').write_text('''import {parentPort} from 'node:worker_threads';
import {createRequire} from 'node:module';
import {pathToFileURL} from 'node:url';
import {value} from '../inside.js';
const require = createRequire(import.meta.url);
parentPort.postMessage([value, require(pathToFileURL(process.env.LIGHTHOUSE_SDK_ROOT + '/inside.cjs').href)]);
''', encoding='utf-8')
            entry = sdk / 'entry.mjs'
            entry.write_text('''import Module from 'node:module';
import {pathToFileURL} from 'node:url';
import {resolve} from 'node:path';
import {Worker} from 'node:worker_threads';
import assert from 'node:assert/strict';
const root = process.env.LIGHTHOUSE_SDK_ROOT, hooks = [];
const register = Module.registerHooks;
Module.registerHooks = options => { hooks.push(options); return register(options); };
await import(process.env.FIXTURE_PRELOAD_URL);
assert.equal(hooks.length, 1, 'SDK ESM fast path was not registered');
const hook = hooks[0].resolve;
const context = {parentURL: pathToFileURL(resolve(root, 'dist/inner/importer.js')).href, conditions: ['node', 'import']};
const inside = pathToFileURL(resolve(root, 'dist/inside.js')).href;
let fallbacks = 0;
const fallback = () => { fallbacks++; return {url:'file:///fallback.js', format:'commonjs'}; };
assert.deepEqual(hook('../inside.js', context, fallback), {url:inside, format:'module', shortCircuit:true});
const cases = [
  ['./state.json', context], ['./inside.js?x=1', context], ['openclaw/plugin-sdk', context], [inside, context],
  ['../../outside.js', context], ['./inside.js', {...context, conditions:['node','require']}],
  ['./inside.js', {...context, parentURL:pathToFileURL(resolve(root, 'workspace/importer.js')).href}],
  ['./inside.js', {...context, parentURL:pathToFileURL(resolve(root, 'dist-other/importer.js')).href}],
  ['./inside.js', {conditions:['import']}]
];
for (const [specifier, ctx] of cases) assert.deepEqual(hook(specifier, ctx, fallback), {url:'file:///fallback.js', format:'commonjs'});
assert.equal(fallbacks, cases.length);
assert.equal((await import('./dist/inner/importer.js')).value, 17);
await assert.rejects(import('./dist/inner/missing.js'));
const worker = new Worker(new URL('./dist/inner/worker.js', import.meta.url), {execArgv:['--import', process.env.FIXTURE_PRELOAD_URL]});
const result = await new Promise((done, reject) => { worker.once('message', done); worker.once('error', reject); });
assert.deepEqual(result, [17,23]);
await worker.terminate();
console.log('Scoped ESM and inherited worker loader OK');
''', encoding='utf-8')
            preload = (Path(lrt.__file__).parent / 'openclaw/native-paths.mjs').resolve().as_uri()
            result = subprocess.run([str(node), str(entry)], env={**os.environ,
                'LIGHTHOUSE_SDK_ROOT': str(sdk), 'FIXTURE_PRELOAD_URL': preload},
                capture_output=True, text=True, timeout=20)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('Scoped ESM and inherited worker loader OK', result.stdout)

    def test_file_url_require_works_only_inside_owned_runtime_and_plugin(self):
        if os.name != 'nt':
            self.skipTest('Windows compatibility adapter')
        try:
            node, _ = lrt.runtime_files()
        except AssistantError:
            self.skipTest('Verified Node runtime is not installed')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sdk, plugin = root / 'sdk', root / 'plugin'
            sdk.mkdir()
            plugin.mkdir()
            (sdk / 'inside.cjs').write_text('module.exports = 17;', encoding='utf-8')
            (plugin / 'inside.cjs').write_text('module.exports = 23;', encoding='utf-8')
            (root / 'outside.cjs').write_text('module.exports = 99;', encoding='utf-8')
            entry = sdk / 'entry.mjs'
            entry.write_text("""import {createRequire} from 'node:module';
import {pathToFileURL, fileURLToPath} from 'node:url';
import {resolve, dirname} from 'node:path';
import assert from 'node:assert/strict';
import fs from 'node:fs';
const require = createRequire(import.meta.url);
const root = dirname(fileURLToPath(import.meta.url));
assert.equal(require(pathToFileURL(resolve(root, 'inside.cjs')).href), 17);
assert.equal(require(pathToFileURL(resolve(root, '../plugin/inside.cjs')).href), 23);
assert.throws(() => require(pathToFileURL(resolve(root, '../outside.cjs')).href));
const session = resolve(root, '../session.json');
fs.writeFileSync(session, 'first');
assert.equal(fs.readFileSync(session, 'utf-8'), 'first');
fs.writeFileSync(session, 'second');
assert.equal(fs.readFileSync(session, 'utf-8'), 'second');
console.log('Scoped native URL resolution OK');
""", encoding='utf-8')
            result = subprocess.run([str(node), '--import', (Path(lrt.__file__).parent / 'openclaw/native-paths.mjs').resolve().as_uri(), str(entry)],
                env={**os.environ, 'LIGHTHOUSE_PLUGIN_DIR': str(plugin)}, capture_output=True, text=True, timeout=20)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('Scoped native URL resolution OK', result.stdout)


class ModelProviderErrorTests(unittest.TestCase):
    def test_provider_errors_return_only_fixed_codes_and_messages(self):
        for status, code in ((401, 'invalid_api_key'), (403, 'invalid_api_key'), (429, 'rate_limit_exceeded'),
                             (404, 'model_not_found'), (504, 'model_timeout'), (502, 'model_service_unavailable'),
                             (400, 'model_request_rejected')):
            with self.subTest(status=status):
                result = lrt.model_provider_error(status, b'{"error":{"message":"private-provider-detail"}}')
                self.assertEqual(result['code'], code)
                self.assertNotIn('private-provider-detail', json.dumps(result))
        for body in (b'invalid-json', b'[]', b'{"error":null}', b'{"error":"invalid"}'):
            self.assertEqual(lrt.model_provider_error(400, body)['code'], 'model_request_rejected')


class ModelTransportPreparationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.runtime = OpenClawRuntime(self.directory.name)

    async def asyncTearDown(self):
        await self.runtime.close()
        self.directory.cleanup()

    async def test_reused_model_transport_never_persists_provider_account_cookies(self):
        real_client, cookies = httpx.AsyncClient, []
        def provider(request):
            cookies.append(request.headers.get('cookie', ''))
            return httpx.Response(200, json={'ok': True}, headers={
                'Set-Cookie': 'provider_session=synthetic-account-a; Path=/; Secure'})
        with patch.object(httpx, 'AsyncClient', side_effect=lambda **kwargs:
                real_client(**kwargs, transport=httpx.MockTransport(provider))):
            client = await self.runtime.model_client()
            for account in ('a', 'b'):
                await client.post('https://provider.invalid/v1/chat/completions',
                    headers={'Authorization': 'Bearer synthetic-' + account}, json={'messages': []})
            self.assertEqual(cookies, ['', ''])
            self.assertEqual(list(client.cookies.items()), [])
            self.assertIs(await self.runtime.model_client(), client)

    async def test_verified_transport_is_offloop_reused_and_makes_no_prewarm_requests(self):
        from openclaw_service.assistant import lighthouse_public as public
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        real_client = httpx.AsyncClient
        instances, threads, options = [], [], []
        loop_thread = threading.get_ident()
        loop = asyncio.get_running_loop()
        entered, release = asyncio.Event(), threading.Event()
        def create_context(**kwargs):
            threads.append(threading.get_ident())
            self.assertEqual(set(kwargs), {'cadata'})
            loop.call_soon_threadsafe(entered.set)
            if not release.wait(5):
                raise TimeoutError('Certificate preparation blocked the event loop')
            return context
        def create_client(**kwargs):
            threads.append(threading.get_ident())
            options.append(kwargs)
            client = real_client(**kwargs, transport=httpx.MockTransport(
                lambda _: self.fail('Preparation must not contact a provider')))
            instances.append(client)
            return client
        ticks = []
        async def tick():
            await asyncio.wait_for(entered.wait(), 2)
            ticks.append(not instances)
            release.set()
        with patch.dict(os.environ, {}, clear=True), patch.object(tls_policy, '_tls_contexts', {}), patch.object(ssl, 'create_default_context', create_context), \
                patch.object(httpx, 'AsyncClient', create_client):
            try:
                clients = await asyncio.wait_for(asyncio.gather(*(self.runtime.model_client() for _ in range(20)), tick()), 10)
            finally:
                release.set()
            self.assertEqual(len(instances), 1)
            self.assertTrue(all(client is instances[0] for client in clients[:-1]))
            self.assertEqual(ticks, [True])
            self.assertNotIn(loop_thread, threads)
            self.assertIs(options[0]['verify'], context)
            self.assertIs(options[0]['trust_env'], False)
            self.assertIs(options[0]['follow_redirects'], False)
            self.assertEqual(options[0]['limits'].max_connections, 40)
            self.assertEqual(options[0]['limits'].max_keepalive_connections, 20)
            self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
            self.assertTrue(context.check_hostname)
            self.assertIs(await self.runtime.model_client(), instances[0])
        await self.runtime.close()
        self.assertTrue(instances[0].is_closed)

    async def test_shutdown_during_certificate_preparation_cannot_create_a_late_client(self):
        from openclaw_service.assistant import lighthouse_public as public
        entered, release = threading.Event(), threading.Event()
        def certificate(**_):
            entered.set()
            release.wait(2)
            return ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        with patch.object(public, '_verified_tls_context', certificate), patch.object(httpx, 'AsyncClient') as create:
            task = asyncio.create_task(self.runtime.model_client())
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait, 1))
                await asyncio.wait_for(self.runtime.close(), .3)
                release.set()
                with self.assertRaises(AssistantError): await task
                create.assert_not_called()
                self.assertIsNone(self.runtime.http)
            finally:
                release.set()
                await asyncio.gather(task, return_exceptions=True)

    async def test_cancelled_client_construction_closes_the_unpublished_client(self):
        from openclaw_service.assistant import lighthouse_public as public
        entered, release = threading.Event(), threading.Event()
        client = Mock(aclose=AsyncMock())
        def create_client(**_):
            entered.set()
            release.wait(2)
            return client
        with patch.object(public, '_verified_tls_context', return_value=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)), \
                patch.object(httpx, 'AsyncClient', create_client):
            task = asyncio.create_task(self.runtime.model_client())
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait, 1))
                task.cancel()
                release.set()
                with self.assertRaises(asyncio.CancelledError): await task
                client.aclose.assert_awaited_once()
                self.assertIsNone(self.runtime.http)
            finally:
                release.set()
                await asyncio.gather(task, return_exceptions=True)


class SharedRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.runtime = OpenClawRuntime(Path(self.directory.name))
        self.process = _FakeProcess()
        self.mocks = ExitStack()
        self.mocks.enter_context(patch.object(self.runtime, '_ready', AsyncMock(side_effect=lambda item, *_: item)))
        self.configured = self.mocks.enter_context(patch.object(self.runtime, '_configured', AsyncMock()))
        self.mocks.enter_context(patch.object(lrt, 'runtime_files', return_value=(Path('node.exe'), Path('openclaw.mjs'))))
        self.popen = self.mocks.enter_context(patch.object(lrt.subprocess, 'Popen', return_value=self.process))
        self.mocks.enter_context(patch('upload_event_module.services.process_lifetime.register_child_process', return_value=True))
        self.model = _model()

    async def asyncTearDown(self):
        await self.runtime.close()
        self.mocks.close()
        self.directory.cleanup()

    async def acquire(self, identity='u-default', profile=None, scopes=('A',)):
        return await self.runtime.acquire(_actor(identity, scopes), self.model, profile or _profile())

    async def test_twenty_accounts_one_process_port_and_twenty_private_agents(self):
        items = await asyncio.gather(*(self.acquire('owner-' + str(i), {**_profile(), 'model': 'model-' + str(i)}) for i in range(20)))
        self.popen.assert_called_once()
        self.assertEqual(self.runtime.maximum, 20)
        self.assertIn('--v8-pool-size=2', self.popen.call_args.args[0])
        for field in ('port', 'token', 'process'):
            self.assertEqual(len({item[field] for item in items}), 1)
        for field in ('key', 'agent_id', 'root', 'fingerprint'):
            self.assertEqual(len({item[field] for item in items}), 20)
        with self.assertRaises(AssistantError):
            await self.acquire('twenty-one')
        items[0]['busy'] = False
        await self.acquire('twenty-one')
        with self.assertRaises(AssistantError):
            await self.acquire('owner-0', {**_profile(), 'model': 'model-0'})
        self.popen.assert_called_once()

    async def test_busy_same_account_rejected(self):
        await self.acquire()
        with self.assertRaises(AssistantError) as error:
            await self.acquire()
        self.assertEqual(error.exception.status, 409)

    async def test_warm_idle_and_same_profile_reuse_without_configuration_rpc(self):
        first = await self.acquire()
        first['busy'] = False
        self.assertIs(first, await self.acquire())
        self.configured.assert_not_awaited()

    async def test_configuration_write_coalesced_and_transient_windows_lock_retried(self):
        await self.acquire()
        write = lrt.atomic_json
        with patch.object(lrt, 'atomic_json', wraps=write) as writer:
            await self.runtime._write_configuration(())
            writer.assert_not_called()
            self.runtime.gateway.pop('config_digest')
            calls = 0
            def locked_once(*args):
                nonlocal calls
                calls += 1
                if calls == 1 and os.name == 'nt':
                    raise PermissionError('fixture watcher lock')
                return write(*args)
            writer.side_effect = locked_once
            await self.runtime._write_configuration(())
            self.assertEqual(calls, 2 if os.name == 'nt' else 1)
            await self.runtime._write_configuration(())
            self.assertEqual(calls, 2 if os.name == 'nt' else 1)

    async def test_model_and_scopes_change_only_this_agent_without_gateway_restart(self):
        first, second = await self.acquire('first'), await self.acquire('second')
        first['busy'] = False
        changed = await self.acquire('first', {**_profile(), 'model': 'another-model'}, ('A', 'B'))
        self.assertIs(first['process'], changed['process'])
        self.assertEqual(first['agent_id'], changed['agent_id'])
        self.assertNotEqual(first['fingerprint'], changed['fingerprint'])
        self.assertIs(self.runtime.accounts[second['key']], second)
        self.assertTrue(second['busy'])
        self.assertEqual(self.process.terminate_count, 0)
        self.popen.assert_called_once()

    async def test_config_and_node_environment_never_contain_real_model_keys(self):
        item = await self.acquire()
        config = json.loads((self.runtime.root / 'shared-gateway/openclaw.json').read_text(encoding='utf-8'))
        serialized = json.dumps(config)
        self.assertNotIn('plaintext-model-key', serialized)
        self.assertNotIn(_profile()['key_cipher'], serialized)
        self.assertNotIn('provider.example', serialized)
        self.assertNotIn(item['model_key'], json.dumps(self.popen.call_args.kwargs['env']))
        self.assertEqual(config['agents']['defaults']['maxConcurrent'], 20)
        self.assertEqual(config['skills']['allowBundled'], ['lighthouse-tools'])
        self.assertEqual(self.popen.call_args.kwargs['env']['GIT_CEILING_DIRECTORIES'], str(self.runtime.root))
        self.assertEqual(self.popen.call_args.kwargs['env']['OPENCLAW_PACKAGED_COMPILE_CACHE_RESPAWNED'], '1')
        self.assertEqual(self.popen.call_args.kwargs['env']['OPENCLAW_DISABLE_BUNDLED_PLUGINS'], '1')
        self.assertEqual(self.popen.call_args.kwargs['env']['LIGHTHOUSE_SDK_ROOT'], str(Path('openclaw.mjs').parent))
        self.assertEqual(config['plugins']['allow'], ['openai', 'lighthouse-tools'])
        self.assertEqual(config['plugins']['load']['paths'], [str(self.runtime.root / 'shared-gateway/plugin'),
            str(Path('openclaw.mjs').parent / 'dist/extensions/openai')])
        self.assertEqual(self.popen.call_args.kwargs['env']['NODE_COMPILE_CACHE'], str(self.runtime.root / 'shared-gateway/node-compile-cache'))
        self.assertTrue(Path(self.popen.call_args.kwargs['env']['OPENCLAW_BUNDLED_SKILLS_DIR']).is_dir())
        self.assertEqual(sum(bool(entry.get('default')) for entry in config['agents']['entries'].values()), 1)
        self.assertFalse(config['tools']['agentToAgent']['enabled'])
        self.assertIn('sessions_history', config['tools']['deny'])
        self.assertEqual(config['tools']['sessions']['visibility'], 'self')
        self.assertEqual(config['meta']['lastTouchedVersion'], lrt.PIN['openclaw_version'])
        entry = config['agents']['entries'][item['agent_id']]
        self.assertEqual(entry['workspace'], str(item['root'] / 'workspace'))
        self.assertEqual(entry['agentDir'], str(item['root'] / 'agent'))
        state = Path(self.popen.call_args.kwargs['env']['OPENCLAW_STATE_DIR'])
        self.assertEqual(state, self.runtime.root)
        self.assertTrue(Path(entry['agentDir']).is_relative_to(state))
        self.assertEqual(config['session']['store'], str(self.runtime.root / 'shared-gateway/agents/{agentId}/sessions/sessions.json'))

    async def test_stop_one_account_never_terminates_gateway_or_other_account(self):
        first, second = await self.acquire('first'), await self.acquire('second')
        await self.runtime._stop(first)
        self.assertTrue(first['blocked'])
        self.assertFalse(first['busy'])
        self.assertTrue(second['busy'])
        self.assertEqual(self.process.terminate_count, 0)
        with self.assertRaises(AssistantError):
            await self.acquire('first')
        second['busy'] = False
        self.assertIs(second, await self.acquire('second'))

    async def test_unconfirmed_configuration_cannot_take_the_fast_path(self):
        first = await self.acquire()
        first['busy'] = False
        self.configured.side_effect = AssistantError('reload incomplete', 503)
        changed = {**_profile(), 'context_window': 64000}
        with self.assertRaises(AssistantError):
            await self.acquire(profile=changed)
        self.configured.side_effect = None
        result = await self.acquire(profile=changed)
        self.assertTrue(result['configured'])
        self.assertEqual(self.configured.await_count, 2)

    async def test_cold_start_shared_and_shutdown_cancels_all_waiters(self):
        entered = asyncio.Event()
        async def wait(item, *_):
            entered.set()
            await asyncio.Event().wait()
        with patch.object(self.runtime, '_ready', side_effect=wait):
            first = asyncio.create_task(self.acquire('first'))
            await entered.wait()
            second = asyncio.create_task(self.acquire('second'))
            await asyncio.sleep(.05)
            self.popen.assert_called_once()
            await self.runtime.close()
            results = await asyncio.gather(first, second, return_exceptions=True)
            self.assertTrue(all(isinstance(result, asyncio.CancelledError) for result in results))
            self.assertEqual(self.runtime.accounts, {})
            self.assertEqual(self.process.terminate_count, 1)

    async def test_failed_process_start_has_no_registered_accounts(self):
        self.popen.side_effect = OSError('fixture')
        with self.assertRaises(AssistantError):
            await self.acquire()
        self.assertIsNone(self.runtime.gateway)
        self.assertEqual(self.runtime.accounts, {})

    async def test_cancelling_one_cold_login_keeps_other_login_and_shared_startup(self):
        entered, release = asyncio.Event(), asyncio.Event()
        async def ready(item, *_):
            entered.set()
            await release.wait()
            return item
        with patch.object(self.runtime, '_ready', side_effect=ready):
            first = asyncio.create_task(self.acquire('first'))
            second = asyncio.create_task(self.acquire('second'))
            await entered.wait()
            first.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await first
            self.assertFalse(self.runtime.accounts[account_key('first')]['busy'])
            self.assertEqual(self.process.terminate_count, 0)
            release.set()
            item = await second
            self.assertTrue(item['configured'])
            self.popen.assert_called_once()

    async def test_attach_failure_terminates_child_and_closes_log(self):
        log = MagicMock()
        log.attach.side_effect = RuntimeError('fixture')
        with patch.object(lrt, 'GatewayLog', return_value=log):
            with self.assertRaises(RuntimeError):
                await self.acquire()
        self.assertEqual(self.process.terminate_count, 1)
        log.close.assert_called_once()
        self.assertEqual(self.runtime.accounts, {})

    async def test_lifecycle_binding_failure_terminates_child(self):
        with patch('upload_event_module.services.process_lifetime.register_child_process', return_value=False):
            with self.assertRaises(AssistantError):
                await self.acquire()
        self.assertEqual(self.process.terminate_count, 1)

    async def test_default_runtime_missing_uses_safe_install_fallback(self):
        from lan_bitable_template_portal import lighthouse_distribution as distribution
        with patch.object(lrt, 'runtime_files', side_effect=[AssistantError('missing', 503), (Path('node'), Path('entry'))]), \
                patch.object(distribution, 'install_runtime', Mock()) as install:
            await self.acquire()
            install.assert_called_once()

    async def test_runtime_install_failure_is_shared_and_retries_after_cooldown(self):
        from lan_bitable_template_portal import lighthouse_distribution as distribution
        with patch.object(lrt, 'runtime_files', side_effect=AssistantError('missing', 503)), \
                patch.object(distribution, 'install_runtime', side_effect=AssistantError('fixture auth failure', 503)) as install:
            failures = await asyncio.gather(*(self.runtime.prepare() for _ in range(8)), return_exceptions=True)
            self.assertTrue(all(isinstance(error, AssistantError) for error in failures))
            install.assert_called_once()
            self.runtime.prepare_retry_at = 0
            with self.assertRaises(AssistantError):
                await self.runtime.prepare()
            self.assertEqual(install.call_count, 2)

    async def test_shutdown_idempotent_terminates_shared_process_once(self):
        await self.acquire('first')
        await self.acquire('second')
        await self.runtime.close()
        await self.runtime.close()
        self.assertEqual(self.process.terminate_count, 1)
        self.assertEqual(self.runtime.accounts, {})
        self.assertEqual(self.runtime.locks, {})

    async def test_legacy_account_files_are_retained(self):
        root = self.runtime.root / account_key('legacy') / 'agents/main/sessions'
        root.mkdir(parents=True)
        (root / 'history.sqlite').write_bytes(b'original private history')
        await self.acquire('legacy')
        self.assertEqual((root / 'history.sqlite').read_bytes(), b'original private history')

    async def test_model_proxy_routes_only_owner_key_model_and_endpoint(self):
        first = await self.acquire('first')
        self.model.unprotect.return_value = 'second-private-key'
        second = await self.acquire('second', {**_profile(), 'endpoint': 'https://second.example/v1/chat/completions', 'model': 'second-model'})
        requests = []
        async def upstream(request):
            requests.append(request)
            return httpx.Response(200, json={'answer': 'fixture'})
        self.runtime.http = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
        app = FastAPI()
        install_model_route(app, self.runtime)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            def url(item): return '/api/assistant/openclaw-models/chat/completions'
            headers = {'Authorization': 'Bearer ' + first['token']}
            for item in (first, second):
                response = await client.post(url(item), headers=headers, json={'model': item['agent_id'], 'messages': []})
                self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(requests[0].headers['authorization'], 'Bearer ' + first['model_key'])
            self.assertEqual(requests[1].headers['authorization'], 'Bearer second-private-key')
            self.assertEqual(str(requests[1].url), second['profile']['endpoint'])
            for bad in ({}, {**headers, 'Origin': 'http://browser'}, {'Authorization': 'Bearer wrong'}, {'Authorization': first['token']}):
                self.assertEqual((await client.post(url(first), headers=bad, json={'model': first['agent_id']})).status_code, 403)
            self.assertEqual((await client.post(url(first), headers=headers, json={'model': 'second-model'})).status_code, 403)
            first['busy'] = False
            self.assertEqual((await client.post(url(first), headers=headers, json={'model': first['agent_id']})).status_code, 403)
            self.assertEqual(len(requests), 2)
            self.assertEqual(json.loads(requests[0].content)['model'], 'fixture-model')
            self.assertEqual(json.loads(requests[1].content)['model'], 'second-model')
            self.assertTrue(all(request.headers.get('cookie') == '' for request in requests))

    async def test_request_stopped_or_replaced_during_preparation_never_reaches_provider(self):
        item = await self.acquire()
        client = Mock(send=AsyncMock(), build_request=Mock())
        app = FastAPI()
        install_model_route(app, self.runtime)
        for change in ('stop', 'replace', 'shutdown'):
            with self.subTest(change=change):
                self.runtime.closing = False
                self.runtime.accounts[item['key']] = item
                item['blocked'], item['busy'] = False, True
                async def prepare():
                    if change == 'stop': item['blocked'] = True
                    elif change == 'replace': self.runtime.accounts[item['key']] = {**item}
                    else: self.runtime.closing = True
                    return client
                with patch.object(self.runtime, 'model_client', side_effect=prepare):
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as caller:
                        response = await caller.post('/api/assistant/openclaw-models/chat/completions',
                            headers={'Authorization': 'Bearer ' + item['token']}, json={'model': item['agent_id']})
                self.assertEqual(response.status_code, 403)
                client.send.assert_not_called()
                client.build_request.assert_not_called()


    async def test_model_proxy_preserves_safe_context_error_without_provider_details(self):
        item = await self.acquire('first')
        async def upstream(request):
            return httpx.Response(400, json={'error': {'code': 'context_length_exceeded',
                'message': item['model_key'] + ' private-record-id exceeded context'}})
        self.runtime.http = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
        app = FastAPI()
        install_model_route(app, self.runtime)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            response = await client.post('/api/assistant/openclaw-models/chat/completions',
                headers={'Authorization': 'Bearer ' + item['token']}, json={'model': item['agent_id']})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()['error'], {'code': 'context_length_exceeded', 'message': 'Context length exceeded'})
        self.assertNotIn(item['model_key'], response.text)
        self.assertNotIn('private-record-id', response.text)

    async def test_model_proxy_recognizes_context_message_but_not_arbitrary_provider_errors(self):
        item = await self.acquire()
        body = {'error': {'message': "This model's maximum context length is 8192 tokens. Your messages resulted in 9000 tokens."}}
        async def upstream(request): return httpx.Response(400, json=body)
        self.runtime.http = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
        app = FastAPI()
        install_model_route(app, self.runtime)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            async def invoke():
                return await client.post('/api/assistant/openclaw-models/chat/completions',
                    headers={'Authorization': 'Bearer ' + item['token']}, json={'model': item['agent_id']})
            self.assertEqual((await invoke()).json()['error']['code'], 'context_length_exceeded')
            body['error'] = {'message': 'Invalid request containing ' + item['model_key']}
            response = await invoke()
            self.assertEqual(response.json()['error']['code'], 'model_request_rejected')
            self.assertNotIn(item['model_key'], response.text)

    async def test_model_proxy_marks_compaction_only_for_request_owner_current_run(self):
        first, second = await self.acquire('first'), await self.acquire('second')
        first['run_id'], second['run_id'] = 'first-run', 'second-run'
        async def upstream(request): return httpx.Response(200, json={'ok': True})
        self.runtime.http = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
        app = FastAPI()
        install_model_route(app, self.runtime)
        summary = 'You are a context summarization assistant. Your task is to read a conversation.'
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            for role in ('user', 'system'):
                await client.post('/api/assistant/openclaw-models/chat/completions',
                    headers={'Authorization': 'Bearer ' + first['token']},
                    json={'model': first['agent_id'], 'messages': [{'role': role, 'content': summary}]})
                if role == 'user': self.assertNotIn('compaction_run_id', first)
        self.assertEqual(first['compaction_run_id'], 'first-run')
        self.assertNotIn('compaction_run_id', second)

    async def test_model_proxy_bounds_error_body_and_closes_upstream(self):
        item = await self.acquire()
        class OversizedError(httpx.AsyncByteStream):
            reads, closed = 0, False
            async def __aiter__(self):
                self.reads += 1
                yield b'x' * 17000
                self.reads += 1
                raise AssertionError('Error body exceeded limit')
            async def aclose(self): self.closed = True
        stream = OversizedError()
        async def upstream(request): return httpx.Response(400, stream=stream)
        self.runtime.http = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
        app = FastAPI()
        install_model_route(app, self.runtime)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            response = await client.post('/api/assistant/openclaw-models/chat/completions',
                headers={'Authorization': 'Bearer ' + item['token']}, json={'model': item['agent_id']})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()['error']['code'], 'model_request_rejected')
        self.assertEqual(stream.reads, 1)
        self.assertTrue(stream.closed)

    async def test_model_proxy_timeout_keeps_timeout_semantics(self):
        item = await self.acquire()
        async def upstream(request): raise httpx.ReadTimeout('private-provider-detail')
        self.runtime.http = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
        app = FastAPI()
        install_model_route(app, self.runtime)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            response = await client.post('/api/assistant/openclaw-models/chat/completions',
                headers={'Authorization': 'Bearer ' + item['token']}, json={'model': item['agent_id']})
        self.assertEqual(response.status_code, 504)
        self.assertEqual(response.json()['error']['code'], 'model_timeout')
        self.assertNotIn('private-provider-detail', response.text)


if __name__ == '__main__':
    unittest.main()

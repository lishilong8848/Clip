"""Isolated resident-host authentication and lifecycle tests; no cloud or Node."""
import asyncio
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import httpx
from openclaw_service.protocol import PROJECT, InstanceLock, ServiceError, atomic_json, control_key, descriptor, identity, migrate_accounts, process_stamp, protect_state_directory, prepare_tool_plugin
from openclaw_service.server import Host, build_app
from lan_bitable_template_portal.lighthouse_runtime import account_key


class FakeManager:
    def __init__(self):
        self.accounts = {}
        self.closed = False
        self.prepare = AsyncMock()
        self.model_client = AsyncMock()
        self.spawns = 0

    async def acquire(self, actor, model, profile, **kwargs):
        key = account_key(actor['id'])
        fingerprint = hashlib.sha256(json.dumps([actor, profile, kwargs['bridge_token'], kwargs['bridge_url']], sort_keys=True).encode()).hexdigest()
        item = self.accounts.get(key)
        if not item or item['fingerprint'] != fingerprint:
            self.spawns += 1
            item = {'key': key, 'process': SimpleNamespace(pid=os.getpid(), poll=lambda: None), 'port': 7301,
                'token': 'synthetic-gateway-token', 'fingerprint': fingerprint, 'protocol': 4, 'used_at': 0}
            self.accounts[key] = item
        item['busy'] = True
        return item

    async def _stop(self, item):
        self.accounts.pop(item['key'], None)

    async def close(self):
        self.accounts.clear()
        self.closed = True


class ProtocolTests(unittest.TestCase):
    def test_plugin_only_writes_changed_content(self):
        with tempfile.TemporaryDirectory() as directory:
            source, target = Path(directory) / 'source', Path(directory) / 'target'
            source.mkdir()
            (source / 'openclaw.plugin.json').write_text('{"id":"lighthouse-tools"}', encoding='utf-8')
            (source / 'index.mjs').write_text('export default {}', encoding='utf-8')
            tools = [{'name': 'lighthouse_query', 'parameters': {'type': 'object'}}]
            prepare_tool_plugin(source, target, tools)
            with patch.object(Path, 'write_bytes', side_effect=AssertionError('unchanged files must not be rewritten')):
                prepare_tool_plugin(source, target, copy.deepcopy(tools))
            before = (target / 'index.mjs').stat().st_mtime_ns
            prepare_tool_plugin(source, target, [*tools, {'name': 'lighthouse_new', 'parameters': {}}])
            self.assertEqual((target / 'index.mjs').stat().st_mtime_ns, before)
            manifest = json.loads((target / 'openclaw.plugin.json').read_text(encoding='utf-8'))
            self.assertEqual(manifest['contracts']['tools'], ['lighthouse_query', 'lighthouse_new'])

    def test_state_acl_skips_unchanged_files_but_repairs_widened_access(self):
        import win32security
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'state'
            root.mkdir()
            child = root / 'existing.txt'
            child.write_bytes(b'private')
            protect_state_directory(root)
            with patch('win32security.SetNamedSecurityInfo', wraps=win32security.SetNamedSecurityInfo) as write:
                protect_state_directory(root)
                write.assert_not_called()
            acl = win32security.ACL()
            acl.AddAccessAllowedAce(win32security.ACL_REVISION, 0x1F01FF,
                win32security.CreateWellKnownSid(win32security.WinWorldSid, None))
            win32security.SetNamedSecurityInfo(str(child), win32security.SE_FILE_OBJECT,
                win32security.DACL_SECURITY_INFORMATION | win32security.PROTECTED_DACL_SECURITY_INFORMATION,
                None, None, acl, None)
            with patch('win32security.SetNamedSecurityInfo', wraps=win32security.SetNamedSecurityInfo) as write:
                protect_state_directory(root)
                self.assertEqual([call.args[0] for call in write.call_args_list], [str(child)])

    def test_state_acl_protects_existing_and_new_owned_files(self):
        import win32security
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'lighthouse_openclaw'
            child = root / 'files/existing.txt'
            child.parent.mkdir(parents=True)
            child.write_bytes(b'owned data')
            protect_state_directory(root)
            created = root / 'new.txt'
            created.write_bytes(b'new owned data')
            for path in (root, child, created):
                sd = win32security.GetNamedSecurityInfo(str(path), win32security.SE_FILE_OBJECT,
                    win32security.DACL_SECURITY_INFORMATION)
                acl = sd.GetSecurityDescriptorDacl()
                sids = {win32security.ConvertSidToStringSid(acl.GetAce(index)[2]) for index in range(acl.GetAceCount())}
                self.assertEqual(len(sids), 3)
                self.assertIn('S-1-5-18', sids)
                self.assertIn('S-1-5-32-544', sids)
                self.assertNotIn('S-1-1-0', sids)
                self.assertNotIn('S-1-5-32-545', sids)
            self.assertEqual(child.read_bytes(), b'owned data')

    def test_state_acl_rejects_links_before_any_permission_change(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'lighthouse_openclaw'
            root.mkdir()
            target = root / 'linked'
            target.mkdir()
            with patch.object(Path, 'is_junction', lambda path: path == target), \
                    patch('win32security.SetNamedSecurityInfo') as changed:
                with self.assertRaises(ServiceError):
                    protect_state_directory(root)
            changed.assert_not_called()

    def test_cli_refuses_to_start_when_parent_process_unavailable(self):
        # Replaces the removed manual BAT observer contract: the unified entry
        # refuses to start when the watchdog reports no usable parent process.
        from openclaw_service import __main__ as entry
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory) / 'project'
            state = Path(directory) / 'state'
            output = io.StringIO()
            with patch('upload_event_module.services.process_lifetime.start_parent_exit_watchdog', return_value=False), \
                    patch.object(entry, 'InstanceLock') as lock, \
                    patch.object(entry, 'serve') as serve, redirect_stdout(output):
                result = entry.main(['--parent-pid', '999999', '--project-root', str(project), '--state-root', str(state)])
            self.assertEqual(result, 1)
            self.assertIn('Parent process is no longer available', output.getvalue())
            lock.assert_not_called()
            serve.assert_not_called()

    def test_cli_refuses_to_start_with_invalid_parent_pid(self):
        # A watchdog that refuses an invalid/zero parent pid must block startup
        # without installing a real watchdog or touching the lock or server.
        from openclaw_service import __main__ as entry
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory) / 'project'
            state = Path(directory) / 'state'
            output = io.StringIO()
            with patch('upload_event_module.services.process_lifetime.start_parent_exit_watchdog', return_value=False), \
                    patch.object(entry, 'InstanceLock') as lock, \
                    patch.object(entry, 'serve') as serve, redirect_stdout(output):
                result = entry.main(['--parent-pid', '0', '--project-root', str(project), '--state-root', str(state)])
            self.assertEqual(result, 1)
            self.assertIn('Parent process is no longer available', output.getvalue())
            lock.assert_not_called()
            serve.assert_not_called()

    def test_account_directory_migration_preserves_history_and_is_repeatable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy = root / ('a' * 32) / 'workspace'
            legacy.mkdir(parents=True)
            (legacy / 'history.txt').write_bytes(b'isolated retained native history')
            (root / 'assistant.sqlite3').write_bytes(b'not a gateway directory')
            self.assertEqual(migrate_accounts(root), 1)
            self.assertEqual((root / 'accounts' / ('a' * 32) / 'workspace/history.txt').read_bytes(), b'isolated retained native history')
            self.assertEqual(migrate_accounts(root), 0)
            self.assertEqual((root / 'assistant.sqlite3').read_bytes(), b'not a gateway directory')

    def test_account_directory_conflict_is_rejected_before_any_move(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for key in ('a' * 32, 'b' * 32):
                (root / key).mkdir()
            (root / 'accounts' / ('b' * 32)).mkdir(parents=True)
            with self.assertRaises(ServiceError):
                migrate_accounts(root)
            self.assertTrue((root / ('a' * 32)).is_dir())
            self.assertTrue((root / ('b' * 32)).is_dir())

    def test_key_is_encrypted_and_reused(self):
        with tempfile.TemporaryDirectory() as root:
            key = control_key(root)
            self.assertEqual(control_key(root), key)
            self.assertNotIn(key.encode(), (Path(root) / 'service-key.dpapi').read_bytes())

    def test_descriptor_checks_exact_process_and_installation(self):
        with tempfile.TemporaryDirectory() as root:
            state = Path(root)
            record = {'identity': identity(PROJECT, state), 'instance': 'a' * 32, 'pid': os.getpid(),
                'process_stamp': process_stamp(os.getpid()), 'port': 7310}
            atomic_json(state / 'service.json', record)
            self.assertEqual(descriptor(state), record)
            self.assertIsNone(descriptor(state, Path(root) / 'foreign'))
            record['process_stamp'] += '-stale'
            atomic_json(state / 'service.json', record)
            self.assertIsNone(descriptor(state))

    def test_mutex_prevents_second_owner_and_allows_reopen(self):
        # Windows mutexes are recursive for the same thread, so a competing
        # launcher uses another thread exactly like a second process would.
        import threading
        with tempfile.TemporaryDirectory() as root:
            result = []
            with InstanceLock(PROJECT, root):
                def second():
                    try:
                        with InstanceLock(PROJECT, root):
                            result.append('unexpected')
                    except ServiceError as exc:
                        result.append(exc.code)
                thread = threading.Thread(target=second)
                thread.start()
                thread.join(2)
                self.assertFalse(thread.is_alive())
                self.assertEqual(result, ['already_running'])
            with InstanceLock(PROJECT, root):
                pass


class HostTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.manager = FakeManager()
        self.host = Host(state=self.directory.name, key='synthetic-control-key-' + 'x' * 40, manager=self.manager)
        self.host.port = 7310
        self.app = build_app(self.host)
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url='http://127.0.0.1:7310',
            headers={'Authorization': 'Bearer ' + self.host.key})
        self.profile = {'id': 'fixture', 'model': 'fixture-model', 'name': 'Fixture',
            'endpoint': 'http://127.0.0.1:1/v1/chat/completions', 'key_cipher': 'synthetic-not-a-real-key'}
        self.actor = {'id': 'fixture-d', 'scopes': ['D']}
        self.definitions = [{'name': 'lighthouse_probe', 'label': 'probe', 'description': 'Fixture',
            'parameters': {'type': 'object', 'properties': {'scope': {'type': 'string'}}}}]

    async def asyncTearDown(self):
        await self.client.aclose()
        await self.host.close()
        self.directory.cleanup()

    async def register(self, portal='fixture-portal'):
        response = await self.client.post('/register', json={'portal_id': portal, 'pid': os.getpid(),
            'process_stamp': process_stamp(os.getpid()), 'callback_url': 'http://127.0.0.1:7312/api/assistant/openclaw-tools',
            'digest': self.host.digest})
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()['data']['lease']

    async def acquire(self, lease, *, warm=True):
        response = await self.client.post('/acquire', json={'lease': lease, 'actor': self.actor, 'profile': self.profile,
            'definitions': self.definitions, 'request_id': 'fixture-reservation', 'warm_only': warm})
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()['data']

    async def begin(self, lease, item, run='fixture-run'):
        response = await self.client.post('/begin-run', json={'lease': lease, 'account': item['key'],
            'reservation': item['reservation'], 'session_key': 'agent:main:fixture', 'run_id': run, 'callback_token': 'synthetic-portal-token'})
        self.assertEqual(response.status_code, 200, response.text)

    async def test_auth_browser_and_external_callback_rejected(self):
        response = await self.client.post('/health', headers={'Authorization': 'wrong'}, json={})
        self.assertEqual(response.status_code, 403)
        response = await self.client.post('/health', headers={'Origin': 'http://127.0.0.1:7312'}, json={})
        self.assertEqual(response.status_code, 403)
        response = await self.client.post('/register', json={'portal_id': 'bad', 'callback_url': 'https://outside.invalid/'})
        self.assertEqual(response.status_code, 400)

    async def test_control_requires_bearer_scheme_and_integer_protocol(self):
        response = await self.client.post('/health', headers={'Authorization': self.host.key}, json={})
        self.assertEqual(response.status_code, 403)
        response = await self.client.post('/health', json={'protocol': True})
        self.assertEqual(response.status_code, 409)

    async def test_portal_reconnect_retains_idle_gateway_and_revokes_old_lease(self):
        first = await self.register('first')
        item = await self.acquire(first)
        response = await self.client.post('/unregister', json={'lease': first})
        self.assertEqual(response.status_code, 200)
        self.assertIn(item['key'], self.manager.accounts)
        second = await self.register('second')
        reused = await self.acquire(second)
        self.assertEqual(reused['pid'], item['pid'])
        self.assertEqual(self.manager.spawns, 1)
        response = await self.client.post('/acquire', json={'lease': first})
        self.assertEqual(response.status_code, 409)

    async def test_old_turn_rejected_before_any_business_callback(self):
        lease = await self.register()
        item = await self.acquire(lease, warm=False)
        await self.begin(lease, item)
        with patch('httpx.AsyncClient') as callback:
            response = await self.client.post('/bridge', headers={'Authorization': 'Bearer ' + self.host.bridge_key(item['key'])},
                json={'tool': 'lighthouse_probe', 'params': {}, 'call_id': 'old', 'session_key': 'agent:main:fixture', 'run_id': 'old-run'})
            self.assertEqual(response.status_code, 403)
            callback.assert_not_called()
        await self.host.end_run({'lease': lease, 'account': item['key'], 'run_id': 'fixture-run', 'reservation': item['reservation'], 'final': True})

    async def test_validation_retry_retains_reservation_and_changes_binding(self):
        lease = await self.register()
        item = await self.acquire(lease, warm=False)
        await self.begin(lease, item)
        await self.host.end_run({'lease': lease, 'account': item['key'], 'run_id': 'fixture-run', 'final': False})
        await self.begin(lease, item, 'fixture-run:validation')
        self.assertEqual(self.host.runs[item['key']]['run_id'], 'fixture-run:validation')
        await self.host.end_run({'lease': lease, 'account': item['key'], 'run_id': 'fixture-run:validation', 'reservation': item['reservation'], 'final': True})
        self.assertFalse(self.manager.accounts[item['key']]['busy'])

    async def test_model_and_scope_changes_replace_only_selected_account(self):
        lease = await self.register()
        await self.acquire(lease)
        self.profile = {**self.profile, 'model': 'changed-model'}
        await self.acquire(lease)
        self.assertEqual(self.manager.spawns, 2)
        self.actor = {**self.actor, 'scopes': ['E']}
        await self.acquire(lease)
        self.assertEqual(self.manager.spawns, 3)

    async def test_stale_pid_or_code_rejected(self):
        response = await self.client.post('/register', json={'portal_id': 'fixture', 'pid': os.getpid(),
            'process_stamp': 'stale', 'callback_url': 'http://127.0.0.1:7312/api/assistant/openclaw-tools', 'digest': self.host.digest})
        self.assertEqual(response.status_code, 409)
        response = await self.client.post('/register', json={'portal_id': 'fixture', 'pid': os.getpid(),
            'process_stamp': process_stamp(os.getpid()), 'callback_url': 'http://127.0.0.1:7312/api/assistant/openclaw-tools', 'digest': 'old'})
        self.assertEqual(response.status_code, 409)

    async def test_prepare_warms_model_transport_and_gateway_concurrently(self):
        gateway_entered, transport_entered = asyncio.Event(), asyncio.Event()
        async def prepare_gateway():
            gateway_entered.set()
            await transport_entered.wait()
        async def prepare_transport():
            transport_entered.set()
            await gateway_entered.wait()
        self.manager.prepare.side_effect = prepare_gateway
        self.manager.model_client.side_effect = prepare_transport
        await asyncio.wait_for(self.host.prepare(), 1)
        self.assertEqual(self.host.runtime_state, 'ready')
        self.manager.prepare.assert_awaited_once()
        self.manager.model_client.assert_awaited_once()
        self.assertEqual(self.manager.spawns, 0)

    async def test_failed_transport_cancels_pending_gateway_preparation(self):
        gateway_started, gateway_cancelled = asyncio.Event(), asyncio.Event()
        async def prepare_gateway():
            gateway_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                gateway_cancelled.set()
        async def prepare_transport():
            await gateway_started.wait()
            raise RuntimeError('sensitive-transport-value')
        self.manager.prepare.side_effect = prepare_gateway
        self.manager.model_client.side_effect = prepare_transport
        await asyncio.wait_for(self.host.prepare(), 1)
        self.assertEqual(self.host.runtime_state, 'failed')
        self.assertEqual(self.host.runtime_error, 'runtime_unavailable')
        self.assertTrue(gateway_cancelled.is_set())

    async def test_prepare_failure_does_not_stop_health(self):
        self.manager.prepare.side_effect = RuntimeError('sensitive-fixture-value')
        await self.host.prepare()
        response = await self.client.post('/health', json={})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['data']['runtime_state'], 'failed')
        self.assertNotIn('sensitive-fixture-value', response.text)


class MainEntryTests(unittest.TestCase):
    """Fully mocked entry.main: no host, Node, scheduler or network."""

    def _start_harness(self, *, lock_error=None, watchdog_ok=True, serve_error=None):
        from openclaw_service import __main__ as entry

        recorded = {'locks': []}

        class FakeLock:
            def __init__(self, project, state_root):
                self.project, self.state_root = project, state_root

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        def make_lock(project, state_root):
            recorded['locks'].append((Path(project), Path(state_root)))
            if lock_error is not None:
                raise lock_error
            return FakeLock(project, state_root)

        serve = AsyncMock()
        if serve_error is not None:
            serve.side_effect = serve_error
        patches = [
            patch('upload_event_module.services.process_lifetime.start_parent_exit_watchdog', return_value=watchdog_ok),
            patch.object(entry, 'InstanceLock', side_effect=make_lock),
            patch.object(entry, 'serve', new=serve),
        ]
        for item in patches:
            item.start()
            self.addCleanup(item.stop)
        return {'entry': entry, 'serve': serve, 'locks': recorded['locks']}

    def test_default_args_use_project_state_root(self):
        harness = self._start_harness()
        with redirect_stdout(io.StringIO()):
            result = harness['entry'].main(['--parent-pid', '100'])
        self.assertEqual(result, 0)
        self.assertEqual(harness['locks'], [(PROJECT, PROJECT / 'bin/data/lighthouse_openclaw')])
        harness['serve'].assert_awaited_once()

    def test_custom_project_without_state_root_derives_under_project(self):
        with tempfile.TemporaryDirectory() as directory:
            custom = Path(directory)
            harness = self._start_harness()
            with redirect_stdout(io.StringIO()):
                result = harness['entry'].main(['--parent-pid', '100', '--project-root', str(custom)])
        self.assertEqual(result, 0)
        self.assertEqual(harness['locks'], [(custom, custom / 'bin/data/lighthouse_openclaw')])
        harness['serve'].assert_awaited_once()

    def test_explicit_state_root_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory) / 'project'
            state = Path(directory) / 'explicit-state'
            harness = self._start_harness()
            with redirect_stdout(io.StringIO()):
                result = harness['entry'].main(
                    ['--parent-pid', '100', '--project-root', str(project), '--state-root', str(state)])
        self.assertEqual(result, 0)
        self.assertEqual(harness['locks'], [(project, state)])
        harness['serve'].assert_awaited_once()

    def test_duplicate_instance_lock_safely_returns_0(self):
        lock_error = ServiceError('助手服务已在运行，不重复启动。', 409, 'already_running')
        harness = self._start_harness(lock_error=lock_error)
        output = io.StringIO()
        with redirect_stdout(output):
            result = harness['entry'].main(['--parent-pid', '100'])
        self.assertEqual(result, 0)
        self.assertIn('助手服务已在运行', output.getvalue())
        harness['serve'].assert_not_awaited()

    def test_duplicate_instance_lock_does_not_start_second_worker(self):
        lock_error = ServiceError('助手服务已在运行，不重复启动。', 409, 'already_running')
        harness = self._start_harness(lock_error=lock_error)
        with redirect_stdout(io.StringIO()):
            result = harness['entry'].main(['--parent-pid', '100'])
        self.assertEqual(result, 0)
        self.assertEqual(harness['locks'], [(PROJECT, PROJECT / 'bin/data/lighthouse_openclaw')])
        harness['serve'].assert_not_awaited()

    def test_explicit_service_error_returns_1_with_clear_message(self):
        serve_error = ServiceError('fixture explicit failure', 503, 'worker_failed')
        harness = self._start_harness(serve_error=serve_error)
        output = io.StringIO()
        with redirect_stdout(output):
            result = harness['entry'].main(['--parent-pid', '100'])
        self.assertEqual(result, 1)
        self.assertIn('fixture explicit failure', output.getvalue())

    def test_runtime_error_returns_1_with_generic_safe_message(self):
        harness = self._start_harness(serve_error=RuntimeError('synthetic secret value'))
        output = io.StringIO()
        with redirect_stdout(output):
            result = harness['entry'].main(['--parent-pid', '100'])
        self.assertEqual(result, 1)
        self.assertTrue(output.getvalue().strip())
        self.assertNotIn('synthetic secret value', output.getvalue())

    def test_keyboard_interrupt_returns_0_safely(self):
        harness = self._start_harness(serve_error=KeyboardInterrupt)
        with redirect_stdout(io.StringIO()):
            result = harness['entry'].main(['--parent-pid', '100'])
        self.assertEqual(result, 0)

    def test_parent_process_unavailable_does_not_enter_lock_or_serve(self):
        harness = self._start_harness(watchdog_ok=False)
        output = io.StringIO()
        with redirect_stdout(output):
            result = harness['entry'].main(['--parent-pid', '100'])
        self.assertEqual(result, 1)
        self.assertIn('Parent process is no longer available', output.getvalue())
        self.assertEqual(harness['locks'], [])
        harness['serve'].assert_not_awaited()

    def test_removed_watch_argument_is_rejected(self):
        harness = self._start_harness()
        with self.assertRaises(SystemExit) as ctx:
            harness['entry'].main(['--parent-pid', '100', '--watch'])
        self.assertEqual(ctx.exception.code, 2)
        harness['serve'].assert_not_awaited()

    def test_removed_automatic_argument_is_rejected(self):
        harness = self._start_harness()
        with self.assertRaises(SystemExit) as ctx:
            harness['entry'].main(['--parent-pid', '100', '--automatic'])
        self.assertEqual(ctx.exception.code, 2)
        harness['serve'].assert_not_awaited()


if __name__ == '__main__':
    unittest.main()

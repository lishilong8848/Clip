"""Bounded ResidentRuntime (openclaw_service.client) tests over an in-memory ASGI Host.

All tests are fully isolated:
  * no real Node, Task Scheduler scheduling, cloud/model call, or production data;
  * the host uses a FakeManager (reused from bin.test_openclaw_service);
  * httpx.AsyncClient is patched so every portal request goes through an
    ASGITransport to the fixture Host while trust_env stays False and no
    redirects/proxies are used;
  * a real service.json descriptor + DPAPI control key are created only in a
    temporary directory with the current process stamp and matching identity.
"""
from __future__ import annotations

import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent))

import httpx

from openclaw_service.client import ResidentRuntime
from openclaw_service.protocol import PROTOCOL, PROJECT, ServiceError, atomic_json, control_key, identity, process_stamp, read_json
from openclaw_service.server import Host, build_app

# Reuse the FakeManager kept next-door so the sibling suite and this suite share
# the same synthetic gateway semantics.
from test_openclaw_service import FakeManager as _BaseFakeManager


class FakeManager(_BaseFakeManager):
    """FakeManager that records the actor tuple each manager.acquire receives."""

    def __init__(self):
        super().__init__()
        self.seen = []

    async def acquire(self, actor, model, profile, **kwargs):
        self.seen.append(dict(actor))
        return await super().acquire(actor, model, profile, **kwargs)


class _TransportClient:
    """Async-context client that funnels portal posts through ASGITransport.

    ``fail_action`` lets a test simulate a client-side transport timeout for a
    single endpoint after the fixture Host has processed the request exactly
    once (so upstream is never retried by the ResidentRuntime).
    """

    def __init__(self, app, real_client_cls, *, fail_action=None, counts=None, client_kwargs=None):
        self.app = app
        self.real = real_client_cls
        self.fail_action = fail_action
        self.counts = counts if counts is not None else {}
        self.client_kwargs = dict(client_kwargs or {})

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    async def post(self, url, *, json=None, headers=None, timeout=None):
        path = urlsplit(url).path.rstrip('/') or '/'
        self.counts[path] = self.counts.get(path, 0) + 1
        options = {k: v for k, v in self.client_kwargs.items() if k != 'transport'}
        options['transport'] = httpx.ASGITransport(app=self.app() if callable(self.app) else self.app)
        if timeout is not None:
            options['timeout'] = timeout
        if self.fail_action and path == self.fail_action:
            # Let the fixture host process the mutation exactly once, then
            # simulate a client-side transport timeout. ResidentRuntime must not
            # auto-retry and must not leak the raw exception or credentials.
            async with self.real(**options) as client:
                await client.post(url, json=json, headers=headers)
            raise httpx.ReadTimeout('simulated portal timeout for bounded test')
        async with self.real(**options) as client:
            return await client.post(url, json=json, headers=headers)

    async def aclose(self):
        pass


class ClientHostTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.state = Path(self.directory.name)
        self.key = control_key(self.state)  # real DPAPI, but only inside temp state
        self.manager = FakeManager()
        self.host = Host(state=self.state, key=self.key, manager=self.manager)
        self.host.port = 7310
        self.hosts = [self.host]
        self.app = build_app(self.host)
        self._runtimes = []
        self._patcher = None
        self.transport_options = []
        self.post_counts = {}
        self.fail_action = None
        self.fake_launch_calls = 0
        self._real_async_client = httpx.AsyncClient

        def bridge_url():
            return 'http://127.0.0.1:7312/api/assistant/openclaw-tools'
        self.callback_url = bridge_url

        self.profile = {'id': 'fixture', 'model': 'fixture-model', 'name': 'Fixture',
            'endpoint': 'http://127.0.0.1:1/v1/chat/completions', 'key_cipher': 'synthetic-not-a-real-key'}
        self.actor = {'id': 'fixture-d', 'scopes': ['D']}
        self.actor_scoped = {'id': 'fixture-d', 'allowed_scopes': ['E'], 'scopes': ['D']}
        self.definitions = [{'name': 'lighthouse_probe', 'label': 'probe', 'description': 'Fixture',
            'parameters': {'type': 'object', 'properties': {'scope': {'type': 'string'}}}}]

        self._patcher = patch('httpx.AsyncClient', self._make_factory())
        self._patcher.start()

    async def asyncTearDown(self):
        for runtime in self._runtimes:
            if getattr(runtime, 'monitor', None) is not None:
                await runtime.close()
        if self._patcher:
            self._patcher.stop()
        for host in self.hosts:
            await host.close()
        self.directory.cleanup()

    # -- helpers --------------------------------------------------------------

    def fake_launch(self, project):
        self.fake_launch_calls += 1

    def make_runtime(self, launch=None):
        runtime = ResidentRuntime(self.state, callback_url=self.callback_url, launch=launch or self.fake_launch)
        self._runtimes.append(runtime)
        return runtime

    def write_descriptor(self, *, port=None, instance=None, host=None):
        host = host or self.host
        port = port or host.port
        instance = instance or host.instance
        atomic_json(self.state / 'service.json', {
            'identity': identity(PROJECT, self.state),
            'instance': instance,
            'pid': os.getpid(),
            'process_stamp': process_stamp(os.getpid()),
            'port': port,
            'protocol': PROTOCOL,
        })

    async def acquire(self, runtime, *, warm=True, actor=None, profile=None):
        actor = actor or self.actor
        profile = profile or self.profile
        return await runtime.acquire(actor, None, profile, definitions=self.definitions, warm_only=warm)

    def _make_factory(self):
        real = self._real_async_client

        def factory(**kwargs):
            if 'transport' in kwargs:
                raise AssertionError('portal client must not pre-set a transport')
            if kwargs.get('trust_env') is not False:
                raise AssertionError('portal client must keep trust_env=False')
            if kwargs.get('follow_redirects') is not False:
                raise AssertionError('portal client must never set follow_redirects')
            self.transport_options.append({k: v for k, v in kwargs.items()})
            return _TransportClient(lambda: self.app, real, fail_action=self.fail_action,
                                    counts=self.post_counts, client_kwargs=kwargs)
        return factory

    # -- tests ----------------------------------------------------------------

    async def test_close_unregisters_but_does_not_stop_manager(self):
        self.write_descriptor()
        runtime = self.make_runtime()
        item = await self.acquire(runtime)  # runs prepare() + acquire()
        self.assertIn(item['key'], self.manager.accounts)
        self.assertIsNotNone(runtime.lease)
        await runtime.close()
        # unregister was sent -> host lease revoked
        self.assertIsNone(self.host.lease)
        # manager/gateway is untouched by close
        self.assertIn(item['key'], self.manager.accounts)
        self.assertFalse(self.manager.closed)

    async def test_retained_idle_gateway_across_two_portal_clients(self):
        self.write_descriptor()
        r1 = self.make_runtime()
        item1 = await self.acquire(r1)
        lease1 = r1.lease
        first_spawn = self.manager.spawns
        await r1.close()
        self.assertIn(item1['key'], self.manager.accounts)

        r2 = self.make_runtime()
        await r2.prepare()
        item2 = await self.acquire(r2)
        # exact same idle gateway, no new process spawned for the second portal
        self.assertEqual(item2['key'], item1['key'])
        self.assertEqual(item2['pid'], item1['pid'])
        self.assertEqual(self.manager.spawns, first_spawn)
        # but it is a fresh lease for the new portal client
        self.assertNotEqual(r2.lease, lease1)
        await r2.close()

    async def test_instance_change_resets_lease_and_re_registers(self):
        self.write_descriptor()
        runtime = self.make_runtime()
        await runtime.prepare()
        item = await self.acquire(runtime)
        self.assertIn(item['key'], runtime.accounts)
        first_lease = runtime.lease
        first_instance = runtime.instance

        # A second resident host comes up with a brand-new instance on a new
        # port (same temp state, same control key). Rewire the ASGI transport.
        manager2 = FakeManager()
        host2 = Host(state=self.state, key=self.key, manager=manager2)
        host2.port = 7311
        self.hosts.append(host2)
        self.app = build_app(host2)
        self.write_descriptor(port=host2.port, instance=host2.instance, host=host2)

        # Same ResidentRuntime re-reads the descriptor, detects the instance
        # change, clears its own account handles and re-registers exactly.
        await runtime.prepare()
        self.assertNotEqual(runtime.instance, first_instance)
        self.assertEqual(runtime.instance, host2.instance)
        self.assertIsNotNone(runtime.lease)
        self.assertNotEqual(runtime.lease, first_lease)
        self.assertEqual(runtime.accounts, {})

        # The resident manager from the first host retained the idle gateway.
        self.assertEqual(self.manager.spawns, 1)
        self.assertIn(item['key'], self.manager.accounts)
        # And the new host can reuse the retained gateway too.
        reused = await self.acquire(runtime)
        self.assertEqual(reused['key'], item['key'])
        self.assertEqual(manager2.spawns, 1)
        await runtime.close()

    async def test_previous_generation_manual_stop_does_not_block_main_startup(self):
        atomic_json(self.state / 'stopped.json', {'reason': 'manual'})
        runtime = self.make_runtime(launch=lambda project: self.write_descriptor())
        await runtime.prepare()
        self.assertFalse(runtime.disconnected)
        self.assertIsNotNone(runtime.monitor)

    async def test_monitor_recovers_without_replaying_account_or_business_requests(self):
        self.write_descriptor()
        runtime = self.make_runtime(launch=lambda project: self.write_descriptor())
        await runtime.prepare()
        self.post_counts.clear()
        (self.state / 'service.json').unlink()
        await runtime._check_connection()
        self.assertFalse(runtime.disconnected)
        self.assertEqual(self.post_counts, {'/health': 1, '/register': 1})

    async def test_monitor_recovers_previously_stopped_worker_without_business_replay(self):
        self.write_descriptor()
        runtime = self.make_runtime(launch=lambda project: self.write_descriptor())
        await runtime.prepare()
        (self.state / 'service.json').unlink()
        atomic_json(self.state / 'stopped.json', {'reason': 'manual'})
        self.post_counts.clear()
        await runtime._check_connection()
        self.assertFalse(runtime.disconnected)
        self.assertEqual(self.fake_launch_calls, 0)
        self.assertEqual(self.post_counts, {'/health': 1, '/register': 1})

    async def test_default_client_owns_worker_and_closes_it_after_unregister(self):
        self.write_descriptor()
        owner = Mock()
        owner.start.return_value = False
        owner.process.poll.return_value = None
        with patch('openclaw_service.launcher.ManagedService', return_value=owner) as factory:
            runtime = ResidentRuntime(self.state, callback_url=self.callback_url)
        self._runtimes.append(runtime)
        factory.assert_called_once_with(runtime.project, runtime.root)
        await runtime.prepare()
        owner.start.assert_called_once_with(runtime.project)
        await runtime.close()
        self.assertIsNone(self.host.lease)
        owner.close.assert_called_once()

    async def test_initial_start_failure_keeps_monitor_for_control_channel_recovery(self):
        def fail_launch(project):
            raise RuntimeError('synthetic startup failure')
        runtime = self.make_runtime(launch=fail_launch)
        with self.assertRaises(ServiceError) as ctx:
            await runtime.prepare()
        self.assertEqual(ctx.exception.code, 'service_launch_denied')
        self.assertIsNotNone(runtime.monitor)
        runtime.launch = lambda project: self.write_descriptor()
        await runtime._check_connection()
        self.assertFalse(runtime.disconnected)
        self.assertEqual(self.post_counts, {'/health': 1, '/register': 1})

    async def test_exited_owned_worker_fails_immediately_without_waiting_startup_timeout(self):
        owner = Mock()
        owner.start.return_value = True
        owner.process.poll.return_value = 1
        with patch('openclaw_service.launcher.ManagedService', return_value=owner):
            runtime = ResidentRuntime(self.state, callback_url=self.callback_url)
        self._runtimes.append(runtime)
        with self.assertRaises(ServiceError) as ctx:
            await runtime.prepare()
        self.assertEqual(ctx.exception.code, 'service_launch_denied')
        self.assertEqual(self.post_counts, {})

    async def test_successful_repeated_main_restarts_do_not_exhaust_recovery_limit(self):
        for _ in range(4):
            (self.state / 'service.json').unlink(missing_ok=True)
            runtime = self.make_runtime(launch=lambda project: self.write_descriptor())
            await runtime.prepare()
            self.assertFalse(runtime.disconnected)
            self.assertEqual(read_json(self.state / 'recovery.json')['attempts'], [])
            await runtime.close()

    async def test_malformed_recovery_state_does_not_crash_or_count_future_attempts(self):
        for attempts in ({'bad': 1}, [True, 'bad', time.time() + 3600]):
            with self.subTest(attempts=attempts):
                (self.state / 'service.json').unlink(missing_ok=True)
                atomic_json(self.state / 'recovery.json', {'attempts': attempts})
                runtime = self.make_runtime(launch=lambda project: self.write_descriptor())
                await runtime.prepare()
                self.assertEqual(read_json(self.state / 'recovery.json')['attempts'], [])
                await runtime.close()

    async def test_update_hold_forbids_launch(self):
        (self.state / 'update-hold.json').write_text('{}', encoding='utf-8')
        runtime = self.make_runtime()
        with self.assertRaises(ServiceError) as ctx:
            await runtime.prepare()
        self.assertEqual(ctx.exception.code, 'service_updating')
        self.assertEqual(self.fake_launch_calls, 0)
        self.assertIsNotNone(runtime.monitor)

    async def test_bounded_recovery_blocks_after_three_recent_attempts(self):
        now = time.time()
        atomic_json(self.state / 'recovery.json', {'attempts': [now - 1, now - 2, now - 3]})
        runtime = self.make_runtime()
        with self.assertRaises(ServiceError) as ctx:
            await runtime.prepare()
        self.assertEqual(ctx.exception.code, 'service_recovery_limited')
        self.assertEqual(self.fake_launch_calls, 0)
        self.assertEqual(len(read_json(self.state / 'recovery.json', {})['attempts']), 3)

    async def test_recovery_allows_launch_when_under_three_recent_attempts(self):
        now = time.time()
        atomic_json(self.state / 'recovery.json', {'attempts': [now - 1, now - 2]})

        def installing_launch(project):
            self.fake_launch_calls += 1
            self.write_descriptor()  # the resident host starts and publishes its descriptor

        runtime = self.make_runtime(launch=installing_launch)
        await runtime.prepare()
        self.assertEqual(self.fake_launch_calls, 1)
        saved = read_json(self.state / 'recovery.json', {})
        self.assertEqual(saved['attempts'], [])
        await runtime.close()

    async def test_no_external_proxy_or_redirects(self):
        self.write_descriptor()
        runtime = self.make_runtime()
        await runtime.prepare()
        await self.acquire(runtime)
        await runtime.close()
        self.assertTrue(self.transport_options)
        for options in self.transport_options:
            self.assertIs(options.get('trust_env'), False)
            self.assertIs(options.get('follow_redirects'), False)
        # The patched factory injects ASGITransport, so no request can escape to
        # a real proxy or network; every URL goes to the fixture Host.
        self.assertNotIn('transport', self.transport_options[0])

    async def test_timeout_does_not_retry_mutation_or_expose_raw_error_or_credentials(self):
        self.write_descriptor()
        self.fail_action = '/acquire'
        runtime = self.make_runtime()
        await runtime.prepare()
        with self.assertRaises(ServiceError) as ctx:
            await self.acquire(runtime)
        err = ctx.exception
        self.assertEqual(err.code, 'service_unavailable')
        for secret in (self.key, 'ReadTimeout', 'simulated', 'httpx'):
            self.assertNotIn(secret, str(err))
        # The mutation was attempted exactly once and never retried.
        self.assertEqual(self.post_counts.get('/acquire', 0), 1)
        self.assertEqual(self.manager.spawns, 1)
        # A transport failure marks the runtime disconnected so re-registration
        # (not an attacker-friendly auto-retry) is required before the next use.
        self.assertTrue(runtime.disconnected)
        await runtime.close()

    async def test_current_actor_and_scopes_passed_through(self):
        self.write_descriptor()
        runtime = self.make_runtime()
        await runtime.prepare()
        # When actor has allowed_scopes it wins over scopes.
        await self.acquire(runtime, actor=self.actor_scoped)
        # Without allowed_scopes, scopes is used unchanged.
        await self.acquire(runtime, actor={'id': 'fixture-d', 'scopes': ['D']})
        self.assertEqual(len(self.manager.seen), 2)
        first, second = self.manager.seen
        self.assertEqual(first['id'], 'fixture-d')
        self.assertEqual(first['scopes'], ['E'])
        self.assertEqual(second['id'], 'fixture-d')
        self.assertEqual(second['scopes'], ['D'])
        await runtime.close()


if __name__ == '__main__':
    unittest.main()

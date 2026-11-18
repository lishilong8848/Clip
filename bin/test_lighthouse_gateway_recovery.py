"""On-demand startup and shared health regressions. All subprocess/network I/O is fake."""
import asyncio
import io
import contextlib
from pathlib import Path
import tempfile
import sys
import time
import unittest
from unittest.mock import AsyncMock, Mock, patch

BIN = Path(__file__).resolve().parent
for location in (BIN.parent, BIN):
    if str(location) not in sys.path:
        sys.path.insert(0, str(location))

from openclaw_service.assistant import lighthouse_runtime as runtime_module
from openclaw_service.assistant import lighthouse_gateway as gateway_module
from openclaw_service.assistant.lighthouse_runtime import OpenClawRuntime, AssistantStartupError, account_key
from openclaw_service.assistant.lighthouse_gateway import GatewayError
from openclaw_service.assistant import lighthouse_startup_log as logs
from bin.test_lighthouse_runtime import _FakeProcess, _actor, _model, _profile


class GatewayRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.runtime = OpenClawRuntime(Path(self.directory.name))
        self.patches = contextlib.ExitStack()
        self.patches.enter_context(patch.object(runtime_module, 'runtime_files', return_value=(Path('node.exe'), Path('entry.mjs'))))
        self.popen = self.patches.enter_context(patch.object(runtime_module.subprocess, 'Popen'))
        self.processes = []
        def process(*args, **kwargs):
            item = _FakeProcess(5000 + len(self.processes))
            self.processes.append(item)
            return item
        self.popen.side_effect = process
        self.patches.enter_context(patch.object(runtime_module, 'free_port', side_effect=lambda: 19999+len(self.processes)))
        self.patches.enter_context(patch('upload_event_module.services.process_lifetime.register_child_process', return_value=True))
        self.network = self.patches.enter_context(patch.object(gateway_module, 'GatewayClient', side_effect=AssertionError('unexpected real gateway connection')))
        self.model = _model()

    async def asyncTearDown(self):
        await self.runtime.close()
        self.patches.close()

    async def acquire(self, identity, **kwargs):
        return await self.runtime.acquire(_actor(identity), self.model, _profile(), **kwargs)

    async def test_one_bad_account_tool_policy_never_restarts_shared_gateway(self):
        runtime = self.runtime
        bad_agent = runtime_module.agent_id('bad')
        class Client:
            protocol = 4
            def __init__(self, *args): pass
            async def __aenter__(self): return self
            async def __aexit__(self, *args): pass
            async def request(self, method, params=None, **kwargs):
                if method == 'models.list':
                    assert params == {'preparedOnly': True}, 'global health must not discover models or depend on account tools'
                    return {}
                if method == 'agents.list':
                    return {'agents': [{'id': item['agent_id'], 'name': item['config_fingerprint']} for item in runtime.accounts.values()]}
                if method == 'tools.effective':
                    tools = [] if params['agentId'] == bad_agent else [{'id': 'query', 'pluginId': 'lighthouse-tools'}]
                    return {'agentId': params['agentId'], 'groups': [{'tools': tools}]}
                if method in {'tools.catalog', 'sessions.patch'}: return {}
                raise AssertionError(method)
        with patch.object(gateway_module, 'GatewayClient', Client):
            results = await asyncio.gather(*(self.acquire(name, tool_names=['query']) for name in ('bad', 'good-a', 'good-b')), return_exceptions=True)
        self.assertIsInstance(results[0], AssistantStartupError)
        self.assertEqual(results[0].category, 'runtime_tools')
        self.assertTrue(all(item['configured'] for item in results[1:]))
        self.popen.assert_called_once()
        self.assertEqual(self.processes[0].terminate_count, 0)
        self.assertEqual(self.runtime._gateway_retry_at, 0)
        self.assertEqual(len(self.runtime.accounts), 3)
        self.assertFalse(self.runtime.accounts[account_key('bad')]['busy'])
        self.assertTrue(all(item['busy'] for item in results[1:]))

    async def test_dead_shared_process_enters_one_backoff_for_all_accounts(self):
        with patch.object(self.runtime, '_ready', AsyncMock()), patch.object(self.runtime, '_configured', AsyncMock()):
            original = await self.acquire('old')
            original['busy'] = False
            original['process']._poll = 1
            errors = await asyncio.gather(*(self.acquire('new-' + str(i)) for i in range(12)), return_exceptions=True)
            self.assertTrue(all(isinstance(error, AssistantStartupError) for error in errors))
            self.popen.assert_called_once()
            self.assertGreater(self.runtime._gateway_retry_at, time.monotonic())
            self.assertEqual(self.runtime._gateway_failure_count, 1)
            self.runtime._gateway_retry_at = 0
            recovered = await self.acquire('recovered')
        self.assertEqual(self.popen.call_count, 2)
        self.assertIsNot(recovered['process'], original['process'])
        self.assertTrue(original['stopped'])

    async def test_cold_failure_and_repeat_logins_share_one_attempt_and_cooldown(self):
        with patch.object(self.runtime, '_ready', AsyncMock(side_effect=AssistantStartupError('fixture', category='handshake_timeout'))):
            errors = await asyncio.gather(*(self.acquire('first-' + str(i)) for i in range(12)), return_exceptions=True)
            self.assertTrue(all(isinstance(error, AssistantStartupError) for error in errors))
            errors = await asyncio.gather(*(self.acquire('second-' + str(i)) for i in range(12)), return_exceptions=True)
        self.assertTrue(all(isinstance(error, AssistantStartupError) for error in errors))
        self.popen.assert_called_once()
        self.assertEqual(self.runtime._gateway_failure_count, 1)
        self.assertEqual(self.processes[0].terminate_count, 1)

    async def test_health_failure_is_coalesced_and_recovers_same_live_pid(self):
        with patch.object(self.runtime, '_ready', AsyncMock()), patch.object(self.runtime, '_configured', AsyncMock()):
            active = await self.acquire('active')
            self.runtime.gateway['health_checked_at'] = time.monotonic()-60
            calls = []
            broken = True
            class Client:
                def __init__(self, *args): calls.append(args)
                async def __aenter__(self): return self
                async def __aexit__(self, *args): pass
                async def request(self, method, params, **kwargs):
                    self.assertions = (method, params)
                    if broken: raise GatewayError('TIMEOUT')
                    return {}
            with patch.object(gateway_module, 'GatewayClient', Client):
                errors = await asyncio.gather(*(self.acquire('health-' + str(i)) for i in range(12)), return_exceptions=True)
                self.assertTrue(all(isinstance(error, AssistantStartupError) for error in errors))
                self.assertEqual(len(calls), 1)
                self.assertTrue(active['busy'])
                self.assertEqual(active['process'].terminate_count, 0)
                self.assertEqual(self.popen.call_count, 1)
                broken = False
                self.runtime.gateway['health_retry_at'] = 0
                recovered = await self.acquire('recovered')
                self.assertIs(recovered['process'], active['process'])
                self.assertEqual(len(calls), 2)
                self.popen.assert_called_once()

    async def test_cancelled_health_waiter_does_not_cancel_other_accounts_probe(self):
        entered, release = asyncio.Event(), asyncio.Event()
        calls = []
        class Client:
            def __init__(self, *args): calls.append(args)
            async def __aenter__(self): return self
            async def __aexit__(self, *args): pass
            async def request(self, *args, **kwargs):
                entered.set()
                await release.wait()
                return {}
        with patch.object(self.runtime, '_ready', AsyncMock()), patch.object(self.runtime, '_configured', AsyncMock()):
            original = await self.acquire('original')
            self.runtime.gateway['health_checked_at'] = time.monotonic()-60
            with patch.object(gateway_module, 'GatewayClient', Client):
                first = asyncio.create_task(self.acquire('cancelled'))
                await entered.wait()
                second = asyncio.create_task(self.acquire('continuing'))
                await asyncio.sleep(.01)
                first.cancel()
                with self.assertRaises(asyncio.CancelledError): await first
                self.assertFalse(self.runtime.gateway['health_task'].cancelled())
                release.set()
                result = await second
        self.assertIs(result['process'], original['process'])
        self.assertEqual(len(calls), 1)
        self.popen.assert_called_once()
        self.assertFalse(self.runtime.accounts[account_key('cancelled')]['busy'])


class DemandOnlyTests(unittest.IsolatedAsyncioTestCase):
    async def test_login_hooks_cannot_prepare_model_gateway_or_knowledge(self):
        from bin.test_lighthouse_startup import make_engine
        with tempfile.TemporaryDirectory() as directory:
            engine, portal = make_engine(directory)
            try:
                engine.manager.acquire = AsyncMock(side_effect=AssertionError('gateway startup from login'))
                portal.assistant.model_for = Mock(side_effect=AssertionError('model lookup from login'))
                for i in range(20): engine.queue_warmup(_actor('account-' + str(i)))
                await asyncio.sleep(0)
                self.assertEqual(engine.warming, {})
                engine.manager.acquire.assert_not_awaited()
                portal.assistant.model_for.assert_not_called()
                engine.warm_errors[account_key('account-0')] = AssistantStartupError('old optional warmup error')
                await engine.wait_warmup(_actor('account-0'), AsyncMock())
            finally:
                await engine.close()

    def test_underlying_startup_stage_and_bounded_retry_are_rendered_without_secrets(self):
        for stage, fields in (
            ('startup_failed', {'last_stage': 'handshake_timeout', 'error': 'AssistantStartupError'}),
            ('gateway_backoff', {'last_stage': 'process_exited', 'retry_ms': 120000, 'failures': 1}),
        ):
            output = io.StringIO()
            with contextlib.redirect_stdout(output): logs.emit(stage, **fields)
            rendered = logs.render(output.getvalue().strip())
            self.assertIsNotNone(rendered)
            self.assertIn('last_stage=', rendered)
        self.assertIsNone(logs.render(logs.PREFIX + '{"stage":"gateway_backoff","last_stage":"Bearer secret"}'))

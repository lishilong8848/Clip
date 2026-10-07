"""Console stop is effective during preparation and despite marker-write failure."""
import asyncio
import inspect
from pathlib import Path
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service import __main__ as entry
from openclaw_service.protocol import PROJECT, read_json


class EarlyExitTests(unittest.IsolatedAsyncioTestCase):
    async def check_stop(self, event, fail_marker=False):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            args = SimpleNamespace(state_root=state, project_root=PROJECT, runtime_root=None, automatic=False)
            callbacks = []
            def register(callback, enabled):
                if enabled:
                    callbacks.append(callback)
            def preparing(root):
                self.assertEqual(root, state)
                self.assertEqual(len(callbacks), 1, 'Stop handler must precede directory preparation')
                self.assertTrue(callbacks[0](event))
            timer = Mock()
            writer = patch.object(entry, 'atomic_json', side_effect=OSError('synthetic disk failure')) if fail_marker else patch.object(entry, 'atomic_json', wraps=entry.atomic_json)
            with patch('win32api.SetConsoleCtrlHandler', side_effect=register) as handlers, \
                    patch('win32process.SetPriorityClass'), \
                    patch.object(entry.threading, 'Timer', return_value=timer), \
                    patch.object(entry, 'protect_state_directory', side_effect=preparing), \
                    patch('upload_event_module.services.process_lifetime.close_child_process_job') as cleanup, \
                    patch('openclaw_service.server.Host', side_effect=AssertionError('A stopped service must not create its host')), writer:
                await asyncio.wait_for(entry.serve(args), 2)
                timer.start.assert_called_once()
                self.assertTrue(cleanup.called)
                self.assertEqual(handlers.call_args.args[1], False)
            if not fail_marker:
                self.assertEqual(read_json(state / 'stopped.json')['reason'], 'manual' if event in (0, 1, 2) else 'system')
                self.assertFalse((state / 'service.json').exists())

    async def test_close_during_preparation_does_not_start_host(self):
        await self.check_stop(2)

    async def test_system_event_during_preparation_records_system_stop(self):
        await self.check_stop(5)

    async def test_marker_failure_cannot_skip_forced_exit_or_child_cleanup(self):
        await self.check_stop(2, fail_marker=True)

    async def test_startup_lock_cannot_delay_exit_deadline_or_owned_child_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            args = SimpleNamespace(state_root=Path(directory), project_root=PROJECT,
                runtime_root=None, automatic=False)
            callbacks, failures = [], []
            deadline_armed, children_closed = threading.Event(), threading.Event()
            timer = Mock()
            timer.start.side_effect = deadline_armed.set
            def register(callback, enabled):
                if enabled:
                    callbacks.append(callback)
            def preparing(root):
                stop = inspect.getclosurevars(callbacks[0]).nonlocals['stop']
                startup_lock = inspect.getclosurevars(stop).nonlocals['stop_lock']
                def close_console():
                    try:
                        callbacks[0](2)
                    except BaseException as exc:
                        failures.append(exc)
                with startup_lock:
                    callback = threading.Thread(target=close_console)
                    callback.start()
                    armed = deadline_armed.wait(.5)
                    closed = children_closed.wait(.5)
                callback.join(2)
                self.assertFalse(callback.is_alive())
                self.assertTrue(armed, 'Startup filesystem lock delayed the forced-exit deadline')
                self.assertTrue(closed, 'Startup filesystem lock delayed owned child cleanup')
            with patch('win32api.SetConsoleCtrlHandler', side_effect=register), \
                    patch('win32process.SetPriorityClass'), \
                    patch.object(entry.threading, 'Timer', return_value=timer), \
                    patch.object(entry, 'protect_state_directory', side_effect=preparing), \
                    patch('upload_event_module.services.process_lifetime.close_child_process_job', side_effect=children_closed.set), \
                    patch('openclaw_service.server.Host', side_effect=AssertionError('Stopped startup constructed a host')):
                await asyncio.wait_for(entry.serve(args), 3)
            self.assertEqual(failures, [])
            timer.start.assert_called_once()
            self.assertEqual(read_json(args.state_root / 'stopped.json')['reason'], 'manual')


if __name__ == '__main__':
    unittest.main()

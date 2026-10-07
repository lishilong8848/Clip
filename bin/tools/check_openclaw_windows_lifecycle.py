"""Real Windows acceptance for integrated workers; only temporary data/loopback."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import win32api
import win32process

from openclaw_service.launcher import ManagedService
from openclaw_service.protocol import PROJECT, ServiceError, atomic_json, descriptor, process_stamp
from openclaw_service.update import AssistantUpdateGuard


def wait_descriptor(state, *, process=None, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        saved = descriptor(state, PROJECT)
        if saved:
            return saved
        if process is not None and process.poll() is not None:
            raise AssertionError('Isolated worker exited before publishing its descriptor')
        time.sleep(.05)
    raise AssertionError('Isolated worker startup timed out')


def wait_exit(saved, *, timeout=6):
    deadline = time.monotonic() + timeout
    while process_stamp(saved['pid']) == saved['process_stamp'] and time.monotonic() < deadline:
        time.sleep(.05)
    assert process_stamp(saved['pid']) != saved['process_stamp'], 'Owned worker did not exit'


def check(runtime):
    from openclaw_service.assistant.lighthouse_runtime import runtime_files
    runtime_files(runtime)  # Require a prepared runtime; never download during this probe.
    with tempfile.TemporaryDirectory(prefix='clipflow-integrated-') as directory:
        root = Path(directory)
        state = root / 'data/lighthouse_openclaw'
        owner = ManagedService(PROJECT, state, runtime_root=runtime)
        migrated = None
        parent = None
        try:
            began = time.monotonic()
            assert owner.start() is True
            saved = wait_descriptor(state, process=owner.process)
            ready_ms = (time.monotonic() - began) * 1000
            assert saved['parent_pid'] == os.getpid()
            assert saved['parent_stamp'] == process_stamp(os.getpid())
            assert saved['console_window'] == 0, 'Worker created a second console'
            handle = win32api.OpenProcess(0x1000, False, saved['pid'])
            try:
                assert win32process.GetPriorityClass(handle) == win32process.BELOW_NORMAL_PRIORITY_CLASS
            finally:
                handle.Close()
            assert owner.start() is False
            other = ManagedService(PROJECT, state, runtime_root=runtime)
            try:
                other.start()
                raise AssertionError('Another owner silently took over a live worker')
            except ServiceError as exc:
                assert exc.code == 'service_owner_active'
            finally:
                other.close()

            # Emulate only the previous generation's descriptor, in isolated state.
            saved.pop('parent_pid')
            saved.pop('parent_stamp')
            atomic_json(state / 'service.json', saved)
            retained = state / 'retained.txt'
            retained.write_text('isolated retained settings', encoding='utf-8')
            migrated = ManagedService(PROJECT, state, runtime_root=runtime)
            assert migrated.start() is True
            wait_exit(saved)
            assert owner.process.wait(timeout=2) == 0, 'Migrated worker teardown failed'
            saved = wait_descriptor(state, process=migrated.process)
            assert retained.read_text(encoding='utf-8') == 'isolated retained settings'

            # Real authenticated stop before applying changes to a private code fixture.
            fixture, payload = root / 'fixture', root / 'patch'
            target = fixture / 'bin/openclaw_service/build_marker.py'
            source = payload / 'bin/openclaw_service/build_marker.py'
            target.parent.mkdir(parents=True)
            source.parent.mkdir(parents=True)
            target.write_text('VERSION = 1\n', encoding='ascii')
            source.write_text('VERSION = 2\n', encoding='ascii')
            guard = AssistantUpdateGuard(fixture, state)
            updating_process = migrated.process
            with patch('openclaw_service.update.descriptor', return_value=saved):
                assert guard.pause(payload, [source]) is True
            wait_exit(saved)
            assert updating_process.wait(timeout=2) == 0, 'Update stop teardown failed'
            try:
                migrated.start()
                raise AssertionError('Worker started while update hold existed')
            except ServiceError as exc:
                assert exc.code == 'service_updating'
            target.write_text(source.read_text(encoding='ascii'), encoding='ascii')
            assert guard.finish(True) == ''
            assert descriptor(state, PROJECT) is None, 'Updater started a detached worker'
            assert migrated.start() is True
            saved = wait_descriptor(state, process=migrated.process)
            stopped = time.monotonic()
            stopping_process = migrated.process
            migrated.close()
            wait_exit(saved)
            assert stopping_process.wait(timeout=2) == 0, 'Owned worker shutdown failed'
            stop_ms = (time.monotonic() - stopped) * 1000
            assert retained.is_file()

            # Job assignment deliberately fails: the exact-parent watchdog must suffice.
            parent_state = root / 'parent-data/lighthouse_openclaw'
            code = (
                "import sys,time; from unittest.mock import patch; "
                "sys.path.insert(0,sys.argv[1]); "
                "from openclaw_service.launcher import ManagedService; "
                "owner=ManagedService(sys.argv[2],sys.argv[3],runtime_root=sys.argv[4]); "
                "\nwith patch('upload_event_module.services.process_lifetime.register_child_process',return_value=False): owner.start()\n"
                "time.sleep(120)"
            )
            parent = subprocess.Popen([sys.executable, '-B', '-u', '-c', code, str(PROJECT / 'bin'),
                str(PROJECT), str(parent_state), str(runtime)], cwd=PROJECT,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW)
            dependent = wait_descriptor(parent_state, process=parent)
            assert dependent['parent_pid'] == parent.pid
            parent.terminate()
            parent.wait(timeout=5)
            wait_exit(dependent)
            print(f'[IntegratedOpenClaw] ready_ms={ready_ms:.1f} stop_ms={stop_ms:.1f}; '
                  'singleton, hidden priority, legacy migration, update hold and parent-exit passed')
        finally:
            if parent is not None and parent.poll() is None:
                parent.terminate()
                parent.wait(timeout=5)
            if migrated is not None:
                migrated.close()
            owner.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--runtime', type=Path, default=PROJECT / 'build_output/lighthouse_openclaw_verified')
    check(parser.parse_args().runtime)

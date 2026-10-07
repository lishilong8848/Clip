"""Real isolated service CLI lifecycle; no model, portal or business-cloud writes."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx
from openclaw_service.protocol import PROJECT, control_key, descriptor, process_stamp


def check(runtime):
    with tempfile.TemporaryDirectory(prefix='resident-cli-') as directory:
        state = Path(directory) / 'data/lighthouse_openclaw'
        output = Path(directory) / 'console.log'
        process = None
        try:
            with output.open('wb') as console:
                start = time.monotonic()
                process = subprocess.Popen([sys.executable, '-B', '-u', '-m', 'openclaw_service',
                    '--state-root', str(state), '--runtime-root', str(runtime), '--parent-pid', str(os.getpid())], cwd=PROJECT,
                    env={**os.environ, 'PYTHONPATH': str(PROJECT / 'bin')},
                    stdout=console, stderr=subprocess.STDOUT, creationflags=subprocess.CREATE_NO_WINDOW)
                deadline = start + 30
                saved = None
                while saved is None and time.monotonic() < deadline:
                    if process.poll() is not None:
                        raise AssertionError('Owned service exited before health readiness')
                    saved = descriptor(state, PROJECT)
                    time.sleep(.05)
                assert saved is not None, 'Owned service did not publish its descriptor'
                assert saved['pid'] == process.pid
                key = control_key(state)
                root = f"http://127.0.0.1:{saved['port']}"
                headers = {'Authorization': 'Bearer ' + key}
                with httpx.Client(trust_env=False, follow_redirects=False, timeout=3) as client:
                    message = {'protocol': 1, 'instance': saved['instance']}
                    response = client.post(root + '/health', json=message, headers=headers)
                    assert response.status_code == 200 and response.json()['data']['accounts'] == 0
                    ready = time.monotonic() - start
                    second = subprocess.run([sys.executable, '-B', '-m', 'openclaw_service',
                        '--state-root', str(state), '--runtime-root', str(runtime), '--parent-pid', str(os.getpid())], cwd=PROJECT,
                        env={**os.environ, 'PYTHONPATH': str(PROJECT / 'bin')},
                        capture_output=True, timeout=15, creationflags=subprocess.CREATE_NO_WINDOW)
                    assert second.returncode == 0, 'Duplicate launcher must reuse the existing instance'
                    assert descriptor(state, PROJECT)['pid'] == process.pid
                    stop = time.monotonic()
                    stopped = client.post(root + '/shutdown', json={**message, 'reason': 'manual'}, headers=headers)
                    assert stopped.status_code == 200
                    process.wait(timeout=5)
                    elapsed = time.monotonic() - stop
                    assert process_stamp(process.pid) is None
                    assert descriptor(state, PROJECT) is None
                    assert elapsed <= 5
                process = None
            assert (state / 'logs/service.log').is_file()
            assert not (state / 'assistant.sqlite3').exists(), 'No portal: do not create or reset assistant data'
            print(f'[ResidentCLI] health_ms={ready * 1000:.1f} stop_ms={elapsed * 1000:.1f}; singleton and exact-process exit passed')
        finally:
            if process is not None and process.poll() is None:
                # Exact Popen handle of this isolated probe only, never a PID search.
                process.terminate()
                process.wait(timeout=5)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--runtime', type=Path, default=PROJECT / 'build_output/lighthouse_openclaw_verified')
    check(parser.parse_args().runtime)

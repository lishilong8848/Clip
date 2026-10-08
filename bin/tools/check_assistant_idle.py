"""Compare portal idle cost with/without its assistant, using empty isolated data."""
import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import urllib.request


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--without-assistant', action='store_true')
    args = parser.parse_args()
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    with tempfile.TemporaryDirectory(prefix='clipflow-idle-') as temporary:
        root = Path(temporary)
        os.environ.update(CLIPFLOW_DATA_DIR=str(root), CLIPFLOW_BACKEND_MOCK_EXTERNAL='1',
                          CLIPFLOW_REQUIRE_REAL_EXTERNAL_CONFIRM='1', CLIPFLOW_REAL_EXTERNAL_CONFIRMED='0')
        from check_openclaw_backend_native import process_resources
        from clipflow_backend import main as backend
        from lan_bitable_template_portal import lighthouse_routes
        from openclaw_service.protocol import read_json
        from openclaw_service.assistant.lighthouse_runtime import free_port
        if args.without_assistant:
            lighthouse_routes.install_lighthouse_routes = lambda *_: None
        controller = backend.FastAPIPortalController(host='127.0.0.1', port=free_port())
        try:
            started = time.monotonic()
            url = controller.start()
            assert controller._state_store.db_path.parent == root
            print('isolated_portal_ready_seconds', round(time.monotonic() - started, 2), flush=True)
            time.sleep(20)
            descriptors = {os.getpid(): 'portal'}
            service = read_json(root / 'lighthouse_openclaw/service.json', {})
            if args.without_assistant:
                assert not service, 'Control run unexpectedly started an assistant'
            if not args.without_assistant:
                assert service.get('pid'), 'Assistant did not start'
                descriptors[service['pid']] = 'assistant'
            before = {pid: process_resources(pid) for pid in descriptors}
            latencies = []
            started = time.monotonic()
            for _ in range(6):
                tick = time.monotonic()
                with urllib.request.urlopen(url + '/api/health', timeout=5) as response:
                    assert response.status == 200
                    response.read()
                latencies.append((time.monotonic() - tick) * 1000)
                time.sleep(5)
            elapsed = time.monotonic() - started
            report = {'assistant_enabled': not args.without_assistant,
                      'health_max_ms': round(max(latencies), 1), 'processes': []}
            for pid, name in descriptors.items():
                sample = process_resources(pid)
                report['processes'].append({'role': name, 'cpu_one_core_pct': round(
                    (sample['cpu_seconds'] - before[pid]['cpu_seconds']) / elapsed * 100, 2),
                    'private_mib': round(sample['private_mib'], 1)})
            print('[AssistantIdle] ' + json.dumps(report), flush=True)
        finally:
            controller.stop()
            import logging
            from upload_event_module import logger
            logger._stop_logging_listener()
            logger._close_crash_trace_stream()
            logging.shutdown()


if __name__ == '__main__':
    main()

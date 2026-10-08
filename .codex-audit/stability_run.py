import ipaddress
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import time
import unittest


ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'bin')]
os.chdir(ROOT)
if os.name == 'nt':
    import ctypes
    ctypes.windll.kernel32.SetPriorityClass(ctypes.windll.kernel32.GetCurrentProcess(), 0x4000)

blocked = []
connect, connect_ex = socket.socket.connect, socket.socket.connect_ex


def permitted(address):
    if isinstance(address, tuple):
        host = str(address[0])
        try:
            local = ipaddress.ip_address(host.split('%')[0]).is_loopback
        except ValueError:
            local = host.lower() == 'localhost'
        if not local:
            blocked.append(host)
            raise OSError('Audit fixture blocked a non-loopback network connection')


def local_connect(self, address):
    permitted(address)
    return connect(self, address)


def local_connect_ex(self, address):
    permitted(address)
    return connect_ex(self, address)


socket.socket.connect, socket.socket.connect_ex = local_connect, local_connect_ex
def main():
    group = sys.argv[1] if len(sys.argv) > 1 else 'all'
    paths = sorted((ROOT / 'bin').glob('test_*.py'))
    if group == 'business-isolated':
        import subprocess
        from concurrent.futures import ThreadPoolExecutor, as_completed
        paths = [path for path in paths if not path.stem.startswith(('test_lighthouse_', 'test_openclaw_', 'test_learning'))]
        def check(path):
            target = ROOT / '.codex-audit' / ('stability-' + path.stem + '.json')
            target.unlink(missing_ok=True)
            with (ROOT / '.codex-audit' / ('stability-' + path.stem + '.log')).open('wb') as output:
                proc = subprocess.run([sys.executable, '-u', str(Path(__file__).resolve()), path.stem],
                    cwd=ROOT, stdout=output, stderr=subprocess.STDOUT)
            report = json.loads(target.read_text(encoding='utf-8')) if target.exists() else {'group': path.stem, 'runner_failed': True}
            report['exit_code'] = proc.returncode
            return report
        reports = []
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(check, path) for path in paths]
            for task in as_completed(futures):
                report = task.result()
                reports.append(report)
                (ROOT / '.codex-audit/stability-business-isolated.json').write_text(json.dumps(reports, ensure_ascii=False, indent=2), encoding='utf-8')
                print('[IsolatedModule]', report['group'], report.get('tests', 0), report['exit_code'], flush=True)
        return 0 if all(report['exit_code'] == 0 for report in reports) else 1
    if group.startswith('test_'):
        paths = [path for path in paths if path.stem == group]
    elif group != 'all':
        paths = [path for path in paths if ('assistant' if path.stem.startswith(('test_lighthouse_', 'test_openclaw_', 'test_learning')) else 'business') == group]
    with tempfile.TemporaryDirectory(prefix='clipflow-stability-', ignore_cleanup_errors=True) as data:
        os.environ['CLIPFLOW_DATA_DIR'] = data
        os.environ.pop('CLIPFLOW_ALLOW_TEST_CREATE_RECORD', None)
        started = time.monotonic()
        if group in {'test_lan_template_work_status', 'test_polling_work_orders'}:
            import hashlib
            from upload_event_module.config import config
            config.user_token = 'audit-fixture-token'
            config.token_expire_time = int(time.time()) + 7200
            config.app_token = 'audit-fixture-app'
            for name in ('weibao', 'biangeng', 'tiaozheng', 'shijian', 'power', 'polling', 'overhaul'):
                setattr(config, 'table_id_' + name, 'tbl' + hashlib.sha256(name.encode()).hexdigest()[:13])
        if group == 'smoke':
            from bin.tools.frontend_runtime_smoke import run_smoke
            report = run_smoke(port=0, desktop_only=True)
            (ROOT / '.codex-audit/stability-smoke.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
            print('[StabilitySmoke]', json.dumps(report, ensure_ascii=False), flush=True)
            return 0
        names = ['bin.' + group + '.' + name for name in sys.argv[2:]] if len(sys.argv) > 2 else ['bin.' + path.stem for path in paths]
        suite = unittest.defaultTestLoader.loadTestsFromNames(names)
        result = unittest.TextTestRunner(stream=sys.__stderr__, verbosity=1).run(suite)
        report = {'group': group, 'modules': len(paths), 'tests': result.testsRun,
                  'failures': [test.id() for test, _ in result.failures],
                  'errors': [test.id() for test, _ in result.errors],
                  'skipped': len(result.skipped), 'seconds': round(time.monotonic() - started, 1),
                  'external_connections_blocked': len(blocked)}
        label = group + ('-selected' if len(sys.argv) > 2 else '')
        (ROOT / '.codex-audit' / ('stability-' + label + '.json')).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        print('[StabilityAudit]', json.dumps(report, ensure_ascii=False), flush=True)
        return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    sys.exit(main())

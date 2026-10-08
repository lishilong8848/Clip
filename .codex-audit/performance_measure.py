"""Local-only measurements; never starts production services or writes cloud data."""
import ast
from contextlib import closing
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'bin'), str(ROOT)]


def previous_method(path, class_name, name, namespace):
    source = subprocess.check_output(['git', 'show', 'HEAD:' + path], cwd=ROOT).decode('utf-8')
    cls = next(node for node in ast.parse(source).body if isinstance(node, ast.ClassDef) and node.name == class_name)
    method = next(node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == name)
    scope = dict(namespace)
    exec(compile(ast.Module(body=[method], type_ignores=[]), '<previous-performance-path>', 'exec'), scope)
    return scope[name]


def median_ms(call, count):
    values = []
    for _ in range(count):
        start = time.perf_counter()
        call()
        values.append((time.perf_counter() - start) * 1000)
    return round(statistics.median(values), 3)


def main():
    if os.name == 'nt':
        import ctypes
        ctypes.windll.kernel32.SetPriorityClass(ctypes.windll.kernel32.GetCurrentProcess(), 0x4000)
    with tempfile.TemporaryDirectory(prefix='clipflow-perf-', ignore_cleanup_errors=True) as temporary:
        os.environ['CLIPFLOW_DATA_DIR'] = temporary
        from lan_bitable_template_portal import state_store
        from openclaw_service.assistant import lighthouse_api
        store = state_store.LanPortalStateStore(Path(temporary) / 'fixture.sqlite3')
        store.put_document('fixture', 'record', {'value': 'preserved'})
        with closing(state_store.sqlite3.connect(store.db_path)) as connection:
            connection.execute('CREATE TABLE fixture_payload (value BLOB)')
            connection.executemany('INSERT INTO fixture_payload VALUES (zeroblob(65536))', [()] * 512)
            connection.commit()
        saved = store.backup_database()
        old_backup = previous_method('bin/lan_bitable_template_portal/state_store.py', 'LanPortalStateStore', 'backup_database', vars(state_store))
        backup = {'bytes': saved['backup_bytes'],
                  'before_ms': median_ms(lambda: old_backup(store), 3),
                  'after_ms': median_ms(store.backup_database, 10)}
        catalog = object.__new__(lighthouse_api.PortalAPICatalog)
        catalog._order = [f'GET /api/fixture/{index}' for index in range(500)]
        catalog._descriptors = {key: {'id': key, 'group': 'Business', 'name': 'fixture',
            'schema': {'properties': {str(index): {'type': 'string', 'description': 'fixture' * 20} for index in range(35)}}}
            for key in catalog._order}
        old_discover = previous_method('bin/openclaw_service/assistant/lighthouse_api.py', 'PortalAPICatalog', 'discover', vars(lighthouse_api))
        assert old_discover(catalog, page=2, page_size=8) == catalog.discover(page=2, page_size=8)
        discovery = {'entries': 500, 'page_size': 8,
                     'before_ms': median_ms(lambda: old_discover(catalog, page_size=8), 20),
                     'after_ms': median_ms(lambda: catalog.discover(page_size=8), 20)}
        print(json.dumps({'backup_reuse': backup, 'catalog_page': discovery}, indent=2), flush=True)


if __name__ == '__main__':
    main()

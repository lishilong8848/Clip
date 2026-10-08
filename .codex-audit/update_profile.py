"""Measure cumulative patch application in disposable local directories."""
import ast
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'bin'), str(ROOT)]


def main():
    with tempfile.TemporaryDirectory(prefix='clipflow-update-profile-', ignore_cleanup_errors=True) as directory:
        temporary = Path(directory)
        os.environ['CLIPFLOW_DATA_DIR'] = str(temporary / 'data')
        from upload_event_module.ui import main_window_patch as module
        source = subprocess.check_output(['git', 'show', 'HEAD:bin/upload_event_module/ui/main_window_patch.py'], cwd=ROOT).decode('utf-8')
        node = next(node for node in ast.parse(source).body if isinstance(node, ast.ClassDef) and node.name == 'PatchUpdateMixin')
        namespace = dict(vars(module))
        exec(compile(ast.Module(body=[node], type_ignores=[]), '<previous-update-path>', 'exec'), namespace)
        results = []
        for label, cls in [('before', namespace['PatchUpdateMixin']), ('after', module.PatchUpdateMixin)]:
            root, payload = temporary / label / 'app', temporary / label / 'patch'
            root.mkdir(parents=True)
            payload.mkdir(parents=True)
            hashes = {}
            for index in range(100):
                name = f'asset-{index}.js'
                old = b'// unchanged local fixture\n' + b'x' * (128 * 1024)
                new = old if index < 98 else old + b' changed'
                (root / name).write_bytes(old)
                (payload / name).write_bytes(new)
                hashes[name] = hashlib.sha256(new).hexdigest()
            item = cls()
            item._last_patch_meta, item._last_patch_source = {'file_sha256': hashes}, 'local'
            item._get_app_root_dir = lambda: root
            item._update_build_meta = Mock()
            item._delete_patch_dir = lambda _: ''
            item._discard_invalid_patch = lambda *_: (_ for _ in ()).throw(AssertionError('invalid fixture'))
            item.patch_update_finished = SimpleNamespace(emit=Mock())
            item._copy_with_retry = Mock(wraps=item._copy_with_retry)
            start = time.perf_counter()
            with patch.object(module.config, 'auto_install_dependencies', False), \
                    patch('upload_event_module.services.process_lifetime.lower_current_thread_priority'):
                item._apply_patch_worker(payload)
            elapsed = time.perf_counter() - start
            assert item.patch_update_finished.emit.call_args.args[0]
            assert all((root / name).read_bytes() == (payload / name).read_bytes() for name in hashes)
            results.append({'version': label, 'seconds': round(elapsed, 3), 'replaced_files': item._copy_with_retry.call_count})
        print(json.dumps(results), flush=True)


if __name__ == '__main__':
    main()

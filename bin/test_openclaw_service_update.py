"""Isolated update scope, lifecycle and exact-process regressions."""
from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from openclaw_service.protocol import atomic_json, process_stamp, read_json
from openclaw_service.update import AssistantUpdateGuard, affected_path


class AssistantUpdateTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.project, self.payload = self.root / 'app', self.root / 'patch'
        self.state = self.project / 'bin/data/lighthouse_openclaw'
        self.project.mkdir()
        self.payload.mkdir()
        self.guard = AssistantUpdateGuard(self.project, self.state)

    def file(self, relative, before, after):
        old, new = self.project / relative, self.payload / relative
        old.parent.mkdir(parents=True, exist_ok=True)
        new.parent.mkdir(parents=True, exist_ok=True)
        old.write_text(before, encoding='utf-8')
        new.write_text(after, encoding='utf-8')
        return old, new

    def test_ui_only_patch_leaves_service_running(self):
        _, source = self.file('bin/lan_bitable_template_portal/frontend/dist/index.html', 'old', 'new')
        with patch('openclaw_service.update.descriptor', side_effect=AssertionError('no service stop needed')):
            self.assertFalse(self.guard.pause(self.payload, [source]))
        self.assertFalse(self.guard.hold.exists())

    def test_unchanged_forced_service_files_do_not_restart(self):
        _, source = self.file('bin/openclaw_service/server.py', 'same', 'same')
        self.assertFalse(self.guard.pause(self.payload, [source]))
        self.assertFalse(self.guard.paused)

    def test_service_patch_holds_until_finish_without_starting_an_idle_service(self):
        old, source = self.file('bin/openclaw_service/server.py', 'old', 'new')
        with patch('openclaw_service.update.descriptor', return_value=None):
            self.assertTrue(self.guard.pause(self.payload, [source]))
        self.assertEqual(read_json(self.guard.hold)['phase'], 'applying')
        self.assertEqual(read_json(self.guard.hold)['pid'], os.getpid())
        old.write_text('new', encoding='utf-8')
        with patch('openclaw_service.launcher.ManagedService.start', side_effect=AssertionError('the updater must never start a detached worker')):
            self.assertEqual(self.guard.finish(True), '')
        self.assertFalse(self.guard.hold.exists())

    def test_shared_dependency_install_holds_even_for_ui_only_patch(self):
        _, source = self.file('bin/lan_bitable_template_portal/frontend/dist/index.html', 'old', 'new')
        with patch('openclaw_service.update.descriptor', return_value=None):
            self.assertTrue(self.guard.pause(self.payload, [source], dependency_change=True))
        self.guard.finish(True)
        self.assertFalse(self.guard.hold.exists())

    def test_failed_update_resumes_only_restored_code(self):
        old, source = self.file('bin/openclaw_service/server.py', 'old', 'new')
        with patch('openclaw_service.update.descriptor', return_value=None):
            self.guard.pause(self.payload, [source])
        old.write_text('mixed', encoding='utf-8')
        self.assertTrue(self.guard.finish(False))
        self.assertEqual(read_json(self.guard.hold)['phase'], 'needs_repair')
        old.write_text('old', encoding='utf-8')
        self.assertEqual(self.guard.finish(False), '')
        self.assertFalse(self.guard.hold.exists())

    def test_failed_dependencies_keep_assistant_stopped(self):
        _, source = self.file('bin/openclaw_service/server.py', 'old', 'new')
        with patch('openclaw_service.update.descriptor', return_value=None):
            self.guard.pause(self.payload, [source])
        self.assertTrue(self.guard.finish(False, dependencies_ready=False))
        self.assertTrue(self.guard.hold.exists())

    def test_another_live_update_is_not_overwritten(self):
        _, source = self.file('bin/openclaw_service/server.py', 'old', 'new')
        original = {'update_id': 'other', 'pid': os.getpid(), 'process_stamp': process_stamp(os.getpid()), 'phase': 'applying'}
        atomic_json(self.guard.hold, original)
        with self.assertRaises(Exception):
            self.guard.pause(self.payload, [source])
        self.assertEqual(read_json(self.guard.hold), original)

    def test_foreign_hold_is_not_removed_on_finish(self):
        self.guard.paused = True
        atomic_json(self.guard.hold, {'update_id': 'other'})
        self.assertEqual(self.guard.finish(True), '')
        self.assertEqual(read_json(self.guard.hold)['update_id'], 'other')

    def test_retained_dead_process_handle_is_not_a_live_descriptor(self):
        if os.name != 'nt':
            self.skipTest('Windows kernel process handle regression')
        child = subprocess.Popen([sys.executable, '-B', '-c', 'pass'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            self.assertEqual(child.wait(timeout=5), 0)
            self.assertIsNone(process_stamp(child.pid))
        finally:
            if child.poll() is None:
                child.terminate()
                child.wait(timeout=5)

    def test_scope_covers_service_contracts_but_not_business_or_ui(self):
        for path in ('bin/openclaw_service/assistant/lighthouse_ai.py', 'bin/openclaw_service/assistant/openclaw/skills/registry.json',
                     'bin/.venv/Lib/site-packages/x.py', 'bin/clipflow_backend/api_models.py',
                     '启动程序.bat', '启动程序openclaw.bat', '启动程序openclaw.py', 'bin/refactored_main.py'):
            self.assertTrue(affected_path(path))
        for path in ('bin/lan_bitable_template_portal/frontend/dist/index.html', 'bin/lan_bitable_template_portal/repair_operations.py'):
            self.assertFalse(affected_path(path))

    def test_qt_patch_result_is_emitted_after_assistant_hold_is_finished(self):
        from bin.test_transport_safety import isolated_class
        from frontend_assets import FRONTEND_INDEX
        namespace = {'Path': Path, 'shutil': shutil, 'config': SimpleNamespace(auto_install_dependencies=False),
            'FRONTEND_INDEX': FRONTEND_INDEX, 'patch_deletions': lambda root, payload, deleted: deleted,
            'log_warning': lambda *_: None}
        installer = isolated_class(Path(__file__).parent / 'upload_event_module/ui/main_window_patch.py',
            'PatchUpdateMixin', {'_apply_patch_worker'}, namespace)
        for failure in ('none', 'copy', 'hash', 'delete', 'pause'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                payload = root / 'patch'
                payload.mkdir()
                source = payload / 'test.py'
                source.write_text('x = 1\n', encoding='utf-8')
                backup = root / 'backup'
                backup.mkdir()
                old = root / 'old.txt'
                old.write_text('old', encoding='utf-8')
                events = []
                guard = SimpleNamespace()
                def pause(*args, **kwargs):
                    events.append('pause')
                    if failure == 'pause':
                        raise RuntimeError('isolated stop failure')
                def finish(applied, **kwargs):
                    events.append(('finish', applied, kwargs['dependencies_ready']))
                    return ''
                guard.pause, guard.finish = pause, finish
                item = installer()
                item._last_patch_meta, item._last_patch_source = {}, 'local'
                item._get_app_root_dir = lambda: root
                item._collect_patch_files = lambda _: [source]
                item._parse_deleted_files = lambda _: [Path('old.txt')]
                item._backup_patch_targets = lambda *args: (backup, [], [Path('old.txt')])
                item._rollback_patch = lambda *args: events.append('rollback')
                def copy(src, dest):
                    if failure == 'copy':
                        return False
                    shutil.copyfile(src, dest)
                    return True
                item._copy_with_retry = copy
                item._sha256_file = lambda path: path.name if failure == 'hash' and path == root / 'test.py' else 'same'
                item._delete_with_retry = lambda _: failure != 'delete'
                item._update_build_meta = lambda *args: None
                item._delete_patch_dir = lambda _: ''
                item._discard_invalid_patch = lambda *args: events.append(('emit', False))
                item.patch_update_finished = SimpleNamespace(emit=lambda success, message: events.append(('emit', success)))
                with patch('openclaw_service.update.AssistantUpdateGuard', return_value=guard):
                    item._apply_patch_worker(payload)
                finished = [event for event in events if isinstance(event, tuple) and event[0] == 'finish']
                self.assertEqual(finished, [('finish', failure == 'none', True)], events)
                self.assertEqual(events[-1], ('emit', failure == 'none'), events)


if __name__ == '__main__':
    unittest.main()

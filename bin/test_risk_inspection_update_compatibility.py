"""Apply runtime updates only to temporary clients; never publish or use real data."""
import ast
import hashlib
import importlib
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import time
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch
from zipfile import ZIP_DEFLATED, ZipFile

from bin.test_transport_safety import isolated_class
from frontend_assets import FRONTEND_DIST, FRONTEND_INDEX, V2_INDEX, V2_MANIFEST, patch_deletions, referenced_assets
from upload_event_module.services.remote_patch_updater import RemotePatchUpdater
import package_portable as packaging


ROOT = Path(__file__).resolve().parents[1]
PORTAL = Path('bin/lan_bitable_template_portal')
RUNTIME = tuple(PORTAL / name for name in (
    'risk_inspection.py', 'risk_inspection_templates.py', 'risk_inspection_cloud.py',
    'risk_inspection_routes.py', 'risk_inspection_imports.py', 'module_access.py',
)) + (Path('bin/clipflow_backend/main.py'),)


class RiskUpdateCompatibilityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = ROOT / 'bin/upload_event_module/ui/main_window_patch.py'
        tree = ast.parse(path.read_text(encoding='utf-8-sig'))
        suffixes = next(ast.literal_eval(node.value) for node in tree.body
                        if isinstance(node, ast.Assign) and any(
                            isinstance(target, ast.Name) and target.id == 'RUNTIME_PATCH_DATA_SUFFIXES'
                            for target in node.targets))
        methods = {
            '_apply_patch_worker', '_collect_patch_files', '_parse_deleted_files',
            '_is_runtime_data_patch_path', '_copy_with_retry', '_delete_with_retry',
            '_backup_patch_targets', '_rollback_patch', '_sha256_file', '_update_build_meta',
            '_read_json_file', '_read_local_build_meta', '_delete_patch_dir', '_discard_invalid_patch',
            '_load_patch_meta', '_validate_patch_meta',
        }
        cls.installer = isolated_class(path, 'PatchUpdateMixin', methods, dict(
            Path=Path, json=json, os=os, shutil=shutil, tempfile=tempfile, time=time,
            hashlib=hashlib, config=SimpleNamespace(auto_install_dependencies=False),
            FRONTEND_INDEX=FRONTEND_INDEX, patch_deletions=patch_deletions,
            RUNTIME_PATCH_DATA_SUFFIXES=suffixes, log_error=lambda *_: None,
            log_warning=lambda *_: None, log_info=lambda *_: None,
        ))

    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix='risk_update_compat_')
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        self.source, self.client, self.output = (self.root / name for name in ('source', 'client', 'build'))
        self.client.mkdir()
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('network forbidden')))
        self.enterContext(patch('upload_event_module.services.process_lifetime.lower_current_thread_priority'))
        for relative in RUNTIME:
            destination = self.source / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / relative, destination)
            old = self.client / relative
            old.parent.mkdir(parents=True, exist_ok=True)
            if relative.name != 'risk_inspection_imports.py':
                old.write_text('# old installed runtime\n', encoding='utf-8')
        (self.client / 'bin/build_meta.json').write_text(json.dumps({
            'build_id': 'old_fixture', 'major_version': 2, 'patch_version': 1,
        }), encoding='utf-8')

    def build(self):
        with patch.object(packaging, 'PROJECT_ROOT', self.source), \
                patch.object(packaging, 'BUILD_DIR', self.output):
            packaging.build_patch(self.root / 'unused', self.client, 'risk_fixture',
                                  target_patch_version=2, ui_changed=True, force_ui_update=True)
        return self.output / 'risk_fixture_patch_only'

    def updater(self, payload):
        instance = self.installer()
        instance._get_app_root_dir = lambda: self.client
        instance._last_patch_meta = json.loads((payload / 'bin/patch_meta.json').read_text(encoding='utf-8'))
        instance._last_patch_source = 'isolated-test'
        instance.results = []
        instance.patch_update_finished = SimpleNamespace(emit=lambda *args: instance.results.append(args))
        return instance

    def remote_payload(self, payload, *, omit=None, corrupt_transport=False):
        from openclaw_service.assistant import lighthouse_distribution as distribution
        from upload_event_module.services import patch_mirror
        archive = io.BytesIO()
        with ZipFile(archive, 'w', ZIP_DEFLATED) as zipped:
            for path in payload.rglob('*'):
                relative = path.relative_to(payload)
                if path.is_file() and relative != omit:
                    zipped.write(path, payload.name + '/' + relative.as_posix())
        content = archive.getvalue()
        digest = hashlib.sha256(content).hexdigest()
        manifest = {'target_version': 'risk_fixture', 'major_version': 2, 'target_patch_version': 2,
                    'ui_changed': True, 'restart_required': True,
                    'zip_name': 'risk_fixture.zip', 'zip_size': len(content), 'zip_sha256': digest}
        manifest['feishu_patch'] = {
            'format': 1, 'component': 'portal_patch', 'version': manifest['target_version'],
            'platform': 'windows_amd64', 'base': distribution.BASE, 'table': distribution.PATCH_TABLE,
            'sha256': digest, 'size': len(content), 'parts': [{
                'index': 1, 'size': len(content), 'sha256': digest,
                'record_id': 'fixture_record', 'file_token': 'fixture_file',
            }],
        }
        received = content + b'corrupt' if corrupt_transport else content
        mirror = SimpleNamespace(download_part=lambda part, target: target.write_bytes(received), close=lambda: None)
        download = patch_mirror.download_patch
        with patch.object(patch_mirror, 'download_patch', side_effect=lambda value, target:
                          download(value, target, mirror_factory=lambda **kwargs: mirror)), \
                patch.object(patch_mirror.time, 'sleep'):
            return RemotePatchUpdater(self.client, self.client / 'bin/data/remote_patch', '').prepare_remote_patch(manifest)

    def installed_portal(self):
        name = '_risk_update_' + self.root.name
        package = ModuleType(name)
        package.__path__ = [str(self.client / PORTAL)]
        sys.modules[name] = package
        def cleanup():
            for key in list(sys.modules):
                if key == name or key.startswith(name + '.'):
                    del sys.modules[key]
        self.addCleanup(cleanup)
        return importlib.import_module(name + '.risk_inspection'), name

    def legacy_client_snapshot(self):
        data = self.client / 'bin/data'
        data.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(data / 'state.sqlite3')
        self.addCleanup(connection.close)
        connection.execute('PRAGMA journal_mode=WAL')
        connection.execute('CREATE TABLE fixture_state(data TEXT NOT NULL)')
        connection.execute('INSERT INTO fixture_state VALUES(?)', (json.dumps({
            'pending': 'old-pending', 'running': 'old-running', 'message_id': 'om_fixture',
        }),))
        connection.commit()
        (data / 'risk_inspection_files').mkdir()
        (data / 'risk_inspection_files/evidence').write_bytes(b'old-evidence')
        (data / 'config.json').write_bytes(b'{"fixture":"old-settings"}')
        return {path.relative_to(self.client): path.read_bytes()
                for path in self.client.rglob('*') if path.is_file()}

    def test_real_runtime_and_both_frontends_install_without_touching_legacy_state(self):
        assets = referenced_assets(ROOT)
        entries = {FRONTEND_INDEX, FRONTEND_INDEX.with_name('assistant.html'), V2_INDEX, V2_MANIFEST}
        for relative in assets | entries:
            destination = self.source / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / relative, destination)
        for version, predicate in (
            ('legacy', lambda relative: not relative.is_relative_to(V2_INDEX.parent)),
            ('v2', lambda relative: relative.is_relative_to(V2_INDEX.parent)),
        ):
            with self.subTest(version=version):
                components = [relative for relative in assets if relative.suffix == '.js'
                              and predicate(relative)
                              and '__name:"RiskInspectionPage"' in (ROOT / relative).read_text(encoding='utf-8')]
                self.assertTrue(components, version + ' production Risk component is missing')

        from bin.lan_bitable_template_portal.risk_inspection import RiskInspections
        database = self.client / 'bin/data/state.sqlite3'
        RiskInspections(database, lambda _: None)
        connection = sqlite3.connect(database)
        self.addCleanup(connection.close)
        connection.execute('PRAGMA journal_mode=WAL')
        release = {'id': 'a' * 32, 'status': 'active', 'reviewer': {'id': 'legacy-reviewer'},
                   'cloud': {'table': 'fixture-table', 'messages': {
                       'old-notice': {'message_id': 'om_fixture', 'attempted_at': 1, 'sent_at': 2}}}}
        person = {'id': 'legacy-person', 'scope': 'B', 'items': [
            {'id': 'old-pending', 'status': 'pending', 'values': {}},
            {'id': 'old-submitted', 'status': 'submitted', 'values': {'result': 'old answer'}}]}
        connection.execute('INSERT INTO risk_releases VALUES(?,?,?,?)',
                           (release['id'], 'legacy-owner', 1, json.dumps(release)))
        connection.execute('INSERT INTO risk_people VALUES(?,?,?,?)',
                           (release['id'], person['id'], 'B', json.dumps(person)))
        for identity, status in (('old-queued', 'queued'), ('old-running', 'running'), ('old-notice', 'done')):
            connection.execute('INSERT INTO risk_jobs VALUES(?,?,?,?,?,?,?,?,?,?)', (
                identity, release['id'], 'notify', '{"fixture":"old"}', status, 3, 12, 34, '', 1))
        connection.execute('INSERT INTO risk_files VALUES(?,?,?,?,?,?,?,?)',
                           ('f' * 32, release['id'], person['id'], 'old-submitted', 'photo', 'evidence.png', 8, 1))
        connection.commit()
        protected = {
            Path('bin/data/state.sqlite3'), Path('bin/data/state.sqlite3-wal'),
            Path('bin/data/state.sqlite3-shm'), Path('bin/data/risk_inspection_files') / ('f' * 32),
            Path('bin/data/config.json'),
        }
        (self.client / 'bin/data/risk_inspection_files').mkdir()
        (self.client / 'bin/data/risk_inspection_files' / ('f' * 32)).write_bytes(b'evidence')
        (self.client / 'bin/data/config.json').write_bytes(b'{"fixture":"local-settings"}')
        before = {relative: (self.client / relative).read_bytes() for relative in protected}
        for relative in protected:
            candidate = self.source / relative
            candidate.parent.mkdir(parents=True, exist_ok=True)
            candidate.write_bytes(b'must-not-ship')
        payload = self.build()
        self.assertFalse((payload / 'bin/data').exists())
        manifest = (payload / 'patch_manifest.txt').read_text(encoding='utf-8')
        self.assertFalse(any(str(relative) in manifest for relative in protected))
        for relative in protected:
            candidate = payload / relative
            candidate.parent.mkdir(parents=True, exist_ok=True)
            candidate.write_bytes(b'must-not-install')
        with (payload / 'patch_manifest.txt').open('a', encoding='utf-8') as stream:
            stream.write('\n' + '\n'.join(str(relative) for relative in protected) + '\n')
        metadata = payload / 'bin/patch_meta.json'
        meta = json.loads(metadata.read_text(encoding='utf-8'))
        meta['file_sha256']['patch_manifest.txt'] = hashlib.sha256((payload / 'patch_manifest.txt').read_bytes()).hexdigest()
        metadata.write_text(json.dumps(meta), encoding='utf-8')
        payload = self.remote_payload(payload)
        updater = self.updater(payload)
        updater._apply_patch_worker(payload)
        self.assertEqual(updater.results[-1][0], True, updater.results)
        self.assertTrue(updater._last_patch_requires_restart)
        self.assertFalse(payload.exists())
        self.assertEqual({relative: (self.client / relative).read_bytes() for relative in protected}, before)
        for relative in set(RUNTIME) | assets | entries:
            with self.subTest(path=relative.as_posix()):
                self.assertEqual((self.client / relative).read_bytes(), (self.source / relative).read_bytes())
        self.assertEqual(json.loads((self.client / 'bin/build_meta.json').read_text())['patch_version'], 2)
        connection.close()
        installed, package_name = self.installed_portal()
        reopened = installed.RiskInspections(database, lambda _: None)
        self.assertEqual(reopened.raw(release['id']), (release, [person]))
        with reopened.connect() as db:
            self.assertEqual([tuple(row) for row in db.execute(
                'SELECT id,status,attempts,next_at,lease FROM risk_jobs ORDER BY id')], [
                    ('old-notice', 'done', 3, 12, 34), ('old-queued', 'queued', 3, 12, 34),
                    ('old-running', 'running', 3, 12, 34)])
            self.assertEqual(db.execute('SELECT COUNT(*) FROM risk_files').fetchone()[0], 1)
        imported = importlib.import_module(package_name + '.risk_inspection_imports')
        self.assertTrue(callable(imported.preview_file))

    def test_copy_failure_restores_runtime_and_keeps_database_and_evidence(self):
        before = self.legacy_client_snapshot()
        payload = self.build()
        updater = self.updater(payload)
        copy = updater._copy_with_retry
        with patch.object(updater, '_copy_with_retry', side_effect=lambda src, dst:
                          False if src == payload / PORTAL / 'risk_inspection_imports.py' else copy(src, dst)):
            updater._apply_patch_worker(payload)
        self.assertFalse(updater.results[-1][0], updater.results)
        for relative, content in before.items():
            self.assertEqual((self.client / relative).read_bytes(), content)
        self.assertFalse((self.client / PORTAL / 'risk_inspection_imports.py').exists())

    def test_remote_transport_corruption_is_rejected_before_extract(self):
        payload = self.build()
        with self.assertRaises(ValueError):
            self.remote_payload(payload, corrupt_transport=True)
        self.assertFalse((self.client / payload.name).exists())
        self.assertFalse((self.client / PORTAL / 'risk_inspection_imports.py').exists())

    def test_missing_declared_import_module_must_reject_update(self):
        before = self.legacy_client_snapshot()
        payload = self.build()
        payload = self.remote_payload(payload, omit=PORTAL / 'risk_inspection_imports.py')
        updater = self.updater(payload)
        valid, _, _ = updater._validate_patch_meta(payload, self.client)
        self.assertTrue(valid)
        with patch('openclaw_service.update.AssistantUpdateGuard',
                   side_effect=AssertionError('incomplete patch must be rejected before creating the guard')) as guard:
            updater._apply_patch_worker(payload)
        guard.assert_not_called()
        self.assertEqual([result[0] for result in updater.results], [False], updater.results)
        self.assertIn('risk_inspection_imports.py', updater.results[0][1])
        for relative, content in before.items():
            with self.subTest(path=relative.as_posix()):
                self.assertEqual((self.client / relative).read_bytes(), content)
        self.assertFalse((self.client / PORTAL / 'risk_inspection_imports.py').exists())
        self.assertFalse((self.client / 'bin/.patch_backup').exists())

    def test_build_metadata_write_failure_must_not_report_success(self):
        before = self.legacy_client_snapshot()
        payload = self.remote_payload(self.build())
        updater = self.updater(payload)
        valid, _, _ = updater._validate_patch_meta(payload, self.client)
        self.assertTrue(valid)
        replace, rejected = os.replace, []
        target = self.client / 'bin/build_meta.json'
        def fail_metadata(source, destination):
            destination = Path(destination)
            self.assertFalse(destination.is_relative_to(self.client / 'bin/data'))
            if destination == target and not rejected:
                rejected.append(Path(source))
                raise PermissionError('isolated read-only version record')
            return replace(source, destination)
        with patch.object(os, 'replace', fail_metadata):
            updater._apply_patch_worker(payload)
        self.assertEqual(len(rejected), 1, 'metadata replacement failure was not injected')
        self.assertEqual([result[0] for result in updater.results], [False], updater.results)
        self.assertIn('isolated read-only version record', updater.results[0][1])
        for relative, content in before.items():
            with self.subTest(path=relative.as_posix()):
                self.assertEqual((self.client / relative).read_bytes(), content)
        self.assertFalse((self.client / PORTAL / 'risk_inspection_imports.py').exists())
        self.assertTrue(payload.is_dir())
        backups = list((self.client / 'bin/.patch_backup').iterdir())
        self.assertEqual(len(backups), 1)
        for relative, content in before.items():
            if not relative.is_relative_to(Path('bin/data')):
                self.assertEqual((backups[0] / relative).read_bytes(), content)
        self.assertFalse((backups[0] / 'bin/data').exists())
        self.assertEqual(list((self.client / 'bin').glob('.build-meta-*')), [])


if __name__ == '__main__':
    unittest.main()

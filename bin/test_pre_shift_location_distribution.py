"""Automatic map configuration with fake Feishu storage and isolated Windows keys."""
import asyncio
import copy
import hashlib
import inspect
import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal import pre_shift_location_distribution as distribution
from lan_bitable_template_portal.pre_shift_location import PhotoLocation
from openclaw_service.assistant.lighthouse_ai import unprotect_key
from openclaw_service.assistant import lighthouse_distribution as mirror_module


class MemoryMirror:
    def __init__(self):
        self.files = {}
        self.uploads = 0
        self.downloads = 0
        self.fail = False
        self.corrupt = False
        self.before_download = lambda: None

    def publish(self, spec, folder, journal, *, component):
        self.assert_component(component)
        state = json.loads(journal.read_bytes()) if journal.exists() else {}
        for part in spec['parts']:
            identity = spec['sha256'] + ':1'
            if identity not in state:
                self.uploads += 1
                state[identity] = {'record_id': 'rec' + str(self.uploads), 'file_token': 'file' + str(self.uploads)}
                self.files[state[identity]['file_token']] = (folder / part['name']).read_bytes()
                journal.write_text(json.dumps(state))
            part.update(state[identity])
        return spec

    def assert_component(self, component):
        if component != 'photo_location_profile':
            raise AssertionError('Wrong component')

    def download_part(self, part, target):
        self.downloads += 1
        self.before_download()
        if self.fail:
            raise OSError('fixture: unavailable')
        target.write_bytes(b'corrupt' if self.corrupt else self.files[part['file_token']])

    def close(self):
        pass


class LocationDistributionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = PhotoLocation(self.root / 'publisher/location_settings.json')
        self.source.save({'key': '0'*32, 'revision': self.source.settings()['revision']})
        self.target = PhotoLocation(self.root / 'client/location_settings.json')
        self.addCleanup(self.source.close)
        self.addCleanup(self.target.close)
        self.spec_path = self.root / 'photo-location-distribution.json'
        self.mirror = MemoryMirror()
        self.factory = lambda: self.mirror

    def publish(self):
        return distribution.publish_location_config(self.source.path, spec_path=self.spec_path, mirror_factory=self.factory)

    def provision(self):
        return distribution.provision_location(self.target, spec_path=self.spec_path, mirror_factory=self.factory)

    def key(self):
        return unprotect_key(self.target._read()[0]['key_cipher'])

    def test_publish_then_first_restart_auto_configures_and_cached_restart_never_downloads(self):
        spec = self.publish()
        self.assertEqual(spec, distribution.read_spec(self.spec_path))
        self.assertNotIn('0'*32, self.spec_path.read_text())
        self.assertTrue(self.provision())
        self.assertEqual(self.key(), '0'*32)
        self.assertNotIn('0'*32, self.target.path.read_text())
        self.assertEqual(self.target._read()[0]['distribution_sha256'], spec['sha256'])
        self.assertEqual(self.mirror.downloads, 2)
        self.assertTrue(self.provision())
        self.assertEqual(self.mirror.downloads, 2)
        restarted = PhotoLocation(self.target.path)
        self.addCleanup(restarted.close)
        with patch.object(distribution, 'FeishuRuntimeMirror') as forbidden:
            self.assertTrue(distribution.provision_location(restarted, spec_path=self.spec_path))
            forbidden.assert_not_called()
        self.assertFalse(list(self.root.rglob('credential.json')))
        self.assertFalse(list(self.root.rglob('verified.json')))
        self.assertFalse(list(self.root.rglob('clipflow-photo-location.json')))

    def test_upload_retry_and_missing_journal_reuse_cloud_record(self):
        first = self.publish()
        self.assertEqual(self.publish(), first)
        journal = self.source.path.parent / 'location_bootstrap/upload-journal.json'
        journal.unlink()
        self.assertEqual(self.publish(), first)
        self.assertEqual(self.mirror.uploads, 1)
        old = self.spec_path.read_bytes()
        self.source.save({'key': '1'*32, 'revision': self.source.settings()['revision']})
        self.mirror.fail = True
        with self.assertRaises(OSError):
            self.publish()
        self.assertEqual(self.spec_path.read_bytes(), old, 'failed verification must not replace a good descriptor')
        self.mirror.fail = False
        second = self.publish()
        self.assertNotEqual(second['sha256'], first['sha256'])
        self.assertEqual(self.mirror.uploads, 2, 'retry reuses the uploaded attachment and record')
        self.source.path.unlink()
        self.assertEqual(self.publish(), second, 'a new publisher can verify the existing descriptor')
        self.assertEqual(self.mirror.uploads, 2)

    def test_new_config_version_rotates_auto_key_but_keeps_manual_override(self):
        self.publish(); self.provision()
        self.source.save({'key': '1'*32, 'revision': self.source.settings()['revision']})
        self.publish(); self.assertTrue(self.provision()); self.assertEqual(self.key(), '1'*32)
        self.target.save({'key': '2'*32, 'revision': self.target.settings()['revision']})
        before = self.mirror.downloads
        self.assertTrue(self.provision()); self.assertEqual(self.key(), '2'*32)
        self.assertEqual(self.mirror.downloads, before, 'explicit local settings are never replaced silently')

    def test_download_failures_preserve_existing_key_and_later_retry_recovers(self):
        self.publish(); self.provision()
        self.source.save({'key': '1'*32, 'revision': self.source.settings()['revision']})
        self.publish()
        previous = self.target.path.read_bytes()
        self.mirror.fail = True
        with self.assertRaises(OSError):
            self.provision()
        self.assertEqual(self.target.path.read_bytes(), previous)
        self.mirror.fail = False; self.mirror.corrupt = True
        with self.assertRaises(ValueError):
            self.provision()
        self.assertEqual(self.target.path.read_bytes(), previous)
        self.mirror.corrupt = False
        self.assertTrue(self.provision()); self.assertEqual(self.key(), '1'*32)
        for broken in ('{"key_cipher":"wrong-machine-cipher"}', '{broken'):
            self.target.path.write_text(broken)
            self.assertTrue(self.provision()); self.assertEqual(self.key(), '1'*32)

    def test_manual_save_or_shutdown_during_download_prevents_late_overwrite(self):
        self.publish()
        def manual_save():
            self.target.save({'key': '2'*32, 'revision': self.target.settings()['revision']})
        self.mirror.before_download = manual_save
        self.assertFalse(self.provision()); self.assertEqual(self.key(), '2'*32)
        self.target.path.unlink()
        self.mirror.before_download = self.target.close
        self.assertFalse(self.provision()); self.assertFalse(self.target.path.exists())

    def test_invalid_descriptor_missing_descriptor_and_isolation_never_use_cloud(self):
        with patch.object(distribution, 'FeishuRuntimeMirror') as forbidden:
            self.assertTrue(distribution.provision_location(self.target, spec_path=self.spec_path))
            forbidden.assert_not_called()
        spec = self.publish()
        for mutate in (lambda s: s.update(table='other'), lambda s: s.update(key='0'*32),
                       lambda s: s['parts'][0].update(file_token='../unsafe'),
                       lambda s: s['parts'][0].update(sha256='f'*64),
                       lambda s: s['parts'][0].update(name='../key.json')):
            candidate = copy.deepcopy(spec); mutate(candidate)
            self.spec_path.write_text(json.dumps(candidate))
            with patch.object(distribution, 'FeishuRuntimeMirror') as forbidden:
                with self.assertRaises(Exception):
                    distribution.provision_location(self.target, spec_path=self.spec_path)
                forbidden.assert_not_called()
            self.assertFalse(self.target.path.exists())
        self.spec_path.write_text(json.dumps(spec))
        with patch.dict('os.environ', {'CLIPFLOW_BACKEND_MOCK_EXTERNAL': '1'}), patch.object(distribution, 'FeishuRuntimeMirror') as forbidden:
            self.assertTrue(distribution.provision_location(self.target, spec_path=self.spec_path))
            with self.assertRaisesRegex(Exception, '禁止发布'):
                distribution.publish_location_config(self.source.path, spec_path=self.spec_path)
            forbidden.assert_not_called()

    def test_real_mirror_labels_key_as_restricted_attachment_without_plain_record_fields(self):
        values = {}; client = Mock()
        def request(method, url, **kwargs):
            if url.endswith('/fields'):
                return {'code': 0, 'data': {'items': [{'field_name': name, 'type': kind} for name, kind in mirror_module.FIELDS.items()]}}
            if method == 'POST' and url.endswith('/records'):
                values.update(kwargs['json_payload']['fields'])
                return {'code': 0, 'data': {'record': {'record_id': 'recFixture'}}}
            return {'code': 0, 'data': {'record': {'fields': values}}}
        client.request_json.side_effect = request
        client.request_file_json.return_value = {'code': 0, 'data': {'file_token': 'fileFixture'}}
        mirror = mirror_module.FeishuRuntimeMirror(client=client, headers={'Authorization': 'Bearer fixture'}, timeout=8)
        with tempfile.TemporaryDirectory() as folder:
            folder = Path(folder); content = b'fixture-credential'; digest = hashlib.sha256(content).hexdigest()
            (folder / 'credential.json').write_bytes(content)
            spec = {'format': 1, 'component': distribution.COMPONENT, 'version': digest, 'sha256': digest,
                    'platform': 'windows_amd64', 'base': mirror_module.BASE, 'table': mirror_module.TABLE,
                    'size': len(content), 'parts': [{'index': 1, 'name': 'credential.json', 'size': len(content), 'sha256': digest}]}
            mirror.publish(spec, folder, folder / 'journal.json', component=distribution.COMPONENT)
            self.assertEqual(values['组件名称'], '灯塔照片地点解析配置（受限）')
            self.assertIn('限制表格及附件读取权限', values['更新说明'])
            self.assertEqual(values['安装包'], [{'file_token': 'fileFixture'}])
            self.assertNotIn(content.decode(), json.dumps(values))
        mirror.close()

    def test_packager_uploads_before_copy_and_skip_upload_stays_offline(self):
        import package_portable as package
        with patch.object(package.subprocess, 'run') as run:
            package._ensure_photo_location_distribution('selected-python')
            args = run.call_args.args[0]
            self.assertEqual(args[:2], ['selected-python', '-c'])
            self.assertIn('publish_location_config()', args[2])
            self.assertTrue(run.call_args.kwargs['check'])
            run.reset_mock()
            package._ensure_photo_location_distribution('selected-python', publish=False)
            run.assert_not_called()
        main = inspect.getsource(package.main)
        self.assertLess(main.index('_ensure_photo_location_distribution('), main.index('copy_project('))
        self.assertLess(main.index('_ensure_photo_location_distribution('), main.index('build_patch('))
        self.assertIn('publish=not args.skip_upload', main)
        self.assertNotIn('_ensure_photo_location_distribution', inspect.getsource(package._run_packaging_preflight_tests))
        self.assertNotIn('_ensure_photo_location_distribution', inspect.getsource(package._run_packaging_preflight_tests_once))
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); meta = root / 'bin/lan_bitable_template_portal/photo-location-distribution.json'
            meta.parent.mkdir(parents=True); meta.write_text('{}')
            private = root / 'bin/data/pre_shift_checks/location_settings.json'
            private.parent.mkdir(parents=True); private.write_text('private fixture')
            files = {p.relative_to(root) for p in package._iter_project_files(root, exclude_venv=True)}
            self.assertIn(meta.relative_to(root), files)
            self.assertNotIn(private.relative_to(root), files)


class LocationBootstrapRouteTests(unittest.TestCase):
    def test_initialization_failure_does_not_escape_shutdown_or_block_startup(self):
        from fastapi import FastAPI
        from lan_bitable_template_portal import pre_shift_routes
        service = Mock()
        runtime = SimpleNamespace(service=Mock()); app = FastAPI()
        with patch.object(pre_shift_routes, 'CheckService', return_value=service):
            pre_shift_routes.install_pre_shift_routes(app, SimpleNamespace(), runtime)
            async def exercise():
                with self.assertLogs(level='WARNING'):
                    await asyncio.wait_for(app.router.startup(), timeout=1)
                    await asyncio.sleep(.05)
                    await asyncio.wait_for(app.router.shutdown(), timeout=1)
            asyncio.run(exercise())
        service.start.assert_called_once(); service.stop.assert_called_once()

    def test_network_stall_is_background_only_and_shutdown_prevents_late_save(self):
        from fastapi import FastAPI
        from lan_bitable_template_portal import pre_shift_routes
        with tempfile.TemporaryDirectory() as folder:
            service = Mock(db=Path(folder) / 'checks.sqlite3')
            runtime = SimpleNamespace(service=Mock()); app = FastAPI()
            started, release, ended = threading.Event(), threading.Event(), threading.Event()
            received, closed_at_return = [], []
            def provision(settings):
                received.append(settings); started.set()
                release.wait(3)
                try:
                    closed_at_return.append(settings.closed)
                    return False
                finally:
                    ended.set()
            with patch.object(pre_shift_routes, 'CheckService', return_value=service), patch.object(
                    distribution, 'provision_location', side_effect=provision):
                pre_shift_routes.install_pre_shift_routes(app, SimpleNamespace(), runtime)
                async def exercise():
                    try:
                        await asyncio.wait_for(app.router.startup(), timeout=1)
                        self.assertTrue(await asyncio.to_thread(started.wait, 2))
                        self.assertFalse(ended.is_set(), 'a pending network read does not hold up startup')
                        await asyncio.wait_for(app.router.shutdown(), timeout=1)
                    finally:
                        release.set()
                    self.assertTrue(await asyncio.to_thread(ended.wait, 2))
                    self.assertTrue(received[0].closed)
                    self.assertEqual(closed_at_return, [True], 'the late worker sees the closed flag')
                asyncio.run(exercise())

    def test_startup_does_not_wait_for_network_and_transient_failure_retries(self):
        from fastapi import FastAPI
        from lan_bitable_template_portal import pre_shift_routes
        with tempfile.TemporaryDirectory() as folder:
            service = Mock(db=Path(folder) / 'checks.sqlite3')
            runtime = SimpleNamespace(service=Mock())
            app = FastAPI(); calls = []; recovered = threading.Event()
            def provision(settings):
                calls.append(settings)
                if len(calls) == 1:
                    raise OSError('fixture: private failure must not be logged')
                recovered.set()
                return True
            with patch.object(pre_shift_routes, 'CheckService', return_value=service), patch.object(
                    distribution, 'provision_location', side_effect=provision), patch.object(pre_shift_routes, '_LOCATION_RETRY_SECONDS', .01):
                pre_shift_routes.install_pre_shift_routes(app, SimpleNamespace(), runtime)
                async def exercise():
                    with self.assertLogs(level='WARNING') as logs:
                        await app.router.startup()
                        self.assertTrue(await asyncio.to_thread(recovered.wait, 2))
                    self.assertEqual(len(calls), 2)
                    self.assertNotIn('private failure', '\n'.join(logs.output))
                    await app.router.shutdown()
                    self.assertTrue(calls[-1].closed)
                asyncio.run(exercise())
            service.start.assert_called_once(); service.stop.assert_called_once()


if __name__ == '__main__':
    unittest.main()

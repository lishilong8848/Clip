"""Patch distribution checks with isolated files and fake Feishu/Gitee transports."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import httpx
import package_portable as packaging
from openclaw_service.assistant import lighthouse_distribution as distribution
from upload_event_module.services import patch_mirror
from upload_event_module.services.remote_patch_updater import RemotePatchUpdater


class PatchMirrorTests(unittest.TestCase):
    def manifest(self, content=b'patch-content'):
        return {'target_version': 'ClipFlow_V2_test', 'target_patch_version': 473,
                'zip_name': 'patch.zip', 'zip_url': 'https://example.invalid/patch.zip',
                'zip_size': len(content), 'zip_sha256': hashlib.sha256(content).hexdigest()}

    def test_publish_download_and_retry_use_only_patch_table_without_duplicate_records(self):
        api = distribution.API.replace(distribution.TABLE, distribution.PATCH_TABLE)
        fields, records, files, calls = {}, {}, {}, []
        client = MagicMock()
        def request(method, url, **kwargs):
            self.assertTrue(url.startswith(api + '/'))
            calls.append((method, url))
            if url.endswith('/fields'):
                if method == 'POST':
                    value = kwargs['json_payload']
                    fields[value['field_name']] = value['type']
                return {'code': 0, 'data': {'items': [{'field_name': k, 'type': v} for k, v in fields.items()]}}
            if url.endswith('/records') and method == 'POST':
                rid = 'rec' + str(len(records))
                records[rid] = {'record_id': rid, 'fields': kwargs['json_payload']['fields']}
                return {'code': 0, 'data': {'record': records[rid]}}
            return {'code': 0, 'data': {'record': records[url.rsplit('/', 1)[-1]]}}
        def upload(method, url, **kwargs):
            self.assertEqual(url, 'https://open.feishu.cn/open-apis/drive/v1/medias/upload_all')
            self.assertEqual(kwargs['data']['parent_node'], distribution.BASE)
            token = 'file' + str(len(files))
            files[token] = Path(kwargs['file_path']).read_bytes()
            return {'code': 0, 'data': {'file_token': token}}
        client.request_json.side_effect, client.request_file_json.side_effect = request, upload
        def factory(**kwargs):
            mirror = distribution.FeishuRuntimeMirror(client=client, headers={'Authorization': 'Bearer fixture'}, **kwargs)
            mirror._download_client = httpx.Client(transport=httpx.MockTransport(
                lambda request: httpx.Response(200, content=files[request.url.path.split('/')[-2]])))
            return mirror
        with tempfile.TemporaryDirectory() as temp, patch.object(patch_mirror, 'PART_BYTES', 4):
            root = Path(temp)
            archive = root / 'patch.zip'
            archive.write_bytes(b'patch-content')
            manifest = self.manifest()
            journal = root / 'journal.json'
            spec = patch_mirror.publish_patch(archive, manifest, journal, mirror_factory=factory)
            self.assertEqual(len(spec['parts']), 4)
            self.assertEqual(spec['table'], distribution.PATCH_TABLE)
            self.assertEqual(fields, distribution.FIELDS)
            manifest['feishu_patch'] = spec
            patch_mirror.publish_patch(archive, manifest, journal, mirror_factory=factory)
            journal.unlink()
            patch_mirror.publish_patch(archive, manifest, journal, mirror_factory=factory)
            self.assertEqual(len(files), 4)
            self.assertEqual(len(records), 4)
            self.assertNotIn('DELETE', [method for method, _ in calls])
            target = patch_mirror.download_patch(manifest, root / 'download.zip', mirror_factory=factory)
            self.assertEqual(target.read_bytes(), archive.read_bytes())
            self.assertFalse(list(root.glob('.feishu-patch-*')))
            self.assertFalse(list(root.glob('.publish-feishu-patch-*')))

    def test_untrusted_spec_never_constructs_mirror(self):
        manifest = self.manifest(b'patch')
        spec = {'format': 1, 'component': 'portal_patch', 'version': manifest['target_version'],
                'platform': 'windows_amd64', 'base': distribution.BASE, 'table': distribution.PATCH_TABLE,
                'size': 5, 'sha256': manifest['zip_sha256'], 'parts': [
                    {'index': 1, 'size': 5, 'sha256': manifest['zip_sha256'], 'record_id': 'rec1', 'file_token': 'file1'}]}
        for key, value in (('table', distribution.TABLE), ('base', 'other'), ('sha256', '0' * 64), ('version', 'other')):
            candidate = copy.deepcopy(spec)
            candidate[key] = value
            with self.subTest(key=key), patch.object(patch_mirror, 'FeishuRuntimeMirror') as factory:
                with self.assertRaises(ValueError):
                    patch_mirror.download_patch({**manifest, 'feishu_patch': candidate}, Path('unused'), mirror_factory=factory)
                factory.assert_not_called()
        with self.assertRaises(ValueError):
            distribution.FeishuRuntimeMirror(table='arbitrary-table', client=MagicMock())

    def test_corrupt_mirror_download_cleans_staging_and_preserves_previous_zip(self):
        manifest = self.manifest(b'patch')
        manifest['feishu_patch'] = {'format': 1, 'component': 'portal_patch', 'version': manifest['target_version'],
            'platform': 'windows_amd64', 'base': distribution.BASE, 'table': distribution.PATCH_TABLE,
            'size': 5, 'sha256': manifest['zip_sha256'], 'parts': [
                {'index': 1, 'size': 5, 'sha256': manifest['zip_sha256'], 'record_id': 'rec1', 'file_token': 'file1'}]}
        mirror = MagicMock()
        mirror.download_part.side_effect = lambda part, target: target.write_bytes(b'wrong')
        with tempfile.TemporaryDirectory() as temp, patch('time.sleep'):
            target = Path(temp) / 'patch.zip'
            target.write_bytes(b'previous')
            with self.assertRaises(ValueError):
                patch_mirror.download_patch(manifest, target, mirror_factory=lambda **kwargs: mirror)
            self.assertEqual(target.read_bytes(), b'previous')
            self.assertEqual(mirror.download_part.call_count, 3)
            self.assertEqual(list(Path(temp).glob('.feishu-patch-*')), [])
            mirror.close.assert_called_once()

    def test_updater_prefers_feishu_and_falls_back_to_original_url(self):
        with tempfile.TemporaryDirectory() as temp:
            updater = RemotePatchUpdater(Path(temp), Path(temp) / 'data', '')
            manifest = {**self.manifest(b'patch'), 'feishu_patch': {'fixture': True}}
            target = updater.data_dir / 'patch.zip'
            def download(manifest, target):
                target.write_bytes(b'patch')
                return target
            with patch.object(patch_mirror, 'download_patch', side_effect=download), patch('requests.get') as get:
                self.assertEqual(updater._download_zip(manifest), target.resolve())
                get.assert_not_called()
            response = MagicMock(status_code=200)
            response.__enter__.return_value = response
            response.iter_content.return_value = iter([b'patch'])
            with patch.object(patch_mirror, 'download_patch', side_effect=OSError('fixture unavailable')), \
                    patch('requests.get', return_value=response) as get:
                self.assertEqual(updater._download_zip(manifest).read_bytes(), b'patch')
                self.assertEqual(get.call_args.args[0], manifest['zip_url'])

    def test_packaging_publishes_verified_mirror_pointer_before_gitee_manifest(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            archive, manifest_path = root / 'patch.zip', root / 'latest_patch.json'
            archive.write_bytes(b'patch')
            manifest = self.manifest(b'patch')
            manifest_path.write_text(json.dumps(manifest))
            sequence = []
            def mirror(*args):
                sequence.append('feishu')
                return {'fixture': 'verified'}
            def gitee(*args, **kwargs):
                self.assertEqual(json.loads(manifest_path.read_text())['feishu_patch'], {'fixture': 'verified'})
                sequence.append('gitee')
                return True
            with patch.object(packaging, 'BUILD_DIR', root), \
                    patch.object(patch_mirror, 'publish_patch', side_effect=mirror), \
                    patch.object(packaging, '_upload_patch_to_gitee', side_effect=gitee), \
                    patch.object(packaging, '_verify_published_patch', side_effect=lambda *a, **k: sequence.append('verify')):
                packaging._publish_patch(archive, manifest_path, manifest, repo_url='https://example.invalid/repo.git',
                                         branch='master', subdir='', manifest_repo_path='latest.json')
            self.assertEqual(sequence, ['feishu', 'gitee', 'verify'])
            with patch.object(patch_mirror, 'publish_patch', side_effect=OSError('fixture upload failed')), \
                    patch.object(packaging, '_upload_patch_to_gitee') as push:
                with self.assertRaises(OSError):
                    packaging._publish_patch(archive, manifest_path, manifest, repo_url='https://example.invalid/repo.git',
                                             branch='master', subdir='', manifest_repo_path='latest.json')
                push.assert_not_called()


if __name__ == '__main__':
    unittest.main()

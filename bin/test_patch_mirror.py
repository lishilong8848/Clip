"""Patch distribution checks with isolated files and fake Feishu transports."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
import uuid
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import httpx
import package_portable as packaging
from openclaw_service.assistant import lighthouse_distribution as distribution
from upload_event_module import logger as _app_logger  # Initialize before assertLogs can capture root handlers.
from upload_event_module.services import patch_mirror
from upload_event_module.services.remote_patch_updater import RemotePatchUpdater


class _RecordFeishuClient:
    """Tiny fake responding to the record GET that download_part performs."""

    def __init__(self, token, *, record_id='rec1'):
        self.token = token
        self.record_id = record_id
        self.closed = False
        self.request_calls = []

    def request_json(self, method, url, headers=None, **kwargs):
        self.request_calls.append((method, url))
        if method == 'GET' and url.endswith('/records/' + self.record_id):
            return {'code': 0, 'data': {'record': {
                'record_id': self.record_id,
                'fields': {'启用': True, '安装包': [{'file_token': self.token}]}}}}
        raise AssertionError('unexpected request_json: %s %s' % (method, url))

    def close(self):
        self.closed = True


class _HttpxStreamCtx:
    def __init__(self, response):
        self.response = response

    def __enter__(self):
        return self.response

    def __exit__(self, *exc_info):
        return False


class _FakeHttpClient:
    """Recording fake httpx client. ``stream`` pops queued responses; an
    ``Exception`` entry is raised on the matching ``stream`` call."""

    def __init__(self, responses=()):
        self.responses = list(responses)
        self.sent = []
        self.closed = False

    def stream(self, method, url, headers=None, **kwargs):
        self.sent.append((url, headers))
        if not self.responses:
            raise httpx.ReadTimeout('fixture: no more network responses', request=None)
        value = self.responses.pop(0)
        if isinstance(value, Exception):
            raise value
        return _HttpxStreamCtx(value)

    def close(self):
        self.closed = True


def _stream_response(chunks, *, error=None):
    """MagicMock response whose ``iter_bytes`` yields ``chunks`` then raises ``error``."""
    response = MagicMock(status_code=200, headers={})

    def iterator():
        for chunk in list(chunks):
            yield chunk
        if error is not None:
            raise error

    response.iter_bytes.return_value = iterator()
    return response


class PatchMirrorTests(unittest.TestCase):
    def manifest(self, content=b'patch-content'):
        return {'target_version': 'ClipFlow_V2_test', 'major_version': 2, 'target_patch_version': 473,
                'zip_name': 'patch.zip', 'zip_url': 'https://example.invalid/patch.zip',
                'zip_size': len(content), 'zip_sha256': hashlib.sha256(content).hexdigest()}

    def _patch_spec(self, content, parts=None):
        manifest = self.manifest(content)
        if parts is None:
            parts = [{'index': 1, 'size': len(content), 'sha256': hashlib.sha256(content).hexdigest(),
                      'record_id': 'rec1', 'file_token': 'file1'}]
        return {'format': 1, 'component': 'portal_patch', 'version': manifest['target_version'],
                'platform': 'windows_amd64', 'base': distribution.BASE, 'table': distribution.PATCH_TABLE,
                'sha256': manifest['zip_sha256'], 'size': len(content), 'parts': parts}

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
            if url.endswith('/records/search'):
                return {'code': 0, 'data': {'has_more': False, 'items': [value for value in records.values()
                    if value['fields']['组件名称'] == patch_mirror.RELEASE_COMPONENT]}}
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
            manifest.pop('zip_url')
            manifest['update_source'] = 'feishu'
            patch_mirror.publish_manifest(manifest, mirror_factory=factory)
            patch_mirror.publish_manifest(manifest, mirror_factory=factory)
            self.assertEqual(len(records), 5)
            self.assertEqual(patch_mirror.fetch_manifest(mirror_factory=factory), manifest)
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
        with tempfile.TemporaryDirectory() as temp, patch('time.sleep'), self.assertLogs(level='INFO') as logs:
            target = Path(temp) / 'patch.zip'
            target.write_bytes(b'previous')
            with self.assertRaises(ValueError):
                patch_mirror.download_patch(manifest, target, mirror_factory=lambda **kwargs: mirror)
            self.assertEqual(target.read_bytes(), b'previous')
            self.assertEqual(mirror.download_part.call_count, 3)
            self.assertEqual(list(Path(temp).glob('.feishu-patch-*')), [])
            mirror.close.assert_called_once()
        combined = '\n'.join(logs.output)
        self.assertNotIn('飞书补丁整包大小及 SHA256 校验通过', combined)

    def test_updater_only_downloads_feishu_even_when_old_url_exists(self):
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
            with patch.object(patch_mirror, 'download_patch', side_effect=OSError('fixture unavailable')), patch('requests.get') as get:
                with self.assertRaises(OSError):
                    updater._download_zip(manifest)
                self.assertEqual(target.read_bytes(), b'patch')
                get.assert_not_called()
            with patch('requests.get') as get:
                with self.assertRaises(Exception):
                    updater._download_zip(self.manifest())
                get.assert_not_called()

    def test_packaging_publishes_feishu_manifest_only_after_archive_verification(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            archive, manifest_path = root / 'patch.zip', root / 'latest_patch.json'
            archive.write_bytes(b'x' * 11_000_000)
            manifest = self.manifest(archive.read_bytes())
            manifest_path.write_text(json.dumps(manifest))
            sequence = []
            def mirror(*args):
                sequence.append('verified-archive')
                return {'fixture': 'verified'}
            def publish(value):
                self.assertNotIn('zip_url', value)
                self.assertEqual(value['feishu_patch'], {'fixture': 'verified'})
                self.assertEqual(value['update_source'], 'feishu')
                sequence.append('published-manifest')
            with patch.object(packaging, 'BUILD_DIR', root), patch.object(patch_mirror, 'publish_patch', side_effect=mirror), \
                    patch.object(patch_mirror, 'publish_manifest', side_effect=publish), \
                    patch.object(packaging.subprocess, 'run') as run, patch('requests.get') as get:
                packaging._publish_patch(archive, manifest_path, manifest)
                run.assert_not_called()
                get.assert_not_called()
            self.assertEqual(sequence, ['verified-archive', 'published-manifest'])
            self.assertEqual(json.loads(manifest_path.read_text(encoding='utf-8')), manifest)
            self.assertNotIn('zip_url', manifest)
            self.assertEqual(archive.stat().st_size, 11_000_000)

    def test_publish_download_timeout_preserves_journal_and_retry_does_not_reupload(self):
        api = distribution.API.replace(distribution.TABLE, distribution.PATCH_TABLE)
        fields, records, files, calls = {}, {}, {}, []
        download_failures = {'remaining': 3}
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

        def download_handler(req):
            if download_failures['remaining']:
                download_failures['remaining'] -= 1
                raise httpx.ReadTimeout('fixture download read timeout', request=req)
            return httpx.Response(200, content=files[req.url.path.split('/')[-2]])

        def factory(**kwargs):
            mirror = distribution.FeishuRuntimeMirror(client=client, headers={'Authorization': 'Bearer fixture'}, **kwargs)
            mirror._download_client = httpx.Client(transport=httpx.MockTransport(download_handler))
            return mirror

        with tempfile.TemporaryDirectory() as temp, \
                patch.object(patch_mirror, 'PART_BYTES', 4), \
                patch('time.sleep'):
            root = Path(temp)
            archive = root / 'patch.zip'
            archive.write_bytes(b'patch-content')
            manifest = self.manifest()
            journal = root / 'journal.json'
            with self.assertRaises(httpx.ReadTimeout):
                patch_mirror.publish_patch(archive, manifest, journal, mirror_factory=factory)
            # The upload phase succeeded and journaled the tokens/records before the
            # download reread failed; local archive is untouched.
            state = json.loads(journal.read_text(encoding='utf-8'))
            part_count = 4
            for index in range(1, part_count + 1):
                entry = state[manifest['zip_sha256'] + ':' + str(index)]
                self.assertIn(entry['file_token'], files)
                self.assertIn(entry['record_id'], records)
            self.assertEqual(len(files), part_count)
            self.assertEqual(len(records), part_count)
            self.assertEqual(archive.read_bytes(), b'patch-content')
            self.assertFalse(list(root.glob('.publish-feishu-patch-*')))

            before = (len(files), len(records),
                      [c for c in calls if c[0] == 'POST' and c[1].endswith('/records')],
                      [c for c in calls if c[0] == 'POST' and c[1].endswith('/fields')])
            spec = patch_mirror.publish_patch(archive, manifest, journal, mirror_factory=factory)
            self.assertEqual(len(spec['parts']), part_count)
            self.assertEqual(spec['table'], distribution.PATCH_TABLE)
            after = (len(files), len(records),
                     [c for c in calls if c[0] == 'POST' and c[1].endswith('/records')],
                     [c for c in calls if c[0] == 'POST' and c[1].endswith('/fields')])
            self.assertEqual(before[0], after[0], "re-run must not re-upload attachments")
            self.assertEqual(before[1], after[1], "re-run must not re-create records")
            self.assertEqual(before[2], after[2], "no new record POSTs on rerun")
            self.assertEqual(before[3], after[3], "no new field POSTs on rerun")
            self.assertFalse(list(root.glob('.feishu-patch-*')))
            self.assertFalse(list(root.glob('.publish-feishu-patch-*')))

    def test_stream_partial_timeout_then_success_keeps_clean_zip_and_logs(self):
        content = b'A' * (2 * 1024 * 1024)
        manifest = self.manifest(content)
        manifest['feishu_patch'] = self._patch_spec(content)
        record_client = _RecordFeishuClient('file1')
        http = _FakeHttpClient([
            _stream_response([b'A' * (1024 * 1024)],
                             error=httpx.ReadTimeout('partial read timed out')),
            _stream_response([content]),
        ])
        mirror = distribution.FeishuRuntimeMirror(client=record_client,
                                                  headers={'Authorization': 'Bearer x'},
                                                  table=distribution.PATCH_TABLE)
        mirror._download_client = http
        with tempfile.TemporaryDirectory() as temp, \
                patch('time.sleep'), self.assertLogs(level='INFO') as logs:
            target = Path(temp) / 'patch.zip'
            patch_mirror.download_patch(manifest, target, mirror_factory=lambda **kw: mirror)
            self.assertEqual(target.read_bytes(), content, "output must be the complete zip exactly once")
            self.assertFalse(list(Path(temp).glob('.feishu-patch-*')), "failed attempt must clean its staging file")
        combined = '\n'.join(logs.output)
        self.assertIn('飞书补丁分片下载未完成', combined)
        self.assertIn('1/3', combined)
        self.assertIn('分片 1/1', combined)
        self.assertIn('已接收', combined)
        self.assertIn('1.00/2.00 MiB', combined)
        self.assertIn('ReadTimeout', combined)

    def test_network_failure_preserves_target_cleans_temp_and_closes_client(self):
        content = b'old-patch'
        manifest = self.manifest(content)
        manifest['feishu_patch'] = self._patch_spec(content)
        record_client = _RecordFeishuClient('file1')
        http = _FakeHttpClient([httpx.ReadTimeout('persistent network failure', request=None)] * 3)
        mirror = distribution.FeishuRuntimeMirror(client=record_client,
                                                  headers={'Authorization': 'Bearer x'},
                                                  table=distribution.PATCH_TABLE)
        mirror._download_client = http
        with tempfile.TemporaryDirectory() as temp, patch('time.sleep'), self.assertLogs(level='INFO') as logs:
            target = Path(temp) / 'patch.zip'
            target.write_bytes(b'previous')
            with self.assertRaises(httpx.ReadTimeout):
                patch_mirror.download_patch(manifest, target, mirror_factory=lambda **kw: mirror)
            self.assertEqual(target.read_bytes(), b'previous', "existing target zip must be preserved")
            self.assertFalse(list(Path(temp).glob('.feishu-patch-*')), "temp dirs must be cleaned on failure")
            self.assertTrue(http.closed, "download client must be closed")
            self.assertTrue(record_client.closed, "feishu client must be closed")
            self.assertEqual(len(http.sent), 3, "stream must be retried exactly three times")
        combined = '\n'.join(logs.output)
        self.assertNotIn('飞书补丁整包大小及 SHA256 校验通过', combined)

    def test_publication_failure_preserves_local_patch_and_retry_succeeds(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            archive, manifest_path = root / 'patch.zip', root / 'latest_patch.json'
            archive.write_bytes(b'patch')
            manifest = self.manifest(b'patch')
            manifest_path.write_text(json.dumps(manifest))
            for stage in ('archive', 'manifest'):
                with self.subTest(stage=stage), patch.object(packaging, 'BUILD_DIR', root), \
                        patch.object(patch_mirror, 'publish_patch', return_value={'fixture': 'verified'}) as upload, \
                        patch.object(patch_mirror, 'publish_manifest') as publish:
                    (upload if stage == 'archive' else publish).side_effect = OSError('fixture failure')
                    before = manifest_path.read_bytes()
                    with self.assertRaises(OSError):
                        packaging._publish_patch(archive, manifest_path, manifest)
                    if stage == 'archive':
                        publish.assert_not_called()
                    self.assertEqual(manifest_path.read_bytes(), before)
                    self.assertEqual(archive.read_bytes(), b'patch')
                    self.assertNotIn('feishu_patch', manifest)
            with patch.object(packaging, 'BUILD_DIR', root), \
                    patch.object(patch_mirror, 'publish_patch', return_value={'fixture': 'verified'}), \
                    patch.object(patch_mirror, 'publish_manifest') as publish:
                packaging._publish_patch(archive, manifest_path, manifest)
                publish.assert_called_once_with(manifest)
            self.assertEqual(manifest['feishu_patch'], {'fixture': 'verified'})


class _ReleaseMirror:
    def __init__(self):
        self.records = []
        self.creates = 0
        self.closed = 0
        self.lose_response = False

    def ensure_fields(self):
        pass

    def close(self):
        self.closed += 1

    def request(self, method, suffix, **kwargs):
        if suffix == '/records/search':
            conditions = kwargs['json_payload']['filter']['conditions']
            assert conditions[0]['value'] == [patch_mirror.RELEASE_COMPONENT]
            items = [r for r in self.records if patch_mirror._text(r['fields'].get('组件名称')) == patch_mirror.RELEASE_COMPONENT]
            offset = int(kwargs['params'].get('page_token', 0))
            return {'items': copy.deepcopy(items[offset:offset+2]), 'has_more': offset+2 < len(items), 'page_token': str(offset+2)}
        if method == 'POST' and suffix == '/records':
            token = kwargs['params']['client_token']
            if uuid.UUID(token).version != 4:
                raise ValueError('1254037 Invalid client token')
            self.creates += 1
            record = {'record_id': 'rec' + str(self.creates), 'fields': kwargs['json_payload']['fields']}
            self.records.append(copy.deepcopy(record))
            if self.lose_response:
                self.lose_response = False
                raise TimeoutError('created but response lost')
            return {'record': record}
        if method == 'GET':
            return {'record': copy.deepcopy(next(r for r in self.records if suffix.endswith('/' + r['record_id'])))}
        raise AssertionError((method, suffix))


class PatchReleaseTests(unittest.TestCase):
    def test_saved_update_source_migrates_without_changing_other_settings(self):
        from upload_event_module.config import ConfigManager, DEFAULT_REMOTE_UPDATE_MANIFEST_URL
        saved = {'remote_update_manifest_url': 'https://gitee.com/old/latest_patch.json',
                 'remote_update_enabled': False, 'remote_update_interval_seconds': 1200,
                 'feishu_app_id': 'fixture-app', 'feishu_app_secret': 'fixture-secret',
                 'table_id_weibao': 'fixture-table'}
        with patch.object(ConfigManager, '_load_config_from_sqlite', return_value=saved), \
                patch.object(ConfigManager, '_save_config_to_sqlite', return_value=True) as persist:
            settings = ConfigManager()
            settings.load()
            self.assertEqual(settings.remote_update_manifest_url, DEFAULT_REMOTE_UPDATE_MANIFEST_URL)
            self.assertFalse(settings.remote_update_enabled)
            self.assertEqual(settings.remote_update_interval_seconds, 1200)
            self.assertEqual(settings.app_id, 'fixture-app')
            self.assertEqual(settings.app_secret, 'fixture-secret')
            persist.assert_not_called()
            self.assertTrue(settings.save(remote_update_manifest_url=saved['remote_update_manifest_url']))
            self.assertEqual(persist.call_args.args[0]['remote_update_manifest_url'], DEFAULT_REMOTE_UPDATE_MANIFEST_URL)
            self.assertEqual(settings.app_id, 'fixture-app')

    def manifest(self, version=481):
        fixture = PatchMirrorTests()
        manifest = fixture.manifest(b'patch')
        manifest.update(target_patch_version=version, update_source='feishu', feishu_patch=fixture._patch_spec(b'patch'))
        manifest.pop('zip_url')
        return manifest

    def factory(self, mirror):
        def build(**kwargs):
            self.assertEqual(kwargs, {'table': distribution.PATCH_TABLE})
            return mirror
        return build

    def test_verified_releases_are_discovered_across_pages_without_duplicate_retry(self):
        mirror = _ReleaseMirror()
        factory = self.factory(mirror)
        self.assertEqual(patch_mirror.fetch_manifest(mirror_factory=factory), {})
        for version in (480, 482, 481, 479):
            patch_mirror.publish_manifest(self.manifest(version), mirror_factory=factory)
        patch_mirror.publish_manifest(self.manifest(481), mirror_factory=factory)
        self.assertEqual(mirror.creates, 4)
        mirror.records.append({'fields': {'组件名称': '灯塔完整更新补丁', '启用': True}})
        self.assertEqual(patch_mirror.fetch_manifest(mirror_factory=factory), self.manifest(482))
        # Feishu text fields may be returned as rich-text arrays.
        for key in ('更新说明', '组件名称', '版本', '整包SHA256'):
            mirror.records[1]['fields'][key] = [{'text': mirror.records[1]['fields'][key]}]
        self.assertEqual(patch_mirror._read_release(mirror.records[1]), self.manifest(482))
        self.assertEqual(patch_mirror.fetch_manifest(mirror_factory=factory), self.manifest(482))

    def test_lost_create_response_is_recovered_without_republishing(self):
        mirror = _ReleaseMirror()
        factory = self.factory(mirror)
        mirror.lose_response = True
        with self.assertRaises(TimeoutError):
            patch_mirror.publish_manifest(self.manifest(), mirror_factory=factory)
        patch_mirror.publish_manifest(self.manifest(), mirror_factory=factory)
        self.assertEqual(mirror.creates, 1)
        self.assertEqual(patch_mirror.fetch_manifest(mirror_factory=factory), self.manifest())
        self.assertEqual(mirror.closed, 3)

    def test_create_retry_reuses_feishu_compatible_uuid4_token(self):
        mirror = _ReleaseMirror()
        factory = self.factory(mirror)
        original = mirror.request
        captured = []

        def request(method, suffix, **kwargs):
            if method == 'POST' and suffix == '/records':
                token = kwargs['params']['client_token']
                self.assertEqual(uuid.UUID(token).version, 4, 'Feishu rejects UUID v5 with 1254037')
                captured.append(token)
                if len(captured) == 1:
                    raise TimeoutError('fixture: request interrupted before create')
            return original(method, suffix, **kwargs)

        with patch.object(mirror, 'request', side_effect=request):
            with self.assertRaises(TimeoutError):
                patch_mirror.publish_manifest(self.manifest(), mirror_factory=factory)
            patch_mirror.publish_manifest(self.manifest(), mirror_factory=factory)
        self.assertEqual(len(captured), 2)
        self.assertEqual(captured[0], captured[1])
        self.assertEqual(mirror.creates, 1)
        self.assertEqual(patch_mirror.fetch_manifest(mirror_factory=factory), self.manifest())

    def test_unverified_or_conflicting_manifest_is_rejected(self):
        mirror = _ReleaseMirror()
        factory = self.factory(mirror)
        for key, value in (('major_version', True), ('target_patch_version', '481'), ('zip_name', ''),
                           ('zip_sha256', 'wrong'), ('feishu_patch', {})):
            invalid = dict(self.manifest(), **{key: value})
            with self.subTest(key=key), self.assertRaises(Exception):
                patch_mirror.publish_manifest(invalid, mirror_factory=factory)
        self.assertEqual(mirror.creates, 0)
        patch_mirror.publish_manifest(self.manifest(), mirror_factory=factory)
        conflict = dict(self.manifest(), restart_required=True)
        with self.assertRaisesRegex(ValueError, '已发布且内容不同'):
            patch_mirror.publish_manifest(conflict, mirror_factory=factory)
        mirror.records.append(copy.deepcopy(mirror.records[0]))
        mirror.records[-1]['fields']['更新说明'] = json.dumps(conflict)
        with self.assertRaisesRegex(ValueError, '同一补丁号'):
            patch_mirror.fetch_manifest(mirror_factory=factory)

    def test_disabled_releases_ignored_and_broken_reads_fail_closed(self):
        mirror = _ReleaseMirror()
        factory = self.factory(mirror)
        patch_mirror.publish_manifest(self.manifest(), mirror_factory=factory)
        mirror.records[0]['fields']['启用'] = False
        self.assertEqual(patch_mirror.fetch_manifest(mirror_factory=factory), {})
        mirror.records[0]['fields']['启用'] = True
        mirror.records[0]['fields']['更新说明'] = 'broken-json'
        with self.assertRaises(ValueError):
            patch_mirror.fetch_manifest(mirror_factory=factory)
        for result in ({'items': [], 'has_more': True}, {'items': [], 'has_more': True, 'page_token': 'same'}):
            with patch.object(mirror, 'request', return_value=result), self.assertRaisesRegex(ValueError, '分页不完整'):
                patch_mirror.fetch_manifest(mirror_factory=factory)
        with patch.object(mirror, 'request', side_effect=TimeoutError), self.assertRaises(TimeoutError):
            patch_mirror.fetch_manifest(mirror_factory=factory)
        for result in ({}, {'items': []}, {'has_more': False}, {'items': [None], 'has_more': False}):
            with patch.object(mirror, 'request', return_value=result), self.assertRaisesRegex(ValueError, '响应不完整'):
                patch_mirror.fetch_manifest(mirror_factory=factory)
        with patch.object(mirror, 'request', return_value={'has_more': False, 'total': 0}):
            self.assertEqual(patch_mirror.fetch_manifest(mirror_factory=factory), {})


if __name__ == '__main__':
    unittest.main()

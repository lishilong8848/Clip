"""Offline auth-cache checks using synthetic Qt credentials and fake transports."""
from contextlib import ExitStack
import hashlib
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
from upload_event_module.services import feishu_service as service
from upload_event_module.services import feishu_token_manager as auth
from upload_event_module.services import http_client as http
from lan_bitable_template_portal.cabinet_power import CabinetError, CabinetFeishu
from openclaw_service.assistant import lighthouse_distribution as dist
from openclaw_service.assistant.lighthouse_ai import AssistantError


class AuthFixture(unittest.TestCase):
    def setUp(self):
        self.source = ['cli_fixture', 'synthetic-secret']
        self.cfg = SimpleNamespace(app_id=self.source[0], app_secret=self.source[1],
            user_token='cached-fixture', token_expire_time=time.time() + 7200)
        def reload_credentials():
            if (self.cfg.app_id, self.cfg.app_secret) != tuple(self.source):
                self.cfg.app_id, self.cfg.app_secret = self.source
                self.cfg.user_token, self.cfg.token_expire_time = '', 0
        def save_token(token, expires, *, expected_credentials):
            if tuple(self.source) != expected_credentials:
                return False
            self.cfg.user_token, self.cfg.token_expire_time = token, expires
            return True
        self.cfg.refresh_credentials = Mock(side_effect=reload_credentials)
        self.cfg.save_tenant_token = save_token
        self.manager = auth.FeishuTokenManager(http_client=Mock())
        self.issued = []
        def issue(app_id, _secret):
            token = 'issued-' + str(len(self.issued) + 1) + '-' + app_id
            self.issued.append(token)
            return token, 7200
        self.manager._request_tenant_token_http = Mock(side_effect=issue)
        self.manager._request_tenant_token_sdk = Mock(side_effect=AssertionError('SDK/network forbidden'))
        stack = ExitStack()
        self.addCleanup(stack.close)
        for item in (patch('upload_event_module.config.config', self.cfg),
                     patch.object(auth, 'config', self.cfg), patch.object(service, 'config', self.cfg),
                     patch.object(auth, 'token_manager', self.manager), patch.object(service, 'token_manager', self.manager)):
            stack.enter_context(item)
        self.get_token = stack.enter_context(patch.object(self.manager, 'get_tenant_token', wraps=self.manager.get_tenant_token))
        self.client = Mock()
        self.client.request_json.return_value = {'code': 0, 'data': {'items': []}}

    def rotate(self, app_id='cli_future'):
        self.source[:] = [app_id, 'synthetic-rotated-secret']

    def mirror(self):
        with patch.object(http, 'FeishuHttpClient', return_value=self.client):
            mirror = dist.FeishuRuntimeMirror()
        self.addCleanup(mirror.close)
        return mirror

    def cabinet(self):
        remote = CabinetFeishu('fixture-table', 'fixture-base')
        remote._http = self.client
        remote.require_write = Mock()
        return remote

    def force_count(self):
        return sum(call.kwargs.get('force_refresh') is True for call in self.get_token.call_args_list)


class ServiceCachedAuthTests(AuthFixture):
    def test_warm_ensure_and_status_revalidate_qt_and_preserve_margin(self):
        self.assertEqual(service.ensure_feishu_token(margin_seconds=17), 'cached-fixture')
        self.get_token.assert_called_once_with(margin_seconds=17)
        self.rotate()
        self.assertEqual(service.check_token_status(), 'issued-1-cli_future')
        self.assertEqual(self.cfg.user_token, 'issued-1-cli_future')
        self.assertEqual(self.get_token.call_count, 2)

    def test_same_app_secret_rotation_invalidates_warm_token(self):
        self.assertEqual(service.ensure_feishu_token(), 'cached-fixture')
        self.rotate('cli_fixture')
        self.assertEqual(service.ensure_feishu_token(), 'issued-1-cli_fixture')
        self.manager._request_tenant_token_http.assert_called_once_with(*self.source)

    def test_revalidation_failure_does_not_return_cached_token(self):
        self.cfg.refresh_credentials.side_effect = OSError('synthetic settings unavailable')
        self.assertIsNone(service.ensure_feishu_token())
        self.assertIsNone(service.check_token_status())
        self.assertEqual(self.issued, [])

    def test_expired_token_keeps_existing_refresh_hook(self):
        self.cfg.token_expire_time = time.time() + 1
        with patch.object(service, 'refresh_feishu_token', return_value='injected-token') as refresh:
            self.assertEqual(service.check_token_status(), 'injected-token')
        refresh.assert_called_once()

    @staticmethod
    def result(code):
        return SimpleNamespace(code=code, msg='token expired' if code else 'ok', success=lambda: code == 0)

    def test_explicit_token_rejection_retries_reads_only_once(self):
        send = Mock(side_effect=[self.result(99991663), self.result(0)])
        self.assertTrue(service._with_token_retry(send).success())
        self.assertEqual(send.call_count, 2)
        self.assertEqual(send.call_args_list[0].args, ('cached-fixture',))
        self.assertEqual(send.call_args_list[1].args, ('issued-1-cli_fixture',))
        self.assertEqual(len(self.issued), 1)

    def test_explicit_token_rejection_does_not_replay_writes(self):
        send = Mock(return_value=self.result(99991663))
        self.assertFalse(service._with_rejected_bitable_write_retry(send).success())
        send.assert_called_once_with('cached-fixture')
        self.assertEqual(len(self.issued), 1)

    def test_expiry_without_message_uses_central_error_codes(self):
        rejected = SimpleNamespace(code=99991668, msg='', success=lambda: False)
        send = Mock(side_effect=[rejected, self.result(0)])
        self.assertTrue(service._with_token_retry(send).success())
        self.assertEqual(send.call_count, 2)
        self.assertEqual(len(self.issued), 1)

    def test_unknown_write_is_not_replayed_or_forced_refreshed(self):
        from requests.exceptions import Timeout
        send = Mock(side_effect=Timeout('synthetic response lost'))
        with self.assertRaises(Timeout):
            service._with_token_retry(send, read_only=False)
        send.assert_called_once()
        self.assertEqual(self.issued, [])


class CabinetCachedAuthTests(AuthFixture):
    def test_local_3000_second_cache_cannot_bypass_qt_credentials(self):
        remote = self.cabinet()
        self.assertEqual(remote.token(), 'cached-fixture')
        remote._token, remote._expires = 'stale-local-token', time.monotonic() + 3000
        self.rotate()
        remote.request('GET', 'records')
        self.assertEqual(self.client.request_json.call_args.kwargs['headers'],
                         {'Authorization': 'Bearer issued-1-cli_future'})
        self.rotate('cli_future')
        remote.request('GET', 'records')
        self.assertEqual(self.client.request_json.call_args.kwargs['headers'],
                         {'Authorization': 'Bearer issued-1-cli_future'})
        self.source[1] = 'another-synthetic-secret'
        remote.request('GET', 'records')
        self.assertEqual(self.client.request_json.call_args.kwargs['headers'],
                         {'Authorization': 'Bearer issued-2-cli_future'})

    def test_failed_revalidation_cannot_fall_back_to_local_cache(self):
        remote = self.cabinet()
        remote._token, remote._expires = 'stale-token', time.monotonic() + 3000
        self.cfg.refresh_credentials.side_effect = OSError('synthetic settings unavailable')
        with self.assertRaises(CabinetError):
            remote.request('GET', 'records')
        self.client.request_json.assert_not_called()

    def test_expired_read_clears_central_cache_and_retries_once(self):
        remote = self.cabinet()
        self.client.request_json.side_effect = [
            {'code': 99991677}, {'code': 0, 'data': {'items': []}}]
        self.assertEqual(remote.request('GET', 'records'), {'items': []})
        self.assertEqual(self.force_count(), 1)
        self.assertEqual(self.client.request_json.call_count, 2)
        self.assertEqual([call.kwargs['headers']['Authorization'] for call in self.client.request_json.call_args_list],
                         ['Bearer cached-fixture', 'Bearer issued-1-cli_fixture'])

    def test_repeated_expiry_stops_after_one_central_refresh(self):
        remote = self.cabinet()
        self.client.request_json.return_value = {'code': 99991663}
        with self.assertRaises(CabinetError):
            remote.request('GET', 'records')
        self.assertEqual(self.force_count(), 1)
        self.assertEqual(self.client.request_json.call_count, 2)

    def test_batch_get_is_read_only_but_create_is_never_replayed(self):
        remote = self.cabinet()
        self.client.request_json.side_effect = [
            {'code': 99991663}, {'code': 0, 'data': {'records': []}}, {'code': 99991663}]
        self.assertEqual(remote.batch_get(['recFixture']), [])
        remote.require_write.assert_not_called()
        with self.assertRaises(CabinetError):
            remote.create({}, 'stable-operation')
        self.assertEqual(self.client.request_json.call_count, 3)
        self.assertEqual(self.force_count(), 2)
        self.assertEqual(self.client.request_json.call_args.kwargs['retries'], 0)

    def test_upload_expiry_refreshes_central_cache_without_post_replay(self):
        remote = self.cabinet()
        self.client.request_file_json.return_value = {'code': 99991663}
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'fixture.bin'
            path.write_bytes(b'fixture')
            with self.assertRaises(CabinetError):
                remote.upload_attachment(path, path.name)
        self.client.request_file_json.assert_called_once()
        self.assertEqual(self.client.request_file_json.call_args.kwargs['retries'], 0)
        self.assertEqual(self.force_count(), 1)

    def test_unknown_upload_does_not_replay_or_force_refresh(self):
        remote = self.cabinet()
        self.client.request_file_json.side_effect = http.FeishuHTTPError('synthetic response lost')
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'fixture.bin'
            path.write_bytes(b'fixture')
            with self.assertRaises(CabinetError):
                remote.upload_attachment(path, path.name)
        self.client.request_file_json.assert_called_once()
        self.assertEqual(self.force_count(), 0)

    def test_upload_share_and_download_revalidate_credentials(self):
        remote = self.cabinet()
        remote._token, remote._expires = 'stale-local', time.monotonic() + 3000
        self.rotate()
        self.client.request_file_json.return_value = {'code': 0, 'data': {'file_token': 'fileFixture'}}
        self.client.request_json.return_value = {'code': 0, 'data': {
            'record_share_links': {'recFixture': 'https://example.invalid/archive'}}}
        self.client.request_bytes.return_value = (b'fixture', 'application/octet-stream')
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'fixture.bin'
            path.write_bytes(b'fixture')
            remote.upload_attachment(path, path.name)
        remote.record_share_link('recFixture')
        self.assertEqual(remote.download_attachment('fileFixture'), b'fixture')
        for method in ('request_file_json', 'request_json', 'request_bytes'):
            self.assertEqual(getattr(self.client, method).call_args.kwargs['headers'],
                             {'Authorization': 'Bearer issued-1-cli_future'})


class MirrorCachedAuthTests(AuthFixture):
    def test_long_lived_mirror_revalidates_qt_app_and_secret(self):
        mirror = self.mirror()
        mirror.request('GET', '/records')
        self.rotate()
        mirror.request('GET', '/records')
        self.assertEqual(self.client.request_json.call_args.kwargs['headers'],
                         {'Authorization': 'Bearer issued-1-cli_future'})
        self.source[1] = 'another-synthetic-secret'
        mirror.request('GET', '/records')
        self.assertEqual(self.client.request_json.call_args.kwargs['headers'],
                         {'Authorization': 'Bearer issued-2-cli_future'})

    def test_invalid_qt_state_does_not_reuse_lifecycle_headers(self):
        mirror = self.mirror()
        self.cfg.refresh_credentials.side_effect = OSError('synthetic settings unavailable')
        with self.assertRaises(AssistantError):
            mirror.request('GET', '/records')
        self.client.request_json.assert_not_called()

    def test_injected_clients_and_headers_remain_explicit_and_offline(self):
        headers = {'Authorization': 'Bearer injected-fixture'}
        mirror = dist.FeishuRuntimeMirror(client=self.client, headers=headers)
        mirror.request('GET', '/records')
        self.assertIs(self.client.request_json.call_args.kwargs['headers'], headers)
        with patch.object(http, 'FeishuHttpClient', return_value=self.client):
            mirror = dist.FeishuRuntimeMirror(headers=headers)
        mirror.request('GET', '/records')
        self.assertIs(self.client.request_json.call_args.kwargs['headers'], headers)
        dist.FeishuRuntimeMirror(client=self.client).request('GET', '/records')
        self.assertIsNone(self.client.request_json.call_args.kwargs['headers'])
        self.get_token.assert_not_called()

    def test_expired_read_forces_refresh_once_and_write_never_replays(self):
        mirror = self.mirror()
        self.client.request_json.side_effect = [
            {'code': 99991663}, {'code': 0, 'data': {'items': []}}, {'code': 99991663}]
        self.assertEqual(mirror.request('GET', '/records'), {'items': []})
        with self.assertRaises(AssistantError):
            mirror.request('POST', '/records', json_payload={}, retries=7)
        self.assertEqual(self.client.request_json.call_count, 3)
        self.assertEqual(self.force_count(), 2)
        self.assertEqual(self.client.request_json.call_args.kwargs['retries'], 0)

    def test_repeated_expiry_and_unknown_write_cannot_loop(self):
        mirror = self.mirror()
        self.client.request_json.return_value = {'code': 99991663}
        with self.assertRaises(AssistantError):
            mirror.request('GET', '/records')
        self.assertEqual(self.client.request_json.call_count, 2)
        self.assertEqual(self.force_count(), 1)
        self.client.request_json.reset_mock()
        self.client.request_json.side_effect = http.FeishuHTTPError('synthetic response lost')
        with self.assertRaises(http.FeishuHTTPError):
            mirror.request('POST', '/records', json_payload={})
        self.client.request_json.assert_called_once()
        self.assertEqual(self.force_count(), 1)

    def test_publish_upload_and_record_headers_revalidate_after_qt_change(self):
        from bin.test_lighthouse_distribution import FakeFeishuClient, all_fields, make_tiny_package
        fake = FakeFeishuClient(fields=all_fields())
        self.client = Mock(wraps=fake)
        def request(method, url, **kwargs):
            value = fake.request_json(method, url, **kwargs)
            if method == 'GET' and url.endswith('/fields'):
                self.rotate()
            return value
        self.client.request_json.side_effect = request
        mirror = self.mirror()
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            mirror.publish(make_tiny_package(root), root, root / 'journal.json')
        self.assertEqual(self.client.request_json.call_args_list[0].kwargs['headers'],
                         {'Authorization': 'Bearer cached-fixture'})
        self.assertEqual(self.client.request_file_json.call_args.kwargs['headers'],
                         {'Authorization': 'Bearer issued-1-cli_future'})
        self.assertEqual(self.client.request_file_json.call_args.kwargs['retries'], 0)
        for call in self.client.request_json.call_args_list[1:]:
            self.assertEqual(call.kwargs['headers'], {'Authorization': 'Bearer issued-1-cli_future'})
            if call.args[0] == 'POST':
                self.assertEqual(call.kwargs['retries'], 0)

    def test_publish_upload_rejection_or_unknown_write_never_replays(self):
        from bin.test_lighthouse_distribution import FakeFeishuClient, all_fields, make_tiny_package
        for expired in (False, True):
            with self.subTest(expired=expired), tempfile.TemporaryDirectory() as folder:
                self.client = Mock(wraps=FakeFeishuClient(fields=all_fields()))
                if expired:
                    self.client.request_file_json.return_value = {'code': 99991663}
                else:
                    self.client.request_file_json.side_effect = http.FeishuHTTPError('synthetic response lost')
                mirror = self.mirror()
                root = Path(folder)
                before = self.force_count()
                with self.assertRaises(AssistantError if expired else http.FeishuHTTPError):
                    mirror.publish(make_tiny_package(root), root, root / 'journal.json')
                self.client.request_file_json.assert_called_once()
                self.assertEqual(self.client.request_file_json.call_args.kwargs['retries'], 0)
                self.assertEqual(self.force_count() - before, int(expired))
                self.assertFalse(any(call.args[0] == 'POST' for call in self.client.request_json.call_args_list))

    def download(self, handler):
        mirror = self.mirror()
        mirror.client.request_json.return_value = {'code': 0, 'data': {'record': {
            'fields': {'安装包': [{'file_token': 'fileFixture'}]}}}}
        mirror._download_client = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)
        part = {'record_id': 'recFixture', 'file_token': 'fileFixture', 'size': 3,
                'sha256': hashlib.sha256(b'abc').hexdigest()}
        return mirror, part

    def test_download_expiry_refreshes_once_then_strips_bearer_on_safe_cdn(self):
        calls = []
        def handler(request):
            calls.append(request)
            if len(calls) == 1:
                return httpx.Response(401, json={'code': 99991663})
            if len(calls) == 2:
                return httpx.Response(302, headers={'location': 'https://cdn.feishucdn.com/fixture'})
            return httpx.Response(200, content=b'abc')
        mirror, part = self.download(handler)
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / 'fixture.bin'
            mirror.download_part(part, target)
            self.assertEqual(target.read_bytes(), b'abc')
        self.assertEqual(self.force_count(), 1)
        self.assertEqual(calls[0].headers['authorization'], 'Bearer cached-fixture')
        self.assertEqual(calls[1].headers['authorization'], 'Bearer issued-1-cli_fixture')
        self.assertNotIn('authorization', calls[2].headers)

    def test_json_http200_token_error_is_refreshed_without_writing_error_body(self):
        calls = []
        def handler(request):
            calls.append(request)
            return httpx.Response(200, json={'code': 99991663}) if len(calls) == 1 else httpx.Response(200, content=b'abc')
        mirror, part = self.download(handler)
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / 'fixture.bin'
            mirror.download_part(part, target)
            self.assertEqual(target.read_bytes(), b'abc')
        self.assertEqual(self.force_count(), 1)

    def test_download_repeated_expiry_stops_before_opening_target(self):
        calls = []
        def handler(request):
            calls.append(request)
            return httpx.Response(401, json={'code': 99991663})
        mirror, part = self.download(handler)
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / 'fixture.bin'
            with self.assertRaises(AssistantError):
                mirror.download_part(part, target)
            self.assertFalse(target.exists())
        self.assertEqual(len(calls), 2)
        self.assertEqual(self.force_count(), 1)

    def test_verified_json_attachment_is_preserved_as_file_content(self):
        content = b'{"fixture":true}'
        mirror, part = self.download(lambda _request: httpx.Response(
            200, content=content, headers={'content-type': 'application/json'}))
        part.update(size=len(content), sha256=hashlib.sha256(content).hexdigest())
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / 'fixture.json'
            mirror.download_part(part, target)
            self.assertEqual(target.read_bytes(), content)
        self.assertEqual(self.force_count(), 0)

    def test_untrusted_or_non_download_api_redirect_never_receives_bearer(self):
        for location in ('https://example.invalid/secret', 'http://cdn.feishucdn.com/secret',
                         'https://open.feishu.cn/other-api', 'https://open.feishu.cn.evil.invalid/secret'):
            with self.subTest(location=location):
                calls = []
                def handler(request):
                    calls.append(request)
                    return httpx.Response(302, headers={'location': location})
                mirror, part = self.download(handler)
                with tempfile.TemporaryDirectory() as folder:
                    target = Path(folder) / 'fixture.bin'
                    with self.assertRaises(AssistantError):
                        mirror.download_part(part, target)
                    self.assertFalse(target.exists())
                self.assertEqual(len(calls), 1)

    def test_app_change_during_private_download_stops_before_cdn_request(self):
        calls = []
        def handler(request):
            calls.append(request)
            self.rotate()
            return httpx.Response(302, headers={'location': 'https://cdn.feishucdn.com/fixture'})
        mirror, part = self.download(handler)
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / 'fixture.bin'
            with self.assertRaises(AssistantError):
                mirror.download_part(part, target)
            self.assertFalse(target.exists())
        self.assertEqual(len(calls), 1)


if __name__ == '__main__':
    unittest.main()

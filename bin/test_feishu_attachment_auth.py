"""Attachment auth boundaries and one read-only refresh, entirely mocked/offline."""
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).parent))
import httpx
from lan_bitable_template_portal import feishu_assistant_files as files
from lan_bitable_template_portal.learning_cloud import LearningCloud, LearningCloudError
from lan_bitable_template_portal.polling_work_orders import PollingSopCloudStore
from lan_bitable_template_portal.portal_service import MaintenancePortalService, PortalError
from openclaw_service.assistant.lighthouse_ai import AssistantError
from upload_event_module.services.http_client import FeishuHttpClient, FeishuHTTPError

API_URL = 'https://open.feishu.cn/open-apis/drive/v1/medias/file_fixture/download'
OLD = {'Authorization': 'Bearer fixture-old-token'}
NEW = {'Authorization': 'Bearer fixture-new-token'}
READERS = ('portal', 'learning', 'polling', 'polling-file')


class AttachmentAuthTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.qt = self.enterContext(patch(
            'upload_event_module.services.feishu_token_manager.token_manager.get_tenant_token',
            side_effect=lambda force_refresh=False: 'fixture-new-token' if force_refresh else 'fixture-old-token'))
        self.ensure = self.enterContext(patch(
            'lan_bitable_template_portal.portal_service.ensure_feishu_token', return_value='fixture-old-token'))
        self.refresh = self.enterContext(patch(
            'lan_bitable_template_portal.portal_service.refresh_feishu_token', return_value='fixture-new-token'))
        self.enterContext(patch('upload_event_module.services.http_client.time.sleep'))

    def reader(self, kind, handler, attachment=None):
        requests = []
        def transport(request):
            self.assertEqual(request.method, 'GET', 'attachment recovery must never write')
            requests.append(request)
            return handler(request, len(requests))
        client = FeishuHttpClient(transport=httpx.MockTransport(transport), retries=0)
        self.addCleanup(client.close)
        target = Path(self.temp.name) / (kind + '.bin')
        attachment = attachment or {'file_token': 'file_fixture'}
        if kind == 'portal':
            service = MaintenancePortalService.__new__(MaintenancePortalService)
            service._http_client = client
            read = lambda: service._download_mop_attachment(attachment)[0]
        elif kind == 'learning':
            service = LearningCloud(http_client=client)
            read = lambda: service.download_attachment('file_fixture', 1024)
        else:
            service = PollingSopCloudStore.__new__(PollingSopCloudStore)
            service.client = client
            def read():
                result = service.download_attachment(attachment, target if kind == 'polling-file' else None)
                return target.read_bytes() if kind == 'polling-file' else result
        return read, requests, service, target

    def reset_auth(self):
        self.qt.reset_mock()
        self.refresh.reset_mock()
        self.ensure.reset_mock()

    def assert_one_refresh(self, kind):
        if kind == 'portal':
            self.refresh.assert_called_once_with()
            self.qt.assert_not_called()
        else:
            self.refresh.assert_not_called()
            self.assertEqual(sum(call.kwargs.get('force_refresh') is True for call in self.qt.call_args_list), 1)

    def test_portal_signed_cdn_and_non_api_urls_receive_no_authorization(self):
        urls = (
            'https://cdn.feishucdn.com/path/file?signature=fixture&expires=123',
            'https://signed.example.invalid/file?signature=fixture',
            'https://open.feishu.cn.evil.invalid/open-apis/file',
            'http://open.feishu.cn/open-apis/file',
            'http://cdn.example.invalid/file?signature=fixture',
            'https://open.feishu.cn:444/open-apis/file',
            'https://open.feishu.cn/other/file',
            'https://tenant.feishu.cn/open-apis/file',
        )
        for url in urls:
            with self.subTest(url=url):
                self.reset_auth()
                read, requests, _, _ = self.reader('portal', lambda r, n: httpx.Response(200, content=b'signed-file'), {'url': url})
                self.assertEqual(read(), b'signed-file')
                self.assertEqual(str(requests[0].url), url)
                self.assertNotIn('authorization', requests[0].headers)
                self.ensure.assert_not_called()
                self.refresh.assert_not_called()

    def test_portal_userinfo_malformed_urls_and_non_http_schemes_fail_before_auth(self):
        for url in ('https://user:pass@open.feishu.cn/open-apis/file',
                    'https://@open.feishu.cn/open-apis/file',
                    'https://open.feishu.cn:bad/open-apis/file',
                    'https://open.feishu.cn\n/open-apis/file',
                    'file:///private/file', 'javascript:alert(1)', '//open.feishu.cn/open-apis/file'):
            with self.subTest(url=url):
                self.reset_auth()
                read, requests, _, _ = self.reader('portal', lambda r, n: httpx.Response(200), {'url': url})
                with self.assertRaises(PortalError):
                    read()
                self.assertEqual(requests, [])
                self.ensure.assert_not_called()

    def test_official_api_downloads_use_qt_headers_and_never_follow_redirects(self):
        for kind in READERS:
            for location in ('https://evil.example.invalid/file', 'http://open.feishu.cn/open-apis/other',
                             'https://cdn.feishucdn.com/file?signature=fixture'):
                with self.subTest(kind=kind, location=location):
                    self.reset_auth()
                    read, requests, _, _ = self.reader(kind, lambda r, n: httpx.Response(302, headers={'location': location}))
                    with self.assertRaises((FeishuHTTPError, LearningCloudError)):
                        read()
                    self.assertEqual(len(requests), 1)
                    self.assertEqual(str(requests[0].url), API_URL)
                    self.assertEqual(requests[0].headers['authorization'], OLD['Authorization'])
                    self.refresh.assert_not_called()
                    self.assertFalse(any(c.kwargs.get('force_refresh') for c in self.qt.call_args_list))

    def test_explicit_expiry_refreshes_once_for_http_and_json_errors(self):
        for kind in READERS:
            for status, payload in ((401, {}), (400, {'code': 99991663}),
                                    (403, {'code': 99991668}), (200, {'code': 99991665})):
                with self.subTest(kind=kind, status=status):
                    self.reset_auth()
                    def response(request, count):
                        if count == 1:
                            return httpx.Response(status, json=payload)
                        return httpx.Response(200, content=b'actual-attachment')
                    read, requests, _, _ = self.reader(kind, response)
                    self.assertEqual(read(), b'actual-attachment')
                    self.assertEqual(len(requests), 2)
                    self.assertEqual([r.headers['authorization'] for r in requests],
                                     [OLD['Authorization'], NEW['Authorization']])
                    self.assert_one_refresh(kind)

    def test_second_expiry_stops_and_never_leaves_an_error_as_an_attachment(self):
        for kind in READERS:
            for status in (401, 200):
                with self.subTest(kind=kind, status=status):
                    self.reset_auth()
                    read, requests, _, target = self.reader(kind, lambda r, n: httpx.Response(status, json={'code': 99991663}))
                    with self.assertRaises((FeishuHTTPError, LearningCloudError, PortalError)):
                        read()
                    self.assertEqual(len(requests), 2)
                    self.assert_one_refresh(kind)
                    self.assertFalse(target.exists())

    def test_permission_denial_and_uncertain_network_error_do_not_refresh(self):
        for kind in READERS:
            for failure in ('permission', 'network'):
                with self.subTest(kind=kind, failure=failure):
                    self.reset_auth()
                    def response(request, count):
                        if failure == 'permission':
                            return httpx.Response(403, json={'code': 99991672, 'msg': 'expired 99991663 is not an expiry code'})
                        raise httpx.ReadTimeout('fixture transport stopped', request=request)
                    read, requests, _, _ = self.reader(kind, response)
                    with self.assertRaises((FeishuHTTPError, LearningCloudError)):
                        read()
                    expected = 2 if kind == 'polling-file' and failure == 'network' else 1
                    self.assertEqual(len(requests), expected)
                    self.refresh.assert_not_called()
                    self.assertFalse(any(c.kwargs.get('force_refresh') for c in self.qt.call_args_list))

    def test_cdn_auth_error_never_refreshes_or_receives_api_credentials(self):
        read, requests, _, _ = self.reader('portal', lambda r, n: httpx.Response(401, json={'code': 99991663}),
                                           {'url': 'https://cdn.example.invalid/signed?signature=fixture'})
        with self.assertRaises(FeishuHTTPError):
            read()
        self.assertEqual(len(requests), 1)
        self.assertNotIn('authorization', requests[0].headers)
        self.ensure.assert_not_called()
        self.refresh.assert_not_called()

    def test_normal_json_attachments_are_preserved(self):
        content = b'{"code":{"user-defined":[1,2]},"content":"a normal file"}'
        for kind in READERS:
            with self.subTest(kind=kind):
                read, _, _, _ = self.reader(kind, lambda r, n: httpx.Response(200, content=content))
                self.assertEqual(read(), content)

    def test_portal_cache_hit_needs_no_credentials_and_refresh_caches_only_final_bytes(self):
        attachment = {'file_token': 'file_fixture', 'app_token': 'fixture-app',
                      'table_id': 'fixture-table', 'record_id': 'fixture-record'}
        read, requests, service, _ = self.reader('portal', lambda r, n: httpx.Response(200, content=b'unused'), attachment)
        service._read_cached_attachment = Mock(return_value=(b'cached-bytes', 'application/octet-stream'))
        service._write_cached_attachment = Mock()
        self.assertEqual(read(), b'cached-bytes')
        self.assertEqual(requests, [])
        self.ensure.assert_not_called()
        service._write_cached_attachment.assert_not_called()
        def response(request, count):
            return httpx.Response(401, json={'code': 99991663}) if count == 1 else httpx.Response(200, content=b'actual-file')
        read, requests, service, _ = self.reader('portal', response, attachment)
        service._read_cached_attachment = Mock(return_value=None)
        service._write_cached_attachment = Mock()
        self.assertEqual(read(), b'actual-file')
        self.assertEqual(len(requests), 2)
        service._write_cached_attachment.assert_called_once()
        self.assertEqual(service._write_cached_attachment.call_args.kwargs['content'], b'actual-file')

    def test_failed_token_refresh_never_retries_with_stale_or_empty_authorization(self):
        self.qt.side_effect = lambda force_refresh=False: '' if force_refresh else 'fixture-old-token'
        self.refresh.return_value = None
        for kind in READERS:
            with self.subTest(kind=kind):
                read, requests, _, _ = self.reader(kind, lambda r, n: httpx.Response(401, json={'code': 99991663}))
                with self.assertRaises((PortalError, LearningCloudError)):
                    read()
                self.assertEqual(len(requests), 1)

    def test_polling_rejects_fake_api_origins_and_credentials_before_auth(self):
        for url in ('http://open.feishu.cn/open-apis/drive/v1/medias/file_fixture/download',
                    API_URL.replace('open.feishu.cn', 'open.feishu.cn.evil.invalid'),
                    API_URL.replace('open.feishu.cn', 'user:pass@open.feishu.cn'),
                    API_URL.replace('open.feishu.cn', 'open.feishu.cn:444'),
                    API_URL.replace('open.feishu.cn', 'open.feishu.cn:bad')):
            with self.subTest(url=url):
                self.reset_auth()
                read, requests, _, _ = self.reader('polling', lambda r, n: httpx.Response(200),
                                                   {'file_token': 'file_fixture', '_cloud_download_url': url})
                with self.assertRaisesRegex(PortalError, '下载地址无效'):
                    read()
                self.assertEqual(requests, [])
                self.qt.assert_not_called()


class AssistantAttachmentAuthTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.resource = {'original_mid': 'om_Fixture', 'key': 'file_Fixture', 'kind': 'file'}

    async def bot(self, handler):
        requests = []
        def transport(request):
            self.assertEqual(request.method, 'GET')
            requests.append(request)
            return handler(request, len(requests))
        client = httpx.AsyncClient(transport=httpx.MockTransport(transport), follow_redirects=True)
        self.addAsyncCleanup(client.aclose)
        messenger = SimpleNamespace(client=AsyncMock(return_value=client),
                                    headers=AsyncMock(side_effect=[dict(OLD), dict(NEW)]), expires=99999999)
        return SimpleNamespace(messenger=messenger), requests

    async def test_expiry_reads_fresh_messenger_headers_once_and_keeps_resource_scope(self):
        for status in (401, 200, 403):
            with self.subTest(status=status):
                bot, requests = await self.bot(lambda r, n: httpx.Response(status, json={'code': 99991663}) if n == 1
                                                else httpx.Response(200, content=b'attachment'))
                self.assertEqual(await files._download(bot, {}, self.resource), b'attachment')
                self.assertEqual(bot.messenger.headers.await_count, 2)
                self.assertEqual(bot.messenger.expires, 0)
                self.assertEqual([r.headers['authorization'] for r in requests], [OLD['Authorization'], NEW['Authorization']])
                self.assertTrue(all(r.url.host == 'open.feishu.cn' and r.url.params['type'] == 'file' for r in requests))

    async def test_second_expiry_permission_denial_and_redirect_never_loop(self):
        for status, code, calls in ((401, 99991663, 2), (200, 99991663, 2), (403, 99991672, 1), (302, 0, 1)):
            with self.subTest(status=status):
                bot, requests = await self.bot(lambda r, n: httpx.Response(status, json={'code': code},
                    headers={'location': 'https://evil.example.invalid/file'}))
                with self.assertRaises(AssistantError):
                    await files._download(bot, {}, self.resource)
                self.assertEqual(len(requests), calls)
                self.assertEqual(bot.messenger.headers.await_count, calls)
                self.assertTrue(all(r.url.host == 'open.feishu.cn' for r in requests))

    async def test_network_failure_does_not_refresh_or_retry(self):
        def response(request, count):
            raise httpx.ReadTimeout('fixture transport stopped', request=request)
        bot, requests = await self.bot(response)
        with self.assertRaises(httpx.ReadTimeout):
            await files._download(bot, {}, self.resource)
        self.assertEqual(len(requests), 1)
        bot.messenger.headers.assert_awaited_once()

    async def test_retired_messenger_never_borrows_new_app_headers_for_old_resource(self):
        new = SimpleNamespace(headers=AsyncMock(return_value=dict(NEW)))
        def response(request, count):
            bot.messenger = new
            return httpx.Response(401, json={'code': 99991663})
        bot, requests = await self.bot(response)
        old = bot.messenger
        old.headers.side_effect = [dict(OLD), AssistantError('fixture retired messenger', 503)]
        with self.assertRaises(AssistantError):
            await files._download(bot, {}, self.resource)
        self.assertEqual(len(requests), 1)
        self.assertEqual(old.headers.await_count, 2)
        new.headers.assert_not_awaited()

    async def test_invalid_resource_never_fetches_headers_or_downloads(self):
        for resource in ({**self.resource, 'original_mid': 'https://evil.invalid/'},
                         {**self.resource, 'key': 'file_Fixture/../../other'}):
            bot, requests = await self.bot(lambda r, n: httpx.Response(200, content=b'x'))
            with self.assertRaises(AssistantError):
                await files._download(bot, {}, resource)
            self.assertEqual(requests, [])
            bot.messenger.headers.assert_not_awaited()

    async def test_size_empty_and_normal_json_boundaries_remain_intact(self):
        for content, status in ((b'', 422), (b'x' * 65, 413), (b'{"code":{}}', None)):
            bot, _ = await self.bot(lambda r, n: httpx.Response(200, content=content))
            with patch.object(files, 'MAX_FILE', 64):
                if status is None:
                    self.assertEqual(await files._download(bot, {}, self.resource), content)
                else:
                    with self.assertRaises(AssistantError) as error:
                        await files._download(bot, {}, self.resource)
                    self.assertEqual(error.exception.status, status)


if __name__ == '__main__':
    unittest.main()

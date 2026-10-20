"""Offline document/workpartner authentication recovery; no real cloud requests."""
import asyncio
import json
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service.assistant import lighthouse_documents as documents
from openclaw_service.assistant.lighthouse_ai import AssistantError
from lan_bitable_template_portal import learning_sharing as sharing
from lan_bitable_template_portal.feishu_assistant import FeishuMessenger
from lan_bitable_template_portal.feishu_workpartner import Workpartner
from lan_bitable_template_portal.feishu_workpartner_models import DocumentAppend
from lan_bitable_template_portal.state_store import LanPortalStateStore
from upload_event_module.services import feishu_token_manager as qt_auth

URL = 'https://vnet.feishu.cn/docx/OfflineFixture'
ACTOR = {'id': 'ou_fixture', 'scopes': ['A'], 'is_admin': False}
APP = 'cli_cloudAuthFixture'
MEETING = 'A楼检查冷却水流量。' * 25 + '\n参会人：@测试甲\n'


class DocumentAuthTests(unittest.TestCase):
    def setUp(self):
        self.config = SimpleNamespace(app_id=APP, app_secret='fixture-secret', user_token='expired')
        self.manager = SimpleNamespace(_credentials=Mock(side_effect=lambda: (self.config.app_id, self.config.app_secret)),
            _credential_identity=(APP, 'fixture-secret'), get_tenant_token=Mock(side_effect=self.refresh))
        for name, value in (('config', self.config), ('token_manager', self.manager)):
            setting = patch.object(qt_auth, name, value)
            setting.start()
            self.addCleanup(setting.stop)
        self.calls = []

    def refresh(self, *, force_refresh):
        if not force_refresh:
            self.assertEqual(self.config.user_token, 'fresh')
            return self.config.user_token
        self.config.user_token = 'fresh'
        return 'fresh'

    def portal(self, failures=None, hook=None):
        failures = {key: list(values) for key, values in (failures or {}).items()}
        def request(method, url, **kwargs):
            suffix = url.removeprefix('https://open.feishu.cn/open-apis/docx/v1/documents/OfflineFixture')
            self.calls.append((method, suffix, dict(kwargs['headers']), dict(kwargs.get('params') or {})))
            if hook:
                value = hook(suffix)
                if value is not None:
                    return value
            if failures.get(suffix):
                code = failures[suffix].pop(0)
                if code:
                    return {'code': code, 'msg': 'fixture remote failure'}
            if suffix == '/raw_content':
                data = {'content': MEETING}
            elif suffix == '/blocks':
                prefix = 'on_' if kwargs['params']['user_id_type'] == 'union_id' else 'ou_'
                data = {'items': [{'text': {'elements': [{'text_run': {'content': '参会人：'}},
                    {'mention_user': {'user_id': prefix + 'fixture'}}]}}], 'has_more': False}
            else:
                data = {'document': {'title': 'A楼会议纪要', 'revision_id': 1}}
            return {'code': 0, 'data': data}
        return SimpleNamespace(_auth_headers=Mock(return_value={'Authorization': 'Bearer expired'}),
                               _request_payload=request)

    def read(self, portal):
        return documents.read_document(portal, ACTOR, URL)

    def test_metadata_and_raw_content_can_each_trigger_one_owned_refresh(self):
        for suffix in ('', '/raw_content'):
            with self.subTest(suffix=suffix):
                self.calls.clear()
                self.manager.get_tenant_token.reset_mock()
                self.config.user_token = 'expired'
                result = self.read(self.portal({suffix: [99991663]}))
                self.assertTrue(result['read_only'])
                self.manager.get_tenant_token.assert_called_once_with(force_refresh=True)
                self.assertEqual([call[0] for call in self.calls], ['GET'] * 3)
                self.assertEqual(self.calls[-1][2], {'Authorization': 'Bearer fresh'})

    def test_all_explicit_expired_codes_have_the_same_bounded_retry(self):
        for code in documents.INVALID_TOKEN_CODES:
            with self.subTest(code=code):
                self.calls.clear()
                self.manager.get_tenant_token.reset_mock()
                self.config.user_token = 'expired'
                self.read(self.portal({'': [code]}))
                self.assertEqual(len(self.calls), 3)
                self.manager.get_tenant_token.assert_called_once()

    def test_a_second_auth_failure_stops_the_complete_read(self):
        for failures, expected_calls in (({'': [99991663, 99991663]}, 2),
                                         ({'': [99991663], '/raw_content': [99991663]}, 3)):
            with self.subTest(failures=failures):
                self.calls.clear()
                self.manager.get_tenant_token.reset_mock()
                self.config.user_token = 'expired'
                with self.assertRaises(AssistantError) as caught:
                    self.read(self.portal(failures))
                self.assertEqual(caught.exception.category, 'feishu_auth_failed')
                self.assertEqual(len(self.calls), expected_calls)
                self.manager.get_tenant_token.assert_called_once()

    def test_permission_format_and_network_errors_are_not_refreshed(self):
        for code, status in ((99991672, 403), (1254302, 503), (99991671, 503)):
            with self.subTest(code=code), self.assertRaises(AssistantError) as caught:
                self.read(self.portal({'': [code]}))
            self.assertEqual(caught.exception.status, status)
        portal = self.portal()
        portal._request_payload = Mock(side_effect=OSError('fixture-network-detail'))
        with self.assertRaises(AssistantError) as caught:
            self.read(portal)
        self.assertNotIn('fixture-network-detail', str(caught.exception))
        self.manager.get_tenant_token.assert_not_called()

    def test_changed_current_token_or_qt_app_never_issues_a_retry(self):
        for previous in (False, True):
            self.calls.clear()
            self.config.user_token = 'replacement-token'
            self.config.app_id = 'cli_fixturePrevious' if previous else APP
            with self.subTest(previous=previous), self.assertRaises(AssistantError) as caught:
                self.read(self.portal({'': [99991663]}))
            self.assertEqual(caught.exception.category, 'feishu_config_changed')
            self.assertEqual(len(self.calls), 1)
        self.manager.get_tenant_token.assert_not_called()

    def test_previous_issuing_identity_is_blocked_before_document_read(self):
        self.manager._credential_identity = ('cli_fixturePrevious', self.config.app_secret)
        with self.assertRaises(AssistantError) as caught:
            self.read(self.portal())
        self.assertEqual(caught.exception.category, 'feishu_config_changed')
        self.assertFalse(self.calls)
        self.manager.get_tenant_token.assert_not_called()

    def test_document_identity_follows_qt_instead_of_an_app_blacklist(self):
        self.config.app_id = 'cli_fixturePrevious'
        self.manager._credential_identity = (self.config.app_id, self.config.app_secret)
        self.assertTrue(self.read(self.portal())['read_only'])
        self.manager.get_tenant_token.assert_not_called()

    def test_missing_issuing_secret_is_rejected_before_document_read(self):
        self.config.app_secret = ''
        self.manager._credential_identity = (APP, '')
        with self.assertRaises(AssistantError) as caught:
            self.read(self.portal())
        self.assertEqual(caught.exception.category, 'feishu_config_changed')
        self.assertFalse(self.calls)
        self.manager.get_tenant_token.assert_not_called()

    def test_app_or_secret_changes_during_refresh_stop_before_retry(self):
        for field in ('app_id', 'app_secret'):
            with self.subTest(field=field):
                self.calls.clear()
                self.config.app_id, self.config.app_secret, self.config.user_token = APP, 'fixture-secret', 'expired'
                def change(**kwargs):
                    setattr(self.config, field, 'fixture-replacement')
                    return 'fresh'
                self.manager.get_tenant_token.side_effect = change
                with self.assertRaises(AssistantError) as caught:
                    self.read(self.portal({'': [99991663]}))
                self.assertEqual(caught.exception.category, 'feishu_config_changed')
                self.assertEqual(len(self.calls), 1)

    def test_changed_issuing_credentials_cannot_reuse_a_stale_current_token(self):
        self.manager._credential_identity = (APP, 'fixture-previous-secret')
        with self.assertRaises(AssistantError) as caught:
            self.read(self.portal({'': [99991663]}))
        self.assertEqual(caught.exception.category, 'feishu_config_changed')
        self.manager.get_tenant_token.assert_not_called()

    def test_concurrent_refresh_with_unchanged_credentials_reuses_live_qt_token(self):
        def hook(suffix):
            if len(self.calls) == 1:
                self.config.user_token = 'fresh'
        self.read(self.portal({'': [99991663]}, hook))
        self.manager.get_tenant_token.assert_called_once_with(force_refresh=False)
        self.assertEqual(self.calls[1][2]['Authorization'], 'Bearer fresh')

    def test_changed_credentials_during_retry_discard_the_success_response(self):
        def hook(suffix):
            if len(self.calls) == 2:
                self.config.app_secret = 'fixture-replacement'
        with self.assertRaises(AssistantError) as caught:
            self.read(self.portal({'': [99991663]}, hook))
        self.assertEqual(caught.exception.category, 'feishu_config_changed')
        self.assertEqual(len(self.calls), 2)

    def test_refresh_failure_is_sanitized_and_does_not_retry_the_document(self):
        self.manager.get_tenant_token.side_effect = RuntimeError('fixture-private-secret')
        with self.assertRaises(AssistantError) as caught:
            self.read(self.portal({'': [99991663]}))
        self.assertEqual(caught.exception.category, 'feishu_auth_failed')
        self.assertNotIn('fixture-private-secret', str(caught.exception))
        self.assertEqual(len(self.calls), 1)

    def test_scope_checks_still_reject_a_refreshed_document(self):
        def hook(suffix):
            if suffix == '/raw_content' and self.calls[-1][2]['Authorization'] == 'Bearer fresh':
                return {'code': 0, 'data': {'content': 'E楼受限资料'}}
        with self.assertRaises(AssistantError) as caught:
            self.read(self.portal({'/raw_content': [99991663]}, hook))
        self.assertEqual(caught.exception.status, 403)

    def test_sharing_metadata_body_and_attendance_use_the_same_recovery(self):
        for suffix in ('', '/raw_content', '/blocks'):
            with self.subTest(suffix=suffix):
                self.calls.clear()
                self.config.user_token = 'expired'
                self.manager.get_tenant_token.reset_mock()
                result = sharing.read_source(self.portal({suffix: [99991663]}), URL)
                self.assertEqual(result['participants'][0]['union_id'], 'on_fixture')
                self.assertEqual(result['revision'], 1)
                self.manager.get_tenant_token.assert_called_once_with(force_refresh=True)
                self.assertTrue(all(call[0] == 'GET' for call in self.calls))

    def test_sharing_wraps_auth_failure_without_returning_partial_source(self):
        with self.assertRaises(sharing.LearningError) as caught:
            sharing.read_source(self.portal({'/raw_content': [99991663, 99991663]}), URL)
        self.assertEqual(caught.exception.status, 503)
        self.assertIn('鉴权', str(caught.exception))
        self.manager.get_tenant_token.assert_called_once()


class WorkpartnerAuthTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.store = LanPortalStateStore(Path(self.folder.name) / 'state.sqlite3')
        self.requests, self.auth_requests = [], []
        self.failures = [99991663, 0]
        self.hook = None
        self.auth_hook = None
        self.http = httpx.AsyncClient(transport=httpx.MockTransport(self.handle))
        self.addAsyncCleanup(self.http.aclose)
        self.config = {'enabled': True, 'app_id': APP, 'secret_cipher': 'fixture-cipher'}
        self.qt_credentials = (APP, 'fixture-secret')
        setting = patch.object(qt_auth, 'token_manager',
                               SimpleNamespace(_credentials=Mock(side_effect=lambda: self.qt_credentials)))
        setting.start()
        self.addCleanup(setting.stop)
        self.messenger = FeishuMessenger(self.config)
        self.messenger.http = self.http
        self.messenger.token, self.messenger.expires = 'expired', time.monotonic() + 600
        self.channel = SimpleNamespace(config=self.config, messenger=self.messenger)
        self.service = Workpartner(SimpleNamespace(state_store=self.store), self.channel)
        for target, value in (
            ('lan_bitable_template_portal.feishu_assistant.unprotect_key', lambda _: 'fixture-secret'),
            ('lan_bitable_template_portal.portal_service.external_real_write_guard', lambda: {'real_write_allowed': True}),
        ):
            setting = patch(target, value)
            setting.start()
            self.addCleanup(setting.stop)

    def handle(self, request):
        if request.url.path.endswith('/tenant_access_token/internal'):
            self.auth_requests.append(request)
            if self.auth_hook:
                self.auth_hook()
            return httpx.Response(200, json={'code': 0, 'tenant_access_token': 'fresh', 'expire': 7200})
        self.requests.append(request)
        if self.hook:
            result = self.hook(request)
            if result is not None:
                return result
        if request.url.path.endswith('/application/v6/scopes'):
            data = {'scopes': [{'scope_name': 'docx:document', 'grant_status': 1}]}
        elif request.url.path.endswith('/children') or request.url.path.endswith('/raw_content'):
            code = self.failures.pop(0) if self.failures else 0
            if code:
                return httpx.Response(200, json={'code': code})
            data = {'content': 'fixture-content'} if request.method == 'GET' else {
                'children': json.loads(request.content)['children'], 'document_revision_id': 2}
        else:
            data = {'document': {'document_id': 'FixtureDoc', 'revision_id': 1, 'title': 'fixture'}}
        return httpx.Response(200, json={'code': 0, 'data': data})

    async def read(self):
        return await self.service.call('GET', '/docx/v1/documents/FixtureDoc/raw_content')

    def business_reads(self):
        return [request for request in self.requests if request.url.path.endswith('/raw_content')]

    def writes(self):
        return [request for request in self.requests if request.method != 'GET']

    async def test_get_refreshes_live_messenger_headers_once(self):
        self.assertEqual((await self.read())['content'], 'fixture-content')
        self.assertEqual([request.headers['Authorization'] for request in self.business_reads()],
                         ['Bearer expired', 'Bearer fresh'])
        self.assertEqual(len(self.auth_requests), 1)
        self.assertEqual(json.loads(self.auth_requests[0].content)['app_id'], APP)

    async def test_second_invalid_token_stops_without_permission_misreport(self):
        self.failures = [99991663, 99991668]
        with self.assertRaises(AssistantError) as caught:
            await self.read()
        self.assertIn('鉴权失败', str(caught.exception))
        self.assertNotIn('资源权限', str(caught.exception))
        self.assertEqual(len(self.business_reads()), 2)
        self.assertEqual(len(self.auth_requests), 1)
        self.assertEqual(self.messenger.expires, 0)

    async def test_permissions_network_and_uncertain_replies_are_never_replayed(self):
        for response in (httpx.Response(403, json={'code': 99991672}), httpx.Response(500, json={'code': 99991663}),
                         httpx.Response(401, text='not an explicit JSON token rejection')):
            self.requests.clear()
            self.hook = lambda request: response
            with self.subTest(status=response.status_code), self.assertRaises(AssistantError):
                await self.read()
            self.assertEqual(len(self.requests), 1)
        self.hook = lambda request: (_ for _ in ()).throw(httpx.ReadTimeout('fixture-private-detail'))
        with self.assertRaises(AssistantError) as caught:
            await self.read()
        self.assertEqual(caught.exception.category, 'feishu_workpartner_uncertain')
        self.assertNotIn('fixture-private-detail', str(caught.exception))
        self.assertFalse(self.auth_requests)

    async def test_unauthorized_success_code_is_not_a_resource_permission_failure(self):
        self.hook = lambda request: httpx.Response(401, json={'code': 0, 'data': {}})
        with self.assertRaises(AssistantError) as caught:
            await self.read()
        self.assertIn('鉴权', str(caught.exception))
        self.assertNotIn('资源权限', str(caught.exception))
        self.assertEqual(caught.exception.category, 'feishu_workpartner_uncertain')
        self.assertEqual(len(self.requests), 1)
        self.assertFalse(self.auth_requests)

    async def test_config_changes_after_rejection_prevent_auth_or_get_retry(self):
        for field in ('app_id', 'secret_cipher', 'enabled'):
            self.config.update(app_id=APP, secret_cipher='fixture-cipher', enabled=True)
            self.requests.clear()
            def change(request):
                self.config[field] = False if field == 'enabled' else 'fixture-replacement'
                return httpx.Response(200, json={'code': 99991663})
            self.hook = change
            with self.subTest(field=field), self.assertRaisesRegex(AssistantError, '鉴权配置已变化'):
                await self.read()
            self.assertEqual(len(self.requests), 1)
        self.assertFalse(self.auth_requests)

    async def test_config_change_during_token_refresh_stops_before_retry(self):
        self.auth_hook = lambda: self.config.update(secret_cipher='fixture-replacement')
        with self.assertRaisesRegex(AssistantError, '鉴权配置已变化'):
            await self.read()
        self.assertEqual(len(self.business_reads()), 1)
        self.assertEqual(len(self.auth_requests), 1)

    async def test_failed_token_refresh_does_not_repeat_the_read_or_leak_details(self):
        def fail():
            raise httpx.ReadTimeout('fixture-private-auth-detail')
        self.auth_hook = fail
        with self.assertRaises(AssistantError) as caught:
            await self.read()
        self.assertIn('鉴权未完成', str(caught.exception))
        self.assertNotIn('fixture-private-auth-detail', str(caught.exception))
        self.assertEqual(len(self.business_reads()), 1)
        self.assertEqual(len(self.auth_requests), 1)

    async def test_concurrent_rejections_do_not_evict_a_newer_messenger_token(self):
        ready = asyncio.Event()
        expired = []
        async def respond(request):
            if request.url.path.endswith('/tenant_access_token/internal'):
                self.auth_requests.append(request)
                return httpx.Response(200, json={'code': 0, 'tenant_access_token': 'fresh', 'expire': 7200})
            self.requests.append(request)
            if request.headers['Authorization'] == 'Bearer expired':
                expired.append(request)
                if len(expired) == 2:
                    ready.set()
                await ready.wait()
                return httpx.Response(200, json={'code': 99991663})
            return httpx.Response(200, json={'code': 0, 'data': {'content': 'fixture-content'}})
        client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        self.addAsyncCleanup(client.aclose)
        self.messenger.http = client
        values = await asyncio.wait_for(asyncio.gather(self.read(), self.read()), timeout=5)
        self.assertEqual([value['content'] for value in values], ['fixture-content'] * 2)
        self.assertEqual(len(self.auth_requests), 1)
        self.assertEqual(len(self.business_reads()), 4)

    async def test_replaced_messenger_does_not_receive_the_original_retry(self):
        def replace(request):
            self.channel.messenger = SimpleNamespace()
            return httpx.Response(200, json={'code': 99991663})
        self.hook = replace
        with self.assertRaisesRegex(AssistantError, '鉴权配置已变化'):
            await self.read()
        self.assertFalse(self.auth_requests)

    async def test_previous_app_is_rejected_before_any_cloud_request(self):
        self.config['app_id'] = 'cli_fixturePrevious'
        with self.assertRaisesRegex(AssistantError, 'Qt 设置中的飞书鉴权') as caught:
            await self.read()
        self.assertEqual(caught.exception.status, 503)
        self.assertFalse(self.requests or self.auth_requests)

    async def test_messenger_and_channel_must_both_match_qt_credentials(self):
        cases = ('channel_app', 'messenger_app', 'both_apps', 'qt_secret', 'messenger_secret', 'missing_qt_secret')
        for case in cases:
            with self.subTest(case=case):
                self.config.update(app_id=APP, secret_cipher='fixture-cipher')
                self.qt_credentials = (APP, 'fixture-secret')
                self.messenger.config = dict(self.config)
                if case in ('channel_app', 'both_apps'):
                    self.config['app_id'] = 'cli_fixturePrevious'
                if case in ('messenger_app', 'both_apps'):
                    self.messenger.config['app_id'] = 'cli_fixturePrevious'
                if case == 'qt_secret':
                    self.qt_credentials = (APP, 'fixture-current-secret')
                elif case == 'messenger_secret':
                    self.messenger.config['secret_cipher'] = 'fixture-previous-cipher'
                elif case == 'missing_qt_secret':
                    self.qt_credentials = (APP, '')
                with self.assertRaisesRegex(AssistantError, 'Qt 设置中的飞书鉴权'):
                    await self.read()
                self.assertFalse(self.requests or self.auth_requests)

    async def test_workpartner_identity_follows_qt_instead_of_an_app_blacklist(self):
        self.config['app_id'] = 'cli_fixturePrevious'
        self.qt_credentials = ('cli_fixturePrevious', 'fixture-secret')
        self.assertEqual((await self.read())['content'], 'fixture-content')
        self.assertEqual(len(self.business_reads()), 2)
        self.assertEqual(len(self.auth_requests), 1)

    async def test_qt_read_failure_is_sanitized_and_does_not_send_requests(self):
        qt_auth.token_manager._credentials.side_effect = RuntimeError('fixture-private-qt-detail')
        with self.assertRaises(AssistantError) as caught:
            await self.read()
        self.assertIn('Qt 设置中的飞书鉴权', str(caught.exception))
        self.assertNotIn('fixture-private-qt-detail', str(caught.exception))
        self.assertFalse(self.requests or self.auth_requests)

    async def test_unreadable_messenger_secret_fails_before_any_cloud_request(self):
        with patch('lan_bitable_template_portal.feishu_assistant.unprotect_key',
                   side_effect=ValueError('fixture-private-cipher-detail')):
            with self.assertRaises(AssistantError) as caught:
                await self.read()
        self.assertIn('Qt 设置中的飞书鉴权', str(caught.exception))
        self.assertNotIn('fixture-private-cipher-detail', str(caught.exception))
        self.assertFalse(self.requests or self.auth_requests)

    async def test_qt_rotation_after_rejection_stops_get_retry(self):
        for field in ('app_id', 'app_secret'):
            self.qt_credentials = (APP, 'fixture-secret')
            self.requests.clear()
            def change(request):
                self.qt_credentials = ('cli_fixturePrevious', 'fixture-secret') if field == 'app_id' else (APP, 'fixture-current-secret')
                return httpx.Response(200, json={'code': 99991663})
            self.hook = change
            with self.subTest(field=field), self.assertRaisesRegex(AssistantError, '鉴权配置已变化'):
                await self.read()
            self.assertEqual(len(self.requests), 1)
            self.assertFalse(self.auth_requests)

    async def test_qt_rotation_during_token_refresh_stops_before_get_retry(self):
        self.auth_hook = lambda: setattr(self, 'qt_credentials', (APP, 'fixture-current-secret'))
        with self.assertRaisesRegex(AssistantError, '鉴权配置已变化'):
            await self.read()
        self.assertEqual(len(self.business_reads()), 1)
        self.assertEqual(len(self.auth_requests), 1)

    async def test_auth_rejected_write_waits_for_manual_retry_with_original_uuid(self):
        self.service._created(ACTOR, 'document', 'FixtureDoc', 'fixture')
        model = DocumentAppend(operation_id='fixture-auth-write', text='fixture paragraph', expected_revision=1)
        with self.assertRaises(AssistantError) as caught:
            await self.service.append_document(ACTOR, 'FixtureDoc', model)
        self.assertIn('本次请求未执行', str(caught.exception))
        receipt = self.service.status(ACTOR, model.operation_id)
        self.assertEqual((receipt['status'], receipt['operation_id']), ('rejected', model.operation_id))
        self.assertEqual(len(self.writes()), 1)
        self.assertFalse(self.auth_requests)
        self.service = Workpartner(SimpleNamespace(state_store=self.store), self.channel)
        result = await self.service.append_document(ACTOR, 'FixtureDoc', model)
        self.assertEqual(result['operation_id'], model.operation_id)
        self.assertEqual(self.service.status(ACTOR, model.operation_id)['status'], 'completed')
        writes = self.writes()
        self.assertEqual(len(writes), 2)
        self.assertEqual(writes[0].url.params['client_token'], writes[1].url.params['client_token'])
        self.assertEqual(writes[0].content, writes[1].content)
        self.assertEqual(len(self.auth_requests), 1)

    async def test_uncertain_write_keeps_original_uuid_and_never_replays(self):
        self.service._created(ACTOR, 'document', 'FixtureDoc', 'fixture')
        model = DocumentAppend(operation_id='fixture-uncertain-write', text='fixture paragraph', expected_revision=1)
        def timeout(request):
            if request.method != 'GET':
                raise httpx.ReadTimeout('fixture-private-detail')
        self.hook = timeout
        for _ in range(2):
            with self.assertRaises(AssistantError):
                await self.service.append_document(ACTOR, 'FixtureDoc', model)
        self.assertEqual(self.service.status(ACTOR, model.operation_id)['status'], 'uncertain')
        self.assertEqual(len(self.writes()), 1)
        self.assertFalse(self.auth_requests)

    async def test_write_success_after_a_config_change_is_uncertain_not_rejected(self):
        self.service._created(ACTOR, 'document', 'FixtureDoc', 'fixture')
        model = DocumentAppend(operation_id='fixture-switched-write', text='fixture paragraph', expected_revision=1)
        def change(request):
            if request.method != 'GET':
                self.config['secret_cipher'] = 'fixture-replacement'
                return httpx.Response(200, json={'code': 0, 'data': {'children': []}})
        self.hook = change
        for _ in range(2):
            with self.assertRaises(AssistantError):
                await self.service.append_document(ACTOR, 'FixtureDoc', model)
        self.config['secret_cipher'] = 'fixture-cipher'
        self.assertEqual(self.service.status(ACTOR, model.operation_id)['status'], 'uncertain')
        self.assertEqual(len(self.writes()), 1)

    async def test_write_success_during_qt_rotation_stays_uncertain_without_replay(self):
        self.service._created(ACTOR, 'document', 'FixtureDoc', 'fixture')
        model = DocumentAppend(operation_id='fixture-qt-switched-write', text='fixture paragraph', expected_revision=1)
        def change(request):
            if request.method != 'GET':
                self.qt_credentials = (APP, 'fixture-current-secret')
                return httpx.Response(200, json={'code': 0, 'data': {'children': []}})
        self.hook = change
        with self.assertRaises(AssistantError) as caught:
            await self.service.append_document(ACTOR, 'FixtureDoc', model)
        self.assertEqual(caught.exception.category, 'feishu_workpartner_uncertain')
        self.qt_credentials = (APP, 'fixture-secret')
        with self.assertRaises(AssistantError) as caught:
            await self.service.append_document(ACTOR, 'FixtureDoc', model)
        self.assertEqual(caught.exception.category, 'feishu_workpartner_uncertain')
        receipt = self.service.status(ACTOR, model.operation_id)
        self.assertEqual((receipt['status'], receipt['operation_id']), ('uncertain', model.operation_id))
        self.assertEqual(len(self.writes()), 1)
        self.assertFalse(self.auth_requests)


if __name__ == '__main__':
    unittest.main()

"""Qt credential-bound cloud retries, using only in-process HTTP transports."""
from contextlib import contextmanager, ExitStack
from pathlib import Path
import socket
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal import learning_cloud, portal_service
from lan_bitable_template_portal.link_directory import LinkRemote
from lan_bitable_template_portal.pre_shift_cloud import CheckCloud
from lan_bitable_template_portal.pre_shift_checks import CheckError
from lan_bitable_template_portal.signature_management import (
    ALIASES_FIELD, ORIGIN_FIELD, SignatureManagement, SignatureManagementError,
)
from upload_event_module.services import feishu_token_manager as qt_auth
from upload_event_module.services.http_client import FeishuHTTPError, FeishuHttpClient

KINDS = ('learning', 'pre_shift', 'directory', 'signature')
IDENTITY = ('cli_fixtureCurrent', 'fixture-current-secret')
REPLACEMENTS = (('cli_fixturePrevious', IDENTITY[1]), (IDENTITY[0], 'fixture-replacement-secret'))
SUCCESS = {'code': 0, 'data': {'record': {'record_id': 'recFixture'}}}
ERRORS = {'learning': learning_cloud.LearningCloudError, 'pre_shift': CheckError,
          'directory': portal_service.PortalError, 'signature': SignatureManagementError}


class CloudTokenGuardTests(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket.socket, 'connect', side_effect=AssertionError('Real network forbidden'))
        network = self.network.start()
        self.addCleanup(self.network.stop)
        self.addCleanup(network.assert_not_called)

    @contextmanager
    def adapter(self, kind, *, method='POST', path='records'):
        self.identity = IDENTITY
        self.calls = []
        self.responses = [99991663, 0]
        self.request_hook = self.refresh_hook = self.headers_hook = None
        self.token = 'fixture-initial-token'

        def token(*, force_refresh=False):
            if force_refresh:
                if self.refresh_hook:
                    self.refresh_hook()
                self.token = 'fixture-refreshed-token'
            elif self.headers_hook:
                self.headers_hook()
            return self.token

        manager = SimpleNamespace(_credentials=Mock(side_effect=lambda: self.identity),
                                  get_tenant_token=Mock(side_effect=token))
        refresh = Mock(side_effect=lambda: manager.get_tenant_token(force_refresh=True))

        def respond(request):
            self.calls.append(request)
            if self.request_hook:
                result = self.request_hook(request)
                if result is not None:
                    return result
            code = self.responses.pop(0) if self.responses else 0
            return httpx.Response(200, json={'code': code, 'data': SUCCESS['data']})

        with ExitStack() as stack:
            stack.enter_context(patch.object(qt_auth, 'token_manager', manager))
            stack.enter_context(patch.object(portal_service, 'refresh_feishu_token', refresh))
            stack.enter_context(patch.object(portal_service, 'external_real_write_guard',
                                            return_value={'real_write_allowed': True}))
            stack.enter_context(patch('upload_event_module.services.http_client.time.sleep'))
            client = FeishuHttpClient(transport=httpx.MockTransport(respond), retries=3)
            stack.callback(client.close)
            service = SimpleNamespace(_http_client=client, _write_http_client=client,
                _auth_headers=lambda: {'Authorization': 'Bearer ' + manager.get_tenant_token(force_refresh=False)})
            body = {'fields': {'fixture': 'same-body'}}
            params = {'client_token': 'fixture-operation-id'}
            if kind == 'learning':
                remote = learning_cloud.LearningCloud(enabled=True, http_client=client)
                invoke = lambda: remote._request(method, 'tables/tblFixture/' + path, body, params)
            elif kind == 'pre_shift':
                remote = CheckCloud(service)
                invoke = lambda: remote.request(method, path, body, params)
            elif kind == 'directory':
                remote = LinkRemote(service)
                invoke = lambda: remote.request(method, path, body, params)
            else:
                meta = SimpleNamespace(field_type=1, has_formula=False)
                service._load_table_fields = Mock(side_effect=[([], {ALIASES_FIELD: meta}),
                    ([], {ALIASES_FIELD: meta, ORIGIN_FIELD: meta})])
                remote = SignatureManagement(service)
                invoke = remote.ensure_fields
            yield SimpleNamespace(invoke=invoke, manager=manager, refresh=refresh, remote=remote, service=service)

    @staticmethod
    def forced_count(adapter):
        return sum(call.kwargs.get('force_refresh') is True
                   for call in adapter.manager.get_tenant_token.call_args_list)

    def assert_switched_refresh_is_blocked(self, kind):
        for replacement in REPLACEMENTS:
            with self.subTest(identity=replacement[0]), self.adapter(kind) as adapter:
                self.refresh_hook = lambda: setattr(self, 'identity', replacement)
                with self.assertRaisesRegex(ERRORS[kind], 'Qt.*鉴权.*变化'):
                    adapter.invoke()
                self.assertEqual(len(self.calls), 1)
                self.assertEqual(self.forced_count(adapter), 1)
                self.assertEqual(self.calls[0].headers['Authorization'], 'Bearer fixture-initial-token')

    def test_learning_cannot_retry_after_qt_app_or_secret_changes_during_refresh(self):
        self.assert_switched_refresh_is_blocked('learning')

    def test_pre_shift_cannot_retry_after_qt_app_or_secret_changes_during_refresh(self):
        self.assert_switched_refresh_is_blocked('pre_shift')

    def test_directory_cannot_retry_after_qt_app_or_secret_changes_during_refresh(self):
        self.assert_switched_refresh_is_blocked('directory')

    def test_signature_cannot_retry_after_qt_app_or_secret_changes_during_refresh(self):
        self.assert_switched_refresh_is_blocked('signature')

    def test_same_credentials_can_recover_once_without_changing_body_or_operation(self):
        for kind in KINDS:
            with self.subTest(kind=kind), self.adapter(kind) as adapter:
                adapter.invoke()
                self.assertEqual(len(self.calls), 2)
                self.assertEqual(self.forced_count(adapter), 1)
                self.assertEqual([r.headers['Authorization'] for r in self.calls],
                    ['Bearer fixture-initial-token', 'Bearer fixture-refreshed-token'])
                self.assertEqual(self.calls[0].content, self.calls[1].content)
                self.assertEqual(self.calls[0].url, self.calls[1].url)

    def test_second_token_rejection_is_not_refreshed_or_retried_again(self):
        for kind in KINDS:
            with self.subTest(kind=kind), self.adapter(kind) as adapter:
                self.responses = [99991663, 99991663]
                with self.assertRaises(ERRORS[kind]):
                    adapter.invoke()
                self.assertEqual(len(self.calls), 2)
                self.assertEqual(self.forced_count(adapter), 1)

    def test_qt_changes_in_first_reply_stop_before_forced_refresh(self):
        for kind in KINDS:
            for replacement in REPLACEMENTS:
                with self.subTest(kind=kind, identity=replacement[0]), self.adapter(kind) as adapter:
                    self.request_hook = lambda request: setattr(self, 'identity', replacement)
                    with self.assertRaisesRegex(ERRORS[kind], 'Qt.*鉴权.*变化'):
                        adapter.invoke()
                    self.assertEqual(len(self.calls), 1)
                    self.assertEqual(self.forced_count(adapter), 0)

    def test_qt_change_while_obtaining_initial_headers_prevents_first_request(self):
        for kind in KINDS:
            with self.subTest(kind=kind), self.adapter(kind) as adapter:
                self.headers_hook = lambda: setattr(self, 'identity', REPLACEMENTS[0])
                with self.assertRaisesRegex(ERRORS[kind], 'Qt.*鉴权.*变化'):
                    adapter.invoke()
                self.assertFalse(self.calls)
                self.assertEqual(self.forced_count(adapter), 0)

    def test_acknowledged_success_is_not_rejected_after_qt_configuration_changes(self):
        for kind in KINDS:
            with self.subTest(kind=kind), self.adapter(kind) as adapter:
                self.responses = [0]
                self.request_hook = lambda request: setattr(self, 'identity', REPLACEMENTS[0])
                result = adapter.invoke()
                self.assertTrue(result)
                self.assertEqual(len(self.calls), 1)
                self.assertEqual(self.forced_count(adapter), 0)

    def test_uncertain_writes_have_one_transport_attempt_including_learning_put(self):
        for kind, method in [(kind, 'POST') for kind in KINDS] + [('learning', 'PUT')]:
            with self.subTest(kind=kind, method=method), self.adapter(kind, method=method) as adapter:
                def timeout(request):
                    raise httpx.ReadTimeout('fixture-private-network-detail', request=request)
                self.request_hook = timeout
                error = (learning_cloud.LearningCloudWriteUncertain if kind == 'learning' else
                         SignatureManagementError if kind == 'signature' else FeishuHTTPError)
                with self.assertRaises(error):
                    adapter.invoke()
                self.assertEqual(len(self.calls), 1)
                self.assertEqual(self.forced_count(adapter), 0)

    def test_server_error_writes_have_one_transport_attempt(self):
        for kind in KINDS:
            with self.subTest(kind=kind), self.adapter(kind) as adapter:
                self.request_hook = lambda request: httpx.Response(503, json={'code': 1255002})
                with self.assertRaises(ERRORS[kind]):
                    adapter.invoke()
                self.assertEqual(len(self.calls), 1)
                self.assertEqual(self.forced_count(adapter), 0)

    def test_permission_rejection_never_forces_refresh(self):
        for kind in KINDS:
            with self.subTest(kind=kind), self.adapter(kind) as adapter:
                self.responses = [99991672]
                with self.assertRaises(ERRORS[kind]):
                    adapter.invoke()
                self.assertEqual(len(self.calls), 1)
                self.assertEqual(self.forced_count(adapter), 0)

    def test_read_only_success_and_expiry_recovery_keep_their_original_call_counts(self):
        for kind in KINDS[:-1]:
            for responses in ([0], [99991663, 0]):
                with self.subTest(kind=kind, responses=responses), self.adapter(kind, method='GET') as adapter:
                    self.responses = list(responses)
                    self.assertEqual(adapter.invoke(), SUCCESS['data'])
                    self.assertEqual(len(self.calls), len(responses))
                    self.assertEqual(self.forced_count(adapter), len(responses) - 1)

    def test_read_only_search_post_keeps_read_transport_retry(self):
        for kind in ('learning', 'pre_shift'):
            with self.subTest(kind=kind), self.adapter(kind, path='records/search') as adapter:
                def timeout_once(request):
                    if len(self.calls) == 1:
                        raise httpx.ReadTimeout('fixture-read-timeout', request=request)
                    return httpx.Response(200, json=SUCCESS)
                self.request_hook = timeout_once
                self.assertEqual(adapter.invoke(), SUCCESS['data'])
                self.assertEqual(len(self.calls), 2)
                self.assertEqual(self.forced_count(adapter), 0)

    def test_existing_signature_fields_remain_read_only_without_authentication(self):
        with self.adapter('signature') as adapter:
            meta = SimpleNamespace(field_type=1, has_formula=False)
            adapter.service._load_table_fields.side_effect = None
            adapter.service._load_table_fields.return_value = ([], {ALIASES_FIELD: meta, ORIGIN_FIELD: meta})
            self.assertEqual(set(adapter.invoke()), {ALIASES_FIELD, ORIGIN_FIELD})
            self.assertFalse(self.calls)
            adapter.manager.get_tenant_token.assert_not_called()
            adapter.manager._credentials.assert_not_called()

    def test_unreadable_qt_snapshot_fails_before_any_request_without_private_details(self):
        for kind in KINDS:
            with self.subTest(kind=kind), self.adapter(kind) as adapter:
                adapter.manager._credentials.side_effect = RuntimeError('fixture-private-auth-detail')
                with self.assertRaisesRegex(ERRORS[kind], 'Qt.*鉴权') as caught:
                    adapter.invoke()
                self.assertNotIn('fixture-private-auth-detail', str(caught.exception))
                self.assertFalse(self.calls)
                self.assertEqual(self.forced_count(adapter), 0)


if __name__ == '__main__':
    unittest.main()

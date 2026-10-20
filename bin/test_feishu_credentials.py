"""Credential failures are local/fake; no real tokens or cloud writes."""
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from upload_event_module.services import feishu_token_manager as auth
from upload_event_module.services.feishu_app_migration import TARGET_APP_ID


class CredentialTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.config = SimpleNamespace(app_id=TARGET_APP_ID, app_secret='fixture-current-secret',
            user_token='', token_expire_time=0, refresh_credentials=Mock(), save_tenant_token=Mock(return_value=True))
        self.enterContext(patch.object(auth, 'DATA_DIR', self.root))
        self.enterContext(patch.object(auth, 'log_info'))
        self.setting = patch.object(auth, 'config', self.config)
        self.setting.start()
        self.addCleanup(self.setting.stop)
        self.errors = patch.object(auth, 'log_error')
        self.error_log = self.errors.start()
        self.addCleanup(self.errors.stop)
        self.client = Mock(request_json=Mock(return_value={'code': 10014, 'msg': 'app secret invalid'}))
        self.manager = auth.FeishuTokenManager(self.client)

    def test_concurrent_rejections_share_cooldown_without_sdk(self):
        barrier = threading.Barrier(12)
        def query(index):
            barrier.wait()
            with self.assertRaises(auth.FeishuCredentialError):
                if index % 2:
                    self.manager.get_app_access_token(force_refresh=True)
                else:
                    self.manager.get_tenant_token(force_refresh=True)
        with patch.object(self.manager, '_request_tenant_token_sdk') as sdk, patch.object(auth, 'log_error') as log:
            with ThreadPoolExecutor(max_workers=12) as pool:
                list(pool.map(query, range(12)))
            self.client.request_json.assert_called_once()
            sdk.assert_not_called()
            log.assert_called_once()
            self.assertNotIn(self.config.app_secret, log.call_args.args[0])
        self.config.refresh_credentials.assert_called()
        self.config.save_tenant_token.assert_not_called()
        self.assertEqual(self.manager._app_token_cache, {})
        self.assertEqual(self.config.user_token, '')

    def test_changed_secret_retries_immediately_and_cooldown_expires(self):
        with patch.object(auth.time, 'monotonic', return_value=100):
            with self.assertRaises(auth.FeishuCredentialError):
                self.manager.get_app_access_token()
        with patch.object(auth.time, 'monotonic', return_value=161):
            with self.assertRaises(auth.FeishuCredentialError):
                self.manager.get_app_access_token()
        self.assertEqual(self.client.request_json.call_count, 2)
        self.config.app_secret = 'fixed-secret'
        self.client.request_json.return_value = {'code': 0, 'app_access_token': 'fixed-token', 'expire': 7200}
        with patch.object(auth.time, 'monotonic', return_value=162):
            self.assertEqual(self.manager.get_app_access_token(), 'fixed-token')
        self.assertEqual(self.client.request_json.call_count, 3)
        self.assertEqual(self.error_log.call_count, 2)
        self.assertEqual(self.manager.get_app_access_token(), 'fixed-token')
        self.assertEqual(self.client.request_json.call_count, 3)
        self.assertEqual(set(self.manager._app_token_cache), {(TARGET_APP_ID, 'fixed-secret')})

    def test_explicit_credentials_do_not_fallback_on_rejection(self):
        self.config.user_token, self.config.token_expire_time = 'fixture-cached-token', 9999999999
        current = (self.config.app_id, self.config.app_secret, self.config.user_token, self.config.token_expire_time)
        with patch.object(self.manager, '_request_tenant_token_sdk') as sdk:
            token, message = self.manager.get_tenant_token_for_credentials('cli_fixtureCandidate', 'bad-secret')
        self.assertEqual(token, '')
        self.assertIn('10014', message)
        sdk.assert_not_called()
        self.error_log.assert_called_once()
        self.assertEqual(self.client.request_json.call_args.kwargs['json_payload'],
                         {'app_id': 'cli_fixtureCandidate', 'app_secret': 'bad-secret'})
        self.assertEqual((self.config.app_id, self.config.app_secret, self.config.user_token,
                          self.config.token_expire_time), current)
        self.config.refresh_credentials.assert_not_called()
        self.config.save_tenant_token.assert_not_called()
        self.assertEqual(self.manager._app_token_cache, {})

    def test_transport_failure_still_uses_existing_sdk_fallback(self):
        self.client.request_json.side_effect = auth.FeishuHTTPError('fixture network error')
        with patch.object(self.manager, '_request_tenant_token_sdk', return_value=('token', 7200)) as sdk:
            self.assertEqual(self.manager.refresh_tenant_token(), 'token')
        sdk.assert_called_once_with(TARGET_APP_ID, 'fixture-current-secret')
        self.assertIn('fixture network error', self.error_log.call_args.args[0])
        self.config.save_tenant_token.assert_called_once()
        self.assertEqual(self.config.save_tenant_token.call_args.args[0], 'token')
        self.assertEqual(self.config.save_tenant_token.call_args.kwargs['expected_credentials'],
                         (TARGET_APP_ID, 'fixture-current-secret'))


if __name__ == '__main__':
    unittest.main()

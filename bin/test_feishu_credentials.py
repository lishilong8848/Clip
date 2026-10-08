"""Credential failures are local/fake; no real tokens or cloud writes."""
import sys
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from upload_event_module.services import feishu_token_manager as auth


class CredentialTests(unittest.TestCase):
    def setUp(self):
        self.config = SimpleNamespace(app_id='fixture-app', app_secret='fixture-secret', user_token='', token_expire_time=0)
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

    def test_explicit_credentials_do_not_fallback_on_rejection(self):
        with patch.object(self.manager, '_request_tenant_token_sdk') as sdk:
            token, message = self.manager.get_tenant_token_for_credentials('fixture-app', 'bad-secret')
        self.assertEqual(token, '')
        self.assertIn('10014', message)
        sdk.assert_not_called()
        self.error_log.assert_called_once()

    def test_transport_failure_still_uses_existing_sdk_fallback(self):
        self.client.request_json.side_effect = auth.FeishuHTTPError('fixture network error')
        with patch.object(self.manager, '_request_tenant_token_sdk', return_value=('token', 7200)) as sdk, \
                patch.object(self.manager, '_save_tenant_token', return_value='token'):
            self.assertEqual(self.manager.refresh_tenant_token(), 'token')
        sdk.assert_called_once()
        self.assertIn('fixture network error', self.error_log.call_args.args[0])


if __name__ == '__main__':
    unittest.main()

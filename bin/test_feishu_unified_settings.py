"""Qt credential propagation tests; all identities, transports and storage are fake."""
import importlib
import json
import sys
import tempfile
import sqlite3
from contextlib import closing, contextmanager
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from upload_event_module.services import feishu_token_manager as auth
from upload_event_module.services.feishu_app_migration import TARGET_APP_ID

PREVIOUS_APP = 'cli_fixturePrevious'


class UnifiedSettingsTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.settings = SimpleNamespace(app_id=TARGET_APP_ID, app_secret='fixture-current-secret',
                                        user_token='cached-token', token_expire_time=9999999999)
        self.enterContext(patch.object(auth, 'config', self.settings))
        self.enterContext(patch('lan_bitable_template_portal.portal_auth.config', self.settings))
        self.enterContext(patch.object(auth, 'DATA_DIR', self.root))
        self.enterContext(patch.object(auth, 'log_error'))
        self.enterContext(patch.object(auth, 'log_info'))
        self.client = Mock()
        self.manager = auth.FeishuTokenManager(self.client)

    @contextmanager
    def delivery_qt_fixture(self):
        with patch.object(self.settings, 'refresh_credentials', create=True,
                          side_effect=lambda: (self.settings.app_id, self.settings.app_secret)) as refresh, \
             patch('upload_event_module.services.robot_webhook.config', self.settings), \
             patch('lan_bitable_template_portal.feishu_delivery.config', self.settings):
            yield refresh

    def test_changed_agent_file_only_signals_reload_from_qt(self):
        marker = self.root / 'feishu_assistant.json'
        marker.write_text(json.dumps({'app_id': 'cli_untrusted', 'app_secret': 'do-not-use'}))
        def load_qt():
            self.settings.app_id, self.settings.app_secret = 'cli_qtconfigured', 'qt-secret'
        self.settings.load = Mock(side_effect=load_qt)
        self.assertEqual(self.manager._credentials(), ('cli_qtconfigured', 'qt-secret'))
        self.manager._credentials()
        self.settings.load.assert_called_once()
        marker.unlink()
        self.manager._credentials()
        self.assertEqual(self.settings.load.call_count, 2)
        self.assertNotEqual(self.settings.app_id, 'cli_untrusted')

    def test_qt_change_invalidates_cached_tenant_and_app_tokens(self):
        self.assertEqual(self.manager.get_tenant_token(), 'cached-token')
        self.manager._app_token_cache[('old-app', 'old-secret')] = {'token': 'old-app-token'}
        self.settings.app_id, self.settings.app_secret = 'cli_qtupdated', 'new-secret'
        self.client.request_json.return_value = {'code': 0, 'tenant_access_token': 'new-token', 'expire': 7200}
        self.settings.save = Mock()
        self.assertEqual(self.manager.get_tenant_token(), 'new-token')
        self.assertFalse(self.manager._app_token_cache)
        self.assertEqual(self.client.request_json.call_args.kwargs['json_payload'],
                         {'app_id': 'cli_qtupdated', 'app_secret': 'new-secret'})

    def test_migrated_qt_credentials_do_not_reuse_previous_cached_token(self):
        self.settings.app_id = PREVIOUS_APP
        self.manager._credentials()
        def migrate():
            self.settings.app_id, self.settings.app_secret = TARGET_APP_ID, 'new-secret'
        self.settings.refresh_credentials = Mock(side_effect=migrate)
        self.manager._credentials()
        self.assertEqual(self.settings.user_token, '')
        self.assertEqual(self.settings.token_expire_time, 0)
        self.assertEqual(self.settings.app_id, TARGET_APP_ID)

    def test_oauth_login_checks_latest_qt_settings_before_building_url(self):
        from lan_bitable_template_portal import portal_auth
        def load_qt():
            self.settings.app_id, self.settings.app_secret = 'cli_updatedOauth', 'fixture-updated'
        self.settings.load = Mock(side_effect=load_qt)
        with patch.object(portal_auth, 'config', self.settings), patch.object(portal_auth, 'token_manager', self.manager):
            self.assertTrue(portal_auth.PortalAuthManager.configured())
        self.settings.load.assert_called_once()
        self.assertEqual(self.settings.app_id, 'cli_updatedOauth')

    def test_authoritative_refresh_does_not_depend_on_agent_mirror_timestamp(self):
        def read_qt():
            self.settings.app_id, self.settings.app_secret = 'cli_canonical', 'canonical-secret'
        self.settings.refresh_credentials = Mock(side_effect=read_qt)
        self.settings.load = Mock(side_effect=AssertionError('not a mirror-triggered reload'))
        self.assertEqual(self.manager._credentials(), ('cli_canonical', 'canonical-secret'))
        self.settings.load.assert_not_called()

    def test_login_capability_fails_closed_without_breaking_guest_status(self):
        from lan_bitable_template_portal import portal_auth
        self.settings.refresh_credentials = Mock(side_effect=OSError('settings unavailable'))
        with patch.object(portal_auth, 'config', self.settings), patch.object(portal_auth, 'token_manager', self.manager):
            self.assertFalse(portal_auth.PortalAuthManager.configured())
        self.client.request_json.assert_not_called()

    def test_login_capability_also_refreshes_without_legacy_load_method(self):
        from lan_bitable_template_portal import portal_auth
        self.settings.refresh_credentials = Mock()
        with patch.object(portal_auth, 'config', self.settings), patch.object(portal_auth, 'token_manager', self.manager):
            self.assertTrue(portal_auth.PortalAuthManager.configured())
        self.settings.refresh_credentials.assert_called_once()

    def test_unreadable_qt_settings_cannot_return_cached_token(self):
        self.settings.refresh_credentials = Mock(side_effect=OSError('unavailable'))
        with self.assertRaisesRegex(auth.FeishuTokenError, '未使用旧配置'):
            self.manager.get_tenant_token()
        self.client.request_json.assert_not_called()

    def test_conditional_token_save_rejection_is_not_reported_as_success(self):
        self.settings.save_tenant_token = Mock(return_value=False)
        self.settings.save = Mock()
        self.client.request_json.return_value = {'code': 0, 'tenant_access_token': 'new-token', 'expire': 7200}
        with self.assertRaises(auth.FeishuTokenError):
            self.manager.refresh_tenant_token()
        self.settings.save_tenant_token.assert_called_once()
        self.assertEqual(self.settings.save_tenant_token.call_args.kwargs['expected_credentials'],
                         (TARGET_APP_ID, 'fixture-current-secret'))
        self.settings.save.assert_not_called()

    def test_oauth_exchange_discards_identity_after_credential_change(self):
        def response(*args, **kwargs):
            self.settings.app_id = 'cli_changed'
            return {'access_token': 'old-user-token'}
        with patch.object(self.manager, '_request_json', side_effect=response), \
             patch.object(self.manager, 'fetch_user_info') as fetch:
            with self.assertRaisesRegex(auth.FeishuTokenError, '重新扫码'):
                self.manager.exchange_oauth_login_code('fixture-code', redirect_uri='https://portal.invalid/callback')
        fetch.assert_not_called()

    def test_oauth_userinfo_discards_identity_after_secret_rotation(self):
        def user_info(*args):
            self.settings.app_secret = 'rotated-secret'
            return {'open_id': 'ou_stale'}
        with patch.object(self.manager, '_request_json', return_value={'access_token': 'old-user-token'}), \
             patch.object(self.manager, 'fetch_user_info', side_effect=user_info):
            with self.assertRaisesRegex(auth.FeishuTokenError, '重新扫码'):
                self.manager.exchange_oauth_login_code('fixture-code', redirect_uri='https://portal.invalid/callback')

    def test_legacy_exchange_discards_identity_after_credential_change(self):
        def response(*args, **kwargs):
            self.settings.app_id = 'cli_changed'
            return {'code': 0, 'data': {'open_id': 'ou_stale'}}
        with patch.object(self.manager, 'get_app_access_token', return_value='fixture-app-token'), \
             patch.object(self.manager, '_request_json', side_effect=response):
            with self.assertRaisesRegex(auth.FeishuTokenError, '重新扫码'):
                self.manager.exchange_login_code('fixture-code')

    def test_record_lookup_tool_cannot_use_environment_or_explicit_credentials(self):
        from upload_event_module.services import query_record_by_record_id as lookup
        with patch.object(auth.token_manager, 'get_tenant_token', return_value='qt-token') as token:
            self.assertEqual(lookup.get_tenant_access_token('cli_foreign', 'foreign-secret'), ('qt-token', None))
        token.assert_called_once_with()

    def test_failed_mirror_sync_does_not_allow_old_production_agent_credentials(self):
        from lan_bitable_template_portal.feishu_assistant_settings import read_config
        from openclaw_service.assistant.lighthouse_ai import AssistantError
        qt_module = importlib.import_module('upload_event_module.config')
        marker = self.root / 'feishu_assistant.json'
        marker.write_text(json.dumps({'app_id': PREVIOUS_APP, 'secret_cipher': 'retired'}))
        self.settings.refresh_credentials = Mock()
        with patch('upload_event_module.utils.DATA_DIR', str(self.root)), \
             patch.object(qt_module, 'config', self.settings), \
             patch('upload_event_module.services.feishu_app_migration.sync_agent_config', side_effect=OSError('disk full')):
            with self.assertRaisesRegex(AssistantError, '未使用旧应用'):
                read_config(marker)
        self.settings.refresh_credentials.assert_called_once()

    def test_stale_production_mirror_is_rejected_after_noop_sync(self):
        from lan_bitable_template_portal import feishu_assistant_settings as settings
        from openclaw_service.assistant.lighthouse_ai import AssistantError
        qt_module = importlib.import_module('upload_event_module.config')
        marker = self.root / 'feishu_assistant.json'
        marker.write_text(json.dumps({'app_id': PREVIOUS_APP, 'secret_cipher': 'retired'}))
        self.settings.refresh_credentials = Mock()
        with patch('upload_event_module.utils.DATA_DIR', str(self.root)), \
             patch.object(qt_module, 'config', self.settings), \
             patch('upload_event_module.services.feishu_app_migration.sync_agent_config', return_value=False), \
             patch.object(settings, 'unprotect_key', return_value='fixture-current-secret'):
            with self.assertRaisesRegex(AssistantError, '未使用旧应用'):
                settings.read_config(marker)

    def test_all_seven_duty_accounts_send_to_current_app_without_lookup(self):
        from lan_bitable_template_portal.feishu_delivery import PublicFeishuSender
        from lan_bitable_template_portal.work_handover_cards import HandoverCards
        from lan_bitable_template_portal.feishu_identity import TARGET_DUTY_OPEN_IDS
        from lan_bitable_template_portal.portal_service import BUILDING_OPEN_ID_MAP
        with self.delivery_qt_fixture() as refresh:
            runtime = SimpleNamespace(state_store=Mock(), service=Mock())
            sender = PublicFeishuSender(runtime)
            cards = HandoverCards(SimpleNamespace(config={'app_id': TARGET_APP_ID}, runtime=runtime))
            for scope, fresh in TARGET_DUTY_OPEN_IDS.items():
                self.assertEqual(BUILDING_OPEN_ID_MAP[scope], fresh)
                self.assertEqual(sender.direct_recipient(fresh, 'open_id'), ('open_id', fresh))
                self.assertEqual(cards.recipient_id({'id': 'recDuty' + scope, 'open_id': fresh}), ('open_id', fresh))
            runtime.service.assert_not_called()
            self.assertEqual(runtime.service.mock_calls, [])
            runtime.state_store.get_document.assert_not_called()
            runtime.state_store.list_documents.assert_not_called()
        refresh.assert_called()

    def test_duty_bot_messages_keep_existing_local_account_identity(self):
        from lan_bitable_template_portal.feishu_assistant import FeishuAssistant
        from lan_bitable_template_portal.feishu_identity import TARGET_DUTY_OPEN_IDS
        from lan_bitable_template_portal.portal_service import BUILDING_OPEN_ID_MAP
        from lan_bitable_template_portal.portal_auth import PortalAuthManager
        auth = SimpleNamespace(current_feishu_app_id=PortalAuthManager.current_feishu_app_id,
            _open_id_explicitly_disabled=lambda _: False, directory_person=lambda _: {},
            _permission_user=lambda _: {'enabled': True, 'name': '值班账号'},
            scopes_for_open_id=lambda oid: [next(scope for scope, canonical in BUILDING_OPEN_ID_MAP.items() if canonical == oid)],
            account_profile=lambda _: {'modules': ['assistant']})
        bot = FeishuAssistant.__new__(FeishuAssistant)
        bot.runtime = SimpleNamespace(state_store=Mock(), auth_manager=auth)
        bot.config = {'app_id': TARGET_APP_ID}
        for scope, fresh in TARGET_DUTY_OPEN_IDS.items():
            self.assertEqual(BUILDING_OPEN_ID_MAP[scope], fresh)
            self.assertEqual(bot._sender_user({'sender_id': fresh})['open_id'], fresh)

    def test_permission_contact_lookup_uses_current_duty_open_id(self):
        from lan_bitable_template_portal.permission_review_cards import PermissionCards
        from lan_bitable_template_portal.feishu_identity import TARGET_DUTY_OPEN_IDS
        from lan_bitable_template_portal.portal_service import BUILDING_OPEN_ID_MAP
        database = self.root / 'sessions.sqlite3'
        with closing(sqlite3.connect(database)) as connection, connection:
            connection.execute('CREATE TABLE auth_sessions (user_json TEXT)')
        service = SimpleNamespace(_auth_headers=lambda: {}, _request_payload=Mock(return_value={
            'code': 0, 'data': {'user': {'open_id': TARGET_DUTY_OPEN_IDS['H'], 'union_id': 'on_Hcurrent'}}}))
        cards = PermissionCards(SimpleNamespace(config={'app_id': TARGET_APP_ID},
            runtime=SimpleNamespace(state_store=SimpleNamespace(db_path=database), service=service)))
        with self.delivery_qt_fixture() as refresh:
            self.assertEqual(cards.union_id(BUILDING_OPEN_ID_MAP['H']), 'on_Hcurrent')
        refresh.assert_called()
        service._request_payload.assert_called_once()
        self.assertTrue(service._request_payload.call_args.args[1].endswith(TARGET_DUTY_OPEN_IDS['H']))

    def test_web_toggle_cannot_roll_back_current_qt_credentials(self):
        from lan_bitable_template_portal import feishu_assistant_settings as settings
        from openclaw_service.assistant.lighthouse_ai import AssistantError
        from upload_event_module.services.feishu_app_migration import qt_settings_transaction, write_settings
        marker = self.root / 'feishu_assistant.json'
        marker.write_text(json.dumps({'app_id': TARGET_APP_ID, 'secret_cipher': 'fixture-cipher', 'enabled': True}))
        original = marker.read_bytes()
        with qt_settings_transaction(self.root) as connection:
            write_settings(connection, {'feishu_app_id': 'cli_newQt', 'feishu_app_secret': 'new-secret'})
        import hashlib
        payload = {'app_id': TARGET_APP_ID, 'enabled': False, 'revision': hashlib.sha256(original).hexdigest()}
        with patch('upload_event_module.utils.DATA_DIR', str(self.root)):
            with self.assertRaises(AssistantError):
                settings.save_config(marker, payload, {})
        self.assertEqual(marker.read_bytes(), original)

    def test_web_channel_toggle_changes_only_enabled_under_qt_lock(self):
        from lan_bitable_template_portal import feishu_assistant_settings as settings
        from upload_event_module.services.feishu_app_migration import qt_settings_transaction, write_settings, read_credentials
        marker = self.root / 'feishu_assistant.json'
        original = {'app_id': TARGET_APP_ID, 'secret_cipher': 'fixture-cipher', 'bridge_cipher': 'same-bridge', 'enabled': True}
        marker.write_text(json.dumps(original))
        with qt_settings_transaction(self.root) as connection:
            write_settings(connection, {'feishu_app_id': TARGET_APP_ID, 'feishu_app_secret': 'current-secret'})
        import hashlib
        payload = {'app_id': TARGET_APP_ID, 'enabled': False, 'revision': hashlib.sha256(marker.read_bytes()).hexdigest()}
        with patch('upload_event_module.utils.DATA_DIR', str(self.root)), \
             patch.object(settings, 'unprotect_key', return_value='current-secret'), \
             patch.object(settings, 'public_config', return_value={'enabled': False}):
            self.assertEqual(settings.save_config(marker, payload, {}), {'enabled': False})
        self.assertEqual(json.loads(marker.read_bytes()), {**original, 'enabled': False})
        with qt_settings_transaction(self.root) as connection:
            self.assertEqual(read_credentials(connection)['feishu_app_id'], TARGET_APP_ID)

    def test_no_network_request_when_qt_migration_fails(self):
        self.settings.refresh_credentials = Mock(side_effect=RuntimeError('migration blocked'))
        with self.assertRaisesRegex(auth.FeishuTokenError, '未使用旧配置'):
            self.manager.get_tenant_token()
        self.client.request_json.assert_not_called()

    def test_qt_connection_validation_does_not_persist_candidate_credentials(self):
        self.client.request_json.return_value = {'code': 0, 'tenant_access_token': 'new-token'}
        self.assertEqual(self.manager.get_tenant_token_for_credentials(TARGET_APP_ID, 'new-secret'),
                         ('new-token', ''))
        self.assertEqual(self.client.request_json.call_args.kwargs['json_payload']['app_id'], TARGET_APP_ID)
        self.assertEqual(self.settings.app_secret, 'fixture-current-secret')
        self.assertEqual(self.settings.user_token, 'cached-token')

    def test_changed_qt_credentials_during_refresh_do_not_save_old_token(self):
        def response(*args, **kwargs):
            self.settings.app_secret = 'rotated-secret'
            return {'code': 0, 'tenant_access_token': 'old-token'}
        self.client.request_json.side_effect = response
        self.settings.save = Mock()
        with self.assertRaisesRegex(auth.FeishuTokenError, '已变化'):
            self.manager.refresh_tenant_token()
        self.settings.save.assert_not_called()
        self.assertEqual(self.settings.user_token, '')

    def test_changed_qt_credentials_during_app_auth_do_not_cache_old_token(self):
        def response(*args, **kwargs):
            self.settings.app_id = 'cli_rotated'
            return {'code': 0, 'app_access_token': 'old-token'}
        self.client.request_json.side_effect = response
        with self.assertRaisesRegex(auth.FeishuTokenError, '已变化'):
            self.manager.get_app_access_token()
        self.assertFalse(self.manager._app_token_cache)

    def test_patch_forces_migration_files_and_ignores_runtime_data(self):
        package = importlib.import_module('package_portable')
        for name in ('feishu_app_migration.py', 'feishu_app_profile.json'):
            relative = Path('bin/upload_event_module/services') / name
            self.assertIn(relative, package.FORCE_PATCH_INCLUDE_FILES)
            self.assertFalse(package._is_excluded(package.PROJECT_ROOT / relative, root=package.PROJECT_ROOT))
        self.assertTrue(package._is_excluded(package.PROJECT_ROOT / 'bin/data/feishu_assistant.json',
                                            root=package.PROJECT_ROOT))


if __name__ == '__main__':
    unittest.main()

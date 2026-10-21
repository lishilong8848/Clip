"""Synthetic credentials and temporary data only; no cloud or installed-config writes."""
import base64
import importlib
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from upload_event_module.services import feishu_app_migration as migration
from openclaw_service.protocol import atomic_json

SOURCE_APP_ID = "cli_fixturePrevious"
SOURCE_SECRET = "fixture-source-secret"
NEW_SECRET = "fixture-new-agent-secret"


def protect(value):
    return "fixture:" + base64.b64encode(value.encode()).decode()


def unprotect(value):
    if not value.startswith("fixture:"):
        raise ValueError("unreadable fixture")
    return base64.b64decode(value[8:], validate=True).decode()


class MigrationTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.profile = self.root / "bundle" / "feishu_app_profile.json"
        self.agent_path = self.root / "feishu_assistant.json"
        self.source = {"feishu_app_id": SOURCE_APP_ID, "feishu_app_secret": SOURCE_SECRET,
                       "feishu_user_token": "fixture-obsolete-token", "token_expire_time": 9999999999,
                       "table_id_weibao": "fixture-table", "custom_setting": {"preserve": True}}
        for name, value in (("protect_key", protect), ("unprotect_key", unprotect),
                            ("PROFILE_PATH", self.profile), ("DATA_DIR", self.root),
                            ("PACKAGED_CONFIG_PATH", self.root / "package" / "config.json")):
            setting = patch.object(migration, name, value)
            setting.start()
            self.addCleanup(setting.stop)

    def build(self):
        return migration.build_migration_profile(SOURCE_SECRET, NEW_SECRET, profile_path=self.profile)

    def legacy_profile(self):
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        salt, nonce = b"s" * 16, b"n" * 12
        value = {"format": 1, "source_app_id": SOURCE_APP_ID, "target_app_id": migration.TARGET_APP_ID,
                 "target_fingerprint": migration._fingerprint(NEW_SECRET), "algorithm": "AES-256-GCM",
                 "kdf": "PBKDF2-HMAC-SHA256", "iterations": migration.ITERATIONS,
                 "salt": base64.b64encode(salt).decode(), "nonce": base64.b64encode(nonce).decode()}
        payload = json.dumps({"app_id": migration.TARGET_APP_ID, "app_secret": NEW_SECRET}).encode()
        value["ciphertext"] = base64.b64encode(AESGCM(migration._key(SOURCE_SECRET, salt, legacy=True)).encrypt(
            nonce, payload, migration._aad(value))).decode()
        atomic_json(self.profile, value)
        return value

    def agent(self, **changes):
        value = {"app_id": migration.TARGET_APP_ID, "secret_cipher": protect(NEW_SECRET),
                 "enabled": True, "bridge_cipher": protect("fixture-bridge"), "custom": {"keep": True}}
        value.update(changes)
        atomic_json(self.agent_path, value)
        return value

    def effective(self, secret=SOURCE_SECRET):
        return migration.effective_credentials(SOURCE_APP_ID, secret, data_root=self.root,
                                               profile_path=self.profile)

    def config_module(self):
        from upload_event_module import utils, logger
        for target, name, value in ((utils, "DATA_DIR", str(self.root)),
                                    (utils, "migrate_legacy_data_file", lambda name: str(self.root / name)),
                                    (logger, "log_info", lambda *_: None), (logger, "log_error", lambda *_: None)):
            setting = patch.object(target, name, value)
            setting.start()
            self.addCleanup(setting.stop)
        module = importlib.import_module("upload_event_module.config")
        for name, value in (("CONFIG_FILE", str(self.root / "config.json")),
                            ("log_info", lambda *_: None), ("log_error", lambda *_: None)):
            setting = patch.object(module, name, value)
            setting.start()
            self.addCleanup(setting.stop)
        from upload_event_module.services.feishu_token_manager import token_manager
        verified = patch.object(token_manager, "get_tenant_token_for_credentials", side_effect=lambda app, secret:
                                ("fixture-verified-token", "") if (app, secret) == (migration.TARGET_APP_ID, NEW_SECRET)
                                else ("", "fixture-invalid-credentials"))
        verified.start()
        self.addCleanup(verified.stop)
        return module

    def store_settings(self, value):
        from lan_bitable_template_portal.state_store import LanPortalStateStore
        store = LanPortalStateStore(self.root / "lan_portal_state.sqlite3")
        store.put_settings(value)
        return store

    def test_non_feishu_fixture_credentials_are_untouched_without_reading_files(self):
        with patch.object(migration, "_read_object", side_effect=AssertionError("must not read")):
            for app in ("fixture-app", ""):
                self.assertEqual(migration.effective_credentials(app, "fixture-secret"), (app, "fixture-secret"))
                value = {**self.source, "feishu_app_id": app}
                self.assertIs(migration.migrate_config(value), value)
            self.assertFalse(migration.sync_agent_config("fixture-app", "fixture-secret"))

    def test_initialized_qt_credentials_and_explicit_disable_are_preserved(self):
        self.agent(enabled=False)
        self.store_settings({"feishu_app_id": migration.TARGET_APP_ID, "feishu_app_secret": NEW_SECRET,
                             migration.MIGRATION_MARKER: 2})
        before = self.agent_path.read_bytes()
        self.assertEqual(self.effective(secret="not-needed"), (migration.TARGET_APP_ID, NEW_SECRET))
        self.assertEqual(self.agent_path.read_bytes(), before)

    def test_pending_target_valid_profile_clears_old_token_atomically_and_only_once(self):
        module = self.config_module()
        self.legacy_profile()
        migration.upgrade_deployment_profile(profile_path=self.profile)
        initial = {**self.source, "feishu_app_id": migration.TARGET_APP_ID, "feishu_app_secret": NEW_SECRET}
        store = self.store_settings(initial)
        history = {"turns": ["fixture-history"], "permissions": {"admin": False}}
        store.put_document("lighthouse_ai", "fixture-person", history)
        manager = module.ConfigManager()
        expected = {**initial, "feishu_user_token": "", "token_expire_time": 0, migration.MIGRATION_MARKER: 2}
        self.assertEqual(store.get_settings(), expected)
        self.assertEqual((manager.app_id, manager.app_secret, manager.user_token), (migration.TARGET_APP_ID, NEW_SECRET, ""))
        self.assertEqual(store.get_document("lighthouse_ai", "fixture-person"), history)
        self.assertTrue(manager.save_tenant_token("fixture-current-token", 9999999999,
                        expected_credentials=(migration.TARGET_APP_ID, NEW_SECRET)))
        with patch.object(migration, "_profile", side_effect=AssertionError("completed migration must not need profile")):
            restarted = module.ConfigManager()
        self.assertEqual(restarted.user_token, "fixture-current-token")

    def test_pending_target_without_profile_bootstrap_discards_supplied_token(self):
        module = self.config_module()
        initial = {**self.source, "feishu_app_id": migration.TARGET_APP_ID, "feishu_app_secret": NEW_SECRET}
        atomic_json(self.root / "config.json", initial)
        store = self.store_settings({})
        manager = module.ConfigManager()
        self.assertEqual((manager.app_id, manager.app_secret), (migration.TARGET_APP_ID, NEW_SECRET))
        self.assertEqual(store.get_settings(), {**initial, "feishu_user_token": "", "token_expire_time": 0,
                                               migration.MIGRATION_MARKER: 2})
        self.assertEqual((manager.user_token, manager.token_expire_time), ("", 0))

    def test_pending_target_new_qt_install_validates_credentials_and_clears_input_token(self):
        module = self.config_module()
        self.build()
        store = self.store_settings({})
        manager = module.ConfigManager()
        self.assertFalse(manager.save(app_id=migration.TARGET_APP_ID, app_secret=SOURCE_SECRET))
        self.assertIsNone(store.get_settings())
        self.assertTrue(manager.save(app_id=migration.TARGET_APP_ID, app_secret=NEW_SECRET,
                                     user_token="fixture-untrusted-token", token_expire_time=9999999999))
        self.assertEqual(store.get_settings(), {"feishu_app_id": migration.TARGET_APP_ID,
                         "feishu_app_secret": NEW_SECRET, "feishu_user_token": "", "token_expire_time": 0,
                         migration.MIGRATION_MARKER: 2})

    def test_empty_qt_save_cannot_mark_new_install_complete_and_bypass_target_validation(self):
        module = self.config_module()
        self.build()
        store = self.store_settings({})
        manager = module.ConfigManager()
        self.assertTrue(manager.save(app_id="", app_secret=""))
        before = store.get_settings()
        self.assertNotIn(migration.MIGRATION_MARKER, before)
        self.assertFalse(manager.save(app_id=migration.TARGET_APP_ID, app_secret=SOURCE_SECRET))
        self.assertEqual(store.get_settings(), before)

    def test_pending_target_missing_secret_cannot_complete_even_without_profile(self):
        module = self.config_module()
        initial = {**self.source, "feishu_app_id": migration.TARGET_APP_ID, "feishu_app_secret": ""}
        store = self.store_settings(initial)
        for has_profile in (False, True):
            with self.subTest(has_profile=has_profile):
                if has_profile:
                    self.build()
                manager = module.ConfigManager()
                self.assertEqual(manager.feishu_app_migration_error, "qt_secret_invalid")
                self.assertEqual((manager.app_id, manager.app_secret, manager.user_token), ("", "", ""))
                self.assertEqual(store.get_settings(), initial)
                self.assertFalse(self.agent_path.exists())

    def test_pending_target_mismatch_blocks_load_refresh_oauth_and_token_requests(self):
        module = self.config_module()
        self.build()
        from upload_event_module.services import feishu_token_manager as auth
        from lan_bitable_template_portal import portal_auth
        from unittest.mock import Mock
        for mode in ("v2", "installed-app-v1"):
            with self.subTest(mode=mode):
                if mode == "installed-app-v1":
                    self.legacy_profile()
                    migration.upgrade_deployment_profile(profile_path=self.profile)
                initial = {**self.source, "feishu_app_id": migration.TARGET_APP_ID, migration.MIGRATION_MARKER: 0}
                store = self.store_settings(initial)
                with patch.object(migration, "_decrypt", side_effect=AssertionError("must not guess old identity")), \
                        patch.object(module, "log_error") as logged:
                    manager = module.ConfigManager()
                self.assertEqual(manager.feishu_app_migration_error, "profile_target_mismatch")
                self.assertEqual((manager.app_id, manager.app_secret, manager.user_token), ("", "", ""))
                self.assertEqual(store.get_settings(), initial)
                self.assertFalse(self.agent_path.exists())
                with self.assertRaisesRegex(migration.FeishuAppMigrationError, "migration_pending"):
                    manager.refresh_credentials()
                http = Mock()
                tokens = auth.FeishuTokenManager(http_client=http)
                with patch.object(auth, "config", manager), patch.object(portal_auth, "config", manager), \
                        patch.object(portal_auth, "token_manager", tokens), \
                        patch.object(tokens, "_request_tenant_token_sdk") as sdk:
                    with self.assertRaises(auth.FeishuTokenError):
                        tokens.get_tenant_token()
                    self.assertFalse(portal_auth.PortalAuthManager.configured())
                http.request_json.assert_not_called()
                sdk.assert_not_called()
                for secret in (SOURCE_SECRET, NEW_SECRET, initial["feishu_user_token"]):
                    self.assertNotIn(secret, str(logged.call_args_list))

    def test_pending_target_corrupt_profile_fails_without_completion_or_mirror(self):
        module = self.config_module()
        self.build()
        self.profile.write_bytes(b'{"unfinished":')
        initial = {**self.source, "feishu_app_id": migration.TARGET_APP_ID, "feishu_app_secret": NEW_SECRET}
        store = self.store_settings(initial)
        manager = module.ConfigManager()
        self.assertEqual(manager.feishu_app_migration_error, "profile_format")
        self.assertEqual(store.get_settings(), initial)
        self.assertFalse(self.agent_path.exists())

    def test_pending_target_qt_repair_validates_candidate_then_commits_without_old_token(self):
        module = self.config_module()
        self.build()
        initial = {**self.source, "feishu_app_id": migration.TARGET_APP_ID}
        store = self.store_settings(initial)
        manager = module.ConfigManager()
        self.assertTrue(manager.save(app_id=migration.TARGET_APP_ID, app_secret=NEW_SECRET,
                                     user_token="fixture-stale-input", token_expire_time=9999999999,
                                     table_id_weibao="fixture-repaired-table"))
        self.assertEqual(store.get_settings(), {**initial, "feishu_app_secret": NEW_SECRET,
                         "feishu_user_token": "", "token_expire_time": 0,
                         "table_id_weibao": "fixture-repaired-table", migration.MIGRATION_MARKER: 2})
        self.assertEqual((manager.app_secret, manager.user_token), (NEW_SECRET, ""))
        self.assertEqual(manager.last_save_error, {})

    def test_pending_target_qt_wrong_candidate_rolls_back_credentials_marker_and_settings(self):
        module = self.config_module()
        self.build()
        manager = module.ConfigManager()
        for stored_secret in (SOURCE_SECRET, NEW_SECRET):
            with self.subTest(valid_stored_secret=stored_secret == NEW_SECRET):
                initial = {**self.source, "feishu_app_id": migration.TARGET_APP_ID,
                           "feishu_app_secret": stored_secret, migration.MIGRATION_MARKER: 0}
                store = self.store_settings(initial)
                self.assertFalse(manager.save(app_id=migration.TARGET_APP_ID, app_secret="fixture-wrong-candidate",
                                               table_id_weibao="must-not-commit"))
                self.assertEqual(store.get_settings(), initial)
                self.assertEqual(manager.last_save_error["code"], "settings_credentials_verification_failed")
        self.assertFalse(self.agent_path.exists())

    def test_pending_target_cannot_save_token_before_migration_finishes(self):
        module = self.config_module()
        manager = module.ConfigManager()
        initial = {**self.source, "feishu_app_id": migration.TARGET_APP_ID, "feishu_app_secret": NEW_SECRET}
        store = self.store_settings(initial)
        self.assertFalse(manager.save_tenant_token("fixture-late-token", 9999999999,
                                                  expected_credentials=(migration.TARGET_APP_ID, NEW_SECRET)))
        self.assertEqual(store.get_settings(), initial)

    def test_pending_target_marker_and_token_clear_roll_back_together_on_write_failure(self):
        module = self.config_module()
        self.build()
        initial = {**self.source, "feishu_app_id": migration.TARGET_APP_ID, "feishu_app_secret": NEW_SECRET}
        store = self.store_settings(initial)
        write = migration.write_settings
        def failed_completion(connection, values):
            write(connection, values)
            if migration.MIGRATION_MARKER in values:
                raise sqlite3.OperationalError("fixture-injected-failure")
        with patch.object(migration, "write_settings", side_effect=failed_completion):
            manager = module.ConfigManager()
        self.assertEqual(store.get_settings(), initial)
        self.assertEqual((manager.app_id, manager.app_secret, manager.user_token), ("", "", ""))
        self.assertFalse(self.agent_path.exists())

    def test_verified_agent_recovers_different_installed_source_and_preserves_ownership(self):
        module = self.config_module()
        self.legacy_profile()
        migration.upgrade_deployment_profile(profile_path=self.profile)
        self.agent(enabled=False)
        mirror = self.agent_path.read_bytes()
        initial = {**self.source, "feishu_app_secret": "fixture-other-install-secret"}
        store = self.store_settings(initial)
        history = {"turns": ["fixture-original-history"]}
        permissions = {"version": 2, "users": {"fixture-user": {"scopes": ["A"], "admin": False}}}
        session = {"user": {"open_id": "fixture-user"}, "expires_at": 9999999999}
        store.put_document("lighthouse_ai", "fixture-person", history)
        store.put_auth_permissions(permissions)
        store.put_auth_session("fixture-session", session)
        saved_permissions, saved_session = store.get_auth_permissions(), store.get_auth_session("fixture-session")
        with patch.object(migration, "_decrypt", wraps=migration._decrypt) as decrypt, \
                patch.object(module, "log_info") as logged:
            manager = module.ConfigManager()
        decrypt.assert_called_once()
        self.assertEqual(sum("迁移已提交" in str(call) for call in logged.call_args_list), 1)
        self.assertEqual((manager.app_id, manager.app_secret, manager.user_token), (migration.TARGET_APP_ID, NEW_SECRET, ""))
        self.assertEqual(store.get_settings(), {**initial, "feishu_app_id": migration.TARGET_APP_ID,
                         "feishu_app_secret": NEW_SECRET, "feishu_user_token": "", "token_expire_time": 0,
                         migration.MIGRATION_MARKER: 2})
        self.assertEqual(store.get_document("lighthouse_ai", "fixture-person"), history)
        self.assertEqual(store.get_auth_permissions(), saved_permissions)
        self.assertEqual(store.get_auth_session("fixture-session"), saved_session)
        self.assertEqual(self.agent_path.read_bytes(), mirror)
        with patch.object(migration, "_agent_secret", side_effect=AssertionError("recovery must run only once")):
            self.assertEqual(module.ConfigManager().refresh_credentials(), (migration.TARGET_APP_ID, NEW_SECRET))

    def test_verified_agent_recovers_pending_target_mismatch_and_missing_source_secret(self):
        module = self.config_module()
        self.legacy_profile()
        migration.upgrade_deployment_profile(profile_path=self.profile)
        self.agent()
        for app, secret in ((migration.TARGET_APP_ID, SOURCE_SECRET), (migration.TARGET_APP_ID, ""), (SOURCE_APP_ID, "")):
            with self.subTest(target=app == migration.TARGET_APP_ID, missing=not secret):
                initial = {**self.source, "feishu_app_id": app, "feishu_app_secret": secret, migration.MIGRATION_MARKER: 0}
                store = self.store_settings(initial)
                manager = module.ConfigManager()
                self.assertEqual((manager.app_id, manager.app_secret, manager.user_token), (migration.TARGET_APP_ID, NEW_SECRET, ""))
                self.assertEqual(store.get_settings(), {**initial, "feishu_app_id": migration.TARGET_APP_ID,
                                 "feishu_app_secret": NEW_SECRET, "feishu_user_token": "", "token_expire_time": 0,
                                 migration.MIGRATION_MARKER: 2})

    @unittest.skipUnless(sys.platform == "win32", "Windows DPAPI integration")
    def test_verified_agent_recovery_uses_real_dpapi_with_synthetic_secret(self):
        from openclaw_service.assistant.lighthouse_ai import protect_key, unprotect_key
        module = self.config_module()
        self.build()
        self.agent(secret_cipher=protect_key(NEW_SECRET), bridge_cipher=protect_key("fixture-bridge"))
        self.store_settings({**self.source, "feishu_app_secret": "fixture-other-install-secret"})
        with patch.object(migration, "protect_key", protect_key), patch.object(migration, "unprotect_key", unprotect_key):
            manager = module.ConfigManager()
        self.assertEqual((manager.app_id, manager.app_secret, manager.user_token), (migration.TARGET_APP_ID, NEW_SECRET, ""))

    def test_unverified_agent_never_recovers_or_reads_other_credential_copies(self):
        module = self.config_module()
        self.build()
        initial = {**self.source, "feishu_app_secret": "fixture-other-install-secret"}
        store = self.store_settings(initial)
        other = {"feishu_app_id": migration.TARGET_APP_ID, "feishu_app_secret": NEW_SECRET}
        atomic_json(self.root / "config.json", other)
        atomic_json(migration.PACKAGED_CONFIG_PATH, other)
        cases = ("other-app", "wrong-secret", "bad-dpapi", "missing", "plaintext-only", "invalid-json")
        for case in cases:
            with self.subTest(case=case):
                self.agent()
                if case == "other-app":
                    self.agent(app_id=SOURCE_APP_ID)
                elif case == "wrong-secret":
                    self.agent(secret_cipher=protect("fixture-wrong-target-secret"))
                elif case == "bad-dpapi":
                    self.agent(secret_cipher="fixture-not-decryptable")
                elif case == "missing":
                    self.agent_path.unlink()
                elif case == "plaintext-only":
                    atomic_json(self.agent_path, {"app_id": migration.TARGET_APP_ID, "app_secret": NEW_SECRET})
                else:
                    self.agent_path.write_bytes(b'{"unfinished":')
                before = self.agent_path.read_bytes() if self.agent_path.exists() else None
                with patch.object(migration, "_read_object", wraps=migration._read_object) as read, \
                        patch.object(module, "log_error") as logged:
                    manager = module.ConfigManager()
                self.assertEqual(manager.feishu_app_migration_error, "profile_authentication")
                self.assertEqual((manager.app_id, manager.app_secret, manager.user_token), ("", "", ""))
                self.assertEqual(store.get_settings(), initial)
                self.assertEqual(self.agent_path.read_bytes() if self.agent_path.exists() else None, before)
                self.assertTrue(all(Path(call.args[0]) in {self.profile, self.agent_path} for call in read.call_args_list))
                for value in (NEW_SECRET, SOURCE_SECRET, initial["feishu_app_secret"]):
                    self.assertNotIn(value, str(logged.call_args_list))

    def test_invalid_profile_never_uses_even_a_valid_local_agent(self):
        module = self.config_module()
        self.build()
        self.agent()
        before = self.agent_path.read_bytes()
        initial = {**self.source, "feishu_app_secret": "fixture-other-install-secret"}
        store = self.store_settings(initial)
        self.profile.write_bytes(b'{"unfinished":')
        with patch.object(migration, "_agent_secret", side_effect=AssertionError("invalid profile cannot authorize recovery")):
            manager = module.ConfigManager()
        self.assertEqual(manager.feishu_app_migration_error, "profile_format")
        self.assertEqual(store.get_settings(), initial)
        self.assertEqual(self.agent_path.read_bytes(), before)

    def test_completed_migration_never_recovers_missing_or_rotated_qt_secret_from_agent(self):
        module = self.config_module()
        self.build()
        self.agent()
        for secret in ("fixture-user-rotated-secret", ""):
            with self.subTest(missing=not secret):
                initial = {**self.source, "feishu_app_id": migration.TARGET_APP_ID,
                           "feishu_app_secret": secret, migration.MIGRATION_MARKER: 2}
                store = self.store_settings(initial)
                with patch.object(migration, "_agent_secret", side_effect=AssertionError("completed Qt authority cannot be replaced")):
                    manager = module.ConfigManager()
                self.assertEqual(store.get_settings(), initial)
                self.assertEqual(manager.app_secret, secret)

    def test_verified_agent_recovery_rolls_back_if_canonical_write_fails(self):
        module = self.config_module()
        self.build()
        self.agent()
        before = self.agent_path.read_bytes()
        initial = {**self.source, "feishu_app_secret": "fixture-other-install-secret"}
        store = self.store_settings(initial)
        write = migration.write_settings
        def fail(connection, values):
            write(connection, values)
            raise sqlite3.OperationalError("fixture-write-failure")
        with patch.object(migration, "write_settings", side_effect=fail) as writes, \
                patch.object(module, "log_info") as logged:
            manager = module.ConfigManager()
        writes.assert_called_once()
        self.assertFalse(any("迁移已提交" in str(call) for call in logged.call_args_list))
        self.assertEqual((manager.app_id, manager.app_secret, manager.user_token), ("", "", ""))
        self.assertEqual(store.get_settings(), initial)
        self.assertEqual(self.agent_path.read_bytes(), before)

    def test_correct_qt_candidate_precedes_local_agent_recovery(self):
        module = self.config_module()
        self.build()
        self.agent(secret_cipher=protect("fixture-other-target-secret"))
        initial = {**self.source, "feishu_app_id": migration.TARGET_APP_ID}
        store = self.store_settings(initial)
        manager = module.ConfigManager()
        with patch.object(migration, "_agent_secret", side_effect=AssertionError("valid Qt candidate must win")):
            self.assertTrue(manager.save(app_id=migration.TARGET_APP_ID, app_secret=NEW_SECRET))
        self.assertEqual((manager.app_secret, manager.user_token), (NEW_SECRET, ""))
        self.assertEqual(store.get_settings()["feishu_app_secret"], NEW_SECRET)
        self.assertEqual(store.get_settings()[migration.MIGRATION_MARKER], 2)

    def test_wrong_explicit_qt_candidate_cannot_be_replaced_by_valid_agent(self):
        module = self.config_module()
        self.build()
        manager = module.ConfigManager()
        self.agent()
        mirror = self.agent_path.read_bytes()
        initial = {**self.source, "feishu_app_id": migration.TARGET_APP_ID}
        store = self.store_settings(initial)
        with patch.object(migration, "_agent_secret", side_effect=AssertionError("explicit candidate cannot use recovery")):
            self.assertFalse(manager.save(app_id=migration.TARGET_APP_ID, app_secret="fixture-wrong-input",
                                          table_id_weibao="must-not-save"))
        self.assertEqual(store.get_settings(), initial)
        self.assertEqual(self.agent_path.read_bytes(), mirror)
        self.assertEqual(manager.last_save_error["code"], "settings_credentials_verification_failed")
        manager.load()
        self.assertEqual((manager.app_secret, manager.user_token), (NEW_SECRET, ""))
        self.assertEqual(store.get_settings()["table_id_weibao"], initial["table_id_weibao"])

    def test_normal_profile_decrypt_precedes_local_agent_recovery(self):
        module = self.config_module()
        self.build()
        self.agent(secret_cipher=protect("fixture-other-target-secret"))
        self.store_settings(self.source)
        with patch.object(migration, "_agent_secret", side_effect=AssertionError("valid source must use profile")):
            manager = module.ConfigManager()
        self.assertEqual((manager.app_id, manager.app_secret, manager.user_token), (migration.TARGET_APP_ID, NEW_SECRET, ""))

    def test_profile_has_no_plaintext_secrets_tokens_or_decryption_key(self):
        self.assertEqual(self.build(), self.profile)
        raw = self.profile.read_text()
        for secret in (SOURCE_SECRET, NEW_SECRET, self.source["feishu_user_token"]):
            self.assertNotIn(secret, raw)
        value = json.loads(raw)
        self.assertEqual(set(value), {"format", "aad_mode", "target_app_id", "target_fingerprint",
                                     "algorithm", "kdf", "iterations", "salt", "nonce", "ciphertext"})
        self.assertEqual(self.effective(), (migration.TARGET_APP_ID, NEW_SECRET))
        agent = json.loads(self.agent_path.read_text())
        self.assertTrue(agent["enabled"])
        self.assertTrue(unprotect(agent["bridge_cipher"]))
        self.assertEqual(unprotect(agent["secret_cipher"]), NEW_SECRET)
        self.assertNotIn("isolated_inbox", agent)

    def test_profile_v1_upgrade_preserves_exact_aad_and_ciphertext_without_source_secret(self):
        original = self.legacy_profile()
        migration.upgrade_deployment_profile(profile_path=self.profile, expected_target_secret=NEW_SECRET)
        upgraded = migration._profile(self.profile)
        self.assertEqual(upgraded["format"], 2)
        self.assertEqual(upgraded["aad_mode"], "installed-app-v1")
        self.assertNotIn("source_app_id", upgraded)
        self.assertNotIn(SOURCE_APP_ID, self.profile.read_text())
        self.assertEqual(migration._aad(upgraded, SOURCE_APP_ID), migration._aad(original))
        for key in ("salt", "nonce", "ciphertext", "target_fingerprint", "iterations"):
            self.assertEqual(upgraded[key], original[key])
        self.assertEqual(migration._decrypt(upgraded, SOURCE_SECRET, SOURCE_APP_ID), NEW_SECRET)
        for app_id, secret in (("cli_fixtureOther", SOURCE_SECRET), (SOURCE_APP_ID, "fixture-wrong-secret")):
            with self.assertRaisesRegex(migration.FeishuAppMigrationError, "profile_authentication"):
                migration._decrypt(upgraded, secret, app_id)
        with self.assertRaisesRegex(migration.FeishuAppMigrationError, "profile_authentication"):
            migration._decrypt({**upgraded, "aad_mode": "v2"}, SOURCE_SECRET, SOURCE_APP_ID)

    def test_profile_v1_can_be_reencrypted_when_source_secret_is_available(self):
        self.legacy_profile()
        before = self.profile.read_bytes()
        with self.assertRaisesRegex(migration.FeishuAppMigrationError, "profile_authentication"):
            migration.upgrade_deployment_profile(profile_path=self.profile, source_secret="fixture-wrong-secret")
        self.assertEqual(self.profile.read_bytes(), before)
        migration.upgrade_deployment_profile(profile_path=self.profile, source_secret=SOURCE_SECRET,
                                             expected_target_secret=NEW_SECRET)
        upgraded = migration._profile(self.profile)
        self.assertEqual(upgraded["aad_mode"], "v2")
        self.assertNotIn(SOURCE_APP_ID, self.profile.read_text())
        self.assertEqual(migration._decrypt(upgraded, SOURCE_SECRET), NEW_SECRET)

    def test_prepare_upgrades_existing_profile_after_source_backups_were_removed(self):
        self.legacy_profile()
        self.store_settings({"feishu_app_id": migration.TARGET_APP_ID, "feishu_app_secret": NEW_SECRET,
                             migration.MIGRATION_MARKER: 2})
        self.assertEqual(migration.prepare_deployment_profile(), self.profile)
        upgraded = migration._profile(self.profile)
        self.assertEqual(upgraded["aad_mode"], "installed-app-v1")
        self.assertEqual(migration._decrypt(upgraded, SOURCE_SECRET, SOURCE_APP_ID), NEW_SECRET)
        before = self.profile.read_bytes()
        self.assertEqual(migration.prepare_deployment_profile(), self.profile)
        self.assertEqual(self.profile.read_bytes(), before)

    def test_empty_sqlite_bootstrap_authenticates_before_writing_migration_marker(self):
        module = self.config_module()
        self.legacy_profile()
        migration.upgrade_deployment_profile(profile_path=self.profile, expected_target_secret=NEW_SECRET)
        store = self.store_settings({})
        history = {"turns": ["fixture-preserved-history"]}
        store.put_document("lighthouse_ai", "fixture-person", history)
        atomic_json(self.root / "config.json", self.source)
        manager = module.ConfigManager()
        saved = store.get_settings()
        self.assertEqual(manager.app_id, migration.TARGET_APP_ID)
        self.assertEqual(saved[migration.MIGRATION_MARKER], 2)
        self.assertEqual(saved["feishu_user_token"], "")
        self.assertEqual(saved["token_expire_time"], 0)
        self.assertEqual(saved["custom_setting"], self.source["custom_setting"])
        self.assertEqual(store.get_document("lighthouse_ai", "fixture-person"), history)
        self.assertNotIn(SOURCE_APP_ID, self.agent_path.read_text())
        self.assertTrue(manager.save(app_id="cli_fixtureFuture", app_secret="fixture-future-secret"))
        with patch.object(migration, "_decrypt", side_effect=AssertionError("one-time migration was replayed")):
            manager.load()
            self.assertEqual(manager.refresh_credentials()[0], "cli_fixtureFuture")
            self.assertEqual(self.effective()[0], "cli_fixtureFuture")

    def test_failed_empty_sqlite_bootstrap_leaves_source_and_marker_untouched(self):
        module = self.config_module()
        self.build()
        store = self.store_settings({})
        source = {**self.source, "feishu_app_secret": "fixture-wrong-secret"}
        atomic_json(self.root / "config.json", source)
        before = (self.root / "config.json").read_bytes()
        manager = module.ConfigManager()
        self.assertEqual(manager.app_id, "")
        self.assertEqual(manager.feishu_app_migration_error, "profile_authentication")
        self.assertIsNone(store.get_settings())
        self.assertEqual((self.root / "config.json").read_bytes(), before)
        self.assertFalse(self.agent_path.exists())

    def test_unmarked_cli_configuration_cannot_bypass_authentication_using_agent_secret(self):
        module = self.config_module()
        self.agent()
        store = self.store_settings(self.source)
        manager = module.ConfigManager()
        self.assertEqual(manager.app_id, "")
        self.assertEqual(store.get_settings(), self.source)
        with self.assertRaisesRegex(migration.FeishuAppMigrationError, "migration_pending"):
            manager.refresh_credentials()

    def test_local_old_app_and_broken_dpapi_can_be_repaired_without_losing_other_fields(self):
        self.build()
        for app, enabled in ((SOURCE_APP_ID, True), (migration.TARGET_APP_ID, False)):
            with self.subTest(app=app):
                previous = self.agent(app_id=app, enabled=enabled, secret_cipher="broken")
                self.assertEqual(self.effective(), (migration.TARGET_APP_ID, NEW_SECRET))
                saved = json.loads(self.agent_path.read_text())
                self.assertEqual(saved["custom"], previous["custom"])
                self.assertEqual(saved["enabled"], enabled)
                self.assertEqual(unprotect(saved["secret_cipher"]), NEW_SECRET)
                self.assertEqual(bool(saved.get("isolated_inbox")), app != migration.TARGET_APP_ID)

    def test_disabled_partial_agent_stays_disabled_without_an_old_inbox_marker(self):
        self.build()
        atomic_json(self.agent_path, {"enabled": False, "custom": "keep"})
        self.effective()
        saved = json.loads(self.agent_path.read_text())
        self.assertFalse(saved["enabled"])
        self.assertEqual(saved["custom"], "keep")
        self.assertNotIn("isolated_inbox", saved)
        self.assertTrue(unprotect(saved["bridge_cipher"]))

    def test_corrupt_local_json_is_not_overwritten(self):
        self.build()
        self.agent_path.write_bytes(b'{"unfinished":')
        with self.assertRaisesRegex(migration.FeishuAppMigrationError, "agent_config_invalid"):
            self.effective()
        self.assertEqual(self.agent_path.read_bytes(), b'{"unfinished":')

    def test_new_active_agent_contains_no_retired_secret_or_cached_auth_tokens(self):
        self.build()
        self.agent(app_id=SOURCE_APP_ID, app_secret=SOURCE_SECRET,
                   feishu_app_id=SOURCE_APP_ID, feishu_app_secret=SOURCE_SECRET,
                   tenant_access_token="fixture-obsolete-token", token_expire_time=9999999999)
        self.effective()
        raw = self.agent_path.read_text()
        self.assertNotIn(SOURCE_SECRET, raw)
        self.assertNotIn(SOURCE_APP_ID, raw)
        self.assertNotIn("fixture-obsolete-token", raw)
        self.assertNotIn("token_expire_time", json.loads(raw))

    def test_wrong_password_and_tampered_metadata_or_ciphertext_fail_closed(self):
        self.build()
        valid = json.loads(self.profile.read_text())
        cases = [(valid, "wrong-fixture-secret")]
        for field, replacement in (("ciphertext", base64.b64encode(b"bad-tag" * 10).decode()),
                                    ("target_fingerprint", "0" * 64),
                                    ("salt", base64.b64encode(b"x" * 16).decode()),
                                    ("nonce", base64.b64encode(b"x" * 12).decode())):
            cases.append(({**valid, field: replacement}, SOURCE_SECRET))
        for profile, password in cases:
            with self.subTest(profile=profile["target_fingerprint"], password=password):
                atomic_json(self.profile, profile)
                with self.assertRaises(migration.FeishuAppMigrationError) as caught:
                    self.effective(password)
                for secret in (SOURCE_SECRET, NEW_SECRET, password):
                    self.assertNotIn(secret, str(caught.exception))
                self.assertFalse(self.agent_path.exists())

    def test_profile_format_is_strict_and_work_factor_is_bounded(self):
        self.build()
        valid = json.loads(self.profile.read_text())
        cases = [{**valid, "target_app_id": SOURCE_APP_ID}, {**valid, "format": True},
                 {**valid, "iterations": 1}, {**valid, "iterations": 10**12},
                 {**valid, "salt": "not base64"}, {**valid, "nonce": "YQ=="},
                 {**valid, "token": "fixture-must-not-be-accepted"}]
        for value in cases:
            with self.subTest(keys=set(value)):
                atomic_json(self.profile, value)
                with self.assertRaisesRegex(migration.FeishuAppMigrationError, "profile_format"):
                    self.effective()
        raw = json.dumps(valid)
        self.profile.write_text(raw[:-1] + ',"format":1}')
        with self.assertRaises(migration.FeishuAppMigrationError):
            self.effective()
        self.profile.write_text(" " * 16385 + raw)
        with self.assertRaises(migration.FeishuAppMigrationError):
            self.effective()

    def test_failed_dpapi_or_authenticated_decryption_never_changes_old_agent_config(self):
        self.build()
        self.agent(app_id=SOURCE_APP_ID, secret_cipher="old-cipher")
        before = self.agent_path.read_bytes()
        with self.assertRaises(migration.FeishuAppMigrationError):
            self.effective("wrong-secret")
        self.assertEqual(self.agent_path.read_bytes(), before)
        with patch.object(migration, "protect_key", side_effect=RuntimeError(NEW_SECRET)):
            with self.assertRaisesRegex(migration.FeishuAppMigrationError, "agent_config_persist_failed"):
                self.effective()
        self.assertEqual(self.agent_path.read_bytes(), before)

    def test_build_reuses_matching_profile_and_requires_old_secret_for_a_new_target(self):
        self.build()
        before = self.profile.read_bytes()
        stamp = self.profile.stat().st_mtime_ns
        self.assertEqual(migration.build_migration_profile(None, NEW_SECRET, profile_path=self.profile), self.profile)
        self.assertEqual(migration.build_migration_profile(SOURCE_SECRET, NEW_SECRET, profile_path=self.profile), self.profile)
        self.assertEqual(self.profile.read_bytes(), before)
        self.assertEqual(self.profile.stat().st_mtime_ns, stamp)
        with self.assertRaisesRegex(migration.FeishuAppMigrationError, "source_secret_missing"):
            migration.build_migration_profile(None, "rotated-fixture-secret", profile_path=self.profile)
        self.assertEqual(self.profile.read_bytes(), before)

    def test_build_discovery_uses_sqlite_before_json_and_can_reuse_after_host_migration(self):
        self.agent()
        self.store_settings(self.source)
        atomic_json(self.root / "config.json", {**self.source, "feishu_app_secret": "wrong-json-secret"})
        self.assertEqual(migration.prepare_deployment_profile(), self.profile)
        self.assertEqual(migration._decrypt(migration._profile(self.profile), SOURCE_SECRET), NEW_SECRET)
        self.agent_path.unlink()
        self.assertEqual(self.effective(), (migration.TARGET_APP_ID, NEW_SECRET))
        self.store_settings({"feishu_app_id": migration.TARGET_APP_ID, "feishu_app_secret": NEW_SECRET})
        self.assertEqual(migration.prepare_deployment_profile(), self.profile)

    def test_first_profile_build_after_config_import_uses_trusted_legacy_inputs_only(self):
        module = self.config_module()
        self.agent()
        store = self.store_settings({**self.source, "feishu_app_id": migration.TARGET_APP_ID,
                                     "feishu_app_secret": NEW_SECRET, migration.MIGRATION_MARKER: 2})
        atomic_json(self.root / "config.json", self.source)
        manager = module.ConfigManager()
        self.assertEqual(manager.app_id, migration.TARGET_APP_ID)
        self.assertEqual(store.get_settings()["feishu_app_id"], migration.TARGET_APP_ID)
        self.assertFalse(self.profile.exists())
        migration.prepare_deployment_profile()
        self.agent_path.unlink()
        self.assertEqual(self.effective(), (migration.TARGET_APP_ID, NEW_SECRET))
        self.profile.unlink()
        atomic_json(self.root / "config.json", {"feishu_app_id": migration.TARGET_APP_ID})
        atomic_json(migration.PACKAGED_CONFIG_PATH, self.source)
        migration.prepare_deployment_profile()
        self.agent_path.unlink()
        self.assertEqual(self.effective(), (migration.TARGET_APP_ID, NEW_SECRET))

    def test_build_uses_authoritative_qt_target_secret_instead_of_stale_agent(self):
        qt_secret = "fixture-qt-current-secret"
        self.agent(secret_cipher=protect("fixture-stale-agent-secret"))
        self.store_settings({**self.source, "feishu_app_id": migration.TARGET_APP_ID,
                             "feishu_app_secret": qt_secret})
        atomic_json(self.root / "config.json", self.source)
        migration.prepare_deployment_profile()
        self.assertEqual(migration._decrypt(migration._profile(self.profile), SOURCE_SECRET), qt_secret)
        self.agent_path.unlink()
        self.assertEqual(self.effective(), (migration.TARGET_APP_ID, qt_secret))

    def test_build_reads_large_legacy_settings_without_relaxing_profile_limit(self):
        self.agent()
        settings = {**self.source, "custom_setting": "x" * 25000}
        atomic_json(self.root / "config.json", settings)
        self.assertEqual(migration.prepare_deployment_profile(), self.profile)
        self.assertEqual(migration._decrypt(migration._profile(self.profile), SOURCE_SECRET), NEW_SECRET)

    def test_build_for_future_qt_app_reuses_retirement_profile_or_needs_none(self):
        self.store_settings({"feishu_app_id": "cli_0123456789abcdef", "feishu_app_secret": "fixture-future-secret",
                             migration.MIGRATION_MARKER: 2})
        with patch.object(migration, "_agent_secret", side_effect=AssertionError("no agent authority")):
            self.assertIsNone(migration.prepare_deployment_profile())
            self.assertFalse(self.profile.exists())
            self.build()
            before = self.profile.read_bytes()
            self.assertEqual(migration.prepare_deployment_profile(), self.profile)
            self.assertEqual(self.profile.read_bytes(), before)
            self.profile.write_bytes(b'{"unfinished":')
            with self.assertLogs(migration._LOG, level="WARNING"):
                self.assertIsNone(migration.prepare_deployment_profile())
        self.assertFalse(self.agent_path.exists())

    def test_qt_load_replaces_stale_agent_secret_preserving_disable_bridge_and_custom_fields(self):
        module = self.config_module()
        previous = self.agent(secret_cipher=protect("fixture-stale-agent-secret"), enabled=False)
        self.store_settings({**self.source, "feishu_app_id": migration.TARGET_APP_ID,
                             "feishu_app_secret": NEW_SECRET})
        manager = module.ConfigManager()
        saved = json.loads(self.agent_path.read_text())
        self.assertEqual(manager.app_secret, NEW_SECRET)
        self.assertEqual(unprotect(saved["secret_cipher"]), NEW_SECRET)
        self.assertEqual(saved["bridge_cipher"], previous["bridge_cipher"])
        self.assertFalse(saved["enabled"])
        self.assertEqual(saved["custom"], previous["custom"])
        self.assertNotIn("isolated_inbox", saved)

    def test_qt_save_supports_future_apps_and_rotates_bridge_only_for_app_change(self):
        module = self.config_module()
        previous = self.agent(enabled=False)
        store = self.store_settings({**self.source, "feishu_app_id": migration.TARGET_APP_ID,
                                     "feishu_app_secret": NEW_SECRET})
        manager = module.ConfigManager()
        future_app = "cli_0123456789abcdef"
        self.assertTrue(manager.save(app_id=future_app, app_secret="fixture-future-secret"))
        first = json.loads(self.agent_path.read_text())
        self.assertEqual((first["app_id"], unprotect(first["secret_cipher"])), (future_app, "fixture-future-secret"))
        self.assertNotEqual(first["bridge_cipher"], previous["bridge_cipher"])
        self.assertTrue(first["isolated_inbox"])
        self.assertFalse(first["enabled"])
        self.assertEqual(manager.user_token, "")
        self.assertEqual(store.get_settings()["feishu_app_id"], future_app)
        self.assertTrue(manager.save(app_secret="fixture-future-rotated", user_token="fixture-stale-token", token_expire_time=9999999999))
        second = json.loads(self.agent_path.read_text())
        self.assertEqual(second["bridge_cipher"], first["bridge_cipher"])
        self.assertEqual(unprotect(second["secret_cipher"]), "fixture-future-rotated")
        self.assertFalse(second["enabled"])
        self.assertEqual(manager.user_token, "")
        before = self.agent_path.read_bytes()
        self.assertTrue(manager.save(user_token="fixture-current-token", token_expire_time=9999999999))
        manager.load()
        self.assertEqual(manager.user_token, "fixture-current-token")
        self.assertEqual(self.agent_path.read_bytes(), before)

    def test_qt_external_credential_change_reloads_agent_and_invalidates_cached_token(self):
        module = self.config_module()
        self.agent(enabled=False)
        store = self.store_settings({**self.source, "feishu_app_id": migration.TARGET_APP_ID,
                                     "feishu_app_secret": NEW_SECRET})
        manager = module.ConfigManager()
        future_app = "cli_fedcba9876543210"
        writer = module.ConfigManager()
        self.assertTrue(writer.save(app_id=future_app, app_secret="fixture-external-secret"))
        manager.load()
        saved = json.loads(self.agent_path.read_text())
        self.assertEqual(saved["app_id"], future_app)
        self.assertEqual(unprotect(saved["secret_cipher"]), "fixture-external-secret")
        self.assertFalse(saved["enabled"])
        self.assertEqual((manager.user_token, manager.token_expire_time), ("", 0))
        self.assertEqual(store.get_settings()["feishu_user_token"], "")

    def current_config(self):
        module = self.config_module()
        self.agent(enabled=False)
        store = self.store_settings({**self.source, "feishu_app_id": migration.TARGET_APP_ID,
                                     "feishu_app_secret": NEW_SECRET})
        return module, store

    def test_stale_non_auth_save_and_no_argument_save_preserve_current_settings(self):
        module, store = self.current_config()
        stale, writer = module.ConfigManager(), module.ConfigManager()
        self.assertTrue(writer.save(app_id="cli_fixturefuture", app_secret="fixture-future-secret",
                                    table_id_weibao="fixture-current-table", disable_alerts=True))
        self.assertTrue(writer.save_tenant_token("fixture-current-token", 9999999999,
                                               expected_credentials=(writer.app_id, writer.app_secret)))
        before = store.get_settings()
        mirror = self.agent_path.read_bytes()
        self.assertTrue(stale.save(disable_speech=False))
        self.assertEqual(store.get_settings(), {**before, "disable_speech": False})
        self.assertEqual(self.agent_path.read_bytes(), mirror)
        self.assertEqual(stale.app_id, "cli_fixturefuture")
        self.assertEqual(stale.table_id_weibao, "fixture-current-table")
        stale.table_id_weibao = "fixture-local-unsaved"
        stale.app_id = SOURCE_APP_ID
        before = store.get_settings()
        self.assertTrue(stale.save())
        self.assertEqual(store.get_settings(), before)
        self.assertEqual(self.agent_path.read_bytes(), mirror)

    def test_stale_token_saves_fail_cas_without_reverting_credentials_or_metadata(self):
        module, store = self.current_config()
        stale, writer = module.ConfigManager(), module.ConfigManager()
        old_pair = stale.app_id, stale.app_secret
        self.assertTrue(writer.save(app_id="cli_fixturefuture", app_secret="fixture-future-secret",
                                    table_id_weibao="fixture-current-table"))
        before = store.get_settings()
        mirror = self.agent_path.read_bytes()
        self.assertFalse(stale.save_tenant_token("fixture-late-token", 9999999999, expected_credentials=old_pair))
        self.assertFalse(stale.save(user_token="fixture-late-token", token_expire_time=9999999999))
        self.assertEqual(store.get_settings(), before)
        self.assertEqual(self.agent_path.read_bytes(), mirror)
        current_pair = writer.app_id, writer.app_secret
        self.assertTrue(stale.save_tenant_token("fixture-current-token", 9999999999, expected_credentials=current_pair))
        self.assertEqual(store.get_settings(), {**before, "feishu_user_token": "fixture-current-token",
                                              "token_expire_time": 9999999999})
        self.assertEqual(self.agent_path.read_bytes(), mirror)
        self.assertTrue(writer.save(app_secret="fixture-rotated-secret"))
        before = store.get_settings()
        self.assertFalse(stale.save_tenant_token("fixture-late-token", 9999999999, expected_credentials=current_pair))
        self.assertEqual(store.get_settings(), before)

    def test_stale_secret_only_save_merges_with_canonical_app_not_local_app(self):
        module, store = self.current_config()
        stale, writer = module.ConfigManager(), module.ConfigManager()
        self.assertTrue(writer.save(app_id="cli_fixturefuture", app_secret="fixture-future-secret"))
        before = json.loads(self.agent_path.read_text())
        self.assertTrue(stale.save(app_secret="fixture-current-rotation"))
        saved = json.loads(self.agent_path.read_text())
        self.assertEqual(store.get_settings()["feishu_app_id"], "cli_fixturefuture")
        self.assertEqual(unprotect(saved["secret_cipher"]), "fixture-current-rotation")
        self.assertEqual(saved["bridge_cipher"], before["bridge_cipher"])
        self.assertFalse(saved["enabled"])

    def test_stale_load_snapshot_cannot_revert_authority_or_agent_bridge(self):
        module, store = self.current_config()
        stale, writer = module.ConfigManager(), module.ConfigManager()
        read = stale._load_config_from_sqlite
        captured = []
        def interleaved():
            snapshot = read()
            self.assertTrue(writer.save(app_id="cli_fixturefuture", app_secret="fixture-future-secret"))
            self.assertTrue(writer.save_tenant_token("fixture-current-token", 9999999999,
                                                   expected_credentials=(writer.app_id, writer.app_secret)))
            captured.append(self.agent_path.read_bytes())
            return snapshot
        with patch.object(stale, "_load_config_from_sqlite", side_effect=interleaved):
            stale.load()
        self.assertEqual(stale.app_id, "cli_fixturefuture")
        self.assertEqual(store.get_settings()["feishu_app_id"], "cli_fixturefuture")
        self.assertEqual(store.get_settings()["feishu_user_token"], "fixture-current-token")
        self.assertEqual(self.agent_path.read_bytes(), captured[-1])
        self.assertFalse(migration.sync_agent_config(migration.TARGET_APP_ID, NEW_SECRET, data_root=self.root))
        self.assertEqual(self.agent_path.read_bytes(), captured[-1])

    def test_agent_sync_holds_sqlite_writer_lock_through_atomic_mirror_write(self):
        _, store = self.current_config()
        store.put_settings({migration.MIGRATION_MARKER: 2})
        self.agent(app_id="cli_fixtureprevious")
        original = migration.atomic_json
        script = """
import json, sqlite3, sys, time
with sqlite3.connect(sys.argv[1], timeout=5) as connection:
    print('ready', flush=True)
    connection.execute('BEGIN IMMEDIATE')
    for key, value in {'feishu_app_id': 'cli_fixturefuture', 'feishu_app_secret': 'fixture-future-secret'}.items():
        connection.execute('INSERT OR REPLACE INTO settings VALUES (?, ?, ?)', (key, json.dumps(value), time.time()))
    connection.commit()
print('done', flush=True)
"""
        processes = []
        def atomic(path, value):
            process = subprocess.Popen([sys.executable, "-B", "-c", script, str(store.db_path)],
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            processes.append(process)
            self.assertEqual(process.stdout.readline().strip(), "ready")
            with self.assertRaises(subprocess.TimeoutExpired, msg="Qt writer entered during mirror replacement"):
                process.wait(timeout=.1)
            original(path, value)
        try:
            with patch.object(migration, "atomic_json", side_effect=atomic):
                self.assertTrue(migration.sync_agent_config(migration.TARGET_APP_ID, NEW_SECRET, data_root=self.root))
            output, error = processes[0].communicate(timeout=5)
            self.assertEqual(processes[0].returncode, 0, error)
            self.assertEqual(output.strip(), "done")
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                process.communicate()
        self.assertTrue(migration.sync_agent_config(migration.TARGET_APP_ID, NEW_SECRET, data_root=self.root))
        self.assertEqual(json.loads(self.agent_path.read_text())["app_id"], "cli_fixturefuture")
        before = self.agent_path.read_bytes()
        self.assertFalse(migration.sync_agent_config(migration.TARGET_APP_ID, NEW_SECRET, data_root=self.root))
        self.assertEqual(self.agent_path.read_bytes(), before)

    def test_auth_sync_does_not_initialize_full_state_schema_per_call(self):
        module, store = self.current_config()
        local = module.ConfigManager()
        from lan_bitable_template_portal.state_store import LanPortalStateStore
        with patch.object(LanPortalStateStore, "_ensure_schema_locked", side_effect=AssertionError("full schema in auth path")):
            for _ in range(3):
                self.assertEqual(local.refresh_credentials()[0], migration.TARGET_APP_ID)
                self.assertFalse(migration.sync_agent_config(migration.TARGET_APP_ID, NEW_SECRET, data_root=self.root))
        isolated = self.root / "bootstrap-only"
        self.build()
        pair = migration.effective_credentials(SOURCE_APP_ID, SOURCE_SECRET, data_root=isolated,
                                               profile_path=self.profile)
        self.assertEqual(pair[0], migration.TARGET_APP_ID)
        with closing(sqlite3.connect(isolated / "lan_portal_state.sqlite3")) as connection:
            tables = connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        self.assertEqual(tables, [("settings",)])

    def test_refresh_credentials_reads_only_four_rows_without_mirror_dependency(self):
        module, store = self.current_config()
        local, writer = module.ConfigManager(), module.ConfigManager()
        self.assertTrue(writer.save(app_id="cli_fixturefuture", app_secret="fixture-future-secret"))
        self.assertTrue(writer.save_tenant_token("fixture-current-token", 9999999999,
                                               expected_credentials=(writer.app_id, writer.app_secret)))
        queries = []
        connect = module.sqlite3.connect
        def traced(*args, **kwargs):
            connection = connect(*args, **kwargs)
            connection.set_trace_callback(queries.append)
            return connection
        before = self.agent_path.read_bytes()
        with patch.object(module.sqlite3, "connect", side_effect=traced), \
                patch.object(module, "sync_agent_config", side_effect=AssertionError("refresh must not touch mirror")):
            pair = local.refresh_credentials()
        self.assertEqual(pair[0], "cli_fixturefuture")
        self.assertEqual(local.user_token, "fixture-current-token")
        self.assertEqual(local.token_expire_time, 9999999999)
        self.assertEqual(len(queries), 1)
        self.assertIn("WHERE key IN", queries[0])
        self.assertEqual(self.agent_path.read_bytes(), before)

    def test_refresh_credentials_sql_or_format_failure_raises_and_clears_local_credentials(self):
        module, store = self.current_config()
        local = module.ConfigManager()
        with patch.object(module.sqlite3, "connect", side_effect=sqlite3.OperationalError(NEW_SECRET)):
            with self.assertRaisesRegex(migration.FeishuAppMigrationError, "settings_read_failed") as caught:
                local.refresh_credentials()
        self.assertNotIn(NEW_SECRET, str(caught.exception))
        self.assertEqual((local.app_id, local.app_secret, local.user_token, local.token_expire_time), ("", "", "", 0))
        store.put_settings({"feishu_app_secret": {"invalid": True}})
        with self.assertRaisesRegex(migration.FeishuAppMigrationError, "settings_credentials_invalid"):
            local.refresh_credentials()
        store.put_settings(self.source)
        with closing(sqlite3.connect(store.db_path)) as connection, connection:
            connection.execute("DELETE FROM settings WHERE key=?", (migration.MIGRATION_MARKER,))
        with self.assertRaisesRegex(migration.FeishuAppMigrationError, "migration_pending"):
            local.refresh_credentials()

    def test_old_migration_mirrors_only_committed_qt_credentials_and_rolls_back_failed_write(self):
        module = self.config_module()
        self.build()
        self.agent()
        store = self.store_settings(self.source)
        original = migration.atomic_json
        seen = []
        self.agent(app_id=SOURCE_APP_ID)
        def atomic(path, value):
            seen.append(store.get_settings()["feishu_app_id"])
            original(path, value)
        with patch.object(migration, "atomic_json", side_effect=atomic):
            manager = module.ConfigManager()
        self.assertEqual(manager.app_id, migration.TARGET_APP_ID)
        self.assertEqual(seen, [migration.TARGET_APP_ID])
        store.put_settings(self.source)
        with closing(sqlite3.connect(store.db_path)) as connection, connection:
            connection.execute("DELETE FROM settings WHERE key=?", (migration.MIGRATION_MARKER,))
        before = self.agent_path.read_bytes()
        write = migration.write_settings
        def fail_after_write(connection, value):
            write(connection, value)
            raise sqlite3.OperationalError("fixture failed transaction")
        with patch.object(migration, "write_settings", side_effect=fail_after_write):
            manager.load()
        self.assertEqual(manager.app_id, "")
        self.assertEqual(store.get_settings(), self.source)
        self.assertEqual(self.agent_path.read_bytes(), before)

    def test_retired_caller_uses_current_qt_future_app_without_agent_or_profile_fallback(self):
        future_app, qt_secret = "cli_0123456789abcdef", "fixture-qt-future-secret"
        self.store_settings({**self.source, "feishu_app_id": future_app, "feishu_app_secret": qt_secret,
                             migration.MIGRATION_MARKER: 2})
        self.agent(enabled=False)
        with patch.object(migration, "_agent_secret", side_effect=AssertionError("no agent authority")), \
                patch.object(migration, "_profile", side_effect=AssertionError("no bootstrap after Qt migration")):
            self.assertEqual(self.effective("unneeded-old-secret"), (future_app, qt_secret))
        saved = json.loads(self.agent_path.read_text())
        self.assertEqual((saved["app_id"], unprotect(saved["secret_cipher"])), (future_app, qt_secret))
        self.assertFalse(saved["enabled"])

    def test_config_load_cleans_only_legacy_auth_fields_after_profile_and_keeps_business_history(self):
        module = self.config_module()
        self.build()
        self.agent(enabled=False)
        store = self.store_settings(self.source)
        history = {"turns": [{"question": "fixture-preserved-history"}]}
        store.put_document("lighthouse_ai", "conversation:fixture", history)
        atomic_json(self.root / "config.json", self.source)
        atomic_json(migration.PACKAGED_CONFIG_PATH, self.source)
        convergence_path = self.root / "plan_convergence" / "feishu.json"
        convergence = {"app_id": SOURCE_APP_ID, "app_secret": SOURCE_SECRET,
                       "tenant_access_token": "fixture-obsolete-token", "app_token": "fixture-business-base",
                       "table_id": "fixture-table", "enabled": False}
        atomic_json(convergence_path, convergence)
        manager = module.ConfigManager()
        self.assertEqual(manager.app_id, migration.TARGET_APP_ID)
        self.assertEqual(store.get_settings()["feishu_app_id"], migration.TARGET_APP_ID)
        for path in (self.root / "config.json", migration.PACKAGED_CONFIG_PATH, convergence_path):
            raw = path.read_text()
            for retired in (SOURCE_APP_ID, SOURCE_SECRET, "fixture-obsolete-token"):
                self.assertNotIn(retired, raw)
            self.assertNotIn(NEW_SECRET, raw)
            self.assertEqual(json.loads(raw)["feishu_credentials_source"], "qt_sqlite")
        saved = json.loads(convergence_path.read_text())
        self.assertEqual(saved["app_token"], convergence["app_token"])
        self.assertEqual(saved["table_id"], convergence["table_id"])
        self.assertFalse(saved["enabled"])
        self.assertEqual(store.get_document("lighthouse_ai", "conversation:fixture"), history)
        self.assertEqual(store.get_settings()["custom_setting"], self.source["custom_setting"])
        before = convergence_path.read_bytes()
        self.assertTrue(manager.save(disable_speech=False))
        self.assertEqual(convergence_path.read_bytes(), before)

    def test_cleanup_waits_for_profile_and_preserves_another_apps_config(self):
        atomic_json(self.root / "config.json", self.source)
        before = (self.root / "config.json").read_bytes()
        self.assertFalse(migration.cleanup_legacy_configs(migration.TARGET_APP_ID, data_root=self.root))
        self.assertEqual((self.root / "config.json").read_bytes(), before)
        self.build()
        another = {"app_id": "cli_0123456789abcdef", "app_secret": "fixture-other-app-secret", "custom": "keep"}
        path = self.root / "plan_convergence" / "feishu.json"
        atomic_json(path, another)
        self.assertTrue(migration.cleanup_legacy_configs(migration.TARGET_APP_ID, data_root=self.root))
        self.assertEqual(json.loads(path.read_text()), another)

    def test_cleanup_of_large_legacy_config_preserves_unrelated_settings(self):
        module = self.config_module()
        self.build()
        self.agent()
        store = self.store_settings(self.source)
        settings = {**self.source, "custom_setting": "x" * 25000}
        atomic_json(self.root / "config.json", settings)
        manager = module.ConfigManager()
        saved = json.loads((self.root / "config.json").read_text())
        self.assertEqual(saved["custom_setting"], settings["custom_setting"])
        self.assertNotIn("feishu_app_secret", saved)
        self.assertEqual((manager.app_id, manager.app_secret), (migration.TARGET_APP_ID, NEW_SECRET))
        self.assertEqual(manager.feishu_app_migration_error, "")
        self.assertEqual(store.get_settings()["feishu_app_id"], migration.TARGET_APP_ID)

    def test_unreadable_legacy_copy_does_not_disable_qt_or_prevent_other_cleanup(self):
        module = self.config_module()
        self.build()
        self.agent()
        self.store_settings({**self.source, "feishu_app_id": migration.TARGET_APP_ID,
                             "feishu_app_secret": NEW_SECRET, migration.MIGRATION_MARKER: 2})
        path = self.root / "config.json"
        for raw in (b'{"unfinished":', b" " * (migration.CONFIG_MAX_BYTES + 1)):
            with self.subTest(size=len(raw)):
                path.write_bytes(raw)
                atomic_json(migration.PACKAGED_CONFIG_PATH, self.source)
                with self.assertLogs(migration._LOG, level="WARNING") as logged:
                    manager = module.ConfigManager()
                    self.assertTrue(manager.save(disable_speech=False))
                self.assertEqual((manager.app_id, manager.app_secret, manager.user_token),
                                 (migration.TARGET_APP_ID, NEW_SECRET, self.source["feishu_user_token"]))
                self.assertEqual(manager.feishu_app_migration_error, "")
                self.assertEqual(path.read_bytes(), raw)
                self.assertNotIn("feishu_app_id", json.loads(migration.PACKAGED_CONFIG_PATH.read_text()))
                self.assertIn("data_config", str(logged.output))
                self.assertNotIn(SOURCE_SECRET, str(logged.output))

    def test_failed_cleanup_write_is_noncritical_and_does_not_log_exception_secrets(self):
        module = self.config_module()
        self.build()
        self.agent()
        self.store_settings({**self.source, "feishu_app_id": migration.TARGET_APP_ID, "feishu_app_secret": NEW_SECRET})
        atomic_json(self.root / "config.json", self.source)
        atomic_json(migration.PACKAGED_CONFIG_PATH, self.source)
        original = migration.atomic_json
        def write(path, value):
            if path == self.root / "config.json":
                raise OSError(SOURCE_SECRET)
            return original(path, value)
        with patch.object(migration, "atomic_json", side_effect=write), self.assertLogs(migration._LOG, level="WARNING") as logged:
            manager = module.ConfigManager()
            self.assertTrue(manager.save(disable_speech=False))
        self.assertEqual((manager.app_id, manager.app_secret), (migration.TARGET_APP_ID, NEW_SECRET))
        self.assertEqual(manager.feishu_app_migration_error, "")
        self.assertNotIn(SOURCE_SECRET, str(logged.output))
        self.assertNotIn("feishu_app_id", json.loads(migration.PACKAGED_CONFIG_PATH.read_text()))

    def test_invalid_unused_profile_does_not_disable_valid_qt_credentials(self):
        module = self.config_module()
        self.build()
        self.profile.write_bytes(b'{"unfinished":')
        self.agent()
        self.store_settings({**self.source, "feishu_app_id": migration.TARGET_APP_ID,
                             "feishu_app_secret": NEW_SECRET, migration.MIGRATION_MARKER: 2})
        atomic_json(self.root / "config.json", self.source)
        before = (self.root / "config.json").read_bytes()
        with self.assertLogs(migration._LOG, level="WARNING"):
            manager = module.ConfigManager()
            self.assertTrue(manager.save(disable_speech=False))
        self.assertEqual((manager.app_id, manager.app_secret), (migration.TARGET_APP_ID, NEW_SECRET))
        self.assertEqual((self.root / "config.json").read_bytes(), before)
        self.assertEqual(manager.feishu_app_migration_error, "")

    def test_config_load_migrates_only_credentials_with_sqlite_priority(self):
        module = self.config_module()
        self.build()
        self.agent(enabled=False)
        store = self.store_settings(self.source)
        history = {"turns": [{"question": "fixture-history"}]}
        store.put_document("lighthouse_ai", "conversation:fixture", history)
        atomic_json(self.root / "config.json", {**self.source, "table_id_weibao": "wrong-json-table"})
        manager = module.ConfigManager()
        expected = {**self.source, "feishu_app_id": migration.TARGET_APP_ID, "feishu_app_secret": NEW_SECRET,
                    "feishu_user_token": "", "token_expire_time": 0, migration.MIGRATION_MARKER: 2}
        self.assertEqual(store.get_settings(), expected)
        self.assertEqual((manager.app_id, manager.app_secret, manager.user_token, manager.token_expire_time),
                         (migration.TARGET_APP_ID, NEW_SECRET, "", 0))
        self.assertEqual(manager.table_id_weibao, "fixture-table")
        self.assertEqual(store.get_document("lighthouse_ai", "conversation:fixture"), history)
        self.assertTrue(manager.save(user_token="fixture-new-token", token_expire_time=9999999999))
        self.assertEqual(manager.user_token, "fixture-new-token")
        self.assertFalse(manager.save_tenant_token("obsolete-token", 9999999999,
                                                  expected_credentials=(SOURCE_APP_ID, SOURCE_SECRET)))
        self.assertEqual(manager.app_id, migration.TARGET_APP_ID)
        self.assertEqual(manager.user_token, "fixture-new-token")
        self.assertEqual(store.get_settings()["custom_setting"], self.source["custom_setting"])

    def test_config_failure_clears_live_credentials_but_retains_original_storage(self):
        module = self.config_module()
        store = self.store_settings(self.source)
        manager = module.ConfigManager()
        self.assertEqual((manager.app_id, manager.app_secret, manager.user_token, manager.token_expire_time), ("", "", "", 0))
        self.assertEqual(manager.feishu_app_migration_error, "profile_missing")
        self.assertEqual(store.get_settings(), self.source)
        self.assertFalse(manager.save(app_id=SOURCE_APP_ID, app_secret=SOURCE_SECRET))
        self.assertEqual(store.get_settings(), self.source)
        self.assertEqual(manager.table_id_weibao, "fixture-table")

    def test_config_migration_persistence_failure_is_closed(self):
        module = self.config_module()
        self.agent()
        store = self.store_settings(self.source)
        with patch.object(module.ConfigManager, "_save_config_to_sqlite", return_value=False):
            manager = module.ConfigManager()
        self.assertEqual(manager.app_id, "")
        self.assertEqual(manager.user_token, "")
        self.assertEqual(manager.feishu_app_migration_error, "settings_persist_failed")
        self.assertEqual(store.get_settings(), self.source)

    def test_failed_reload_never_retains_live_retired_credentials_or_logs_secrets(self):
        module = self.config_module()
        manager = module.ConfigManager()
        manager.app_id, manager.app_secret = SOURCE_APP_ID, SOURCE_SECRET
        manager.user_token, manager.token_expire_time = "fixture-obsolete-token", 9999999999
        with patch.object(manager, "_load_config_from_sqlite", side_effect=OSError(SOURCE_SECRET)), \
                patch.object(module, "log_error") as log:
            manager.load()
        self.assertEqual((manager.app_id, manager.app_secret, manager.user_token, manager.token_expire_time), ("", "", "", 0))
        self.assertNotIn(SOURCE_SECRET, str(log.call_args_list))

    def test_config_fixture_credentials_keep_the_existing_token(self):
        module = self.config_module()
        value = {**self.source, "feishu_app_id": "fixture-app", "feishu_app_secret": "fixture-secret"}
        store = self.store_settings(value)
        manager = module.ConfigManager()
        self.assertEqual((manager.app_id, manager.app_secret, manager.user_token),
                         ("fixture-app", "fixture-secret", value["feishu_user_token"]))
        self.assertTrue(manager.save(disable_speech=False))
        self.assertEqual(manager.user_token, value["feishu_user_token"])
        self.assertEqual(store.get_settings()["custom_setting"], value["custom_setting"])


if __name__ == "__main__":
    unittest.main()

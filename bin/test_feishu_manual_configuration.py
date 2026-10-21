"""Manual Qt credential repair: temporary fixtures, mocked validation, no network."""
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bin import test_feishu_app_migration as fixtures
from upload_event_module.services import feishu_app_migration as migration

MANUAL_SECRET = "fixture-user-entered-new-secret"
VALIDATION_TOKEN = "fixture-validation-only-token"


class FeishuManualConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch.dict(os.environ, {"CLIPFLOW_DISABLE_SQLITE_WRITE_WORKER": "1"}))
        for target in ("socket.socket.connect", "socket.socket.connect_ex", "socket.socket.sendto", "socket.getaddrinfo"):
            blocked = self.enterContext(patch(target, side_effect=AssertionError("Network forbidden")))
            self.addCleanup(blocked.assert_not_called)
        self.fixture = fixtures.MigrationTests(methodName="runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.root = self.fixture.root
        self.module = self.fixture.config_module()
        from upload_event_module.services.feishu_token_manager import token_manager
        self.verify = self.enterContext(patch.object(token_manager, "get_tenant_token_for_credentials",
                                                     return_value=(VALIDATION_TOKEN, "")))
        self.history = {"turns": [{"role": "user", "content": "fixture-history"}], "permissions": {"admin": False}}
        self.permissions = {"fixture-person": {"modules": ["assistant"], "disabled": False}}
        self.session = {"fixture-session": {"person": "fixture-person", "expires": 9999999999}}

    def configured(self, *, profile="missing", initial=None):
        if profile == "corrupt":
            self.fixture.profile.parent.mkdir(parents=True, exist_ok=True)
            self.fixture.profile.write_bytes(b'{"unfinished":')
        elif profile == "installed":
            self.fixture.legacy_profile()
            migration.upgrade_deployment_profile(profile_path=self.fixture.profile)
        elif profile == "valid":
            self.fixture.build()
        self.store = self.fixture.store_settings(self.fixture.source if initial is None else initial)
        self.store.put_document("lighthouse_ai", "fixture-person", self.history)
        self.store.put_document("permissions", "fixture-person", self.permissions)
        self.store.put_document("portal_auth", "fixture-session", self.session)
        self.manager = self.module.ConfigManager()
        self.before = self.store.get_settings() or {}

    def rows(self):
        with closing(sqlite3.connect(self.root / "lan_portal_state.sqlite3")) as connection:
            return connection.execute("SELECT key, value_json, updated_at FROM settings ORDER BY key").fetchall()

    def save(self, secret=MANUAL_SECRET):
        return self.manager.save(app_id=migration.TARGET_APP_ID, app_secret=secret)

    def assert_preserved_documents(self):
        self.assertEqual(self.store.get_document("lighthouse_ai", "fixture-person"), self.history)
        self.assertEqual(self.store.get_document("permissions", "fixture-person"), self.permissions)
        self.assertEqual(self.store.get_document("portal_auth", "fixture-session"), self.session)

    def assert_saved(self, *, check_mirror=True):
        expected = {**self.before, "feishu_app_id": migration.TARGET_APP_ID,
                    "feishu_app_secret": MANUAL_SECRET, "feishu_user_token": "", "token_expire_time": 0,
                    migration.MIGRATION_MARKER: 2}
        self.assertEqual(self.store.get_settings(), expected)
        self.assertEqual((self.manager.app_id, self.manager.app_secret, self.manager.user_token,
                          self.manager.token_expire_time), (migration.TARGET_APP_ID, MANUAL_SECRET, "", 0))
        self.assertEqual(self.manager.feishu_app_migration_error, "")
        self.assertNotIn(VALIDATION_TOKEN, json.dumps(expected))
        self.assert_preserved_documents()
        if check_mirror:
            agent = json.loads(self.fixture.agent_path.read_text(encoding="utf-8"))
            self.assertEqual(agent["app_id"], migration.TARGET_APP_ID)
            self.assertEqual(fixtures.unprotect(agent["secret_cipher"]), MANUAL_SECRET)
            self.assertNotIn(MANUAL_SECRET, self.fixture.agent_path.read_text(encoding="utf-8"))

    def assert_verified_save(self):
        self.assertTrue(self.save(), self.manager.last_save_error)
        self.verify.assert_called_once_with(migration.TARGET_APP_ID, MANUAL_SECRET)
        self.assert_saved()

    def test_missing_profile_manual_repair_clears_old_token_and_syncs_agent(self):
        self.configured()
        self.assertTrue(self.manager.feishu_app_migration_error)
        self.assert_verified_save()

    def test_corrupt_profile_cannot_block_verified_manual_repair(self):
        self.configured(profile="corrupt")
        self.assertTrue(self.manager.feishu_app_migration_error)
        self.assert_verified_save()

    def test_installed_profile_with_different_old_source_secret_can_be_repaired(self):
        self.configured(profile="installed", initial={**self.fixture.source,
                        "feishu_app_secret": "fixture-unrelated-old-source-secret"})
        self.assertTrue(self.manager.feishu_app_migration_error)
        self.assert_verified_save()

    def test_target_with_old_secret_accepts_verified_candidate_not_old_fingerprint(self):
        self.configured(profile="valid", initial={**self.fixture.source, "feishu_app_id": migration.TARGET_APP_ID})
        self.assertTrue(self.manager.feishu_app_migration_error)
        self.assertNotEqual(migration._fingerprint(MANUAL_SECRET), migration._fingerprint(fixtures.NEW_SECRET))
        self.assert_verified_save()

    def test_empty_configuration_accepts_verified_manual_credentials(self):
        self.configured(initial={})
        self.assertEqual(self.manager.app_id, "")
        self.assert_verified_save()

    def test_empty_configuration_with_corrupt_old_profile_accepts_manual_credentials(self):
        self.configured(profile="corrupt", initial={})
        self.assertEqual(self.manager.app_id, "")
        self.assert_verified_save()

    def test_completed_qt_rotation_retains_existing_behavior_without_old_profile(self):
        self.configured(profile="corrupt", initial={**self.fixture.source,
                        "feishu_app_id": migration.TARGET_APP_ID, migration.MIGRATION_MARKER: 2})
        self.assertEqual(self.manager.feishu_app_migration_error, "")
        self.assertTrue(self.save(), self.manager.last_save_error)
        self.verify.assert_not_called()
        self.assert_saved()

    def test_completed_qt_clear_restart_then_late_validation_mock_saves_new_credentials(self):
        self.configured(initial={**self.fixture.source, "feishu_app_id": migration.TARGET_APP_ID,
                        "feishu_app_secret": fixtures.NEW_SECRET, migration.MIGRATION_MARKER: 2})
        self.assertEqual(self.manager.feishu_app_migration_error, "")
        self.assertEqual(self.manager.user_token, self.fixture.source["feishu_user_token"])
        self.assertTrue(self.manager.save(app_id="", app_secret=""), self.manager.last_save_error)
        cleared = {**self.before, "feishu_app_id": "", "feishu_app_secret": "",
                   "feishu_user_token": "", "token_expire_time": 0}
        self.assertEqual(self.store.get_settings(), cleared)
        self.assertEqual(cleared[migration.MIGRATION_MARKER], 2)
        self.assert_preserved_documents()

        self.manager = self.module.ConfigManager()
        self.assertEqual((self.manager.app_id, self.manager.app_secret, self.manager.user_token,
                          self.manager.token_expire_time), ("", "", "", 0))
        self.assertEqual(self.manager.feishu_app_migration_error, "")
        self.verify.assert_not_called()
        self.before = cleared
        from upload_event_module.services.feishu_token_manager import token_manager
        # Install this exact mock only after both fixture setup and the simulated restart.
        with patch.object(token_manager, "get_tenant_token_for_credentials",
                          return_value=(VALIDATION_TOKEN, "")) as verify_after_restart:
            self.assertTrue(self.save(), self.manager.last_save_error)
            verify_after_restart.assert_called_once_with(migration.TARGET_APP_ID, MANUAL_SECRET)
        self.verify.assert_not_called()
        self.assert_saved()

    def test_malformed_old_credential_json_is_repaired_after_direct_verification(self):
        self.configured(initial={**self.fixture.source, "feishu_app_id": migration.TARGET_APP_ID,
                        migration.MIGRATION_MARKER: 2})
        with migration.qt_settings_transaction(self.root) as connection:
            connection.executemany("UPDATE settings SET value_json=? WHERE key=?",
                                   [("{", key) for key in migration.CREDENTIAL_FIELDS])
        damaged = {key: raw for key, raw, _ in self.rows()}
        self.assertTrue(all(damaged[key] == "{" for key in migration.CREDENTIAL_FIELDS))
        self.manager = self.module.ConfigManager()
        self.assertEqual(self.manager.app_id, "")
        self.assertTrue(self.manager.feishu_app_migration_error)
        self.verify.assert_not_called()
        from upload_event_module.services.feishu_token_manager import token_manager
        with patch.object(token_manager, "get_tenant_token_for_credentials",
                          return_value=(VALIDATION_TOKEN, "")) as verify_after_restart:
            self.assertTrue(self.save(), self.manager.last_save_error)
            verify_after_restart.assert_called_once_with(migration.TARGET_APP_ID, MANUAL_SECRET)
        self.verify.assert_not_called()
        self.assert_saved()

    def test_verified_save_survives_restart_and_preserves_permissions_history_and_session(self):
        self.configured(profile="corrupt")
        self.assert_verified_save()
        self.assertTrue(self.manager.save_tenant_token("fixture-current-tenant-token", 9999999999,
                        expected_credentials=(migration.TARGET_APP_ID, MANUAL_SECRET)))
        before_restart = self.rows()
        self.verify.reset_mock()
        restarted = self.module.ConfigManager()
        self.assertEqual(restarted.refresh_credentials(), (migration.TARGET_APP_ID, MANUAL_SECRET))
        self.assertEqual((restarted.user_token, restarted.token_expire_time), ("fixture-current-tenant-token", 9999999999))
        self.assertEqual(restarted.feishu_app_migration_error, "")
        self.verify.assert_not_called()
        self.assertEqual(self.rows(), before_restart)
        self.assert_preserved_documents()

    def test_rejected_candidate_does_not_change_rows_marker_or_agent(self):
        self.configured()
        self.fixture.agent(app_id=fixtures.SOURCE_APP_ID, secret_cipher=fixtures.protect(fixtures.SOURCE_SECRET))
        before, mirror = self.rows(), self.fixture.agent_path.read_bytes()
        live = (self.manager.app_id, self.manager.app_secret, self.manager.user_token)
        self.verify.return_value = ("", "fixture-invalid-credentials")
        self.assertFalse(self.save("fixture-invalid-candidate"))
        self.verify.assert_called_once_with(migration.TARGET_APP_ID, "fixture-invalid-candidate")
        self.assertEqual(self.manager.last_save_error["code"], "settings_credentials_verification_failed")
        self.assertEqual(self.rows(), before)
        self.assertEqual(self.fixture.agent_path.read_bytes(), mirror)
        self.assertEqual((self.manager.app_id, self.manager.app_secret, self.manager.user_token), live)
        self.assert_preserved_documents()

    def test_empty_install_rejected_candidate_does_not_create_credentials_or_marker(self):
        self.configured(initial={})
        self.verify.return_value = ("", "fixture-invalid-credentials")
        before = self.rows()
        self.assertFalse(self.save("fixture-invalid-candidate"))
        self.assertEqual(self.manager.last_save_error["code"], "settings_credentials_verification_failed")
        self.assertEqual(self.rows(), before)
        self.assertFalse(self.fixture.agent_path.exists())
        self.assert_preserved_documents()

    def test_validation_unavailable_keeps_original_data(self):
        self.configured(profile="corrupt")
        before = self.rows()
        self.verify.return_value = ("", "fixture-validation-unavailable")
        self.assertFalse(self.save())
        self.assertEqual(self.manager.last_save_error["code"], "settings_credentials_verification_failed")
        self.assertEqual(self.rows(), before)
        self.assertFalse(self.fixture.agent_path.exists())

    def test_validation_exception_is_sanitized_and_not_saved(self):
        self.configured(profile="corrupt")
        before = self.rows()
        self.verify.side_effect = RuntimeError(MANUAL_SECRET)
        with patch.object(self.module, "log_error") as logged:
            self.assertFalse(self.save())
        self.assertEqual(self.manager.last_save_error["code"], "settings_credentials_verification_failed")
        self.assertEqual(self.rows(), before)
        self.assertNotIn(MANUAL_SECRET, str(self.manager.last_save_error) + str(logged.call_args_list))

    def test_token_with_error_is_not_a_successful_verification(self):
        self.configured(profile="corrupt")
        before = self.rows()
        self.verify.return_value = (VALIDATION_TOKEN, "fixture-verification-error")
        self.assertFalse(self.save())
        self.assertEqual(self.manager.last_save_error["code"], "settings_credentials_verification_failed")
        self.assertEqual(self.rows(), before)

    def test_verification_precedes_sqlite_write_lock_and_candidate_rows(self):
        self.configured(profile="corrupt")
        before = self.rows()
        def verify(app_id, secret):
            self.assertEqual((app_id, secret), (migration.TARGET_APP_ID, MANUAL_SECRET))
            self.assertEqual(self.rows(), before)
            with closing(sqlite3.connect(self.root / "lan_portal_state.sqlite3", timeout=0.05)) as writer:
                writer.execute("BEGIN IMMEDIATE")
                writer.rollback()
            return VALIDATION_TOKEN, ""
        self.verify.side_effect = verify
        self.assert_verified_save()

    def test_storage_failure_after_verification_rolls_back_credentials_and_marker(self):
        self.configured(profile="corrupt")
        before = self.rows()
        original = self.module.write_settings
        def fail(connection, values):
            original(connection, values)
            raise sqlite3.OperationalError("fixture-storage-failure")
        with patch.object(self.module, "write_settings", side_effect=fail):
            self.assertFalse(self.save())
        self.verify.assert_called_once_with(migration.TARGET_APP_ID, MANUAL_SECRET)
        self.assertEqual(self.rows(), before)
        self.assertFalse(self.fixture.agent_path.exists())
        self.assert_preserved_documents()

    def test_mirror_failure_after_commit_is_saved_and_retries_on_restart(self):
        self.configured(profile="corrupt")
        self.fixture.agent(app_id=fixtures.SOURCE_APP_ID, secret_cipher=fixtures.protect(fixtures.SOURCE_SECRET),
                           enabled=False)
        before_mirror = self.fixture.agent_path.read_bytes()
        with patch.object(migration, "atomic_json", side_effect=PermissionError(MANUAL_SECRET)), \
                patch.object(self.module, "log_error") as logged:
            self.assertTrue(self.save(), self.manager.last_save_error)
        self.verify.assert_called_once_with(migration.TARGET_APP_ID, MANUAL_SECRET)
        self.assertEqual(self.manager.last_save_error["code"], "settings_saved_mirror_failed")
        self.assertEqual(self.fixture.agent_path.read_bytes(), before_mirror)
        self.assert_saved(check_mirror=False)
        self.assertNotIn(MANUAL_SECRET, str(logged.call_args_list) + str(self.manager.last_save_error))
        restarted = self.module.ConfigManager()
        self.assertEqual(restarted.refresh_credentials(), (migration.TARGET_APP_ID, MANUAL_SECRET))
        self.assertEqual(restarted.feishu_agent_sync_error, "")
        self.assert_saved()
        agent = json.loads(self.fixture.agent_path.read_text(encoding="utf-8"))
        self.assertFalse(agent["enabled"])
        self.assertEqual(agent["custom"], {"keep": True})


if __name__ == "__main__":
    unittest.main()

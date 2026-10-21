"""Qt save callbacks and canonical SQLite commits, using only temporary data."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
import os
from pathlib import Path
import sqlite3
import sys
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

BIN = Path(__file__).resolve().parent
if str(BIN) not in sys.path:
    sys.path.insert(0, str(BIN))


class QtSettingsSavePipelineTests(unittest.TestCase):
    def setUp(self):
        from bin.test_feishu_app_migration import MigrationTests
        self.enterContext(patch.dict(os.environ, {"CLIPFLOW_DISABLE_SQLITE_WRITE_WORKER": "1"}))
        for target in ("socket.socket.connect", "socket.socket.connect_ex", "socket.getaddrinfo"):
            self.enterContext(patch(target, side_effect=AssertionError("Network forbidden")))
        fixture = MigrationTests(methodName="runTest")
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.module = fixture.config_module()
        from upload_event_module.services import feishu_app_migration as migration
        self.initial = {**fixture.source, "feishu_app_id": migration.TARGET_APP_ID,
                        "feishu_app_secret": "fixture-current-secret", migration.MIGRATION_MARKER: 2,
                        "disable_speech": True}
        self.store = fixture.store_settings(self.initial)
        self.root = fixture.root
        self.manager = self.module.ConfigManager()
        self.before = self.store.get_settings()
        from upload_event_module.ui import settings_dialog
        self.dialog_module = settings_dialog

    def dialog(self):
        dialog = SimpleNamespace(save_btn=Mock(), settings_save_finished=Mock(),
                                 settings_saved=Mock(), hide=Mock(), _relay_health_executor=Mock())
        text_fields = {
            "app_id": self.manager.app_id, "app_secret": self.manager.app_secret,
            "app_token": "fixture-base", "lan_template_portal_host": "0.0.0.0",
            "lan_template_public_host": "", "polling_work_order_public_relay_url": "",
        }
        for name in ("table_id_weibao", "table_id_biangeng", "table_id_tiaozheng", "table_id_shijian",
                     "table_id_power", "table_id_polling", "table_id_overhaul", "group_name_change_i3",
                     "group_name_maintenance", "group_name_event_i2", "group_name_event_i3", "group_name_event_prompt"):
            text_fields[name] = str(getattr(self.manager, name))
        for name, value in text_fields.items():
            setattr(dialog, name + "_input", Mock(text=Mock(return_value=value)))
        for name in ("lan_low_performance", "polling_work_order_public_relay", "disable_hot_reload",
                     "disable_alerts", "disable_speech"):
            setattr(dialog, name + "_checkbox", Mock(isChecked=Mock(return_value=False)))
        for name in ("_is_valid_lan_template_portal_host", "_is_valid_lan_template_public_host",
                     "_is_valid_polling_work_order_public_relay_url"):
            setattr(dialog, name, getattr(self.dialog_module.SettingsDialog, name))
        dialog._on_settings_save_finished = lambda result: self.dialog_module.SettingsDialog._on_settings_save_finished(dialog, result)
        return dialog

    def run_dialog_worker(self, dialog):
        self.dialog_module.SettingsDialog.save_settings(dialog)
        callback = dialog._relay_health_executor.submit.call_args.args[0]
        dialog.settings_save_finished.emit.assert_not_called()
        callback()
        return dialog.settings_save_finished.emit.call_args.args[0]

    def test_current_qt_settings_save_keeps_unprovided_credentials_and_history(self):
        self.assertTrue(self.manager.save(app_id=self.manager.app_id, app_secret=self.manager.app_secret,
                                          app_token="fixture-base", table_id_weibao="fixture-updated-table",
                                          disable_speech=False))
        saved = self.store.get_settings()
        self.assertEqual(saved["table_id_weibao"], "fixture-updated-table")
        self.assertFalse(saved["disable_speech"])
        for key in ("feishu_user_token", "token_expire_time", "custom_setting"):
            self.assertEqual(saved[key], self.before[key])
        self.assertEqual(self.manager.last_save_error, {})

    def test_partial_database_write_rolls_back_every_setting(self):
        write = self.module.write_settings
        failure = sqlite3.OperationalError("fixture-sensitive-database-detail")
        failure.sqlite_errorcode = sqlite3.SQLITE_BUSY
        def partial_write(connection, values):
            write(connection, values)
            raise failure
        with patch.object(self.module, "write_settings", side_effect=partial_write), \
                patch.object(self.module, "log_error") as logged:
            self.assertFalse(self.manager.save(table_id_weibao="changed", disable_speech=False))
        self.assertEqual(self.store.get_settings(), self.before)
        self.assertEqual(self.manager.table_id_weibao, self.before["table_id_weibao"])
        self.assertEqual(self.manager.last_save_error["code"], "settings_database_busy")
        self.assertNotIn(str(failure), str(self.manager.last_save_error) + str(logged.call_args_list))

    def test_invalid_new_secret_cannot_commit_or_clear_existing_configuration(self):
        self.assertFalse(self.manager.save(app_secret="", table_id_weibao="changed"))
        self.assertEqual(self.store.get_settings(), self.before)
        self.assertEqual(self.manager.app_secret, self.before["feishu_app_secret"])
        self.assertEqual(self.manager.feishu_app_migration_error, "")
        self.assertEqual(self.manager.last_save_error["code"], "settings_credentials_blocked")

    def test_invalid_credential_row_type_rolls_back_with_other_settings(self):
        self.assertFalse(self.manager.save(app_secret=123, disable_speech=False))
        self.assertEqual(self.store.get_settings(), self.before)
        self.assertEqual(self.manager.last_save_error["code"], "settings_credentials_blocked")

    def test_missing_runtime_symbol_has_safe_actionable_diagnostic(self):
        failure = NameError("fixture-secret-must-not-be-shown")
        with patch.object(self.module, "read_credentials", side_effect=failure), \
                patch.object(self.module, "log_error") as logged:
            self.assertFalse(self.manager.save(disable_speech=False))
        self.assertEqual(self.store.get_settings(), self.before)
        self.assertEqual(self.manager.last_save_error["code"], "settings_runtime_incomplete")
        self.assertIn("NameError", str(logged.call_args_list))
        self.assertNotIn(str(failure), str(self.manager.last_save_error) + str(logged.call_args_list))
        self.assertTrue(self.manager.save(disable_speech=False))
        self.assertEqual(self.manager.last_save_error, {})

    def test_permission_error_does_not_expose_paths_or_credentials(self):
        with patch.object(self.module, "write_settings", side_effect=PermissionError("fixture-secret/private-path")), \
                patch.object(self.module, "log_error") as logged:
            self.assertFalse(self.manager.save(disable_speech=False))
        self.assertEqual(self.store.get_settings(), self.before)
        self.assertEqual(self.manager.last_save_error["code"], "settings_permission_denied")
        self.assertNotIn("fixture-secret/private-path", str(self.manager.last_save_error) + str(logged.call_args_list))

    def test_committed_save_with_reload_failure_is_not_reported_as_uncommitted(self):
        with patch.object(self.manager, "load", side_effect=OSError("fixture-sensitive-detail")), \
                patch.object(self.module, "log_error") as logged:
            self.assertTrue(self.manager.save(table_id_weibao="committed-table"))
        saved = self.store.get_settings()
        self.assertEqual(saved["table_id_weibao"], "committed-table")
        self.assertEqual(saved["custom_setting"], self.before["custom_setting"])
        self.assertEqual(self.manager.last_save_error["code"], "settings_saved_reload_failed")
        self.assertNotIn("fixture-sensitive-detail", str(self.manager.last_save_error) + str(logged.call_args_list))

    def test_agent_mirror_failure_after_commit_preserves_canonical_settings(self):
        failure = self.module.FeishuAppMigrationError("agent_config_persist_failed")
        with patch.object(self.module, "sync_agent_config", side_effect=failure):
            self.assertTrue(self.manager.save(disable_speech=False))
        saved = self.store.get_settings()
        self.assertFalse(saved["disable_speech"])
        self.assertEqual(saved["feishu_app_secret"], self.before["feishu_app_secret"])
        self.assertEqual(saved["custom_setting"], self.before["custom_setting"])
        self.assertEqual(self.manager.app_secret, saved["feishu_app_secret"])
        self.assertEqual(self.manager.user_token, saved["feishu_user_token"])
        self.assertEqual(self.manager.feishu_app_migration_error, "")
        self.assertEqual(self.manager.last_save_error["code"], "settings_saved_mirror_failed")

    def test_real_mirror_write_failure_keeps_new_credentials_and_existing_history(self):
        from upload_event_module.services import feishu_app_migration as migration
        history = {"turns": ["fixture-history"], "permissions": {"admin": False}}
        self.store.put_document("lighthouse_ai", "fixture-person", history)
        mirror = self.root / "feishu_assistant.json"
        before_mirror = mirror.read_bytes()
        with patch.object(migration, "atomic_json", side_effect=PermissionError("fixture-sensitive-detail")), \
                patch.object(self.module, "log_error") as logged:
            self.assertTrue(self.manager.save(app_secret="fixture-rotated-secret", table_id_weibao="fixture-new-table"))
        self.assertEqual(self.manager.app_secret, "fixture-rotated-secret")
        self.assertEqual(self.manager.user_token, "")
        self.assertEqual(self.manager.feishu_app_migration_error, "")
        self.assertEqual(self.manager.last_save_error["code"], "settings_saved_mirror_failed")
        self.assertEqual(mirror.read_bytes(), before_mirror)
        self.assertEqual(self.store.get_settings()["custom_setting"], self.before["custom_setting"])
        self.assertEqual(self.store.get_document("lighthouse_ai", "fixture-person"), history)
        self.assertNotIn("fixture-sensitive-detail", str(logged.call_args_list))
        self.manager.load()
        self.assertEqual(self.manager.feishu_agent_sync_error, "")
        self.assertNotEqual(mirror.read_bytes(), before_mirror)

    def test_real_commit_busy_rolls_back_all_rows_and_keeps_live_credentials(self):
        connect = sqlite3.connect
        def short_timeout(*args, **kwargs):
            kwargs["timeout"] = .01
            return connect(*args, **kwargs)
        with closing(connect(self.store.db_path)) as reader:
            reader.execute("PRAGMA journal_mode=DELETE")
            reader.execute("BEGIN")
            reader.execute("SELECT * FROM settings").fetchall()
            with patch.object(self.module.sqlite3, "connect", side_effect=short_timeout):
                self.assertFalse(self.manager.save(app_secret="fixture-uncommitted-secret", table_id_weibao="uncommitted"))
            self.assertEqual(self.manager.last_save_error["code"], "settings_database_busy")
            self.assertEqual(self.manager.app_secret, self.before["feishu_app_secret"])
            reader.rollback()
        self.assertEqual(self.store.get_settings(), self.before)

    def test_canonical_rotated_token_is_reused_without_another_cloud_request(self):
        from upload_event_module.services import feishu_token_manager as auth
        client = Mock()
        tokens = auth.FeishuTokenManager(http_client=client)
        with patch.object(auth, "config", self.manager), \
                patch.object(tokens, "_request_tenant_token_sdk", side_effect=AssertionError("SDK forbidden")) as sdk:
            self.assertEqual(tokens.get_tenant_token(), self.before["feishu_user_token"])
            self.assertTrue(self.manager.save(app_secret="fixture-next-secret"))
            self.assertTrue(self.manager.save_tenant_token("fixture-next-token", 9999999999,
                            expected_credentials=(self.manager.app_id, self.manager.app_secret)))
            tokens._app_token_cache[(self.before["feishu_app_id"], self.before["feishu_app_secret"])] = {"token": "old-app-token"}
            self.assertEqual(tokens.get_tenant_token(), "fixture-next-token")
        client.request_json.assert_not_called()
        sdk.assert_not_called()
        self.assertEqual(tokens._app_token_cache, {})

    def test_marked_but_invalid_sqlite_secret_still_fails_closed_without_writes(self):
        self.store.put_settings({"feishu_app_secret": ""})
        before = self.store.get_settings()
        with patch.object(self.manager, "_save_config_to_sqlite") as save:
            self.manager.load()
        save.assert_not_called()
        self.assertEqual((self.manager.app_id, self.manager.app_secret, self.manager.user_token), ("", "", ""))
        self.assertEqual(self.manager.feishu_app_migration_error, "qt_secret_invalid")
        self.assertEqual(self.store.get_settings(), before)

    def test_settings_read_never_initializes_business_schema_or_requests_writer_lock(self):
        from lan_bitable_template_portal.state_store import LanPortalStateStore
        with closing(sqlite3.connect(self.store.db_path)) as writer:
            writer.execute("BEGIN IMMEDIATE")
            with patch.object(LanPortalStateStore, "_ensure_schema_locked", side_effect=AssertionError("read requested schema writes")):
                self.assertEqual(self.manager._load_config_from_sqlite(), self.before)
                self.assertEqual(self.manager.refresh_credentials()[0], self.before["feishu_app_id"])

    def test_database_read_error_cannot_trigger_legacy_json_bootstrap(self):
        from openclaw_service.protocol import atomic_json
        atomic_json(self.root / "config.json", {"feishu_app_id": "cli_fixturePrevious",
                                               "feishu_app_secret": "fixture-old-secret"})
        with patch.object(self.module.sqlite3, "connect", side_effect=sqlite3.OperationalError("fixture-sensitive-detail")), \
                patch.object(self.manager, "_save_config_to_sqlite") as save, \
                patch.object(self.module, "log_error") as logged:
            self.manager.load()
        save.assert_not_called()
        self.assertEqual((self.manager.app_id, self.manager.app_secret, self.manager.user_token), ("", "", ""))
        self.assertEqual(self.store.get_settings(), self.before)
        self.assertNotIn("fixture-sensitive-detail", str(logged.call_args_list))

    def test_writer_after_commit_does_not_erase_live_credentials_or_lose_saved_setting(self):
        transaction = self.module.qt_settings_transaction
        connect = sqlite3.connect
        with closing(connect(self.store.db_path)) as writer:
            @contextmanager
            def commit_then_lock(data_root):
                with transaction(data_root) as connection:
                    yield connection
                writer.execute("BEGIN IMMEDIATE")

            def short_timeout(*args, **kwargs):
                kwargs["timeout"] = .01
                return connect(*args, **kwargs)

            with patch.object(self.module, "qt_settings_transaction", side_effect=commit_then_lock) as transactions, \
                    patch.object(self.module.sqlite3, "connect", side_effect=short_timeout):
                self.assertTrue(self.manager.save(table_id_weibao="committed-under-contention"))
            transactions.assert_called_once()
            self.assertEqual(self.manager.app_id, self.before["feishu_app_id"])
            self.assertEqual(self.manager.user_token, self.before["feishu_user_token"])
            self.assertEqual(self.manager.table_id_weibao, "committed-under-contention")
            self.assertEqual(self.manager.feishu_app_migration_error, "")
            writer.rollback()
        self.assertEqual(self.store.get_settings()["table_id_weibao"], "committed-under-contention")

    def test_dialog_committed_warning_keeps_resolved_token_and_never_claims_unsaved(self):
        dialog = self.dialog()
        with patch.object(self.dialog_module, "config", self.manager), \
                patch.object(self.dialog_module, "resolve_bitable_app_token", return_value=("fixture-resolved-base", True)), \
                patch.object(self.manager, "load", side_effect=OSError("fixture-sensitive-detail")), \
                patch.object(self.dialog_module, "show_toast_message") as toast:
            result = self.run_dialog_worker(dialog)
            self.assertTrue(result["ok"])
            self.assertEqual(result["code"], "settings_saved_reload_failed")
            self.dialog_module.SettingsDialog._on_settings_save_finished(dialog, result)
        self.assertEqual(self.store.get_settings()["feishu_app_token"], "fixture-resolved-base")
        dialog.app_token_input.setText.assert_called_once_with("fixture-resolved-base")
        dialog.hide.assert_not_called()
        dialog.settings_saved.emit.assert_not_called()
        self.assertIn("配置已保存", toast.call_args.args[1])
        self.assertNotIn("fixture-sensitive-detail", str(toast.call_args_list))

    def test_dialog_mirror_warning_still_applies_successfully_loaded_settings(self):
        dialog = self.dialog()
        with patch.object(self.dialog_module, "config", self.manager), \
                patch.object(self.dialog_module, "resolve_bitable_app_token", return_value=("fixture-base", False)), \
                patch.object(self.module, "sync_agent_config", side_effect=PermissionError("fixture-sensitive-detail")), \
                patch.object(self.dialog_module, "show_toast_message") as toast, \
                patch.object(self.dialog_module.QTimer, "singleShot", side_effect=lambda delay, callback: callback()):
            result = self.run_dialog_worker(dialog)
            self.assertTrue(result["ok"])
            self.assertEqual(result["code"], "settings_saved_mirror_failed")
            self.dialog_module.SettingsDialog._on_settings_save_finished(dialog, result)
        dialog.settings_saved.emit.assert_called_once_with()
        dialog.hide.assert_not_called()
        self.assertIn("配置已保存", toast.call_args.args[1])
        self.assertFalse(self.manager.disable_speech)

    def test_save_diagnostics_do_not_cross_threads(self):
        barrier = threading.Barrier(2)
        def fail(code):
            self.manager._record_save_failure(code=code)
            barrier.wait(timeout=5)
            return self.manager.last_save_error["code"]
        codes = ("settings_database_busy", "settings_permission_denied")
        with ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(tuple(pool.map(fail, codes)), codes)
        self.assertEqual(self.manager.last_save_error, {})

    def test_dialog_defers_save_then_restores_button_and_keeps_form_on_failure(self):
        dialog = self.dialog()
        with patch.object(self.dialog_module, "config", self.manager), \
                patch.object(self.dialog_module, "resolve_bitable_app_token", return_value=("fixture-base", False)), \
                patch.object(self.module, "read_credentials", side_effect=NameError("fixture-sensitive-detail")), \
                patch.object(self.dialog_module, "show_toast_message") as toast:
            result = self.run_dialog_worker(dialog)
            self.assertFalse(result["ok"])
            self.assertEqual(result["code"], "settings_runtime_incomplete")
            dialog.save_btn.setEnabled.assert_called_once_with(False)
            self.dialog_module.SettingsDialog._on_settings_save_finished(dialog, result)
        self.assertEqual(self.store.get_settings(), self.before)
        dialog.save_btn.setEnabled.assert_called_with(True)
        dialog.save_btn.setText.assert_called_with("保存")
        dialog.app_secret_input.setText.assert_not_called()
        dialog.hide.assert_not_called()
        dialog.settings_saved.emit.assert_not_called()
        self.assertNotIn("fixture-sensitive-detail", str(toast.call_args_list))

    def test_dialog_unexpected_worker_exception_never_echoes_secret(self):
        dialog = self.dialog()
        with patch.object(self.dialog_module, "config", self.manager), \
                patch.object(self.dialog_module, "resolve_bitable_app_token", side_effect=RuntimeError("fixture-sensitive-detail")):
            result = self.run_dialog_worker(dialog)
        self.assertFalse(result["ok"])
        self.assertEqual(result["code"], "settings_save_failed")
        self.assertNotIn("fixture-sensitive-detail", str(result))
        self.assertEqual(self.store.get_settings(), self.before)

    def test_background_worker_reports_its_own_save_error_to_gui_callback(self):
        dialog = self.dialog()
        with ThreadPoolExecutor(max_workers=1) as pool, \
                patch.object(self.dialog_module, "config", self.manager), \
                patch.object(self.dialog_module, "resolve_bitable_app_token", return_value=("fixture-base", False)), \
                patch.object(self.module, "read_credentials", side_effect=NameError("fixture-sensitive-detail")):
            futures = []
            dialog._relay_health_executor = SimpleNamespace(submit=lambda work: futures.append(pool.submit(work)))
            self.dialog_module.SettingsDialog.save_settings(dialog)
            futures[0].result(timeout=5)
        result = dialog.settings_save_finished.emit.call_args.args[0]
        self.assertFalse(result["ok"])
        self.assertEqual(result["code"], "settings_runtime_incomplete")
        self.assertEqual(self.manager.last_save_error, {})
        with patch.object(self.dialog_module, "show_toast_message"):
            self.dialog_module.SettingsDialog._on_settings_save_finished(dialog, result)
        dialog.save_btn.setEnabled.assert_called_with(True)
        self.assertEqual(self.store.get_settings(), self.before)

    def test_executor_rejection_restores_save_button_without_replaying(self):
        dialog = self.dialog()
        dialog._relay_health_executor.submit.side_effect = RuntimeError("fixture-sensitive-detail")
        with patch.object(self.dialog_module, "show_toast_message") as toast, \
                patch.object(self.manager, "save") as save, \
                patch.object(self.dialog_module, "config", self.manager):
            self.dialog_module.SettingsDialog.save_settings(dialog)
        save.assert_not_called()
        dialog.save_btn.setEnabled.assert_called_with(True)
        dialog.hide.assert_not_called()
        self.assertNotIn("fixture-sensitive-detail", str(toast.call_args_list))

    def test_shared_refresh_failure_disables_oauth_and_clears_tokens_until_recovery(self):
        from lan_bitable_template_portal import portal_auth
        from upload_event_module.services import feishu_token_manager
        http = Mock()
        manager = feishu_token_manager.FeishuTokenManager(http_client=http)
        with patch.object(portal_auth, "config", self.manager), \
                patch.object(portal_auth, "token_manager", manager), \
                patch.object(feishu_token_manager, "config", self.manager):
            self.assertTrue(portal_auth.PortalAuthManager.configured())
            with patch.object(self.module.sqlite3, "connect", side_effect=sqlite3.OperationalError("fixture-sensitive-detail")), \
                    patch.object(self.module, "log_error") as logged:
                self.assertFalse(portal_auth.PortalAuthManager.configured())
                self.assertFalse(portal_auth.PortalAuthManager.configured())
                self.assertEqual((self.manager.app_id, self.manager.app_secret, self.manager.user_token,
                                  self.manager.token_expire_time), ("", "", "", 0))
                logged.assert_called_once()
            self.assertEqual(self.manager.feishu_app_migration_error, "settings_read_failed")
            self.assertEqual(self.manager._credential_read_error["kind"], "OperationalError")
            self.assertEqual(Path(self.manager._credential_read_error["database"]), self.store.db_path.resolve())
            self.assertNotIn("fixture-sensitive-detail", str(logged.call_args_list))
            self.assertTrue(portal_auth.PortalAuthManager.configured())
            self.assertEqual(self.manager.user_token, self.before["feishu_user_token"])
            self.assertEqual(self.manager._credential_read_error, {})
        self.assertEqual(self.store.get_settings(), self.before)
        self.assertEqual(http.mock_calls, [])

    def test_wrong_database_path_is_reported_without_creating_or_falling_back(self):
        wrong_config = self.store.db_path.parent / "missing-directory" / "config.json"
        with patch.object(self.module, "CONFIG_FILE", str(wrong_config)), \
                patch.object(self.module, "log_error") as logged:
            with self.assertRaises(self.module.FeishuAppMigrationError) as caught:
                self.manager.refresh_credentials()
        self.assertEqual(caught.exception.code, "settings_read_failed")
        expected = wrong_config.with_name("lan_portal_state.sqlite3").resolve()
        self.assertEqual(self.manager._credential_read_error["database"], str(expected))
        self.assertIn(str(expected), logged.call_args.args[0])
        self.assertNotIn(self.before["feishu_app_secret"], str(logged.call_args_list))
        self.assertNotIn(str(expected), str(caught.exception))
        self.assertFalse(wrong_config.parent.exists())
        self.assertEqual(self.store.get_settings(), self.before)
        self.manager.refresh_credentials()
        self.assertEqual(self.manager._credential_read_error, {})

    def test_pending_migration_retains_storage_and_does_not_use_cached_credentials(self):
        from upload_event_module.services.feishu_app_migration import MIGRATION_MARKER
        self.store.put_settings({"feishu_app_id": "cli_fixturePrevious"})
        from contextlib import closing
        with closing(sqlite3.connect(self.store.db_path)) as connection, connection:
            connection.execute("DELETE FROM settings WHERE key=?", (MIGRATION_MARKER,))
        before = self.store.get_settings()
        with patch.object(self.module, "log_error") as logged:
            with self.assertRaises(self.module.FeishuAppMigrationError) as caught:
                self.manager.refresh_credentials()
        self.assertEqual(caught.exception.code, "migration_pending")
        self.assertEqual(self.manager._credential_read_error["code"], "migration_pending")
        self.assertEqual((self.manager.app_id, self.manager.app_secret, self.manager.user_token), ("", "", ""))
        self.assertEqual(self.store.get_settings(), before)
        self.assertNotIn(self.before["feishu_app_secret"], str(logged.call_args_list))


if __name__ == "__main__":
    unittest.main()

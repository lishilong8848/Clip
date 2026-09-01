from __future__ import annotations

import sys
import inspect
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from upload_event_module.ui import main_window_runtime
from upload_event_module.ui.main_window_runtime import MainWindowRuntimeMixin
from upload_event_module.ui.settings_dialog import SettingsDialog
from upload_event_module.config import (
    ConfigManager,
    DEFAULT_POLLING_WORK_ORDER_PUBLIC_RELAY_URL,
)


class SettingsSaveRuntimeTests(unittest.TestCase):
    def test_settings_save_submits_blocking_work_to_background_executor(self) -> None:
        source = inspect.getsource(SettingsDialog.save_settings)
        self.assertIn("def run()", source)
        self.assertIn("self._relay_health_executor.submit(run)", source)
        self.assertGreater(source.index("config.save("), source.index("def run()"))

    def test_public_relay_url_has_no_hard_coded_default(self) -> None:
        self.assertEqual(DEFAULT_POLLING_WORK_ORDER_PUBLIC_RELAY_URL, "")

    def test_public_relay_url_change_is_blocked_by_open_public_group(self) -> None:
        manager = ConfigManager.__new__(ConfigManager)
        manager.polling_work_order_public_relay_url = "https://old.example"
        store = MagicMock()
        store.list_documents.return_value = [
            {
                "payload": {
                    "state": "active",
                    "relay": {"mode": "public_relay"},
                }
            }
        ]
        with patch(
            "lan_bitable_template_portal.state_store.LanPortalStateStore",
            return_value=store,
        ):
            reason = manager.polling_work_order_relay_url_change_block_reason(
                "https://new.example"
            )
        self.assertIn("未结束的公网工单", reason)
        self.assertEqual(
            manager.polling_work_order_relay_url_change_block_reason(
                "https://old.example/"
            ),
            "",
        )

    def test_relay_http_allows_only_loopback_or_private_lan(self) -> None:
        self.assertTrue(
            SettingsDialog._is_valid_polling_work_order_public_relay_url(
                "http://192.168.224.122:18767"
            )
        )
        self.assertTrue(
            SettingsDialog._is_valid_polling_work_order_public_relay_url(
                "http://127.0.0.1:18767"
            )
        )
        self.assertFalse(
            SettingsDialog._is_valid_polling_work_order_public_relay_url(
                "http://example.com:18767"
            )
        )

    def test_unrelated_setting_save_does_not_restart_hot_reload(self) -> None:
        manager = MagicMock()
        window = SimpleNamespace(
            _applied_disable_hot_reload=False,
            hot_reload_manager=manager,
        )
        with patch.object(main_window_runtime.config, "load"), patch.object(
            main_window_runtime.config,
            "disable_hot_reload",
            False,
        ):
            MainWindowRuntimeMixin.refresh_hot_reload_setting(window)

        manager.stop.assert_not_called()
        manager.start.assert_not_called()

    def test_actual_hot_reload_toggle_still_stops_watcher(self) -> None:
        manager = MagicMock()
        window = SimpleNamespace(
            _applied_disable_hot_reload=False,
            hot_reload_manager=manager,
        )
        with patch.object(main_window_runtime.config, "load"), patch.object(
            main_window_runtime.config,
            "disable_hot_reload",
            True,
        ):
            MainWindowRuntimeMixin.refresh_hot_reload_setting(window)

        manager.stop.assert_called_once_with()
        self.assertIsNone(window.hot_reload_manager)
        self.assertTrue(window._applied_disable_hot_reload)


if __name__ == "__main__":
    unittest.main()

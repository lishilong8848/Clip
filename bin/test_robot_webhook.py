# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from upload_event_module.services import robot_webhook


class RobotWebhookTests(unittest.TestCase):
    @patch.object(
        robot_webhook,
        "_send_message_to_open_id",
        return_value=(False, "Bot has NO availability to this user."),
    )
    @patch.object(
        robot_webhook,
        "_get_tenant_access_token",
        return_value=("tenant-token", ""),
    )
    def test_bot_unavailable_failure_is_structured(
        self,
        _token_mock,
        _send_mock,
    ) -> None:
        ok, message, results = robot_webhook.send_text_to_open_ids(
            "签名链接",
            ["ou_external_user"],
        )
        self.assertFalse(ok)
        self.assertIn("机器人对该用户不可用", message)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["failure_kind"], "bot_unavailable")

    @patch.object(
        robot_webhook,
        "_send_message_to_open_id",
        return_value=(True, "ok"),
    )
    @patch.object(
        robot_webhook,
        "_get_tenant_access_token",
        return_value=("tenant-token", ""),
    )
    def test_success_has_no_failure_kind(
        self,
        _token_mock,
        _send_mock,
    ) -> None:
        ok, _message, results = robot_webhook.send_text_to_open_ids(
            "签名链接",
            ["ou_current_user"],
        )
        self.assertTrue(ok)
        self.assertEqual(results[0]["failure_kind"], "")


if __name__ == "__main__":
    unittest.main()

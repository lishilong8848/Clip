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
    def test_card_requires_complete_explicit_success_receipt(self):
        invalid = ({}, {'code': False}, {'code': 'invalid'}, {'code': 0},
                   {'code': 0, 'data': {'message_id': 'om_ok'}},
                   {'code': 0, 'data': {'message_id': 'wrong', 'chat_id': 'oc_ok'}})
        for response in invalid:
            with self.subTest(response=response), patch.object(robot_webhook, '_request_json', return_value=response):
                receipt = {'message_id': 'om_old'}
                ok, message = robot_webhook._send_interactive_to_receive_id(
                    'fixture', 'ou_user', {'elements': []}, receive_id_type='open_id', receipt=receipt)
                self.assertFalse(ok)
                self.assertIn('未确认', message)
                self.assertEqual(receipt, {})

    def test_card_receipt_and_token_refresh_keep_uuid_and_real_error_code(self):
        receipt = {}
        with patch.object(robot_webhook, '_request_json', side_effect=[
                {'code': 99991663, 'msg': 'token expired'},
                {'code': 0, 'data': {'message_id': 'om_card', 'chat_id': 'oc_private'}}]) as send, \
             patch.object(robot_webhook, '_get_tenant_access_token', return_value=('new-fixture', '')), \
             patch.object(robot_webhook.config, 'app_id', 'cli_fixture'):
            ok, _ = robot_webhook._send_interactive_to_receive_id(
                'fixture', 'ou_user', {'elements': []}, receive_id_type='open_id',
                message_uuid='stable', receipt=receipt)
        self.assertTrue(ok)
        self.assertEqual([c.kwargs['json_payload']['uuid'] for c in send.call_args_list], ['stable', 'stable'])
        self.assertEqual(receipt['message_id'], 'om_card')
        self.assertEqual(receipt['chat_id'], 'oc_private')
        self.assertEqual(receipt['sender_app_id'], 'cli_fixture')
        self.assertEqual(receipt['message_uuid'], 'stable')
        self.assertGreater(receipt['accepted_at'], 0)
        with patch.object(robot_webhook, '_request_json', return_value={'code': 230013, 'msg': 'Bot has NO availability to this user.'}):
            ok, message = robot_webhook._send_interactive_to_receive_id(
                'fixture', 'ou_user', {'elements': []}, receive_id_type='open_id', receipt=receipt)
        self.assertFalse(ok)
        self.assertIn('230013', message)
        self.assertEqual(receipt, {})

    def test_bulk_card_has_distinct_stable_uuids_and_individual_receipts(self):
        def send(_method, _url, **kwargs):
            recipient = kwargs['json_payload']['receive_id'].removeprefix('ou_')
            return {'code': 0, 'data': {'message_id': 'om_' + recipient, 'chat_id': 'oc_' + recipient}}
        with patch.object(robot_webhook, '_get_tenant_access_token', return_value=('fixture', '')), \
             patch.object(robot_webhook, '_request_json', side_effect=send) as transport:
            ok, _, receipts = robot_webhook.send_interactive_to_open_ids({'elements': []}, ['ou_a', 'ou_b'], message_uuid='batch')
            first = [c.kwargs['json_payload']['uuid'] for c in transport.call_args_list]
            transport.reset_mock()
            robot_webhook.send_interactive_to_open_ids({'elements': []}, ['ou_b', 'ou_a'], message_uuid='batch')
            second = [c.kwargs['json_payload']['uuid'] for c in transport.call_args_list]
            robot_webhook.send_interactive_to_open_ids({'elements': []}, ['ou_b'], message_uuid=receipts[1]['message_uuid'])
            retry_uuid = transport.call_args.kwargs['json_payload']['uuid']
        self.assertTrue(ok)
        self.assertEqual(len(set(first)), 2)
        self.assertEqual(first, list(reversed(second)))
        self.assertEqual(first[1], retry_uuid)
        self.assertEqual([r['message_id'] for r in receipts], ['om_a', 'om_b'])
        self.assertEqual([r['chat_id'] for r in receipts], ['oc_a', 'oc_b'])

    def test_single_recipient_keeps_legacy_uuid_after_update(self):
        with patch.object(robot_webhook, '_get_tenant_access_token', return_value=('fixture', '')), \
             patch.object(robot_webhook, '_request_json', return_value={
                 'code': 0, 'data': {'message_id': 'om_oldrequest', 'chat_id': 'oc_user'}}) as transport:
            ok, _, receipts = robot_webhook.send_interactive_to_open_ids(
                {'elements': []}, ['ou_user'], message_uuid='legacy-queued-uuid')
        self.assertTrue(ok)
        self.assertEqual(transport.call_args.kwargs['json_payload']['uuid'], 'legacy-queued-uuid')
        self.assertEqual(receipts[0]['message_uuid'], 'legacy-queued-uuid')

    def test_bulk_card_missing_receipt_fails_only_that_recipient(self):
        with patch.object(robot_webhook, '_get_tenant_access_token', return_value=('fixture', '')), \
             patch.object(robot_webhook, '_request_json', side_effect=[{},
                 {'code': 0, 'data': {'message_id': 'om_b', 'chat_id': 'oc_b'}}]):
            ok, message, receipts = robot_webhook.send_interactive_to_open_ids({'elements': []}, ['ou_a', 'ou_b'])
        self.assertFalse(ok)
        self.assertIn('未确认', message)
        self.assertFalse(receipts[0]['ok'])
        self.assertNotIn('message_id', receipts[0])
        self.assertTrue(receipts[1]['ok'])
        self.assertEqual(receipts[1]['message_id'], 'om_b')

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

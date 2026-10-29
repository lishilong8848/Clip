"""Isolated authorization callbacks; never contact Feishu or send messages."""
import copy
import json
import sys
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.state_store import LanPortalStateStore
from lan_bitable_template_portal import signature_usage_cards as cards
from lan_bitable_template_portal.feishu_card_lifetime import MAX_CARD_AGE


class SignatureUsageCardsTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory(); self.addCleanup(folder.cleanup)
        self.store = LanPortalStateStore(str(Path(folder.name) / 'state.sqlite'))
        self.runtime = SimpleNamespace(state_store=self.store,
            auth_manager=SimpleNamespace(_open_id_explicitly_disabled=lambda _: False))
        self.check = patch('lan_bitable_template_portal.feishu_delivery.current_recipient_open_id', side_effect=lambda _rt, oid, _app: oid)
        self.check.start(); self.addCleanup(self.check.stop)
        self.created = self.store.create_mop_signature_usage_confirmation(scope='A', notice_key='notice-1',
            role='implementer', signer_record_id='rec-person', signer_open_id='ou_person', signer_name='签名人',
            requested_by_openid='ou_requester', payload={'app_id': 'cli_test', 'notice_title': '维保任务',
                'role_text': '实施人', 'public_base_url': 'https://fixture.invalid'})

    def event(self, **values):
        return {'header': {'app_id': 'cli_test', 'event_type': 'card.action.trigger'},
            'event': {'operator': {'open_id': 'ou_person'}, 'action': {'value': {'kind': 'signature_usage',
                'token': self.created['token'], 'confirmation_id': self.created['confirmation_id'],
                'decision': 'confirmed', **values}}}}

    def row(self):
        return self.store.get_mop_signature_usage_confirmation(token=self.created['token'], allow_expired=True)

    def test_exact_three_actions_and_no_signature_image(self):
        card = cards.render(self.row(), self.created['token'], 'https://fixture.invalid/confirm')
        buttons = [e for e in card['body']['elements'] if e['tag'] == 'button']
        self.assertEqual([b['text']['content'] for b in buttons], ['确认使用签名', '拒绝', '打开灯塔处理'])
        self.assertEqual(buttons[2]['behaviors'][0]['default_url'], 'https://fixture.invalid/confirm')
        self.assertNotIn('image_base64', json.dumps(card))

    def test_card_and_web_decision_are_idempotent_first_wins(self):
        result = cards.apply(self.runtime, self.event(), 'cli_test')
        self.assertTrue(result['ok'])
        self.assertEqual(self.store.reject_mop_signature_usage(token=self.created['token'])['status'], 'confirmed')
        replay = cards.apply(self.runtime, self.event(decision='rejected'), 'cli_test')
        self.assertEqual(result, replay)
        self.assertEqual([e['text']['content'] for e in result['card']['body']['elements'] if e['tag'] == 'button'], ['打开灯塔处理'])

    def test_web_first_reject_card_cannot_reverse(self):
        self.store.reject_mop_signature_usage(token=self.created['token'])
        self.assertEqual(cards.apply(self.runtime, self.event(), 'cli_test')['toast'], '已拒绝使用签名')

    def test_concurrent_opposite_decisions_never_double_apply(self):
        with ThreadPoolExecutor(2) as pool:
            results = list(pool.map(lambda action: cards.apply(self.runtime, self.event(decision=action), 'cli_test'), ['confirmed', 'rejected']))
        self.assertEqual(results[0]['toast'], results[1]['toast'])

    def test_forwarded_mismatched_token_app_and_disabled_actor_do_not_mutate(self):
        forged = self.event(); forged['event']['operator']['open_id'] = 'ou_other'
        cases = [forged, self.event(confirmation_id='other'), self.event(token='x' * 43)]
        wrong_app = self.event(); wrong_app['header']['app_id'] = 'cli_other'; cases.append(wrong_app)
        for data in cases:
            with self.subTest(data=str(data)[:50]), self.assertRaises(ValueError):
                cards.apply(self.runtime, data, 'cli_test')
            self.assertEqual(self.row()['status'], 'pending')
        self.runtime.auth_manager._open_id_explicitly_disabled = lambda _: True
        with self.assertRaises(ValueError): cards.apply(self.runtime, self.event(), 'cli_test')
        self.assertEqual(self.row()['status'], 'pending')

    def test_seven_day_boundary_blocks_card_and_web_and_keeps_history(self):
        at = self.created['created_at'] + MAX_CARD_AGE
        with patch('time.time', return_value=at):
            result = cards.apply(self.runtime, self.event(), 'cli_test')
            self.assertIn('7天', result['toast'])
            self.assertNotIn('callback', json.dumps(result['card']))
            with self.assertRaisesRegex(ValueError, '过期'):
                self.store.confirm_mop_signature_usage(token=self.created['token'])
        self.assertEqual(self.row()['status'], 'pending')

    def test_atomic_identity_check_inside_store(self):
        with self.assertRaises(ValueError):
            self.store.decide_mop_signature_usage(token=self.created['token'], decision='confirmed', expected_signer='ou_other')
        self.assertEqual(self.row()['status'], 'pending')

    def test_same_record_new_account_does_not_authorize_old_account_request(self):
        fresh = self.store.create_mop_signature_usage_confirmation(scope='A', notice_key='notice-1', role='implementer',
            signer_record_id='rec-person', signer_open_id='ou_newperson', requested_by_openid='ou_requester')
        self.store.confirm_mop_signature_usage(token=fresh['token'])
        self.assertEqual(self.row()['status'], 'pending')

    def test_shorter_deadline_not_extended_and_longer_ttl_capped(self):
        for ttl, actual in [(30, 30), (30 * 86400, MAX_CARD_AGE)]:
            created = self.store.create_mop_signature_usage_confirmation(scope='B', notice_key='separate', role='auditor',
                signer_record_id='rec-person', signer_open_id='ou_person', ttl_seconds=ttl)
            self.assertEqual(created['expires_at'] - created['created_at'], actual)

    def test_creating_new_request_preserves_expired_history(self):
        with patch('time.time', return_value=self.created['created_at'] + 30 * 86400):
            self.store.create_mop_signature_usage_confirmation(scope='B', notice_key='other', role='auditor',
                signer_record_id='rec-person', signer_open_id='ou_person')
        self.assertEqual(self.row()['confirmation_id'], self.created['confirmation_id'])

    def test_delivery_uses_card_stable_identity_and_reports_failure(self):
        item = {'card': cards.render(self.row(), self.created['token'], 'https://fixture.invalid'),
                'open_id': 'ou_person', 'confirmation_id': self.created['confirmation_id']}
        with patch('upload_event_module.services.robot_webhook.send_interactive_to_open_ids', return_value=(False, 'not sent', [])) as sender:
            self.assertFalse(cards.send(item)[0]); cards.send(item)
        self.assertEqual(sender.call_args_list[0], sender.call_args_list[1])
        self.assertEqual(self.row()['status'], 'pending')


if __name__ == '__main__': unittest.main()

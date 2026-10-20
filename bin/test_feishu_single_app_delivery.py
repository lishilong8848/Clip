"""Offline single-app delivery contracts; no real tokens, HTTP or business data."""
from __future__ import annotations

import copy
from contextlib import ExitStack, closing, contextmanager
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from bin.test_feishu_delivery import CALLBACKS, STATIC, Store, TARGET_APP_ID, PREVIOUS_APP_ID, qt_credentials_fixture
from lan_bitable_template_portal.feishu_delivery import PublicFeishuSender
from upload_event_module.services import robot_webhook as robot


@contextmanager
def standalone_delivery_fixture(*open_ids):
    store = Store()
    for open_id in open_ids:
        store.put_document('feishu_assistant_sender', TARGET_APP_ID + ':' + open_id,
                           {'open_id': open_id, 'matched_by': 'record_id'})
    sender = PublicFeishuSender(SimpleNamespace(state_store=store))
    with ExitStack() as stack:
        stack.enter_context(qt_credentials_fixture())
        stack.enter_context(patch.object(robot, '_AGENT_SENDER', None))
        stack.enter_context(patch.object(robot.config, 'app_id', TARGET_APP_ID))
        stack.enter_context(patch('lan_bitable_template_portal.feishu_delivery.standalone_sender', return_value=sender))
        yield sender


class SingleAppDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(qt_credentials_fixture())
        self.store = Store()
        self.sender = PublicFeishuSender(SimpleNamespace(state_store=self.store))
        self.bind(TARGET_APP_ID, 'ou_new')
        self.bind(PREVIOUS_APP_ID, 'ou_old')
        patches = [
            patch.object(robot, '_AGENT_SENDER', None),
            patch.object(robot.config, 'app_id', TARGET_APP_ID),
            patch.object(robot.token_manager, 'get_tenant_token', return_value='fixture-token'),
            patch.object(robot, '_request_json', return_value={
                'code': 0, 'data': {'message_id': 'om_current', 'chat_id': 'oc_private'}}),
            patch('lan_bitable_template_portal.feishu_delivery.standalone_sender', return_value=self.sender),
            patch('lan_bitable_template_portal.portal_service.external_real_write_guard',
                  return_value={'real_write_allowed': True}),
        ]
        mocks = []
        for item in patches:
            mocks.append(item.start())
            self.addCleanup(item.stop)
        self.token, self.wire, self.factory = mocks[2:5]

    def bind(self, app_id, mapped, *, matched_by='record_id'):
        self.store.put_document('feishu_assistant_sender', app_id + ':' + mapped,
                                {'open_id': mapped, 'matched_by': matched_by})

    def text(self, identity='stable', recipient='ou_new'):
        return robot.send_text_to_open_ids('offline', [recipient], message_uuid=identity)

    def test_agent_rejection_never_selects_direct_or_old_app(self):
        agent = Mock(return_value=robot.DeliveryAttempt('not_sent', 'rejected', code=230013))
        with patch.object(robot, '_AGENT_SENDER', agent):
            ok, _, rows = self.text()
        self.assertFalse(ok)
        self.assertEqual(rows[0]['code'], 230013)
        self.factory.assert_not_called()
        self.wire.assert_not_called()
        self.token.assert_not_called()

    def test_standalone_qt_preserves_current_open_id_and_saves_current_receipt(self):
        ok, _, rows = self.text()
        self.assertTrue(ok)
        self.token.assert_called_once_with(force_refresh=False)
        request = self.wire.call_args.kwargs
        self.assertEqual(request['json_payload']['receive_id'], 'ou_new')
        self.assertEqual(request['headers']['Authorization'], 'Bearer fixture-token')
        self.assertEqual(request['params']['receive_id_type'], 'open_id')
        self.assertEqual(rows[0]['open_id'], 'ou_new')
        self.assertEqual(rows[0]['sender_app_id'], TARGET_APP_ID)
        self.assertEqual(rows[0]['delivery_channel'], 'direct')
        self.assertEqual(rows[0]['message_uuid'], 'stable')
        key = self.sender.route_key('ou_new', 'open_id', 'stable')
        self.assertEqual(self.sender.route(key)['receipt']['sender_app_id'], TARGET_APP_ID)
        self.assertTrue(self.text()[0])
        self.assertEqual(self.wire.call_count, 1)

    def test_standalone_static_card_uses_same_mapping_and_root_token(self):
        ok, _, rows = robot.send_interactive_to_open_ids(STATIC, ['ou_new'], message_uuid='card')
        self.assertTrue(ok)
        self.assertEqual(rows[0]['sender_app_id'], TARGET_APP_ID)
        self.assertEqual(self.wire.call_args.kwargs['json_payload']['receive_id'], 'ou_new')
        self.token.assert_called_once_with(force_refresh=False)

    def test_callbacks_never_use_direct_even_without_runtime(self):
        for card in CALLBACKS:
            with self.subTest(card=card):
                ok, _, rows = robot.send_interactive_to_open_ids(card, ['ou_new'])
                self.assertFalse(ok)
                self.assertEqual(rows[0]['failure_kind'], 'agent_required')
        self.factory.assert_not_called()
        self.token.assert_not_called()
        self.wire.assert_not_called()
        self.assertEqual(self.sender.send_direct('ou_new', 'interactive', CALLBACKS[0],
                                                'open_id', 'bypass').status, 'blocked')

    def test_old_pending_uncertain_and_posting_never_repost_or_report_success(self):
        for status in ('posting', 'pending', 'uncertain'):
            for channel in ('legacy', 'agent', 'direct'):
                with self.subTest(status=status, channel=channel):
                    identity = status + channel
                    key = self.sender.route_key('ou_new', 'open_id', identity)
                    original = {'status': status, 'channel': channel, 'app_id': PREVIOUS_APP_ID,
                                'attempted_at': 123., 'receipt': {'message_id': 'om_unconfirmed'}}
                    self.sender.save_route(key, original)
                    ok, _, rows = self.text(identity)
                    self.assertFalse(ok)
                    self.assertEqual(rows[0]['failure_kind'], 'uncertain')
                    self.assertEqual(self.sender.route(key), original)
        self.wire.assert_not_called()
        self.token.assert_not_called()

    def test_old_sent_receipt_is_read_only_history(self):
        key = self.sender.route_key('ou_new', 'open_id', 'stable')
        original = {'status': 'sent', 'channel': 'legacy', 'app_id': PREVIOUS_APP_ID,
                    'receipt': {'sender_app_id': PREVIOUS_APP_ID, 'message_id': 'om_historical',
                                'delivery_channel': 'legacy'}}
        self.sender.save_route(key, copy.deepcopy(original))
        ok, _, rows = self.text()
        self.assertTrue(ok)
        self.assertEqual(rows[0]['sender_app_id'], PREVIOUS_APP_ID)
        self.assertEqual(rows[0]['message_id'], 'om_historical')
        self.assertEqual(self.sender.route(key), original)
        self.wire.assert_not_called()
        self.token.assert_not_called()

    def test_future_qt_app_change_is_dynamic_not_target_locked(self):
        self.assertTrue(self.text()[0])
        self.bind('cli_future', 'ou_future')
        with patch.object(robot.config, 'app_id', 'cli_future'), \
             patch.object(robot.config, 'app_secret', 'offline-rotated-secret'):
            ok, _, rows = self.text('future', 'ou_future')
        self.assertTrue(ok)
        self.assertEqual(rows[0]['sender_app_id'], 'cli_future')
        self.assertEqual(self.wire.call_args.kwargs['json_payload']['receive_id'], 'ou_future')
        self.assertEqual(self.wire.call_args.kwargs['headers']['Authorization'], 'Bearer fixture-token')
        self.assertEqual(self.token.call_count, 2)

    def test_secret_change_on_explicit_token_rejection_never_reposts_under_new_credentials(self):
        def rejected(*args, **kwargs):
            robot.config.app_secret = 'fixture-rotated-secret'
            return {'code': 99991663, 'msg': 'expired'}
        self.wire.side_effect = rejected
        ok, _, rows = self.text()
        self.assertFalse(ok)
        self.assertEqual(rows[0]['failure_kind'], 'uncertain')
        self.wire.assert_called_once()
        self.token.assert_called_once_with(force_refresh=False)
        key = self.sender.route_key('ou_new', 'open_id', 'stable')
        self.assertEqual(self.sender.route(key)['status'], 'posting')

    def test_unreadable_qt_and_unverified_or_ambiguous_open_ids_are_not_sent(self):
        with patch.object(robot.config, 'refresh_credentials', side_effect=OSError('fixture Qt unavailable')):
            self.assertFalse(self.text('unavailable-qt')[0])
        self.token.assert_not_called()
        for recipient in ('ou_foreign', 'ou_old'):
            self.assertFalse(self.text(recipient, recipient)[0])
        from lan_bitable_template_portal.personnel_password_login import DIRECTORY_NS, DIRECTORY_KEY
        self.store.put_document(DIRECTORY_NS, DIRECTORY_KEY, {'app_id': TARGET_APP_ID, 'people': {
            'recOne': {'login_ids': ['ou_new'], 'selectable': True},
            'recTwo': {'login_ids': ['ou_new'], 'selectable': True}}})
        self.assertFalse(self.text('ambiguous')[0])
        self.wire.assert_not_called()

    def test_explicit_union_pass_through_but_sessions_never_change_the_recipient(self):
        self.assertTrue(self.text('union', 'on_common')[0])
        self.assertEqual(self.wire.call_args.kwargs['params']['receive_id_type'], 'union_id')
        with tempfile.TemporaryDirectory() as directory:
            self.store.db_path = Path(directory) / 'fixture.sqlite3'
            with closing(sqlite3.connect(self.store.db_path)) as db, db:
                db.execute('CREATE TABLE auth_sessions (user_json TEXT)')
                db.execute('INSERT INTO auth_sessions VALUES (?)',
                           (json.dumps({'open_id': 'ou_unmapped', 'union_id': 'on_verified', 'app_id': PREVIOUS_APP_ID}),))
            self.assertFalse(self.text('old-session', 'ou_unmapped')[0])
            with closing(sqlite3.connect(self.store.db_path)) as db, db:
                db.execute('UPDATE auth_sessions SET user_json=?',
                           (json.dumps({'open_id': 'ou_unmapped', 'union_id': 'on_verified', 'app_id': TARGET_APP_ID}),))
            self.assertTrue(self.text('session', 'ou_unmapped')[0])
        self.assertEqual(self.wire.call_args.kwargs['json_payload']['receive_id'], 'ou_unmapped')
        self.assertEqual(self.wire.call_args.kwargs['params']['receive_id_type'], 'open_id')

    def test_app_change_during_token_refresh_cannot_repost_with_new_token(self):
        def reject(*args, **kwargs):
            robot.config.app_id = 'cli_future'
            return {'code': 99991663, 'msg': 'expired'}
        self.wire.side_effect = reject
        ok, _, rows = self.text()
        self.assertFalse(ok)
        self.assertEqual(rows[0]['failure_kind'], 'uncertain')
        self.token.assert_called_once_with(force_refresh=False)
        self.wire.assert_called_once()
        key = self.sender.route_key('ou_new', 'open_id', 'stable')
        self.assertEqual(self.sender.route(key)['app_id'], TARGET_APP_ID)
        self.assertEqual(self.sender.route(key)['status'], 'posting')

    def test_current_uncertain_receipt_remains_pinned_after_future_app_change(self):
        from upload_event_module.services.http_client import FeishuHTTPError
        self.wire.side_effect = FeishuHTTPError('offline timeout')
        self.assertFalse(self.text()[0])
        key = self.sender.route_key('ou_new', 'open_id', 'stable')
        original = self.sender.route(key)
        with patch.object(robot.config, 'app_id', 'cli_future'):
            ok, _, rows = self.text()
        self.assertFalse(ok)
        self.assertEqual(rows[0]['failure_kind'], 'uncertain')
        self.assertEqual(self.sender.route(key), original)
        self.wire.assert_called_once()

    def test_wrong_group_receipt_stays_uncertain(self):
        ok, _ = robot.send_text_to_chat_id('offline', 'oc_original', message_uuid='group')
        self.assertFalse(ok)
        key = self.sender.route_key('oc_original', 'chat_id', 'group')
        self.assertEqual(self.sender.route(key)['status'], 'posting')

    def test_group_cache_is_invalidated_when_qt_changes_app(self):
        with patch.object(robot, '_CHAT_CACHE', {'items': [], 'timestamp': 0.}), \
             patch.object(robot, '_get_bot_chats', side_effect=[
                 ([{'chat_id': 'oc_first'}], ''), ([{'chat_id': 'oc_second'}], '')]) as chats:
            self.assertEqual(robot._get_chat_cache()[0][0]['chat_id'], 'oc_first')
            self.assertEqual(robot._get_chat_cache()[0][0]['chat_id'], 'oc_first')
            with patch.object(robot.config, 'app_id', 'cli_future'):
                self.assertEqual(robot._get_chat_cache()[0][0]['chat_id'], 'oc_second')
        self.assertEqual(chats.call_count, 2)

    def test_unreadable_ledger_cannot_be_downgraded_to_definitely_not_sent(self):
        with patch.object(self.sender, 'route', side_effect=OSError('offline ledger unavailable')):
            ok, _, rows = self.text()
        self.assertFalse(ok)
        self.assertEqual(rows[0]['failure_kind'], 'uncertain')
        self.wire.assert_not_called()
        self.token.assert_not_called()

    def test_standalone_restarts_read_the_same_persistent_receipt(self):
        from lan_bitable_template_portal.state_store import LanPortalStateStore
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'fixture.sqlite3'
            store = LanPortalStateStore(db_path=path)
            store.put_document('feishu_assistant_sender', TARGET_APP_ID + ':ou_current',
                               {'open_id': 'ou_current', 'matched_by': 'union_id'})
            first = PublicFeishuSender(SimpleNamespace(state_store=store))
            with patch('lan_bitable_template_portal.feishu_delivery.standalone_sender', return_value=first):
                self.assertTrue(self.text(recipient='ou_current')[0])
            restarted = PublicFeishuSender(SimpleNamespace(state_store=LanPortalStateStore(db_path=path)))
            with patch('lan_bitable_template_portal.feishu_delivery.standalone_sender', return_value=restarted):
                ok, _, rows = self.text(recipient='ou_current')
            self.assertTrue(ok)
            self.assertEqual(rows[0]['sender_app_id'], TARGET_APP_ID)
            self.wire.assert_called_once()


if __name__ == '__main__':
    unittest.main()

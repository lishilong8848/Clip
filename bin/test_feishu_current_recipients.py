"""Current-only recipient proof and exact migrated operation lineage; entirely offline."""
import asyncio
import copy
from contextlib import closing
import hashlib
import json
from pathlib import Path
import socket
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from bin.test_feishu_delivery import Store, TARGET_APP_ID, PREVIOUS_APP_ID, qt_credentials_fixture, active_config, response
from lan_bitable_template_portal.feishu_delivery import PublicFeishuSender, FeishuDeliveryError, current_recipient_open_id
from lan_bitable_template_portal.personnel_password_login import DIRECTORY_NS, DIRECTORY_KEY
from lan_bitable_template_portal.work_handover_cards import HandoverCards, HandoverError
from upload_event_module.services import robot_webhook as robot


class CurrentRecipientTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(qt_credentials_fixture())
        self.loop = asyncio.new_event_loop()
        self.addCleanup(self.loop.close)
        self.enterContext(patch.object(socket.socket, 'connect', side_effect=AssertionError('network forbidden')))
        self.enterContext(patch('lan_bitable_template_portal.feishu_identity_manifest.load_identity_manifest',
                                return_value={'mappings': []}))
        self.store = Store()
        self.auth = SimpleNamespace(directory_person=Mock(return_value={}))
        self.runtime = SimpleNamespace(state_store=self.store, auth_manager=self.auth)
        self.sender = PublicFeishuSender(self.runtime)

    def directory(self, people, app_id=TARGET_APP_ID):
        self.store.put_document(DIRECTORY_NS, DIRECTORY_KEY, {'app_id': app_id, 'people': people})

    def person(self, open_id='ou_current', **changes):
        return {'id': 'recPerson', 'login_ids': [open_id], 'selectable': True, 'inactive': False, **changes}

    def test_current_personnel_directory_needs_no_login_or_mapping(self):
        self.directory({'recPerson': self.person()})
        self.assertEqual(self.sender.direct_recipient('ou_current', 'open_id'), ('open_id', 'ou_current'))
        self.auth.current_feishu_open_id = Mock(side_effect=AssertionError('alias forbidden'))
        bot = SimpleNamespace(config=active_config(), messenger=SimpleNamespace(
            recipient_open_id=AsyncMock(side_effect=AssertionError('cross-app lookup forbidden'))))
        self.assertEqual(self.loop.run_until_complete(self.sender.recipient(bot, 'ou_current')), ('open_id', 'ou_current'))
        self.auth.current_feishu_open_id.assert_not_called()
        bot.messenger.recipient_open_id.assert_not_awaited()

    def test_explicit_current_app_field_does_not_authorize_its_old_login_alias(self):
        self.directory({'recPerson': self.person('ou_old', feishu_open_ids={TARGET_APP_ID: 'ou_current'})})
        self.assertEqual(self.sender.direct_recipient('ou_current', 'open_id'), ('open_id', 'ou_current'))
        self.store.put_document('feishu_assistant_sender', TARGET_APP_ID + ':ou_old',
                                {'open_id': 'ou_old', 'matched_by': 'record_id'})
        with self.assertRaises(FeishuDeliveryError):
            self.sender.direct_recipient('ou_old', 'open_id')

    def test_previous_app_or_cross_id_binding_cannot_replace_the_input(self):
        for key, payload in ((PREVIOUS_APP_ID + ':ou_current', {'open_id': 'ou_current', 'matched_by': 'record_id'}),
                             (TARGET_APP_ID + ':ou_different', {'open_id': 'ou_current', 'matched_by': 'record_id'}),
                             (TARGET_APP_ID + ':ou_current', {'open_id': 'ou_different', 'matched_by': 'admin'})):
            with self.subTest(key=key, payload=payload):
                self.store.data.clear()
                self.store.put_document('feishu_assistant_sender', key, payload)
                with self.assertRaises(FeishuDeliveryError):
                    self.sender.direct_recipient('ou_current', 'open_id')

    def test_inactive_ambiguous_and_untrusted_current_rows_are_rejected(self):
        for people in ({'recPerson': self.person(inactive=True)},
                       {'recPerson': self.person(selectable=False)},
                       {'recPerson': self.person(), 'recOther': self.person(id='recOther')},
                       {'recPerson': self.person(login_ids=['ou_current', 'ou_different'])}):
            with self.subTest(people=people):
                self.directory(people)
                with self.assertRaises(FeishuDeliveryError):
                    self.sender.direct_recipient('ou_current', 'open_id')
        self.directory({'recPerson': self.person()}, app_id=PREVIOUS_APP_ID)
        with self.assertRaises(FeishuDeliveryError):
            self.sender.direct_recipient('ou_current', 'open_id')

    def test_current_same_id_trusted_binding_is_accepted_without_union_lookup(self):
        for matched_by in ('name', 'unknown', None, 'record_id', 'union_id', 'admin'):
            self.store.put_document('feishu_assistant_sender', TARGET_APP_ID + ':ou_current',
                                    {'open_id': 'ou_current', 'matched_by': matched_by})
            if matched_by in {'record_id', 'union_id', 'admin'}:
                self.assertEqual(self.sender.direct_recipient('ou_current', 'open_id'), ('open_id', 'ou_current'))
            else:
                with self.assertRaises(FeishuDeliveryError):
                    self.sender.direct_recipient('ou_current', 'open_id')

    def test_handover_preserves_current_id_and_requires_exact_person_record(self):
        self.directory({'recPerson': self.person()})
        forbidden = Mock(side_effect=AssertionError('union conversion forbidden'))
        bot = SimpleNamespace(config=active_config(), runtime=self.runtime, permission_cards=SimpleNamespace(union_id=forbidden))
        cards = HandoverCards(bot)
        self.assertEqual(cards.recipient_id({'id': 'recPerson', 'open_id': 'ou_current'}), ('open_id', 'ou_current'))
        with self.assertRaises(HandoverError):
            cards.recipient_id({'id': 'recDifferent', 'open_id': 'ou_current'})
        forbidden.assert_not_called()

    def test_current_session_proves_only_its_unchanged_id_and_must_not_be_expired(self):
        with tempfile.TemporaryDirectory() as temp:
            self.store.db_path = Path(temp) / 'session.sqlite3'
            with closing(sqlite3.connect(self.store.db_path)) as db, db:
                db.execute('CREATE TABLE auth_sessions(user_json TEXT,payload_json TEXT,expires_at REAL,revoked_at REAL)')
                db.execute('INSERT INTO auth_sessions VALUES(?,?,?,NULL)', (
                    json.dumps({'open_id': 'ou_current', 'union_id': 'on_foreign'}),
                    json.dumps({'app_id': TARGET_APP_ID}), 9999999999))
            self.assertEqual(self.sender.direct_recipient('ou_current', 'open_id'), ('open_id', 'ou_current'))
            with self.assertRaises(FeishuDeliveryError):
                self.sender.direct_recipient('ou_different', 'open_id')
            with closing(sqlite3.connect(self.store.db_path)) as db, db:
                db.execute('UPDATE auth_sessions SET expires_at=0')
            with self.assertRaises(FeishuDeliveryError):
                self.sender.direct_recipient('ou_current', 'open_id')

    def test_new_claim_and_receipt_keep_original_operation_inputs(self):
        self.directory({'recPerson': self.person()})
        def post(*args, receipt, **kwargs):
            receipt.update(message_id='om_current', chat_id='oc_current', message_uuid='same-uuid')
            return True, 'ok'
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed': True}), \
             patch.object(robot, '_get_tenant_access_token', return_value=('fixture', '')), \
             patch.object(robot, '_send_message_to_receive_id', side_effect=post) as send:
            result = self.sender.send_direct('ou_current', 'text', {'text': 'same'}, 'open_id', 'same-uuid')
        self.assertEqual(result.status, 'sent')
        row = self.sender.route(self.sender.route_key('ou_current', 'open_id', 'same-uuid'))
        self.assertEqual((row['receive_id'], row['receive_type'], row['message_uuid']), ('ou_current', 'open_id', 'same-uuid'))
        send.assert_called_once()

    def test_verified_ledger_lineage_returns_original_receipt_without_sending(self):
        old_key = self.sender.route_key('ou_before', 'open_id', 'same-uuid')
        row = {'status': 'sent', 'app_id': PREVIOUS_APP_ID, 'channel': 'agent',
               'receipt': {'sender_app_id': PREVIOUS_APP_ID, 'message_id': 'om_original', 'message_uuid': 'same-uuid'},
               'identity_migration': {'version': 1, 'previous_receive_id': 'ou_before', 'new_receive_id': 'ou_current',
                   'receive_type': 'open_id', 'message_uuid': 'same-uuid', 'route_digest_verified': True}}
        self.sender.save_route(old_key, copy.deepcopy(row))
        with patch.object(robot, '_get_tenant_access_token', side_effect=AssertionError('replay forbidden')):
            result = self.sender.send_direct('ou_current', 'text', {'text': 'same'}, 'open_id', 'same-uuid')
        self.assertEqual(result.status, 'sent')
        self.assertEqual(result.receipt, row['receipt'])
        self.assertEqual(self.sender.route(old_key), row)
        self.assertFalse(self.sender.route(self.sender.route_key('ou_current', 'open_id', 'same-uuid')))

    def test_opaque_old_hash_requires_exact_manifest_lineage_and_uuid(self):
        old_key = self.sender.route_key('ou_before', 'open_id', 'original-uuid')
        row = {'status': 'sent', 'app_id': PREVIOUS_APP_ID, 'target': ['open_id', 'ou_current'],
               'receipt': {'sender_app_id': PREVIOUS_APP_ID, 'message_id': 'om_original'},
               'identity_migration': {'version': 1, 'opaque_key': True}}
        self.sender.save_route(old_key, copy.deepcopy(row))
        with patch('lan_bitable_template_portal.feishu_identity_manifest.load_identity_manifest', return_value={
                'mappings': [{'old_open_id': 'ou_before', 'new_open_id': 'ou_current'}]}):
            self.assertEqual(self.sender.delivery_route('ou_current', 'open_id', 'original-uuid'), row)
            self.assertEqual(self.sender.delivery_route('ou_current', 'open_id', 'different-uuid'), {})
            self.assertEqual(self.sender.delivery_route('ou_current', 'chat_id', 'original-uuid'), {})
            with self.assertRaises(FeishuDeliveryError):
                self.sender.direct_recipient('ou_before', 'open_id')

    def test_migrated_unknown_results_stay_uncertain_and_never_use_old_app(self):
        for status in ('pending', 'posting', 'sending', 'uncertain'):
            old_key = self.sender.route_key('ou_before', 'open_id', status)
            row = {'status': status, 'app_id': TARGET_APP_ID, 'target': ['open_id', 'ou_current'],
                   'identity_migration': {'version': 1, 'opaque_key': True, 'blocked': True}}
            self.sender.save_route(old_key, copy.deepcopy(row))
            with patch('lan_bitable_template_portal.feishu_identity_manifest.load_identity_manifest', return_value={
                    'mappings': [{'old_open_id': 'ou_before', 'new_open_id': 'ou_current'}]}), \
                 patch.object(robot, '_get_tenant_access_token', side_effect=AssertionError('replay forbidden')):
                result = self.sender.send_direct('ou_current', 'text', {'text': 'same'}, 'open_id', status)
            self.assertEqual(result.status, 'uncertain')
            self.assertEqual(self.sender.route(old_key), row)

    def test_fixed_current_app_accounts_do_not_authorize_another_app(self):
        from lan_bitable_template_portal.portal_service import BUILDING_OPEN_ID_MAP
        open_id = BUILDING_OPEN_ID_MAP['A']
        with patch('lan_bitable_template_portal.feishu_identity.TARGET_APP_ID', TARGET_APP_ID):
            self.assertEqual(self.sender.direct_recipient(open_id, 'open_id'), ('open_id', open_id))
            with self.assertRaises(FeishuDeliveryError):
                current_recipient_open_id(self.runtime, open_id, PREVIOUS_APP_ID)

    def test_nine_shared_hr_rows_cannot_block_fixed_duty_or_required_l0(self):
        from lan_bitable_template_portal.portal_auth import REQUIRED_ADMIN_USERS
        from lan_bitable_template_portal.portal_service import BUILDING_OPEN_ID_MAP
        fixed_ids = {*BUILDING_OPEN_ID_MAP.values(), *REQUIRED_ADMIN_USERS}
        self.auth.directory_person.return_value = {'selectable': False, 'inactive': True}
        with patch('lan_bitable_template_portal.feishu_identity.TARGET_APP_ID', TARGET_APP_ID):
            for open_id in sorted(fixed_ids):
                with self.subTest(open_id=open_id):
                    self.directory({f'recShared{i}': self.person(open_id, id=f'recShared{i}',
                        selectable=False, inactive=i % 2 == 0) for i in range(9)})
                    self.assertEqual(self.sender.direct_recipient(open_id, 'open_id'), ('open_id', open_id))
                    self.assertEqual(self.loop.run_until_complete(self.sender.recipient(
                        SimpleNamespace(config=active_config()), open_id)), ('open_id', open_id))
        self.auth.directory_person.assert_not_called()

    def test_fixed_duty_explicit_disable_or_quarantine_still_blocks(self):
        from lan_bitable_template_portal.portal_auth import REQUIRED_ADMIN_USERS
        from lan_bitable_template_portal.portal_service import BUILDING_OPEN_ID_MAP
        with patch('lan_bitable_template_portal.feishu_identity.TARGET_APP_ID', TARGET_APP_ID):
            for open_id in (BUILDING_OPEN_ID_MAP['A'], *REQUIRED_ADMIN_USERS):
                for disabled, quarantined in ((True, False), (False, True)):
                    with self.subTest(open_id=open_id, disabled=disabled, quarantined=quarantined):
                        self.auth._open_id_explicitly_disabled = Mock(return_value=disabled)
                        self.auth._identity_quarantined = Mock(return_value=quarantined)
                        with self.assertRaises(FeishuDeliveryError):
                            self.sender.direct_recipient(open_id, 'open_id')
        self.auth.directory_person.assert_not_called()

        from lan_bitable_template_portal.feishu_identity_migration import QUARANTINE_TABLE
        open_id = BUILDING_OPEN_ID_MAP['A']
        standalone = SimpleNamespace(state_store=self.store)
        self.store.get_auth_permissions = Mock(return_value={'users': {open_id: {'enabled': False}}})
        with patch('lan_bitable_template_portal.feishu_identity.TARGET_APP_ID', TARGET_APP_ID):
            with self.assertRaises(FeishuDeliveryError):
                current_recipient_open_id(standalone, open_id, TARGET_APP_ID)
            self.store.get_auth_permissions.return_value = {'users': {}}
            with tempfile.TemporaryDirectory() as temp:
                self.store.db_path = Path(temp) / 'quarantine.sqlite3'
                with closing(sqlite3.connect(self.store.db_path)) as db, db:
                    db.execute('CREATE TABLE ' + QUARANTINE_TABLE + '(identity_hash TEXT)')
                    db.execute('INSERT INTO ' + QUARANTINE_TABLE + ' VALUES(?)',
                               (hashlib.sha256(open_id.encode()).hexdigest(),))
                with self.assertRaises(FeishuDeliveryError):
                    current_recipient_open_id(standalone, open_id, TARGET_APP_ID)

    def test_old_duty_id_and_stale_qt_app_cannot_use_current_fixed_allowlist(self):
        from lan_bitable_template_portal.portal_service import BUILDING_OPEN_ID_MAP
        current_id = BUILDING_OPEN_ID_MAP['A']
        self.directory({'recLegacy': self.person('ou_oldDuty', name='same duty label',
            feishu_open_ids={TARGET_APP_ID: current_id})})
        self.store.put_document('feishu_assistant_sender', TARGET_APP_ID + ':ou_oldDuty',
            {'open_id': 'ou_oldDuty', 'matched_by': 'record_id'})
        with patch('lan_bitable_template_portal.feishu_identity.TARGET_APP_ID', TARGET_APP_ID):
            with self.assertRaises(FeishuDeliveryError):
                self.sender.direct_recipient('ou_oldDuty', 'open_id')
            self.assertEqual(self.sender.direct_recipient(current_id, 'open_id'), ('open_id', current_id))
            with patch.object(robot.config, 'app_id', PREVIOUS_APP_ID), self.assertRaises(FeishuDeliveryError):
                current_recipient_open_id(self.runtime, current_id, TARGET_APP_ID)

    def test_ordinary_people_and_ma_still_require_unique_active_matching_hr_record(self):
        from lan_bitable_template_portal.portal_auth import REQUIRED_ADMIN_USERS
        from lan_bitable_template_portal.portal_service import MA_JINYU_OPEN_ID
        self.assertNotIn(MA_JINYU_OPEN_ID, REQUIRED_ADMIN_USERS)
        with patch('lan_bitable_template_portal.feishu_identity.TARGET_APP_ID', TARGET_APP_ID):
            for open_id in ('ou_current', MA_JINYU_OPEN_ID):
                with self.subTest(open_id=open_id):
                    self.directory({'recPerson': self.person(open_id)})
                    self.assertEqual(current_recipient_open_id(self.runtime, open_id, TARGET_APP_ID,
                        record_id='recPerson'), open_id)
                    with self.assertRaises(FeishuDeliveryError):
                        current_recipient_open_id(self.runtime, open_id, TARGET_APP_ID, record_id='recOther')
                    for people in ({'recPerson': self.person(open_id),
                                    'recOther': self.person(open_id, id='recOther')},
                                   {'recPerson': self.person(open_id, inactive=True)},
                                   {'recPerson': self.person(open_id, selectable=False)}, {}):
                        self.directory(people)
                        with self.assertRaises(FeishuDeliveryError):
                            self.sender.direct_recipient(open_id, 'open_id')

    def test_actual_migration_transform_keeps_original_receipt_and_unknown_result(self):
        from lan_bitable_template_portal.feishu_identity_documents import transform_document
        for status in ('sent', 'sending'):
            old_key = self.sender.route_key('ou_before', 'open_id', status)
            original = {'status': status, 'app_id': PREVIOUS_APP_ID, 'channel': 'agent',
                'target': ['open_id', 'ou_before'], 'receipt': {'message_id': 'om_original',
                    'message_uuid': status, 'sender_app_id': PREVIOUS_APP_ID}}
            key, row = transform_document('feishu_public_delivery', old_key, original,
                                          {'ou_before': 'ou_current'})
            self.assertEqual(key, old_key)
            self.assertEqual(row['receipt'], original['receipt'])
            self.assertEqual(row['app_id'], PREVIOUS_APP_ID)
            self.sender.save_route(key, row)
            with patch.object(robot, '_get_tenant_access_token', side_effect=AssertionError('replay forbidden')):
                result = self.sender.send_direct('ou_current', 'text', {'text': 'same'}, 'open_id', status)
            self.assertEqual(result.status, 'sent' if status == 'sent' else 'uncertain')
            self.assertEqual(self.sender.route(old_key), row)
            if status == 'sent':
                self.assertEqual(result.receipt, original['receipt'])


if __name__ == '__main__':
    unittest.main()

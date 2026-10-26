import datetime as dt
from concurrent.futures import ThreadPoolExecutor
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock, patch

from bin.test_learning import learning, FakeCloud
from lan_bitable_template_portal import learning_reminders as reminders


class ShiftReminderTests(TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.reader = Mock(return_value=[{'open_id': 'ou_night', 'shift': '夜', 'scope': 'A', 'name': '夜班人员'},
                                         {'open_id': 'ou_external', 'shift': '白', 'scope': 'A', 'name': '外部人员'}])
        self.people = {'people': {
            'night': {'name': '夜班人员', 'scopes': ['A'], 'selectable': True, 'position': '值班长', 'login_ids': ['ou_night']},
            'external': {'name': '外部人员', 'scopes': ['A'], 'selectable': True, 'position': '值班员', 'login_ids': ['ou_external']},
        }}
        self.sender = Mock(return_value=(True, '', []))
        self.service = self.make_service()
        with self.service.transaction() as conn:
            self.service._put('settings', 'main', {**learning.DEFAULT_SETTINGS, 'enabled': True}, conn, False)

    def make_service(self):
        return learning.LearningService(root=self.tmp.name, cloud=FakeCloud(), send_message=self.sender,
            get_shift_roster=self.reader, get_portal_url=lambda: 'http://portal.test:18766',
            get_login_people=lambda: self.people)

    def run_at(self, hour, day=10):
        self.service.send_notifications(dt.datetime(2026, 10, day, hour, tzinfo=learning.TZ))

    def test_daily_snapshot_times_restart_and_concurrent_dedup(self):
        self.run_at(7)
        self.reader.assert_not_called()
        self.run_at(8)
        self.sender.assert_not_called()
        self.run_at(13)
        self.assertEqual([c.args[0] for c in self.sender.call_args_list], ['ou_external'])
        self.service = self.make_service()
        with ThreadPoolExecutor(max_workers=5) as pool:
            list(pool.map(lambda _: self.run_at(20), range(5)))
        self.reader.assert_called_once_with('2026-10-10')
        self.assertEqual([c.args[0] for c in self.sender.call_args_list], ['ou_external', 'ou_night'])
        for call in self.sender.call_args_list:
            self.assertEqual(call.args[1]['elements'][-1]['actions'][0]['url'], 'http://portal.test:18766/learning?view=practice')
        self.run_at(13, 11)
        self.assertEqual(self.reader.call_count, 2)

    def test_failure_preserves_other_recipient_and_retries_same_identity(self):
        self.sender.side_effect = [OSError('offline'), (True, '', []), (True, '', [])]
        with patch.object(learning.time, 'time', return_value=1000):
            self.run_at(20)
        self.assertEqual(self.sender.call_count, 2)
        with patch.object(learning.time, 'time', return_value=1301):
            self.run_at(20)
        self.assertEqual(self.sender.call_args_list[0].args[2], self.sender.call_args_list[2].args[2])
        self.reader.assert_called_once()

    def test_delivery_receipt_is_persisted_and_survives_restart(self):
        receipt = {'open_id': 'ou_external', 'ok': True, 'sender_app_id': 'cli_fixture',
                   'message_id': 'om_fixture', 'chat_id': 'oc_fixture', 'message_uuid': 'stable', 'accepted_at': 1000}
        self.sender.return_value = (True, 'ok', [receipt])
        self.run_at(13)
        key = 'shift:2026-10-10:白:ou_external'
        saved = self.service._get('notification', key)
        self.assertEqual(saved['status'], 'sent')
        self.assertEqual(saved['delivery'], [receipt])
        self.assertTrue(saved['sent_at'])
        self.service = self.make_service()
        self.run_at(13)
        self.sender.assert_called_once()
        self.assertEqual(self.service._get('notification', key)['delivery'], [receipt])

    def test_incomplete_send_result_stays_pending_and_later_receipt_is_saved(self):
        self.sender.return_value = (False, '发送结果未确认', [{'open_id': 'ou_external', 'ok': False}])
        with patch.object(learning.time, 'time', return_value=1000):
            self.run_at(13)
        key = 'shift:2026-10-10:白:ou_external'
        self.assertEqual(self.service._get('notification', key)['status'], 'pending')
        self.assertNotIn('sent_at', self.service._get('notification', key))
        receipt = {'open_id': 'ou_external', 'ok': True, 'message_id': 'om_retry', 'chat_id': 'oc_retry'}
        self.sender.return_value = (True, 'ok', [receipt])
        self.service = self.make_service()
        with patch.object(learning.time, 'time', return_value=1301):
            self.run_at(13)
        self.assertEqual(self.sender.call_args_list[0].args[2], self.sender.call_args_list[1].args[2])
        self.assertEqual(self.service._get('notification', key)['delivery'], [receipt])

    def test_real_card_adapter_requires_receipt_before_marking_sent(self):
        from upload_event_module.services import robot_webhook
        from bin.test_feishu_single_app_delivery import standalone_delivery_fixture
        self.service._sender = None
        key = 'shift:2026-10-10:白:ou_external'
        with standalone_delivery_fixture('ou_external'), \
             patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed': True}), \
             patch.object(robot_webhook, '_get_tenant_access_token', return_value=('fixture', '')), \
             patch.object(robot_webhook, '_request_json', side_effect=[{'code': 0},
                 {'code': 0, 'data': {'message_id': 'om_realreceipt', 'chat_id': 'oc_private'}}]) as transport:
            with patch.object(learning.time, 'time', return_value=1000):
                self.run_at(13)
            first = self.service._get('notification', key)
            self.assertEqual(first['status'], 'pending')
            self.assertIn('未确认', first['error'])
            self.assertNotIn('sent_at', first)
            with patch.object(learning.time, 'time', return_value=1301):
                self.run_at(13)
        saved = self.service._get('notification', key)
        self.assertEqual(saved['status'], 'sent')
        self.assertEqual(saved['delivery'][0]['open_id'], 'ou_external')
        self.assertEqual(saved['delivery'][0]['message_id'], 'om_realreceipt')
        self.assertEqual(saved['delivery'][0]['chat_id'], 'oc_private')
        self.assertEqual(transport.call_args_list[0].kwargs['json_payload']['uuid'],
                         transport.call_args_list[1].kwargs['json_payload']['uuid'])

    def test_reader_failure_keeps_building_reminder_and_retries_later(self):
        self.reader.side_effect = ValueError('incomplete')
        with self.service.transaction() as conn:
            self.service._put('notification', 'building', {'id': 'building', 'scope': 'A', 'date': '2026-10-10', 'kind': 'publish', 'status': 'pending'}, conn)
        with patch.object(reminders.time, 'time', return_value=1000):
            self.run_at(13)
            self.run_at(13)
        self.reader.assert_called_once()
        self.assertEqual(self.sender.call_args.args[0], 'A')
        self.assertEqual(self.sender.call_args.args[1]['elements'][-1]['actions'][0]['url'], 'http://portal.test:18766/learning?scope=A')

    def test_disabled_never_reads_or_sends(self):
        with self.service.transaction() as conn:
            self.service._put('settings', 'main', learning.DEFAULT_SETTINGS, conn, False)
        self.run_at(13)
        self.reader.assert_not_called()
        self.sender.assert_not_called()

    def test_roster_not_position_decides_recipients_and_cached_identity_is_rechecked(self):
        self.people['people'].update({
            'engineer': {'name': '人员甲', 'scopes': ['A'], 'selectable': True, 'position': '工程师', 'login_ids': ['ou_engineer']},
            'blank': {'name': '人员乙', 'scopes': ['A'], 'selectable': True, 'position': '', 'login_ids': ['ou_blank']},
        })
        self.reader.return_value += [{'open_id': oid, 'shift': '白', 'scope': 'A', 'name': name}
                                    for oid, name in (('ou_engineer', '人员甲'), ('ou_blank', '人员乙'))]
        self.run_at(13)
        self.assertCountEqual([c.args[0] for c in self.sender.call_args_list], ['ou_external', 'ou_engineer', 'ou_blank'])
        self.people['people']['night']['inactive'] = True
        with self.service.transaction() as conn:
            key = 'shift:2026-10-10:夜:ou_night'
            self.service._put('notification', key, {'id': key, 'kind': 'shift', 'shift': '夜',
                'scope': '', 'date': '2026-10-10', 'open_id': 'ou_night', 'status': 'pending'}, conn)
        self.run_at(20)
        self.assertEqual(self.service._get('notification', key)['status'], 'skipped_roster')
        self.assertCountEqual([c.args[0] for c in self.sender.call_args_list], ['ou_external', 'ou_engineer', 'ou_blank'])
        self.reader.assert_called_once()

    def test_upgrade_reloads_text_roster_and_recovers_skipped_without_resending_sent(self):
        self.reader.return_value.append({'open_id': '', 'shift': '白', 'scope': 'A', 'name': '漏填人员'})
        self.people['people']['missing'] = {'name': '漏填人员', 'scopes': ['A'], 'selectable': True,
                                           'position': '', 'login_ids': ['ou_missing']}
        with self.service.transaction() as conn:
            self.service._put('local', 'shift_roster', {'date': '2026-10-10', 'version': 2,
                'people': self.reader.return_value[:2]}, conn, False)
            for oid, status in (('ou_external', 'sent'), ('ou_missing', 'skipped_role')):
                key = 'shift:2026-10-10:白:' + oid
                self.service._put('notification', key, {'id': key, 'kind': 'shift', 'shift': '白',
                    'scope': '', 'date': '2026-10-10', 'open_id': oid, 'status': status}, conn)
        self.run_at(13)
        self.assertEqual([call.args[0] for call in self.sender.call_args_list], ['ou_missing'])
        self.assertEqual(self.service._get('local', 'shift_roster')['version'], reminders.ROSTER_VERSION)
        self.service = self.make_service()
        self.run_at(13)
        self.sender.assert_called_once()
        self.reader.assert_called_once()

    def test_unresolved_name_does_not_block_others_and_recovers_from_local_directory(self):
        self.reader.return_value.append({'open_id': '', 'shift': '白', 'scope': 'A', 'name': '漏填人员'})
        self.run_at(13)
        self.assertEqual([call.args[0] for call in self.sender.call_args_list], ['ou_external'])
        self.assertIn('漏填人员', self.service._last_error)
        self.people['people']['missing'] = {'name': '漏填人员', 'scopes': ['A'], 'selectable': True,
                                           'login_ids': ['ou_missing']}
        self.run_at(13)
        self.assertEqual([call.args[0] for call in self.sender.call_args_list], ['ou_external', 'ou_missing'])
        self.reader.assert_called_once()

    def test_missing_local_role_directory_defers_until_recovered(self):
        saved = self.people
        self.people = {}
        self.run_at(20)
        self.reader.assert_not_called()
        self.sender.assert_not_called()
        self.people = saved
        self.run_at(20)
        self.assertEqual(self.sender.call_count, 2)

    def test_old_pending_person_not_in_today_roster_does_not_receive(self):
        self.people['people']['resting'] = {'scopes': ['A'], 'selectable': True, 'position': '值班员', 'login_ids': ['ou_resting']}
        key = 'shift:2026-10-10:白:ou_resting'
        with self.service.transaction() as conn:
            self.service._put('notification', key, {'id': key, 'kind': 'shift', 'shift': '白',
                'scope': '', 'date': '2026-10-10', 'open_id': 'ou_resting', 'status': 'pending'}, conn)
        self.run_at(13)
        self.assertEqual(self.service._get('notification', key)['status'], 'skipped_roster')
        self.assertEqual([c.args[0] for c in self.sender.call_args_list], ['ou_external'])

    def test_duty_policy_rejects_shared_inactive_ambiguous_and_other_roles(self):
        self.people['people'].update({
            'fallback': {'scopes': ['B'], 'selectable': True, 'assignments': ['值班长'], 'login_ids': ['ou_fallback']},
            'left': {'scopes': ['A'], 'selectable': True, 'inactive': True, 'position': '值班员', 'login_ids': ['ou_left']},
            'station': {'scopes': ['110'], 'selectable': True, 'position': '值班长', 'login_ids': ['ou_station']},
            'manager': {'scopes': ['H'], 'selectable': True, 'position': '经理', 'login_ids': ['ou_manager']},
            'shared': {'scopes': ['A'], 'selectable': True, 'position': '值班员', 'login_ids': ['ou_duty']},
            'duplicate': {'scopes': ['A'], 'selectable': True, 'position': '工程师', 'login_ids': ['ou_external']},
        })
        with patch('lan_bitable_template_portal.portal_service.BUILDING_OPEN_ID_MAP', {'A': 'ou_duty'}):
            self.assertEqual(reminders.duty_recipients(self.service), {'ou_night', 'ou_fallback'})

    def test_old_pending_night_waits_until_new_time(self):
        key = 'shift:2026-10-10:夜:ou_night'
        with self.service.transaction() as conn:
            self.service._put('notification', key, {'id': key, 'kind': 'shift', 'shift': '夜',
                'scope': '', 'date': '2026-10-10', 'open_id': 'ou_night', 'status': 'pending'}, conn)
        self.run_at(13)
        self.assertEqual([c.args[0] for c in self.sender.call_args_list], ['ou_external'])
        self.run_at(20)
        self.run_at(21)
        self.assertEqual([c.args[0] for c in self.sender.call_args_list], ['ou_external', 'ou_night'])

    def test_old_single_time_settings_get_new_shift_defaults_without_resetting_other_flags(self):
        with self.service.transaction() as conn:
            self.service._put('settings', 'main', {'enabled': True, 'publish_time': '08:00',
                'reminder_enabled': True, 'reminder_time': '18:00'}, conn, False)
        settings = self.service.settings()
        self.assertEqual((settings['publish_time'], settings['night_publish_time']), ('13:00', '20:00'))
        self.assertTrue(settings['reminder_enabled'])
        self.assertEqual(settings['reminder_time'], '18:00')

    def test_legacy_eight_am_night_default_moves_to_twenty_without_resetting_flags(self):
        with self.service.transaction() as conn:
            self.service._put('settings', 'main', {'enabled': True, 'publish_time': '13:00',
                'night_publish_time': '08:00', 'reminder_enabled': True, 'reminder_time': '18:00'}, conn, False)
        self.service = self.make_service()
        settings = self.service.settings()
        self.assertEqual((settings['publish_time'], settings['night_publish_time']), ('13:00', '20:00'))
        self.assertTrue(settings['enabled'])
        self.assertTrue(settings['reminder_enabled'])
        self.assertEqual(settings['reminder_time'], '18:00')
        self.run_at(8)
        self.sender.assert_not_called()
        self.run_at(13)
        self.assertEqual([call.args[0] for call in self.sender.call_args_list], ['ou_external'])
        self.run_at(20)
        self.assertEqual([call.args[0] for call in self.sender.call_args_list], ['ou_external', 'ou_night'])

    def test_newly_saved_explicit_night_time_is_not_repeatedly_migrated(self):
        self.service.dispatch('settings.save', {'night_publish_time': '08:00'}, {'id': 'fixture-admin', 'is_admin': True}, {})
        self.service = self.make_service()
        self.assertEqual(self.service.settings()['night_publish_time'], '08:00')

    def test_configured_shift_times_apply_to_both_people_and_cached_roster(self):
        with self.service.transaction() as conn:
            self.service._put('settings', 'main', {**learning.DEFAULT_SETTINGS, 'enabled': True,
                'publish_time': '14:00', 'night_publish_time': '21:00'}, conn, False)
        self.run_at(13)
        self.reader.assert_not_called()
        self.run_at(14)
        self.run_at(20)
        self.assertEqual([c.args[0] for c in self.sender.call_args_list], ['ou_external'])
        self.run_at(21)
        self.assertEqual(self.sender.call_count, 2)
        self.reader.assert_called_once()

    def test_restored_invalid_shift_times_fall_back_without_stopping_worker(self):
        with self.service.transaction() as conn:
            self.service._put('settings', 'main', {'enabled': True, 'publish_time': None,
                'night_publish_time': '25:61'}, conn, False)
        self.assertEqual(reminders.shift_times(self.service.settings()), {'白': '13:00', '夜': '20:00'})

    def test_employee_pool_read_failure_preserves_existing_notifications(self):
        from lan_bitable_template_portal import personnel_source
        with patch.object(personnel_source, 'daily_roster', side_effect=ValueError('source unavailable')):
            with self.assertRaises(ValueError): reminders.read_roster(Mock(), '2026-10-10')
        self.sender.assert_not_called()

    def test_sender_guard_and_retry_uuid(self):
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed': True}), \
             patch('lan_bitable_template_portal.portal_service.BUILDING_OPEN_ID_MAP', {'A': 'ou_A'}), \
             patch('upload_event_module.services.robot_webhook.send_interactive_to_open_ids', return_value=(True, '', [])) as send:
            card = reminders.reminder_card('学练', 'http://portal.test/learning')
            reminders.send_reminder('A', card, 'stable')
            first = send.call_args
            reminders.send_reminder('A', card, 'stable')
            self.assertEqual(send.call_args, first)
            self.assertEqual(first.args, (card, ['ou_A']))
            self.assertTrue(first.kwargs['message_uuid'])

    def test_group_sender_reuses_uuid_and_real_write_guard(self):
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed': True}) as guard, \
             patch('upload_event_module.services.robot_webhook.send_interactive_to_chat_id', return_value=(True, 'ok')) as send:
            card = reminders.reminder_card('周报', 'http://portal.test/learning')
            reminders.send_reminder('oc_group', card, 'week:group')
            first = send.call_args
            reminders.send_reminder('oc_group', card, 'week:group')
            self.assertEqual(send.call_args, first)
            self.assertEqual(first.args, (card, 'oc_group'))
            self.assertTrue(first.kwargs['message_uuid'])
            send.reset_mock()
            guard.return_value = {'real_write_allowed': False}
            self.assertFalse(reminders.send_reminder('oc_group', card, 'week:group')[0])
            guard.return_value = {'mock_external': True}
            self.assertTrue(reminders.send_reminder('oc_group', card, 'week:group')[0])
            send.assert_not_called()

    def test_group_transport_passes_uuid_to_feishu_and_old_call_still_works(self):
        from upload_event_module.services import robot_webhook
        from bin.test_feishu_single_app_delivery import standalone_delivery_fixture
        from uuid import UUID
        with standalone_delivery_fixture(), \
             patch.object(robot_webhook, '_get_tenant_access_token', return_value=('fixture', '')), \
             patch.object(robot_webhook, '_request_json', return_value={
                 'code': 0, 'data': {'message_id': 'om_group', 'chat_id': 'oc_group'}}) as send:
            card = reminders.reminder_card('周报', 'http://portal.test/learning')
            self.assertTrue(robot_webhook.send_interactive_to_chat_id(card, 'oc_group', message_uuid='stable')[0])
            self.assertEqual(send.call_args.kwargs['params'], {'receive_id_type': 'chat_id'})
            self.assertEqual(send.call_args.kwargs['json_payload']['uuid'], 'stable')
            self.assertTrue(robot_webhook.send_interactive_to_chat_id(card, 'oc_group')[0])
            self.assertEqual(str(UUID(send.call_args.kwargs['json_payload']['uuid'])),
                             send.call_args.kwargs['json_payload']['uuid'])

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
        self.reader = Mock(return_value=[{'open_id': 'ou_night', 'shift': '夜', 'scope': 'A'}, {'open_id': 'ou_external', 'shift': '白', 'scope': 'A'}])
        self.people = {'people': {
            'night': {'scopes': ['A'], 'selectable': True, 'position': '值班长', 'login_ids': ['ou_night']},
            'external': {'scopes': ['A'], 'selectable': True, 'position': '值班员', 'login_ids': ['ou_external']},
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

    def test_only_current_duty_roles_receive_and_cached_roster_is_rechecked(self):
        self.people['people'].update({
            'engineer': {'scopes': ['A'], 'selectable': True, 'position': '工程师', 'login_ids': ['ou_engineer']},
            'supervisor': {'scopes': ['A'], 'selectable': True, 'position': '主管', 'login_ids': ['ou_supervisor']},
        })
        self.reader.return_value += [{'open_id': oid, 'shift': '白', 'scope': 'A'} for oid in ('ou_engineer', 'ou_supervisor')]
        self.run_at(13)
        self.assertEqual([c.args[0] for c in self.sender.call_args_list], ['ou_external'])
        self.people['people']['night']['position'] = '工程师'
        with self.service.transaction() as conn:
            key = 'shift:2026-10-10:夜:ou_night'
            self.service._put('notification', key, {'id': key, 'kind': 'shift', 'shift': '夜',
                'scope': '', 'date': '2026-10-10', 'open_id': 'ou_night', 'status': 'pending'}, conn)
        self.run_at(20)
        self.assertEqual(self.service._get('notification', key)['status'], 'skipped_role')
        self.assertEqual([c.args[0] for c in self.sender.call_args_list], ['ou_external'])
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

    def test_roster_view_pagination_dates_exclusions_and_external_users(self):
        timestamp = int(dt.datetime(2026, 10, 10, tzinfo=learning.TZ).timestamp()*1000)
        def row(group='A班', shift='夜', oid='ou_test', day=timestamp, scope='A楼'):
            return {'fields': {'班组': group, '班次': shift, '排班日期': day, '机楼': scope, '人员': {'users': [{'id': oid}]}}}
        service = Mock()
        service._request_json.side_effect = [
            {'code': 0, 'data': {'items': [row(), row('长白', oid='ou_longday'), row('110站'), row(day=timestamp-86400000), row(shift='休'), row(scope='110站', oid='ou_station')], 'has_more': True, 'page_token': 'next'}},
            {'code': 0, 'data': {'items': [row(), row(shift='白', oid='ou_external'), row(scope='H楼', oid='ou_H')], 'has_more': False}},
        ]
        self.assertEqual(reminders.read_roster(service, '2026-10-10'), [
            {'open_id': 'ou_test', 'shift': '夜', 'scope': 'A', 'name': ''},
            {'open_id': 'ou_external', 'shift': '白', 'scope': 'A', 'name': ''},
            {'open_id': 'ou_H', 'shift': '夜', 'scope': 'H', 'name': ''}])
        self.assertEqual(service._request_json.call_args.kwargs['params']['view_id'], reminders.VIEW_ID)
        service._request_json.side_effect = None
        service._request_json.return_value = {'code': 0, 'data': {'items': [row()], 'has_more': True}}
        with self.assertRaises(ValueError): reminders.read_roster(service, '2026-10-10')

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
        with patch.object(robot_webhook, '_get_tenant_access_token', return_value=('fixture', '')), \
             patch.object(robot_webhook, '_send_interactive_to_receive_id', return_value=(True, 'ok')) as send:
            card = reminders.reminder_card('周报', 'http://portal.test/learning')
            robot_webhook.send_interactive_to_chat_id(card, 'oc_group', message_uuid='stable')
            self.assertEqual(send.call_args.kwargs, {'receive_id_type': 'chat_id', 'message_uuid': 'stable'})
            robot_webhook.send_interactive_to_chat_id(card, 'oc_group')
            self.assertEqual(send.call_args.kwargs['message_uuid'], '')

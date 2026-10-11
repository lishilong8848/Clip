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
        self.reader = Mock(return_value=[{'open_id': 'ou_night', 'shift': '夜'}, {'open_id': 'ou_external', 'shift': '白'}])
        self.sender = Mock(return_value=(True, '', []))
        self.service = self.make_service()
        with self.service.transaction() as conn:
            self.service._put('settings', 'main', {**learning.DEFAULT_SETTINGS, 'enabled': True}, conn, False)

    def make_service(self):
        return learning.LearningService(root=self.tmp.name, cloud=FakeCloud(), send_message=self.sender,
            get_shift_roster=self.reader, get_portal_url=lambda: 'http://portal.test:18766')

    def run_at(self, hour, day=10):
        self.service.send_notifications(dt.datetime(2026, 10, day, hour, tzinfo=learning.TZ))

    def test_daily_snapshot_times_restart_and_concurrent_dedup(self):
        self.run_at(7)
        self.reader.assert_not_called()
        self.run_at(8)
        self.assertEqual([c.args[0] for c in self.sender.call_args_list], ['ou_night'])
        self.service = self.make_service()
        with ThreadPoolExecutor(max_workers=5) as pool:
            list(pool.map(lambda _: self.run_at(13), range(5)))
        self.reader.assert_called_once_with('2026-10-10')
        self.assertEqual([c.args[0] for c in self.sender.call_args_list], ['ou_night', 'ou_external'])
        for call in self.sender.call_args_list:
            self.assertEqual(call.args[1]['elements'][-1]['actions'][0]['url'], 'http://portal.test:18766/learning?view=practice')
        self.run_at(8, 11)
        self.assertEqual(self.reader.call_count, 2)

    def test_failure_preserves_other_recipient_and_retries_same_identity(self):
        self.sender.side_effect = [OSError('offline'), (True, '', []), (True, '', [])]
        with patch.object(learning.time, 'time', return_value=1000):
            self.run_at(13)
        self.assertEqual(self.sender.call_count, 2)
        with patch.object(learning.time, 'time', return_value=1301):
            self.run_at(13)
        self.assertEqual(self.sender.call_args_list[0].args[2], self.sender.call_args_list[2].args[2])
        self.reader.assert_called_once()

    def test_reader_failure_keeps_building_reminder_and_retries_later(self):
        self.reader.side_effect = ValueError('incomplete')
        with self.service.transaction() as conn:
            self.service._put('notification', 'building', {'id': 'building', 'scope': 'A', 'date': '2026-10-10', 'kind': 'publish', 'status': 'pending'}, conn)
        with patch.object(reminders.time, 'time', return_value=1000):
            self.run_at(8)
            self.run_at(8)
        self.reader.assert_called_once()
        self.assertEqual(self.sender.call_args.args[0], 'A')
        self.assertEqual(self.sender.call_args.args[1]['elements'][-1]['actions'][0]['url'], 'http://portal.test:18766/learning?scope=A')

    def test_disabled_never_reads_or_sends(self):
        with self.service.transaction() as conn:
            self.service._put('settings', 'main', learning.DEFAULT_SETTINGS, conn, False)
        self.run_at(13)
        self.reader.assert_not_called()
        self.sender.assert_not_called()

    def test_roster_view_pagination_dates_exclusions_and_external_users(self):
        timestamp = int(dt.datetime(2026, 10, 10, tzinfo=learning.TZ).timestamp()*1000)
        def row(group='A班', shift='夜', oid='ou_test', day=timestamp):
            return {'fields': {'班组': group, '班次': shift, '排班日期': day, '人员': {'users': [{'id': oid}]}}}
        service = Mock()
        service._request_json.side_effect = [
            {'code': 0, 'data': {'items': [row(), row('长白'), row('110站'), row(day=timestamp-86400000)], 'has_more': True, 'page_token': 'next'}},
            {'code': 0, 'data': {'items': [row(), row(shift='白', oid='ou_external')], 'has_more': False}},
        ]
        self.assertEqual(reminders.read_roster(service, '2026-10-10'), [
            {'open_id': 'ou_test', 'shift': '夜', 'name': ''}, {'open_id': 'ou_external', 'shift': '白', 'name': ''}])
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

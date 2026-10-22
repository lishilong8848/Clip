"""Isolated read-only delivery reporting; no real directory, config, or cloud access."""
import copy
import datetime as dt
import json
import socket
import sys
import types
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

BIN = Path(__file__).resolve().parent
if str(BIN) not in sys.path:
    sys.path.insert(0, str(BIN))

from lan_bitable_template_portal import learning as core
from lan_bitable_template_portal import learning_delivery_status as delivery
from lan_bitable_template_portal.learning_reminders import ROSTER_VERSION

DATE = '2026-10-22'
NOW = dt.datetime.fromisoformat(DATE + 'T21:00:00+08:00')


class DeliveryStatusTests(unittest.TestCase):
    def setUp(self):
        self.temp = self.enterContext(TemporaryDirectory())
        self.enterContext(patch.object(socket.socket, 'connect', side_effect=AssertionError('network forbidden')))
        self.enterContext(patch.object(socket.socket, 'connect_ex', side_effect=AssertionError('network forbidden')))
        # The resolver only needs these constant maps; do not import the application runtime.
        portal = types.ModuleType('lan_bitable_template_portal.portal_service')
        portal.BUILDING_OPEN_ID_MAP = {}
        identity = types.ModuleType('lan_bitable_template_portal.feishu_identity')
        identity.TARGET_DUTY_OPEN_IDS = {}
        self.enterContext(patch.dict(sys.modules, {portal.__name__: portal, identity.__name__: identity}))
        self.directory = {'schema': 4, 'app_id': 'cli_test', 'people': {
            'one': {'name': '甲', 'scopes': ['A'], 'selectable': True, 'login_ids': ['ou_one']},
            'two': {'name': '乙', 'scopes': ['B'], 'selectable': True, 'login_ids': ['ou_two']},
        }}
        self.cloud = Mock(spec=['current_app_id'])
        self.cloud.current_app_id.return_value = 'cli_test'
        self.sender = Mock(side_effect=AssertionError('send forbidden'))
        self.reader = Mock(side_effect=AssertionError('roster refresh forbidden'))
        self.service = core.LearningService(self.temp, cloud=self.cloud, send_message=self.sender,
            get_login_people=lambda: self.directory, get_shift_roster=self.reader)
        self.put('settings', 'main', {**core.DEFAULT_SETTINGS, 'enabled': True})
        self.roster()
        self.put('publication', self.service._publication_key(DATE), {'date': DATE})

    def put(self, kind, key, value, dirty=False):
        with self.service.transaction() as conn:
            self.service._put(kind, key, value, conn, dirty)

    def roster(self, people=None, date=DATE, version=ROSTER_VERSION, dated=True):
        people = people if people is not None else [
            {'name': '甲', 'scope': 'A', 'shift': '白', 'open_id': ''},
            {'name': '乙', 'scope': 'B', 'shift': '白', 'open_id': ''},
            {'name': '甲', 'scope': 'A', 'shift': '夜', 'open_id': ''}]
        self.put('local', 'shift_roster:' + date if dated else 'shift_roster',
                 {'date': date, 'version': version, 'people': people})

    def notice(self, oid='ou_one', shift='白', status='sent', date=DATE, receipts=None, **extra):
        value = {'id': 'shift:' + date + ':' + shift + ':' + oid, 'kind': 'shift',
                 'date': date, 'shift': shift, 'open_id': oid, 'status': status,
                 'delivery': receipts if receipts is not None else [
                     {'open_id': oid, 'ok': True, 'sender_app_id': 'cli_test', 'message_id': 'om_test'}], **extra}
        self.put('notification', value['id'], value, dirty=True)
        return value

    def read(self, current=NOW):
        return delivery.report(self.service, current)

    def day(self, current=NOW):
        return self.read(current)['shifts'][0]

    def complete(self):
        self.notice(); self.notice('ou_two'); self.notice(shift='夜')

    def test_all_current_app_receipts_and_publication_succeed(self):
        self.complete()
        result = self.read()
        self.assertEqual(result['date'], DATE)
        self.assertEqual([v['state'] for v in result['shifts']], ['success', 'success'])
        self.assertEqual([(v['expected'], v['sent'], v['failed'], v['pending']) for v in result['shifts']],
                         [(2, 2, 0, 0), (1, 1, 0, 0)])
        self.assertEqual([v['publish_time'] for v in result['shifts']], ['13:00', '20:00'])
        self.assertEqual(result['shifts'][0]['issues'], [])

    def test_partial_failure_lists_roster_name_and_scope_without_raw_error(self):
        self.notice()
        self.notice('ou_two', status='pending', receipts=[{'open_id': 'ou_two', 'ok': False,
            'failure_kind': 'bot_unavailable', 'message': 'Bearer SECRET ou_two'}], error='app_secret=SECRET ou_two')
        result = self.day()
        self.assertEqual((result['state'], result['sent'], result['failed'], result['pending']), ('partial', 1, 1, 0))
        self.assertEqual(result['issues'], [{'name': '乙', 'scope': 'B', 'status': 'failed', 'reason': '机器人对该人员不可用'}])
        self.assertNotIn('SECRET', json.dumps(result))
        self.assertNotIn('ou_', json.dumps(result))

    def test_later_success_drops_old_errors(self):
        self.notice(status='pending', receipts=[], error='old failure')
        self.assertEqual(self.day()['failed'], 1)
        self.notice(error='old failure'); self.notice('ou_two')
        self.assertEqual(self.day()['state'], 'success')
        self.assertEqual(self.day()['issues'], [])

    def test_no_notifications_are_pending_not_success(self):
        result = self.day()
        self.assertEqual((result['state'], result['expected'], result['pending']), ('pending', 2, 2))
        self.assertEqual([v['name'] for v in result['issues']], ['甲', '乙'])

    def test_missing_or_dirty_publication_never_succeeds(self):
        self.complete()
        with self.service.transaction() as conn:
            conn.execute("DELETE FROM documents WHERE kind='publication'")
        self.assertEqual(self.day()['state'], 'pending')
        self.put('publication', self.service._publication_key(DATE), {'date': DATE}, dirty=True)
        self.assertEqual(self.day()['state'], 'pending')
        self.assertEqual(self.day()['sent'], 2)

    def test_empty_roster_cannot_succeed(self):
        self.roster([])
        self.assertEqual((self.day()['state'], self.day()['expected']), ('unverified', 0))

    def test_missing_stale_and_old_version_rosters_cannot_succeed(self):
        self.complete()
        with self.service.transaction() as conn:
            conn.execute("DELETE FROM documents WHERE kind='local'")
        self.roster(date='2026-10-21', dated=False)
        self.assertEqual(self.day()['state'], 'unverified')
        self.roster(version=ROSTER_VERSION - 1)
        self.assertEqual(self.day()['state'], 'unverified')

    def test_generic_snapshot_fallback_and_dated_snapshot_precedence(self):
        self.complete()
        self.roster([], dated=False)
        self.assertEqual(self.day()['state'], 'success')
        with self.service.transaction() as conn:
            conn.execute("DELETE FROM documents WHERE kind='local' AND key=?", ('shift_roster:' + DATE,))
        self.roster(dated=False)
        self.assertEqual(self.day()['state'], 'success')

    def test_not_due_and_disabled_do_not_read_directory(self):
        self.complete()
        self.assertEqual([v['state'] for v in self.read(NOW.replace(hour=12))['shifts']], ['not_due', 'not_due'])
        self.cloud.current_app_id.assert_not_called()
        self.put('settings', 'main', {**core.DEFAULT_SETTINGS, 'enabled': False})
        self.assertEqual([v['state'] for v in self.read()['shifts']], ['disabled', 'disabled'])
        self.cloud.current_app_id.assert_not_called()

    def test_configured_times_and_exact_due_boundary(self):
        self.complete()
        self.put('settings', 'main', {**core.DEFAULT_SETTINGS, 'enabled': True, 'publish_time': '14:15'})
        self.assertEqual(self.day(NOW.replace(hour=14, minute=14))['state'], 'not_due')
        self.assertEqual(self.day(NOW.replace(hour=14, minute=15))['state'], 'success')

    def test_old_sent_without_receipts_is_unverified(self):
        self.notice(receipts=[]); self.notice('ou_two')
        self.assertEqual((self.day()['state'], self.day()['pending']), ('unverified', 1))

    def test_invalid_wrong_recipient_and_old_app_receipts_are_unverified(self):
        self.notice('ou_two')
        for change in ({'message_id': 'not-valid'}, {'sender_app_id': 'cli_old'},
                       {'open_id': 'ou_stranger'}, {'ok': False}, {'message_id': 'om_test\n'}):
            with self.subTest(change=change):
                receipt = {'open_id': 'ou_one', 'ok': True, 'sender_app_id': 'cli_test', 'message_id': 'om_test'}
                self.notice(receipts=[{**receipt, **change}])
                self.assertEqual(self.day()['state'], 'unverified')

    def test_uncertain_and_expired_notifications_are_not_sent(self):
        self.notice('ou_two')
        self.notice(status='pending', receipts=[{'open_id': 'ou_one', 'ok': False, 'failure_kind': 'uncertain'}])
        self.assertEqual(self.day()['state'], 'unverified')
        self.notice(status='expired')
        self.assertEqual((self.day()['state'], self.day()['failed']), ('partial', 1))

    def test_directory_unready_mismatched_app_and_exception_are_unverified(self):
        self.complete()
        for change in ({'app_id': 'cli_old'}, {'schema': 3}, {'people': None}):
            with self.subTest(change=change):
                with patch.dict(self.directory, change):
                    self.assertEqual(self.day()['state'], 'unverified')
        self.cloud.current_app_id.side_effect = RuntimeError('SECRET ou_one')
        self.assertEqual(self.day()['state'], 'unverified')
        self.assertNotIn('SECRET', json.dumps(self.read()))

    def test_unmatched_and_duplicate_names_stay_in_expected_count(self):
        self.complete()
        self.directory['people']['duplicate'] = {**self.directory['people']['one'], 'login_ids': ['ou_duplicate']}
        result = self.day()
        self.assertEqual((result['state'], result['expected'], result['sent'], result['pending']), ('unverified', 2, 1, 1))
        self.assertEqual(result['issues'][0]['name'], '甲')
        self.directory['people'].pop('two')
        self.assertEqual(self.day()['expected'], 2)

    def test_old_linked_openid_is_resolved_by_exact_roster_name(self):
        self.complete()
        self.roster([{'name': '甲', 'scope': 'A', 'shift': '白', 'open_id': 'ou_previousApp'}])
        self.assertEqual((self.day()['state'], self.day()['expected']), ('success', 1))

    def test_inactive_wrong_scope_and_shared_identity_are_unverified(self):
        self.complete()
        original = copy.deepcopy(self.directory['people'])
        for change in ({'inactive': True}, {'selectable': False}, {'scopes': ['B']}, {'login_ids': ['ou_two']}):
            with self.subTest(change=change):
                self.directory['people'] = copy.deepcopy(original)
                self.directory['people']['one'].update(change)
                result = self.day()
                self.assertEqual(result['state'], 'unverified')
                self.assertEqual(result['expected'], 2)
                self.assertEqual(result['issues'][0]['name'], '甲')

    def test_malformed_roster_and_empty_directory_cannot_succeed(self):
        self.complete()
        for people in ([{'name': '甲', 'scope': '', 'shift': '白'}], [None], {'not': 'a list'}):
            with self.subTest(people=people):
                self.roster(people)
                self.assertEqual(self.day()['state'], 'unverified')
        self.roster()
        self.directory['people'] = {}
        self.assertEqual((self.day()['state'], self.day()['pending']), ('unverified', 2))

    def test_pending_cannot_reuse_an_old_success_receipt(self):
        self.notice('ou_two')
        self.notice(status='pending', error='latest send failed')
        result = self.day()
        self.assertEqual((result['sent'], result['failed'], result['state']), (1, 1, 'partial'))

    def test_result_contract_and_unknown_failure_are_safe(self):
        self.notice('ou_two')
        self.notice(status='pending', error='Authorization: Bearer SECRET', receipts=[
            {'open_id': 'ou_one', 'ok': False, 'failure_kind': 'SECRET', 'message': 'ou_one SECRET'}])
        result = self.read()
        self.assertEqual(set(result), {'date', 'shifts'})
        for shift in result['shifts']:
            self.assertEqual(set(shift), {'shift', 'publish_time', 'state', 'label', 'expected', 'sent', 'failed', 'pending', 'issues'})
            for key in ('expected', 'sent', 'failed', 'pending'):
                self.assertIs(type(shift[key]), int)
            for issue in shift['issues']:
                self.assertEqual(set(issue), {'name', 'scope', 'status', 'reason'})
        self.assertNotIn('SECRET', json.dumps(result))
        self.assertNotIn('ou_one', json.dumps(result))

    def test_duplicate_text_slots_do_not_inflate_expected(self):
        row = {'name': '甲', 'scope': 'A', 'shift': '白', 'open_id': ''}
        self.roster([row, copy.deepcopy(row)])
        self.notice()
        self.assertEqual((self.day()['state'], self.day()['expected']), ('success', 1))

    def test_duplicate_notifications_are_unverified(self):
        value = self.notice(); self.notice('ou_two')
        self.put('notification', 'duplicate', {**value, 'id': 'duplicate'})
        self.assertEqual(self.day()['state'], 'unverified')

    def test_other_days_shifts_and_notification_kinds_are_isolated(self):
        self.notice(date='2026-10-21'); self.notice(shift='夜'); self.notice('ou_two', kind='publish')
        self.assertEqual((self.day()['sent'], self.day()['pending']), (0, 2))
        self.assertEqual(self.read()['shifts'][1]['state'], 'success')
        self.assertEqual(self.read(NOW + dt.timedelta(days=1))['date'], '2026-10-23')
        self.assertEqual(self.day(NOW + dt.timedelta(days=1))['state'], 'unverified')

    def test_beijing_date_and_naive_time(self):
        result = self.read(dt.datetime(2026, 10, 21, 17, tzinfo=dt.timezone.utc))
        self.assertEqual(result['date'], DATE)
        self.assertEqual(result['shifts'][0]['state'], 'not_due')
        self.assertEqual(self.read(NOW.replace(tzinfo=None)), self.read(NOW))
        with patch.object(core, 'now', return_value=NOW):
            self.assertEqual(delivery.report(self.service), self.read(NOW))

    def test_report_is_repeatable_readonly_and_does_not_send(self):
        self.complete()
        before = Path(self.service.db).read_bytes()
        rows_before = {kind: self.service._all(kind) for kind in ('local', 'notification', 'publication', 'settings')}
        connect = self.service._connect
        def readonly():
            conn = connect()
            conn.execute('PRAGMA query_only=ON')
            return conn
        with patch.object(self.service, '_connect', side_effect=readonly), \
                patch.object(self.service, '_put', side_effect=AssertionError('write forbidden')), \
                patch.object(self.service, 'transaction', side_effect=AssertionError('transaction forbidden')):
            self.assertEqual(self.read(), self.read())
        self.assertEqual(rows_before, {kind: self.service._all(kind) for kind in rows_before})
        self.assertEqual(before, Path(self.service.db).read_bytes())
        self.sender.assert_not_called()
        self.reader.assert_not_called()
        self.assertEqual({call[0] for call in self.cloud.mock_calls}, {'current_app_id'})


if __name__ == '__main__':
    unittest.main()

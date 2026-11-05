import copy
import datetime as dt
import hashlib
import json
import tempfile
import threading
import time
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from lan_bitable_template_portal import daily_inspection_reminders as reminders
from lan_bitable_template_portal.daily_inspection import CN
from lan_bitable_template_portal.daily_inspection_reconcile import reconcile_inspection_delivery
from lan_bitable_template_portal.state_store import LanPortalStateStore


def moment(day, hour, minute=0):
    return dt.datetime(2026, 11, day, hour, minute, tzinfo=CN)


def person(record, name, open_id, scope, shift):
    return {'record_id': record, 'name': name, 'employee_no': record[3:], 'open_id': open_id,
            'scope': scope, 'shift': shift, 'duty_kind': shift}


def snapshot(day, people):
    return {'date': day, 'app_id': 'cli_test', 'complete': True, 'people': copy.deepcopy(people), 'issues': []}


def receipt(open_id, identity):
    return {'open_id': open_id, 'ok': True, 'message_uuid': identity, 'message_id': 'om_fixture',
            'chat_id': 'oc_fixture', 'sender_app_id': 'cli_test'}


class ReminderRecoveryTests(unittest.TestCase):
    def setUp(self):
        folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.path = folder / 'state.sqlite3'
        self.store = LanPortalStateStore(self.path)
        self.white = person('recWhite', '甲', 'ou_White', 'D', '白')
        self.night = person('recNight', '乙', 'ou_Night', 'E', '夜')
        self.tomorrow = person('recTomorrow', '丙', 'ou_Tomorrow', 'D', '白')
        self.rosters = {'2026-11-03': snapshot('2026-11-03', [self.white, self.night]),
                        '2026-11-04': snapshot('2026-11-04', [self.tomorrow])}
        self.calls = []

    def test_future_copy_per_building_retries_without_repeating_staff(self):
        self.white_e=person('recWhiteE','乙','ou_WhiteE','E','白')
        copied=person('recCopy','李世龙','ou_Copy','H','白')
        failed=[False]
        def send(card,recipients,*,message_uuid):
            who=recipients[0]
            title=card['header']['title']['content']
            self.calls.append((who,message_uuid,title))
            if who=='ou_Copy' and title.startswith('E') and not failed[0]:
                failed[0]=True
                return False,'未发送',[{'open_id':who,'ok':False,'failure_kind':'not_sent'}]
            return True,'',[receipt(who,message_uuid)]
        def tick(when):
            roster=lambda day:{**snapshot(day,[self.white,self.white_e]),'observed_at':when.timestamp()}
            reminders.process_reminders(LanPortalStateStore(self.path),roster,lambda **_:'https://inspection.example',send,
                send_card=send,current=when,copy_recipient=lambda app:copied)
        self.seed(moment(3,9))
        tick(moment(3,9))
        tick(moment(3,9,30))
        tick(moment(3,9,36))
        tick(moment(3,9,42))
        self.assertEqual(sum(c[0]=='ou_White' for c in self.calls),1)
        self.assertEqual(sum(c[0]=='ou_WhiteE' for c in self.calls),1)
        copies=[c for c in self.calls if c[0]=='ou_Copy']
        self.assertEqual(len(copies),3)
        self.assertEqual(len({c[1] for c in copies}),2)
        self.assertTrue(self.store.get_document(reminders.NAMESPACE,'2026-11-03:白:1')['complete'])

    def seed(self, when):
        self.store.put_document(reminders.NAMESPACE, reminders.SCHEDULE_KEY,
                                {'version': 2, 'cursor': when.timestamp()})

    def send(self, card, recipients, *, message_uuid):
        open_id, = recipients
        self.calls.append((open_id, message_uuid, copy.deepcopy(card)))
        return True, '', [receipt(open_id, message_uuid)]

    def tick(self, when, *, send=None, roster=None, resolver=None, reconcile=None):
        store = LanPortalStateStore(self.path)
        def observed(day):
            value = copy.deepcopy((roster or (lambda key: self.rosters[key]))(day))
            value.setdefault('observed_at', when.timestamp())
            return value
        reminders.process_reminders(store, observed,
            lambda **_: 'https://inspection.example', send or self.send, send_card=send or self.send,
            current=when, resolve_recipient=resolver, reconcile_delivery=reconcile)
        return store

    @staticmethod
    def card_slot(card):
        return card['elements'][0]['text']['content']

    def test_white_shift_offline_backfills_from_original_shift_snapshot(self):
        self.seed(moment(3, 9, 20))
        self.tick(moment(3, 9, 25))  # Preserve the original white-shift list before the outage.
        self.rosters['2026-11-03']['people'][0]['open_id'] = 'ou_Reassigned'
        self.tick(moment(3, 20, 1))
        white = [row for row in self.calls if '白班' in self.card_slot(row[2])]
        night = [row for row in self.calls if '夜班' in self.card_slot(row[2])]
        self.assertEqual([row[0] for row in white], ['ou_White', 'ou_White'])
        self.assertEqual([row[0] for row in night], ['ou_Night'])
        self.assertTrue(all('2026-11-03' in self.card_slot(row[2]) for row in self.calls))
        self.tick(moment(3, 20, 7))
        self.assertEqual(len(self.calls), 3)

    def test_midnight_failure_and_restart_keep_previous_night_recipients(self):
        self.seed(moment(3, 19, 59))
        attempts = []
        def fail_once(card, recipients, *, message_uuid):
            open_id, = recipients
            attempts.append((open_id, message_uuid, copy.deepcopy(card)))
            if len(attempts) == 1:
                return False, '', [{'open_id': open_id, 'ok': False, 'failure_kind': 'not_sent'}]
            return True, '', [receipt(open_id, message_uuid)]
        self.tick(moment(3, 20), send=fail_once)
        self.tick(moment(4, 9, 10), send=fail_once)
        self.tick(moment(4, 9, 16), send=fail_once)
        self.assertEqual([row[0] for row in attempts], ['ou_Night', 'ou_Night', 'ou_Night'])
        self.assertEqual(attempts[0][1], attempts[1][1])
        self.assertNotEqual(attempts[1][1], attempts[2][1])
        self.assertTrue(all('2026-11-03' in self.card_slot(row[2]) and '夜班' in self.card_slot(row[2]) for row in attempts))

    def test_missing_old_roster_stays_pending_and_does_not_block_current_shift(self):
        self.seed(moment(3, 17, 59))
        self.tick(moment(4, 9, 10))
        old = self.store.get_document(reminders.NAMESPACE, '2026-11-03:夜:1')
        self.assertFalse(old.get('complete', False))
        self.assertIn('不可核验', old['last_error'])
        self.assertFalse(self.calls)
        self.tick(moment(4, 9, 30))
        self.assertEqual([row[0] for row in self.calls], ['ou_Tomorrow'])
        original = {**self.rosters['2026-11-03'], 'captured_at': moment(3, 20).timestamp(),
                    'observed_at': moment(3, 20).timestamp()}
        self.store.put_document(reminders.NAMESPACE, reminders.shift_roster_key('2026-11-03', '夜'), original)
        self.tick(moment(4, 9, 36))
        self.assertEqual([row[0] for row in self.calls].count('ou_Night'), 2)
        self.assertEqual([row[0] for row in self.calls].count('ou_Tomorrow'), 1)

    def test_midnight_does_not_treat_old_daytime_cache_as_night_roster(self):
        self.seed(moment(3, 19, 59))
        old = {**self.rosters['2026-11-03'], 'observed_at': moment(3, 14).timestamp()}
        self.tick(moment(4, 0), roster=lambda _day: old)
        self.assertFalse(self.calls, '读取时间属于白班的缓存不能用作夜班')
        for number in (1, 2):
            self.assertFalse(self.store.get_document(reminders.NAMESPACE, f'2026-11-03:夜:{number}').get('complete'))
        self.assertIsNone(self.store.get_document(reminders.NAMESPACE, reminders.shift_roster_key('2026-11-03', '夜')))
        valid = {**self.rosters['2026-11-03'], 'observed_at': moment(3, 20).timestamp()}
        reminders.remember_shift_roster(self.store, '2026-11-03', '夜', valid, moment(3, 20))
        self.tick(moment(4, 0, 6), roster=lambda _day: old)
        self.assertEqual([row[0] for row in self.calls], ['ou_Night', 'ou_Night'])

    def test_durable_success_receipt_survives_restart_and_account_change(self):
        self.seed(moment(3, 9, 29))
        attempts = []
        def lose_reply(card, recipients, *, message_uuid):
            open_id, = recipients
            attempts.append(open_id)
            data = receipt(open_id, message_uuid)
            route_key = hashlib.sha256(json.dumps(['open_id', open_id, message_uuid]).encode()).hexdigest()
            fingerprint = hashlib.sha256(json.dumps(['interactive', card], sort_keys=True, ensure_ascii=False).encode()).hexdigest()
            self.store.put_document('feishu_public_delivery', route_key, {'status': 'sent', 'app_id': 'cli_test',
                'receive_id': open_id, 'receive_type': 'open_id', 'message_uuid': message_uuid,
                'fingerprint': fingerprint, 'receipt': data})
            raise TimeoutError('模拟成功后回执丢失')
        self.tick(moment(3, 9, 30), send=lose_reply)
        def changed_account(record_id, _old, _app, _person):
            return 'ou_Reassigned' if record_id == 'recWhite' else 'ou_Night'
        self.tick(moment(3, 18, 1), resolver=changed_account)
        job = self.store.get_document(reminders.NAMESPACE, '2026-11-03:白:1')
        self.assertTrue(job['complete'])
        self.assertEqual(job['recipient_records']['recWhite'], 'ou_White')
        self.assertEqual(attempts, ['ou_White'])
        self.assertNotIn('ou_Reassigned', [row[0] for row in self.calls if '第 1 次' in self.card_slot(row[2])])

    def test_expired_uncertain_send_is_neither_repeated_nor_reported_success(self):
        self.seed(moment(3, 9, 29))
        attempts = []
        def uncertain(card, recipients, *, message_uuid):
            attempts.append((recipients[0], message_uuid))
            return False, '', [{'open_id': recipients[0], 'ok': False, 'failure_kind': 'uncertain'}]
        self.tick(moment(3, 9, 30), send=uncertain)
        self.tick(moment(3, 10, 31), send=uncertain)
        self.assertEqual(len(attempts), 1)
        job = self.store.get_document(reminders.NAMESPACE, '2026-11-03:白:1')
        self.assertFalse(job['complete'])
        self.assertEqual(job['inflight_records']['recWhite'], 'ou_White')

    def test_expired_uncertain_send_is_completed_only_after_history_confirms_it(self):
        self.seed(moment(3, 9, 29))
        attempts, checks = [], []
        def uncertain(card, recipients, *, message_uuid):
            open_id, = recipients
            attempts.append((open_id, message_uuid))
            key = hashlib.sha256(json.dumps(['open_id', open_id, message_uuid]).encode()).hexdigest()
            fingerprint = hashlib.sha256(json.dumps(['interactive', card], sort_keys=True, ensure_ascii=False).encode()).hexdigest()
            self.store.put_document('feishu_public_delivery', key, {
                'status': 'posting', 'channel': 'agent', 'app_id': 'cli_test',
                'receive_id': open_id, 'receive_type': 'open_id', 'message_uuid': message_uuid,
                'target': ['open_id', open_id], 'fingerprint': fingerprint,
                'attempted_at': moment(3, 9, 30).timestamp()})
            return False, '', [{'open_id': open_id, 'ok': False, 'failure_kind': 'uncertain'}]
        def reconcile(open_id, message_uuid, target, route):
            checks.append((open_id, message_uuid, target, route))
            return {**receipt(open_id, message_uuid), 'reconciled_from_history': True}
        self.tick(moment(3, 9, 30), send=uncertain)
        self.tick(moment(3, 10, 31), send=uncertain, reconcile=reconcile)
        job = self.store.get_document(reminders.NAMESPACE, '2026-11-03:白:1')
        self.assertEqual(len(attempts), 1)
        self.assertEqual((checks[0][0], checks[0][1]), attempts[0])
        self.assertTrue(job['complete'])
        self.assertTrue(job['receipts']['recWhite']['reconciled_from_history'])

    def test_missing_original_message_or_record_id_cannot_be_resent(self):
        self.seed(moment(3, 9, 29))
        self.store.put_document(reminders.NAMESPACE, '2026-11-03:白:1', {
            'inflight_records': {'recWhite': 'ou_White'},
            'inflight_since': {'recWhite': moment(3, 9, 29).timestamp()}})
        self.tick(moment(3, 9, 30))
        self.assertFalse(self.calls)
        self.assertFalse(self.store.get_document(reminders.NAMESPACE, '2026-11-03:白:1')['complete'])
        self.rosters['2026-11-03']['people'][0].pop('record_id')
        self.tick(moment(3, 12))
        self.assertFalse(self.calls)
        self.assertFalse(self.store.get_document(reminders.NAMESPACE, '2026-11-03:白:2')['complete'])

    def test_one_bad_recipient_or_bad_receipt_does_not_hide_other_deliveries(self):
        self.seed(moment(3, 9, 29))
        self.rosters['2026-11-03']['people'].append(person('recWhiteE', '丁', 'ou_WhiteE', 'E', '白'))
        def resolve(record_id, open_id, app_id, row):
            if record_id == 'recWhite':
                raise ValueError('身份暂不可核验')
            return open_id
        def incomplete(card, recipients, *, message_uuid):
            open_id, = recipients
            self.calls.append((open_id, message_uuid, copy.deepcopy(card)))
            return True, '', [{'open_id': open_id, 'ok': True, 'message_uuid': message_uuid}]
        self.tick(moment(3, 9, 30), send=incomplete, resolver=resolve)
        job = self.store.get_document(reminders.NAMESPACE, '2026-11-03:白:1')
        self.assertFalse(job['complete'])
        self.assertFalse(job.get('sent'))
        self.assertEqual([row[0] for row in self.calls], ['ou_WhiteE'])

    def test_concurrent_workers_claim_one_slot_once(self):
        self.seed(moment(3, 9, 29))
        entered, release = threading.Event(), threading.Event()
        def slow(card, recipients, *, message_uuid):
            self.calls.append((recipients[0], message_uuid, card))
            entered.set()
            self.assertTrue(release.wait(5))
            return True, '', [receipt(recipients[0], message_uuid)]
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self.tick, moment(3, 9, 30), send=slow)
            self.assertTrue(entered.wait(5))
            second = pool.submit(self.tick, moment(3, 9, 30), send=slow)
            second.result(timeout=5)
            release.set()
            first.result(timeout=5)
        self.assertEqual(len(self.calls), 1)

    def test_schedule_cursor_and_slots_roll_back_together(self):
        self.seed(moment(3, 9, 20))
        with patch.object(reminders, '_put', side_effect=RuntimeError('模拟事务中断')):
            with self.assertRaises(RuntimeError):
                reminders.schedule_missed_slots(self.store, moment(3, 12, 1))
        self.assertIsNone(self.store.get_document(reminders.NAMESPACE, '2026-11-03:白:1'))
        self.assertEqual(self.store.get_document(reminders.NAMESPACE, reminders.SCHEDULE_KEY)['cursor'], moment(3, 9, 20).timestamp())
        reminders.schedule_missed_slots(self.store, moment(3, 12, 1))
        self.assertIsNotNone(self.store.get_document(reminders.NAMESPACE, '2026-11-03:白:1'))
        self.assertIsNotNone(self.store.get_document(reminders.NAMESPACE, '2026-11-03:白:2'))

    def test_unconfirmed_results_from_old_versions_do_not_reset_the_original_id(self):
        self.seed(moment(3, 9, 29))
        self.store.put_document(reminders.NAMESPACE, '2026-11-03:白:1', {
            'targets': {'ou_White': {'kind': 'card', 'scope': 'D',
                'card': reminders.reminder_card('D', '2026-11-03', '白', 1, 'https://inspection.example/daily-inspection')}},
            'inflight_records': {'recWhite': 'ou_White'}})
        self.tick(moment(3, 9, 30))
        job = self.store.get_document(reminders.NAMESPACE, '2026-11-03:白:1')
        self.assertFalse(job['complete'])
        self.assertEqual(job['inflight_records']['recWhite'], 'ou_White')
        self.assertFalse(self.calls)

    def test_exact_personnel_record_is_required_for_account_rebinding(self):
        runtime = SimpleNamespace(state_store=self.store)
        original = {'name': '甲', 'employee_no': '101'}
        directory = {'schema': 4, 'app_id': 'cli_test', 'people': {'recWhite': {
            'id': 'recWhite', 'name': '甲', 'employee_no': '101', 'selectable': True,
            'inactive': False, 'login_ids': ['ou_New']}}}
        with (patch('upload_event_module.services.robot_webhook.current_app_credentials', return_value=('cli_test', 'secret')),
              patch('lan_bitable_template_portal.feishu_delivery.current_recipient_open_id', side_effect=lambda _r, oid, _a, **_k: oid)):
            self.assertEqual(reminders.current_roster_recipient(runtime, lambda: directory, 'recWhite', 'ou_Old', 'cli_test', original), 'ou_New')
            directory['people']['recWhite']['name'] = '他人'
            with self.assertRaises(ValueError):
                reminders.current_roster_recipient(runtime, lambda: directory, 'recWhite', 'ou_Old', 'cli_test', original)


class ReminderHistoryReconcileTests(unittest.TestCase):
    def setUp(self):
        folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.store = LanPortalStateStore(folder / 'state.sqlite3')
        self.app, self.open_id = 'cli_test', 'ou_White'
        self.identity = str(uuid.uuid4())
        self.card = reminders.reminder_card('D', '2026-11-03', '白', 1, 'https://inspection.example/daily-inspection')
        self.target = {'kind': 'card', 'scope': 'D', 'card': self.card}
        self.attempted = time.time() - 4000
        self.route = {'status': 'posting', 'channel': 'agent', 'app_id': self.app,
                      'target': ['open_id', self.open_id], 'attempted_at': self.attempted}
        self.store.put_document('feishu_public_delivery', 'prior', {'status': 'sent', 'app_id': self.app,
            'receive_type': 'open_id', 'receive_id': self.open_id,
            'message_uuid': 'previous-id', 'receipt': {'sender_app_id': self.app, 'message_uuid': 'previous-id',
                'message_id': 'om_previous', 'chat_id': 'oc_fixture'}})

    def invoke(self, messages):
        result = {'code': 0, 'data': {'items': messages, 'has_more': False}}
        with (patch('upload_event_module.services.robot_webhook.current_app_credentials', return_value=(self.app, 'secret')),
              patch('upload_event_module.services.robot_webhook._get_tenant_access_token', return_value=('token', '')),
              patch('upload_event_module.services.robot_webhook._request_json', return_value=result)):
            return reconcile_inspection_delivery(self.store, self.open_id, self.identity, self.target, self.route)

    def test_only_exact_same_bot_card_in_verified_private_chat_confirms_delivery(self):
        message = {'message_id': 'om_exact', 'chat_id': 'oc_fixture', 'msg_type': 'interactive',
                   'deleted': False, 'sender': {'id': self.app}, 'create_time': str(int(self.attempted * 1000)),
                   'body': {'content': json.dumps(self.card, ensure_ascii=False)}}
        self.assertEqual(self.invoke([message])['message_id'], 'om_exact')
        for change in ({'sender': {'id': 'cli_other'}}, {'chat_id': 'oc_other'}, {'deleted': True},
                       {'body': {'content': '{}'}}):
            self.assertIsNone(self.invoke([{**message, **change}]))
        self.assertIsNone(self.invoke([]), '查不到消息不是未送达的证明')


if __name__ == '__main__':
    unittest.main()

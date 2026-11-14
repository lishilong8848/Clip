import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from upload_event_module.services import robot_webhook

from lan_bitable_template_portal import repair_progress_notifications as module
from lan_bitable_template_portal.portal_service import BUILDING_CHAT_ID_MAP
from lan_bitable_template_portal.repair_progress_notifications import (
    NOTICE_TITLE, REMINDER_INTERVAL_SECONDS, REMINDER_TITLE, RETRY_BACKOFF_SECONDS, process,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore

BASE = 'http://10.0.0.5:18766'
CHATS = {'D': 'oc_d', 'E': 'oc_e'}


def record(workflow='维修中', codes=('D',), rid='recA', title='冷却泵检修'):
    return {
        'record_id': rid, 'workflow': workflow,
        'state': 'in_progress' if workflow == '维修中' else 'not_started',
        'building_codes': list(codes), 'repair_title': title,
        'fault_time': '2026-11-12 08:10', 'repair_start_time': '2026-11-12 09:00',
    }


class RepairProgressNotificationTests(unittest.TestCase):
    def setUp(self):
        folder = self.enterContext(tempfile.TemporaryDirectory())
        self.store = LanPortalStateStore(Path(folder) / 'state.sqlite3')
        self.sends = []

        def send(card, chat_id, message_uuid):
            self.sends.append({'card': card, 'chat_id': chat_id, 'uuid': message_uuid})
            return True, ''

        self.send = send

    def scan(self, records, now):
        return process(self.store, records=records, base_url=BASE, send=self.send,
                       chat_ids=CHATS, now=now)

    def header(self, card):
        return card['header']['title']['content']

    def body(self, card):
        return card['elements'][0]['text']['content']

    def button_url(self, card):
        return card['elements'][1]['actions'][0]['url']

    def test_first_scan_only_baselines_and_never_replays_history(self):
        result = self.scan([record()], 1000.0)
        self.assertEqual((result['scanned'], result['noticed'], result['reminded']), (1, 0, 0))
        self.assertEqual(self.sends, [], '首轮只建基线，不把存量维修中记录当新通知补发')

    def test_transition_into_in_progress_notifies_only_own_buildings_once(self):
        self.scan([record(workflow='未开始', codes=('D', 'E', 'H'))], 1000.0)
        result = self.scan([record(workflow='维修中', codes=('D', 'E', 'H'))], 2000.0)
        self.assertEqual(result['noticed'], 2, 'H 楼没有配置运维群，只发 D 与 E')
        self.assertEqual([item['chat_id'] for item in self.sends], ['oc_d', 'oc_e'])
        card = self.sends[0]['card']
        self.assertEqual(self.header(card), NOTICE_TITLE)
        self.assertIn('楼栋：**D楼**', self.body(card))
        self.assertIn('检修名称：**冷却泵检修**', self.body(card))
        self.assertIn('故障发生时间：2026-11-12 08:10', self.body(card))
        self.assertIn('维修开始时间：2026-11-12 09:00', self.body(card))
        self.assertEqual(self.button_url(card), BASE + '/repair-management?scope=D&record_id=recA')
        self.assertIn('楼栋：**E楼**', self.body(self.sends[1]['card']))

        self.sends.clear()
        self.scan([record(workflow='维修中', codes=('D', 'E', 'H'))], 3000.0)
        self.assertEqual(self.sends, [], '同一跃迁不重复通知')

    def test_reminder_repeats_only_after_interval_and_uses_distinct_uuids(self):
        self.assertEqual(REMINDER_INTERVAL_SECONDS, 24 * 60 * 60)
        self.scan([record(workflow='未开始')], 1000.0)
        self.scan([record()], 2000.0)
        self.sends.clear()
        self.scan([record()], 2000.0 + REMINDER_INTERVAL_SECONDS - 1)
        self.assertEqual(self.sends, [], '间隔内不提醒')
        self.scan([record()], 2000.0 + REMINDER_INTERVAL_SECONDS)
        self.assertEqual(len(self.sends), 1)
        card = self.sends[0]['card']
        self.assertEqual(self.header(card), REMINDER_TITLE)
        self.assertIn('**D楼 · 维修中 1 项**', self.body(card))
        self.assertIn('**冷却泵检修**', card['elements'][1]['text']['content'])
        self.assertIn('维修开始时间：2026-11-12 09:00', card['elements'][1]['text']['content'])
        self.assertEqual(card['elements'][1]['extra']['url'],
                         BASE + '/repair-management?scope=D&record_id=recA&workspace_tab=followups')
        first_uuid = self.sends[0]['uuid']

        self.sends.clear()
        self.scan([record()], 2000.0 + REMINDER_INTERVAL_SECONDS + 10)
        self.assertEqual(self.sends, [], '同一轮提醒不重发')
        self.scan([record()], 2000.0 + 2 * REMINDER_INTERVAL_SECONDS)
        self.assertEqual(len(self.sends), 1, '下一个间隔再提醒一次')
        self.assertNotEqual(self.sends[0]['uuid'], first_uuid, '每轮提醒使用不同幂等键')

    def test_one_digest_per_building_contains_all_valid_rows_and_caps_preview(self):
        rows = [record(rid=f'rec{i:02}', title=f'检修{i:02}') for i in range(10)]
        rows += [record(rid='recE', codes=('E',)), record(rid='recEmpty', title='未命名维修项目')]
        self.scan(rows, 1000.0)
        result = self.scan(rows + [rows[0]], 1000.0 + REMINDER_INTERVAL_SECONDS)
        self.assertEqual((result['noticed'], result['reminded']), (0, 2))
        by_chat = {item['chat_id']: item['card'] for item in self.sends}
        card = by_chat['oc_d']
        self.assertIn('维修中 10 项', self.body(card))
        self.assertEqual(sum('extra' in element for element in card['elements']), 8)
        self.assertEqual(card['elements'][-2]['text']['content'], '另有 2 项维修中')
        self.assertEqual(card['elements'][-1]['actions'][0]['url'],
                         BASE + '/repair-management?scope=D&state=in_progress')
        self.assertIn('维修中 1 项', self.body(by_chat['oc_e']))
        self.scan(rows + [record(rid='recLate')], 1010.0 + REMINDER_INTERVAL_SECONDS)
        self.assertEqual([self.header(item['card']) for item in self.sends],
                         [REMINDER_TITLE, REMINDER_TITLE, NOTICE_TITLE])

    def test_digest_retry_after_restart_uses_original_payload_and_isolated_group_state(self):
        rows = [record(), record(rid='recE', codes=('E',))]
        self.scan(rows, 1000.0)
        due = 1000.0 + REMINDER_INTERVAL_SECONDS
        accepted = {}
        attempts = []

        def lose_receipt(card, chat_id, message_uuid):
            attempts.append((card, chat_id, message_uuid))
            first = message_uuid not in accepted
            prior = accepted.setdefault(message_uuid, card)
            self.assertEqual(card, prior)
            return (False, '回执丢失') if chat_id == 'oc_d' and first else (True, '')

        self.send = lose_receipt
        self.assertEqual(self.scan(rows, due)['reminded'], 1)
        self.store = LanPortalStateStore(self.store.db_path)
        updated = [dict(rows[0], repair_title='修改后的项目名'), rows[1]]
        self.scan(updated, due + RETRY_BACKOFF_SECONDS - 1)
        self.assertEqual(len(attempts), 2)
        self.assertEqual(self.scan(updated, due + RETRY_BACKOFF_SECONDS)['reminded'], 1)
        self.scan(updated, due + RETRY_BACKOFF_SECONDS + 60)
        self.assertEqual([item[1] for item in attempts], ['oc_d', 'oc_e', 'oc_d'])
        self.assertEqual(attempts[0], attempts[2])
        self.assertEqual(len(accepted), 2)

    def test_legacy_reminder_on_same_day_suppresses_digest_even_after_completion(self):
        rows = [record(), record(rid='recOld'), record(rid='recE', codes=('E',))]
        self.scan(rows, 1000.0)
        due = 1000.0 + REMINDER_INTERVAL_SECONDS
        key = module.PRUNE_PREFIX + 'recOld'
        state = self.store.get_document(module.NAMESPACE, key)
        state['reminder']['D'] = {'count': 1, 'at': due - 60}
        self.store.put_document(module.NAMESPACE, key, state)
        rows[1]['workflow'] = '维修完成'
        self.scan(rows, due)
        self.assertEqual([item['chat_id'] for item in self.sends], ['oc_e'])
        self.scan(rows, due + REMINDER_INTERVAL_SECONDS)
        self.assertEqual([item['chat_id'] for item in self.sends], ['oc_e', 'oc_d', 'oc_e'])

    def test_incomplete_scan_defers_digest_until_full_list_available(self):
        rows = [record(), record(rid='recB')]
        self.scan(rows, 1000.0)
        due = 1000.0 + REMINDER_INTERVAL_SECONDS
        process(self.store, records=rows[:1], base_url=BASE, send=self.send,
                chat_ids=CHATS, now=due, complete=False)
        self.assertFalse(self.sends)
        self.scan(rows, due + 60)
        self.assertIn('维修中 2 项', self.body(self.sends[0]['card']))

    def test_beijing_midnight_does_not_trigger_an_extra_digest(self):
        due = datetime(2026, 11, 14, 23, 59, tzinfo=module.BEIJING).timestamp()
        self.scan([record()], due - REMINDER_INTERVAL_SECONDS)
        self.assertEqual(self.scan([record()], due)['reminded'], 1)
        self.assertEqual(self.scan([record()], due + 120)['reminded'], 0)
        self.assertEqual(self.scan([record()], due + REMINDER_INTERVAL_SECONDS)['reminded'], 1)
        self.assertEqual(len({item['uuid'] for item in self.sends}), 2)

    def test_restart_after_midnight_recovers_pending_uuid_before_allocating_another(self):
        due = datetime(2026, 11, 14, 23, 59, 30, tzinfo=module.BEIJING).timestamp()
        rows = [record()]
        self.scan(rows, due - REMINDER_INTERVAL_SECONDS)
        attempts = []

        def lost_receipt(card, chat_id, message_uuid):
            attempts.append((card, chat_id, message_uuid))
            return len(attempts) > 1, '模拟超时'

        self.send = lost_receipt
        self.assertEqual(self.scan(rows, due)['reminded'], 0)
        self.store = LanPortalStateStore(self.store.db_path)
        self.assertEqual(self.scan(rows, due + 45)['reminded'], 0)
        self.assertEqual(len(attempts), 1)
        self.assertEqual(self.scan(rows, due + RETRY_BACKOFF_SECONDS)['reminded'], 1)
        self.assertEqual(attempts[0], attempts[1])
        self.assertEqual(self.scan(rows, due + REMINDER_INTERVAL_SECONDS + 60)['reminded'], 0)
        self.assertEqual(self.scan(rows, due + RETRY_BACKOFF_SECONDS + REMINDER_INTERVAL_SECONDS)['reminded'], 1)

    def test_overlapping_scan_cannot_claim_a_second_digest(self):
        rows = [record()]
        self.scan(rows, 1000.0)
        due = 1000.0 + REMINDER_INTERVAL_SECONDS
        store = LanPortalStateStore(self.store.db_path)
        sent = self.send

        def overlap(card, chat_id, message_uuid):
            result = process(store, records=rows, base_url=BASE, send=sent, chat_ids=CHATS,
                             now=due + RETRY_BACKOFF_SECONDS)
            self.assertEqual(result['reminded'], 0)
            return sent(card, chat_id, message_uuid)

        self.send = overlap
        self.assertEqual(self.scan(rows, due)['reminded'], 1)
        self.assertEqual(len(self.sends), 1)

    def test_success_after_midnight_reserves_the_date_of_delivery(self):
        due = datetime(2026, 11, 14, 23, 59, 30, tzinfo=module.BEIJING).timestamp()
        self.scan([record()], due - REMINDER_INTERVAL_SECONDS)
        current_time = due
        sent = self.send

        def deliver_after_midnight(card, chat_id, message_uuid):
            nonlocal current_time
            current_time += 45
            return sent(card, chat_id, message_uuid)

        with patch.object(module.time, 'time', side_effect=lambda: current_time):
            result = process(self.store, records=[record()], base_url=BASE,
                             send=deliver_after_midnight, chat_ids=CHATS)
        self.assertEqual(result['reminded'], 1)
        marker = self.store.get_document(module.NAMESPACE, 'digest:2026-11-15:D')
        self.assertEqual(marker['sent_at'], current_time)
        self.assertEqual(self.scan([record()], due + 900)['reminded'], 0)

    def test_expired_midnight_recovery_reserves_only_current_day(self):
        due = datetime(2026, 11, 14, 23, 59, 30, tzinfo=module.BEIJING).timestamp()
        rows = [record()]
        self.scan(rows, due - REMINDER_INTERVAL_SECONDS)
        send = self.send
        self.send = lambda *args: (False, '模拟结果未知')
        self.assertEqual(self.scan(rows, due)['reminded'], 0)
        self.send = send
        self.assertEqual(self.scan(rows, due + REMINDER_INTERVAL_SECONDS)['reminded'], 0)
        self.assertEqual(self.scan(rows, due + 2 * REMINDER_INTERVAL_SECONDS)['reminded'], 1)

    def test_waits_for_real_name_and_time_then_formats_epoch_milliseconds(self):
        self.scan([], 1000.0)
        row = dict(record(title='未命名维修项目'), repair_start_time='1794552360000')
        self.assertEqual(self.scan([row], 2000.0)['noticed'], 0)
        row.update(repair_title='1794552360000', notification_title='')
        self.assertEqual(self.scan([row], 2500.0)['noticed'], 0)
        row.update(repair_title='冷却泵检修', repair_start_time='待填写')
        row.pop('notification_title')
        self.assertEqual(self.scan([row], 3000.0)['noticed'], 0)
        row.update(repair_start_time='1794552360000')
        result = self.scan([row], 3000.0 + REMINDER_INTERVAL_SECONDS)
        self.assertEqual((result['noticed'], result['reminded']), (1, 0))
        body = self.body(self.sends[0]['card'])
        self.assertIn('检修名称：**冷却泵检修**', body)
        self.assertIn('维修开始时间：2026-11-13 14:46', body)
        self.assertNotIn('1794552360000', body)

    def test_completed_then_reopened_re_arms_the_notice(self):
        self.scan([record(workflow='未开始')], 1000.0)
        self.scan([record()], 2000.0)
        self.sends.clear()
        self.scan([record(workflow='维修完成')], 3000.0)
        self.assertEqual(self.sends, [])
        self.scan([record()], 4000.0)
        self.assertEqual([self.header(item['card']) for item in self.sends], [NOTICE_TITLE],
                         '重新进入维修中应再次通知')

    def test_failed_send_is_retried_with_the_same_uuid_after_backoff(self):
        def failing(card, chat_id, message_uuid):
            self.sends.append({'card': card, 'chat_id': chat_id, 'uuid': message_uuid})
            return False, '飞书返回限流'

        self.scan([record(workflow='未开始')], 1000.0)
        result = process(self.store, records=[record()], base_url=BASE, send=failing,
                         chat_ids=CHATS, now=2000.0)
        self.assertEqual(result['noticed'], 0)
        self.assertTrue(result['errors'])
        first = self.sends[0]['uuid']
        self.sends.clear()
        process(self.store, records=[record()], base_url=BASE, send=self.send,
                chat_ids=CHATS, now=2100.0)
        self.assertEqual(self.sends, [], '退避期内不重复轰炸失败的群')
        process(self.store, records=[record()], base_url=BASE, send=self.send,
                chat_ids=CHATS, now=2000.0 + RETRY_BACKOFF_SECONDS)
        self.assertEqual(len(self.sends), 1)
        self.assertEqual(self.sends[0]['uuid'], first, '重试复用同一幂等键，由飞书侧去重')

    def test_missing_base_url_defers_without_marking_sent(self):
        self.scan([record(workflow='未开始')], 1000.0)
        result = process(self.store, records=[record()], base_url='', send=self.send,
                         chat_ids=CHATS, now=2000.0)
        self.assertEqual(self.sends, [])
        self.assertTrue(result['errors'])
        self.scan([record()], 2100.0)
        self.assertEqual(len(self.sends), 1, '地址恢复后补发')

    def test_record_absent_from_a_complete_scan_re_arms_the_notice(self):
        self.scan([record(workflow='未开始')], 1000.0)
        self.scan([record()], 2000.0)
        self.sends.clear()
        self.scan([], 3000.0)
        self.scan([record()], 4000.0)
        self.assertEqual([self.header(item['card']) for item in self.sends], [NOTICE_TITLE],
                         '离开维修中后再进入应重新通知')

    def test_incomplete_scan_does_not_re_arm_absent_records(self):
        self.scan([record(workflow='未开始')], 1000.0)
        self.scan([record()], 2000.0)
        self.sends.clear()
        process(self.store, records=[], base_url=BASE, send=self.send,
                chat_ids=CHATS, now=3000.0, complete=False)
        self.scan([record()], 4000.0)
        self.assertEqual(self.sends, [], '扫描不完整时不因缺席而重报')

    def test_record_first_seen_in_progress_after_bootstrap_still_notifies(self):
        self.scan([], 500.0)
        result = self.scan([record()], 1000.0)
        self.assertEqual(result['noticed'], 1, '基线之后首见即维修中，说明是在监视期内进入的')
        self.assertEqual(self.header(self.sends[0]['card']), NOTICE_TITLE)

    def test_incomplete_first_scan_does_not_seal_the_baseline(self):
        process(self.store, records=[record()], base_url=BASE, send=self.send,
                chat_ids=CHATS, now=1000.0, complete=False)
        self.sends.clear()
        result = self.scan([record()], 2000.0)
        self.assertEqual(result['noticed'], 0, '首轮不完整时继续按基线处理，不补发存量')
        self.scan([record()], 3000.0)
        result = self.scan([record(rid='recNew')], 4000.0)
        self.assertEqual(result['noticed'], 1, '基线封存后新记录照常通知')

    def test_reassigned_building_gets_its_own_notice_without_renotifying_the_old_one(self):
        self.scan([record(workflow='未开始', codes=('D',))], 1000.0)
        self.scan([record(codes=('D',))], 2000.0)
        self.assertEqual([item['chat_id'] for item in self.sends], ['oc_d'])
        self.sends.clear()
        self.scan([record(codes=('D', 'E'))], 3000.0)
        self.assertEqual([item['chat_id'] for item in self.sends], ['oc_e'], '改派新增楼栋只通知新群')
        self.sends.clear()
        self.scan([record(codes=('D', 'E'))], 4000.0)
        self.assertEqual(self.sends, [], '原楼栋不重复通知')

    def test_run_pages_status_and_routes_through_the_real_building_group_map(self):
        rows = [record(workflow='未开始', rid='recA'), record(workflow='未开始', rid='recB')]
        calls = []

        class FakeService:
            def get_repair_management_status(self, *, scope='ALL', state='all', limit=100, offset=0, **kw):
                calls.append(offset)
                return {'records': rows[offset:offset + 1], 'has_more': offset == 0, 'total': len(rows)}

            def _critical_guard_public_base_url(self):
                return BASE

        def fake_send(card, chat_id, message_uuid=''):
            self.sends.append({'card': card, 'chat_id': chat_id, 'uuid': message_uuid})
            return True, ''

        with patch.object(robot_webhook, 'send_interactive_to_chat_id', fake_send):
            module.run(self.store, FakeService(), now=1000.0)
            self.assertEqual(calls, [0, 1], '应分页读完维修中清单')
            self.assertEqual(self.sends, [])
            for row in rows:
                row['workflow'] = '维修中'
            module.run(self.store, FakeService(), now=2000.0)
        self.assertEqual([item['chat_id'] for item in self.sends],
                         [BUILDING_CHAT_ID_MAP['D'], BUILDING_CHAT_ID_MAP['D']],
                         '应走真实楼栋群映射')
        self.assertTrue(all(BASE + '/repair-management?scope=D&record_id=' in item['card']
                            ['elements'][1]['actions'][0]['url'] for item in self.sends))


if __name__ == '__main__':
    unittest.main()

import datetime as dt
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from bin.lan_bitable_template_portal.risk_inspection import RiskError, RiskInspections
from bin.lan_bitable_template_portal.tool_management import ToolManagement
from bin.lan_bitable_template_portal.tool_management_lifecycle import (
    AUDIT_LIMIT, HISTORY_LIMIT, building_reminder_uuid,
)
from bin.lan_bitable_template_portal.tool_self_check import TZ, dumps
from bin.test_tool_self_check import actor


NOW = dt.datetime(2026, 11, 15, 9, tzinfo=TZ)


class ToolManagementLifecycleTest(TestCase):
    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.inspection = RiskInspections(Path(tmp.name) / 'tools.db', lambda _: {})
        self.service = ToolManagement(self.inspection, None)
        self.a = actor() | {'name': 'A 楼长'}
        self.b = actor('B', 'leader_b') | {'name': 'B 楼长'}
        with self.service.connect(True) as db:
            self.service.set_state(db, 'leaders', dumps({'A': [self.a['open_id']], 'B': [self.b['open_id']]}))
            self.service.set_state(db, 'inventory_at', 'legacy')
        clock = patch('bin.lan_bitable_template_portal.tool_management.clock', return_value=NOW)
        clock.start()
        self.addCleanup(clock.stop)

    def asset(self, name, scope='A', **fields):
        asset = {'id': name, 'name': name, 'scope': scope, 'location': 'fixture', 'model': 'fixture',
                 'quantity': 1, 'self_cycle': '每月', 'external_cycle': '', **fields}
        with self.service.connect(True) as db:
            db.execute('INSERT OR REPLACE INTO tool_inventory(id,source,active) VALUES(?,?,1)', (name, dumps(asset)))
        return asset

    def order(self, asset, identity, month, due, *, kind='self', workspace=False, **fields):
        with self.service.connect(True) as db:
            self.service.insert_work_order(db, identity, month, asset, kind, due)
            for field, value in fields.items():
                db.execute(f'UPDATE tool_work_orders SET {field}=? WHERE id=?', (value, identity))
            if workspace:
                db.execute('''INSERT INTO tool_workspace_items(session_id,asset_id,position,asset,order_id,
                    template_id,template_version,template_key,previous_template,draft)
                    VALUES(?,?,0,?,?,'','','','','{}')''', ('batch-' + identity, asset['id'], dumps(asset), identity))

    def test_carryover_uses_original_order_even_after_cycle_changes(self):
        asset = self.asset('recCarryover', self_cycle='', external_cycle='一年')
        self.order(asset, 'original', '2026-10', '2026-10-01')
        self.assertEqual([row['id'] for row in self.service.list(self.a)['carryovers']], ['original'])
        self.assertFalse(self.service.list(self.b)['carryovers'])
        self.assertFalse(self.service.planned_order_candidates(self.a)['items'])
        with self.assertRaises(RiskError):
            self.service.create_order(self.a, {'asset_id': asset['id'], 'kind': 'external'})
        with self.service.connect() as db:
            self.assertEqual(self.service._workspace_asset_state(db, self.a, asset, 'external')['mode'], 'direct')
        self.service.schedule(NOW)
        claimed = self.service.action(self.a, 'original', 'claim', {'revision': 0})
        self.assertEqual((claimed['id'], claimed['month'], claimed['kind']), ('original', '2026-10', 'self'))

    def test_restore_only_selected_draft_and_retry_same_revision(self):
        first = self.asset('recFirst')
        second = self.asset('recSecond')
        self.order(first, 'first', '2026-11', '2026-11-01',
                   owner=self.a['id'], owner_name=self.a['name'], draft=dumps({'reason': '保留草稿'}))
        self.order(second, 'second', '2026-11', '2026-11-01')
        self.service.cancel_orders(self.a, {'month': '2026-11', 'orders': [
            {'id': 'first', 'revision': 0}, {'id': 'second', 'revision': 0},
        ]})
        with self.assertRaises(RiskError):
            self.service.restore_order(self.b, 'first', {'revision': 1})
        restored = self.service.restore_order(self.a, 'first', {'revision': 1})
        self.assertEqual(restored, self.service.restore_order(self.a, 'first', {'revision': 1}))
        self.assertEqual(self.service.detail(self.a, 'first')['draft']['reason'], '保留草稿')
        self.assertEqual(self.service.detail(self.a, 'second')['status'], 'cancelled')

    def test_history_checks_each_order_before_cap_and_exposes_only_safe_fields(self):
        asset = self.asset('scope fixture')
        other = dict(asset, scope='B', location='hidden location')
        with self.service.connect(True) as db:
            for i in range(HISTORY_LIMIT + 1):
                month = f'{2025 + i // 12}-{i % 12 + 1:02}'
                self.service.insert_work_order(db, f'hidden-{i}', month, other, 'self', month + '-20')
            self.service.insert_work_order(db, 'visible', '2024-03', asset, 'self', '2024-03-31')
            db.execute('''UPDATE tool_work_orders SET submitted_at=?,reason=?,outcome='normal',owner='internal-id',
                owner_name=?,records=?,draft=?,report=? WHERE id='visible' ''', (
                '2024-03-30T09:00:00+08:00', 'visible reason', 'visible owner',
                dumps([{'inspector': 'visible inspector', 'notes': 'visible note', 'signature': 'PRIVATE_SIGNATURE'}]),
                '{"draft":"PRIVATE_DRAFT"}', b'PRIVATE_ATTACHMENT'))
            for i in range(AUDIT_LIMIT + 2):
                self.service.audit(db, 'visible', self.a, 'review', f'detail-{i}')
            self.service.audit(db, 'visible', self.a, '核验并接续旧工作台归档', 'PRIVATE_REMOTE_ID')
            self.service.enqueue(db, 'visible', 'maintenance')
        result = self.service.tool_history(self.a, asset['id'])
        self.assertEqual([r['id'] for r in result['orders']], ['visible'])
        self.assertFalse(result['has_more'])
        visible, = result['orders']
        self.assertEqual(visible['notes'], ['visible note'])
        self.assertEqual(visible['members'], ['visible owner', 'A 楼长', 'visible inspector'])
        self.assertEqual(visible['status'], 'submitted')
        self.assertEqual(visible['sync'], [{'label': '正常维护台账', 'state': 'pending'}])
        self.assertEqual(len(visible['history']), AUDIT_LIMIT)
        self.assertEqual(visible['history'][-1]['detail'], '')
        self.assertNotRegex(json.dumps(result), 'PRIVATE|hidden location|internal-id')
        for denied in (self.b,):
            with self.assertRaises(RiskError) as error:
                self.service.tool_history(denied, asset['id'])
            self.assertEqual(error.exception.status, 404)
        ordinary = self.service.tool_history(actor(identity='not_leader'), asset['id'])
        self.assertEqual(ordinary['orders'], [])
        admin_view = self.service.tool_history(actor(admin=True), asset['id'])
        self.assertTrue(admin_view['has_more'])
        self.assertEqual(len(admin_view['orders']), HISTORY_LIMIT)

    def test_external_certificate_deadline_and_legacy_cycle_are_distinct(self):
        asset = self.asset('certificate', self_cycle='', external_cycle='一年')
        self.order(asset, 'old', '2025-11', '2025-11-15', kind='external',
                   actual='2025-11-16', submitted_at='2025-11-16T09:00:00+08:00')
        legacy = self.service.tool_history(self.a, asset['id'])
        self.assertEqual(legacy['next_external_due'], '2026-11-16')
        self.assertEqual(legacy['certificate']['validity'], 'unknown')
        self.assertEqual(legacy['orders'][0]['inspection'], {})
        meta = {'report_no': 'No. 5', 'institution': 'Lab', 'result': 'qualified', 'expires_on': '2026-11-14',
                'confirmed_asset_ids': ['not-public']}
        self.order(asset, 'checked', '2026-10', '2026-10-01', kind='external', actual='2026-10-03',
                   submitted_at='2026-10-03T09:00:00+08:00', inspection_meta=dumps(meta))
        self.order(asset, 'opened', '2026-11', '2026-11-16', kind='external')
        checked = self.service.tool_history(self.a, asset['id'])
        self.assertEqual(checked['next_external_due'], '2026-11-14')
        self.assertEqual(checked['certificate']['validity'], 'expired')
        self.assertEqual(checked['certificate']['valid_until'], '2026-11-14')
        self.assertNotIn('confirmed_asset_ids', checked['certificate'])
        with self.service.connect(True) as db:
            db.execute('UPDATE tool_work_orders SET inspection_meta=? WHERE id=?',
                       (dumps({**meta, 'result': 'unqualified'}), 'checked'))
        failure = self.service.tool_history(self.a, asset['id'])
        self.assertEqual(failure['next_external_due'], '2026-10-03')
        self.assertEqual(failure['certificate']['valid_until'], '')
        self.assertEqual(failure['certificate']['validity'], 'unqualified')

    def test_delivers_by_building_at_beijing_nine_and_only_with_actionable_items(self):
        carryover = self.asset('carryover-A')
        waiting = self.asset('waiting-B', 'B')
        cancelled = self.asset('cancelled-A')
        self.order(carryover, 'late', '2026-08', '2026-08-29', workspace=True)
        self.order(waiting, 'review', '2026-11', '2026-11-15', workspace=True)
        self.order(cancelled, 'no', '2026-09', '2026-09-01', workspace=True, outcome='cancelled')
        self.order(self.asset('legacy-only'), 'legacy', '2026-11', '2026-11-01')
        with self.service.connect(True) as db:
            db.execute('''INSERT INTO tool_work_reviews(order_id,status,payload,submitted_by,submitted_at)
                VALUES ('review','pending','{}','person','2026-11-15T08:00:00+08:00')''')
        due = self.asset('due-seven-B', 'B', self_cycle='', external_cycle='一年', external_first='2026-11-22')
        self.order(due, 'due', '2026-11', '2026-11-22', kind='external', workspace=True)
        self.asset('due-eight-E', 'E', self_cycle='', external_cycle='一年', external_first='2026-11-23')
        self.asset('expired-A', self_cycle='', external_cycle='一年', external_last='2025-10-03')
        self.asset('retired-H', 'H', self_cycle='', external_cycle='一年', retired=True)
        calls = {}

        def deliver(scope, day, card):
            self.assertNotIn(scope, calls)
            calls[scope] = (day, '\n'.join(item['text']['content'] for item in card['body']['elements']))
            return True, 'confirmed'

        utc = NOW.astimezone(dt.timezone.utc)
        self.assertEqual(self.service.send_building_reminders(deliver, utc - dt.timedelta(seconds=1))['sent'], [])
        self.assertEqual(self.service.send_building_reminders(deliver, utc)['sent'], ['A', 'B'])
        self.assertEqual(calls['A'][0], '2026-11-15')
        self.assertIn('carryover-A', calls['A'][1])
        self.assertIn('证书到期', calls['A'][1])
        self.assertIn('expired-A（2026-10-03）', calls['A'][1])
        self.assertNotIn('cancelled-A', calls['A'][1])
        self.assertNotIn('legacy-only', calls['A'][1])
        self.assertIn('waiting-B', calls['B'][1])
        self.assertIn('due-seven-B', calls['B'][1])
        self.assertEqual(self.service.send_building_reminders(deliver, utc)['sent'], [])
        self.assertEqual(ToolManagement(self.inspection, None).send_building_reminders(deliver, utc)['sent'], [])
        with self.service.connect() as db:
            self.assertEqual(self.service.state(db, 'inventory_at'), 'legacy')

    def test_failed_delivery_retries_same_payload_and_uuid_with_active_lease(self):
        asset = self.asset('late-original')
        self.order(asset, 'late', '2026-10', '2026-10-31', workspace=True)
        attempted, overlaps = [], []

        def fail(scope, day, card):
            attempted.append((building_reminder_uuid(scope, day), dumps(card)))
            overlaps.append(self.service.send_building_reminders(lambda *_: True, NOW))
            return False, 'unconfirmed'

        self.assertEqual(self.service.send_building_reminders(fail, NOW), {'sent': [], 'failed': ['A']})
        self.assertEqual(overlaps, [{'sent': [], 'failed': []}])
        retry = NOW + dt.timedelta(seconds=60)
        restarted = ToolManagement(self.inspection, None)
        self.assertEqual(restarted.send_building_reminders(fail, retry - dt.timedelta(seconds=1))['failed'], [])

        def succeed(scope, day, card):
            attempted.append((building_reminder_uuid(scope, day), dumps(card)))
            return True

        self.assertEqual(restarted.send_building_reminders(succeed, retry)['sent'], ['A'])
        self.assertEqual(len(attempted), 2)
        self.assertEqual(attempted[0], attempted[1])
        self.assertNotEqual(building_reminder_uuid('A', '2026-11-15'), building_reminder_uuid('A', '2026-11-16'))
        self.assertNotEqual(building_reminder_uuid('A', '2026-11-15'), building_reminder_uuid('B', '2026-11-15'))
        self.assertEqual(restarted.send_building_reminders(succeed, retry)['sent'], [])

    def test_submitted_or_missing_orders_are_silent_and_pending_link_is_card_v2(self):
        submitted = self.asset('submitted', 'D', self_cycle='', external_cycle='一年')
        self.asset('inventory-only', 'D', self_cycle='', external_cycle='一年')
        self.order(submitted, 'done', '2026-11', '2026-11-01', kind='external',
                   workspace=True, submitted_at='2026-11-02T09:00:00+08:00', actual='2026-11-02')
        calls = []
        deliver = lambda scope, _, card: calls.append((scope, card)) or True
        self.assertEqual(self.service.send_building_reminders(deliver, NOW)['sent'], [])
        pending = self.asset('pending-E', 'E', self_cycle='', external_cycle='一年')
        self.order(pending, 'pending', '2026-11', '2026-11-01', kind='external', workspace=True)
        link = 'https://portal.example/risk-inspections?view=tool-self-check&scope=E'
        self.assertEqual(self.service.send_building_reminders(deliver, NOW, link_for_scope=lambda _: link)['sent'], ['E'])
        scope, card = calls[0]
        self.assertEqual((scope, card['schema']), ('E', '2.0'))
        self.assertEqual(card['body']['elements'][-1]['behaviors'], [{'type': 'open_url', 'default_url': link}])
        self.assertNotIn('inventory-only', dumps(card))

    def test_failed_card_is_not_replayed_after_all_orders_close(self):
        asset = self.asset('pending-retry')
        self.order(asset, 'retry', '2026-10', '2026-10-31', workspace=True)
        first = self.service.send_building_reminders(lambda *_: False, NOW)
        self.assertEqual(first['failed'], ['A'])
        with self.service.connect(True) as db:
            db.execute("UPDATE tool_work_orders SET outcome='cancelled' WHERE id='retry'")
        retry = NOW + dt.timedelta(seconds=60)
        self.assertEqual(self.service.send_building_reminders(lambda *_: self.fail('sent closed order'), retry),
                         {'sent': [], 'failed': []})

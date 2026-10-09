# -*- coding: utf-8 -*-
"""离线测试：plan_convergence_expiry 域到界提醒模块（无网络、无真实发送）。"""
import copy
import sys
import logging
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lan_bitable_template_portal import plan_convergence_expiry as module
from lan_bitable_template_portal.plan_convergence_expiry import (
    ConvergenceExpiryReminders,
    BUILDING_OPEN_ID_MAP as BLD_MAP,
)


def _ts(s):
    return module._ts(s)


def _base_ts():
    # Asia/Shanghai 2026-10-09 12:00:00
    return _ts('2026-10-09 12:00:00')


class MemoryStore:
    def __init__(self):
        self.documents = {}
        self.calls = []

    def get_document(self, namespace, key):
        self.calls.append(('get', namespace, key))
        return copy.deepcopy(self.documents.get((namespace, key)))

    def put_document(self, namespace, key, payload):
        self.calls.append(('put', namespace, key))
        self.documents[(namespace, key)] = copy.deepcopy(payload)


class FakeService:
    def __init__(self, people):
        self.people = people
        self.people_calls = 0

    def _building_codes_from_value(self, value):
        value = str(value or '')
        codes = []
        if '110' in value:
            codes.append('110')
        for c in 'ABCDEH':
            if c in value:
                codes.append(c)
        return codes

    def _load_signature_people(self, *, force=False):
        self.people_calls += 1
        return copy.deepcopy(self.people)


def _person(building, position, open_id=None, can=True):
    return {'building': building, 'position': position,
            'open_id': open_id or ('ou_' + building + position[0] + '1'),
            'can_receive_message': can}


class FakeConvergence:
    """真实 provider 每次返回全新深拷贝快照，且状态一旦变更即保持（不回头）。

    - maintenance_records：records_by_call 在指定调用序发生变更后，后续调用
      持续返回该变更后的快照；second_records 同理（第二个调用起保持）。
    - blocks(refresh=True)：若提供 refreshed_blocks，则刷新时返回新的活动列表
      （用于模拟延期后刷新拿到延长 end），否则回落 blocks_doc。
    """

    def __init__(self, records, blocks, details=None, second_records=None,
                 records_by_call=None, refreshed_blocks=None):
        self.records = records
        self.second_records = second_records
        self.records_by_call = records_by_call or {}
        self.refreshed_blocks = refreshed_blocks
        self.blocks_doc = {'items': blocks, 'loaded_at': _base_ts()}
        self.details = details or {}
        self.calls = {'records': 0, 'blocks': 0, 'block': []}
        self._current_records = copy.deepcopy(records)
        self._latched = False

    def maintenance_records(self):
        self.calls['records'] += 1
        n = self.calls['records']
        if n in self.records_by_call:
            self._current_records = copy.deepcopy(self.records_by_call[n])
            self._latched = True
        elif n > 1 and self.second_records is not None:
            self._current_records = copy.deepcopy(self.second_records)
            self._latched = True
        elif not self._latched:
            self._current_records = copy.deepcopy(self.records)
        return copy.deepcopy(self._current_records)

    def blocks(self, refresh=False, *, deadline=None):
        self.calls['blocks'] += 1
        if refresh and self.refreshed_blocks is not None:
            return copy.deepcopy(self.refreshed_blocks)
        return copy.deepcopy(self.blocks_doc)

    def block(self, block_id, *, deadline=None):
        self.calls['block'].append(block_id)
        detail = copy.deepcopy(self.details.get(str(block_id), {}))
        if detail:
            detail.setdefault('blockId', str(block_id))
        return detail


def _record(record_id, building, hits):
    return {'record_id': record_id, 'name': '检修-' + record_id,
            'building': building, 'hits': hits}


def _hit(block_id, end, name='屏蔽计划'):
    return {'blockId': block_id, 'endTime': end, 'endTimestamp': end,
            'blockName': name}


class SendRecorder:
    def __init__(self):
        self.sends = []
        self.fail = {}

    def __call__(self, text, recipients, *, message_uuid=''):
        recipient = recipients[0]
        self.sends.append({'text': text, 'recipient': recipient,
                           'message_uuid': message_uuid})
        ok = not self.fail.get(recipient)
        return ok, '' if ok else 'fail', [] if ok else [{'open_id': recipient, 'ok': False}]


def _make(people=None, records=None, blocks=None, details=None,
          second_records=None, records_by_call=None, refreshed_blocks=None):
    store = MemoryStore()
    service = FakeService(people or [])
    conv = FakeConvergence(records or [], blocks or [], details or {},
                           second_records, records_by_call, refreshed_blocks)
    send = SendRecorder()
    reminders = ConvergenceExpiryReminders(service, store, lambda: conv, send)
    return reminders, store, send, conv, service


def _assert_no_send(send):
    assert send.sends == [], send.sends


class TestPlanConvergenceExpiry(unittest.TestCase):
    def test_window_boundaries(self):
        end = _ts('2026-10-09 14:00:00')
        rem, store, send, conv, _svc = _make(
            people=[_person('A楼', '工程师'), _person('A楼', '主管')],
            records=[_record('r1', 'A楼', [_hit('b1', '2026-10-09 14:00:00')])],
            blocks=[{'blockId': 'b1', 'status': '1', 'endTime': '2026-10-09 14:00:00'}])
        # end-3600 整点：提醒（含边界）
        when = end - 3600.0
        m = rem.tick(now=when)
        self.assertEqual(m['due'], 1, m)
        self.assertEqual(m['sent'], len(send.sends), m)
        self.assertTrue(send.sends, m)
        self.assertEqual(send.sends[0]['message_uuid'], module._uuid_for(
            'r1:b1:%d:%s' % (int(end), send.sends[0]['recipient'])))
        # 恢复新实例，快到 end 但未到 end：仍只发一次（已去重）
        rem2, store2, send2, conv2, _ = _make(
            people=[_person('A楼', '工程师')],
            records=[_record('r1', 'A楼', [_hit('b1', '2026-10-09 14:00:00')])],
            blocks=[{'blockId': 'b1', 'status': '1', 'endTime': '2026-10-09 14:00:00'}])
        store2.documents = copy.deepcopy(store.documents)
        m2 = rem2.tick(now=end - 10.0)
        self.assertEqual(m2['due'], 1, m2)
        self.assertEqual(send2.sends, [], m2)
        # 到 end 时不再提醒
        rem3, store3, send3, conv3, _ = _make(
            people=[_person('A楼', '工程师')],
            records=[_record('r1', 'A楼', [_hit('b1', '2026-10-09 14:00:00')])],
            blocks=[{'blockId': 'b1', 'status': '1', 'endTime': '2026-10-09 14:00:00'}])
        m3 = rem3.tick(now=end)
        self.assertEqual(m3['due'], 0, m3)
        self.assertEqual(send3.sends, [], m3)

    def test_end_completed_and_before_window(self):
        end = _ts('2026-10-09 14:00:00')
        rem, _, send, conv, _ = _make(
            people=[_person('A楼', '工程师')],
            records=[_record('r1', 'A楼', [_hit('b1', '2026-10-09 14:00:00')])],
            blocks=[{'blockId': 'b1', 'status': '1', 'endTime': '2026-10-09 14:00:00'}])
        m = rem.tick(now=end + 60)          # 已结束
        self.assertEqual(m['due'], 0, m)
        self.assertEqual(send.sends, [], m)
        m2 = rem.tick(now=end - 7200)       # 早于窗口
        self.assertEqual(m2['due'], 0, m2)
        self.assertEqual(send.sends, [], m2)

    def test_status_closed_skip(self):
        end = _ts('2026-10-09 14:00:00')
        rem, _, send, conv, _ = _make(
            people=[_person('A楼', '工程师')],
            records=[_record('r1', 'A楼', [_hit('b1', '2026-10-09 14:00:00')])],
            blocks=[{'blockId': 'b1', 'status': '2', 'endTime': '2026-10-09 14:00:00'}])
        m = rem.tick(now=end - 600)
        self.assertEqual(m['due'], 0, m)
        self.assertEqual(send.sends, [], m)
        self.assertEqual(conv.calls['block'], [], m)

    def test_de_skip_and_multibuilding_filter(self):
        people = [_person('D楼', '工程师'), _person('E楼', '主管'),
                  _person('A楼', '工程师', 'ou_engA'), _person('B楼', '主管', 'ou_supB')]
        rem, _, send, conv, _ = _make(
            people=people,
            records=[_record('rD', 'D楼', [_hit('bd', '2026-10-09 14:00:00')]),
                     _record('rE', 'E楼', [_hit('be', '2026-10-09 14:00:00')]),
                     _record('rA', 'A楼', [_hit('ba', '2026-10-09 14:00:00')])],
            blocks=[{'blockId': b, 'status': '1', 'endTime': '2026-10-09 14:00:00'}
                    for b in ('bd', 'be', 'ba')])
        end = _ts('2026-10-09 14:00:00')
        m = rem.tick(now=end - 600)
        # 仅 A 楼发；D/E 完全不过问
        self.assertEqual(m['due'], 1, m)
        got = {s['recipient'] for s in send.sends}
        self.assertEqual(got, {BLD_MAP['A'], 'ou_engA'}, got)
        self.assertFalse(got & {'ou_D工程师1', 'ou_E主管1'}, got)
        # 多楼栋：A+B，只保留 A/B 并去重
        rem2, _, send2, conv2, _ = _make(
            people=people,
            records=[_record('rAB', 'A楼、B楼',
                             [_hit('bab', '2026-10-09 14:00:00')])],
            blocks=[{'blockId': 'bab', 'status': '1', 'endTime': '2026-10-09 14:00:00'}])
        m2 = rem2.tick(now=end - 600)
        got = {s['recipient'] for s in send2.sends}
        self.assertIn(BLD_MAP['A'], got)
        self.assertIn(BLD_MAP['B'], got)
        self.assertIn('ou_engA', got)
        self.assertIn('ou_supB', got)
        # D/E 人员不会进入 A/B 楼提醒
        self.assertFalse({'ou_D工程师1', 'ou_E主管1'} & got, got)

    def test_timezone_text_parsing(self):
        end = _ts('2026-10-09 14:00:00')
        rem, _, send, conv, _ = _make(
            people=[_person('C楼', '工程师', 'ou_engC')],
            records=[_record('r1', 'C楼', [_hit('b1', '2026-10-09 14:00')])],  # 无秒
            blocks=[{'blockId': 'b1', 'status': '1', 'endTime': 1791525600_000}])  # 毫秒
        m = rem.tick(now=end - 600)
        self.assertEqual(m['due'], 1, m)
        self.assertTrue(send.sends, m)
        self.assertEqual(module._ts('2026-10-09 14:00'), module._ts('2026-10-09 14:00:00'))

    def test_ts_numeric_string_and_iso_offset(self):
        base = _ts('2026-10-09 14:00:00')
        self.assertEqual(module._ts('1791525600'), base)           # 数字字符串（秒）
        self.assertEqual(module._ts('1791525600000'), base)        # 数字字符串（毫秒）
        self.assertEqual(module._ts('2026-10-09T06:00:00Z'), base)  # UTC ISO 带 Z
        self.assertEqual(module._ts('2026-10-09T14:00:00+08:00'), base)  # 带偏移 ISO
        # NaN/inf/bool 一律拒绝
        for bad in (float('nan'), float('inf'), float('-inf'), 'nan', 'inf',
                    True, False):
            self.assertIsNone(module._ts(bad), bad)

    def test_extension_creates_new_key(self):
        end = _ts('2026-10-09 14:00:00')
        people = [_person('A楼', '工程师')]  # A 接收人 = 值班负责人 + 该工程师（共 2 人）
        rem, store, send, conv, _ = _make(
            people=people,
            records=[_record('r1', 'A楼', [_hit('b1', '2026-10-09 14:00:00')])],
            blocks=[{'blockId': 'b1', 'status': '1', 'endTime': '2026-10-09 14:00:00'}])
        rem.tick(now=end - 600)
        first = len(send.sends)
        self.assertEqual(first, 2, send.sends)
        # 同 key 不重发
        rem.tick(now=end - 100)
        self.assertEqual(len(send.sends), 2)
        # 延期 → 新 end 键 → 重新提醒
        new_end = _ts('2026-10-09 16:00:00')
        conv.blocks_doc = {'items': [{'blockId': 'b1', 'status': '1',
                                      'endTime': '2026-10-09 16:00:00'}], 'loaded_at': _base_ts()}
        m = rem.tick(now=new_end - 600)
        self.assertEqual(m['due'], 1, m)
        self.assertEqual(len(send.sends), 4, send.sends)
        # 新发送 key 使用新 end → uuid 全部不同
        uuids = [s['message_uuid'] for s in send.sends]
        self.assertEqual(len(set(uuids)), 4, uuids)

    def test_unique_per_recipient_and_restart_dedupe(self):
        people = [_person('A楼', '工程师', 'ou_engA'),
                  _person('A楼', '主管', 'ou_supA')]
        records = [_record('r1', 'A楼', [_hit('b1', '2026-10-09 14:00:00')])]
        blocks = [{'blockId': 'b1', 'status': '1', 'endTime': '2026-10-09 14:00:00'}]
        end = _ts('2026-10-09 14:00:00')
        rem, store, send, conv, _ = _make(people, records, blocks)
        m = rem.tick(now=end - 600)
        # A 接收人 = 值班负责人 + 工程师 + 主管（共 3 人）
        self.assertEqual(len(send.sends), 3, send.sends)
        self.assertEqual(m['sent'], 3, send.sends)
        # 重启：新实例 + 相同 store（持久化）→ 不重发
        rem2, store2, send2, conv2, _ = _make(people, records, blocks)
        store2.documents = copy.deepcopy(store.documents)
        m2 = rem2.tick(now=end - 300)
        self.assertEqual(m2['due'], 1, m2)
        self.assertEqual(send2.sends, [], m2)
        # store 仅写入一次接收人快照（coalesce once per tick）
        puts = [c for c in store.calls if c[0] == 'put' and c[1] == module.NS and c[2] == 'recipients']
        self.assertEqual(len(puts), 1, store.calls)

    def test_retry_failures_up_to_three(self):
        people = [_person('A楼', '工程师', 'ou_engA')]
        end = _ts('2026-10-09 14:00:00')
        rem, store, send, conv, _ = _make(
            people, [_record('r1', 'A楼', [_hit('b1', '2026-10-09 14:00:00')])],
            [{'blockId': 'b1', 'status': '1', 'endTime': '2026-10-09 14:00:00'}])
        # 两个接收人（值班负责人 + ou_engA）都持续失败
        send.fail[BLD_MAP['A']] = True
        send.fail['ou_engA'] = True
        for _ in range(3):
            m = rem.tick(now=end - 1200)
            self.assertEqual(m['due'], 1, m)
            self.assertEqual(m['sent'], 0, m)
        attempts = 3 * 2  # 每人 3 次，共 2 人
        self.assertEqual(len(send.sends), attempts, send.sends)
        # 第 4 次不再尝试（已达上限）
        rem.tick(now=end - 1100)
        self.assertEqual(len(send.sends), attempts)
        # 仅当确认成功才标记 sent
        for recipient in (BLD_MAP['A'], 'ou_engA'):
            key = 'r1:b1:%d:%s' % (int(end), recipient)
            state = store.documents[(module.NS, key)]
            self.assertIs(state['sent'], False)
            self.assertEqual(state['attempts'], 3, state)

    def test_ended_during_fetch_stops_send(self):
        end = _ts('2026-10-09 14:00:00')
        people = [_person('A楼', '工程师')]  # A 接收人 = 值班负责人 + 该工程师（共 2 人）
        blocks = [{'blockId': 'b1', 'status': '1', 'endTime': '2026-10-09 14:00:00'}]
        # 第二次 snapshot（_live/接收人之后）已无该记录：真实 provider 每次返回全新深拷贝快照
        rem, store, send, conv, _ = _make(
            people, [_record('r1', 'A楼', [_hit('b1', '2026-10-09 14:00:00')])],
            blocks, second_records=[])
        m = rem.tick(now=end - 600)
        # 快照在投递前已变为空 → 同一 tick 内即零发送，绝不基于陈旧数据补发
        self.assertEqual(m['due'], 1, m)
        self.assertEqual(m['sent'], 0, m)
        self.assertEqual(send.sends, [], (m, send.sends))
        self.assertGreaterEqual(conv.calls['records'], 2, conv.calls)   # 复核调用了 fresh 快照

    def test_old_persisted_hit_without_end_resolved(self):
        end = _ts('2026-10-09 14:00:00')
        # 旧持久化核对结果只有 blockId，无 endTimestamp/endTime → 仍按 blockId 保留候选，
        # 由活动列表/明细补权威 end；不得静默错过每条旧匹配记录。
        rem, _, send, conv, _ = _make(
            people=[_person('A楼', '工程师', 'ou_engA')],
            records=[_record('r1', 'A楼', [{'blockId': 'b1', 'blockName': '旧计划'}])],
            blocks=[{'blockId': 'b1', 'status': '1', 'endTime': '2026-10-09 14:00:00'}])
        m = rem.tick(now=end - 600)
        self.assertEqual(m['due'], 1, m)
        self.assertTrue(send.sends, (m, send.sends))

    def test_building_change_recalculates_scope(self):
        end = _ts('2026-10-09 14:00:00')
        people = [_person('A楼', '工程师', 'ou_engA'), _person('B楼', '工程师', 'ou_engB')]
        changed = [_record('r1', 'B楼', [_hit('b1', '2026-10-09 14:00:00')])]
        rem, _, send, conv, _ = _make(
            people=people,
            records=[_record('r1', 'A楼', [_hit('b1', '2026-10-09 14:00:00')])],
            blocks=[{'blockId': 'b1', 'status': '1', 'endTime': '2026-10-09 14:00:00'}],
            records_by_call={2: changed})
        m = rem.tick(now=end - 600)
        got = {s['recipient'] for s in send.sends}
        # 楼栋由 A→B：接收人按新街域 B 重算，绝不泄漏到旧的 A 范围
        self.assertIn(BLD_MAP['B'], got)
        self.assertIn('ou_engB', got)
        self.assertFalse(got & {BLD_MAP['A'], 'ou_engA'}, got)

    def test_midloop_building_change_stops_remaining_recipients(self):
        end = _ts('2026-10-09 14:00:00')
        people = [_person('A楼', '工程师', 'ou_engA'),
                  _person('A楼', '主管', 'ou_supA'),
                  _person('B楼', '工程师', 'ou_engB')]
        # 第 1 次记录（_run）、第 2 次（_deliver 起始复核）、第 3 次（第 1 个接收人复核）
        # 均为 A；第 4 次（第 2 个接收人复核）楼栋变为 B → 立即停止第 2/3 个接收人
        changed = [_record('r1', 'B楼', [_hit('b1', '2026-10-09 14:00:00')])]
        rem, _, send, conv, _ = _make(
            people=people,
            records=[_record('r1', 'A楼', [_hit('b1', '2026-10-09 14:00:00')])],
            blocks=[{'blockId': 'b1', 'status': '1', 'endTime': '2026-10-09 14:00:00'}],
            records_by_call={4: changed})
        m = rem.tick(now=end - 600)
        got = {s['recipient'] for s in send.sends}
        # 第一个 A 接收人（值班负责人）已在变更前发出；其余 A 接收人因楼栋变更被安全截断
        self.assertEqual(got, {BLD_MAP['A']}, got)
        self.assertEqual(len(send.sends), 1, send.sends)
        self.assertFalse(got & {'ou_engB'}, got)
        self.assertGreaterEqual(conv.calls['records'], 4, conv.calls)

    def test_recipients_correct_dedup_and_cache(self):
        people = [
            _person('A楼', '工程师', 'ou_engA'),
            _person('A楼', '工程师', 'ou_engA2'),
            _person('A楼', '主管', 'ou_supA'),
            _person('D楼', '工程师', 'ou_engD'),       # D 不参与
            _person('H楼', '值班员', 'ou_shiftH'),      # 非工程师/主管 → 排除
            _person('B楼', '工程师', '', can=True),     # 无 open_id 排除
            _person('C楼', '工程师', 'ou_engC', can=False),  # 不能接收排除
        ]
        end = _ts('2026-10-09 14:00:00')
        rem, store, send, conv, service = _make(
            people=people,
            records=[_record('r1', 'A楼', [_hit('b1', '2026-10-09 14:00:00')])],
            blocks=[{'blockId': 'b1', 'status': '1', 'endTime': '2026-10-09 14:00:00'}])
        m = rem.tick(now=end - 600)
        got = {s['recipient'] for s in send.sends}
        self.assertEqual(got, {BLD_MAP['A'], 'ou_engA', 'ou_engA2', 'ou_supA'}, got)
        self.assertEqual(m['sent'], 4)
        # 缓存：多记录同 tick 只加载一次 people
        service.people_calls = 0
        rem.tick(now=end - 100)  # 已去重；但快照未过期
        self.assertEqual(service.people_calls, 0)
        # 快照是持久化最小信息，不含 raw/signature
        snap = store.documents[(module.NS, 'recipients')]
        self.assertNotIn('raw', str(snap).lower(), snap)
        self.assertNotIn('signature', str(snap).lower(), snap)

    def test_recipient_snapshot_stale_at_1h(self):
        now = _base_ts()
        rem, _, send, conv, service = _make(people=[_person('A楼', '工程师')])
        rem._recipient_snapshot(now)
        self.assertEqual(service.people_calls, 1)
        rem._recipient_snapshot(now + 1000)   # < 1h：命中缓存
        self.assertEqual(service.people_calls, 1)
        rem._recipient_snapshot(now + 4000)   # > 1h：快照过期，重新加载
        self.assertEqual(service.people_calls, 2)

    def test_pending_persist_before_send(self):
        end = _ts('2026-10-09 14:00:00')
        store = MemoryStore()
        service = FakeService([_person('A楼', '工程师', 'ou_engA')])
        conv = FakeConvergence(
            [_record('r1', 'A楼', [_hit('b1', '2026-10-09 14:00:00')])],
            [{'blockId': 'b1', 'status': '1', 'endTime': '2026-10-09 14:00:00'}])
        checked = []

        def send(text, recipients, *, message_uuid=''):
            recipient = recipients[0]
            key = 'r1:b1:%d:%s' % (int(end), recipient)
            state = store.get_document(module.NS, key) or {}
            # 调用 send 前，该接收人的待发送记录已持久化（崩溃后可稳定 uuid 重试）
            checked.append((recipient, state.get('sent'), state.get('attempts'),
                            state.get('message_uuid')))
            return True, '', []

        rem = ConvergenceExpiryReminders(service, store, lambda: conv, send)
        m = rem.tick(now=end - 600)
        # A 接收人 = 值班负责人 + 工程师（2 人）
        self.assertEqual(len(checked), 2, checked)
        for _recipient, sent, attempts, mu in checked:
            self.assertIs(sent, False)
            self.assertEqual(attempts, 1, checked)
            self.assertTrue(mu, checked)
        self.assertEqual(m['sent'], 2)

    def test_warning_either_engineer_or_supervisor_missing(self):
        end = _ts('2026-10-09 14:00:00')
        # 只有工程师、没有主管 → 仍须告警（早期仅两者都缺才告警）
        rem, _, send, _, _ = _make(
            people=[_person('A楼', '工程师', 'ou_engA')],
            records=[_record('r1', 'A楼', [_hit('b1', '2026-10-09 14:00:00')])],
            blocks=[{'blockId': 'b1', 'status': '1', 'endTime': '2026-10-09 14:00:00'}])
        with self.assertLogs(module.LOG, level=logging.WARNING) as cm:
            m = rem.tick(now=end - 600)
        self.assertEqual(m['sent'], 2, m)
        self.assertTrue(send.sends, m)
        text = '\n'.join(cm.output)
        self.assertIn('缺少', text)
        self.assertIn('工程师/主管', text)

    def test_no_remote_when_no_repairs(self):
        rem, store, send, conv, _ = _make(people=[_person('A楼', '工程师')],
                                          records=[])  # 无任何记录/命中
        m = rem.tick(now=_base_ts())
        self.assertEqual(m, {'checked': 0, 'candidates': 0, 'due': 0, 'sent': 0, 'refreshed': False})
        self.assertEqual(conv.calls['blocks'], 0)
        self.assertEqual(conv.calls['records'], 1)
        self.assertEqual(send.sends, [])

    def test_remote_unavailable_skips_stale_send(self):
        # blocks refresh 抛异常（模拟远端不可用）→ 不发基于陈旧 match end 的提醒
        end = _ts('2026-10-09 14:00:00')
        people = [_person('A楼', '工程师')]

        class UnavailableConv(FakeConvergence):
            def blocks(self, refresh=False, *, deadline=None):
                if refresh:
                    raise RuntimeError('unavailable')
                return {'items': [], 'loaded_at': 0}  # 陈旧

        conv = UnavailableConv([_record('r1', 'A楼', [_hit('b1', '2026-10-09 14:00:00')])],
                               [], {})
        rem = ConvergenceExpiryReminders(FakeService(people), MemoryStore(), lambda: conv, SendRecorder())
        out = rem.tick(now=end - 600)
        self.assertTrue(out.get('error') or out.get('due', 0) == 0)

    def test_detail_fetch_only_for_matched_blocks(self):
        end = _ts('2026-10-09 14:00:00')
        rem, _, send, conv, _ = _make(
            people=[_person('A楼', '工程师')],
            records=[_record('r1', 'A楼', [_hit('b1', '2026-10-09 14:00:00')])],
            # 列表含 b1 但无 end → 细节补查；同时存在无关 b2（未匹配）不应查
            blocks=[{'blockId': 'b1', 'status': '1', 'endTime': ''},
                    {'blockId': 'b2', 'status': '1', 'endTime': ''}],
            details={'b1': {'endTime': '2026-10-09 14:00:00', 'blockName': '计划B1'}})
        m = rem.tick(now=end - 600)
        self.assertEqual(m['due'], 1, m)
        self.assertTrue(send.sends, m)
        self.assertEqual(conv.calls['block'], ['b1'], conv.calls['block'])
        self.assertNotIn('失败', send.sends[0]['text'])
        self.assertIn('计划B1', send.sends[0]['text'])

    def test_live_detail_budget_break(self):
        end = _ts('2026-10-09 14:00:00')
        rem, _, send, conv, _ = _make(
            people=[_person('A楼', '工程师')],
            records=[_record('r1', 'A楼', [_hit('b1', '2026-10-09 14:00:00')])],
            blocks=[{'blockId': 'b1', 'status': '1', 'endTime': ''}],  # 缺 end → 需 detail
            details={'b1': {'endTime': '2026-10-09 14:00:00'}})
        # 预算已耗尽 → 不再逐个发起 detail，避免成千上万次逐一超时
        live = rem._live(conv, rem._candidates(conv.maintenance_records()),
                         time.monotonic(), end - 600)
        self.assertEqual(conv.calls['block'], [])
        self.assertEqual(live, {})

    def test_expired_cached_end_extended_refreshed(self):
        base = _base_ts()  # 12:00
        end = _ts('2026-10-09 14:00:00')
        # 缓存的屏蔽列表写着 08:00（早已过期）且 loaded_at 很旧 → 必须刷新；
        # 刷新拿到的活动列表已延期到 14:00 → 使用新 end 计算窗口。
        rem, store, send, conv, _ = _make(
            people=[_person('A楼', '工程师')],
            records=[_record('r1', 'A楼', [_hit('b1', '2026-10-09 08:00:00')])],
            blocks=[{'blockId': 'b1', 'status': '1', 'endTime': '2026-10-09 08:00:00'}])
        conv.blocks_doc['loaded_at'] = base - 7200  # 缓存于 10:00（已过期）
        conv.refreshed_blocks = {'items': [{'blockId': 'b1', 'status': '1',
                                            'endTime': '2026-10-09 14:00:00'}],
                                 'loaded_at': base}
        out = rem.tick(now=base)
        # 缓存 end 过期且 TTL 超时 → 刷新成功，不再基于旧 end（08:00）发送
        self.assertTrue(out['refreshed'], out)
        self.assertEqual(out['due'], 0, out)
        self.assertEqual(send.sends, [])
        # 进入延期后的新窗口（13:20）→ 用延长后的 end 正常提醒
        out2 = rem.tick(now=_ts('2026-10-09 13:20:00'))
        self.assertEqual(out2['due'], 1, out2)
        self.assertTrue(send.sends, out2)
        self.assertEqual(send.sends[0]['message_uuid'],
                         module._uuid_for('r1:b1:%d:%s' % (int(end), send.sends[0]['recipient'])))
        self.assertIn('14:00', send.sends[0]['text'])


if __name__ == '__main__':
    unittest.main()
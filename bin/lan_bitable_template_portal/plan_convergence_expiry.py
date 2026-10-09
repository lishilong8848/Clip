# -*- coding: utf-8 -*-
"""独立有界的「计划收敛到期」提醒模块：仅在检修通告与智航屏蔽计划
（A/B/C/H/110）临近结束（end-3600s..end) 时提醒仍未完成的检修；
不自动结束、不写表、不启动线程。D/E 楼主始终不检查、不提醒。
"""
import datetime as dt
import logging
import math
import time
import uuid

from .portal_service import BUILDING_OPEN_ID_MAP

LOG = logging.getLogger(__name__)
NS = 'plan_convergence_expiry'
ALLOWED = ('110', 'A', 'B', 'C', 'H')       # D/E 永不提醒
WINDOW = 3600.0                             # 到期前 1 小时即开始提醒
RECIPIENT_TTL = 3600.0                      # 接收人快照缓存 1h
BLOCKS_TTL = 60.0                           # 屏蔽列表本地缓存有效性
CHECK_BUDGET = 10.0                         # 单次 tick 整体 ≤10s 按单调时钟预算
MAX_ATTEMPTS = 3                            # 单个接收人失败重试上限
TZ = dt.timezone(dt.timedelta(hours=8))     # Asia/Shanghai（无夏令时）

def _ts(value):
    """数值/数字字符串/Asia-Shanghai 文本或带偏移 ISO → 秒时间戳；NaN/inf/bool/无法解析 → None。"""
    if value is None or value == '' or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        num = float(value)
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.lstrip('+-').replace('.', '', 1).isdigit():
            try:
                num = float(text)
            except ValueError:
                return None
        else:
            try:
                parsed = dt.datetime.fromisoformat(text)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=TZ)
                return parsed.timestamp()
            except ValueError:
                return None
    if not math.isfinite(num):
        return None
    return num / 1000.0 if num > 10 ** 12 else num


def _uuid_for(key):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, 'plan_convergence_expiry:' + key))


def _label(code):
    return '110站' if code == '110' else code + '楼'


class ConvergenceExpiryReminders:
    def __init__(self, service, store, convergence, send):
        self.service = service
        self.store = store
        self.convergence = convergence
        self.send = send
    def tick(self, now=None):
        try:
            return self._run(now)
        except Exception as exc:
            LOG.warning('计划收敛到期提醒一次执行异常: %s', type(exc).__name__)
            return {'error': type(exc).__name__}
    def _run(self, now):
        now = time.time() if now is None else float(now)
        conv = self.convergence()
        records = conv.maintenance_records() or []
        candidates = self._candidates(records)
        if not candidates:
            # 无本地命中记录 → 不发网络、不启动任何请求
            return {'checked': len(records), 'candidates': 0,
                    'due': 0, 'sent': 0, 'refreshed': False}
        self._refreshed = False
        deadline = time.monotonic() + CHECK_BUDGET
        live = self._live(conv, candidates, deadline, now)
        due = [{**item, 'end': info['end'], 'block_name': info['name']}
               for item in candidates
               for info in [live.get(item['block_id'])]
               if info and info['end'] - WINDOW <= now < info['end']]
        metrics = {'checked': len(records), 'candidates': len(candidates),
                   'due': len(due), 'sent': 0, 'refreshed': self._refreshed}
        if not due:
            return metrics
        snapshot = self._recipient_snapshot(now)   # 只有真正到期才查人
        for item in due:
            metrics['sent'] += self._deliver(conv, item, snapshot, now)
        return metrics
    def _candidates(self, records):
        # 旧持久化核对可能缺 endTime：按 blockId 保留，由活动列表/明细补权威 end
        out = []
        for rec in records:
            codes = [c for c in self.service._building_codes_from_value(rec.get('building'))
                     if c in ALLOWED]
            if not codes:
                continue
            for hit in rec.get('hits') or []:
                block_id = str(hit.get('blockId') or '')
                if block_id:
                    out.append({'record': rec, 'block_id': block_id, 'codes': codes})
        return out
    def _rows_to_live(self, rows):
        live = {}
        for row in rows or []:
            bid = str(row.get('blockId') or row.get('id') or '')
            if not bid or str(row.get('status')) != '1':
                continue
            end = _ts(row.get('endTimestamp', row.get('endTime')))
            if end is not None:
                live[bid] = {'end': end, 'name': str(row.get('blockName') or '')}
        return live
    def _live(self, conv, candidates, deadline, now):
        cached = conv.blocks(refresh=False) or {'items': [], 'loaded_at': 0}
        rows = cached.get('items') or []
        stale = self._rows_to_live(rows)
        candidate_ids = {c['block_id'] for c in candidates}
        # 只需刷新候选：缺 end 或正处于提醒窗口内
        pending = {bid for bid in candidate_ids
                   if bid not in stale
                   or stale[bid]['end'] - WINDOW <= now}
        if pending and now - float(cached.get('loaded_at') or 0) >= BLOCKS_TTL \
                and time.monotonic() < deadline:
            cached = conv.blocks(refresh=True, deadline=deadline) or {'items': [], 'loaded_at': 0}
            self._refreshed = True
            rows = cached.get('items') or []
            stale = self._rows_to_live(rows)
        live = dict(stale)
        active_ids = {str(r.get('blockId') or r.get('id') or '')
                      for r in rows if str(r.get('status')) == '1'}
        # 只补查「活动中但缺 end」的匹配块；超预算即停，避免上千次逐一超时
        for bid in sorted((candidate_ids - set(live)) & active_ids):
            if time.monotonic() >= deadline:
                break
            try:
                detail = conv.block(bid, deadline=deadline) or {}
                end = _ts(detail.get('endTimestamp', detail.get('endTime')))
                if end is not None:
                    live[bid] = {'end': end, 'name': str(detail.get('blockName') or '')}
            except Exception as exc:
                LOG.warning('屏蔽明细读取失败，跳过到期提醒 %s: %s', bid, type(exc).__name__)
        return live
    def _recipient_snapshot(self, now):
        cached = self.store.get_document(NS, 'recipients') or {}
        if cached and now - float(cached.get('loaded_at') or 0) < RECIPIENT_TTL:
            return cached
        engineers, supervisors = {}, {}
        for person in self.service._load_signature_people(force=False) or []:
            open_id = str(person.get('open_id') or '').strip()
            if not open_id or not person.get('can_receive_message'):
                continue
            position = str(person.get('position') or '')
            codes = [c for c in self.service._building_codes_from_value(person.get('building'))
                     if c in ALLOWED]
            if not codes:
                continue
            pool = engineers if '工程师' in position else supervisors if '主管' in position else None
            if pool is not None:
                for code in codes:
                    pool.setdefault(code, []).append(open_id)
        snapshot = {'loaded_at': now, 'engineers': engineers, 'supervisors': supervisors}
        self.store.put_document(NS, 'recipients', snapshot)
        return snapshot
    def _recipients_for(self, codes, snapshot):
        recipients = []
        engineers, supervisors = snapshot.get('engineers') or {}, snapshot.get('supervisors') or {}
        for code in codes:
            duty = BUILDING_OPEN_ID_MAP.get(code)
            if duty:
                recipients.append(duty)
            eng, sup = engineers.get(code) or [], supervisors.get(code) or []
            recipients.extend(eng)
            recipients.extend(sup)
            if not eng or not sup:
                LOG.warning('计划收敛到期提醒缺少 %s 的在岗工程师/主管', _label(code))
        return list(dict.fromkeys(str(open_id) for open_id in recipients if open_id))
    def _fresh_record(self, conv, item):
        # 真实 provider 每次返回全新归一化快照：投递前复核是否仍是「活动同 id」
        records = conv.maintenance_records() or []
        current = next((r for r in records
                        if str(r.get('record_id')) == str(item['record']['record_id'])), None)
        if current is None:
            return None
        codes = [c for c in self.service._building_codes_from_value(current.get('building'))
                 if c in ALLOWED]
        if not codes:
            return None
        return {'record': current, 'codes': codes,
                'hits_ids': {str(h.get('blockId') or '') for h in current.get('hits') or []}}
    def _deliver(self, conv, item, snapshot, now):
        fresh = self._fresh_record(conv, item)
        if fresh is None or item['block_id'] not in fresh['hits_ids']:
            return 0   # 已结束/删除，或该屏蔽已不再命中 → 不发陈旧数据
        send_item = {**item, 'record': fresh['record'], 'codes': fresh['codes']}
        recipients = self._recipients_for(fresh['codes'], snapshot)
        if not recipients:
            return 0
        sent_count = 0
        for recipient in recipients:
            # 每个接收人/重试前再复核一次，避免清单遍历期间记录关闭
            alive = self._fresh_record(conv, item)
            if (alive is None or item['block_id'] not in alive['hits_ids']
                    or set(alive['codes']) != set(send_item['codes'])):
                break
            key = ':'.join((str(item['record']['record_id']), item['block_id'],
                            str(int(item['end'])), recipient))
            state = self.store.get_document(NS, key) or {}
            if state.get('sent') or int(state.get('attempts') or 0) >= MAX_ATTEMPTS:
                continue
            attempts = int(state.get('attempts') or 0) + 1
            message_uuid = _uuid_for(key)
            base = {'attempts': attempts, 'message_uuid': message_uuid, 'last_attempted': now}
            # 先持久化待发送（崩溃后可用稳定 uuid 重试），再真正调用 send
            self.store.put_document(NS, key, dict(base, sent=False))
            try:
                ok = bool(self.send(self._message(send_item, recipient),
                                    [recipient], message_uuid=message_uuid)[0])
            except Exception:
                ok = False
            self.store.put_document(NS, key, dict(base, sent=ok))
            sent_count += 1 if ok else 0
        return sent_count
    @staticmethod
    def _message(item, recipient):
        end_text = dt.datetime.fromtimestamp(item['end'], TZ).strftime('%Y-%m-%d %H:%M')
        plan_name = item.get('block_name') or item['block_id']
        record_name = str(item['record'].get('name') or item['record']['record_id'])
        buildings = '、'.join(_label(code) for code in item['codes'])
        return '\n'.join([
            '【计划收敛到期提醒】',
            '检修通告：' + record_name,
            '楼栋：' + buildings,
            '屏蔽计划：' + str(plan_name),
            '计划结束：' + end_text,
            '该检修仍未完成，请在屏蔽计划到期前及时完成收敛。',
        ])

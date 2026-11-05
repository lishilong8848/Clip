"""Deliver the four daily inspection reminders to the scheduled staff."""

import datetime as dt
import copy
import hashlib
import json
import logging
import math
import re
import time
import unicodedata
import uuid
from collections import Counter
from contextlib import closing, contextmanager

from .daily_inspection import CN, SOURCES, duty_period

NAMESPACE = 'daily_inspection_reminders'
SLOTS = {'白': (dt.time(9, 30), dt.time(12)), '夜': (dt.time(20), dt.time(0))}
RETRY_INTERVAL = dt.timedelta(minutes=5)
LOGGER = logging.getLogger(__name__)
SCHEDULE_KEY = '__schedule_v2__'
MAX_NEW_SLOTS = 128
MAX_DUE_SLOTS = 32
LEASE_SECONDS = 180


def slot_time(day, shift, number):
    date = dt.date.fromisoformat(day)
    if shift not in SLOTS or number not in (1, 2):
        raise ValueError('巡检提醒时点无效')
    date += dt.timedelta(days=int(shift == '夜' and number == 2))
    return dt.datetime.combine(date, SLOTS[shift][number - 1], CN)


def parse_slot(key):
    match = re.fullmatch(r'(\d{4}-\d{2}-\d{2}):(白|夜):([12])', str(key))
    if not match:
        return None
    day, shift, number = match.group(1), match.group(2), int(match.group(3))
    try:
        return day, shift, number, slot_time(day, shift, number)
    except ValueError:
        return None


def slots_between(start, end, limit=MAX_NEW_SLOTS):
    """Enumerate missed slots without advancing past an uncreated job."""
    first = dt.datetime.fromtimestamp(start, CN).date() - dt.timedelta(days=1)
    last = dt.datetime.fromtimestamp(end, CN).date()
    result = []
    date = first
    while date <= last:
        day = date.isoformat()
        for shift, number in (('白', 1), ('白', 2), ('夜', 1), ('夜', 2)):
            when = slot_time(day, shift, number)
            if start < when.timestamp() <= end:
                result.append((day, shift, number, when))
                if len(result) >= limit:
                    return result, False
        date += dt.timedelta(days=1)
    return result, True


@contextmanager
def _transaction(store):
    # Reminder documents and their scan cursor share one durable SQLite transaction.
    if store._document_table_for_namespace(NAMESPACE):
        raise RuntimeError('巡检提醒存储结构不兼容')
    with store._lock, closing(store._connect()) as conn:
        store._ensure_schema_locked(conn)
        conn.execute('BEGIN IMMEDIATE')
        try:
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise


def _document(conn, key):
    row = conn.execute('SELECT payload_json FROM json_documents WHERE namespace=? AND key=?',
                       (NAMESPACE, key)).fetchone()
    if row is None:
        return None
    value = json.loads(row['payload_json'])
    if not isinstance(value, dict):
        raise ValueError('巡检提醒任务损坏，未重新发送')
    return value


def _put(conn, key, value):
    conn.execute('INSERT OR REPLACE INTO json_documents(namespace,key,payload_json,updated_at) VALUES(?,?,?,?)',
                 (NAMESPACE, key, json.dumps(value, ensure_ascii=False), time.time()))


def schedule_missed_slots(store, current):
    """Persist every due slot and its watermark atomically, including across restarts."""
    now = current.timestamp()
    with _transaction(store) as conn:
        meta = _document(conn, SCHEDULE_KEY)
        if meta is None:
            keys = [row['key'] for row in conn.execute(
                'SELECT key FROM json_documents WHERE namespace=? AND key<>?', (NAMESPACE, SCHEDULE_KEY))]
            previous = [item[3].timestamp() for key in keys if (item := parse_slot(key)) and item[3].timestamp() <= now]
            current_slots = [slot_time(*item).timestamp() for item in due_slots(current)]
            # Older versions lack a watermark: use their first actual slot as the
            # start of coverage, and retain every existing per-person receipt.
            anchor = min(previous) if previous else min(current_slots, default=now)
            cursor = anchor - 0.001 if previous or current_slots else now
        else:
            cursor = meta.get('cursor')
            if meta.get('version') != 2 or not isinstance(cursor, (float, int)) or not math.isfinite(cursor):
                raise ValueError('巡检提醒调度记录损坏，未重建或覆盖旧任务')
            if cursor > now:
                return  # A clock rollback must not replay future reminders.
        slots, exhausted = slots_between(cursor, now)
        for day, shift, number, when in slots:
            key = f'{day}:{shift}:{number}'
            conn.execute('INSERT OR IGNORE INTO json_documents(namespace,key,payload_json,updated_at) VALUES(?,?,?,?)',
                         (NAMESPACE, key, json.dumps({'scheduled_at': when.timestamp()}), time.time()))
        new_cursor = now if exhausted else slots[-1][3].timestamp()
        _put(conn, SCHEDULE_KEY, {'version': 2, 'cursor': new_cursor})


def pending_slots(store, current):
    current_keys = [f'{day}:{shift}:{number}' for day, shift, number in due_slots(current)]
    priorities = ','.join('?' for _ in current_keys) or "''"
    with store._lock, closing(store._connect()) as conn:
        store._ensure_schema_locked(conn)
        rows = conn.execute(f'''SELECT key FROM json_documents
            WHERE namespace=? AND key<>? AND length(key)=14 AND substr(key,11,1)=':' AND substr(key,13,1)=':' AND json_valid(payload_json)
              AND COALESCE(json_extract(payload_json,'$.complete'),0)<>1
              AND COALESCE(json_extract(payload_json,'$.retry_at'),0)<=?
              AND COALESCE(json_extract(payload_json,'$._lease.until'),0)<=?
            ORDER BY CASE WHEN key IN ({priorities}) THEN 0 ELSE 1 END,
                     substr(key,1,10), CASE substr(key,12,1) WHEN '白' THEN 0 ELSE 1 END,
                     substr(key,14,1) LIMIT ?''',
            (NAMESPACE, SCHEDULE_KEY, current.timestamp(), time.time(), *current_keys, MAX_DUE_SLOTS)).fetchall()
    return [item for row in rows if (item := parse_slot(row['key'])) and item[3] <= current]


class LeaseLost(RuntimeError):
    pass


def claim_slot(store, key, current):
    with _transaction(store) as conn:
        state = _document(conn, key)
        if state is None or state.get('complete') or state.get('retry_at', 0) > current.timestamp():
            return None
        lease = state.get('_lease') or {}
        if lease.get('until', 0) > time.time():
            return None
        token = uuid.uuid4().hex
        state['_lease'] = {'token': token, 'until': time.time() + LEASE_SECONDS}
        state['retry_at'] = (current + RETRY_INTERVAL).timestamp()
        _put(conn, key, state)
        return state, token


def save_claimed_slot(store, key, state, token, *, release=False):
    with _transaction(store) as conn:
        current = _document(conn, key) or {}
        if (current.get('_lease') or {}).get('token') != token:
            raise LeaseLost('巡检提醒任务已由其他进程接管')
        if release:
            state.pop('_lease', None)
        else:
            state['_lease'] = {'token': token, 'until': time.time() + LEASE_SECONDS}
        _put(conn, key, state)


def freeze_roster(snapshot, day):
    if (not isinstance(snapshot, dict) or snapshot.get('date') != day or snapshot.get('complete') is not True
            or not isinstance(snapshot.get('people'), list) or not isinstance(snapshot.get('issues', []), list)):
        raise ValueError('原班次排班快照不完整')
    # Store only what is needed to preserve the original assignment, not secrets.
    person_keys = ('record_id', 'name', 'employee_no', 'open_id', 'scope', 'shift', 'duty_kind')
    issue_keys = ('record_id', 'scope', 'duty_kind')
    if not all(isinstance(item, dict) for item in snapshot['people'] + snapshot.get('issues', [])):
        raise ValueError('原班次排班快照格式无效')
    return {'date': day, 'complete': True, 'app_id': snapshot.get('app_id', ''),
            'observed_at': snapshot.get('observed_at'),
            'people': [{key: item.get(key) for key in person_keys if key in item} for item in snapshot['people']],
            'issues': [{key: item.get(key) for key in issue_keys if key in item} for item in snapshot.get('issues', [])]}


def shift_roster_key(day, shift):
    return f'__roster__:{day}:{shift}'


def shift_bounds(day, shift):
    date = dt.date.fromisoformat(day)
    if shift not in ('白', '夜'):
        raise ValueError('巡检班次无效')
    start = dt.datetime.combine(date, dt.time(9 if shift == '白' else 18), CN)
    end = (dt.datetime.combine(date, dt.time(18), CN) if shift == '白' else
           dt.datetime.combine(date + dt.timedelta(days=1), dt.time(9), CN))
    return start.timestamp(), end.timestamp()


def freeze_shift_roster(snapshot, day, shift):
    frozen = freeze_roster(snapshot, day)
    start, end = shift_bounds(day, shift)
    observed = frozen.get('observed_at')
    if (not isinstance(observed, (int, float)) or not math.isfinite(observed)
            or not start <= observed < end):
        raise ValueError('排班快照不是在原班次内读取的')
    return frozen


def remember_shift_roster(store, day, shift, snapshot, current):
    frozen = {**freeze_shift_roster(snapshot, day, shift), 'captured_at': current.timestamp()}
    start, end = shift_bounds(day, shift)
    if (not start <= frozen['captured_at'] < end
            or frozen['observed_at'] > frozen['captured_at'] + 120):
        raise ValueError('排班快照保存时间不在原班次内')
    key = shift_roster_key(day, shift)
    with _transaction(store) as conn:
        previous = _document(conn, key)
        previous_observed = previous.get('observed_at') if previous else None
        previous_captured = previous.get('captured_at') if previous else None
        previous_valid = (isinstance(previous_observed, (int, float)) and math.isfinite(previous_observed)
                          and isinstance(previous_captured, (int, float)) and math.isfinite(previous_captured)
                          and start <= previous_observed < end and start <= previous_captured < end
                          and previous_observed <= previous_captured + 120)
        if not previous_valid or previous_observed <= frozen['observed_at']:
            _put(conn, key, frozen)


def past_shift_roster(store, day, shift):
    """Never replace a missing old assignment with today's live roster."""
    start, end = shift_bounds(day, shift)
    saved = store.get_document(NAMESPACE, shift_roster_key(day, shift))
    if saved:
        captured = saved.get('captured_at')
        if (isinstance(captured, (int, float)) and math.isfinite(captured) and start <= captured < end):
            try:
                frozen = freeze_shift_roster(saved, day, shift)
                if frozen['observed_at'] <= captured + 120:
                    return frozen
            except ValueError:
                pass
    from .personnel_source import ROSTER_NAMESPACE, TABLE_ID
    saved = store.get_document(ROSTER_NAMESPACE, day) or {}
    observed = saved.get('observed_at')
    if (saved.get('source') != TABLE_ID or not isinstance(observed, (int, float))
            or not math.isfinite(observed) or not start <= observed < end):
        raise ValueError('未保存原班次排班快照，不能套用现班人员')
    return freeze_shift_roster(saved, day, shift)


def due_slots(current):
    current = current.astimezone(CN)
    day, shift = duty_period(current)
    duty_date = dt.date.fromisoformat(day)
    due = []
    for number, clock in enumerate(SLOTS[shift], 1):
        slot_date = duty_date + dt.timedelta(days=int(shift == '夜' and number == 2))
        if dt.datetime.combine(slot_date, clock, CN) <= current:
            due.append((day, shift, number))
    return due


def reminder_recipients(snapshot, day, shift):
    if snapshot.get('date') != day or snapshot.get('complete') is not True:
        raise ValueError('巡检排班快照不完整')
    identity_counts = Counter(person.get('open_id') for person in snapshot['people'])
    record_counts = Counter(person.get('record_id') for person in snapshot['people'] if person.get('record_id'))
    assignments = {}
    missing = sum(
        (issue.get('scope') in SOURCES and issue.get('duty_kind') in (shift, '待核对'))
        or (not issue.get('scope') and issue.get('duty_kind') == shift)
        for issue in snapshot.get('issues', [])
    )
    for person in snapshot['people']:
        scope = person.get('scope')
        if scope not in SOURCES or person.get('shift') != shift or person.get('duty_kind') == '长白':
            continue
        open_id = person.get('open_id') or ''
        record_id = person.get('record_id')
        if (not re.fullmatch(r'ou_[A-Za-z0-9]+', open_id) or identity_counts[open_id] != 1
                or not re.fullmatch(r'rec[A-Za-z0-9]+', str(record_id or ''))
                or record_counts[record_id] != 1):
            missing += 1
        else:
            assignments[open_id] = (scope, record_id, person)
    return assignments, missing


def current_roster_recipient(runtime, directory, record_id, original_id, snapshot_app_id, original_person):
    """Reconfirm the same personnel record; a changed account is never guessed by name."""
    from upload_event_module.services.robot_webhook import current_app_credentials
    from .feishu_delivery import current_recipient_open_id

    app_id = current_app_credentials()[0]
    if (not re.fullmatch(r'rec[A-Za-z0-9]+', str(record_id or '')) or not app_id
            or snapshot_app_id != app_id):
        raise ValueError('原班次人员或应用身份无法核验')
    snapshot = directory()
    if snapshot.get('schema') != 4 or snapshot.get('app_id') != app_id:
        raise ValueError('当前人员目录不可核验')
    people = snapshot.get('people') or {}
    person = people.get(record_id) if isinstance(people, dict) else None
    if (not isinstance(person, dict) or person.get('id', record_id) != record_id
            or person.get('inactive') or person.get('selectable') is not True):
        raise ValueError('原班次人员已停用或无法核验')
    normalize = lambda value: re.sub(r'\s+', '', unicodedata.normalize('NFKC', str(value or ''))).casefold()
    original_name = normalize(original_person.get('name')) if isinstance(original_person, dict) else ''
    original_number = normalize(original_person.get('employee_no')) if isinstance(original_person, dict) else ''
    if (not original_name or original_name != normalize(person.get('name'))
            or original_number and original_number != normalize(person.get('employee_no'))):
        raise ValueError('人员记录姓名或工号发生变化，不能自动替换账号')
    mapping = person.get('feishu_open_ids') or {}
    mapped = mapping.get(app_id) if isinstance(mapping, dict) else None
    ids = [mapped] if mapped else person.get('login_ids') or []
    if not isinstance(ids, list) or len(set(ids)) != 1:
        raise ValueError('原班次人员的当前飞书账号不唯一')
    candidate = ids[0]
    if not isinstance(candidate, str) or not re.fullmatch(r'ou_[A-Za-z0-9]+', candidate):
        raise ValueError('原班次人员的当前飞书账号无效')
    return current_recipient_open_id(runtime, candidate, app_id, record_id=record_id)


def reminder_card(scope, day, shift, number, url):
    return {
        'config': {'wide_screen_mode': True},
        'header': {'template': 'blue', 'title': {'tag': 'plain_text', 'content': f'{scope}楼 · 日常巡检'}},
        'elements': [
            {'tag': 'div', 'text': {'tag': 'lark_md',
                'content': f'**排班日期**  {day}\n**当班班次**  {shift}班\n**巡检轮次**  第 {number} 次'}},
            {'tag': 'div', 'text': {'tag': 'plain_text', 'content': '请核对设备参数；如发现不符可直接修改，完成所选专业后确认。'}},
            {'tag': 'action', 'actions': [{'tag': 'button', 'type': 'primary',
                'text': {'tag': 'plain_text', 'content': '查看并核对巡检'}, 'url': url}]},
        ],
    }


def process_reminders(store, roster, public_base, send, *, send_card=None, current=None,
                      stopping=lambda: False, resolve_recipient=None, reconcile_delivery=None, copy_recipient=None):
    current = current or dt.datetime.now(CN)
    current = current.replace(tzinfo=CN) if current.tzinfo is None else current.astimezone(CN)
    snapshots = {}
    if copy_recipient and not store.get_document(NAMESPACE,'__copy_enabled_at__'):
        store.put_document(NAMESPACE,'__copy_enabled_at__',{'timestamp':current.timestamp()})

    def snapshot(day):
        if day not in snapshots:
            snapshots[day] = roster(day)
        return snapshots[day]

    schedule_missed_slots(store, current)
    current_day, current_shift = duty_period(current)
    if not store.get_document(NAMESPACE, shift_roster_key(current_day, current_shift)):
        try:
            remember_shift_roster(store, current_day, current_shift, snapshot(current_day), current)
        except Exception as error:
            LOGGER.warning('日常巡检原班次排班暂未保存（%s）', type(error).__name__)
    for day, shift, number, _ in pending_slots(store, current):
        if stopping():
            return
        try:
            process_slot(store, snapshot, public_base, send, send_card or send, current, day, shift, number,
                         stopping, resolve_recipient=resolve_recipient, reconcile_delivery=reconcile_delivery, copy_recipient=copy_recipient)
        except Exception as error:
            LOGGER.warning('日常巡检提醒 %s %s班第%d次暂未完成（%s）', day, shift, number, type(error).__name__)


def delivery_route(store, open_id, message_uuid, target):
    key = hashlib.sha256(json.dumps(['open_id', open_id, message_uuid]).encode()).hexdigest()
    route = store.get_document('feishu_public_delivery', key) or {}
    if isinstance(target, dict) and target.get('kind') == 'card':
        kind, content = 'interactive', target.get('card')
    elif isinstance(target, str):
        kind, content = 'text', {'text': target}
    else:
        return {}
    fingerprint = hashlib.sha256(json.dumps([kind, content], sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    if (route.get('fingerprint') != fingerprint or route.get('receive_id') != open_id
            or route.get('receive_type') != 'open_id' or route.get('message_uuid') != message_uuid):
        return {}
    return route


def saved_receipt(store, open_id, message_uuid, target):
    """A durable sender receipt resolves a crash after delivery without a resend."""
    route = delivery_route(store, open_id, message_uuid, target)
    receipt = route.get('receipt') or {}
    if (route.get('status') == 'sent' and receipt.get('message_uuid') == message_uuid
            and receipt.get('sender_app_id') == route.get('app_id') and route.get('app_id')
            and re.fullmatch(r'om_[A-Za-z0-9]+', str(receipt.get('message_id') or ''))
            and re.fullmatch(r'oc_[A-Za-z0-9]+', str(receipt.get('chat_id') or ''))):
        return copy.deepcopy(receipt)
    return None


def process_slot(store, roster, public_base, send, send_card, current, day, shift, number, stopping,
                 *, resolve_recipient=None, reconcile_delivery=None, copy_recipient=None, job_key=None):
    key = job_key or f'{day}:{shift}:{number}'
    claimed = claim_slot(store, key, current)
    if not claimed:
        return
    state, token = claimed
    save = lambda: save_claimed_slot(store, key, state, token)
    try:
        if state.get('roster_snapshot'):
            snapshot = freeze_shift_roster(state['roster_snapshot'], day, shift)
        elif duty_period(current) == (day, shift):
            try:
                snapshot = freeze_shift_roster(roster(day), day, shift)
                if snapshot['observed_at'] > current.timestamp() + 120:
                    raise ValueError('排班快照读取时间尚未到达')
            except Exception:
                # A same-date cache read can still contain the previous shift.
                snapshot = past_shift_roster(store, day, shift)
            state['roster_snapshot'] = snapshot
            save()  # The original assignment must be durable before any send.
            remember_shift_roster(store, day, shift, snapshot, current)
        else:
            snapshot = past_shift_roster(store, day, shift)
            state['roster_snapshot'] = snapshot
            save()
        resolved_snapshot = copy.deepcopy(snapshot)
        if resolve_recipient:
            counts = Counter(person.get('open_id') for person in resolved_snapshot['people'] if person.get('open_id'))
            for person in resolved_snapshot['people']:
                if person.get('record_id') and (not person.get('open_id') or counts[person['open_id']] > 1):
                    try:
                        person['open_id'] = resolve_recipient(person['record_id'], person.get('open_id', ''), snapshot.get('app_id', ''), person)
                    except Exception:
                        person['open_id'] = ''  # Do not retain an identity that cannot be uniquely verified.
        recipients, missing = reminder_recipients(resolved_snapshot, day, shift)
        if missing:
            LOGGER.warning('日常巡检提醒 %s 有 %d 条值班记录的飞书身份无法核对', key, missing)
        sent = set(state.get('sent', []))
        records = state.setdefault('recipient_records', {})
        targets = state.setdefault('targets', {})
        inflight = state.setdefault('inflight_records', {})
        inflight_since = state.setdefault('inflight_since', {})
        target_uuids = state.setdefault('target_uuids', {})
        receipts_saved = state.setdefault('receipts', {})
        retargetable = set(state.get('retargetable', []))
        messages = {}
        failed = 0
        for original_id, (scope, record_id, original_person) in recipients.items():
            if stopping():
                return
            if original_id in sent or (record_id and records.get(record_id) in sent):
                continue
            old_id = inflight.get(record_id) if record_id else None
            if old_id:
                old_uuid = target_uuids.get(old_id) or str(uuid.uuid5(uuid.NAMESPACE_URL, f'clipflow:daily-inspection:{key}:{old_id}'))
                if old_id not in targets:
                    # A damaged/incomplete old task cannot be reconstructed safely:
                    # the content of the possibly delivered message is unknown.
                    failed += 1
                    continue
                previous = saved_receipt(store, old_id, old_uuid, targets.get(old_id))
                if previous:
                    sent.add(old_id)
                    records[record_id] = old_id
                    inflight.pop(record_id, None)
                    inflight_since.pop(record_id, None)
                    receipts_saved[record_id] = previous
                    state['sent'] = sorted(sent)
                    save()
                    continue
                old_target = targets.get(old_id)
                route = delivery_route(store, old_id, old_uuid, old_target)
                if route.get('status') == 'not_sent':
                    inflight.pop(record_id, None)
                    inflight_since.pop(record_id, None)
                    retargetable.add(old_id)
                    state['retargetable'] = sorted(retargetable)
                    save()
                    old_id = None
                else:
                    attempted_at = route.get('attempted_at') or inflight_since.get(record_id)
                    age = (current.timestamp() - attempted_at if isinstance(attempted_at, (int, float))
                           and math.isfinite(attempted_at) else None)
                    if age is None or age < -120 or age >= 3000:
                        save()
                        try:
                            recovered = reconcile_delivery(old_id, old_uuid, old_target, route) if reconcile_delivery else None
                        except Exception:
                            recovered = None
                        if recovered:
                            sent.add(old_id)
                            records[record_id] = old_id
                            inflight.pop(record_id, None)
                            inflight_since.pop(record_id, None)
                            receipts_saved[record_id] = recovered
                            state['sent'] = sorted(sent)
                            save()
                        else:
                            failed += 1  # After the dedupe window, absence of a receipt is not proof of failure.
                        continue
            try:
                open_id = resolve_recipient(record_id, original_id, snapshot.get('app_id', ''), original_person) if resolve_recipient else original_id
            except Exception:
                failed += 1
                continue
            if not re.fullmatch(r'ou_[A-Za-z0-9]+', str(open_id or '')):
                failed += 1
                continue
            if old_id and old_id != open_id:
                failed += 1  # Never redirect an unconfirmed send to another account.
                continue
            message_uuid = target_uuids.get(open_id) or str(uuid.uuid5(uuid.NAMESPACE_URL, f'clipflow:daily-inspection:{key}:{open_id}'))
            receipt = {}
            try:
                target = targets.get(open_id)
                if not target:
                    if scope not in messages:
                        url = public_base(scope=scope).rstrip('/') + '/daily-inspection'
                        messages[scope] = {'kind': 'card', 'scope': scope,
                                           'card': reminder_card(scope, day, shift, number, url)}
                    target = targets[open_id] = messages[scope]
                target_scope = target.get('scope') if isinstance(target, dict) else ''
                if isinstance(target, str):
                    match = re.match(r'【日常巡检提醒】([DE])楼 ', target)
                    target_scope = match.group(1) if match else ''
                if target_scope != scope:
                    if open_id not in retargetable:
                        failed += 1
                        continue
                    url = public_base(scope=scope).rstrip('/') + '/daily-inspection'
                    target = targets[open_id] = {'kind': 'card', 'scope': scope,
                        'card': reminder_card(scope, day, shift, number, url)}
                    revision = int(state.setdefault('target_revisions', {}).get(open_id, 0)) + 1
                    state['target_revisions'][open_id] = revision
                    message_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, f'clipflow:daily-inspection:{key}:{open_id}:revision:{revision}'))
                target_uuids[open_id] = message_uuid
                retargetable.discard(open_id)
                state['retargetable'] = sorted(retargetable)
                if record_id:
                    inflight[record_id] = open_id
                    inflight_since.setdefault(record_id, current.timestamp())
                save()  # Persist exact recipient, content and UUID before the network request.
                if isinstance(target, dict) and target.get('kind') == 'card':
                    ok, _, receipts = send_card(target['card'], [open_id], message_uuid=message_uuid)
                else:
                    ok, _, receipts = send(target, [open_id], message_uuid=message_uuid)
                receipt = next((row for row in receipts if row.get('open_id') == open_id), {})
                delivered = ok and receipt.get('ok') is True
                if resolve_recipient:
                    delivered = delivered and receipt.get('sender_app_id') == snapshot.get('app_id') and receipt.get('message_uuid') == message_uuid and all(
                        re.fullmatch(pattern, str(receipt.get(field) or '')) for field, pattern in
                        (('message_id', r'om_[A-Za-z0-9]+'), ('chat_id', r'oc_[A-Za-z0-9]+'),
                         ('sender_app_id', r'cli_[A-Za-z0-9]+')))
            except LeaseLost:
                raise
            except Exception:
                delivered = False
            if delivered:
                sent.add(open_id)
                retargetable.discard(open_id)
                state['retargetable'] = sorted(retargetable)
                state['sent'] = sorted(sent)
                if record_id:
                    inflight.pop(record_id, None)
                    inflight_since.pop(record_id, None)
                    records[record_id] = open_id
                    receipts_saved[record_id] = dict(receipt)
                save()
            else:
                if receipt.get('failure_kind') in {'not_sent', 'bot_unavailable'}:
                    if record_id:
                        inflight.pop(record_id, None)
                        inflight_since.pop(record_id, None)
                    retargetable.add(open_id)
                    state['retargetable'] = sorted(retargetable)
                    save()
                failed += 1
        enabled_at = (store.get_document(NAMESPACE,'__copy_enabled_at__') or {}).get('timestamp',current.timestamp())
        if copy_recipient and not job_key and slot_time(day,shift,number).timestamp() >= enabled_at:
            for building in sorted({scope for scope,_,_ in recipients.values()}):
                child_key = 'copy:'+key+':'+building
                child = store.get_document(NAMESPACE,child_key)
                try:
                    if child is None:
                        person = dict(copy_recipient(snapshot.get('app_id','')))
                        # If already an original recipient for this building, use that receipt.
                        original = recipients.get(person.get('open_id'))
                        if original and original[0] == building and person.get('open_id') in sent:
                            continue
                        person.update(scope=building,shift=shift,duty_kind=shift)
                        copied = {**snapshot,'people':[person],'issues':[]}
                        with _transaction(store) as conn:
                            if _document(conn,child_key) is None:
                                _put(conn,child_key,{'roster_snapshot':copied,'complete':False})
                    process_slot(store,roster,public_base,send,send_card,current,day,shift,number,stopping,
                        resolve_recipient=resolve_recipient,reconcile_delivery=reconcile_delivery,job_key=child_key)
                    if not (store.get_document(NAMESPACE,child_key) or {}).get('complete'):
                        failed += 1
                except Exception:
                    failed += 1
                    LOGGER.warning('日常巡检 %s %s楼抄送待重试',key,building)
        state.update(missing=missing, failed=failed, complete=not missing and not failed,
                     last_error='' if not missing and not failed else '原班次人员或飞书投递尚未全部确认')
        save()
        if failed:
            LOGGER.warning('日常巡检提醒 %s 有 %d 条未确认送达，将继续核验和重试', key, failed)
    except LeaseLost:
        raise
    except Exception:
        state['last_error'] = '原班次排班或投递暂不可核验，任务已保留'
        save()
        raise
    finally:
        try:
            save_claimed_slot(store, key, state, token, release=True)
        except LeaseLost:
            pass


def inspection_copy_person(directory, app_id):
    snapshot = directory()
    if snapshot.get('schema') != 4 or snapshot.get('app_id') != app_id:
        raise ValueError('抄送人员目录的应用身份未核验')
    matches = [(rid,p) for rid,p in (snapshot.get('people') or {}).items()
               if str(p.get('employee_no') or '').strip()=='92998' and p.get('name')=='李世龙']
    if len(matches)!=1:
        raise ValueError('李世龙的人员记录无法唯一核验')
    rid,person=matches[0]
    mapped=(person.get('feishu_open_ids') or {}).get(app_id)
    ids=[mapped] if mapped else person.get('login_ids') or []
    if len(set(ids))!=1 or not re.fullmatch(r'ou_[A-Za-z0-9]+',str(ids[0])):
        raise ValueError('抄送人员的当前飞书账号无法核验')
    return {'name':person['name'],'employee_no':person['employee_no'],'record_id':rid,'open_id':ids[0]}

"""Deliver the four daily inspection reminders to the scheduled staff."""

import datetime as dt
import logging
import re
import uuid
from collections import Counter

from .daily_inspection import CN, SOURCES, duty_period

NAMESPACE = 'daily_inspection_reminders'
SLOTS = {'白': (dt.time(9, 30), dt.time(12)), '夜': (dt.time(20), dt.time(0))}
RETRY_INTERVAL = dt.timedelta(minutes=5)
LOGGER = logging.getLogger(__name__)


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
        if not re.fullmatch(r'ou_[A-Za-z0-9]+', open_id) or identity_counts[open_id] != 1:
            missing += 1
        else:
            assignments[open_id] = (scope, person.get('record_id'))
    return assignments, missing


def process_reminders(store, roster, public_base, send, *, current=None, stopping=lambda: False):
    realtime = current is None
    current = current or dt.datetime.now(CN)
    snapshots = {}

    def snapshot(day):
        if day not in snapshots:
            snapshots[day] = roster(day)
        return snapshots[day]

    for day, shift, number in due_slots(current):
        def stop_slot():
            return stopping() or (realtime and duty_period(dt.datetime.now(CN)) != (day, shift))

        if stop_slot():
            return
        try:
            process_slot(store, snapshot, public_base, send, current, day, shift, number, stop_slot)
        except Exception as error:
            LOGGER.warning('日常巡检提醒 %s %s班第%d次暂未完成（%s）', day, shift, number, type(error).__name__)


def process_slot(store, roster, public_base, send, current, day, shift, number, stopping):
    key = f'{day}:{shift}:{number}'
    state = store.get_document(NAMESPACE, key) or {}
    if state.get('complete') or state.get('retry_at', 0) > current.timestamp():
        return
    state['retry_at'] = (current + RETRY_INTERVAL).timestamp()
    store.put_document(NAMESPACE, key, state)

    recipients, missing = reminder_recipients(roster(day), day, shift)
    if missing:
        LOGGER.warning('日常巡检提醒 %s 有 %d 条值班记录的飞书身份无法核对', key, missing)
    sent = set(state.get('sent', []))
    records = state.setdefault('recipient_records', {})
    targets = state.setdefault('targets', {})
    inflight = state.setdefault('inflight_records', {})
    messages = {}
    failed = 0
    for open_id, (scope, record_id) in recipients.items():
        if stopping():
            return
        if open_id in sent or (record_id and records.get(record_id) in sent):
            continue
        if record_id and inflight.get(record_id, open_id) != open_id:
            # A changed account cannot reuse the original recipient's delivery receipt.
            failed += 1
            continue
        message_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, f'clipflow:daily-inspection:{key}:{open_id}'))
        receipt = {}
        try:
            message = targets.get(open_id)
            if not message or not message.startswith(f'【日常巡检提醒】{scope}楼 '):
                if scope not in messages:
                    url = public_base(scope=scope).rstrip('/') + '/daily-inspection'
                    messages[scope] = (f'【日常巡检提醒】{scope}楼 {day} {shift}班 · 第 {number} 次\n'
                                       f'请查看本班巡检进度，并填写待完成的巡检项目。\n{url}')
                message = targets[open_id] = messages[scope]
            if record_id:
                inflight[record_id] = open_id
            store.put_document(NAMESPACE, key, state)
            ok, _, receipts = send(message, [open_id], message_uuid=message_uuid)
            receipt = next((row for row in receipts if row.get('open_id') == open_id), {})
            delivered = ok and receipt.get('ok') is True
        except Exception:
            delivered = False
        if delivered:
            sent.add(open_id)
            state['sent'] = sorted(sent)
            if record_id:
                inflight.pop(record_id, None)
                records[record_id] = open_id
            store.put_document(NAMESPACE, key, state)
        else:
            if record_id and receipt.get('failure_kind') in {'not_sent', 'bot_unavailable'}:
                inflight.pop(record_id, None)
                store.put_document(NAMESPACE, key, state)
            failed += 1
    state['missing'] = missing
    state['failed'] = failed
    state['complete'] = not missing and not failed
    store.put_document(NAMESPACE, key, state)
    if failed:
        LOGGER.warning('日常巡检提醒 %s 有 %d 条未确认送达，将在重试时段内继续尝试', key, failed)

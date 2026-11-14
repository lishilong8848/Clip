"""Immediate repair notices and one progress digest per building and Beijing day."""
from __future__ import annotations

import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Mapping
from urllib.parse import urlencode

NAMESPACE = 'repair_progress_notifications'
IN_PROGRESS = '维修中'
NOTICE_TITLE = '维修跟进通知'
REMINDER_TITLE = '维修进度跟进'
REMINDER_INTERVAL_SECONDS = 24 * 60 * 60
# A group that rejects cards must not be hammered every minute; environment gaps (no public
# base url) stay on the fast path because they recover as soon as configuration returns.
RETRY_BACKOFF_SECONDS = 15 * 60
# The common Feishu sender waits up to 60 seconds for a receipt. Recover a
# pending attempt close to midnight before opening a different daily UUID.
MIDNIGHT_RECOVERY_SECONDS = 120
MIDNIGHT_RETRY_WINDOW_SECONDS = 45 * 60
PRUNE_SECONDS = 45 * 24 * 60 * 60
BUTTON_LABEL = '查看维修记录'
PRUNE_PREFIX = 'record:'
BOOTSTRAP_KEY = 'meta:bootstrap'
DIGEST_PREFIX = 'digest:'
DIGEST_LIMIT = 8
BEIJING = timezone(timedelta(hours=8))


def building_label(code: str) -> str:
    return '110站' if code == '110' else code + '楼'


def _load(store, key: str) -> dict[str, Any]:
    value = store.get_document(NAMESPACE, key)
    return value if isinstance(value, dict) else {}


def _save(store, key: str, payload: dict[str, Any]) -> None:
    store.put_document(NAMESPACE, key, payload)


def detail_url(base_url: str, code: str, record_id: str, *, followup: bool = False) -> str:
    params = {'scope': code, 'record_id': record_id}
    if followup:
        params['workspace_tab'] = 'followups'
    query = urlencode(params)
    return base_url.rstrip('/') + '/repair-management?' + query


def _card(title: str, lines: Iterable[str], url: str) -> dict[str, Any]:
    elements: list[dict[str, Any]] = [
        {'tag': 'div', 'text': {'tag': 'lark_md', 'content': '\n'.join(lines)}},
    ]
    if url:
        elements.append({'tag': 'action', 'actions': [{
            'tag': 'button', 'type': 'primary',
            'text': {'tag': 'plain_text', 'content': BUTTON_LABEL}, 'url': url,
        }]})
    return {
        'config': {'wide_screen_mode': True, 'enable_forward': True},
        'header': {'title': {'tag': 'plain_text', 'content': title}, 'template': 'blue'},
        'elements': elements,
    }


def _repair_name(record: Mapping[str, Any]) -> str:
    keys = ('notification_title',) if 'notification_title' in record else ('repair_title', 'title')
    for key in keys:
        value = ' '.join(str(record.get(key) or '').split())
        if (value and value not in {'-', '—', '无', '未填写', '未命名维修项目', '未命名维修单'}
                and not re.fullmatch(r'(?:因)?(?:110站|[ABCDEH]楼)(?:进行)?(?:维修|检修)?(?:通告)?', value)):
            return re.sub(r'([\\`*_\[\]<>])', r'\\\1', value)
    return ''


def _format_time(value: Any) -> str:
    text = str(value or '').strip()
    try:
        if match := re.fullmatch(r'([0-9]{10}|[0-9]{13})(?:\.[0-9]+)?', text):
            seconds = float(text) / (1000 if len(match[1]) == 13 else 1)
            parsed = datetime.fromtimestamp(seconds, BEIJING)
        else:
            if len(text) < 16:
                return ''
            parsed = datetime.fromisoformat(text.replace('/', '-'))
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(BEIJING)
        return parsed.strftime('%Y-%m-%d %H:%M')
    except (ValueError, OverflowError, OSError):
        return ''


def notice_card(record: Mapping[str, Any], label: str, url: str) -> dict[str, Any]:
    fault = _format_time(record.get('fault_time'))
    return _card(NOTICE_TITLE, [
        f'楼栋：**{label}**',
        f'检修名称：**{_repair_name(record)}**',
        f'维修开始时间：{_format_time(record.get("repair_start_time"))}',
        *([f'故障发生时间：{fault}'] if fault else []),
    ], url)


def reminder_card(records: list[Mapping[str, Any]], code: str, base_url: str) -> dict[str, Any]:
    card = _card(REMINDER_TITLE, [f'**{building_label(code)} · 维修中 {len(records)} 项**'], '')
    for record in records[:DIGEST_LIMIT]:
        card['elements'].append({
            'tag': 'div',
            'text': {'tag': 'lark_md', 'content':
                     f'**{_repair_name(record)}**\n维修开始时间：{_format_time(record.get("repair_start_time"))}'},
            'extra': {'tag': 'button', 'type': 'default',
                      'text': {'tag': 'plain_text', 'content': '去灯塔跟进'},
                      'url': detail_url(base_url, code, str(record['record_id']), followup=True)},
        })
    if len(records) > DIGEST_LIMIT:
        card['elements'].append({'tag': 'div', 'text': {
            'tag': 'plain_text', 'content': f'另有 {len(records) - DIGEST_LIMIT} 项维修中',
        }})
    url = base_url.rstrip('/') + '/repair-management?' + urlencode({'scope': code, 'state': 'in_progress'})
    card['elements'].append({'tag': 'action', 'actions': [{
        'tag': 'button', 'type': 'primary', 'text': {'tag': 'plain_text', 'content': '查看本楼全部'}, 'url': url,
    }]})
    return card


def _send_card(send: Callable[..., Any], card: dict, chat_id: str, message_uuid: str) -> tuple[bool, str]:
    try:
        ok, message = send(card, chat_id, message_uuid)
    except Exception as exc:  # A throwing transport must not stop the remaining buildings.
        return False, str(exc) or '发送异常'
    return bool(ok), str(message or '')


def _send_notice(code, record, chat_id, base_url, send):
    record_id = str(record['record_id'])
    message_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, f'repair-progress:{record_id}:{code}:notice:'))
    return _send_card(send, notice_card(record, building_label(code), detail_url(base_url, code, record_id)),
                      chat_id, message_uuid)


def _legacy_reminder_days(store):
    days = set()
    for document in store.list_documents(NAMESPACE, key_prefix=PRUNE_PREFIX):
        for code, entry in ((document.get('payload') or {}).get('reminder') or {}).items():
            if isinstance(entry, dict) and entry.get('count') and entry.get('at'):
                days.add((datetime.fromtimestamp(entry['at'], BEIJING).date(), code))
    return days


def _send_building_digest(store, code, records, *, due, chat_id, base_url, send, clock, legacy_days, result):
    now = clock()
    current = datetime.fromtimestamp(now, BEIJING)
    day = current.date()
    yesterday = day - timedelta(days=1)
    key = f'{DIGEST_PREFIX}{day}:{code}'
    previous_key = f'{DIGEST_PREFIX}{yesterday}:{code}'
    job = _load(store, key)
    previous = _load(store, previous_key)
    if job.get('sent_at') is not None or (day, code) in legacy_days:
        return
    sent_at = previous.get('sent_at')
    if sent_at is not None and (
        datetime.fromtimestamp(sent_at, BEIJING).date() == day or now - sent_at < REMINDER_INTERVAL_SECONDS
    ):
        return
    midnight = current.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    previous_attempt = previous.get('attempted_at')
    if not job and sent_at is None and previous_attempt is not None and previous_attempt >= midnight - MIDNIGHT_RECOVERY_SECONDS:
        # An older unresolved send may have landed today. Keep today's slot
        # reserved, and let tomorrow use a fresh one after this retry window.
        if now - midnight >= MIDNIGHT_RETRY_WINDOW_SECONDS:
            return
        key, job = previous_key, previous
    if not job:
        if not due:
            return
        if not base_url:
            result['errors'].append(f'{building_label(code)} {REMINDER_TITLE}：未配置可供手机访问的灯塔地址')
            return
        records.sort(key=lambda row: (_format_time(row.get('repair_start_time')), str(row['record_id'])))
        job = {
            'card': reminder_card(records, code, base_url),
            'chat_id': chat_id,
            'message_uuid': str(uuid.uuid5(uuid.NAMESPACE_URL, f'repair-progress-digest:{day}:{code}')),
        }
    if job.get('chat_id') != chat_id:
        result['errors'].append(f'{building_label(code)} {REMINDER_TITLE}：原投递目标已变化，保留原任务待核对')
        return
    if 'attempted_at' in job and now - job['attempted_at'] < RETRY_BACKOFF_SECONDS:
        return
    job['attempted_at'] = now
    _save(store, key, job)
    ok, message = _send_card(send, job['card'], chat_id, job['message_uuid'])
    if ok:
        sent_at = clock()
        delivered_day = datetime.fromtimestamp(sent_at, BEIJING).date()
        delivered_key = f'{DIGEST_PREFIX}{delivered_day}:{code}'
        receipts = {key: {**job, 'sent_at': sent_at}}
        if delivered_key != key:
            receipts[delivered_key] = {'sent_at': sent_at, 'original_key': key}
        store.put_documents(NAMESPACE, receipts)
        result['reminded'] += 1
    else:
        result['errors'].append(f'{building_label(code)} {REMINDER_TITLE}：{message}')


def _send_digests(store, by_building, due, chat_ids, base_url, send, clock, result):
    legacy_days = _legacy_reminder_days(store)
    for code, records in by_building.items():
        lock_key = f'repair-progress-digest:{code}'
        acquired, owner = store.acquire_notice_operation_lock(
            lock_key, action='repair_progress_digest', ttl_seconds=180,
        )
        if not acquired:
            continue
        try:
            _send_building_digest(store, code, records, due=code in due, chat_id=chat_ids[code],
                                  base_url=base_url, send=send, clock=clock, legacy_days=legacy_days, result=result)
        finally:
            store.release_notice_operation_lock(lock_key, owner)
    day = datetime.fromtimestamp(clock(), BEIJING).date()
    cutoff = day - timedelta(days=PRUNE_SECONDS // 86400)
    for document in store.list_documents(NAMESPACE, key_prefix=DIGEST_PREFIX):
        key = document['key']
        if key.split(':', 2)[1] < cutoff.isoformat():
            store.delete_document(NAMESPACE, key)


def _reconcile_absent(store, seen: set[str], now: float, complete: bool) -> None:
    """Records missing from a complete scan have left 维修中.

    The scanner only reads the in-progress list, so without this a record that completes
    and is later reopened would look like it never left, and its re-entry would go unnoticed.
    Absence is only trusted when the whole list was paged through without error.
    """
    for document in store.list_documents(NAMESPACE, key_prefix=PRUNE_PREFIX):
        key = str(document.get('key') or '')
        record_id = key[len(PRUNE_PREFIX):]
        if record_id in seen:
            continue
        payload = document.get('payload')
        state = payload if isinstance(payload, dict) else {}
        if complete and str(state.get('workflow') or '') == IN_PROGRESS:
            state.update({'workflow': '', 'notice': {}})
            _save(store, key, state)
        elif now - float(state.get('seen_at') or 0) > PRUNE_SECONDS:
            store.delete_document(NAMESPACE, key)


def process(store, *, records: Iterable[Mapping[str, Any]], base_url: str,
            send: Callable[..., Any], chat_ids: Mapping[str, str], now: float | None = None,
            complete: bool = True) -> dict[str, Any]:
    fixed_now = now
    clock = time.time if fixed_now is None else lambda: fixed_now
    now = clock()
    result: dict[str, Any] = {'scanned': 0, 'noticed': 0, 'reminded': 0, 'errors': []}
    seen: set[str] = set()
    by_building: dict[str, list[Mapping[str, Any]]] = {}
    due: set[str] = set()
    # Only the first scan ever may treat "already 维修中" as history. After it, a record first
    # seen in 维修中 really did enter that state under our watch (created or started since),
    # so it must notify instead of being silently baselined.
    bootstrap = not _load(store, BOOTSTRAP_KEY)
    for record in records:
        record_id = str(record.get('record_id') or '')
        if not record_id or record_id in seen:
            continue
        seen.add(record_id)
        result['scanned'] += 1
        workflow = str(record.get('workflow') or '')
        codes = [code for code in dict.fromkeys(record.get('building_codes') or []) if code in chat_ids]
        key = PRUNE_PREFIX + record_id
        state = _load(store, key)
        if not state:
            suppress = bootstrap and workflow == IN_PROGRESS
            state = {
                'workflow': workflow if suppress else '',
                'seen_at': now,
                'notice': {code: now for code in codes} if suppress else {},
                'reminder': {code: {'count': 0, 'at': now} for code in codes} if suppress else {},
                'codes': list(codes),
            }
            previous = str(state['workflow'])
            if workflow != IN_PROGRESS:
                _save(store, key, state)
                continue
        else:
            previous = str(state.get('workflow') or '')
            state['seen_at'] = now
        if workflow != IN_PROGRESS:
            if previous == IN_PROGRESS:
                # Keep the legacy reminder receipt for upgrade-day deduplication.
                state['notice'] = {}
            state['workflow'] = workflow
            state['codes'] = list(codes)
            _save(store, key, state)
            continue
        notice = state.get('notice') if isinstance(state.get('notice'), dict) else {}
        reminder = state.get('reminder') if isinstance(state.get('reminder'), dict) else {}
        fail = state.get('fail') if isinstance(state.get('fail'), dict) else {}
        if not _repair_name(record) or not _format_time(record.get('repair_start_time')):
            state.update({'workflow': workflow, 'codes': list(codes)})
            _save(store, key, state)
            continue
        for code in codes:
            backoff_elapsed = now - float(fail.get(code) or 0) >= RETRY_BACKOFF_SECONDS
            # Absence from `notice` covers three cases at once: a fresh transition, a failed
            # earlier attempt (retry with the same uuid), and a building added by reassignment.
            if code not in notice and backoff_elapsed:
                ok, message = (_send_notice(code, record, chat_ids[code], base_url, send) if base_url
                               else (False, '未配置可供手机访问的灯塔地址'))
                if ok:
                    notice[code] = now
                    fail.pop(code, None)
                    result['noticed'] += 1
                else:
                    if base_url:
                        fail[code] = now
                    result['errors'].append(f'{record_id} {building_label(code)} {NOTICE_TITLE}：{message}')
            by_building.setdefault(code, []).append(record)
            entry = reminder.get(code) if isinstance(reminder.get(code), dict) else {}
            if code in notice and now - max(float(notice[code]), float(entry.get('at') or 0)) >= REMINDER_INTERVAL_SECONDS:
                due.add(code)
        state.update({'workflow': workflow, 'notice': notice, 'reminder': reminder,
                      'codes': list(codes), 'fail': fail})
        _save(store, key, state)
    if bootstrap and complete:
        # An incomplete first scan must not seal the baseline: records it never saw would
        # otherwise be treated as post-deploy transitions and notified in bulk.
        _save(store, BOOTSTRAP_KEY, {'at': now})
    if complete:
        _send_digests(store, by_building, due, chat_ids, base_url, send, clock, result)
    _reconcile_absent(store, seen, now, complete)
    return result


def run(store, service, *, now: float | None = None) -> dict[str, Any]:
    """Adapter used by the scheduler: page the in-progress list, resolve the public base url."""
    from upload_event_module.services.robot_webhook import send_interactive_to_chat_id

    from lan_bitable_template_portal.portal_service import BUILDING_CHAT_ID_MAP

    records: list[dict[str, Any]] = []
    offset = 0
    complete = True
    while offset <= 2000:
        try:
            board = service.get_repair_management_status(scope='ALL', state='in_progress', limit=200, offset=offset)
        except Exception:
            complete = False
            break
        page = board.get('records') or []
        records.extend(page)
        if not board.get('has_more'):
            if int(board.get('total') or 0) > len({row.get('record_id') for row in records}):
                complete = False
            break
        if not page:
            complete = False
            break
        offset += len(page)
    else:
        complete = False  # paging budget exhausted: the in-progress list was not fully seen
    try:
        base_url = service._critical_guard_public_base_url() or ''
    except Exception:
        base_url = ''
    return process(store, records=records, base_url=base_url,
                   send=lambda card, chat_id, message_uuid: send_interactive_to_chat_id(
                       card, chat_id, message_uuid=message_uuid),
                   chat_ids=BUILDING_CHAT_ID_MAP, now=now, complete=complete)

"""Feishu cards for repair records that enter, or stay in, 维修中.

Two cards, each routed only to the ops groups of the buildings the record belongs to:

- 维修跟进通知: once per building, when the workflow first becomes 维修中. Carries the
  building and repair names, 维修开始时间 and (when available) 故障发生时间, plus a jump
  button to the record detail.
- 维修进度跟进: repeatedly while the record is still 维修中, at most once per
  REMINDER_INTERVAL_SECONDS per building.

The scanner keeps its own per-record memory in ``json_documents`` so a restart neither
replays old transitions as fresh notices nor double-sends a reminder. Message UUIDs are
derived from the record/building/kind/round, so a crash between sending and recording
state is absorbed by Feishu-side deduplication instead of producing a second card.
"""
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
PRUNE_SECONDS = 45 * 24 * 60 * 60
BUTTON_LABEL = '查看维修记录'
PRUNE_PREFIX = 'record:'
BOOTSTRAP_KEY = 'meta:bootstrap'
BEIJING = timezone(timedelta(hours=8))


def building_label(code: str) -> str:
    return '110站' if code == '110' else code + '楼'


def _load(store, key: str) -> dict[str, Any]:
    value = store.get_document(NAMESPACE, key)
    return value if isinstance(value, dict) else {}


def _save(store, key: str, payload: dict[str, Any]) -> None:
    store.put_document(NAMESPACE, key, payload)


def detail_url(base_url: str, code: str, record_id: str) -> str:
    query = urlencode({'scope': code, 'record_id': record_id})
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


def reminder_card(record: Mapping[str, Any], label: str, url: str) -> dict[str, Any]:
    name = _repair_name(record)
    start = _format_time(record.get('repair_start_time'))
    return _card(REMINDER_TITLE, [f'**{label}**—**{name}**，请及时跟进维修进度。',
                                 f'维修开始时间：{start}'], url)


def _deliver(kind: str, code: str, record: Mapping[str, Any], chat_ids: Mapping[str, str],
             base_url: str, send: Callable[..., Any], bucket: str) -> tuple[bool, str, bool]:
    if not base_url:
        return False, '未配置可供手机访问的灯塔地址', True
    record_id = str(record.get('record_id') or '')
    identity = f'repair-progress:{record_id}:{code}:{kind}:{bucket}'
    message_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, identity))
    card = notice_card(record, building_label(code), detail_url(base_url, code, record_id)) \
        if kind == 'notice' else reminder_card(record, building_label(code), detail_url(base_url, code, record_id))
    try:
        ok, message = send(card, chat_ids[code], message_uuid)
    except Exception as exc:  # A throwing transport must not stop the remaining buildings.
        return False, str(exc) or '发送异常', False
    return bool(ok), str(message or ''), False


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
            state.update({'workflow': '', 'notice': {}, 'reminder': {}})
            _save(store, key, state)
        elif now - float(state.get('seen_at') or 0) > PRUNE_SECONDS:
            store.delete_document(NAMESPACE, key)


def process(store, *, records: Iterable[Mapping[str, Any]], base_url: str,
            send: Callable[..., Any], chat_ids: Mapping[str, str], now: float | None = None,
            complete: bool = True) -> dict[str, Any]:
    now = time.time() if now is None else now
    result: dict[str, Any] = {'scanned': 0, 'noticed': 0, 'reminded': 0, 'errors': []}
    seen: set[str] = set()
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
                # Leaving 维修中 re-arms both cards, so a reopened record notifies again.
                state['notice'] = {}
                state['reminder'] = {}
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
                ok, message, deferred = _deliver('notice', code, record, chat_ids, base_url, send, '')
                if ok:
                    notice[code] = now
                    fail.pop(code, None)
                    reminder[code] = {'count': 0, 'at': now}
                    result['noticed'] += 1
                else:
                    if not deferred:
                        fail[code] = now
                    result['errors'].append(f'{record_id} {building_label(code)} {NOTICE_TITLE}：{message}')
            entry = reminder.get(code) if isinstance(reminder.get(code), dict) else None
            entry = entry or {'count': 0, 'at': now}
            if (code in notice and now - float(entry.get('at') or 0) >= REMINDER_INTERVAL_SECONDS
                    and now - float(fail.get(code) or 0) >= RETRY_BACKOFF_SECONDS):
                bucket = str(int(entry.get('count') or 0) + 1)
                ok, message, deferred = _deliver('reminder', code, record, chat_ids, base_url, send, bucket)
                if ok:
                    reminder[code] = {'count': int(bucket), 'at': now}
                    fail.pop(code, None)
                    result['reminded'] += 1
                else:
                    if not deferred:
                        fail[code] = now
                    result['errors'].append(f'{record_id} {building_label(code)} {REMINDER_TITLE}：{message}')
            else:
                reminder[code] = entry
        state.update({'workflow': workflow, 'notice': notice, 'reminder': reminder,
                      'codes': list(codes), 'fail': fail})
        _save(store, key, state)
    if bootstrap and complete:
        # An incomplete first scan must not seal the baseline: records it never saw would
        # otherwise be treated as post-deploy transitions and notified in bulk.
        _save(store, BOOTSTRAP_KEY, {'at': now})
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
        if not board.get('has_more') or not page:
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

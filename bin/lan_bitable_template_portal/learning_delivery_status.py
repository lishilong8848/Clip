"""Local-only shift publication/delivery summary; the caller controls admin access."""
import re
from collections import Counter, defaultdict
from contextlib import closing

from . import learning as core
from .learning_reminders import ROSTER_VERSION, resolve_roster, shift_times
from .learning_shift_storage import current_directory


LABELS = {'success': '发布及发送成功', 'partial': '部分未完成', 'pending': '待发送',
          'not_due': '未到发布时间', 'disabled': '未启用', 'unverified': '结果待核验'}


def _state(result, state, label=None):
    result.update(state=state, label=label or LABELS[state])
    return result


def _issue(person, status, reason):
    return {'name': person.get('name', ''), 'scope': person.get('scope', ''),
            'status': status, 'reason': reason}


def _people(snapshot, shift):
    people = snapshot.get('people')
    if not isinstance(people, list):
        return [], '本日排班快照未就绪'
    reason = '' if snapshot.get('version') == ROSTER_VERSION else '排班快照版本待核对'
    selected = {}
    for row in people:
        if not isinstance(row, dict) or row.get('shift') not in ('白', '夜'):
            reason = '排班名单格式待核对'
            continue
        if row['shift'] != shift:
            continue
        name = row.get('name')
        name = name.strip() if isinstance(name, str) else ''
        scope = row.get('scope') if row.get('scope') in core.SCOPES else ''
        if not name or not scope:
            reason = '排班姓名或楼栋待核对'
        person = {'name': name, 'scope': scope, 'shift': shift,
                  'open_id': row.get('open_id') if isinstance(row.get('open_id'), str) else ''}
        key = (name, scope)
        if key in selected and selected[key]['open_id'] != person['open_id']:
            person['open_id'] = ''
        selected[key] = person
    return list(selected.values()), reason


def _delivery(notices, open_id, app_id):
    if not notices:
        return 'pending', '通知尚未生成'
    if len(notices) != 1:
        return 'unverified', '存在多条通知，发送结果待核对'
    notice = notices[0]
    deliveries = notice.get('delivery')
    receipts = [item for item in deliveries if isinstance(item, dict)
                and item.get('open_id') == open_id] if isinstance(deliveries, list) else []
    if notice.get('status') == 'sent':
        # A later confirmed success supersedes earlier errors, but never an old-app receipt.
        if any(item.get('ok') is True and item.get('sender_app_id') == app_id
               and isinstance(item.get('message_id'), str)
               and re.fullmatch(r'om_[A-Za-z0-9]+', item['message_id']) for item in receipts):
            return 'sent', ''
        return 'unverified', '缺少当前应用有效回执，发送结果待核验'
    status = notice.get('status')
    if status == 'expired':
        return 'failed', '通知已过期'
    if status == 'cancelled':
        return 'failed', '通知已取消'
    if status in {'skipped_roster', 'skipped_role'}:
        return 'unverified', '通知人员与当前排班或身份不一致'
    if any(item.get('failure_kind') == 'uncertain' for item in receipts):
        return 'unverified', '发送结果未确认，待核验回执'
    if status in {'pending', 'failed'}:
        failures = [item for item in receipts if item.get('ok') is False]
        if failures or notice.get('error') or status == 'failed':
            reasons = {'bot_unavailable': '机器人对该人员不可用',
                       'blocked': '发送受限，待核对配置',
                       'agent_required': '消息发送通道未就绪'}
            reason = next((reasons[item.get('failure_kind')] for item in failures
                           if item.get('failure_kind') in reasons), '发送失败，等待后台重试')
            return 'failed', reason
        return 'pending', '通知待发送'
    return 'unverified', '通知状态待核对'


def _shift(shift, publish_time, current, enabled, snapshot, publication, directory, ready, app_id, notices):
    people, roster_reason = _people(snapshot, shift)
    result = {'shift': shift, 'publish_time': publish_time, 'expected': len(people),
              'sent': 0, 'failed': 0, 'pending': len(people), 'issues': []}
    if not enabled:
        return _state(result, 'disabled')
    if current.strftime('%H:%M') < publish_time:
        return _state(result, 'not_due')
    result['issues'].extend(_issue(issue, 'unverified', issue.get('reason') or '排班资料待核对')
                            for issue in snapshot.get('issues', [])
                            if issue.get('duty_kind') not in ('白', '夜') or issue['duty_kind'] == shift)
    if roster_reason or not people or not ready:
        reason = roster_reason or ('本班没有排班人员' if not people else '当前应用人员目录未就绪')
        result['issues'].extend(_issue(person, 'unverified', reason) for person in people or ([] if result['issues'] else [{}]))
        return _state(result, 'unverified')

    resolved = []
    for person in people:
        try:
            matches, warnings = resolve_roster([person], directory)
            resolved.append(matches[0]['open_id'] if len(matches) == 1 and not warnings else '')
        except (KeyError, TypeError, ValueError, AttributeError):
            resolved.append('')
    counts = Counter(resolved)
    for person, open_id in zip(people, resolved):
        if not open_id or counts[open_id] != 1:
            status, reason = 'unverified', '排班人员未唯一匹配当前应用身份'
        else:
            status, reason = _delivery(notices.get((shift, open_id), []), open_id, app_id)
        if status == 'sent':
            result['sent'] += 1
        else:
            if status == 'failed':
                result['failed'] += 1
            result['issues'].append(_issue(person, status, reason))
    result['pending'] = result['expected'] - result['sent'] - result['failed']
    published = bool(publication and publication.get('date') == current.date().isoformat()
                     and publication.get('_dirty') is False)
    if not published:
        reason = '发布记录尚未生成' if not publication else '发布记录尚未确认同步完成'
        result['issues'].append(_issue({}, 'pending', reason))
    if any(issue['status'] == 'unverified' for issue in result['issues']):
        return _state(result, 'unverified')
    if result['failed'] or 0 < result['sent'] < result['expected']:
        return _state(result, 'partial')
    if not published:
        return _state(result, 'pending', '发布待完成')
    return _state(result, 'success' if result['sent'] == result['expected'] else 'pending')


def report(service, current=None):
    """Read local evidence only; pending includes recipients whose result is unverified."""
    current = current if current is not None else core.now()
    current = current.astimezone(core.TZ) if current.tzinfo else current.replace(tzinfo=core.TZ)
    date = current.date().isoformat()
    settings = service.settings()
    times = shift_times(settings)
    with closing(service._connect()) as conn:
        conn.execute('BEGIN')
        snapshot = service._get('local', 'shift_roster:' + date, conn) or {}
        if snapshot.get('date') != date:
            snapshot = service._get('local', 'shift_roster', conn) or {}
        if snapshot.get('date') != date:
            snapshot = {}
        publication = service._get('publication', service._publication_key(date), conn)
        notifications = service._documents('notification', start=date, end=date, conn=conn)
    notices = defaultdict(list)
    for item in notifications:
        if item.get('kind') == 'shift' and item.get('date') == date:
            notices[item.get('shift'), item.get('open_id')].append(item)
    enabled = settings.get('enabled') is True
    directory, ready, app_id = ({}, False, '')
    if enabled and current.strftime('%H:%M') >= min(times.values()):
        directory, ready, app_id = current_directory(service)
    return {'date': date, 'shifts': [
        _shift(shift, times[shift], current, enabled, snapshot, publication,
               directory, ready and bool(app_id), app_id, notices) for shift in ('白', '夜')]}

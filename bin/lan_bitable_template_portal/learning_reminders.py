"""Read today's duty view once and enqueue durable, per-person reminders."""
import datetime as dt
import re
import time
from collections import Counter, defaultdict
from uuid import NAMESPACE_URL, uuid5

from .personnel_source import APP_TOKEN, TABLE_ID
VIEW_ID = ''
TZ = dt.timezone(dt.timedelta(hours=8))
SHIFT_TIMES = {'夜': '20:00', '白': '13:00'}
ROSTER_VERSION = 6
ROSTER_POLICY_VERSION = 2
APP_ACCESS_URL = 'https://applink.feishu.cn/T9aTCORtUJLb'


def shift_times(settings):
    return {'白': settings.get('publish_time') or SHIFT_TIMES['白'],
            '夜': settings.get('night_publish_time') or SHIFT_TIMES['夜']}


def duty_recipients(service):
    """Eligible personal recipients from the local HR snapshot; None means not ready."""
    from .account_policy import categorize_position
    from .learning_personal import _login_directory, _hr_active, _hr_learning_scopes
    try:
        directory, valid = _login_directory(service)
    except Exception:
        return None
    if not valid:
        return None
    from .portal_service import BUILDING_OPEN_ID_MAP
    owners = Counter(oid for row in directory.values() for oid in set(row.get('login_ids') or []))
    shared = set(BUILDING_OPEN_ID_MAP.values())
    targets = set()
    for row in directory.values():
        category = categorize_position(row.get('position'))
        if category == 'other':
            category = categorize_position(' '.join(row.get('assignments') or []))
        if not _hr_active(row) or not _hr_learning_scopes(row) or category not in {'operator', 'shift_leader'}:
            continue
        targets.update(oid for oid in row.get('login_ids') or []
                       if isinstance(oid, str) and re.fullmatch(r'ou_[A-Za-z0-9]+', oid)
                       and owners[oid] == 1 and oid not in shared)
    return targets


def excluded_long_day(person):
    # 岗位 is the source of this daily-learning exception; 体系岗位=实习生
    # includes other jobs (e.g. FOC) and must not broaden the recipient set.
    return (person.get('duty_kind') == '长白' or person.get('group') == '长白'
            or person.get('shift') == '长白') and person.get('position') != '运维实习生'


def read_roster(service, day, *, scopes=tuple('ABCDEH'), include_long_day=False, with_status=False):
    from .personnel_source import daily_roster
    snapshot = daily_roster(service, day)
    people = [dict(person) for person in snapshot['people'] if person['scope'] in scopes
              and (include_long_day or not excluded_long_day(person))]
    issues = [dict(issue) for issue in snapshot.get('issues', []) if (not issue.get('scope') or issue['scope'] in scopes)
              and (include_long_day or not excluded_long_day(issue))]
    return {'people': people, 'issues': issues} if with_status else people


def resolve_roster(people, directory, *, allowed_scopes=tuple('ABCDEH')):
    """Use exact names and unique live identities, never substitute a linked stranger."""
    from .learning_personal import _hr_active
    from .portal_service import BUILDING_OPEN_ID_MAP
    from .feishu_identity import TARGET_DUTY_OPEN_IDS

    owners = Counter(oid for row in directory.values() for oid in set(row.get('login_ids') or []))
    shared = set(BUILDING_OPEN_ID_MAP.values()) | set(TARGET_DUTY_OPEN_IDS.values())
    names = defaultdict(list)
    for row in directory.values():
        names[str(row.get('name') or '').strip()].append(row)
    resolved, warnings = {}, []
    for person in people:
        if person.get('employee_linked') is False:
            continue
        record_id = person.get('record_id')
        if record_id:
            candidates = [row for rid, row in directory.items()
                          if rid == record_id or row.get('id') == record_id]
            # An explicitly cleared employee link is not a reason to try a
            # namesake or a historical account. The learner record stays intact.
            if len(candidates) == 1 and not candidates[0].get('login_ids'):
                continue
        else:
            candidates = names[person['name']]
            linked = [row for row in candidates if person['open_id'] and person['open_id'] in (row.get('login_ids') or [])]
            candidates = linked or candidates
        candidates = [row for row in candidates
                      if str(row.get('name') or '').strip() == person['name']
                      and _hr_active(row) and person['scope'] in allowed_scopes
                      and list(dict.fromkeys(s for s in row.get('scopes') or [] if s)) == [person['scope']]]
        ids = []
        if len(candidates) == 1:
            ids = [oid for oid in candidates[0].get('login_ids') or []
                   if isinstance(oid, str) and re.fullmatch(r'ou_[A-Za-z0-9]+', oid)
                   and owners[oid] == 1 and oid not in shared]
        open_id = (ids[0] if len(ids) == 1 else '') if record_id else (
            person['open_id'] if person['open_id'] in ids else ids[0] if len(ids) == 1 else '')
        if not open_id:
            warnings.append(f"{person['scope']}楼{person['shift']}班 {person['name']}：未找到唯一的在岗人员飞书身份")
            continue
        resolved[(person['shift'], open_id)] = {**person, 'open_id': open_id}
    return list(resolved.values()), warnings


def roster_snapshot(service, day):
    snapshot = service._get('local', 'shift_roster') or {}
    if snapshot.get('date') != day:
        snapshot = service._get('local', 'shift_roster:' + day) or {}
    if (snapshot.get('date') == day and day < dt.datetime.now(TZ).date().isoformat()
            and snapshot.get('version') in {3, 4, 5, ROSTER_VERSION} and isinstance(snapshot.get('people'), list)):
        return snapshot
    if (snapshot.get('date') != day or snapshot.get('version') != ROSTER_VERSION
            or snapshot.get('policy_version') != ROSTER_POLICY_VERSION
            or time.time() - float(snapshot.get('observed_at') or 0) >= 300):
        retry = service._get('local', 'shift_roster_retry:' + day) or service._get('local', 'shift_roster_retry') or {}
        if (snapshot.get('version') in (None, ROSTER_VERSION)
                and retry.get('policy_version') == ROSTER_POLICY_VERSION
                and retry.get('date') == day and retry.get('retry_at', 0) > time.time()):
            return
        try:
            value = service._shift_roster_reader(day)
            people = value.get('people') if isinstance(value, dict) else value
            issues = value.get('issues', []) if isinstance(value, dict) else []
            if not isinstance(people, list) or not isinstance(issues, list):
                raise ValueError('invalid roster snapshot')
        except Exception:
            with service.transaction() as conn:
                retry = {'date': day, 'retry_at': time.time() + 300, 'policy_version': ROSTER_POLICY_VERSION}
                service._put('local', 'shift_roster_retry', retry, conn, False)
                service._put('local', 'shift_roster_retry:' + day, retry, conn, False)
            service._last_error = '学练排班名单读取失败，5分钟后重试；原楼栋提醒不受影响。'
            return
        snapshot = {'date': day, 'people': people, 'issues': issues, 'version': ROSTER_VERSION,
                    'policy_version': ROSTER_POLICY_VERSION, 'observed_at': time.time()}
        with service.transaction() as conn:
            service._put('local', 'shift_roster', snapshot, conn, False)
            service._put('local', 'shift_roster:' + day, snapshot, conn, False)
    return snapshot


def shift_learners(service, day, shift):
    from .learning_personal import _login_directory, _hr_learning_scopes, resolve_self

    if not service._shift_roster_reader:
        return None
    try:
        directory, valid = _login_directory(service)
    except Exception:
        return None
    if not valid:
        return None
    snapshot = roster_snapshot(service, day)
    if snapshot is None:
        return None
    roster, warnings = resolve_roster(snapshot['people'], directory)
    if warnings:
        service._last_error = '学练排班待核对：' + '；'.join(warnings)
    learners = []
    for duty in roster:
        if duty['shift'] != shift:
            continue
        rows = [rid for rid, row in directory.items() if str(row.get('name') or '').strip() == duty['name']
                and _hr_learning_scopes(row) == [duty['scope']] and duty['open_id'] in (row.get('login_ids') or [])]
        if len(rows) == 1:
            resolved = resolve_self(service, 'personnel_' + rows[0])
            if resolved.get('person_id'):
                learners.append((duty, resolved['person']))
    return learners


def enqueue_shift_reminders(service, current):
    from .learning_personal import _login_directory
    from .learning_schedule import windows

    times = shift_times(service.settings())
    if not service._shift_roster_reader or current.strftime('%H:%M') < min(times.values()):
        return
    try:
        directory, valid = _login_directory(service)
    except Exception:
        directory, valid = {}, False
    if not valid:
        service._last_error = '本地人员目录暂未就绪，个人学练提醒等待核对后发送。'
        return
    day = current.date().isoformat()
    snapshot = roster_snapshot(service, day)
    if snapshot is None:
        return
    people, warnings = resolve_roster(snapshot['people'], directory)
    warnings.extend((str(issue.get('name') or '未命名人员') + '：' + str(issue.get('reason') or '排班待核对'))
                    for issue in snapshot.get('issues', []))
    if warnings:
        service._last_error = '学练排班待核对：' + '；'.join(warnings)
    with service.transaction() as conn:
        for person in people:
            if current.strftime('%H:%M') < times[person['shift']]:
                continue
            expires_at = windows(day, person['shift'])[1].timestamp() if person.get('duty_kind') == '长白' else None
            if expires_at is not None and current.timestamp() >= expires_at:
                continue
            key = f"shift:{day}:{person['shift']}:{person['open_id']}"
            existing = service._get('notification', key, conn)
            if existing and existing.get('status') not in {'skipped_role', 'skipped_roster'}:
                continue
            service._put('notification', key, {'id': key, 'kind': 'shift', 'scope': '', 'date': day,
                'open_id': person['open_id'], 'shift': person['shift'], 'status': 'pending',
                **({'expires_at': expires_at} if expires_at is not None else {}),
                'message': f"【画像学练】{day} {person['shift']}班\n请登录本人的灯塔账号，进入今日学练完成答题。"}, conn)
    return {(person['shift'], person['open_id']) for person in people}


def notification_jobs(service, current, assignments):
    """Complete personal attempts first, then aggregate confirmed access failures per building."""
    yield from (job for job in service._all('notification') if job.get('kind') != 'app_access')
    from .learning_personal import _login_directory
    try:
        directory, ready = _login_directory(service)
    except Exception:
        directory, ready = {}, False
    if ready and assignments is not None:
        day = current.date().isoformat()
        notices = service._all('notification')
        reported = {member for job in notices if job.get('kind') == 'app_access' and job.get('date') == day
                    for member in job.get('members', [])}
        by_id = defaultdict(list)
        for person in directory.values():
            for oid in person.get('login_ids', []):
                by_id[oid].append(person)
        groups = defaultdict(list)
        for job in notices:
            oid, shift = job.get('open_id'), job.get('shift')
            if (job.get('kind') != 'shift' or job.get('date') != day or job.get('status') != 'pending'
                    or job['id'] in reported or (shift, oid) not in assignments or len(by_id[oid]) != 1):
                continue
            person = by_id[oid][0]
            nature = str(person.get('account_nature') or '').strip().upper()
            scopes = list(dict.fromkeys(person.get('scopes') or []))
            if not nature or nature == 'VNET' or len(scopes) != 1 or scopes[0] not in tuple('ABCDEH'):
                continue
            if not any(isinstance(d, dict) and d.get('open_id') == oid and d.get('ok') is False
                       and (str(d.get('code')) == '230013' or d.get('failure_kind') == 'bot_unavailable')
                       for d in job.get('delivery') or []):
                continue
            groups[scopes[0], shift].append((job['id'], person.get('name') or '未命名人员'))
        with service.transaction() as conn:
            for (scope, shift), members in groups.items():
                identities = sorted(member[0] for member in members)
                identity = 'app-access:' + str(uuid5(NAMESPACE_URL, '|'.join(identities)))
                if service._get('notification', identity, conn):
                    continue
                names = '、'.join(dict.fromkeys(member[1] for member in members))
                message = (f'【学练应用授权待处理】{day} {scope}楼{shift}班\n'
                           f'以下非VNET人员的个人答题提醒被飞书拒绝投递：{names}。\n'
                           '请值班人员联系本人申请使用小塔飞书应用；授权生效后，本日待发送提醒将自动重试。'
                           '其他人员正常发送。\n申请链接：' + APP_ACCESS_URL)
                service._put('notification', identity, {'id': identity, 'kind': 'app_access', 'date': day,
                    'scope': scope, 'shift': shift, 'members': identities, 'status': 'pending', 'message': message}, conn)
    yield from (job for job in service._all('notification') if job.get('kind') == 'app_access')


def reminder_card(message, url, *, button='打开画像学练'):
    return {'config': {'wide_screen_mode': True},
            'header': {'title': {'tag': 'plain_text', 'content': '画像学练'}, 'template': 'blue'},
            'elements': [{'tag': 'div', 'text': {'tag': 'plain_text', 'content': message}},
                         {'tag': 'action', 'actions': [{'tag': 'button', 'type': 'primary',
                           'text': {'tag': 'plain_text', 'content': button}, 'url': url}]}]}


def send_reminder(recipient, card, identity):
    from .portal_service import BUILDING_OPEN_ID_MAP, external_real_write_guard
    from upload_event_module.services.robot_webhook import send_interactive_to_open_ids, send_interactive_to_chat_id
    open_id = BUILDING_OPEN_ID_MAP.get(recipient, recipient)
    if not re.fullmatch(r'(?:ou|oc)_[A-Za-z0-9]+', open_id):
        raise ValueError('学习提醒接收人无效。')
    guard = external_real_write_guard()
    if guard.get('mock_external'):
        return True, 'mock external send skipped', []
    if not guard.get('real_write_allowed'):
        return False, guard.get('reason') or '真实外部写入未确认。', []
    message_uuid = str(uuid5(NAMESPACE_URL, identity))
    if open_id.startswith('oc_'):
        return send_interactive_to_chat_id(card, open_id, message_uuid=message_uuid)
    return send_interactive_to_open_ids(card, [open_id], message_uuid=message_uuid)

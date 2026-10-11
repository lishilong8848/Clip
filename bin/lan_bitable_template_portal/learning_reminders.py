"""Read today's duty view once and enqueue durable, per-person reminders."""
import datetime as dt
import re
import time
from uuid import NAMESPACE_URL, uuid5

APP_TOKEN = 'KdCZbSL5CaTErKsFwFucddA5nYd'
TABLE_ID = 'tblRV9KeWFh9xCkm'
VIEW_ID = 'vew6jLlGzq'
TZ = dt.timezone(dt.timedelta(hours=8))
SHIFT_TIMES = {'夜': '08:00', '白': '13:00'}


def read_roster(service, day):
    people, cursor, seen = {}, '', set()
    for _ in range(20):
        result = service._request_json('records', app_token=APP_TOKEN, table_id=TABLE_ID,
            params={'page_size': 500, 'view_id': VIEW_ID, 'user_id_type': 'open_id',
                    **({'page_token': cursor} if cursor else {})})
        data = result.get('data') or {}
        if result.get('code') != 0 or not isinstance(data.get('items'), list) or type(data.get('has_more')) is not bool:
            raise ValueError('学练排班视图读取不完整，未使用部分名单发送。')
        for row in data['items']:
            fields = row.get('fields') or {}
            if fields.get('班组') in {'长白', '110站'} or fields.get('班次') not in SHIFT_TIMES:
                continue
            value = fields.get('排班日期')
            try:
                roster_day = dt.datetime.fromtimestamp(float(value) / 1000, TZ).date().isoformat()
            except (TypeError, ValueError, OverflowError, OSError):
                raise ValueError('学练排班视图中排班日期无效，未发送。') from None
            if roster_day != day:
                continue
            users = fields.get('人员') or {}
            users = users.get('users', []) if isinstance(users, dict) else users
            if not isinstance(users, list):
                raise ValueError('学练排班视图中人员字段格式无效。')
            for user in users:
                open_id = str(user.get('id') or user.get('open_id') or '') if isinstance(user, dict) else ''
                if not re.fullmatch(r'ou_[A-Za-z0-9]+', open_id):
                    raise ValueError('学练排班人员缺少有效 OpenID，未使用不完整名单发送。')
                shift = fields['班次']
                people[(shift, open_id)] = {'open_id': open_id, 'shift': shift,
                                           'name': str(user.get('name') or '')}
        if not data['has_more']:
            return list(people.values())
        cursor = data.get('page_token')
        if not isinstance(cursor, str) or not cursor or cursor in seen:
            break
        seen.add(cursor)
    raise ValueError('学练排班视图分页未完成，未发送。')


def enqueue_shift_reminders(service, current):
    if not service._shift_roster_reader or current.strftime('%H:%M') < '08:00':
        return
    day = current.date().isoformat()
    snapshot = service._get('local', 'shift_roster') or {}
    if snapshot.get('date') != day:
        retry = service._get('local', 'shift_roster_retry') or {}
        if retry.get('date') == day and retry.get('retry_at', 0) > time.time():
            return
        try:
            people = service._shift_roster_reader(day)
        except Exception:
            with service.transaction() as conn:
                service._put('local', 'shift_roster_retry', {'date': day, 'retry_at': time.time() + 300}, conn, False)
            service._last_error = '学练排班名单读取失败，5分钟后重试；原楼栋提醒不受影响。'
            return
        snapshot = {'date': day, 'people': people}
        with service.transaction() as conn:
            service._put('local', 'shift_roster', snapshot, conn, False)
    with service.transaction() as conn:
        for person in snapshot['people']:
            if current.strftime('%H:%M') < SHIFT_TIMES[person['shift']]:
                continue
            key = f"shift:{day}:{person['shift']}:{person['open_id']}"
            if service._get('notification', key, conn):
                continue
            service._put('notification', key, {'id': key, 'kind': 'shift', 'scope': '', 'date': day,
                'open_id': person['open_id'], 'shift': person['shift'], 'status': 'pending',
                'message': f"【画像学练】{day} {person['shift']}班\n请登录本人的灯塔账号，进入今日学练完成答题。"}, conn)


def reminder_card(message, url):
    return {'config': {'wide_screen_mode': True},
            'header': {'title': {'tag': 'plain_text', 'content': '画像学练'}, 'template': 'blue'},
            'elements': [{'tag': 'div', 'text': {'tag': 'plain_text', 'content': message}},
                         {'tag': 'action', 'actions': [{'tag': 'button', 'type': 'primary',
                           'text': {'tag': 'plain_text', 'content': '打开画像学练'}, 'url': url}]}]}


def send_reminder(recipient, card, identity):
    from .portal_service import BUILDING_OPEN_ID_MAP, external_real_write_guard
    from upload_event_module.services.robot_webhook import send_interactive_to_open_ids
    open_id = BUILDING_OPEN_ID_MAP.get(recipient, recipient)
    if not re.fullmatch(r'ou_[A-Za-z0-9]+', open_id):
        raise ValueError('学习提醒接收人无效。')
    guard = external_real_write_guard()
    if guard.get('mock_external'):
        return True, 'mock external send skipped', []
    if not guard.get('real_write_allowed'):
        return False, guard.get('reason') or '真实外部写入未确认。', []
    return send_interactive_to_open_ids(card, [open_id], message_uuid=str(uuid5(NAMESPACE_URL, identity)))

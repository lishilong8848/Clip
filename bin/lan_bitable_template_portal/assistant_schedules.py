"""Account-owned assistant schedules in the portal's existing durable document store."""
from __future__ import annotations

import calendar
import copy
import datetime as dt
import hashlib
import json
import math
import re
import time
import unicodedata
import uuid
from contextlib import closing, contextmanager

from openclaw_service.assistant.lighthouse_ai import AssistantError, CONTACT, private_identifier, safe_text

NAMESPACE = 'assistant_schedules'
TZ = dt.timezone(dt.timedelta(hours=8))
LEASE_SECONDS = 120


def local_time(value):
    if not isinstance(value, str):
        raise AssistantError('请提供明确的定时时间。')
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        raise AssistantError('定时时间无效，请说明日期和时刻。') from None
    if parsed.tzinfo is None:
        raise AssistantError('定时时间须包含时区；默认使用北京时间。')
    return parsed.astimezone(TZ)


def validate_spec(value, now):
    if not isinstance(value, dict) or set(value) - {'title', 'instruction', 'kind', 'start_at', 'repeat', 'interval_minutes', 'weekdays', 'day_of_month', 'until'}:
        raise AssistantError('定时任务参数无效。')
    result = copy.deepcopy(value)
    for name, limit in [('title', 120), ('instruction', 2000)]:
        text = result.get(name)
        clean = unicodedata.normalize('NFKC', text) if isinstance(text, str) else ''
        if (not clean.strip() or len(clean) > limit or private_identifier(clean)
                or CONTACT.search(clean) or safe_text(clean, limit=None) != clean
                or any(unicodedata.category(char) in {'Cf', 'Cc'} and char not in '\n\r\t' for char in clean)):
            raise AssistantError('定时任务内容无效或包含不可保存的敏感信息。')
        result[name] = text.strip()
    if result.get('kind') not in {'reminder', 'task'} or result.get('repeat') not in {'once', 'daily', 'weekly', 'monthly', 'interval'}:
        raise AssistantError('定时任务类型或重复规则无效。')
    start = local_time(result.get('start_at'))
    if start.timestamp() <= now:
        raise AssistantError('首次执行时间已过，请重新指定将来的时间；未创建任务。')
    result['start_at'] = start.isoformat(timespec='seconds')
    until = result.get('until') or ''
    if until:
        end = local_time(until)
        if end < start:
            raise AssistantError('结束时间不能早于首次执行时间。')
        until = end.isoformat(timespec='seconds')
    result['until'] = until
    interval, days, day = result.get('interval_minutes', 0), result.get('weekdays', []), result.get('day_of_month', 0)
    if type(interval) is not int or not 0 <= interval <= 525600:
        raise AssistantError('间隔分钟数无效。')
    if not isinstance(days, list) or len(days) > 7 or any(type(d) is not int or not 0 <= d <= 6 for d in days):
        raise AssistantError('星期规则无效。')
    if type(day) is not int or day not in {-1, *range(32)}:
        raise AssistantError('每月日期规则无效。')
    repeat = result['repeat']
    if (repeat == 'interval' and interval < 1 or repeat != 'interval' and interval
            or repeat == 'weekly' and (not days or start.weekday() not in days)
            or repeat != 'weekly' and days or repeat == 'monthly' and day == 0
            or repeat != 'monthly' and day):
        raise AssistantError('首次执行日期与重复规则不一致，请核对。')
    if repeat == 'monthly' and start.day != (calendar.monthrange(start.year, start.month)[1] if day == -1 else day):
        raise AssistantError('首次执行日期与每月排期不一致。')
    result.update(interval_minutes=interval, weekdays=sorted(set(days)), day_of_month=day)
    return result


def next_occurrence(spec, after):
    """Skip missed repetitions after restart; never replay a backlog of reminders."""
    start = local_time(spec['start_at'])
    repeat = spec['repeat']
    if start.timestamp() > after:
        result = start
    elif repeat == 'once':
        return None
    elif repeat == 'interval':
        seconds = spec['interval_minutes'] * 60
        result = start + dt.timedelta(seconds=(math.floor((after - start.timestamp()) / seconds) + 1) * seconds)
    else:
        current = dt.datetime.fromtimestamp(after, TZ)
        result = dt.datetime.combine(current.date(), start.timetz())
        if repeat in {'daily', 'weekly'}:
            if result.timestamp() <= after:
                result += dt.timedelta(days=1)
            while repeat == 'weekly' and result.weekday() not in spec['weekdays']:
                result += dt.timedelta(days=1)
        else:
            year, month = current.year, current.month
            while True:
                last = calendar.monthrange(year, month)[1]
                day = last if spec['day_of_month'] == -1 else spec['day_of_month']
                if day <= last:
                    result = start.replace(year=year, month=month, day=day)
                    if result.timestamp() > after:
                        break
                year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    if spec.get('until') and result > local_time(spec['until']):
        return None
    return result.timestamp()


def origin_for(context, actor, conversation_id):
    if not isinstance(conversation_id, str) or not re.fullmatch('[a-f0-9]{32}', conversation_id):
        raise AssistantError('原会话标识无效，未设置定时任务。')
    request = context['request']
    if getattr(request.state, 'assistant_schedule_id', ''):
        raise AssistantError('定时任务不能再次创建或修改定时任务。', 403)
    session = context.get('actor') or actor
    if session['id'] != actor['id']:
        raise AssistantError('定时任务账号不一致。', 403)
    route = copy.deepcopy(getattr(request.state, 'feishu_message_route', None))
    topic = context.get('feishu_topic') or {}
    if actor.get('channel', '').startswith('feishu:'):
        if (not isinstance(route, dict) or route.get('chat_id') != actor['channel'].removeprefix('feishu:')
                or not re.fullmatch('om_[A-Za-z0-9]+', route.get('message_id', ''))):
            raise AssistantError('无法核对定时任务的原飞书会话。', 403)
        if route.get('chat_type') == 'group' and (not topic.get('id') or not route.get('root_id')):
            raise AssistantError('群内定时任务必须绑定原话题。', 403)
    elif route:
        raise AssistantError('定时任务来源不一致。', 403)
    return {'conversation_id': conversation_id, 'channel': actor.get('channel', ''),
            'topic_id': topic.get('id', ''), 'route': route,
            'client_id': request.headers.get('x-lighthouse-client', ''), 'name': actor.get('name', '')}


class ScheduledTasks:
    def __init__(self, store):
        self.store = store

    @contextmanager
    def transaction(self):
        with self.store._lock, closing(self.store._connect()) as conn:
            self.store._ensure_schema_locked(conn)
            conn.execute("CREATE INDEX IF NOT EXISTS assistant_schedule_due ON json_documents(namespace,json_extract(payload_json,'$.status'),json_extract(payload_json,'$.next_at')) WHERE namespace='assistant_schedules'")
            conn.execute("CREATE INDEX IF NOT EXISTS assistant_schedule_retries ON json_documents(namespace,json_extract(payload_json,'$.status'),json_extract(payload_json,'$.lease_until'),json_extract(payload_json,'$.retry_at')) WHERE namespace='assistant_schedules'")
            conn.execute('BEGIN IMMEDIATE')
            with conn:
                yield conn

    @staticmethod
    def read(conn, key):
        row = conn.execute('SELECT payload_json FROM json_documents WHERE namespace=? AND key=?', (NAMESPACE, key)).fetchone()
        return json.loads(row[0]) if row else None

    @staticmethod
    def write(conn, key, value):
        conn.execute('INSERT OR REPLACE INTO json_documents(namespace,key,payload_json,updated_at) VALUES(?,?,?,?)',
                     (NAMESPACE, key, json.dumps(value, ensure_ascii=False, allow_nan=False), time.time()))

    @staticmethod
    def visible(task, actor, origin):
        return task['owner'] == actor['id'] and task['origin']['topic_id'] == origin['topic_id']

    @staticmethod
    def public(task):
        return {key: copy.deepcopy(task.get(key)) for key in
                ('id', 'spec', 'status', 'next_at', 'last_at', 'last_status', 'last_error', 'revision')}

    def dispatch(self, context, actor, payload, now=None):
        now = time.time() if now is None else now
        if set(payload) - {'action', 'id', 'spec', 'operation_id', 'conversation_id', 'revision'}:
            raise AssistantError('定时任务参数含未授权字段。')
        origin = origin_for(context, actor, payload.get('conversation_id'))
        action = payload.get('action')
        with self.transaction() as conn:
            rows = [json.loads(row[0]) for row in conn.execute(
                "SELECT payload_json FROM json_documents WHERE namespace=? AND key LIKE 'task:%'", (NAMESPACE,))]
            visible = [task for task in rows if self.visible(task, actor, origin)]
            operation = payload.get('operation_id')
            key = 'op:' + hashlib.sha256((actor['id'] + '|' + origin['topic_id'] + '|' + str(operation)).encode()).hexdigest()
            if action == 'list':
                prior = self.read(conn, key) if isinstance(operation, str) else None
                return {'tasks': [self.public(task) for task in visible], 'timezone': 'Asia/Shanghai',
                        'previous_result': prior.get('result') if prior else None}
            if action not in {'create', 'update', 'pause', 'resume', 'cancel'}:
                raise AssistantError('定时任务操作无效。')
            if not isinstance(operation, str) or not re.fullmatch('[A-Za-z0-9_-]{8,100}', operation):
                raise AssistantError('缺少定时任务操作标识。')
            fingerprint = hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
            prior = self.read(conn, key)
            if prior:
                if prior['fingerprint'] != fingerprint:
                    raise AssistantError('这次操作已处理，不能用重试覆盖其他设置。', 409)
                return prior['result']
            if action == 'create':
                spec = validate_spec(payload.get('spec'), now)
                task = {'id': uuid.uuid4().hex, 'owner': actor['id'], 'origin': origin, 'spec': spec,
                        'created_at': now, 'revision': 1, 'status': 'active', 'next_at': local_time(spec['start_at']).timestamp()}
            else:
                task = next((row for row in visible if row['id'] == payload.get('id')), None)
                if not task:
                    raise AssistantError('定时任务不存在或不属于当前账号和话题。', 404)
                if payload.get('revision') != task['revision']:
                    raise AssistantError('定时任务已变化，请重新查看后操作。', 409)
                if task['status'] == 'cancelled':
                    raise AssistantError('该任务已取消，请重新创建。', 409)
                task['revision'] += 1
                if action == 'update':
                    task['spec'] = validate_spec(payload.get('spec'), now)
                    task.update(status='active', next_at=local_time(task['spec']['start_at']).timestamp())
                elif action == 'resume':
                    active = self.read(conn, 'run:' + task.get('active_run', ''))
                    if active and active['status'] in {'blocked', 'pending', 'running'}:
                        active.update(status='pending', revision=task['revision'], lease='', claim_token='', lease_until=0, retry_at=0)
                        self.write(conn, 'run:' + active['id'], active)
                        task['status'] = 'active'
                    else:
                        due = next_occurrence(task['spec'], now)
                        if due is None:
                            raise AssistantError('这项任务的时间已结束，请修改执行时间后恢复。', 409)
                        task.update(status='active', next_at=due)
                else:
                    task['status'] = 'paused' if action == 'pause' else 'cancelled'
            self.write(conn, 'task:' + task['id'], task)
            result = {'task': self.public(task), 'action': action, 'timezone': 'Asia/Shanghai'}
            self.write(conn, key, {'fingerprint': fingerprint, 'result': result})
            return result

    def _expire(self, conn, task, now, job=None):
        if not task['spec'].get('until') or local_time(task['spec']['until']).timestamp() >= now:
            return False
        error = '排期已到截止时间，未补发过期任务。'
        task.update(status='completed', next_at=None, last_at=now, last_status='expired', last_error=error)
        self.write(conn, 'task:' + task['id'], task)
        if job is not None:
            job.update(status='expired', error=error, claim_token='', lease='', lease_until=0, retry_at=0)
            self.write(conn, 'run:' + job['id'], job)
        return True

    @staticmethod
    def _valid_claim(current, task, job, worker, now):
        return bool(current and task and worker and current.get('claim_token') == worker
                    and job.get('claim_token') == worker and current['status'] == 'running'
                    and current.get('lease_until', 0) > now and task['status'] == 'active'
                    and current['task_id'] == task['id'] == job['task_id']
                    and current['revision'] == task['revision'] == job['revision']
                    and task.get('active_run') == current['id'])

    def claim(self, worker, now=None):
        """Return (task, job) with a fresh claim_token; worker is diagnostic only."""
        now = time.time() if now is None else now
        with self.transaction() as conn:
            for (raw,) in conn.execute("SELECT payload_json FROM json_documents WHERE namespace=? AND key LIKE 'run:%' AND json_extract(payload_json,'$.status') IN ('pending','running') AND COALESCE(json_extract(payload_json,'$.lease_until'),0)<=? AND COALESCE(json_extract(payload_json,'$.retry_at'),0)<=? ORDER BY updated_at LIMIT 8", (NAMESPACE, now, now)).fetchall():
                job = json.loads(raw)
                if job['status'] not in {'pending', 'running'} or job.get('retry_at', 0) > now or job.get('lease_until', 0) > now:
                    continue
                task = self.read(conn, 'task:' + job['task_id'])
                if (not task or task['status'] != 'active' or task['revision'] != job['revision']
                        or task.get('active_run') != job['id']):
                    job.update(status='cancelled', claim_token='', lease='', lease_until=0)
                    self.write(conn, 'run:' + job['id'], job)
                    continue
                if self._expire(conn, task, now, job):
                    continue
                job.update(status='running', lease=worker, claim_token=uuid.uuid4().hex, lease_until=now + LEASE_SECONDS)
                self.write(conn, 'run:' + job['id'], job)
                return task, job
            for (raw,) in conn.execute("""
                SELECT task.payload_json FROM json_documents AS task
                WHERE task.namespace=? AND task.key LIKE 'task:%'
                  AND json_extract(task.payload_json,'$.status')='active'
                  AND json_extract(task.payload_json,'$.next_at')<=?
                  AND NOT EXISTS (
                    SELECT 1 FROM json_documents AS active
                    WHERE active.namespace=task.namespace
                      AND active.key='run:' || json_extract(task.payload_json,'$.active_run')
                      AND json_extract(active.payload_json,'$.revision')=json_extract(task.payload_json,'$.revision')
                      AND json_extract(active.payload_json,'$.status') IN ('pending','running'))
                ORDER BY json_extract(task.payload_json,'$.next_at') LIMIT 8
                """, (NAMESPACE, now)).fetchall():
                task = json.loads(raw)
                if task['status'] != 'active' or task.get('next_at') is None or task['next_at'] > now:
                    continue
                if self._expire(conn, task, now):
                    continue
                due = task['next_at']
                identity = uuid.uuid5(uuid.NAMESPACE_URL, f"assistant:{task['id']}:{task['revision']}:{due}").hex
                job = {'id': identity, 'task_id': task['id'], 'revision': task['revision'], 'due_at': due,
                       'status': 'running', 'phase': 'queued', 'lease': worker,
                       'claim_token': uuid.uuid4().hex, 'lease_until': now + LEASE_SECONDS}
                task.update(active_run=identity, next_at=next_occurrence(task['spec'], now))
                self.write(conn, 'run:' + identity, job)
                self.write(conn, 'task:' + task['id'], task)
                return task, job

    def checkpoint(self, job, worker, **changes):
        """Renew with worker=job['claim_token']; stale/expired claims raise 409."""
        if changes.keys() & {'id', 'task_id', 'revision', 'due_at', 'status', 'lease', 'claim_token', 'lease_until', 'retry_at'}:
            raise AssistantError('检查点不能修改任务身份或领取状态。')
        with self.transaction() as conn:
            now = time.time()
            current = self.read(conn, 'run:' + job['id'])
            task = self.read(conn, 'task:' + job['task_id'])
            if not self._valid_claim(current, task, job, worker, now):
                raise AssistantError('定时任务领取已失效，或已暂停、修改、取消，未继续执行。', 409)
            if task['spec'].get('until') and local_time(task['spec']['until']).timestamp() < now:
                raise AssistantError('定时任务已到截止时间，未继续执行。', 409)
            current.update(changes, lease_until=now + LEASE_SECONDS)
            self.write(conn, 'run:' + job['id'], current)
        job.update(current)

    def finish(self, job, worker, *, error='', permanent=False):
        """Finish with a live claim_token; stale calls are no-ops, never retries."""
        with self.transaction() as conn:
            now = time.time()
            current = self.read(conn, 'run:' + job['id'])
            task = self.read(conn, 'task:' + job['task_id'])
            if not self._valid_claim(current, task, job, worker, now):
                return
            if self._expire(conn, task, now, current):
                return
            answer_failed = not error and current.get('answer_status') in {'failed', 'stopped', 'cancelled', 'superseded'}
            status = 'blocked' if permanent else 'pending' if error else 'failed' if answer_failed else 'completed'
            current.update(status=status, error=error, claim_token='', lease='', lease_until=0, retry_at=now + 60 if error else 0)
            self.write(conn, 'run:' + job['id'], current)
            if task['revision'] == job['revision']:
                last_error = (current.get('answer_error') or '本次回答未完成，可查看原会话。') if answer_failed else error
                task.update(last_at=now, last_status=status, last_error=last_error)
                if permanent:
                    task['status'] = 'paused'
                elif not error and task.get('next_at') is None:
                    task['status'] = 'failed' if answer_failed else 'completed'
                self.write(conn, 'task:' + task['id'], task)

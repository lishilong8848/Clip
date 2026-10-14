"""Durable building/shift checklists; no network work on dashboard reads."""
from __future__ import annotations

import datetime as dt
import json
import logging
import re
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

TZ = dt.timezone(dt.timedelta(hours=8))
SCOPES = ('110', 'A', 'B', 'C', 'D', 'E', 'H')
DUTY = {'operator', 'shift_leader', 'duty'}


class CheckError(Exception):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


def now():
    return dt.datetime.now(TZ)


def windows(day, shift):
    date = dt.date.fromisoformat(day)
    if shift == '白':
        return dt.datetime.combine(date, dt.time(8, 50), TZ), dt.datetime.combine(date, dt.time(18), TZ)
    if shift == '夜':
        return dt.datetime.combine(date, dt.time(17, 50), TZ), dt.datetime.combine(date + dt.timedelta(days=1), dt.time(8), TZ)
    raise CheckError('班次无效。')


def can_manage(actor):
    return actor.get('level') in {'L0', 'L1'} or actor.get('category') in {'supervisor', 'manager'}


def scopes_for(actor):
    if can_manage(actor):
        return [s for s in SCOPES if s in actor.get('scope_rules', []) or 'ALL' in actor.get('scope_rules', [])]
    return [actor['home_scope']] if actor.get('category') in DUTY and actor.get('home_scope') in SCOPES else []


def authorized(actor, scope, manage=False):
    if scope not in scopes_for(actor) or manage and not can_manage(actor):
        raise CheckError('没有该楼栋班前检查的操作权限。', 403)


def dump(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))


class CheckService:
    def __init__(self, root=None, *, roster=None, cloud=None, sender=None, portal_url=None):
        if root is None:
            from upload_event_module.utils import get_data_file_path
            root = get_data_file_path('pre_shift_checks')
        Path(root).mkdir(parents=True, exist_ok=True)
        self.db = Path(root) / 'checks.sqlite3'
        self.roster, self.cloud, self.sender, self.portal_url = roster, cloud, sender, portal_url
        self.stop_event = threading.Event()
        self.wake = threading.Event()
        self.tick_lock = threading.Lock()
        self.thread = None
        self.last_error = ''
        with self.connection() as conn:
            conn.executescript('''
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS definitions(scope TEXT PRIMARY KEY, version INTEGER, items TEXT, actor TEXT, updated TEXT);
                CREATE TABLE IF NOT EXISTS rosters(day TEXT PRIMARY KEY, people TEXT, retry_at REAL DEFAULT 0, error TEXT DEFAULT '');
                CREATE TABLE IF NOT EXISTS tasks(id TEXT PRIMARY KEY, day TEXT, scope TEXT, shift TEXT, due REAL, deadline REAL,
                    definition_version INTEGER, items TEXT, people TEXT, actor_id TEXT DEFAULT '', actor_name TEXT DEFAULT '',
                    submitted TEXT DEFAULT '', answers TEXT DEFAULT '', cloud_key TEXT DEFAULT '');
                CREATE INDEX IF NOT EXISTS task_window ON tasks(deadline, due);
                CREATE INDEX IF NOT EXISTS task_building_day ON tasks(scope, day);
                CREATE INDEX IF NOT EXISTS task_cloud ON tasks(cloud_key);
                CREATE TABLE IF NOT EXISTS deliveries(id TEXT PRIMARY KEY, lease REAL DEFAULT 0, sent INTEGER DEFAULT 0);
                CREATE TABLE IF NOT EXISTS outbox(id TEXT PRIMARY KEY, revision INTEGER, synced INTEGER DEFAULT 0,
                    record_id TEXT DEFAULT '', retry_at REAL DEFAULT 0, error TEXT DEFAULT '');
                CREATE INDEX IF NOT EXISTS pending_uploads ON outbox(retry_at) WHERE revision>synced;
            ''')

    @contextmanager
    def connection(self, write=False):
        conn = sqlite3.connect(self.db, timeout=5)
        conn.row_factory = sqlite3.Row
        try:
            if write:
                conn.execute('BEGIN IMMEDIATE')
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def definition(self, scope, conn):
        row = conn.execute('SELECT * FROM definitions WHERE scope=?', (scope,)).fetchone()
        return {'version': row['version'], 'items': json.loads(row['items'])} if row else {'version': 0, 'items': []}

    def save_definition(self, actor, scope, version, items):
        authorized(actor, scope, True)
        if type(version) is not int or not isinstance(items, list) or len(items) > 50:
            raise CheckError('清单最多50项，请重新读取后编辑。')
        result, seen = [], set()
        for item in items:
            if not isinstance(item, dict) or set(item) - {'id', 'text'}:
                raise CheckError('检查项格式无效。')
            key, text = item.get('id'), item.get('text')
            if not isinstance(key, str) or not re.fullmatch(r'[a-zA-Z0-9_-]{1,64}', key) or key in seen:
                raise CheckError('检查项标识重复或无效。')
            if not isinstance(text, str) or not 1 <= len(text.strip()) <= 200:
                raise CheckError('每条检查项需填写1至200字。')
            seen.add(key)
            result.append({'id': key, 'text': text.strip()})
        current = now()
        with self.connection(True) as conn:
            if self.definition(scope, conn)['version'] != version:
                raise CheckError('清单已被其他管理人员修改，请重新读取。', 409)
            conn.execute('INSERT OR REPLACE INTO definitions VALUES(?,?,?,?,?)',
                         (scope, version + 1, dump(result), actor['id'], current.isoformat()))
            self.prepare_local(conn, current, [scope])
        self.wake.set()
        return {'version': version + 1, 'items': result}

    def state(self, actor, scope, current=None):
        authorized(actor, scope)
        current = current or now()
        day = current.date().isoformat()
        with self.connection() as conn:
            definition = self.definition(scope, conn)
            tasks = []
            for row in conn.execute("SELECT * FROM tasks WHERE scope=? AND (submitted!='' OR items!='[]') AND (day=? OR (deadline>? AND due<=?)) ORDER BY due DESC",
                                    (scope, day, current.timestamp(), current.timestamp())):
                if not row['submitted'] and not definition['items']:
                    continue
                task = dict(row)
                for key in ('items', 'people'):
                    task[key] = json.loads(task[key])
                task['answers'] = json.loads(task['answers']) if task['answers'] else []
                task['can_submit'] = self.can_submit(actor, task, current)
                outbox = conn.execute('SELECT * FROM outbox WHERE id=?', (task['cloud_key'],)).fetchone()
                task['sync_status'] = 'synced' if outbox and outbox['synced'] == outbox['revision'] else 'pending'
                task['sync_error'] = outbox['error'] if outbox else ''
                tasks.append(task)
            roster = conn.execute('SELECT people,error FROM rosters WHERE day=?', (day,)).fetchone()
            roster_ready = bool(roster and roster['people'])
            roster_error = roster['error'] if roster else ''
            if roster_error.startswith('{'):
                roster_error = json.loads(roster_error).get(scope, '')
            return {'scope': scope, 'scopes': scopes_for(actor), 'can_manage': can_manage(actor),
                    'definition': definition, 'tasks': tasks,
                    'roster_ready': roster_ready, 'roster_error': roster_error or self.last_error,
                    'server_time': current.isoformat()}

    @staticmethod
    def can_submit(actor, task, current):
        people = json.loads(task['people']) if isinstance(task['people'], str) else task['people']
        return (not task['submitted'] and task['due'] <= current.timestamp() < task['deadline']
                and (actor['id'] in {p['open_id'] for p in people}
                     or actor.get('category') == 'duty' and actor.get('home_scope') == task['scope']))

    def submit(self, actor, task_id, version, answers, current=None):
        current = current or now()
        with self.connection(True) as conn:
            row = conn.execute('SELECT * FROM tasks WHERE id=?', (task_id,)).fetchone()
            if not row:
                raise CheckError('班前检查不存在，请刷新。', 404)
            authorized(actor, row['scope'])
            if row['submitted']:
                # Lost responses and concurrent submissions never replace the first result.
                return {'completed': True, 'already_completed': True, 'actor_name': row['actor_name']}
            definition = self.definition(row['scope'], conn)
            if not definition['items'] or row['definition_version'] != definition['version']:
                raise CheckError('清单已更新或已清空，请重新读取。', 409)
            if not self.can_submit(actor, row, current):
                raise CheckError('仅本班排班人员或本楼值班账号可在班次内提交。', 403)
            if type(version) is not int or version != row['definition_version']:
                raise CheckError('清单版本已变化，请重新读取。', 409)
            items = json.loads(row['items'])
            if not items or not isinstance(answers, list) or len(answers) != len(items):
                raise CheckError('请逐项完成检查后提交。')
            mapped = {}
            for answer in answers:
                if not isinstance(answer, dict) or set(answer) != {'id', 'status', 'note'}:
                    raise CheckError('检查结果格式无效。')
                key, status, note = answer['id'], answer['status'], answer['note']
                if not isinstance(key, str) or key in mapped or status not in ('normal', 'abnormal'):
                    raise CheckError('每条检查项必须且只能选择无异常或异常。')
                if not isinstance(note, str) or len(note.strip()) > 500 or status == 'abnormal' and not note.strip():
                    raise CheckError('异常项请填写备注（最多500字）。')
                mapped[key] = {'status': status, 'note': note.strip() if status == 'abnormal' else ''}
            if set(mapped) != {item['id'] for item in items}:
                raise CheckError('检查项不一致，请重新读取清单。', 409)
            results = [{**item, **mapped[item['id']]} for item in items]
            # One person/day/building cloud row includes both shifts if a duty account covers both.
            cloud_key = ':'.join(('pre-shift', row['day'], row['scope'], actor['id']))
            conn.execute('UPDATE tasks SET actor_id=?,actor_name=?,submitted=?,answers=?,cloud_key=? WHERE id=?',
                         (actor['id'], actor['name'], current.isoformat(), dump(results), cloud_key, task_id))
            conn.execute('INSERT INTO outbox(id,revision) VALUES(?,1) ON CONFLICT(id) DO UPDATE SET revision=revision+1,retry_at=0', (cloud_key,))
        self.wake.set()
        return {'completed': True, 'already_completed': False, 'actor_name': actor['name']}

    def prepare(self, current):
        with self.connection() as conn:
            configured = [row['scope'] for row in conn.execute("SELECT scope FROM definitions WHERE items!='[]'")]
        if not configured or not self.roster or current.time() < dt.time(8, 50):
            return
        # Previous-night tasks remain durable through midnight; do not re-read yesterday's changing view.
        day = current.date().isoformat()
        with self.connection() as conn:
            cached = conn.execute('SELECT * FROM rosters WHERE day=?', (day,)).fetchone()
        if not cached or not cached['people'] and cached['retry_at'] <= current.timestamp():
            try:
                snapshot = self.roster(day)
                with self.connection(True) as conn:
                    conn.execute('INSERT OR REPLACE INTO rosters VALUES(?,?,0,?)', (day, dump(snapshot['people']), dump(snapshot.get('warnings', {}))))
            except Exception as exc:
                with self.connection(True) as conn:
                    conn.execute('INSERT OR REPLACE INTO rosters VALUES(?,?,?,?)',
                                 (day, '', current.timestamp() + 300, (str(exc) if isinstance(exc, CheckError) else '排班读取未完成。') + '5分钟后重试；不会按不完整名单推送。'))
                return
        with self.connection(True) as conn:
            self.prepare_local(conn, current, configured)

    def prepare_local(self, conn, current, scopes):
        # Editing updates open checks atomically; submitted and past shifts keep their snapshot.
        day = current.date().isoformat()
        cached = conn.execute('SELECT people FROM rosters WHERE day=?', (day,)).fetchone()
        people = json.loads(cached['people']) if cached and cached['people'] else []
        for scope in scopes:
            definition = self.definition(scope, conn)
            items = dump(definition['items'])
            conn.execute("UPDATE tasks SET definition_version=?,items=? WHERE scope=? AND submitted='' AND deadline>? AND (definition_version!=? OR items!=?)",
                         (definition['version'], items, scope, current.timestamp(), definition['version'], items))
            if not definition['items']:
                continue
            for shift in ('白', '夜'):
                due, deadline = windows(day, shift)
                if not due <= current < deadline:
                    continue
                assigned = [p for p in people if p['scope'] == scope and p['shift'] == shift]
                if len(assigned) != 2:
                    continue
                conn.execute('INSERT OR IGNORE INTO tasks(id,day,scope,shift,due,deadline,definition_version,items,people) VALUES(?,?,?,?,?,?,?,?,?)',
                             (f'{day}:{scope}:{shift}', day, scope, shift, due.timestamp(), deadline.timestamp(),
                              definition['version'], items, dump(assigned)))

    def notify(self, current):
        if not self.sender or not self.portal_url:
            return
        base = str(self.portal_url() or '').rstrip('/')
        if not base.startswith(('http://', 'https://')):
            self.last_error = '班前检查访问地址未配置，暂未推送。'
            return
        with self.connection() as conn:
            tasks = [dict(row) for row in conn.execute("SELECT * FROM tasks WHERE submitted='' AND items!='[]' AND scope IN (SELECT scope FROM definitions WHERE items!='[]') AND due<=? AND deadline>?", (current.timestamp(), current.timestamp()))]
        for task in tasks:
            slot = int((current.timestamp() - task['due']) // 7200)
            for person in json.loads(task['people']):
                if self.stop_event.is_set():
                    return
                key = f"{task['id']}:{slot}:{person['open_id']}"
                with self.connection(True) as conn:
                    latest = conn.execute('SELECT submitted,items FROM tasks WHERE id=?', (task['id'],)).fetchone()
                    if latest['submitted'] or latest['items'] == '[]':
                        break
                    row = conn.execute('SELECT * FROM deliveries WHERE id=?', (key,)).fetchone()
                    if row and (row['sent'] or row['lease'] > current.timestamp()):
                        continue
                    conn.execute('INSERT OR REPLACE INTO deliveries VALUES(?,?,0)', (key, current.timestamp() + 300))
                label = '班前检查' if slot == 0 else '班前检查未完成提醒'
                card = {'config': {'wide_screen_mode': True},
                    'header': {'template': 'blue' if slot == 0 else 'orange', 'title': {'tag': 'plain_text', 'content': label}},
                    'elements': [{'tag': 'div', 'text': {'tag': 'plain_text', 'content':
                        f"{task['day']} · {task['scope']}楼 · {task['shift']}班\n本班2人任意1人完成并提交即可。请逐项检查，有异常时填写备注。"}},
                        {'tag': 'action', 'actions': [{'tag': 'button', 'type': 'primary', 'text': {'tag': 'plain_text', 'content': '打开班前检查'},
                            'url': base + '/assets/workspace-v2/index.html?__page=/&pre_shift=1'}]}]}
                try:
                    result = self.sender(person['open_id'], card, 'pre-shift:' + key)
                    if not result[0]:
                        raise CheckError('班前检查消息发送失败，稍后重试。')
                    with self.connection(True) as conn:
                        conn.execute('UPDATE deliveries SET sent=1 WHERE id=?', (key,))
                except Exception:
                    self.last_error = '班前检查消息暂未送达，后台将自动重试。'

    def sync(self, current):
        if not self.cloud:
            return
        with self.connection() as conn:
            jobs = [dict(row) for row in conn.execute('SELECT * FROM outbox WHERE revision>synced AND retry_at<=? LIMIT 10', (current.timestamp(),))]
        for job in jobs:
            if self.stop_event.is_set():
                return
            with self.connection() as conn:
                rows = [dict(row) for row in conn.execute('SELECT * FROM tasks WHERE cloud_key=? ORDER BY due', (job['id'],))]
            try:
                record_id = self.cloud.upsert(job['id'], rows, job['record_id'])
                with self.connection(True) as conn:
                    conn.execute("UPDATE outbox SET record_id=?,synced=?,error='' WHERE id=?", (record_id, job['revision'], job['id']))
            except Exception:
                with self.connection(True) as conn:
                    conn.execute('UPDATE outbox SET retry_at=?,error=? WHERE id=?',
                                 (current.timestamp() + 300, '本地已保存，飞书同步暂未完成，将自动重试。', job['id']))

    def tick(self, current=None):
        if not self.tick_lock.acquire(False):
            return
        try:
            current = current or now()
            self.last_error = ''
            self.prepare(current)
            self.notify(current)
            self.sync(current)
        finally:
            self.tick_lock.release()

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        def run():
            while not self.stop_event.is_set():
                self.wake.wait(30)
                self.wake.clear()
                if self.stop_event.is_set():
                    break
                try:
                    self.tick()
                except Exception:
                    self.last_error = '班前检查后台暂忙，稍后自动重试。'
                    logging.exception('Pre-shift worker failed')
        self.thread = threading.Thread(target=run, name='pre-shift-checks', daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.wake.set()
        if self.thread:
            self.thread.join(timeout=2)

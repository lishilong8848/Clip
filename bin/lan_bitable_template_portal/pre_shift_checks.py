"""Durable building/shift checklists with per-item scheduling and attachments.

Runs entirely offline on dashboard reads; only the background syncer talks to
Feishu, and only after an explicit external-write guard passes.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import os
import re
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from pathlib import Path

from .pre_shift_schedule import normalize_frequency, occurrences_for_date, upcoming_occurrences

TZ = dt.timezone(dt.timedelta(hours=8))
SCOPES = ('110', 'A', 'B', 'C', 'D', 'E', 'H')
DUTY = {'operator', 'shift_leader', 'duty'}
ROSTER_VERSION = 2
CLOUD_DETAIL_VERSION = 1

ALLOWED_EXTS = {'.jpg', '.jpeg', '.png', '.webp', '.pdf', '.doc', '.docx',
                '.xls', '.xlsx', '.md', '.txt', '.csv'}
IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.webp'}
MAX_FILE_BYTES = 20 * 1024 * 1024          # 20 MiB per file
MAX_TASK_BYTES = 100 * 1024 * 1024         # 100 MiB per task
MAX_FILES_PER_ITEM = 10


class CheckError(Exception):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


def now():
    return dt.datetime.now(TZ)


def windows(day, shift):
    date = dt.date.fromisoformat(day)
    if shift == '白':
        return dt.datetime.combine(date, dt.time(8), TZ), dt.datetime.combine(date, dt.time(18), TZ)
    if shift == '夜':
        return dt.datetime.combine(date, dt.time(17, 50), TZ), dt.datetime.combine(date + dt.timedelta(days=1), dt.time(8), TZ)
    raise CheckError('班次无效。')


def shift_deadline(day, shift):
    return windows(day, shift)[1]


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


def _sanitize_name(name):
    if not isinstance(name, str) or not name:
        raise CheckError('文件名无效。')
    name = name.replace('\\', '/').rsplit('/', 1)[-1]
    name = ''.join(ch for ch in name if ch >= ' ' and ch != '\x7f')
    name = name.strip(' .')
    if not name or len(name) > 200:
        raise CheckError('文件名无效。')
    return name


def _image_ext(path):
    return Path(path).suffix.lower() in IMAGE_EXTS


class CheckService:
    def __init__(self, root=None, *, roster=None, cloud=None, sender=None, portal_url=None):
        if root is None:
            from upload_event_module.utils import get_data_file_path
            root = get_data_file_path('pre_shift_checks')
        Path(root).mkdir(parents=True, exist_ok=True)
        self.db = Path(root) / 'checks.sqlite3'
        self.files_dir = Path(root) / 'files'
        self.files_dir.mkdir(parents=True, exist_ok=True)
        self.staging_dir = self.files_dir / 'staging'
        self.staging_dir.mkdir(parents=True, exist_ok=True)
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
                CREATE TABLE IF NOT EXISTS rosters(day TEXT PRIMARY KEY, people TEXT, retry_at REAL DEFAULT 0, error TEXT DEFAULT '', version INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS tasks(id TEXT PRIMARY KEY, day TEXT, scope TEXT, shift TEXT, due REAL, deadline REAL,
                    definition_version INTEGER, items TEXT, people TEXT, actor_id TEXT DEFAULT '', actor_name TEXT DEFAULT '',
                    submitted TEXT DEFAULT '', answers TEXT DEFAULT '', cloud_key TEXT DEFAULT '', evidence TEXT DEFAULT '');
                CREATE INDEX IF NOT EXISTS task_window ON tasks(deadline, due);
                CREATE INDEX IF NOT EXISTS task_building_day ON tasks(scope, day);
                CREATE INDEX IF NOT EXISTS task_cloud ON tasks(cloud_key);
                CREATE INDEX IF NOT EXISTS task_history ON tasks(scope, submitted DESC, id DESC) WHERE submitted!='';
                CREATE TABLE IF NOT EXISTS deliveries(id TEXT PRIMARY KEY, lease REAL DEFAULT 0, sent INTEGER DEFAULT 0);
                CREATE TABLE IF NOT EXISTS outbox(id TEXT PRIMARY KEY, revision INTEGER, synced INTEGER DEFAULT 0,
                    record_id TEXT DEFAULT '', retry_at REAL DEFAULT 0, error TEXT DEFAULT '');
                CREATE INDEX IF NOT EXISTS pending_uploads ON outbox(retry_at) WHERE revision>synced;
                CREATE TABLE IF NOT EXISTS files(id TEXT PRIMARY KEY, task_id TEXT DEFAULT '', item_id TEXT DEFAULT '',
                    kind TEXT NOT NULL, day TEXT DEFAULT '', scope TEXT DEFAULT '', shift TEXT DEFAULT '',
                    name TEXT NOT NULL, size INTEGER NOT NULL, is_image INTEGER NOT NULL DEFAULT 0,
                    disk_name TEXT NOT NULL, uploader_id TEXT NOT NULL, uploader_name TEXT NOT NULL,
                    submitted INTEGER NOT NULL DEFAULT 0, created TEXT NOT NULL, file_token TEXT DEFAULT '',
                    content_hash TEXT DEFAULT '');
                CREATE UNIQUE INDEX IF NOT EXISTS one_handover ON files(day, scope, shift) WHERE kind='handover';
                CREATE INDEX IF NOT EXISTS idx_files_task ON files(task_id);
            ''')
            task_cols = {r[1] for r in conn.execute('PRAGMA table_info(tasks)')}
            if 'version' not in {r[1] for r in conn.execute('PRAGMA table_info(rosters)')}:
                conn.execute('ALTER TABLE rosters ADD COLUMN version INTEGER NOT NULL DEFAULT 0')
            if 'evidence' not in task_cols:
                conn.execute("ALTER TABLE tasks ADD COLUMN evidence TEXT DEFAULT ''")
            file_cols = {r[1] for r in conn.execute('PRAGMA table_info(files)')}
            if 'content_hash' not in file_cols:
                conn.execute("ALTER TABLE files ADD COLUMN content_hash TEXT DEFAULT ''")
            if 'detail_version' not in {r[1] for r in conn.execute('PRAGMA table_info(outbox)')}:
                conn.execute('ALTER TABLE outbox ADD COLUMN detail_version INTEGER NOT NULL DEFAULT 0')
            # Reuse the existing row and retry queue to backfill old submissions once.
            conn.execute('UPDATE outbox SET revision=revision+1,retry_at=0 WHERE detail_version<? AND revision=synced',
                         (CLOUD_DETAIL_VERSION,))

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
        current = now()
        with self.connection() as conn:
            stored = self.definition(scope, conn)
        previous = {item['id']: item for item in stored['items']}
        default_start = current.date().isoformat()
        for item in items:
            if not isinstance(item, dict) or set(item) - {'id', 'text', 'frequency'}:
                raise CheckError('检查项格式无效。')
            key, text = item.get('id'), item.get('text')
            if not isinstance(key, str) or not re.fullmatch(r'[a-zA-Z0-9_-]{1,64}', key) or key in seen:
                raise CheckError('检查项标识重复或无效。')
            if not isinstance(text, str) or not 1 <= len(text.strip()) <= 200:
                raise CheckError('每条检查项需填写1至200字。')
            seen.add(key)
            freq_value = item.get('frequency')
            if freq_value is not None:
                try:
                    rule = normalize_frequency(freq_value, default_start)
                except (ValueError, TypeError):
                    raise CheckError('检查项排期格式无效。')
            elif key in previous and previous[key].get('frequency') is not None:
                # Preserve the anchor on silent text edits; do not re-anchor.
                rule = previous[key]['frequency']
            else:
                try:
                    rule = normalize_frequency(None, default_start)
                except (ValueError, TypeError):
                    raise CheckError('检查项排期格式无效。')
            result.append({'id': key, 'text': text.strip(), 'frequency': rule})
        with self.connection(True) as conn:
            if self.definition(scope, conn)['version'] != version:
                raise CheckError('清单已被其他管理人员修改，请重新读取。', 409)
            conn.execute('INSERT OR REPLACE INTO definitions VALUES(?,?,?,?,?)',
                         (scope, version + 1, dump(result), actor['id'], current.isoformat()))
            self.prepare_local(conn, current, [scope])
        self.wake.set()
        return {'version': version + 1, 'items': result}

    def _task_id(self, day, scope, shift, due):
        d = dt.date.fromisoformat(day)
        standard_white = dt.datetime.combine(d, dt.time(8, 50), TZ)
        if shift == '白' and due == standard_white:
            return f'{day}:{scope}:白'
        standard_night = dt.datetime.combine(d, dt.time(17, 50), TZ)
        if shift == '夜' and due == standard_night:
            return f'{day}:{scope}:夜'
        return f'{day}:{scope}:{shift}:{due.strftime("%H%M")}'

    def _occurrence_groups(self, items, scope, day, current):
        """Open per-item occurrence groups for one nominal ``day`` (ISO).

        Returns groups as dicts {day, shift, due, deadline, ids: [...]} whose
        window contains ``current``. Items with identical (day, shift, due)
        are merged.
        """
        groups = {}
        for item in items:
            try:
                rule = normalize_frequency(item.get('frequency'), day)
                occurrences = occurrences_for_date(rule, day)
            except (ValueError, TypeError):
                continue
            for occ in occurrences:
                due = occ['due']
                shift = occ['shift']
                deadline = shift_deadline(occ['day'], shift)
                if not (due <= current < deadline):
                    continue
                key = (occ['day'], shift, due.timestamp())
                group = groups.get(key)
                if group is None:
                    group = {'day': occ['day'], 'shift': shift, 'due': due,
                             'deadline': deadline, 'ids': []}
                    groups[key] = group
                group['ids'].append(item['id'])
        return list(groups.values())

    def _people_for(self, conn, day, scope, shift):
        row = conn.execute('SELECT people FROM rosters WHERE day=?', (day,)).fetchone()
        if not row or not row['people']:
            return []
        people = [p for p in json.loads(row['people'])
                  if p.get('scope') == scope and p.get('shift') == shift]
        return people if len(people) == 2 else []

    def _clear_open_tasks(self, conn, current, scope):
        rows = [r['id'] for r in conn.execute(
            "SELECT id FROM tasks WHERE scope=? AND submitted='' AND items!='[]' AND deadline>?",
            (scope, current.timestamp()))]
        for task_id in rows:
            conn.execute("UPDATE tasks SET items='[]' WHERE id=? AND submitted=''", (task_id,))

    def _reconcile_groups(self, conn, current, scope, groups, def_version):
        pending = conn.execute(
            "SELECT * FROM tasks WHERE scope=? AND submitted='' AND items!='[]' AND deadline>?",
            (scope, current.timestamp())).fetchall()
        all_items = {item['id']: item for item in self.definition(scope, conn)['items']}
        active_ids = set()
        for group in groups:
            people = self._people_for(conn, group['day'], scope, group['shift'])
            due = group['due']
            task_id = self._task_id(group['day'], scope, group['shift'], due)
            active_ids.add(task_id)
            subset = [{'id': iid, 'text': all_items[iid]['text']} for iid in group['ids'] if iid in all_items]
            if not subset:
                continue
            items = dump(subset)
            due_ts = due.timestamp()
            deadline_ts = group['deadline'].timestamp()
            ppl = dump(people)
            row = conn.execute('SELECT * FROM tasks WHERE id=?', (task_id,)).fetchone()
            if row and row['submitted']:
                continue  # immutable submitted/past snapshot
            if row:
                if (row['items'] != items or row['definition_version'] != def_version
                        or row['people'] != ppl or row['due'] != due_ts
                        or row['deadline'] != deadline_ts):
                    conn.execute(
                        "UPDATE tasks SET definition_version=?,items=?,due=?,deadline=?,people=? WHERE id=? AND submitted=''",
                        (def_version, items, due_ts, deadline_ts, ppl, task_id))
            else:
                conn.execute(
                    "INSERT INTO tasks(id,day,scope,shift,due,deadline,definition_version,items,people) VALUES(?,?,?,?,?,?,?,?,?)",
                    (task_id, group['day'], scope, group['shift'], due_ts,
                     deadline_ts, def_version, items, ppl))
        for row in pending:
            if row['id'] not in active_ids:
                conn.execute("UPDATE tasks SET definition_version=?,items='[]' WHERE id=? AND submitted=''",
                             (def_version, row['id']))

    def prepare_local(self, conn, current, scopes):
        # Editing/open re-computation reconciles pending groups atomically.
        for scope in scopes:
            definition = self.definition(scope, conn)
            items = definition['items']
            if not items:
                self._clear_open_tasks(conn, current, scope)
                continue
            groups = {}
            for item in items:
                try:
                    rule = normalize_frequency(item.get('frequency'), current.date().isoformat())
                    occurrences = upcoming_occurrences(rule, current)
                except (ValueError, TypeError):
                    continue
                seen = set()
                for occ in occurrences:
                    deadline = shift_deadline(occ['day'], occ['shift'])
                    if deadline <= current or occ['shift'] in seen:
                        continue
                    seen.add(occ['shift'])
                    key = (occ['day'], occ['shift'], occ['due'].timestamp())
                    group = groups.setdefault(key, {**occ, 'deadline': deadline, 'ids': []})
                    group['ids'].append(item['id'])
            self._reconcile_groups(conn, current, scope, list(groups.values()), definition['version'])

    def _needs_prior(self, current, prior):
        today = current.date().isoformat()
        with self.connection() as conn:
            for scope in [r['scope'] for r in conn.execute("SELECT scope FROM definitions WHERE items!='[]'")]:
                definition = self.definition(scope, conn)
                for day in (today, prior):
                    groups = self._occurrence_groups(definition['items'], scope, day, current)
                    if any(g['due'] <= current < g['deadline'] and g['day'] == prior for g in groups):
                        return True
        return False

    def _ensure_roster(self, current, day):
        with self.connection() as conn:
            row = conn.execute('SELECT * FROM rosters WHERE day=?', (day,)).fetchone()
        if row and row['version'] == ROSTER_VERSION and row['people'] and not row['retry_at']:
            return
        if row and row['version'] == ROSTER_VERSION and row['retry_at'] > current.timestamp():
            return
        try:
            snapshot = self.roster(day)
            with self.connection(True) as conn:
                warnings = snapshot.get('warnings', {})
                conn.execute('INSERT OR REPLACE INTO rosters(day,people,retry_at,error,version) VALUES(?,?,?,?,?)',
                             (day, dump(snapshot['people']), current.timestamp() + 300 if warnings else 0, dump(warnings), ROSTER_VERSION))
        except Exception as exc:
            with self.connection(True) as conn:
                conn.execute('INSERT OR REPLACE INTO rosters(day,people,retry_at,error,version) VALUES(?,?,?,?,?)',
                             (day, '', current.timestamp() + 300,
                              (str(exc) if isinstance(exc, CheckError) else '排班读取未完成。') + '5分钟后重试；不会按不完整名单推送。', ROSTER_VERSION))

    def prepare(self, current):
        with self.connection() as conn:
            configured = [row['scope'] for row in conn.execute("SELECT scope FROM definitions WHERE items!='[]'")]
        if not configured or not self.roster:
            return
        # Previous-night tasks remain durable through midnight; do not re-read yesterday's changing view
        # unless there is an open prior-day occurrence with no cached roster yet.
        today = current.date().isoformat()
        prior = (current.date() - dt.timedelta(days=1)).isoformat()
        self._ensure_roster(current, today)
        if self._needs_prior(current, prior):
            self._ensure_roster(current, prior)
        with self.connection(True) as conn:
            self.prepare_local(conn, current, configured)

    @staticmethod
    def can_submit(actor, task, current):
        people = json.loads(task['people']) if isinstance(task['people'], str) else task['people']
        return (not task['submitted'] and bool(json.loads(task['items']) if isinstance(task['items'], str) else task['items'])
                and (actor['id'] in {p['open_id'] for p in people}
                     or actor.get('category') in DUTY and actor.get('home_scope') == task['scope']))

    def _is_roster_or_duty(self, actor, task):
        people = json.loads(task['people']) if isinstance(task['people'], str) else task['people']
        return (actor['id'] in {p['open_id'] for p in people}
                or actor.get('category') in DUTY and actor.get('home_scope') == task['scope'])

    def _evidence_file(self, frow):
        return {'id': frow['id'], 'item_id': frow.get('item_id') or '', 'name': frow['name'],
                'size': frow['size'], 'is_image': bool(frow['is_image']), 'kind': frow.get('kind', '')}

    def _can_delete_file(self, actor, frow, task, current):
        if actor.get('id') != frow['uploader_id']:
            return False
        if frow['kind'] == 'handover':
            if frow['submitted']:
                return False
            row = self._any_submitted_in_shift(task['day'], task['scope'], task['shift'])
            if row:
                return False
            return self._is_roster_or_duty(actor, task)
        # item file
        if frow['submitted'] or task['submitted']:
            return False
        try:
            return self.can_submit(actor, task, current)
        except Exception:
            return False

    def _any_submitted_in_shift(self, day, scope, shift, conn=None):
        if conn is None:
            with self.connection() as c:
                return c.execute("SELECT id FROM tasks WHERE day=? AND scope=? AND shift=? AND submitted!='' LIMIT 1",
                                 (day, scope, shift)).fetchone() is not None
        return conn.execute("SELECT id FROM tasks WHERE day=? AND scope=? AND shift=? AND submitted!='' LIMIT 1",
                            (day, scope, shift)).fetchone() is not None

    def state(self, actor, scope, current=None):
        authorized(actor, scope)
        current = current or now()
        day = current.date().isoformat()
        with self.connection() as conn:
            definition = self.definition(scope, conn)
            tasks = []
            for row in conn.execute("SELECT * FROM tasks WHERE scope=? AND (submitted!='' OR items!='[]') AND (submitted='' OR deadline>?) ORDER BY due,id",
                                    (scope, current.timestamp())):
                if not row['submitted'] and not definition['items'] and row['deadline'] > current.timestamp():
                    continue
                task = dict(row)
                for key in ('items', 'people'):
                    task[key] = json.loads(task[key])
                task['answers'] = json.loads(task['answers']) if task['answers'] else []
                task['can_submit'] = self.can_submit(actor, task, current)
                outbox = conn.execute('SELECT * FROM outbox WHERE id=?', (task['cloud_key'],)).fetchone()
                task['sync_status'] = 'synced' if outbox and outbox['synced'] == outbox['revision'] else 'pending'
                task['sync_error'] = outbox['error'] if outbox else ''
                task['files'] = self._state_files(conn, actor, task, current)
                task['handover_photo'] = self._state_handover(conn, actor, task, current)
                task['can_upload_handover'] = self._can_upload_handover(conn, actor, task, current)
                tasks.append(task)
            roster = conn.execute('SELECT people,error FROM rosters WHERE day=?', (day,)).fetchone()
            roster_ready = bool(roster and roster['people'] and any(p.get('scope') == scope for p in json.loads(roster['people'])))
            roster_error = roster['error'] if roster else ''
            if roster_error.startswith('{'):
                roster_error = json.loads(roster_error).get(scope, '')
            return {'scope': scope, 'scopes': scopes_for(actor), 'can_manage': can_manage(actor),
                    'definition': definition, 'tasks': tasks,
                    'roster_ready': roster_ready, 'roster_error': roster_error or self.last_error,
                    'overview': self._overview(actor, conn, current) if can_manage(actor) else None,
                    'server_time': current.isoformat()}

    def history(self, actor, scope, page=1, page_size=20, current=None):
        authorized(actor, scope)
        if not str(page).isdigit() or not str(page_size).isdigit():
            raise CheckError('历史记录分页参数无效。')
        page, page_size = int(page), int(page_size)
        if not 1 <= page <= 1000000 or not 1 <= page_size <= 50:
            raise CheckError('历史记录分页参数无效。')
        current = current or now()
        with self.connection() as conn:
            total = conn.execute("SELECT COUNT(*) FROM tasks WHERE scope=? AND submitted!=''", (scope,)).fetchone()[0]
            page = min(page, max(1, (total + page_size - 1) // page_size))
            rows = conn.execute("SELECT * FROM tasks WHERE scope=? AND submitted!='' ORDER BY submitted DESC,id DESC LIMIT ? OFFSET ?",
                                (scope, page_size, (page - 1) * page_size)).fetchall()
            items = []
            for row in rows:
                task = dict(row)
                item = {key: task[key] for key in ('id', 'day', 'scope', 'shift', 'due', 'deadline',
                                                  'definition_version', 'actor_name', 'submitted')}
                item['answers'] = json.loads(task['answers'])
                item['normal_count'] = sum(a['status'] == 'normal' for a in item['answers'])
                item['abnormal_count'] = sum(a['status'] == 'abnormal' for a in item['answers'])
                item['files'] = self._state_files(conn, actor, task, current)
                item['handover_photo'] = self._state_handover(conn, actor, task, current)
                outbox = conn.execute('SELECT revision,synced,error FROM outbox WHERE id=?', (task['cloud_key'],)).fetchone()
                item['sync_status'] = 'synced' if outbox and outbox['revision'] == outbox['synced'] else 'pending'
                item['sync_error'] = outbox['error'] if outbox else ''
                items.append(item)
        return {'scope': scope, 'items': items, 'total': total, 'page': page, 'page_size': page_size}

    def _overview(self, actor, conn, current):
        rows = []
        for scope in scopes_for(actor):
            configured = bool(self.definition(scope, conn)['items'])
            for shift in ('白', '夜'):
                day = (current.date() - dt.timedelta(days=1) if shift == '夜' and current.hour < 8 else current.date()).isoformat()
                tasks = [dict(r) for r in conn.execute("SELECT * FROM tasks WHERE day=? AND scope=? AND shift=? AND items!='[]' ORDER BY due,id",
                                                     (day, scope, shift))]
                total = sum(len(json.loads(t['items'])) for t in tasks)
                completed = [t for t in tasks if t['submitted']]
                pending = [t for t in tasks if not t['submitted']]
                status = ('未配置清单' if not configured else '本班暂无清单') if not tasks else (
                    '已完成' if not pending else '逾期待补做' if any(t['deadline'] <= current.timestamp() for t in pending)
                    else '待完成' if any(t['due'] <= current.timestamp() for t in pending) else '未到时间')
                photo = conn.execute("SELECT id,name FROM files WHERE kind='handover' AND day=? AND scope=? AND shift=? LIMIT 1",
                                     (day, scope, shift)).fetchone()
                rows.append({'id': day + ':' + scope + ':' + shift, 'day': day, 'scope': scope, 'shift': shift,
                    'status': status, 'total': total, 'completed': sum(len(json.loads(t['items'])) for t in completed),
                    'abnormal': sum(a['status'] == 'abnormal' for t in completed for a in json.loads(t['answers'] or '[]')),
                    'submitted_by': '、'.join(dict.fromkeys(t['actor_name'] for t in completed)),
                    'submitted_at': max((t['submitted'] for t in completed), default=''),
                    'handover_photo': {'name': photo['name'], 'url': '/api/pre-shift-checks/files/' + photo['id']} if photo else None})
        return {'rows': rows}

    def _state_files(self, conn, actor, task, current):
        if task['submitted'] and task['evidence']:
            ev = json.loads(task['evidence'])
            refs = ev.get('item_files', [])
            return [self._public_ref(r, actor, task, current, can_delete=False)
                    for r in refs if r.get('kind') != 'handover']
        rows = conn.execute("SELECT * FROM files WHERE task_id=? AND kind='item' ORDER BY created", (task['id'],)).fetchall()
        out = []
        for row in rows:
            f = dict(row)
            out.append(self._public_file(f, actor, task, current))
        return out

    def _public_ref(self, ref, actor, task, current, can_delete=False):
        return {'id': ref['id'], 'item_id': ref.get('item_id') or '', 'name': ref['name'],
                'size': ref.get('size', 0), 'is_image': bool(ref.get('is_image', False)),
                'url': f'/api/pre-shift-checks/files/{ref["id"]}', 'can_delete': bool(can_delete)}

    def _public_file(self, frow, actor, task, current):
        return {'id': frow['id'], 'item_id': frow.get('item_id') or '', 'name': frow['name'],
                'size': frow['size'], 'is_image': bool(frow['is_image']),
                'url': f'/api/pre-shift-checks/files/{frow["id"]}',
                'can_delete': self._can_delete_file(actor, frow, task, current)}

    def _state_handover(self, conn, actor, task, current):
        row = conn.execute("SELECT * FROM files WHERE kind='handover' AND day=? AND scope=? AND shift=? ORDER BY created LIMIT 1",
                           (task['day'], task['scope'], task['shift'])).fetchone()
        if not row:
            return None
        return {'id': row['id'], 'item_id': 'handover', 'name': row['name'], 'size': row['size'],
                'is_image': bool(row['is_image']),
                'url': f'/api/pre-shift-checks/files/{row["id"]}',
                'can_delete': self._handover_can_delete(conn, actor, task, current, row)}

    def _handover_can_delete(self, conn, actor, task, current, row):
        if row['uploader_id'] != actor['id']:
            return False
        if row['submitted']:
            return False
        if self._any_submitted_in_shift(task['day'], task['scope'], task['shift'], conn):
            return False
        return self._is_roster_or_duty(actor, task)

    def _can_upload_handover(self, conn, actor, task, current):
        if not self._is_roster_or_duty(actor, task):
            return False
        h = conn.execute("SELECT id FROM files WHERE kind='handover' AND day=? AND scope=? AND shift=? LIMIT 1",
                         (task['day'], task['scope'], task['shift'])).fetchone()
        return h is None

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
            if row['deadline'] > current.timestamp() and (not definition['items'] or row['definition_version'] != definition['version']):
                raise CheckError('清单已更新或已清空，请重新读取。', 409)
            if not self.can_submit(actor, row, current):
                raise CheckError('仅本楼值班人员或值班账号可提交该次检查。', 403)
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
            # Snapshot file references for current items only (removed definition items never sync).
            current_items = {item['id'] for item in items}
            file_refs = [self._evidence_file(dict(r)) for r in conn.execute(
                "SELECT * FROM files WHERE task_id=? AND kind='item' AND submitted=0 ORDER BY created", (task_id,))]
            file_refs = [ref for ref in file_refs if ref['item_id'] in current_items]
            for ref in file_refs:
                conn.execute('UPDATE files SET submitted=1 WHERE id=?', (ref['id'],))
            evidence = {'item_files': file_refs}
            h = conn.execute("SELECT * FROM files WHERE kind='handover' AND day=? AND scope=? AND shift=? AND submitted=0 ORDER BY created LIMIT 1",
                             (row['day'], row['scope'], row['shift'])).fetchone()
            if h:
                conn.execute('UPDATE files SET submitted=1 WHERE id=?', (h['id'],))
                evidence['handover'] = self._evidence_file(dict(h))
            conn.execute('UPDATE tasks SET actor_id=?,actor_name=?,submitted=?,answers=?,cloud_key=?,evidence=? WHERE id=?',
                         (actor['id'], actor['name'], current.isoformat(), dump(results), cloud_key,
                          dump(evidence), task_id))
            conn.execute('INSERT INTO outbox(id,revision) VALUES(?,1) ON CONFLICT(id) DO UPDATE SET revision=revision+1,retry_at=0', (cloud_key,))
        self.wake.set()
        return {'completed': True, 'already_completed': False, 'actor_name': actor['name']}

    def stage_upload(self, actor, task_id, item_id, kind, name, current=None):
        """Validate upload intent before reading request body; returns a session dict."""
        current = current or now()
        if kind not in ('item', 'handover'):
            raise CheckError('附件类型无效。')
        name = _sanitize_name(name)
        ext = Path(name).suffix.lower()
        if ext not in ALLOWED_EXTS:
            raise CheckError('不允许该文件类型。')
        if kind == 'handover' and ext not in IMAGE_EXTS:
            raise CheckError('交接班照片仅支持图片。')
        if kind == 'item' and not task_id:
            raise CheckError('缺少检查任务标识。')
        with self.connection() as conn:
            task = conn.execute('SELECT * FROM tasks WHERE id=?', (task_id,)).fetchone() if task_id else None
            if task is None:
                raise CheckError('班前检查任务不存在，请刷新。', 404)
            authorized(actor, task['scope'])
            if kind == 'item':
                items = set(i['id'] for i in json.loads(task['items']))
                if item_id not in items:
                    raise CheckError('检查项不存在，请刷新。', 404)
                if not self.can_submit(actor, task, current):
                    raise CheckError('仅本楼值班人员或值班账号可上传检查附件。', 403)
            else:
                if not self._is_roster_or_duty(actor, task):
                    raise CheckError('仅本楼值班人员或值班账号可上传交接班照片。', 403)
        staging = self.staging_dir / f'{uuid.uuid4().hex}.upload'
        staging.write_bytes(b'')
        return {'path': str(staging), 'max_bytes': MAX_FILE_BYTES, 'kind': kind,
                'task_id': task_id, 'item_id': item_id or '', 'name': name,
                'actor_id': actor.get('id'), 'scope': task['scope'],
                'definition_version': task['definition_version']}

    def abort_upload(self, session):
        try:
            if session and session.get('path'):
                Path(session['path']).unlink(missing_ok=True)
        except OSError:
            pass

    def commit_upload(self, actor, session, size, current=None):
        """Re-validate and commit a streamed upload. ``size`` is final byte count."""
        current = current or now()
        staging = Path(session['path'])
        kind = session['kind']
        name = session['name']
        if not staging.exists():
            self.abort_upload(session)
            raise CheckError('上传文件不存在或已损坏。', 409)
        actual = staging.stat().st_size
        if size is None:
            size = actual
        if size <= 0 or actual <= 0:
            self.abort_upload(session)
            raise CheckError('上传文件为空。')
        if size != actual:
            self.abort_upload(session)
            raise CheckError('上传文件已变化，请重新上传。', 409)
        if actual > MAX_FILE_BYTES:
            self.abort_upload(session)
            raise CheckError('单个附件不能超过20MiB。')
        try:
            self._verify_file(staging, name)
        except Exception:
            self.abort_upload(session)
            raise CheckError('文件内容与扩展名不符或无效。')
        try:
            if kind == 'item':
                return self._commit_item_file(actor, session, staging, size, current)
            return self._commit_handover(actor, session, staging, size, current)
        except Exception:
            self.abort_upload(session)
            raise

    def _verify_file(self, path, name):
        ext = Path(name).suffix.lower()
        if ext in IMAGE_EXTS:
            self._verify_image(path, ext)
        else:
            self._verify_document(path, ext)

    def _verify_image(self, path, ext):
        from PIL import Image
        expected = {'.jpg': 'jpeg', '.jpeg': 'jpeg', '.png': 'png', '.webp': 'webp'}[ext]
        try:
            context = Image.open(path)
        except Image.DecompressionBombError:
            raise CheckError('图片像素过大，无法上传。')
        with context as image:
            fmt = (image.format or '').lower()
            if fmt != expected:
                raise CheckError('图片内容与扩展名不符。')
            # Size bound must be enforced before any full decode/verify so oversized
            # images are rejected without decoding and without touching the shared
            # Pillow MAX_IMAGE_PIXELS global (concurrent uploads must not race it).
            if image.width * image.height > 25_000_000:
                raise CheckError('图片像素过大，无法上传。')
            try:
                image.verify()
            except CheckError:
                raise
            except Exception:
                raise CheckError('图片文件已损坏或像素过大。')

    def _verify_document(self, path, ext):
        with open(path, 'rb') as fh:
            header = fh.read(8)
        if ext == '.pdf':
            if not header.startswith(b'%PDF-'):
                raise CheckError('PDF文件格式无效。')
        elif ext in ('.doc', '.xls'):
            if header[:8] != bytes.fromhex('D0CF11E0A1B11AE1'):
                raise CheckError('文件不是有效的Office文档。')
        elif ext in ('.docx', '.xlsx'):
            if header[:2] != b'PK':
                raise CheckError('文件不是有效的Office文档。')
        elif ext in ('.md', '.txt', '.csv'):
            with open(path, 'rb') as fh:
                data = fh.read()
            try:
                data.decode('utf-8')
            except UnicodeDecodeError:
                raise CheckError('文本文件编码无效。')
            if b'\x00' in data:
                raise CheckError('文本文件包含无效字符。')

    def _commit_item_file(self, actor, session, staging, size, current):
        if actor.get('id') != session.get('actor_id'):
            raise CheckError('上传身份已变化，请重新上传。', 409)
        with open(staging, 'rb') as fh:
            content_hash = hashlib.file_digest(fh, 'sha256').hexdigest()
        ext = Path(session['name']).suffix.lower()
        file_id = uuid.uuid4().hex
        final = self.files_dir / f'{file_id}{ext}'
        try:
            with self.connection(True) as conn:
                task = conn.execute('SELECT * FROM tasks WHERE id=?', (session['task_id'],)).fetchone()
                if task is None:
                    raise CheckError('班前检查任务不存在，请刷新。', 404)
                authorized(actor, task['scope'])
                if task['definition_version'] != session.get('definition_version'):
                    raise CheckError('清单版本已变化，请重新上传。', 409)
                if not self.can_submit(actor, task, current):
                    raise CheckError('仅本楼值班人员或值班账号可上传检查附件。', 403)
                items = {i['id'] for i in json.loads(task['items'])}
                if session.get('item_id') not in items:
                    raise CheckError('检查项不存在，请刷新。', 404)
                existing = conn.execute(
                    "SELECT * FROM files WHERE task_id=? AND item_id=? AND kind='item' AND uploader_id=? AND content_hash=? LIMIT 1",
                    (session['task_id'], session['item_id'], actor['id'], content_hash)).fetchone()
                if existing:
                    return self._public_dict(existing['id'], existing['item_id'], existing['name'],
                                             existing['size'], bool(existing['is_image']))
                count = conn.execute("SELECT COUNT(*) FROM files WHERE task_id=? AND item_id=? AND kind='item' AND submitted=0",
                                     (session['task_id'], session['item_id'])).fetchone()[0]
                if count >= MAX_FILES_PER_ITEM:
                    raise CheckError('每个检查项最多上传10个附件。')
                total = conn.execute("SELECT COALESCE(SUM(size),0) FROM files WHERE task_id=? AND kind='item' AND submitted=0",
                                     (session['task_id'],)).fetchone()[0]
                if total + size > MAX_TASK_BYTES:
                    raise CheckError('单个检查任务附件合计不能超过100MiB。')
                os.replace(staging, final)
                created = current.isoformat()
                conn.execute(
                    "INSERT INTO files(id,task_id,item_id,kind,day,scope,shift,name,size,is_image,disk_name,uploader_id,uploader_name,created,content_hash) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (file_id, session['task_id'], session.get('item_id'), 'item', task['day'], task['scope'],
                     task['shift'], session['name'], size, 1 if ext in IMAGE_EXTS else 0, final.name,
                     actor['id'], actor['name'], created, content_hash))
        except Exception:
            try:
                if final.exists():
                    final.unlink()
            except OSError:
                pass
            raise
        return self._public_dict(file_id, session.get('item_id'), session['name'], size,
                                 ext in IMAGE_EXTS)

    def _commit_handover(self, actor, session, staging, size, current):
        if actor.get('id') != session.get('actor_id'):
            raise CheckError('上传身份已变化，请重新上传。', 409)
        with open(staging, 'rb') as fh:
            content_hash = hashlib.file_digest(fh, 'sha256').hexdigest()
        ext = Path(session['name']).suffix.lower()
        file_id = uuid.uuid4().hex
        final = self.files_dir / f'{file_id}{ext}'
        try:
            with self.connection(True) as conn:
                task = conn.execute('SELECT * FROM tasks WHERE id=?', (session['task_id'],)).fetchone()
                if task is None:
                    raise CheckError('班前检查任务不存在，请刷新。', 404)
                authorized(actor, task['scope'])
                if not self._is_roster_or_duty(actor, task):
                    raise CheckError('仅本楼值班人员或值班账号可上传交接班照片。', 403)
                exists = conn.execute("SELECT * FROM files WHERE kind='handover' AND day=? AND scope=? AND shift=? LIMIT 1",
                                      (task['day'], task['scope'], task['shift'])).fetchone()
                if exists:
                    if exists['uploader_id'] == actor['id'] and exists['content_hash'] == content_hash:
                        return self._public_dict(exists['id'], '', exists['name'], exists['size'],
                                                 bool(exists['is_image']))
                    self.abort_upload(session)
                    raise CheckError('该班次交接班照片已存在，不能重复上传。', 409)
                os.replace(staging, final)
                created = current.isoformat()
                conn.execute(
                    "INSERT INTO files(id,task_id,item_id,kind,day,scope,shift,name,size,is_image,disk_name,uploader_id,uploader_name,created,content_hash) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (file_id, session['task_id'], '', 'handover', task['day'], task['scope'],
                     task['shift'], session['name'], size, 1 if ext in IMAGE_EXTS else 0, final.name,
                     actor['id'], actor['name'], created, content_hash))
                self._bump_shift_cloud(conn, task['day'], task['scope'], task['shift'])
        except Exception:
            try:
                if final.exists():
                    final.unlink()
            except OSError:
                pass
            raise
        self.wake.set()
        return self._public_dict(file_id, '', session['name'], size, True)

    def _bump_shift_cloud(self, conn, day, scope, shift):
        keys = {r['cloud_key'] for r in conn.execute(
            "SELECT cloud_key FROM tasks WHERE day=? AND scope=? AND shift=? AND submitted!='' AND cloud_key!=''",
            (day, scope, shift))}
        for key in keys:
            conn.execute('INSERT INTO outbox(id,revision) VALUES(?,1) ON CONFLICT(id) DO UPDATE SET revision=revision+1,retry_at=0',
                         (key,))

    def _public_dict(self, file_id, item_id, name, size, is_image):
        return {'id': file_id, 'item_id': item_id, 'name': name, 'size': size,
                'is_image': bool(is_image), 'url': f'/api/pre-shift-checks/files/{file_id}',
                'can_delete': False}

    def get_file(self, actor, file_id, current=None):
        with self.connection() as conn:
            row = conn.execute('SELECT * FROM files WHERE id=?', (file_id,)).fetchone()
        if not row:
            raise CheckError('附件不存在。', 404)
        authorized(actor, row['scope'])
        path = self.files_dir / row['disk_name']
        if not path.is_file():
            raise CheckError('附件文件已丢失。', 404)
        return {'path': str(path), 'name': row['name'], 'size': row['size'],
                'is_image': bool(row['is_image']), 'item_id': row['item_id'],
                'kind': row['kind'], 'scope': row['scope']}

    def delete_file(self, actor, file_id, current=None):
        current = current or now()
        with self.connection(True) as conn:
            row = conn.execute('SELECT * FROM files WHERE id=?', (file_id,)).fetchone()
            if not row:
                raise CheckError('附件不存在。', 404)
            authorized(actor, row['scope'])
            task = conn.execute('SELECT * FROM tasks WHERE id=?', (row['task_id'],)).fetchone()
            if row['kind'] == 'handover':
                if row['submitted']:
                    raise CheckError('已提交的交接班照片不能删除。', 403)
                if self._any_submitted_in_shift(row['day'], row['scope'], row['shift'], conn):
                    raise CheckError('该班次已有提交记录，交接班照片已归档不能删除。', 403)
                if row['uploader_id'] != actor['id']:
                    raise CheckError('仅上传人可删除交接班照片。', 403)
                if not self._is_roster_or_duty(actor, task):
                    raise CheckError('仅本楼值班人员可删除交接班照片。', 403)
            else:
                if row['submitted'] or (task and task['submitted']):
                    raise CheckError('已提交的检查附件不能删除或修改。', 403)
                if row['uploader_id'] != actor['id']:
                    raise CheckError('仅上传人可删除检查附件。', 403)
                if not self.can_submit(actor, task, current):
                    raise CheckError('当前账号不能删除该检查附件。', 403)
            disk = row['disk_name']
            conn.execute('DELETE FROM files WHERE id=?', (file_id,))
        path = self.files_dir / disk
        try:
            Path(path).unlink(missing_ok=True)
        except OSError:
            pass
        return {'ok': True}

    def notify(self, current):
        if not self.sender or not self.portal_url:
            return
        base = str(self.portal_url() or '').rstrip('/')
        if not base.startswith(('http://', 'https://')):
            self.last_error = '班前检查访问地址未配置，暂未推送。'
            return
        with self.connection() as conn:
            tasks = [dict(row) for row in conn.execute(
                "SELECT * FROM tasks WHERE submitted='' AND items!='[]' AND scope IN (SELECT scope FROM definitions WHERE items!='[]') AND due<=? AND deadline>?",
                (current.timestamp(), current.timestamp()))]
        for task in tasks:
            slot = int((current.timestamp() - task['due']) // 7200)
            for person in json.loads(task['people']):
                if self.stop_event.is_set():
                    return
                if self.wake.is_set():
                    self.wake.clear()
                    self.sync(current)
                key = f"{task['id']}:{slot}:{person['open_id']}"
                with self.connection(True) as conn:
                    latest = conn.execute('SELECT submitted,items FROM tasks WHERE id=?', (task['id'],)).fetchone()
                    if not latest or latest['submitted'] or latest['items'] == '[]':
                        break
                    row = conn.execute('SELECT * FROM deliveries WHERE id=?', (key,)).fetchone()
                    if row and (row['sent'] or row['lease'] > current.timestamp()):
                        continue
                    conn.execute('INSERT OR REPLACE INTO deliveries VALUES(?,?,0)', (key, current.timestamp() + 300))
                label = '班前检查' if slot == 0 else '班前检查未完成提醒'
                card = {'config': {'wide_screen_mode': True},
                    'header': {'template': 'blue' if slot == 0 else 'orange', 'title': {'tag': 'plain_text', 'content': label}},
                    'elements': [{'tag': 'div', 'text': {'tag': 'plain_text', 'content':
                        f"{task['day']} · {task['scope']}楼 · {task['shift']}班\n本楼任意一名值班人员或值班账号提交本次清单即可。请逐项检查，有异常时填写备注。"}},
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

    def _internal_files(self, conn, task):
        """Enriched internal file metadata for cloud sync (paths never public)."""
        refs, seen = [], set()
        if task['submitted'] and task['evidence']:
            ev = json.loads(task['evidence'])
            for ref in ev.get('item_files', []):
                if ref.get('id') and ref['id'] not in seen:
                    seen.add(ref['id'])
                    refs.append(dict(ref))
            if ev.get('handover'):
                h = ev['handover']
                if h.get('id') and h['id'] not in seen:
                    seen.add(h['id'])
                    refs.append({**h, 'kind': 'handover'})
        else:
            rows = conn.execute("SELECT * FROM files WHERE task_id=? AND kind='item' ORDER BY created", (task['id'],)).fetchall()
            for r in rows:
                ref = self._evidence_file(dict(r))
                if ref['id'] not in seen:
                    seen.add(ref['id'])
                    refs.append(ref)
        # Live shared handover for the shift always syncs (it may be uploaded after submit).
        handover = conn.execute(
            "SELECT * FROM files WHERE kind='handover' AND day=? AND scope=? AND shift=? ORDER BY created LIMIT 1",
            (task['day'], task['scope'], task['shift'])).fetchone()
        if handover:
            ref = self._evidence_file(dict(handover))
            if ref['id'] not in seen:
                seen.add(ref['id'])
                refs.append(ref)
        out = []
        for ref in refs:
            row = conn.execute('SELECT * FROM files WHERE id=?', (ref['id'],)).fetchone()
            if not row:
                raise CheckError('证据附件信息缺失，暂缓同步。')
            path = self.files_dir / row['disk_name']
            if not path.is_file():
                raise CheckError(f"证据附件文件缺失：{row['name']}")
            out.append({'file_id': ref['id'], 'task_id': task['id'], 'item_id': row['item_id'] or '',
                        'kind': ref.get('kind') or 'item', 'name': row['name'], 'size': row['size'],
                        'is_image': bool(row['is_image']), 'path': str(path),
                        'token': row['file_token'] or '', 'day': row['day'], 'scope': row['scope'],
                        'shift': row['shift']})
        return out

    def sync(self, current):
        if not self.cloud:
            return
        with self.connection() as conn:
            jobs = [dict(row) for row in conn.execute('SELECT * FROM outbox WHERE revision>synced AND retry_at<=? LIMIT 10', (current.timestamp(),))]
        for job in jobs:
            if self.stop_event.is_set():
                return
            try:
                with self.connection() as conn:
                    rows = [dict(row) for row in conn.execute('SELECT * FROM tasks WHERE cloud_key=? AND submitted!=\'\' ORDER BY due', (job['id'],))]
                    for task in rows:
                        task['_files'] = self._internal_files(conn, task)
            except Exception:
                with self.connection(True) as conn:
                    conn.execute('UPDATE outbox SET retry_at=?,error=? WHERE id=?',
                                 (current.timestamp() + 300, '本地附件缺失，同步需重试。', job['id']))
                continue
            if not rows:
                continue
            self.cloud.on_file_token = self._persist_file_token
            try:
                record_id = self.cloud.upsert(job['id'], rows, job['record_id'])
                with self.connection(True) as conn:
                    conn.execute("UPDATE outbox SET record_id=?,synced=?,detail_version=?,error='' WHERE id=?",
                                 (record_id, job['revision'], CLOUD_DETAIL_VERSION, job['id']))
            except Exception:
                with self.connection(True) as conn:
                    conn.execute('UPDATE outbox SET retry_at=?,error=? WHERE id=?',
                                 (current.timestamp() + 300, '本地已保存，飞书同步暂未完成，将自动重试。', job['id']))

    def _persist_file_token(self, file_id, token):
        with self.connection(True) as conn:
            conn.execute("UPDATE files SET file_token=? WHERE id=? AND file_token=''", (token, file_id))

    def tick(self, current=None):
        if not self.tick_lock.acquire(False):
            return
        try:
            self.wake.clear()
            current = current or now()
            self.last_error = ''
            self.sync(current)
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

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
import shutil
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from pathlib import Path

from .pre_shift_schedule import normalize_frequency, occurrences_for_date, upcoming_occurrences

TZ = dt.timezone(dt.timedelta(hours=8))
SCOPES = ('110', 'A', 'B', 'C', 'D', 'E', 'H')
DUTY = {'operator', 'shift_leader', 'duty'}
ROSTER_VERSION = 3
REMINDER_LEAD_SECONDS = 600
CLOUD_DETAIL_VERSION = 1

ALLOWED_EXTS = {'.jpg', '.jpeg', '.png', '.webp', '.pdf', '.doc', '.docx',
                '.xls', '.xlsx', '.md', '.txt', '.csv'}
IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.webp'}
MAX_FILE_BYTES = 20 * 1024 * 1024          # 20 MiB per file
MAX_TASK_BYTES = 100 * 1024 * 1024         # 100 MiB per task
MAX_FILES_PER_ITEM = 10
PHOTO_PREVIEW_SECONDS = 3600
MAX_PREVIEW_BYTES = 512 * 1024 * 1024


class CheckError(Exception):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


class CheckDeliveryUncertain(CheckError):
    pass


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
    def __init__(self, root=None, *, roster=None, cloud=None, sender=None, portal_url=None, photo_sender=None, recipient_directory=None):
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
        self.photo_sender = photo_sender
        self.recipient_directory = recipient_directory
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
                CREATE TABLE IF NOT EXISTS photo_deliveries(file_id TEXT PRIMARY KEY, recipient TEXT NOT NULL,
                    watermark TEXT NOT NULL, image_key TEXT DEFAULT '', sender_app_id TEXT DEFAULT '',
                    message_id TEXT DEFAULT '', sent INTEGER DEFAULT 0, first_attempt REAL DEFAULT 0,
                    lease REAL DEFAULT 0, retry_at REAL DEFAULT 0, error TEXT DEFAULT '');
                CREATE INDEX IF NOT EXISTS pending_photos ON photo_deliveries(retry_at,lease) WHERE sent=0;
                CREATE TABLE IF NOT EXISTS photo_previews(id TEXT PRIMARY KEY, actor_id TEXT NOT NULL,
                    scope TEXT NOT NULL, session TEXT NOT NULL, size INTEGER NOT NULL, content_hash TEXT NOT NULL,
                    expires REAL NOT NULL, lease REAL DEFAULT 0, request_hash TEXT DEFAULT '', result TEXT DEFAULT '');
                CREATE INDEX IF NOT EXISTS photo_preview_expiry ON photo_previews(expires,lease);
                CREATE TABLE IF NOT EXISTS photo_preferences(actor_id TEXT PRIMARY KEY, version INTEGER NOT NULL, style TEXT NOT NULL);
            ''')
            task_cols = {r[1] for r in conn.execute('PRAGMA table_info(tasks)')}
            delivery_cols = {r[1] for r in conn.execute('PRAGMA table_info(deliveries)')}
            for field, sql_type in {'message_id': "TEXT DEFAULT ''", 'sender_app_id': "TEXT DEFAULT ''",
                                    'error': "TEXT DEFAULT ''", 'first_attempt': 'REAL DEFAULT 0'}.items():
                if field not in delivery_cols:
                    conn.execute(f'ALTER TABLE deliveries ADD COLUMN {field} {sql_type}')
            if 'version' not in {r[1] for r in conn.execute('PRAGMA table_info(rosters)')}:
                conn.execute('ALTER TABLE rosters ADD COLUMN version INTEGER NOT NULL DEFAULT 0')
            if 'loaded_at' not in {r[1] for r in conn.execute('PRAGMA table_info(rosters)')}:
                conn.execute('ALTER TABLE rosters ADD COLUMN loaded_at REAL NOT NULL DEFAULT 0')
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
        recipient_options = None
        for item in items:
            if not isinstance(item, dict) or set(item) - {'id', 'text', 'frequency', 'fixed_recipients'}:
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
            recipients = item.get('fixed_recipients', previous.get(key, {}).get('fixed_recipients', []))
            if (not isinstance(recipients, list) or len(recipients) > 50
                    or any(not isinstance(oid, str) or not re.fullmatch(r'ou_[A-Za-z0-9]+', oid) for oid in recipients)):
                raise CheckError('固定接收人格式无效，请从人员列表中选择。')
            recipients = sorted(set(recipients))
            if recipients:
                if recipient_options is None:
                    if not self.recipient_directory:
                        raise CheckError('固定接收人目录暂未就绪，请稍后重试。', 503)
                    recipient_options = {p['open_id'] for p in self.recipient_directory(scope)}
                if not set(recipients) <= recipient_options:
                    raise CheckError('固定接收人已停用或没有本楼检查权限，请重新选择。', 403)
            result.append({'id': key, 'text': text.strip(), 'frequency': rule,
                           **({'fixed_recipients': recipients} if recipients else {})})
        with self.connection(True) as conn:
            if self.definition(scope, conn)['version'] != version:
                raise CheckError('清单已被其他管理人员修改，请重新读取。', 409)
            conn.execute('INSERT OR REPLACE INTO definitions VALUES(?,?,?,?,?)',
                         (scope, version + 1, dump(result), actor['id'], current.isoformat()))
            if (any(not i.get('fixed_recipients') for i in result)
                    and not any(not i.get('fixed_recipients') for i in stored['items'])):
                conn.execute('UPDATE rosters SET version=0')
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
        return list({p['open_id']: p for p in people if p.get('open_id')}.values())

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
        fixed_people = {}
        fixed_ids = {oid for item in all_items.values() for oid in item.get('fixed_recipients', [])}
        if fixed_ids:
            try:
                fixed_people = {p['open_id']: p for p in self.recipient_directory(scope)} if self.recipient_directory else {}
            except Exception:
                self.last_error = '固定接收人目录暂未就绪，将自动重试；未改发给排班人员。'
            else:
                if fixed_ids - fixed_people.keys():
                    self.last_error = '部分固定接收人暂不可用，请核对在岗状态及本楼检查权限；未改发给排班人员。'
        active_ids = set()
        for group in groups:
            rostered = self._people_for(conn, group['day'], scope, group['shift'])
            due = group['due']
            task_id = self._task_id(group['day'], scope, group['shift'], due)
            active_ids.add(task_id)
            subset, people = [], {}
            routed = any(all_items[iid].get('fixed_recipients') for iid in group['ids'] if iid in all_items)
            for iid in group['ids']:
                if iid not in all_items:
                    continue
                item = all_items[iid]
                assigned = ([fixed_people[oid] for oid in item['fixed_recipients'] if oid in fixed_people]
                            if item.get('fixed_recipients') else rostered)
                people.update((p['open_id'], p) for p in assigned)
                subset.append({'id': iid, 'text': item['text'],
                               **({'recipient_ids': [p['open_id'] for p in assigned]} if routed else {})})
            if not subset:
                continue
            items = dump(subset)
            due_ts = due.timestamp()
            deadline_ts = group['deadline'].timestamp()
            ppl = dump(list(people.values()))
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
                    groups = self._occurrence_groups([i for i in definition['items'] if not i.get('fixed_recipients')], scope, day, current + dt.timedelta(seconds=REMINDER_LEAD_SECONDS))
                    if any(g['due'] - dt.timedelta(seconds=REMINDER_LEAD_SECONDS) <= current < g['deadline'] and g['day'] == prior for g in groups):
                        return True
        return False

    def _ensure_roster(self, current, day):
        configured = set(self.configured_scopes(roster_only=True))
        if not configured:
            return
        with self.connection() as conn:
            row = conn.execute('SELECT * FROM rosters WHERE day=?', (day,)).fetchone()
            # Recheck once on entering each reminder window, not on every worker tick.
            due_refresh = row and conn.execute("""SELECT 1 FROM tasks WHERE day=? AND submitted=''
                AND items!='[]' AND scope IN (SELECT scope FROM definitions WHERE items!='[]')
                AND due<=? AND deadline>? AND due-?>? LIMIT 1""",
                (day, current.timestamp() + REMINDER_LEAD_SECONDS, current.timestamp(),
                 REMINDER_LEAD_SECONDS, row['loaded_at'])).fetchone()
        covered = {p['scope'] for p in json.loads(row['people'] or '[]')} if row else set()
        if (row and row['version'] == ROSTER_VERSION and row['people'] and not row['retry_at']
                and (day < current.date().isoformat() or configured <= covered and not due_refresh)):
            return
        if row and row['version'] == ROSTER_VERSION and row['retry_at'] > current.timestamp():
            return
        try:
            snapshot = self.roster(day)
            with self.connection(True) as conn:
                warnings = {scope: message for scope, message in snapshot.get('warnings', {}).items() if scope in configured}
                conn.execute('INSERT OR REPLACE INTO rosters(day,people,retry_at,error,version,loaded_at) VALUES(?,?,?,?,?,?)',
                             (day, dump(snapshot['people']), current.timestamp() + 300 if warnings else 0,
                              dump(warnings), ROSTER_VERSION, current.timestamp()))
        except Exception as exc:
            with self.connection(True) as conn:
                conn.execute('INSERT OR REPLACE INTO rosters(day,people,retry_at,error,version) VALUES(?,?,?,?,?)',
                             (day, '', current.timestamp() + 300,
                              (str(exc) if isinstance(exc, CheckError) else '排班读取未完成。') + '5分钟后重试；不会按不完整名单推送。', ROSTER_VERSION))

    def prepare(self, current):
        configured = self.configured_scopes()
        if not configured:
            return
        # Previous-night tasks remain durable through midnight; do not re-read yesterday's changing view
        # unless there is an open prior-day occurrence with no cached roster yet.
        today = current.date().isoformat()
        prior = (current.date() - dt.timedelta(days=1)).isoformat()
        if self.roster:
            self._ensure_roster(current, today)
            if self._needs_prior(current, prior):
                self._ensure_roster(current, prior)
        with self.connection(True) as conn:
            self.prepare_local(conn, current, configured)

    def configured_scopes(self, *, roster_only=False):
        with self.connection() as conn:
            return [row['scope'] for row in conn.execute("SELECT scope,items FROM definitions WHERE items!='[]'")
                    if not roster_only or any(not i.get('fixed_recipients') for i in json.loads(row['items']))]

    @classmethod
    def can_submit(cls, actor, task, current):
        return (not task['submitted'] and bool(json.loads(task['items']) if isinstance(task['items'], str) else task['items'])
                and cls._can_complete(actor, task))

    @staticmethod
    def _can_complete(actor, task):
        if task['scope'] not in scopes_for(actor):
            return False
        if can_manage(actor):
            return True
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
            return self._can_complete(actor, task)
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
            if not any(not item.get('fixed_recipients') for item in definition['items']):
                roster_error = ''
            return {'scope': scope, 'scopes': scopes_for(actor), 'can_manage': can_manage(actor),
                    'definition': definition, 'tasks': tasks,
                    'roster_ready': roster_ready, 'roster_error': (roster_error or self.last_error) if definition['items'] else '',
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
        rows, pending_count = [], 0
        for scope in scopes_for(actor):
            configured = bool(self.definition(scope, conn)['items'])
            pending_count += conn.execute("""SELECT COUNT(*) FROM tasks WHERE scope=? AND submitted=''
                AND items!='[]' AND due<=? AND (? OR deadline<=?)""",
                (scope, current.timestamp(), configured, current.timestamp())).fetchone()[0]
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
        return {'rows': rows, 'pending': pending_count}

    def _state_files(self, conn, actor, task, current):
        if task['submitted'] and task['evidence']:
            ev = json.loads(task['evidence'])
            refs = ev.get('item_files', [])
            return [{**self._public_ref(r, actor, task, current, can_delete=False), **self._photo_status(conn, r['id'], actor)}
                    for r in refs if r.get('kind') != 'handover']
        rows = conn.execute("SELECT * FROM files WHERE task_id=? AND kind='item' ORDER BY created", (task['id'],)).fetchall()
        out = []
        for row in rows:
            f = dict(row)
            out.append({**self._public_file(f, actor, task, current), **self._photo_status(conn, f['id'], actor)})
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
                'can_delete': self._handover_can_delete(conn, actor, task, current, row),
                **self._photo_status(conn, row['id'], actor)}

    def _photo_status(self, conn, file_id, actor):
        delivery = conn.execute('SELECT * FROM photo_deliveries WHERE file_id=?', (file_id,)).fetchone()
        if not delivery:
            return {}
        stamp = json.loads(delivery['watermark'])
        return {'watermarked': bool(stamp.get('applied', True) or stamp.get('existing')), 'watermark': stamp,
                **({'delivery_status': 'sent' if delivery['sent'] else 'pending',
                    'delivery_error': delivery['error']} if delivery['recipient'] == actor['id'] else {})}

    def _handover_can_delete(self, conn, actor, task, current, row):
        if row['uploader_id'] != actor['id']:
            return False
        if row['submitted']:
            return False
        if self._any_submitted_in_shift(task['day'], task['scope'], task['shift'], conn):
            return False
        return self._can_complete(actor, task)

    def _can_upload_handover(self, conn, actor, task, current):
        if not self._can_complete(actor, task):
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
                raise CheckError('仅本楼值班人员、值班账号或有该楼管理权限的上级可提交该次检查。', 403)
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

    def stage_upload(self, actor, task_id, item_id, kind, name, current=None, *, captured=False):
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
        if not isinstance(captured, bool) or captured and ext not in IMAGE_EXTS:
            raise CheckError('拍照仅支持图片。')
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
                    raise CheckError('仅本楼值班人员、值班账号或有该楼管理权限的上级可上传检查附件。', 403)
            else:
                if not self._can_complete(actor, task):
                    raise CheckError('仅本楼值班人员、值班账号或有该楼管理权限的上级可上传交接班照片。', 403)
        staging = self.staging_dir / f'{uuid.uuid4().hex}.upload'
        staging.write_bytes(b'')
        return {'path': str(staging), 'max_bytes': MAX_FILE_BYTES, 'kind': kind,
                'task_id': task_id, 'item_id': item_id or '', 'name': name,
                'actor_id': actor.get('id'), 'scope': task['scope'],
                'captured': captured,
                'definition_version': task['definition_version']}

    def abort_upload(self, session):
        try:
            if session and session.get('path'):
                Path(session['path']).unlink(missing_ok=True)
        except OSError:
            pass

    def _preview_path(self, session):
        path = Path(session['path']).resolve()
        if path.parent != self.staging_dir.resolve() or not re.fullmatch(r'[a-f0-9]{32}\.upload', path.name):
            raise CheckError('照片暂存路径无效。', 409)
        return path

    def cleanup_photo_previews(self, current=None):
        current = current or now()
        with self.connection(True) as conn:
            rows = conn.execute('SELECT id,session FROM photo_previews WHERE expires<? AND lease<=? LIMIT 100',
                                (current.timestamp(), current.timestamp())).fetchall()
            for row in rows:
                try:
                    path = self._preview_path(json.loads(row['session']))
                except (ValueError, KeyError, TypeError, CheckError):
                    conn.execute('DELETE FROM photo_previews WHERE id=?', (row['id'],))
                    continue
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    continue
                conn.execute('DELETE FROM photo_previews WHERE id=?', (row['id'],))
            referenced = set()
            for row in conn.execute('SELECT session FROM photo_previews'):
                try:
                    referenced.add(str(self._preview_path(json.loads(row[0]))))
                except (ValueError, KeyError, TypeError, CheckError):
                    continue
        for index, path in enumerate(self.staging_dir.glob('*.upload')):
            if index >= 500:
                break
            try:
                if str(path) not in referenced and path.stat().st_mtime < current.timestamp() - PHOTO_PREVIEW_SECONDS * 2:
                    path.unlink(missing_ok=True)
            except OSError:
                pass

    def photo_preferences(self, actor):
        from .pre_shift_photo import photo_style
        with self.connection() as conn:
            row = conn.execute('SELECT * FROM photo_preferences WHERE actor_id=?', (actor['id'],)).fetchone()
        if not row:
            return {'version': 0, 'style': {}}
        try:
            style = photo_style(json.loads(row['style']))
        except (ValueError, TypeError):
            return {'version': row['version'], 'style': {}, 'warning': '上次水印样式无法读取，本次使用默认样式。'}
        return {'version': row['version'], 'style': style}

    def retain_photo_preview(self, actor, session, size, current=None):
        current = current or now()
        self.cleanup_photo_previews(current)
        info = self.commit_upload(actor, session, size, current, preview=True)
        preferences = self.photo_preferences(actor)
        session = {**session, 'photo_inspection': {key: info[key] for key in ('original_time', 'time_origin', 'time_source')},
                   'preference_version': preferences['version']}
        path = self._preview_path(session)
        with path.open('rb') as stream:
            content_hash = hashlib.file_digest(stream, 'sha256').hexdigest()
        key = uuid.uuid4().hex
        with self.connection(True) as conn:
            count, total = conn.execute("SELECT COUNT(*),COALESCE(SUM(size),0) FROM photo_previews WHERE actor_id=? AND result=''", (actor['id'],)).fetchone()
            global_total = conn.execute("SELECT COALESCE(SUM(size),0) FROM photo_previews WHERE result=''").fetchone()[0]
            if count >= MAX_FILES_PER_ITEM or total + size > MAX_TASK_BYTES or global_total + size > MAX_PREVIEW_BYTES:
                raise CheckError('待确认照片较多，请先完成或取消已有照片后重试。', 409)
            conn.execute('INSERT INTO photo_previews(id,actor_id,scope,session,size,content_hash,expires) VALUES(?,?,?,?,?,?,?)',
                         (key, actor['id'], session['scope'], dump(session), size, content_hash, current.timestamp() + PHOTO_PREVIEW_SECONDS))
        return {**info, 'preview_id': key, 'preferences': preferences}

    def _photo_preview(self, conn, actor, key, current):
        if not isinstance(key, str) or not re.fullmatch(r'[a-f0-9]{32}', key):
            raise CheckError('照片预览不存在或已过期，请重新选择。', 404)
        row = conn.execute('SELECT * FROM photo_previews WHERE id=? AND actor_id=?', (key, actor['id'])).fetchone()
        if not row:
            raise CheckError('照片预览不存在或不属于当前账号。', 404)
        authorized(actor, row['scope'])
        if row['expires'] < current.timestamp():
            raise CheckError('照片预览已过期，请重新选择。', 410)
        return row

    def discard_photo_preview(self, actor, key, current=None):
        current = current or now()
        with self.connection(True) as conn:
            row = self._photo_preview(conn, actor, key, current)
            if row['result']:
                return {'cancelled': False, 'uploaded': True}
            if row['request_hash']:
                raise CheckError('照片已提交上传，请先核验列表中的原结果，不要重复上传。', 409)
            self._preview_path(json.loads(row['session'])).unlink(missing_ok=True)
            conn.execute('DELETE FROM photo_previews WHERE id=?', (key,))
        return {'cancelled': True}

    def _complete_photo_preview(self, actor, key, session, options, remember, result, current, lease):
        from .pre_shift_photo import photo_style
        with self.connection(True) as conn:
            row = conn.execute('SELECT lease FROM photo_previews WHERE id=? AND actor_id=?', (key, actor['id'])).fetchone()
            if not row or row['lease'] != lease:
                raise CheckError('原上传仍在核验，请稍候重试原预览。', 409)
            if remember and options['enabled'] and not options['existing'] and result.get('watermark', {}).get('applied'):
                row = conn.execute('SELECT version FROM photo_preferences WHERE actor_id=?', (actor['id'],)).fetchone()
                version = row['version'] if row else 0
                if version != session['preference_version']:
                    result['preference_warning'] = '照片已上传；另一设备已更新水印样式，本次未覆盖。'
                else:
                    conn.execute('INSERT OR REPLACE INTO photo_preferences VALUES(?,?,?)',
                                 (actor['id'], version + 1, dump(photo_style(options))))
            conn.execute('UPDATE photo_previews SET result=?,lease=0,expires=? WHERE id=?',
                         (dump(result), current.timestamp() + PHOTO_PREVIEW_SECONDS, key))

    def commit_photo_preview(self, actor, key, watermark, remember=True, current=None):
        from .pre_shift_photo import photo_options
        current = current or now()
        if type(remember) is not bool:
            raise CheckError('水印样式记忆参数无效。')
        try:
            options = photo_options(watermark)
            if options is None:
                raise ValueError('请先核对水印设置。')
        except ValueError as exc:
            raise CheckError(str(exc)) from None
        request_hash = hashlib.sha256(json.dumps([options, remember], sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        with self.connection(True) as conn:
            row = self._photo_preview(conn, actor, key, current)
            if row['request_hash'] and row['request_hash'] != request_hash:
                raise CheckError('该预览已提交过其他设置，请核验原照片，不要重复上传。', 409)
            if row['result']:
                result = json.loads(row['result'])
                if not conn.execute('SELECT 1 FROM files WHERE id=?', (result['id'],)).fetchone():
                    raise CheckError('原照片已被移除，请重新选择。', 409)
                return {**result, **self._photo_status(conn, result['id'], actor)}
            if row['lease'] > current.timestamp():
                raise CheckError('照片正在上传，请稍候重试原预览。', 409)
            session = json.loads(row['session'])
            source = self._preview_path(session)
            if not source.is_file():
                raise CheckError('照片暂存已丢失，请重新选择。', 410)
            lease = current.timestamp() + 180
            conn.execute('UPDATE photo_previews SET lease=?,request_hash=?,expires=? WHERE id=?',
                         (lease, request_hash, current.timestamp() + PHOTO_PREVIEW_SECONDS, key))
        work = {**session, 'path': str(self.staging_dir / f'{uuid.uuid4().hex}.upload')}
        try:
            with source.open('rb') as stream:
                if hashlib.file_digest(stream, 'sha256').hexdigest() != row['content_hash']:
                    raise CheckError('照片暂存已变化，请重新选择。', 409)
            shutil.copyfile(source, work['path'])
            # Fixed ordering preserves the old web form's file dedup identity.
            ordered = {field: options[field] for field in ('enabled', 'existing', 'scale', 'title', 'area', 'weather', 'location', 'time', 'logo', 'position') if field in options}
            work['photo_capture'] = {**session.get('photo_capture', {}), 'watermark': ordered}
            result = self.commit_upload(actor, work, row['size'], current)
            try:
                self._complete_photo_preview(actor, key, session, options, remember, result, current, lease)
            except Exception:
                logging.exception('Photo saved; preview receipt/preferences will recover on retry')
                result['preference_warning'] = '照片已上传，水印样式暂未保存；请勿重复上传。'
            else:
                try:
                    source.unlink(missing_ok=True)
                except OSError:
                    logging.warning('Saved photo preview source will be removed by expiry cleanup')
            return result
        except CheckError:
            raise
        except Exception:
            logging.exception('Photo preview commit requires retry')
            raise CheckError('照片上传结果暂未确认，请保留当前预览重试，不要重新上传。', 503) from None
        finally:
            self.abort_upload(work)
            try:
                with self.connection(True) as conn:
                    conn.execute('UPDATE photo_previews SET lease=0 WHERE id=? AND lease=?', (key, lease))
            except sqlite3.Error:
                logging.exception('Photo preview lease will expire automatically')

    def commit_upload(self, actor, session, size, current=None, *, preview=False):
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
            from .pre_shift_photo import inspect_photo, photo_options
            try:
                options = photo_options((session.get('photo_capture') or {}).get('watermark'))
                if options is not None and options['enabled'] and Path(name).suffix.lower() in IMAGE_EXTS:
                    from PIL import Image
                    with Image.open(staging) as image:
                        marker = image.info.get('comment', b'')
                    if isinstance(marker, bytes) and marker.startswith(b'ClipFlowWatermark:'):
                        options = {**options, 'enabled':False, 'existing':True}
            except ValueError as exc:
                raise CheckError(str(exc)) from None
            if preview:
                if Path(name).suffix.lower() not in IMAGE_EXTS:
                    raise CheckError('照片预览仅支持图片。')
                with self.connection() as conn:
                    task = conn.execute('SELECT * FROM tasks WHERE id=?',(session['task_id'],)).fetchone()
                    if not task or actor.get('id') != session.get('actor_id'):
                        raise CheckError('上传身份或任务已变化，请重新选择。',409)
                    authorized(actor,task['scope'])
                    if session['kind']=='item' and (not self.can_submit(actor,task,current)
                            or session['item_id'] not in {i['id'] for i in json.loads(task['items'])}):
                        raise CheckError('当前检查项不能上传照片。',403)
                    if session['kind']=='handover' and not self._can_complete(actor,task):
                        raise CheckError('当前账号不能上传交接班照片。',403)
                return inspect_photo(staging,session['scope'],current,session.get('photo_weather'),
                                     handover=kind=='handover',capture=session.get('photo_capture'))
            session['photo_options'] = options
            capture = {key: value for key, value in (session.get('photo_capture') or {}).items() if key != '_time_audit'}
            if options is not None and Path(name).suffix.lower() in IMAGE_EXTS:
                from PIL import Image
                from .pre_shift_photo import _photo_time, watermark_details, time_audit
                info = session.get('photo_inspection')
                if not info:
                    with Image.open(staging) as image:
                        original = _photo_time(image.getexif(), current)
                        details = watermark_details(session['scope'], current, exif=image.getexif(), capture=capture)
                    origin = 'exif' if original else 'camera' if details['time'] != '未获取' else 'unknown'
                    original = original or (details['time'].replace('.', '-') if details['time'] != '未获取' else '')
                    info = {'original_time': original, 'time_origin': origin,
                            'time_source': '照片拍摄信息' if original else '未识别拍摄时间'}
                display_time = options['time'] if options['enabled'] and not options['existing'] else info.get('original_time', '')
                session['photo_audit'] = time_audit(info, display_time, actor, current)
                capture['_time_audit'] = session['photo_audit']
            session['photo_capture'] = capture
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
        watermark = None
        rendered = self.staging_dir / f'{file_id}.jpg'
        name = session['name']
        options = session.get('photo_options')
        stamp_enabled = options['enabled'] and not options['existing'] if options is not None else session.get('captured')
        if options is not None:
            content_hash = hashlib.sha256((content_hash + dump(options)).encode()).hexdigest()
        if stamp_enabled:
            content_hash = hashlib.sha256(('camera-watermark-v1:' + content_hash).encode()).hexdigest()
            from .pre_shift_photo import render_handover
            try:
                watermark = render_handover(staging, rendered, session['scope'], current, session.get('photo_weather'), handover=False, capture=session.get('photo_capture'))
                ext, size = '.jpg', rendered.stat().st_size
                name = Path(name).stem[:100] + '_班前检查水印.jpg'
            except (ValueError, OSError) as exc:
                rendered.unlink(missing_ok=True)
                raise CheckError('检查照片水印生成失败：' + str(exc)) from exc
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
                    raise CheckError('仅本楼值班人员、值班账号或有该楼管理权限的上级可上传检查附件。', 403)
                items = {i['id'] for i in json.loads(task['items'])}
                if session.get('item_id') not in items:
                    raise CheckError('检查项不存在，请刷新。', 404)
                existing = conn.execute(
                    "SELECT * FROM files WHERE task_id=? AND item_id=? AND kind='item' AND uploader_id=? AND content_hash=? LIMIT 1",
                    (session['task_id'], session['item_id'], actor['id'], content_hash)).fetchone()
                if existing:
                    return {**self._public_dict(existing['id'], existing['item_id'], existing['name'],
                                             existing['size'], bool(existing['is_image'])), **self._photo_status(conn, existing['id'], actor)}
                count = conn.execute("SELECT COUNT(*) FROM files WHERE task_id=? AND item_id=? AND kind='item' AND submitted=0",
                                     (session['task_id'], session['item_id'])).fetchone()[0]
                if count >= MAX_FILES_PER_ITEM:
                    raise CheckError('每个检查项最多上传10个附件。')
                total = conn.execute("SELECT COALESCE(SUM(size),0) FROM files WHERE task_id=? AND kind='item' AND submitted=0",
                                     (session['task_id'],)).fetchone()[0]
                if total + size > MAX_TASK_BYTES:
                    raise CheckError('单个检查任务附件合计不能超过100MiB。')
                os.replace(rendered if watermark else staging, final)
                created = current.isoformat()
                conn.execute(
                    "INSERT INTO files(id,task_id,item_id,kind,day,scope,shift,name,size,is_image,disk_name,uploader_id,uploader_name,created,content_hash) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (file_id, session['task_id'], session.get('item_id'), 'item', task['day'], task['scope'],
                     task['shift'], name, size, 1 if ext in IMAGE_EXTS else 0, final.name,
                     actor['id'], actor['name'], created, content_hash))
                if watermark:
                    conn.execute('INSERT INTO photo_deliveries(file_id,recipient,watermark) VALUES(?,?,?)',
                                 (file_id, actor['id'], dump(watermark)))
                elif options is not None and ext in IMAGE_EXTS:
                    watermark = {'applied':False,'existing':options['existing'], 'audit': session.get('photo_audit', {})}
                    conn.execute('INSERT INTO photo_deliveries(file_id,recipient,watermark) VALUES(?,?,?)',
                                 (file_id, actor['id'], dump(watermark)))
        except Exception:
            try:
                if final.exists():
                    final.unlink()
            except OSError:
                pass
            raise
        finally:
            rendered.unlink(missing_ok=True)
            self.abort_upload(session)
        if watermark:
            self.wake.set()
        return {**self._public_dict(file_id, session.get('item_id'), name, size, ext in IMAGE_EXTS),
                **({'watermarked': bool(watermark.get('applied',True) or watermark.get('existing')), 'watermark': watermark, 'delivery_status': 'pending'} if watermark else {})}

    def _commit_handover(self, actor, session, staging, size, current):
        if actor.get('id') != session.get('actor_id'):
            raise CheckError('上传身份已变化，请重新上传。', 409)
        with open(staging, 'rb') as fh:
            content_hash = hashlib.file_digest(fh, 'sha256').hexdigest()
        file_id = uuid.uuid4().hex
        options = session.get('photo_options')
        stamp_enabled = options['enabled'] and not options['existing'] if options is not None else True
        if options is not None:
            content_hash = hashlib.sha256((content_hash + dump(options)).encode()).hexdigest()
        final = self.files_dir / (file_id + ('.jpg' if stamp_enabled else Path(session['name']).suffix.lower()))
        rendered = self.staging_dir / f'{file_id}.jpg'
        name = Path(session['name']).stem[:100] + '_交接班水印.jpg' if stamp_enabled else session['name']
        try:
            authorized(actor, session['scope'])
            from .pre_shift_photo import render_handover
            try:
                watermark = (render_handover(staging, rendered, session['scope'], current, session.get('photo_weather'), capture=session.get('photo_capture'))
                             if stamp_enabled else {'applied':False,'existing':options['existing'], 'audit': session.get('photo_audit', {})})
            except (ValueError, OSError) as exc:
                raise CheckError('交接班照片水印生成失败：' + str(exc)) from exc
            size = (rendered if stamp_enabled else staging).stat().st_size
            with self.connection(True) as conn:
                task = conn.execute('SELECT * FROM tasks WHERE id=?', (session['task_id'],)).fetchone()
                if task is None:
                    raise CheckError('班前检查任务不存在，请刷新。', 404)
                authorized(actor, task['scope'])
                if not self._can_complete(actor, task):
                    raise CheckError('仅本楼值班人员、值班账号或有该楼管理权限的上级可上传交接班照片。', 403)
                exists = conn.execute("SELECT * FROM files WHERE kind='handover' AND day=? AND scope=? AND shift=? LIMIT 1",
                                      (task['day'], task['scope'], task['shift'])).fetchone()
                if exists:
                    if exists['uploader_id'] == actor['id'] and exists['content_hash'] == content_hash:
                        return {**self._public_dict(exists['id'], '', exists['name'], exists['size'],
                                                 bool(exists['is_image'])), **self._photo_status(conn, exists['id'], actor)}
                    self.abort_upload(session)
                    raise CheckError('该班次交接班照片已存在，不能重复上传。', 409)
                os.replace(rendered if stamp_enabled else staging, final)
                created = current.isoformat()
                conn.execute(
                    "INSERT INTO files(id,task_id,item_id,kind,day,scope,shift,name,size,is_image,disk_name,uploader_id,uploader_name,created,content_hash) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (file_id, session['task_id'], '', 'handover', task['day'], task['scope'],
                     task['shift'], name, size, 1, final.name,
                     actor['id'], actor['name'], created, content_hash))
                conn.execute('INSERT INTO photo_deliveries(file_id,recipient,watermark) VALUES(?,?,?)',
                             (file_id, actor['id'], dump(watermark)))
                self._bump_shift_cloud(conn, task['day'], task['scope'], task['shift'])
        except Exception:
            try:
                if final.exists():
                    final.unlink()
            except OSError:
                pass
            raise
        finally:
            rendered.unlink(missing_ok=True)
            self.abort_upload(session)
        self.wake.set()
        return {**self._public_dict(file_id, '', name, size, True), 'watermarked': bool(watermark.get('applied',True) or watermark.get('existing')),
                'watermark': watermark, 'delivery_status': 'pending'}

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
                if not self._can_complete(actor, task):
                    raise CheckError('仅有本楼检查权限的上传人可删除未提交的交接班照片。', 403)
                delivery = conn.execute('SELECT lease FROM photo_deliveries WHERE file_id=?', (file_id,)).fetchone()
                if delivery and delivery['lease'] > current.timestamp():
                    raise CheckError('交接班照片正在发送给本人，请稍后再移除。', 409)
            else:
                if row['submitted'] or (task and task['submitted']):
                    raise CheckError('已提交的检查附件不能删除或修改。', 403)
                if row['uploader_id'] != actor['id']:
                    raise CheckError('仅上传人可删除检查附件。', 403)
                if not self.can_submit(actor, task, current):
                    raise CheckError('当前账号不能删除该检查附件。', 403)
                delivery = conn.execute('SELECT lease FROM photo_deliveries WHERE file_id=?', (file_id,)).fetchone()
                if delivery and delivery['lease'] > current.timestamp():
                    raise CheckError('水印照片正在发送给本人，请稍后再移除。', 409)
            disk = row['disk_name']
            conn.execute('DELETE FROM files WHERE id=?', (file_id,))
            conn.execute('DELETE FROM photo_deliveries WHERE file_id=?', (file_id,))
        path = self.files_dir / disk
        try:
            Path(path).unlink(missing_ok=True)
        except OSError:
            pass
        return {'ok': True}

    def notify(self, current):
        if not self.sender or not self.portal_url:
            return
        with self.connection() as conn:
            tasks = [dict(row) for row in conn.execute(
                    "SELECT * FROM tasks WHERE submitted='' AND items!='[]' AND scope IN (SELECT scope FROM definitions WHERE items!='[]') AND due<=? AND deadline>? ORDER BY due,id",
                (current.timestamp() + REMINDER_LEAD_SECONDS, current.timestamp()))]
        if not tasks:
            return
        base = str(self.portal_url() or '').rstrip('/')
        if not base.startswith(('http://', 'https://')):
            self.last_error = '班前检查访问地址未配置，暂未推送。'
            return
        for task in tasks:
            slot = max(0, int((current.timestamp() - task['due']) // 7200))
            for person in json.loads(task['people']):
                if self.stop_event.is_set():
                    return
                key = f"{task['id']}:{slot}:{person['open_id']}"
                with self.connection(True) as conn:
                    latest = conn.execute('SELECT * FROM tasks WHERE id=?', (task['id'],)).fetchone()
                    definition = self.definition(task['scope'], conn)
                    configured = {item['id'] for item in definition['items']}
                    if (not latest or latest['submitted'] or not configured
                            or latest['definition_version'] != definition['version']
                            or not any(item['id'] in configured for item in json.loads(latest['items']))):
                        break
                    items = [item for item in json.loads(latest['items'])
                             if 'recipient_ids' not in item or person['open_id'] in item['recipient_ids']]
                    if not items or person['open_id'] not in {p['open_id'] for p in json.loads(latest['people'])}:
                        continue
                    row = conn.execute('SELECT * FROM deliveries WHERE id=?', (key,)).fetchone()
                    if row and (row['sent'] or row['lease'] > current.timestamp()):
                        if not row['sent'] and row['error']:
                            self.last_error = row['error']
                        continue
                    if row and row['first_attempt'] and current.timestamp() - row['first_attempt'] >= 50 * 60:
                        self.last_error = '班前检查原消息回执未确认，请管理员核对；未重复新增同一轮消息。'
                        conn.execute('UPDATE deliveries SET error=? WHERE id=?', (self.last_error, key))
                        continue
                    conn.execute('INSERT OR IGNORE INTO deliveries(id) VALUES(?)', (key,))
                    conn.execute('UPDATE deliveries SET lease=?,first_attempt=CASE WHEN first_attempt=0 THEN ? ELSE first_attempt END WHERE id=?',
                                 (current.timestamp() + 300, current.timestamp(), key))
                label = '班前检查预提醒' if slot == 0 and current.timestamp() < task['due'] else '班前检查' if slot == 0 else '班前检查未完成提醒'
                summary = '\n'.join(f"{index + 1}. {item['text']}" for index, item in enumerate(items[:8]))
                if len(items) > 8:
                    summary += f'\n其余{len(items) - 8}项请打开清单查看。'
                due_text = dt.datetime.fromtimestamp(task['due'], TZ).strftime('%m-%d %H:%M')
                card = {'config': {'wide_screen_mode': True},
                    'header': {'template': 'blue' if slot == 0 else 'orange', 'title': {'tag': 'plain_text', 'content': label}},
                    'elements': [{'tag': 'div', 'text': {'tag': 'plain_text', 'content':
                        f"{task['day']} · {task['scope']}楼 · {task['shift']}班\n计划检查：{due_text}，共{len(items)}项\n{summary}\n本楼值班人员、值班账号或有该楼管理权限的上级，任意一人提交本次清单即可。其他时段的清单仍需分别完成。请逐项检查，有异常时填写备注。"}},
                        {'tag': 'action', 'actions': [{'tag': 'button', 'type': 'primary', 'text': {'tag': 'plain_text', 'content': '打开班前检查'},
                            'url': base + '/assets/workspace-v2/index.html?__page=/&pre_shift=1'}]}]}
                try:
                    result = self.sender(person['open_id'], card, 'pre-shift:' + key)
                    if not result[0]:
                        raise CheckError(str(result[1] or '班前检查消息发送失败，稍后重试。'))
                    receipts = result[2] if len(result) > 2 and isinstance(result[2], list) else []
                    receipt = next((r for r in receipts if r.get('open_id') == person['open_id']
                                    and re.fullmatch(r'om_[A-Za-z0-9]+', str(r.get('message_id') or ''))
                                    and r.get('sender_app_id')), None)
                    if not receipt:
                        raise CheckDeliveryUncertain('班前检查未取得有效投递回执，等待原任务核验。')
                    with self.connection(True) as conn:
                        conn.execute("UPDATE deliveries SET sent=1,lease=0,error='',message_id=?,sender_app_id=? WHERE id=?",
                                     (receipt['message_id'], receipt['sender_app_id'], key))
                except Exception as exc:
                    self.last_error = str(exc) if isinstance(exc, CheckError) else '班前检查消息暂未确认送达，后台将核验原任务。'
                    with self.connection(True) as conn:
                        conn.execute('UPDATE deliveries SET error=?,first_attempt=CASE WHEN ? THEN 0 ELSE first_attempt END WHERE id=?',
                                     (self.last_error, isinstance(exc, CheckError) and not isinstance(exc, CheckDeliveryUncertain), key))

    def deliver_photos(self, current):
        if not self.photo_sender:
            return
        for _ in range(2):
            if self.stop_event.is_set():
                return
            with self.connection(True) as conn:
                row = conn.execute('''SELECT p.*,f.disk_name,f.name FROM photo_deliveries p
                    JOIN files f ON f.id=p.file_id WHERE p.sent=0 AND p.retry_at<=? AND p.lease<=?
                    ORDER BY f.created,p.file_id LIMIT 1''', (current.timestamp(), current.timestamp())).fetchone()
                if row is None:
                    return
                photo = dict(row)
                # Feishu message UUID dedup lasts one hour; never blindly retry past it.
                if photo['first_attempt'] and current.timestamp() - photo['first_attempt'] >= 50 * 60:
                    conn.execute('UPDATE photo_deliveries SET retry_at=?,error=? WHERE file_id=?',
                                 (current.timestamp() + 86400, '发送回执长时间未确认，请管理员核对原消息；照片已保留，未重复发送。', photo['file_id']))
                    continue
                conn.execute('UPDATE photo_deliveries SET lease=? WHERE file_id=?', (current.timestamp() + 120, photo['file_id']))

            def checkpoint(image_key, app_id, file_id=photo['file_id']):
                with self.connection(True) as conn:
                    conn.execute('''UPDATE photo_deliveries SET image_key=?,sender_app_id=?,
                        first_attempt=CASE WHEN first_attempt=0 THEN ? ELSE first_attempt END WHERE file_id=? AND sent=0''',
                        (image_key, app_id, current.timestamp(), file_id))

            try:
                path = self.files_dir / photo['disk_name']
                if not path.is_file():
                    raise CheckError('交接班照片文件缺失，请管理员核对。')
                message_id = self.photo_sender(photo['recipient'], {**photo, 'path': str(path)},
                                               'pre-shift-photo:' + photo['file_id'], checkpoint)
                if not isinstance(message_id, str) or not re.fullmatch(r'om_[A-Za-z0-9]+', message_id):
                    raise CheckError('飞书未返回有效图片消息回执。')
                with self.connection(True) as conn:
                    conn.execute("UPDATE photo_deliveries SET sent=1,message_id=?,lease=0,error='' WHERE file_id=?",
                                 (message_id, photo['file_id']))
            except Exception as exc:
                error = str(exc) if isinstance(exc, CheckError) else '照片已保存，飞书暂未确认送达，后台将继续核验原发送。'
                with self.connection(True) as conn:
                    conn.execute('UPDATE photo_deliveries SET retry_at=?,lease=0,error=? WHERE file_id=?',
                                 (current.timestamp() + 300, error, photo['file_id']))
        with self.connection() as conn:
            if conn.execute('SELECT 1 FROM photo_deliveries WHERE sent=0 AND retry_at<=? AND lease<=? LIMIT 1',
                            (current.timestamp(), current.timestamp())).fetchone():
                self.wake.set()

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
            self.cleanup_photo_previews(current)
            self.prepare(current)
            self.notify(current)
            self.sync(current)
            self.deliver_photos(current)
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

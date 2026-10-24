"""Durable minute batches of already-sanitized diagnostics; no network or workers."""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import time
import uuid


class LeaseLostError(RuntimeError):
    """The caller no longer owns this batch's live upload lease."""


def _json(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(',', ':'), allow_nan=False)


def batch_digest(batch):
    payload = {key: batch[key] for key in ('id', 'machine_id', 'started_at', 'ended_at', 'error_count', 'events')}
    return hashlib.sha256(_json(payload).encode('utf-8')).hexdigest()


def _seconds(value):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise ValueError('Time must be a finite nonnegative number')
    return float(value)


def _now(value=None):
    return _seconds(time.time() if value is None else value)


def _text(value, name, *, empty=False):
    if (not isinstance(value, str) or not empty and not value
            or len(value) > 512 or any(ord(char) < 32 for char in value)):
        raise ValueError(name + ' must be a bounded string')
    return value


class DiagnosticStore:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.db_path = self.root / 'diagnostics.sqlite3'
        with self._connection() as conn:
            deadline = time.monotonic() + .5
            while True:
                try:
                    conn.execute('PRAGMA journal_mode=WAL').fetchone()
                    break
                except sqlite3.OperationalError as exc:
                    # Simultaneous first opens can bypass SQLite's busy timeout here.
                    if getattr(exc, 'sqlite_errorcode', None) not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED) or time.monotonic() >= deadline:
                        raise
                    time.sleep(.01)
            conn.execute('BEGIN IMMEDIATE')
            conn.execute('CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)')
            conn.execute("""CREATE TABLE IF NOT EXISTS batches(
                id TEXT PRIMARY KEY,
                state TEXT NOT NULL CHECK(state IN ('open','pending','leased','done')),
                started_at REAL NOT NULL, ended_at REAL,
                error_count INTEGER NOT NULL DEFAULT 0,
                payload_hash TEXT NOT NULL DEFAULT '',
                attempts INTEGER NOT NULL DEFAULT 0, next_at REAL NOT NULL DEFAULT 0,
                lease_until REAL NOT NULL DEFAULT 0, lease_token TEXT NOT NULL DEFAULT '',
                file_token TEXT NOT NULL DEFAULT '', auth_fingerprint TEXT NOT NULL DEFAULT '',
                record_id TEXT NOT NULL DEFAULT '', last_error TEXT NOT NULL DEFAULT '')""")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS one_open_batch ON batches(state) WHERE state='open'")
            conn.execute('CREATE INDEX IF NOT EXISTS batch_due ON batches(state,next_at,lease_until)')
            conn.execute("""CREATE TABLE IF NOT EXISTS events(
                batch_id TEXT NOT NULL REFERENCES batches(id), event_json TEXT NOT NULL,
                count INTEGER NOT NULL, first_at REAL NOT NULL, last_at REAL NOT NULL,
                PRIMARY KEY(batch_id,event_json))""")
            conn.execute("INSERT OR IGNORE INTO meta VALUES('machine_id',?)", (str(uuid.uuid4()),))
            self.machine_id = conn.execute("SELECT value FROM meta WHERE key='machine_id'").fetchone()[0]

    @contextmanager
    def _connection(self, *, write=False, read=False):
        conn = sqlite3.connect(self.db_path, timeout=.5, isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute('PRAGMA foreign_keys=ON')
            conn.execute('PRAGMA synchronous=FULL')
            if write or read:
                conn.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _payload(self, conn, row):
        events = []
        for item in conn.execute('SELECT * FROM events WHERE batch_id=? ORDER BY first_at,event_json', (row['id'],)):
            events.append({**json.loads(item['event_json']), 'count': item['count'],
                           'first_at': item['first_at'], 'last_at': item['last_at']})
        return {'id': row['id'], 'machine_id': self.machine_id, 'started_at': row['started_at'],
                'ended_at': row['ended_at'], 'error_count': row['error_count'], 'events': events}

    def _batch(self, conn, identity):
        row = conn.execute('SELECT * FROM batches WHERE id=?', (identity,)).fetchone()
        return {**dict(row), **self._payload(conn, row)} if row is not None else None

    def _freeze_due(self, conn, current):
        row = conn.execute("SELECT * FROM batches WHERE state='open' AND started_at+60<=?", (current,)).fetchone()
        if row is not None:
            ended = row['started_at'] + 60
            payload = self._payload(conn, {**dict(row), 'ended_at': ended})
            conn.execute("UPDATE batches SET state='pending',ended_at=?,payload_hash=?,next_at=? WHERE id=?",
                         (ended, batch_digest(payload), ended, row['id']))

    def append(self, event, now=None):
        """Return the batch ID; occurrence times come only from the store clock."""
        if not isinstance(event, dict) or any(not isinstance(key, str) for key in event):
            raise ValueError('Event must be a JSON object with string keys')
        if {'count', 'first_at', 'last_at'} & event.keys():
            raise ValueError('Event contains reserved aggregation fields')
        event_json = _json({key: value for key, value in event.items()
                            if key not in {'at', 'timestamp', 'occurred_at'}})
        with self._connection(write=True) as conn:
            current = _now(now)
            self._freeze_due(conn, current)
            row = conn.execute("SELECT id FROM batches WHERE state='open'").fetchone()
            identity = row['id'] if row else self.machine_id + ':' + uuid.uuid4().hex
            if row is None:
                conn.execute("INSERT INTO batches(id,state,started_at) VALUES(?,'open',?)", (identity, current))
            conn.execute("""INSERT INTO events VALUES(?,?,1,?,?)
                ON CONFLICT(batch_id,event_json) DO UPDATE SET count=count+1,
                first_at=MIN(first_at,excluded.first_at),last_at=MAX(last_at,excluded.last_at)""",
                         (identity, event_json, current, current))
            conn.execute('UPDATE batches SET error_count=error_count+1 WHERE id=?', (identity,))
        return identity

    def claim(self, now=None, lease_seconds=300):
        """Atomically claim one due retry before freezing/claiming the open batch."""
        duration = _seconds(lease_seconds)
        if duration <= 0:
            raise ValueError('Lease duration must be positive')
        with self._connection(write=True) as conn:
            current = _now(now)
            until = _seconds(current + duration)
            conn.execute("""UPDATE batches SET state='pending',next_at=lease_until,lease_until=0,lease_token=''
                            WHERE state='leased' AND lease_until<=?""", (current,))
            due = "SELECT id FROM batches WHERE state='pending' AND next_at<=? ORDER BY next_at,started_at,id LIMIT 1"
            row = conn.execute(due, (current,)).fetchone()
            if row is None:
                self._freeze_due(conn, current)
                row = conn.execute(due, (current,)).fetchone()
            if row is None:
                return None
            conn.execute("UPDATE batches SET state='leased',attempts=attempts+1,lease_token=?,lease_until=? WHERE id=?",
                         (uuid.uuid4().hex, until, row['id']))
            return self._batch(conn, row['id'])

    @staticmethod
    def _owned(conn, identity, lease_token, current):
        row = conn.execute("SELECT * FROM batches WHERE id=? AND state='leased' AND lease_token=? AND lease_until>?",
                           (identity, lease_token, current)).fetchone()
        if row is None:
            raise LeaseLostError('Diagnostic batch lease is no longer owned')
        return row

    def checkpoint(self, id, lease_token, *, file_token, auth_fingerprint):
        _text(file_token, 'file_token', empty=True)
        _text(auth_fingerprint, 'auth_fingerprint', empty=True)
        with self._connection(write=True) as conn:
            self._owned(conn, id, lease_token, _now())
            conn.execute('UPDATE batches SET file_token=?,auth_fingerprint=? WHERE id=?', (file_token, auth_fingerprint, id))

    def complete(self, id, lease_token, record_id):
        _text(record_id, 'record_id')
        with self._connection(write=True) as conn:
            self._owned(conn, id, lease_token, _now())
            conn.execute("UPDATE batches SET state='done',record_id=?,last_error='',lease_until=0,lease_token='' WHERE id=?",
                         (record_id, id))

    def fail(self, id, lease_token, code, now=None):
        if not isinstance(code, str) or re.fullmatch(r'[A-Za-z0-9_.:-]{1,80}', code) is None:
            raise ValueError('Failure code must be a bounded safe code, not exception text')
        with self._connection(write=True) as conn:
            current = _now(now)
            row = self._owned(conn, id, lease_token, current)
            delay = min(900, 60 * 2 ** min(max(row['attempts'] - 1, 0), 4))
            conn.execute("""UPDATE batches SET state='pending',next_at=?,last_error=?,lease_until=0,lease_token=''
                            WHERE id=?""", (_seconds(current + delay), code, id))

    def next_delay(self, now=None):
        with self._connection(read=True) as conn:
            current = _now(now)
            due = conn.execute("""SELECT MIN(CASE state WHEN 'open' THEN started_at+60
                WHEN 'pending' THEN next_at WHEN 'leased' THEN lease_until END)
                FROM batches WHERE state!='done'""").fetchone()[0]
        return 60.0 if due is None else min(60.0, max(0.0, due - current))

    def get_batch(self, id):
        with self._connection(read=True) as conn:
            return self._batch(conn, id)

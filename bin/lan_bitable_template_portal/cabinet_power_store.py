"""Cabinet-only persistence. Each building owns its database and write lock."""
from __future__ import annotations
import copy
import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager, ExitStack
from pathlib import Path
from .cabinet_power_excel import CabinetError

_LOCKS = {}
_LOCKS_GUARD = threading.Lock()


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


class CabinetStore:
    def __init__(self, root):
        self.root = Path(root)
        self._ready = set()

    def path(self, scope):
        if scope not in tuple("ABCDE"): raise CabinetError("楼栋无效")
        return self.root / (scope + ".sqlite3")

    def lock(self, scope):
        key = str(self.path(scope).resolve())
        with _LOCKS_GUARD: return _LOCKS.setdefault(key, threading.RLock())

    @contextmanager
    def locked(self, scopes):
        with ExitStack() as stack:
            for scope in sorted(set(scopes)):
                if scope: stack.enter_context(self._file_lock(scope))
            yield

    @contextmanager
    def _file_lock(self, scope):
        with self.lock(scope):
            self.root.mkdir(parents=True, exist_ok=True)
            with self.path(scope).with_suffix(".lock").open("a+b") as handle:
                if not handle.seek(0, 2): handle.write(b"0"); handle.flush()
                deadline = time.monotonic() + 30
                while True:
                    handle.seek(0)
                    try:
                        if os.name == "nt":
                            import msvcrt
                            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                        else:
                            import fcntl
                            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except OSError:
                        if time.monotonic() >= deadline: raise CabinetError("该楼正在保存，请稍后重试", 409)
                        time.sleep(.05)
                try: yield
                finally:
                    handle.seek(0)
                    if os.name == "nt": msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                    else: fcntl.flock(handle, fcntl.LOCK_UN)

    @contextmanager
    def connect(self, scope):
        path = self.path(scope)
        self.root.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            if scope not in self._ready:
                with self.lock(scope):
                    conn.execute("PRAGMA journal_mode=WAL")
                    conn.executescript("""
                        CREATE TABLE IF NOT EXISTS records (
                            record_id TEXT PRIMARY KEY, room TEXT NOT NULL, rack TEXT NOT NULL,
                            ordinal INTEGER NOT NULL, payload TEXT NOT NULL);
                        CREATE INDEX IF NOT EXISTS records_cabinet ON records(room,rack);
                        CREATE TABLE IF NOT EXISTS inventory (
                            room TEXT NOT NULL, rack TEXT NOT NULL, payload TEXT NOT NULL,
                            PRIMARY KEY(room,rack));
                        CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, payload TEXT NOT NULL);
                        CREATE TABLE IF NOT EXISTS documents (key TEXT PRIMARY KEY, payload TEXT NOT NULL);
                    """)
                    if scope in ("D", "E"):
                        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS one_cabinet ON records(room,rack)")
                    conn.commit(); self._ready.add(scope)
            conn.execute("PRAGMA synchronous=FULL")
            yield conn
        finally: conn.close()

    @staticmethod
    def _put(conn, table, key, value):
        conn.execute(f"INSERT OR REPLACE INTO {table}(key,payload) VALUES (?,?)", (key, encode(value)))

    def document(self, scope, key, value=None):
        with self.connect(scope) as conn:
            if value is not None:
                with conn: self._put(conn, "documents", key, value)
                return value
            row = conn.execute("SELECT payload FROM documents WHERE key=?", (key,)).fetchone()
            return json.loads(row[0]) if row else None

    def documents(self, scope, prefix, pending_only=False):
        with self.connect(scope) as conn:
            condition=" AND COALESCE(json_extract(payload,'$.status'),'') NOT IN ('completed','cancelled')" if pending_only else ""
            return [json.loads(r[0]) for r in conn.execute("SELECT payload FROM documents WHERE key LIKE ?"+condition, (prefix + "%",))]

    def version(self, scope):
        with self.connect(scope) as conn:
            row = conn.execute("SELECT payload FROM meta WHERE key='version'").fetchone()
            return int(row[0]) if row else 0

    @staticmethod
    def _version(conn):
        row = conn.execute("SELECT payload FROM meta WHERE key='version'").fetchone()
        version = int(row[0]) + 1 if row else 1
        CabinetStore._put(conn, "meta", "version", version)
        CabinetStore._put(conn, "meta", "updated_at", time.strftime("%Y-%m-%d %H:%M:%S"))

    def load(self, scope):
        with self.connect(scope) as conn:
            conn.execute("BEGIN")
            meta = {r[0]: json.loads(r[1]) for r in conn.execute("SELECT key,payload FROM meta")}
            config = meta.get("config")
            if not config: return None
            config["inventory"] = [json.loads(r[0]) for r in conn.execute("SELECT payload FROM inventory ORDER BY room,rack")]
            records = [dict(json.loads(r["payload"]), ordinal=r["ordinal"]) for r in conn.execute("SELECT * FROM records ORDER BY ordinal,record_id")]
            return {"config": config, "records": records, "baseline": meta.get("baseline", []),
                    "version": meta["version"], "updated_at": meta["updated_at"]}

    @staticmethod
    def _record(conn, record, ordinal=None):
        from .cabinet_power_excel import room_code,text_value
        fields=record["fields"]; scope=text_value(fields.get("楼栋")).replace("楼","")
        room,_=room_code(text_value(fields.get("包间系统名称")),scope)
        op={"record_id":record["record_id"],"room":room,"rack":text_value(fields.get("机架")).upper()}
        if ordinal is None:
            old=conn.execute("SELECT ordinal FROM records WHERE record_id=?", (op["record_id"],)).fetchone()
            ordinal = old[0] if old else conn.execute("SELECT COALESCE(MAX(ordinal),0)+1 FROM records").fetchone()[0]
        conn.execute("INSERT INTO records VALUES(?,?,?,?,?) ON CONFLICT(record_id) DO UPDATE SET room=excluded.room,rack=excluded.rack,ordinal=excluded.ordinal,payload=excluded.payload",
                     (op["record_id"], op["room"], op["rack"], ordinal, encode(record)))

    def replace(self, scope, config, records, baseline, extend_baseline=False):
        with self.connect(scope) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            previous = {r[0]: r[1] for r in conn.execute("SELECT record_id,ordinal FROM records")}
            conn.execute("DELETE FROM records"); conn.execute("DELETE FROM inventory")
            ordinal=max(previous.values(),default=0)
            for record in records:
                if record["record_id"] not in previous: ordinal+=1
                self._record(conn,record,previous.get(record["record_id"],ordinal))
            for rack in config["inventory"]:
                conn.execute("INSERT INTO inventory VALUES(?,?,?)", (rack["room"], rack["rack"], encode(rack)))
            self._put(conn, "meta", "config", {k:v for k,v in config.items() if k not in ("inventory", "path")})
            prior_baseline=conn.execute("SELECT payload FROM meta WHERE key='baseline'").fetchone()
            if not prior_baseline or extend_baseline: self._put(conn,"meta","baseline",sorted(set(baseline)|set(json.loads(prior_baseline[0]) if prior_baseline else [])))
            self._version(conn)

    def commit_operation(self, scope, journal, record=None, inventory=None, remove_id="", complete=False, baseline_ids=()):
        with self.connect(scope) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            if record: self._record(conn, record)
            if inventory:
                conn.execute("INSERT OR REPLACE INTO inventory VALUES(?,?,?)", (inventory["room"], inventory["rack"], encode(inventory)))
            if remove_id: conn.execute("DELETE FROM records WHERE record_id=?", (remove_id,))
            if baseline_ids:
                row=conn.execute("SELECT payload FROM meta WHERE key='baseline'").fetchone()
                self._put(conn,"meta","baseline",sorted(set(json.loads(row[0]) if row else [])|set(baseline_ids)))
            self._version(conn)
            final = copy.deepcopy(journal)
            if complete: final.update(status="completed", error="", error_stage="", completed_at=time.time())
            self._put(conn, "documents", "write:" + journal["operation_id"], final)
        return final

    def backup(self, scope, destination):
        target = Path(destination); target.parent.mkdir(parents=True, exist_ok=True)
        with self.connect(scope) as conn, sqlite3.connect(target) as out: conn.backup(out)
        return str(target)

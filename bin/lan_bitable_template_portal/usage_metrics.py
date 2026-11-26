"""Durable page visit receipts and daily counters. No user or record identifiers."""
import json
import logging
import sqlite3
import time
import uuid
from collections import Counter
from contextlib import closing
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path


_DB_NAME = "page_usage.sqlite3"
_BEIJING = timezone(timedelta(hours=8))
WEB_CLIENTS = frozenset({"web_legacy", "web_v2", "web_standalone"})
MAX_BATCH = 50
_SCHEMA = """
CREATE TABLE IF NOT EXISTS receipts (id BLOB PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS daily (
  day TEXT NOT NULL, client TEXT NOT NULL, module TEXT NOT NULL, page TEXT NOT NULL,
  module_label TEXT NOT NULL, page_label TEXT NOT NULL, key TEXT NOT NULL UNIQUE,
  count INTEGER NOT NULL, synced INTEGER NOT NULL DEFAULT 0, record_id TEXT NOT NULL DEFAULT '',
  failures INTEGER NOT NULL DEFAULT 0, next_try REAL NOT NULL DEFAULT 0,
  PRIMARY KEY(day,client,module,page)
);
CREATE INDEX IF NOT EXISTS pending_usage ON daily(next_try,day) WHERE synced < count;
"""


@lru_cache(maxsize=2)
def _catalog(qt):
    name = "usage_qt_pages.json" if qt else "usage_pages.json"
    return json.loads(Path(__file__).with_name(name).read_text(encoding="utf-8-sig"))


def record_usage_events(state_dir, events, *, web_only=False, public_only=False, guest=False, now=None):
    if not isinstance(events, list) or not 1 <= len(events) <= MAX_BATCH:
        raise ValueError("使用统计每批须有 1 至 50 项。")
    now = time.time() if now is None else now
    rows = []
    for event in events:
        if not isinstance(event, dict) or event.keys() - {"event_id","client","module","page","occurred_at"}:
            raise ValueError("使用统计字段无效。")
        client, module, page = (event.get(k) for k in ("client", "module", "page"))
        if not all(isinstance(v,str) for v in (client,module,page)) or (
            client not in WEB_CLIENTS and (web_only or client != "qt")
        ):
            raise ValueError("使用统计客户端无效。")
        spec = _catalog(client == "qt").get(module)
        if not spec or page not in spec["pages"]:
            raise ValueError("使用统计功能或页面无效。")
        public_page = page in spec.get("public_pages", ())
        if public_only and not public_page:
            raise PermissionError("请登录后上报该页面。")
        if guest and not public_page and module != "cabinet":
            raise PermissionError("访客仅可上报机柜页面。")
        identity = event.get("event_id")
        try:
            event_uuid = uuid.UUID(identity) if isinstance(identity,str) else None
        except ValueError:
            event_uuid = None
        if not event_uuid or event_uuid.version != 4 or str(event_uuid) != identity:
            raise ValueError("使用统计事件编号无效。")
        stamp = event.get("occurred_at", int(now * 1000))
        if type(stamp) is not int:
            raise ValueError("使用统计时间无效。")
        # Implausible device clocks must not create rows for 1970 or a future date.
        seconds = stamp / 1000 if 946684800000 <= stamp <= (now + 300) * 1000 else now
        day = datetime.fromtimestamp(seconds, _BEIJING).date().isoformat()
        rows.append((event_uuid.bytes,(day,client,module,page,spec["label"],spec["pages"][page])))

    root = Path(state_dir)
    root.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(root / _DB_NAME, timeout=3)) as db:
        db.executescript(_SCHEMA)
        with db:
            counts = Counter()
            for identity, bucket in rows:
                if db.execute("INSERT OR IGNORE INTO receipts VALUES (?)", (identity,)).rowcount:
                    counts[bucket] += 1
            db.executemany("""
                INSERT INTO daily(day,client,module,page,module_label,page_label,key,count)
                VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT(day,client,module,page) DO UPDATE SET count=daily.count+excluded.count
            """, [(*bucket,str(uuid.uuid4()),count) for bucket,count in counts.items()])
    return {"accepted":len(events)}


def sync_usage(state_dir, sink=None, *, limit=32, stopping=None):
    """Ship absolute totals. Hold the exporter lock, never the collector DB, during HTTP."""
    root = Path(state_dir)
    db_path = root / _DB_NAME
    if not db_path.is_file():
        return 0
    # A dedicated SQLite lock file supplies a crash-released process lock on Windows and Unix.
    # Concurrent scheduler instances cannot send an older total after a newer one.
    with closing(sqlite3.connect(root / "page_usage_sync.sqlite3", timeout=0)) as lock:
        try:
            lock.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            if getattr(exc, "sqlite_errorcode", None) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                return 0
            raise
        with closing(sqlite3.connect(db_path, timeout=3)) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute("""
                SELECT * FROM daily WHERE synced<count AND next_try<=?
                ORDER BY next_try,day,rowid LIMIT ?
            """,(time.time(),limit)).fetchall()
            if rows and sink is None:
                from .usage_bitable import UsageBitableSink
                sink = UsageBitableSink()
            done = 0
            for row in rows:
                if stopping and stopping():
                    break
                try:
                    record_id = sink.upsert(dict(row))
                    if not isinstance(record_id,str) or not record_id:
                        raise ValueError("缺少飞书记录回执")
                except Exception as exc:
                    delay = min(3600, 60 * 2**min(row["failures"],6))
                    with db:
                        db.execute("UPDATE daily SET failures=failures+1,next_try=? WHERE key=?",
                                   (time.time()+delay,row["key"]))
                    logging.warning("使用统计同步失败，%s/%s 将在 %d 秒后重试 (%s)",
                                    row["module_label"],row["page_label"],delay,type(exc).__name__)
                    continue
                with db:
                    # New or long-waiting rows precede a busy page that is already visible online.
                    db.execute("UPDATE daily SET record_id=?,synced=?,failures=0,next_try=? WHERE key=?",
                               (record_id,row["count"],time.time(),row["key"]))
                done += 1
            return done

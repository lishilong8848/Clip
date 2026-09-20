"""Local cabinet-power batch inbox and text-PDF recognition."""
from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import os
import re
import sqlite3
import tempfile
import threading
import uuid
import io
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

from .cabinet_power_excel import (
    CabinetError,
    OPS,
    RACK_TYPES,
    STATES,
    completed_state_event,
    digest,
    notice_summary_baseline_date,
)


MAX_FILES = 10
MAX_FILE_BYTES = 10 * 1024 * 1024
MAX_TOTAL_BYTES = 30 * 1024 * 1024
MAX_PAGES = 100
MAX_ROWS = 2000
MAX_IMAGES = 200
SCOPES = frozenset("ABCDE")
DATE_RE = re.compile(r"20\d{2}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?")
ROW_RE = re.compile(
    r"^\s*EA118\s+([A-E])([1-4])-(\d{1,2})\.EA118\s+([A-Z]\d{2})\s+(\S+)\s+"
    r"(网络机柜|服务器机柜)\s+(\S+)(.*)$"
)
POWER_ACTIONS_BY_STATE = {
    "off": frozenset(("上测试电", "上正式电")),
    "test": frozenset(("测试电转正式电", "下测试电")),
    "formal": frozenset(("正式电转测试电", "下正式电")),
}
POWER_STATE_LABELS = {"off": "未上电/已下电", "test": "测试电", "formal": "正式电"}
NOTICE_ROOM_RE = re.compile(
    r"(?<![A-Z0-9])([A-E])\s*[-_－—]?\s*([1-4]\d{2})\s*(?:包间|运营商机房|机房)?",
    re.IGNORECASE,
)
NOTICE_RACK_RE = re.compile(
    r"(?<![A-Z0-9])([A-Z])\s*[-_－—]?\s*(\d{1,2})(?!\d)",
    re.IGNORECASE,
)
EDITABLE_FIELDS = {
    "scope", "room", "rack", "supplier_rack", "rack_type", "type_detail",
    "action", "expected", "actual", "result", "failure_reason", "type_resolution",
}
ACTIVE_ROW_STATUSES = {"ready", "failed", "rolled_back"}
LOCKED_ROW_STATUSES = {"queued", "writing", "completed", "rollback_queued", "rolling_back", "rollback_failed", "rollback_blocked"}
_NOTICE_CUTOFFS = {}
_NOTICE_CUTOFFS_LOCK = threading.Lock()


def now():
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _json(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


class CabinetBatchStore:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS batches(
                    batch_id TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    source_hash TEXT UNIQUE,
                    scopes_json TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS batch_status_date ON batches(status, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_batches_source ON batches(json_extract(payload_json, '$.source'));
                CREATE TABLE IF NOT EXISTS notice_handoffs(
                    event_key TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS batch_meta(
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS notice_summary_rows(
                    batch_id TEXT NOT NULL,
                    row_id TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    room TEXT NOT NULL,
                    rack TEXT NOT NULL,
                    sent_at TEXT NOT NULL,
                    notice_type TEXT NOT NULL,
                    action TEXT NOT NULL,
                    rack_type TEXT NOT NULL,
                    eligible INTEGER NOT NULL,
                    PRIMARY KEY(batch_id,row_id)
                );
                CREATE INDEX IF NOT EXISTS notice_summary_scope_time
                    ON notice_summary_rows(scope,sent_at,batch_id,row_id);
                CREATE TABLE IF NOT EXISTS notice_summary_baselines(
                    scope TEXT NOT NULL,
                    template_hash TEXT NOT NULL,
                    cutoff_date TEXT NOT NULL,
                    frozen_keys_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(scope,template_hash)
                );
                CREATE TABLE IF NOT EXISTS batch_runtime_status(
                    batch_id TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS batch_rows(
                    batch_id TEXT NOT NULL,
                    item_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY(batch_id,item_id)
                );
                CREATE INDEX IF NOT EXISTS batch_rows_order ON batch_rows(batch_id,ordinal);
                CREATE TABLE IF NOT EXISTS batch_images(
                    batch_id TEXT NOT NULL,
                    item_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY(batch_id,item_id)
                );
                CREATE INDEX IF NOT EXISTS batch_images_order ON batch_images(batch_id,ordinal);
            """)
            conn.execute("BEGIN IMMEDIATE")
            normalized = conn.execute(
                "SELECT value FROM batch_meta WHERE key='batch_payload_normalization_version'"
            ).fetchone()
            if not normalized or normalized[0] != "1":
                for row in conn.execute("SELECT batch_id,payload_json FROM batches"):
                    payload = json.loads(row["payload_json"])
                    self._sync_children(conn, "batch_rows", row["batch_id"], payload.pop("rows", []), "row_id")
                    self._sync_children(conn, "batch_images", row["batch_id"], payload.pop("images", []), "image_id")
                    conn.execute("UPDATE batches SET payload_json=? WHERE batch_id=?", (_json(payload), row["batch_id"]))
                conn.execute(
                    "INSERT OR REPLACE INTO batch_meta(key,value) VALUES('batch_payload_normalization_version','1')"
                )
            marker = conn.execute(
                "SELECT value FROM batch_meta WHERE key='notice_summary_projection_version'"
            ).fetchone()
            if not marker or marker[0] != "3":
                conn.execute("DELETE FROM notice_summary_rows")
                conn.execute("DELETE FROM batch_runtime_status")
                for row in conn.execute("SELECT * FROM batches"):
                    batch = self._decode(row, conn)
                    self._sync_notice_summary_rows(conn, batch)
                    self._sync_runtime_status(conn, batch)
                conn.execute(
                    "INSERT OR REPLACE INTO batch_meta(key,value) VALUES('notice_summary_projection_version','3')"
                )
            conn.commit()

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.path, timeout=3)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=3000")
        conn.execute("PRAGMA synchronous=FULL")
        try:
            yield conn
        finally:
            conn.close()

    def _decode(self, row, conn=None):
        if row is None:
            return None
        payload = json.loads(row["payload_json"])
        if conn is None:
            with self._connect() as child_conn:
                return self._decode(row, child_conn)
        payload["rows"] = [json.loads(item[0]) for item in conn.execute(
            "SELECT payload_json FROM batch_rows WHERE batch_id=? ORDER BY ordinal", (row["batch_id"],))]
        payload["images"] = [json.loads(item[0]) for item in conn.execute(
            "SELECT payload_json FROM batch_images WHERE batch_id=? ORDER BY ordinal", (row["batch_id"],))]
        payload.update(
            batch_id=row["batch_id"], owner_id=row["owner_id"], status=row["status"],
            source_hash=row["source_hash"] or "", scopes=json.loads(row["scopes_json"]),
            version=row["version"], created_at=row["created_at"], updated_at=row["updated_at"],
        )
        return payload

    def get(self, batch_id):
        with self._connect() as conn:
            return self._decode(conn.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone(), conn)

    def by_hash(self, source_hash):
        if not source_hash:
            return None
        with self._connect() as conn:
            return self._decode(conn.execute("SELECT * FROM batches WHERE source_hash=?", (source_hash,)).fetchone(), conn)

    def notice_batches(self):
        with self._connect() as conn:
            return [self._decode(row, conn) for row in conn.execute(
                "SELECT * FROM batches WHERE json_extract(payload_json, '$.source')='notice' ORDER BY rowid"
            )]

    @staticmethod
    def _sync_notice_summary_rows(conn, batch):
        batch_id = str(batch.get("batch_id") or "")
        if not batch_id:
            return
        conn.execute("DELETE FROM notice_summary_rows WHERE batch_id=?", (batch_id,))
        if batch.get("source") != "notice":
            return
        source = batch.get("source_notice") or {}
        sent_at = str(source.get("sent_at") or "")
        notice_type = str(source.get("notice_type") or "")
        deleted = bool(source.get("deleted_at"))
        invalid_codes = {"scope", "room", "rack", "inventory", "notice_scope"}
        seen = set()
        rows = []
        for row in batch.get("rows", []):
            key = (str(row.get("scope") or ""), str(row.get("room") or ""), str(row.get("rack") or ""))
            issues = {str(issue.get("code") or "") for issue in row.get("issues", []) if isinstance(issue, dict)}
            eligible = not (
                deleted or row.get("notice_removed") or row.get("exclude_notice_summary")
                or not sent_at or notice_type not in ("上电通告", "下电通告")
                or key in seen or bool(issues & invalid_codes)
            )
            seen.add(key)
            rows.append((
                batch_id, str(row.get("row_id") or ""), *key, sent_at, notice_type,
                str(row.get("action") or ""), str(row.get("rack_type") or ""), int(eligible),
            ))
        conn.executemany(
            "INSERT OR REPLACE INTO notice_summary_rows"
            "(batch_id,row_id,scope,room,rack,sent_at,notice_type,action,rack_type,eligible)"
            " VALUES(?,?,?,?,?,?,?,?,?,?)",
            rows,
        )

    def notice_summary_items(self, scope):
        with self._connect() as conn:
            return [dict(row) for row in conn.execute(
                "SELECT batch_id,row_id,scope,room,rack,sent_at,notice_type,action,rack_type "
                "FROM notice_summary_rows WHERE scope=? AND eligible=1 ORDER BY sent_at,batch_id,row_id",
                (scope,),
            )]

    @staticmethod
    def _sync_runtime_status(conn, batch):
        source = batch.get("source_notice") or {}
        compact = {
            "batch_id": batch.get("batch_id", ""), "owner_id": batch.get("owner_id", ""),
            "scopes": batch.get("scopes", []), "version": batch.get("version", 0),
            "status": batch.get("status", ""), "error": batch.get("error", ""),
            "source": batch.get("source", ""), "entry_scope": batch.get("entry_scope", ""),
            "stats": copy.deepcopy(batch.get("stats") or {}),
            "created_at": batch.get("created_at", ""), "updated_at": batch.get("updated_at", ""),
            "progress": batch.get("progress") or {},
            "source_notice": {key: copy.deepcopy(source.get(key)) for key in
                              ("sent_at", "ended_at", "deleted_at", "end_time_check", "rollback_error")},
            "images": [{"image_id": item.get("image_id", ""), "status": item.get("status", ""),
                        "error": item.get("error", ""), "deleted_at": item.get("deleted_at", "")}
                       for item in batch.get("images", [])],
            "rows": [{key: copy.deepcopy(item.get(key)) for key in
                      ("row_id", "scope", "status", "notice_removed", "wrote_record",
                       "operation_started", "issues")}
                     for item in batch.get("rows", [])],
        }
        conn.execute("INSERT OR REPLACE INTO batch_runtime_status(batch_id,payload_json) VALUES(?,?)",
                     (compact["batch_id"], _json(compact)))

    def runtime_status(self, batch_id):
        with self._connect() as conn:
            row = conn.execute("SELECT payload_json FROM batch_runtime_status WHERE batch_id=?", (batch_id,)).fetchone()
        return json.loads(row[0]) if row else None

    def runtime_list(self):
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT runtime.payload_json,batches.owner_id,batches.status,batches.scopes_json,"
                "batches.version,batches.created_at,batches.updated_at "
                "FROM batch_runtime_status runtime JOIN batches USING(batch_id) "
                "ORDER BY batches.updated_at DESC"
            )
            result = []
            for row in rows:
                item = json.loads(row[0])
                item.update(owner_id=row[1], status=row[2], scopes=json.loads(row[3]),
                            version=row[4], created_at=row[5], updated_at=row[6])
                result.append(item)
            return result

    def all_images(self):
        with self._connect() as conn:
            return [json.loads(row[0]) for row in conn.execute("SELECT payload_json FROM batch_images")]

    def notice_summary_baseline(self, scope, template_hash, cutoff_date, item_keys):
        if not template_hash:
            return set()
        with self._lock, self._connect() as conn, conn:
            row = conn.execute(
                "SELECT frozen_keys_json FROM notice_summary_baselines WHERE scope=? AND template_hash=?",
                (scope, template_hash),
            ).fetchone()
            if row:
                try:
                    return set(json.loads(row[0]))
                except (TypeError, ValueError):
                    return set()
            frozen = sorted(set(item_keys)) if cutoff_date else []
            conn.execute(
                "INSERT OR IGNORE INTO notice_summary_baselines(scope,template_hash,cutoff_date,frozen_keys_json,created_at) "
                "VALUES(?,?,?,?,?)",
                (scope, template_hash, cutoff_date, _json(frozen), now()),
            )
            stored = conn.execute(
                "SELECT frozen_keys_json FROM notice_summary_baselines WHERE scope=? AND template_hash=?",
                (scope, template_hash),
            ).fetchone()
            return set(json.loads(stored[0])) if stored else set(frozen)

    def failed_notice_handoffs(self, limit=100):
        with self._connect() as conn:
            return [{"key": row[0], "payload": json.loads(row[1]), "attempts": row[2],
                     "error": row[3], "updated_at": row[4]} for row in conn.execute(
                "SELECT event_key,payload_json,attempts,error,updated_at FROM notice_handoffs "
                "WHERE status='failed' ORDER BY updated_at DESC LIMIT ?", (max(1, min(int(limit), 500)),)
            )]

    def notice_by_target(self, record_id):
        with self._connect() as conn:
            return self._decode(conn.execute(
                "SELECT * FROM batches WHERE json_extract(payload_json, '$.source')='notice' "
                "AND json_extract(payload_json, '$.source_notice.target_record_id')=? LIMIT 1",
                (record_id,),
            ).fetchone(), conn)

    def queue_notice_handoff(self, payload):
        event_key = str(payload.get("idempotency_key") or "")
        if not event_key:
            raise CabinetError("通告联动缺少操作标识")
        with self._lock, self._connect() as conn, conn:
            conn.execute("INSERT OR IGNORE INTO notice_handoffs(event_key,payload_json,updated_at) VALUES(?,?,?)",
                         (event_key, _json(payload), now()))

    def next_notice_handoff(self):
        with self._connect() as conn:
            row = conn.execute("SELECT event_key,payload_json,attempts FROM notice_handoffs "
                               "WHERE status='pending' ORDER BY updated_at LIMIT 1").fetchone()
        return {"key": row[0], "payload": json.loads(row[1]), "attempts": row[2]} if row else None

    def requeue_failed_notice_handoffs(self):
        with self._lock, self._connect() as conn, conn:
            cursor = conn.execute("UPDATE notice_handoffs SET status='pending',attempts=0 WHERE status='failed'")
            return max(0, int(cursor.rowcount or 0))

    def finish_notice_handoff(self, event_key, error=""):
        with self._lock, self._connect() as conn, conn:
            if error:
                conn.execute("UPDATE notice_handoffs SET attempts=attempts+1,error=?,"
                             "status=CASE WHEN attempts>=4 THEN 'failed' ELSE 'pending' END,updated_at=? "
                             "WHERE event_key=?", (str(error), now(), event_key))
                row = conn.execute("SELECT status,attempts FROM notice_handoffs WHERE event_key=?", (event_key,)).fetchone()
                return {"status": row[0], "attempts": row[1]} if row else {"status": "failed", "attempts": 5}
            else:
                conn.execute("DELETE FROM notice_handoffs WHERE event_key=?", (event_key,))
                return {"status": "done", "attempts": 0}

    def create(self, batch):
        created = now()
        with self._lock, self._connect() as conn, conn:
            conn.execute(
                "INSERT INTO batches VALUES(?,?,?,?,?,?,?,?,?)",
                (batch["batch_id"], batch["owner_id"], batch["status"], batch.get("source_hash") or None,
                 _json(batch.get("scopes", [])), _json(self._payload(batch)), 1, created, created),
            )
            self._sync_children(conn, "batch_rows", batch["batch_id"], batch.get("rows", []), "row_id")
            self._sync_children(conn, "batch_images", batch["batch_id"], batch.get("images", []), "image_id")
            self._sync_notice_summary_rows(conn, {**batch, "version": 1})
            self._sync_runtime_status(conn, {**batch, "version": 1, "created_at": created, "updated_at": created})
        return self.get(batch["batch_id"])

    @staticmethod
    def _payload(batch):
        columns = {"batch_id", "owner_id", "status", "source_hash", "scopes", "version", "created_at", "updated_at", "rows", "images"}
        return {key: value for key, value in batch.items() if key not in columns}

    @staticmethod
    def _sync_children(conn, table, batch_id, items, id_field):
        normalized = [(str(item.get(id_field) or ""), index, _json(item))
                      for index, item in enumerate(items) if isinstance(item, dict) and item.get(id_field)]
        existing = {row[0] for row in conn.execute(f"SELECT item_id FROM {table} WHERE batch_id=?", (batch_id,))}
        current = {item_id for item_id, _index, _payload in normalized}
        conn.executemany(
            f"INSERT INTO {table}(batch_id,item_id,ordinal,payload_json) VALUES(?,?,?,?) "
            "ON CONFLICT(batch_id,item_id) DO UPDATE SET ordinal=excluded.ordinal,payload_json=excluded.payload_json "
            f"WHERE {table}.ordinal<>excluded.ordinal OR {table}.payload_json<>excluded.payload_json",
            [(batch_id, item_id, index, payload) for item_id, index, payload in normalized],
        )
        conn.executemany(f"DELETE FROM {table} WHERE batch_id=? AND item_id=?",
                         [(batch_id, item_id) for item_id in existing - current])

    def save(self, batch, expected_version, changed_row_ids=None):
        updated = now()
        with self._lock, self._connect() as conn, conn:
            version = int(expected_version) + 1
            cursor = conn.execute(
                "UPDATE batches SET status=?,scopes_json=?,payload_json=?,version=?,updated_at=? "
                "WHERE batch_id=? AND version=?",
                (batch["status"], _json(batch.get("scopes", [])), _json(self._payload(batch)),
                 version, updated, batch["batch_id"], int(expected_version)),
            )
            if cursor.rowcount != 1:
                exists = conn.execute("SELECT 1 FROM batches WHERE batch_id=?", (batch["batch_id"],)).fetchone()
                raise CabinetError("批次已被其他操作更新，请重新载入" if exists else "批次不存在", 409 if exists else 404)
            if changed_row_ids is None:
                self._sync_children(conn, "batch_rows", batch["batch_id"], batch.get("rows", []), "row_id")
            else:
                changed_row_ids = set(changed_row_ids)
                conn.executemany(
                    "INSERT INTO batch_rows(batch_id,item_id,ordinal,payload_json) VALUES(?,?,?,?) "
                    "ON CONFLICT(batch_id,item_id) DO UPDATE SET ordinal=excluded.ordinal,payload_json=excluded.payload_json",
                    [(batch["batch_id"], row["row_id"], index, _json(row))
                     for index, row in enumerate(batch.get("rows", [])) if row.get("row_id") in changed_row_ids],
                )
            self._sync_children(conn, "batch_images", batch["batch_id"], batch.get("images", []), "image_id")
            self._sync_notice_summary_rows(conn, batch)
            self._sync_runtime_status(conn, {**batch, "version": version, "updated_at": updated})
        return self.get(batch["batch_id"])

    def list(self, limit=200):
        with self._connect() as conn:
            if limit is None:
                rows = conn.execute("SELECT * FROM batches ORDER BY updated_at DESC")
            else:
                rows = conn.execute("SELECT * FROM batches ORDER BY updated_at DESC LIMIT ?",
                                    (max(1, min(int(limit), 1000)),))
            return [self._decode(row, conn) for row in rows]

    def later_completed(self, batch_id, scope, room, rack):
        with self._connect() as conn:
            rows=conn.execute(
                "SELECT child.payload_json FROM batch_rows child JOIN batches parent ON parent.batch_id=child.batch_id "
                "WHERE parent.rowid>(SELECT rowid FROM batches WHERE batch_id=?) ORDER BY parent.rowid,child.ordinal",
                (batch_id,),
            )
            for item in rows:
                row=json.loads(item[0])
                if (row.get("scope"), row.get("room"), row.get("rack")) == (scope,room,rack) and row.get("wrote_record") is not False and row.get("status") in {"completed","rollback_queued","rolling_back","rollback_failed","rollback_blocked"}:
                    return True
        return False


class CabinetBatchService:
    def __init__(self, cabinet_service, root):
        self.cabinet = cabinet_service
        self.root = Path(root)
        self.import_root = self.root / "imports"
        self.store = CabinetBatchStore(self.root / "batches.sqlite3")
        self._lock = threading.RLock()
        self._pdf_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="cabinet-pdf")
        self._ocr_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="cabinet-ocr")
        self._confirm_pool = ThreadPoolExecutor(max_workers=5, thread_name_prefix="cabinet-batch")
        self._scope_locks = {scope: threading.Lock() for scope in "ABCDE"}
        self._recover_interrupted()

    def shutdown(self, wait=False):
        self._pdf_pool.shutdown(wait=wait, cancel_futures=not wait)
        self._ocr_pool.shutdown(wait=wait, cancel_futures=not wait)
        self._confirm_pool.shutdown(wait=wait, cancel_futures=not wait)

    @staticmethod
    def _directory_size(path):
        path = Path(path)
        return sum(item.stat().st_size for item in path.rglob("*") if item.is_file()) if path.exists() else 0

    def storage_status(self):
        grouped = {}
        for image in self.store.all_images():
            grouped.setdefault(str(image.get("image_id") or ""), []).append(image)
        eligible = {image_id for image_id, items in grouped.items()
                    if image_id and all(item.get("cloud_file_token") for item in items)}
        evidence = self.root / "evidence"
        return {
            "evidence_bytes": self._directory_size(evidence),
            "thumbnail_bytes": self._directory_size(self.root / "evidence_thumbnails"),
            "import_bytes": self._directory_size(self.import_root),
            "export_bytes": self._directory_size(self.root / "exports"),
            "cloud_backed_files": sum(any(evidence.glob(image_id + ".*")) for image_id in eligible),
        }

    def cleanup_evidence_cache(self):
        active = {"recognizing", "queued", "writing", "rollback_queued", "rolling_back"}
        if any(row.get("status") in active for batch in self.store.runtime_list() for row in batch.get("rows", [])):
            raise CabinetError("仍有截图识别、写入或回退任务，暂不能清理缓存", 409)
        grouped = {}
        for image in self.store.all_images():
            grouped.setdefault(str(image.get("image_id") or ""), []).append(image)
        eligible = {image_id for image_id, items in grouped.items()
                    if image_id and all(item.get("cloud_file_token") for item in items)}
        deleted = bytes_removed = 0
        for image_id in eligible:
            for path in (self.root / "evidence").glob(image_id + ".*"):
                bytes_removed += path.stat().st_size
                path.unlink()
                deleted += 1
            thumbnail = self.root / "evidence_thumbnails" / (image_id + ".png")
            if thumbnail.is_file():
                bytes_removed += thumbnail.stat().st_size
                thumbnail.unlink()
        return {"deleted": deleted, "bytes_removed": bytes_removed, **self.storage_status()}

    @staticmethod
    def _image_scopes(batch, image_id):
        scopes = {
            str(row.get("scope") or "") for row in batch.get("rows", [])
            if image_id in row.get("evidence_images", []) and row.get("scope") in SCOPES
        }
        image = next((item for item in batch.get("images", []) if item.get("image_id") == image_id), None)
        if image:
            scopes.update(str(item.get("scope") or "") for item in image.get("suggestions", [])
                          if item.get("scope") in SCOPES)
        return scopes or set(batch.get("scopes", []))

    def add_images(self, batch_id, files, owner, allowed, admin=False):
        from PIL import Image

        batch = self.get(batch_id)
        if not admin and (batch["owner_id"] != owner or not set(batch.get("scopes", [])) <= set(allowed)):
            raise CabinetError("无权向该批次上传截图", 403)
        if batch["status"] == "cancelled":
            raise CabinetError("已作废批次不能上传截图", 409)
        if batch.get("rows") and not any(row.get("status") not in LOCKED_ROW_STATUSES and not str(row.get("status","")).startswith("excluded_") for row in batch["rows"]):
            raise CabinetError("已完成记录不可修改截图；请先回退需要更正的记录",409)
        if not files or len(files) > 10 or sum(len(content) for _name, content in files) > MAX_TOTAL_BYTES:
            raise CabinetError("每次须选择1至10张图片，合计不超过30MiB", 413)
        active_ids = {item.get("image_id") for item in batch.get("images", []) if not item.get("deleted_at")}
        accepted = []
        for name, content in files:
            if not content or len(content) > MAX_FILE_BYTES:
                raise CabinetError("单张图片不能超过10MiB", 413)
            try:
                with Image.open(io.BytesIO(content)) as image:
                    image.verify()
                with Image.open(io.BytesIO(content)) as image:
                    if image.width * image.height > 40_000_000 or image.format not in ("JPEG", "PNG", "WEBP"):
                        raise ValueError("图片格式或像素数不支持")
                    extension = {"JPEG": ".jpg", "PNG": ".png", "WEBP": ".webp"}[image.format]
            except Exception as exc:
                raise CabinetError(f"{name} 不是有效的 JPG、PNG 或 WebP 图片：{exc}", 400) from exc
            image_id = hashlib.sha256(content).hexdigest()
            if any(item.get("image_id") == image_id and item.get("deleted_at") for item in batch.get("images", [])):
                raise CabinetError("这张截图已删除，请在待办详情撤回删除", 409)
            path = self.root / "evidence" / (image_id + extension)
            if not path.exists():
                self._atomic_write(path, content)
            accepted.append({"image_id": image_id, "name": Path(name).name[:200], "extension": extension,
                             "size": len(content), "status": "recognizing", "suggestions": [], "error": ""})
        if len(active_ids | {item["image_id"] for item in accepted}) > MAX_IMAGES:
            raise CabinetError(f"每批最多保留{MAX_IMAGES}张确认截图，请删除无用图片后重试", 413)

        existing_status = {item["image_id"]: item.get("status") for item in batch.get("images", [])}
        def add(current):
            images = current.setdefault("images", [])
            known = {item["image_id"]: item for item in images}
            for item in accepted:
                if item["image_id"] not in known:
                    images.append(item)
                elif known[item["image_id"]].get("status") == "failed":
                    known[item["image_id"]].update(status="recognizing", error="", suggestions=[])
        updated = self._change(batch_id, add)
        submitted = set()
        for item in accepted:
            if item["image_id"] not in submitted and existing_status.get(item["image_id"]) in (None, "failed"):
                self._ocr_pool.submit(self._recognize_image, batch_id, item)
                submitted.add(item["image_id"])
        return updated

    def _recognize_image(self, batch_id, image):
        from .cabinet_power_evidence import recognize_image_with_timeout

        try:
            path = self.root / "evidence" / (image["image_id"] + image["extension"])
            suggestions = recognize_image_with_timeout(path.read_bytes())
            error = "" if suggestions else "未识别到完整的机柜表格，请手动核对截图"
        except Exception as exc:
            suggestions, error = [], f"截图识别失败：{exc}"

        inventories = {}
        try:
            before = self.get(batch_id)
            if before.get("source") == "image":
                for scope in {item.get("scope") for item in suggestions} & set(before.get("recognition_scopes", [])):
                    snap = self.cabinet._snapshot(scope)
                    inventories[scope] = {(item["room"], item["rack"]): item for item in snap["config"]["inventory"]}
        except Exception as exc:
            suggestions, error = [], f"机柜目录读取失败：{exc}"

        def apply(batch):
            if batch.get("status") == "cancelled":
                return
            target = next((item for item in batch.get("images", []) if item["image_id"] == image["image_id"]), None)
            if target is None or target.get("deleted_at"):
                return
            target.update(status="done" if suggestions else "failed", error=error, suggestions=[])
            image_source = batch.get("source") == "image"
            for index, candidate in enumerate(suggestions, 1):
                if image_source and candidate.get("scope") not in batch.get("recognition_scopes", []):
                    target["suggestions"].append({**candidate, "row_id": "", "status": "unauthorized", "applied_fields": []})
                    continue
                if image_source:
                    matches = [row for row in batch.get("rows", [])
                               if (row.get("scope"), row.get("room"), row.get("rack")) ==
                               (candidate.get("scope"), candidate.get("room"), candidate.get("rack"))
                               and (not candidate.get("action") or not row.get("action") or row["action"] == candidate["action"])
                               and (not candidate.get("actual") or not row.get("actual") or row["actual"] == candidate["actual"])]
                    if not matches and len(batch.get("rows", [])) < MAX_ROWS:
                        current = {key: "" for key in EDITABLE_FIELDS}
                        current.update({key: str(candidate.get(key) or "") for key in
                                        ("scope", "room", "rack", "supplier_rack", "action", "expected", "actual", "result")})
                        current["rack_type"] = str(inventories.get(candidate["scope"], {}).get(
                            (candidate["room"], candidate["rack"]), {}).get("rack_type") or "")
                        row_id = "row_" + digest([batch_id, image["image_id"], index, current])[:24]
                        row = {**current, "row_id": row_id, "source_index": len(batch["rows"]) + 1,
                               "file_id": image["image_id"], "file_name": image["name"],
                               "file_sha256": image["image_id"], "page": 0, "source_row": index,
                               "application_ids": [], "applicant": "",
                               "original": copy.deepcopy(current), "edits": [], "status": "ready", "issues": [],
                               "error": "", "operation_id": "batch_" + digest([batch_id, row_id])[:32]}
                        batch["rows"].append(row)
                        matches = [row]
                else:
                    matches = [row for row in batch.get("rows", [])
                               if (row.get("scope"), row.get("room")) == (candidate["scope"], candidate["room"])
                               and (row.get("rack") == candidate["rack"] or candidate.get("supplier_rack")
                                    and row.get("supplier_rack") == candidate["supplier_rack"])]
                if len(matches) > 1 and candidate["action"]:
                    matches = [row for row in matches if row.get("action") == candidate["action"]]
                suggestion = {**candidate, "row_id": matches[0]["row_id"] if len(matches) == 1 else "",
                              "status": "needs_review", "applied_fields": []}
                target["suggestions"].append(suggestion)
                if len(matches) != 1:
                    continue
                row = matches[0]
                if row.get("status") in LOCKED_ROW_STATUSES or str(row.get("status", "")).startswith("excluded_") or row.get("operation_started") and row.get("status") != "rolled_back":
                    continue
                if candidate["action"] and row.get("action") and candidate["action"] != row["action"]:
                    continue
                if not image_source and (not candidate["expected"] or not candidate["actual"]):
                    continue
                if any(row.get(key) and row[key] != candidate[key] for key in ("expected", "actual")):
                    continue
                for key in ("expected", "actual", "action", "supplier_rack", "result"):
                    if candidate.get(key) and not row.get(key):
                        row[key] = candidate[key]
                        suggestion["applied_fields"].append(key)
                refs = row.setdefault("evidence_images", [])
                if image["image_id"] not in refs:
                    refs.append(image["image_id"])
                    row.setdefault("edits", []).append({"field": "evidence_images", "after": image["image_id"], "owner": "ocr", "at": now()})
                suggestion["status"] = "applied"
        try:
            self._change(batch_id, apply, validate=True)
        except Exception as exc:
            def failed(batch):
                target = next((item for item in batch.get("images", []) if item["image_id"] == image["image_id"]), None)
                if target:
                    target.update(status="failed", error=f"截图匹配失败：{exc}")
            self._change(batch_id, failed)

    def image_path(self, batch_id, image_id, owner, allowed, admin=False, thumbnail=False):
        batch = self.get(batch_id)
        image = next((item for item in batch.get("images", []) if item["image_id"] == image_id), None)
        if image is None:
            raise CabinetError("截图不存在", 404)
        image_scopes = self._image_scopes(batch, image_id)
        if not admin and batch["owner_id"] != owner and (not image_scopes or not image_scopes <= set(allowed)):
            raise CabinetError("无权查看截图", 403)
        path = self.root / "evidence" / (image_id + image["extension"])
        if not path.is_file():
            token = str(image.get("cloud_file_token") or "")
            scope = next((row.get("scope") for row in batch.get("rows", [])
                          if image_id in row.get("evidence_images", []) and row.get("scope") in SCOPES), "")
            if not token or scope not in SCOPES:
                raise CabinetError("截图本地文件不可用", 410)
            content = self.cabinet.remote_for(scope).download_attachment(token)
            if hashlib.sha256(content).hexdigest() != image_id:
                raise CabinetError("飞书确认截图与待办记录校验值不一致", 409)
            self._atomic_write(path, content)
        if thumbnail:
            from .cabinet_power_evidence import ensure_thumbnail
            path = ensure_thumbnail(path, self.root / "evidence_thumbnails" / (image_id + ".png"))
        return path, image

    def delete_image(self, batch_id, image_id, expected_version, owner, allowed, admin=False):
        batch = self.get(batch_id)
        if not admin and (batch["owner_id"] != owner or not set(batch.get("scopes", [])) <= set(allowed)):
            raise CabinetError("无权删除该批次截图", 403)
        try:
            expected_version = int(expected_version)
        except (TypeError, ValueError) as exc:
            raise CabinetError("缺少有效批次版本", 400) from exc

        def remove(current):
            if current["status"] == "cancelled":
                raise CabinetError("已作废批次不能修改截图", 409)
            image = next((item for item in current.get("images", []) if item.get("image_id") == image_id), None)
            if image is None:
                raise CabinetError("截图不存在", 404)
            if image.get("deleted_at"):
                raise CabinetError("截图已经删除，可点击撤回删除", 409)
            linked = [row for row in current.get("rows", []) if image_id in row.get("evidence_images", [])]
            if any(row.get("status") in LOCKED_ROW_STATUSES or
                   row.get("operation_started") and row.get("status") != "rolled_back" for row in linked):
                raise CabinetError("截图已随机柜记录提交；请先回退相关记录再删除", 409)
            removed_statuses = {}
            for row in linked:
                row["evidence_images"].remove(image_id)
                row.setdefault("edits", []).append({"field": "evidence_images", "before": image_id,
                                                    "after": "", "owner": owner, "at": now()})
                if current.get("source") == "image" and row.get("file_id") == image_id and not row["evidence_images"]:
                    removed_statuses[row["row_id"]] = row["status"]
                    row["status"] = "excluded_image"
            image.update(deleted_at=now(), deleted_by=owner, removed_row_ids=[row["row_id"] for row in linked],
                         removed_row_statuses=removed_statuses)

        return self._change(batch_id, remove, expected_version=expected_version)

    def restore_image(self, batch_id, image_id, expected_version, owner, allowed, admin=False):
        batch = self.get(batch_id)
        if not admin and (batch["owner_id"] != owner or not set(batch.get("scopes", [])) <= set(allowed)):
            raise CabinetError("无权恢复该批次截图", 403)
        try:
            expected_version = int(expected_version)
        except (TypeError, ValueError) as exc:
            raise CabinetError("缺少有效批次版本", 400) from exc

        def restore(current):
            if current["status"] == "cancelled":
                raise CabinetError("已作废批次不能恢复截图", 409)
            image = next((item for item in current.get("images", []) if item.get("image_id") == image_id), None)
            if image is None or not image.get("deleted_at"):
                raise CabinetError("没有可撤回的截图删除操作", 409)
            rows = [row for row in current.get("rows", []) if row["row_id"] in image.get("removed_row_ids", [])]
            if len(rows) != len(image.get("removed_row_ids", [])) or any(
                row.get("status") in LOCKED_ROW_STATUSES or
                row.get("operation_started") and row.get("status") != "rolled_back" for row in rows
            ):
                raise CabinetError("相关机柜已提交，须先回退后才能恢复截图关联", 409)
            for row in rows:
                refs = row.setdefault("evidence_images", [])
                if image_id not in refs:
                    refs.append(image_id)
                    row.setdefault("edits", []).append({"field": "evidence_images", "before": "",
                                                        "after": image_id, "owner": owner, "at": now()})
                if row.get("status") == "excluded_image" and row["row_id"] in image.get("removed_row_statuses", {}):
                    row["status"] = image["removed_row_statuses"][row["row_id"]]
            image.update(deleted_at="", deleted_by="", removed_row_ids=[], removed_row_statuses={})

        updated = self._change(batch_id, restore, expected_version=expected_version, validate=True)
        image = next(item for item in updated["images"] if item["image_id"] == image_id)
        if image.get("status") == "recognizing":
            self._ocr_pool.submit(self._recognize_image, batch_id, image)
        return updated

    def apply_image(self, batch_id, image_id, payload, owner, allowed, admin=False):
        row_id = str(payload.get("row_id") or "")
        fields = payload.get("fields") or {}
        if not row_id or not isinstance(fields, dict) or set(fields) - {"action", "expected", "actual", "supplier_rack"}:
            raise CabinetError("截图匹配参数无效", 400)
        try:
            expected_version = int(payload.get("version"))
            index = int(payload.get("candidate_index", -1))
        except (TypeError, ValueError) as exc:
            raise CabinetError("批次版本或候选行无效", 400) from exc

        def apply(batch):
            image = next((item for item in batch.get("images", []) if item["image_id"] == image_id), None)
            row = next((item for item in batch.get("rows", []) if item["row_id"] == row_id), None)
            if image is None or row is None:
                raise CabinetError("截图或待办行不存在", 404)
            if image.get("deleted_at"):
                raise CabinetError("截图已删除，请先撤回删除", 409)
            if not admin and row.get("scope") not in allowed:
                raise CabinetError("无权修改该楼栋待办", 403)
            if row.get("status") in LOCKED_ROW_STATUSES or row.get("operation_started") and row.get("status") != "rolled_back":
                raise CabinetError("该机柜已开始正式提交", 409)
            if index >= 0 and index >= len(image.get("suggestions", [])):
                raise CabinetError("识别候选行不存在", 404)
            if index >= 0 and image["suggestions"][index].get("status") == "unauthorized":
                raise CabinetError("无权使用其他楼栋的识别内容", 403)
            for key, raw in fields.items():
                value = str(raw or "").strip().replace("T", " ")
                if value != row.get(key, ""):
                    row.setdefault("edits", []).append({"field": key, "before": row.get(key, ""),
                                                        "after": value, "owner": owner, "at": now()})
                    row[key] = value
            refs = row.setdefault("evidence_images", [])
            if payload.get("attach", True):
                if image_id not in refs:
                    refs.append(image_id)
            elif image_id in refs:
                refs.remove(image_id)
            if index >= 0:
                image["suggestions"][index].update(
                    row_id=row_id, status="applied" if image_id in refs else "needs_review",
                    applied_fields=list(fields), reviewer=owner, reviewed_at=now(),
                )
        return self._change(batch_id, apply, expected_version=expected_version, validate=True)

    def _recover_interrupted(self):
        for batch in self.store.list(1000):
            changed = False
            if batch["status"] == "recognizing":
                batch["status"] = "failed"
                batch["error"] = "PDF识别因服务退出而中断，请重新上传文件"
                changed = True
            elif batch["status"] == "running":
                for row in batch.get("rows", []):
                    if row.get("status") in ("queued", "writing"):
                        row.update(status="failed", error="提交因服务退出而中断，可继续重试")
                        changed = True
                    elif row.get("status") in ("rollback_queued", "rolling_back"):
                        row.update(status="rollback_failed",error="回退因服务退出中断，可继续重试")
                        changed = True
                if changed:
                    self._refresh_summary(batch)
            if changed:
                try:
                    self.store.save(batch, batch["version"])
                except CabinetError:
                    pass
            for image in batch.get("images", []):
                if image.get("status") == "recognizing" and not image.get("deleted_at"):
                    self._ocr_pool.submit(self._recognize_image, batch["batch_id"], image)

    @staticmethod
    def _stats(rows):
        stats = {key: 0 for key in ("total", "ready", "duplicate", "conflict", "invalid", "completed", "failed", "excluded", "rolled_back", "rollback_failed", "rollback_blocked")}
        stats["total"] = len(rows)
        for row in rows:
            status = str(row.get("status") or "invalid")
            if status.startswith("excluded_"):
                stats["excluded"] += 1
            elif status in stats:
                stats[status] += 1
        stats["confirmable"] = sum(not row.get("notice_removed") and row.get("status") in ACTIVE_ROW_STATUSES and not row.get("issues") for row in rows)
        stats["new"] = sum(not row.get("notice_removed") and row.get("status") not in ("duplicate", "completed", "rolled_back") and not str(row.get("status", "")).startswith("excluded_") for row in rows)
        return stats

    def _refresh_summary(self, batch):
        rows = batch.get("rows", [])
        batch["stats"] = self._stats(rows)
        batch["scopes"] = sorted({row.get("scope") for row in rows if row.get("scope") in SCOPES})
        if batch.get("status") == "cancelled" and not any(row.get("status") in ("rollback_queued", "rolling_back") for row in rows):
            return
        if batch.get("status") == "recognizing" or batch.get("status") == "failed" and not rows:
            return
        if any(row.get("status") in ("queued", "writing", "rollback_queued", "rolling_back") for row in rows):
            batch["status"] = "running"
        elif rows and all(row.get("status") == "rolled_back" or str(row.get("status", "")).startswith("excluded_") for row in rows):
            batch["status"] = "pending" if batch.get("source") == "image" and all(str(row.get("status", "")).startswith("excluded_") for row in rows) else "rolled_back"
        elif rows and all(row.get("status") == "completed" or str(row.get("status", "")).startswith("excluded_") for row in rows):
            batch["status"] = "completed"
        elif any(row.get("status") in ("completed", "rolled_back") for row in rows):
            batch["status"] = "partial"
        elif any(row.get("status") in ("rollback_failed", "rollback_blocked") for row in rows):
            batch["status"] = "failed"
        elif rows:
            batch["status"] = "pending"

    @staticmethod
    def _notice_datetime(value):
        if isinstance(value, (int, float)) and value > 1000000000:
            return dt.datetime.fromtimestamp(value / (1000 if value > 100000000000 else 1)).strftime("%Y-%m-%d %H:%M:%S")
        text = str(value or "").strip().replace("T", " ").replace("：", ":")
        if text.isdigit() and len(text) in (10, 13):
            return CabinetBatchService._notice_datetime(int(text))
        try:
            return dt.datetime.fromisoformat(text).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return ""

    @staticmethod
    def _notice_quantity(value):
        match = re.search(r"\d+", str(value or "").replace(",", ""))
        return int(match.group()) if match else None

    @staticmethod
    def _parse_notice_cabinets(value):
        text = str(value or "").strip().upper()
        anchors = list(NOTICE_ROOM_RE.finditer(text))
        if not anchors:
            return [], {
                "duplicate_count": 0,
                "unparsed_fragments": [text] if text else ["柜号未填写"],
            }
        rows = []
        duplicates = 0
        unparsed = []
        seen = set()
        prefix = text[: anchors[0].start()]
        if re.sub(r"[\s,，、;；:：/\\]+", "", prefix):
            unparsed.append(prefix.strip())
        for index, anchor in enumerate(anchors):
            end = anchors[index + 1].start() if index + 1 < len(anchors) else len(text)
            segment = text[anchor.end() : end]
            matched_spans = []
            for rack_match in NOTICE_RACK_RE.finditer(segment):
                rack = f"{rack_match.group(1).upper()}{int(rack_match.group(2)):02d}"
                key = (anchor.group(1).upper(), anchor.group(2), rack)
                matched_spans.append(rack_match.span())
                if key in seen:
                    duplicates += 1
                    continue
                seen.add(key)
                rows.append({"scope": key[0], "room": key[1], "rack": key[2]})
            remainder = segment
            for start, stop in reversed(matched_spans):
                remainder = remainder[:start] + remainder[stop:]
            remainder = re.sub(
                r"(?:柜号|机柜|机架|柜)|[\s,，、;；。:：/\\]+",
                "",
                remainder,
            )
            if remainder:
                unparsed.append(segment.strip())
        return rows, {
            "duplicate_count": duplicates,
            "unparsed_fragments": [item[:200] for item in unparsed if item],
        }

    @staticmethod
    def _infer_notice_action(snapshot, room, rack, direction, cutoff):
        if not any(
            (item.get("room"), item.get("rack")) == (room, rack)
            for item in (snapshot.get("config") or {}).get("inventory", [])
        ):
            return "", "机柜未匹配当前目录，无法自动识别操作类型", "unknown"
        history = []
        for operation in snapshot.get("operations", []):
            if (operation.get("room"), operation.get("rack")) != (room, rack):
                continue
            for event in operation.get("events", []):
                item = {**operation, **event}
                actual = str(item.get("actual") or "")
                if (
                    completed_state_event(item)
                    and (not cutoff or actual <= cutoff)
                ):
                    history.append(item)
        latest = max(history, key=lambda item: str(item.get("actual") or ""), default=None)
        if latest is None:
            if direction == "up":
                return "上正式电", "当前机柜未上电，默认上正式电，可人工改为上测试电", "off"
            return "", "当前机柜未上电，下电操作需人工核对", "off"
        latest_action = str(latest.get("action") or "")
        state = STATES.get(latest_action, "unknown")
        if direction == "down":
            if state == "formal":
                return "下正式电", f"根据最近成功操作 {latest_action} 推断", state
            if state == "test":
                return "下测试电", f"根据最近成功操作 {latest_action} 推断", state
            return "", "当前已下电或状态不明，请人工核对", state
        if state == "off":
            return "上正式电", "当前机柜未上电，默认上正式电，可人工改为上测试电", state
        if state in {"formal", "test"}:
            return "", "当前已上电，不自动猜测转换操作", state
        return "", "历史无法确定上电类型，请人工选择", state

    def _refresh_notice_warnings(self, batch):
        if batch.get("source") != "notice":
            return
        rows = [
            row
            for row in batch.get("rows", [])
            if not row.get("notice_removed") and not str(row.get("status") or "").startswith("excluded_")
        ]
        source = batch.get("source_notice") or {}
        diagnostics = batch.get("parse_diagnostics") or {}
        warnings = []
        declared = source.get("declared_quantity")
        if declared is None:
            warnings.append({"code": "quantity_missing", "message": "通告数量未填写或无法识别"})
        elif int(declared) != len(rows):
            warnings.append(
                {
                    "code": "quantity_mismatch",
                    "message": f"通告声明 {declared} 柜，当前待办为 {len(rows)} 柜",
                }
            )
        duplicate_count = int(diagnostics.get("duplicate_count") or 0)
        if duplicate_count:
            warnings.append(
                {
                    "code": "duplicate_tokens",
                    "message": f"柜号中有 {duplicate_count} 个重复机柜，待办已按唯一机柜保留",
                }
            )
        fragments = list(diagnostics.get("unparsed_fragments") or [])
        if fragments:
            warnings.append(
                {
                    "code": "unparsed_fragments",
                    "message": "柜号中存在未识别内容：" + "；".join(fragments[:3]),
                }
            )
        invalid_codes = {"scope", "room", "rack", "inventory", "notice_scope"}
        unmatched = sum(
            bool(invalid_codes & {str(issue.get("code") or "") for issue in row.get("issues", [])})
            for row in rows
        )
        batch["notice_counts"] = {
            "declared": declared,
            "unique": len(rows),
            "directory_matched": max(0, len(rows) - unmatched),
        }
        if unmatched:
            warnings.append(
                {
                    "code": "directory_mismatch",
                    "message": f"{unmatched} 个机柜未通过当前楼栋目录校验",
                }
            )
        fingerprint = digest(
            [
                source.get("target_record_id"),
                declared,
                [(row.get("scope"), row.get("room"), row.get("rack")) for row in rows],
                warnings,
            ]
        )
        acknowledgement = batch.get("warning_acknowledgement") or {}
        if not warnings or acknowledgement.get("fingerprint") != fingerprint:
            batch.pop("warning_acknowledgement", None)
        requested_by = str(batch.pop("_warning_acknowledger", "") or "")
        if requested_by and warnings:
            batch["warning_acknowledgement"] = {
                "fingerprint": fingerprint,
                "owner": requested_by,
                "at": now(),
            }
        batch["blocking_warnings"] = warnings
        batch["warning_fingerprint"] = fingerprint if warnings else ""
        batch["warnings_acknowledged"] = bool(
            warnings
            and (batch.get("warning_acknowledgement") or {}).get("fingerprint")
            == fingerprint
        )

    def _change(self, batch_id, callback, *, expected_version=None, validate=False, partial_rows=False):
        for _attempt in range(3):
            with self._lock:
                batch = self.store.get(batch_id)
                if batch is None:
                    raise CabinetError("批次不存在", 404)
                if expected_version is not None and int(batch["version"]) != int(expected_version):
                    raise CabinetError("批次已被其他操作更新，请重新载入", 409)
                before_rows = {row["row_id"]: copy.deepcopy(row) for row in batch.get("rows", [])} if partial_rows else None
                callback(batch)
                if validate:
                    self._validate_rows(batch)
                self._refresh_summary(batch)
                try:
                    changed = None if before_rows is None else {
                        row["row_id"] for row in batch.get("rows", []) if before_rows.get(row["row_id"]) != row
                    }
                    return self.store.save(batch, batch["version"], changed)
                except CabinetError as exc:
                    if exc.status_code != 409 or expected_version is not None:
                        raise
        raise CabinetError("批次正在被其他任务更新，请稍后重试", 409)

    @staticmethod
    def _atomic_write(path, content):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=".upload-", dir=path.parent)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def recognize(self, files, owner):
        if not files or len(files) > MAX_FILES:
            raise CabinetError("每批须上传1至10份PDF")
        total = sum(len(content) for _name, content in files)
        if total > MAX_TOTAL_BYTES:
            raise CabinetError("单批PDF总大小不能超过30MiB", 413)
        metas = []
        for index, (filename, content) in enumerate(files):
            if len(content) > MAX_FILE_BYTES:
                raise CabinetError(f"{filename} 超过10MiB", 413)
            if not content.startswith(b"%PDF"):
                raise CabinetError(f"{filename} 不是有效PDF")
            sha = hashlib.sha256(content).hexdigest()
            metas.append({"file_id": f"f{index + 1}_{sha[:12]}", "name": Path(filename or f"file-{index + 1}.pdf").name,
                          "sha256": sha, "size": len(content), "status": "waiting", "pages": 0,
                          "processed_pages": 0, "error": ""})
        source_hash = hashlib.sha256("\n".join(sorted(item["sha256"] for item in metas)).encode()).hexdigest()
        existing = self.store.by_hash(source_hash)
        if existing:
            if existing["status"] == "failed" and all((self.import_root / existing["batch_id"] / f"{item['file_id']}.pdf").is_file() for item in existing.get("files", [])):
                def retry(batch):
                    batch.update(status="recognizing", error="", rows=[])
                    for item in batch.get("files", []): item.update(status="waiting", pages=0, processed_pages=0, error="")
                    batch["progress"]={"files_done":0,"files_total":len(batch.get("files",[])),"pages_done":0,"pages_total":0}
                existing=self._change(existing["batch_id"],retry)
                self._pdf_pool.submit(self._parse_batch,existing["batch_id"])
            existing["duplicate_upload"] = True
            return existing
        batch_id = uuid.uuid4().hex
        batch = self.store.create({
            "batch_id": batch_id, "owner_id": owner, "status": "recognizing", "source_hash": source_hash,
            "scopes": [], "source": "pdf", "files": metas, "rows": [], "error": "",
            "progress": {"files_done": 0, "files_total": len(files), "pages_done": 0, "pages_total": 0},
        })
        folder = self.import_root / batch_id
        try:
            for meta, (_name, content) in zip(metas, files):
                self._atomic_write(folder / f"{meta['file_id']}.pdf", content)
        except Exception as exc:
            self._change(batch_id, lambda item: item.update(status="failed", error=f"原PDF保存失败：{exc}"))
            raise CabinetError("原PDF保存失败") from exc
        self._pdf_pool.submit(self._parse_batch, batch_id)
        return batch

    def status(self, batch_id, owner, allowed, admin=False):
        batch = self.store.runtime_status(batch_id)
        if batch is None:
            raise CabinetError("批次不存在", 404)
        if not admin and batch["owner_id"] != owner and not set(batch.get("scopes", [])) & set(allowed):
            raise CabinetError("无权查看该批次", 403)
        return {key: copy.deepcopy(value) for key, value in batch.items() if key not in {"owner_id", "scopes", "rows"}}

    @staticmethod
    def _pdf_reader(path):
        try:
            from pypdf import PdfReader
        except ImportError as exc:
            raise CabinetError("缺少PDF解析组件 pypdf，请运行依赖修复后重试", 503) from exc
        reader = PdfReader(path)
        if reader.is_encrypted:
            raise CabinetError("加密PDF暂不支持，请改用手工批量填写")
        if len(reader.pages) > MAX_PAGES:
            raise CabinetError("单份PDF不能超过100页")
        return reader

    @staticmethod
    def _extract_page(page):
        try:
            return page.extract_text() or ""
        except TypeError:
            return page.extract_text(extraction_mode="layout") or ""

    @staticmethod
    def _current_state(snapshot, room, rack):
        latest = max(
            (event for operation in snapshot["operations"] if (operation["room"], operation["rack"]) == (room, rack)
             for event in operation["events"] if completed_state_event(event)),
            key=lambda event: str(event.get("actual") or ""), default=None,
        )
        return STATES.get(latest["action"], "unknown") if latest else "off"

    def _set_file_progress(self, batch_id, file_id, **values):
        def update(batch):
            target = next(item for item in batch["files"] if item["file_id"] == file_id)
            target.update(values)
            progress = batch["progress"]
            progress["files_done"] = sum(item["status"] in ("completed", "failed") for item in batch["files"])
            progress["pages_done"] = sum(int(item.get("processed_pages") or 0) for item in batch["files"])
            progress["pages_total"] = sum(int(item.get("pages") or 0) for item in batch["files"])
        self._change(batch_id, update)

    def _parse_pdf(self, batch_id, meta):
        path = self.import_root / batch_id / f"{meta['file_id']}.pdf"
        reader = self._pdf_reader(path)
        self._set_file_progress(batch_id, meta["file_id"], status="parsing", pages=len(reader.pages))
        texts = []
        for index, page in enumerate(reader.pages):
            text = self._extract_page(page)
            if not text.strip():
                raise CabinetError(f"第{index + 1}页没有可读取文本，扫描件请改用手工批量填写")
            texts.append(text)
            self._set_file_progress(batch_id, meta["file_id"], processed_pages=index + 1)
        complete_text = "\n".join(texts)
        title = re.search(r"机柜\s*([^\n]+?)确认单", complete_text)
        title_action = next((action for action in OPS if title and action in title.group(1)), "")
        header_action = next((action for action in OPS if re.search(r"操作类型\s*[：:]\s*" + re.escape(action), complete_text)), "")
        if not title_action or not header_action or title_action != header_action:
            raise CabinetError("文件标题与操作类型不一致或版式无法识别")
        applicant = (re.search(r"申请人\s*[：:]\s*([^\s]+)", complete_text) or [None, ""])[1]
        if applicant == "[null]":
            applicant = ""
        application_ids = list(dict.fromkeys(re.findall(r"Z\d{12,}", complete_text)))
        parsed = []
        source_row = 0
        for page_number, text in enumerate(texts, 1):
            for line in text.splitlines():
                if not line.strip().startswith("EA118 "):
                    continue
                match = ROW_RE.match(line)
                if not match:
                    raise CabinetError(f"第{page_number}页存在无法识别的机柜行，请改用手工批量填写")
                scope, floor, room_number, rack, supplier, rack_type, detail, tail = match.groups()
                dates = DATE_RE.findall(tail)
                result_match = re.search(r"成功|失败", tail)
                result = result_match.group() if result_match else ""
                remainder = DATE_RE.sub("", tail)
                remainder = re.sub(r"成功|失败", "", remainder).strip()
                if remainder or len(dates) > 2:
                    raise CabinetError(f"第{page_number}页存在无法识别的机柜行，请改用手工批量填写")
                actual = dates[0] if len(dates) == 2 or len(dates) == 1 and result else ""
                source_row += 1
                room = f"{floor}{int(room_number):02d}"
                current = {
                    "scope": scope, "room": room, "rack": rack, "supplier_rack": supplier,
                    "rack_type": rack_type, "type_detail": detail, "action": header_action,
                    "expected": actual, "actual": actual, "result": result,
                }
                parsed.append({
                    **current, "row_id": "row_" + digest([meta["file_id"], meta["sha256"], page_number, source_row, current])[:24],
                    "source_index": 0, "file_id": meta["file_id"], "file_name": meta["name"],
                    "file_sha256": meta["sha256"], "page": page_number, "source_row": source_row,
                    "application_ids": application_ids,
                    "applicant": applicant, "original": copy.deepcopy(current), "edits": [],
                    "status": "ready", "issues": [], "error": "", "type_resolution": "",
                })
        if not parsed:
            raise CabinetError("未识别到机柜明细；扫描件或未知版式请改用手工批量填写")
        return parsed

    def _parse_batch(self, batch_id):
        rows = []
        try:
            batch = self.store.get(batch_id)
            for meta in batch["files"]:
                try:
                    parsed = self._parse_pdf(batch_id, meta)
                    rows.extend(parsed)
                    self._set_file_progress(batch_id, meta["file_id"], status="completed", error="")
                except Exception as exc:
                    self._set_file_progress(batch_id, meta["file_id"], status="failed", error=str(exc))
            if len(rows) > MAX_ROWS:
                raise CabinetError("单批识别结果不能超过2000行")
            for index, row in enumerate(rows, 1):
                row["source_index"] = index
                row["operation_id"] = "batch_" + digest([batch_id, row["row_id"]])[:32]
            def complete(batch):
                batch["rows"] = rows
                errors = [item["name"] + "：" + item["error"] for item in batch["files"] if item["status"] == "failed"]
                batch["error"] = "；".join(errors)
                batch["validation_error"] = False
                batch["status"] = "recognizing" if rows else "failed"
            self._change(batch_id, complete)
            if rows:
                self._change(batch_id, lambda batch: batch.update(status="pending"), validate=True)
        except Exception as exc:
            def failed(batch):
                if rows and not batch.get("rows"):
                    for index, row in enumerate(rows, 1):
                        row["source_index"] = index
                        row.setdefault("operation_id", "batch_" + digest([batch_id, row["row_id"]])[:32])
                    batch["rows"] = rows
                if batch.get("rows"):
                    for row in batch["rows"]:
                        if row.get("status") not in LOCKED_ROW_STATUSES:
                            row.update(status="invalid", issues=[{"code":"validation_error","message":str(exc)}])
                    batch["status"] = "pending"
                else:
                    batch["status"] = "failed"
                batch["error"] = str(exc)
                batch["validation_error"] = bool(batch.get("rows"))
            self._change(batch_id, failed)

    def _normalize_row(self, row):
        row.pop("order_time", None)
        row.pop("application_time", None)
        for key in ("scope", "room", "rack", "supplier_rack", "rack_type", "type_detail", "action", "expected", "actual", "result", "failure_reason", "type_resolution"):
            row[key] = str(row.get(key) or "").strip()
        row["scope"] = row["scope"].upper().replace("楼", "")
        row["rack"] = row["rack"].upper()
        if row["supplier_rack"].lower() in {"-", "/", "null", "[null]", "none"}:
            row["supplier_rack"] = ""
        for key in ("expected", "actual"):
            row[key] = row[key].replace("T", " ")
    @staticmethod
    def _valid_date(value, allow_future=False):
        if not DATE_RE.fullmatch(value):
            return False
        try:
            return allow_future or dt.datetime.fromisoformat(value) <= dt.datetime.now()
        except ValueError:
            return False

    def _validation_context(self, scopes):
        inventories, exact, slots, states = {}, set(), {}, {}
        for scope in scopes:
            snap = self.cabinet._snapshot(scope)
            inventories[scope] = {(item["room"], item["rack"]): item for item in snap["config"]["inventory"]}
            latest = {}
            for operation in snap["operations"]:
                for event in operation["events"]:
                    if not event.get("action") or not event.get("actual"):
                        continue
                    key = (scope, operation["room"], operation["rack"], event["action"], event["actual"])
                    exact.add(key)
                    slots.setdefault(key[:3] + (key[4],), set()).add(key[3])
                    cabinet_key = (operation["room"], operation["rack"])
                    if completed_state_event(event) and (cabinet_key not in latest or event["actual"] > latest[cabinet_key]["actual"]):
                        latest[cabinet_key] = event
            states[scope] = {cabinet_key: STATES.get(event["action"], "unknown") for cabinet_key,event in latest.items()}
        return inventories, exact, slots, states

    def _validate_rows(self, batch):
        rows = batch.get("rows", [])
        notice_scope = str((batch.get("source_notice") or {}).get("scope") or "").upper()
        scopes = {str(row.get("scope") or "").upper().replace("楼", "") for row in rows}
        valid_scopes = {scope for scope in scopes if scope in SCOPES}
        inventories, existing_exact, existing_slots, current_states = self._validation_context(valid_scopes) if valid_scopes else ({}, set(), {}, {})
        normalized = []
        batch_slots = {}
        for row in rows:
            if row.get("status") in LOCKED_ROW_STATUSES or str(row.get("status", "")).startswith("excluded_"):
                continue
            self._normalize_row(row)
            key = (row["scope"], row["room"], row["rack"], row["action"], row["actual"])
            slot = key[:3] + (key[4],)
            normalized.append((row, key, slot))
            if all(key):
                batch_slots.setdefault(slot, set()).add(row["action"])
        image_states = {}
        if batch.get("source") == "image":
            pending_states = {scope: states.copy() for scope, states in current_states.items()}
            for pending_row, key, slot in sorted(normalized, key=lambda item: (item[0].get("actual", ""), item[0].get("source_index", 0))):
                scope, room, rack, action, _actual = key
                state = pending_states.get(scope, {}).get((room, rack), "off")
                image_states[pending_row["row_id"]] = state
                if (pending_row.get("result") == "成功" and action in POWER_ACTIONS_BY_STATE.get(state, ())
                        and all(key) and key not in existing_exact and len(batch_slots.get(slot, ())) == 1):
                    pending_states.setdefault(scope, {})[(room, rack)] = STATES.get(action, state)
        seen = set()
        for row, key, slot in normalized:
            issues = []
            rolled_back = row.get("status") == "rolled_back"
            scope, room, rack, action, actual = key
            if scope not in SCOPES:
                issues.append({"code": "scope", "message": "楼栋无效"})
            if notice_scope in SCOPES and scope != notice_scope:
                issues.append({"code": "notice_scope", "message": f"柜号楼栋与通告{notice_scope}楼不一致"})
            if not re.fullmatch(r"[1-4]\d{2}", room):
                issues.append({"code": "room", "message": "包间格式无效"})
            if not re.fullmatch(r"[A-Z]\d{2}", rack):
                issues.append({"code": "rack", "message": "机柜编号格式无效"})
            if action not in OPS:
                issues.append({"code": "action", "message": "操作类型无效"})
            if not row["expected"] or not self._valid_date(row["expected"], allow_future=True):
                issues.append({"code": "expected", "message": "期望完成时间必填且须为有效时间"})
            if not actual or not self._valid_date(actual):
                issues.append({"code": "actual", "message": "实际完成时间须为有效且不晚于当前的时间"})
            if row["result"] not in ("成功", "失败"):
                issues.append({"code": "result", "message": "操作结果须选择成功或失败"})
            if row["result"] == "失败" and not row.get("failure_reason"):
                issues.append({"code": "failure_reason", "message": "操作失败时须填写失败原因"})
            if len(row.get("failure_reason", "")) > 1000:
                issues.append({"code": "failure_reason_length", "message": "失败原因不能超过1000字"})
            inventory = inventories.get(scope, {}).get((room, rack))
            if batch.get("source") in ("pdf", "image") and inventory is not None:
                state = image_states.get(row["row_id"], current_states.get(scope, {}).get((room, rack), "off"))
                row["current_power_state"] = state
                if action and action not in POWER_ACTIONS_BY_STATE.get(state, ()):
                    issues.append({"code": "state_action", "message": f"当前为{POWER_STATE_LABELS.get(state, '状态待核实')}，只能选择{'或'.join(sorted(POWER_ACTIONS_BY_STATE.get(state, ()))) or '核实状态后操作'}"})
            if inventory is None and scope in SCOPES and re.fullmatch(r"[1-4]\d{2}", room) and re.fullmatch(r"[A-Z]\d{2}", rack):
                issues.append({"code": "inventory", "message": "机柜不在当前楼栋目录中"})
            elif inventory:
                current_type = str(inventory.get("rack_type") or "")
                row["current_rack_type"] = current_type
                if row["rack_type"] not in RACK_TYPES:
                    issues.append({"code": "rack_type", "message": "机柜类型无效"})
                elif current_type and row["rack_type"] != current_type and row.get("type_resolution") not in ("keep_current", "sync_current"):
                    issues.append({"code": "type_mismatch", "message": f"文件类型为{row['rack_type']}，当前目录为{current_type}"})
            conflict_actions = set(existing_slots.get(slot, set())) | set(batch_slots.get(slot, set()))
            conflict = bool(all(slot) and any(candidate != action for candidate in conflict_actions))
            duplicate = bool(all(key) and (key in existing_exact or key in seen))
            if rolled_back and self.store.later_completed(batch["batch_id"], scope, room, rack):
                issues.append({"code": "later_batch", "message": "该机柜已有后续批次操作，不能再次确认"})
            if conflict:
                issues.append({"code": "time_conflict", "message": "同一机柜同一实际时间存在不同操作，须人工核对"})
                row["status"] = "rolled_back" if rolled_back else "conflict"
            elif duplicate:
                row["status"] = "rolled_back" if rolled_back else "duplicate"
                issues = [{"code": "overlap", "message": "与本批前序行或既有台账完全重叠"}]
            elif issues:
                row["status"] = "rolled_back" if rolled_back else "invalid"
            elif row.get("status") != "failed":
                row["status"] = "rolled_back" if rolled_back else "ready"
            row["issues"] = issues
            row["error"] = row.get("error", "") if row["status"] == "failed" else ""
            if all(key):
                seen.add(key)
        self._refresh_notice_warnings(batch)

    def create_manual(self, rows, owner):
        if not isinstance(rows, list) or not rows or len(rows) > MAX_ROWS:
            raise CabinetError("手工批量须包含1至2000行")
        batch_id = uuid.uuid4().hex
        prepared = []
        for index, source in enumerate(rows, 1):
            if not isinstance(source, dict):
                raise CabinetError(f"第{index}行格式无效")
            current = {key: str(source.get(key) or "").strip() for key in EDITABLE_FIELDS}
            if not current["expected"] and current["actual"]:
                current["expected"] = current["actual"]
            row_id = "row_" + digest([batch_id, index, current])[:24]
            prepared.append({
                **current, "row_id": row_id, "source_index": index, "file_id": "", "file_name": "手工批量",
                "file_sha256": "", "page": 0, "source_row": index, "application_ids": [],
                "applicant": "", "original": copy.deepcopy(current), "edits": [],
                "status": "ready", "issues": [], "error": "", "operation_id": "batch_" + digest([batch_id, row_id])[:32],
            })
        batch = self.store.create({
            "batch_id": batch_id, "owner_id": owner, "status": "pending", "source_hash": "", "scopes": [],
            "source": "manual", "files": [], "rows": prepared, "error": "",
            "progress": {"files_done": 0, "files_total": 0, "pages_done": 0, "pages_total": 0},
        })
        return self._change(batch_id, lambda _batch: None, validate=True)

    def create_image_batch(self, owner, allowed, entry_scope=""):
        scopes = sorted(set(allowed) & SCOPES)
        if not scopes:
            raise CabinetError("没有可识别的机柜楼栋权限", 403)
        entry_scope = str(entry_scope or "").upper().replace("楼", "")
        if entry_scope and entry_scope not in scopes:
            raise CabinetError("无权创建该楼栋图片待办", 403)
        batch = {"batch_id": uuid.uuid4().hex, "owner_id": owner, "status": "pending",
                 "source_hash": "", "scopes": [], "source": "image", "recognition_scopes": scopes,
                 "entry_scope": entry_scope, "files": [], "images": [], "rows": [], "error": "",
                 "progress": {"files_done": 0, "files_total": 0, "pages_done": 0, "pages_total": 0}}
        self._refresh_summary(batch)
        return self.store.create(batch)

    def _notice_rows(self, source, source_hash, parsed):
        direction = "up" if source["notice_type"] == "上电通告" else "down"
        expected = self._notice_datetime(source.get("end_time"))
        cutoff = self._notice_datetime(source.get("sent_at")) or self._notice_datetime(source.get("start_time"))
        snapshots = {scope: self.cabinet._snapshot(scope) for scope in {item["scope"] for item in parsed}}
        inventories = {
            scope: {(item["room"], item["rack"]): item for item in snapshot["config"]["inventory"]}
            for scope, snapshot in snapshots.items()
        }
        rows = []
        for index, item in enumerate(parsed, 1):
            inventory = inventories[item["scope"]].get((item["room"], item["rack"]))
            action, inference, current_power_state = self._infer_notice_action(
                snapshots[item["scope"]], item["room"], item["rack"], direction, cutoff,
            )
            current = {
                **item, "supplier_rack": "", "rack_type": str((inventory or {}).get("rack_type") or ""),
                "type_detail": "", "action": action, "expected": expected, "actual": "",
                "result": "成功", "type_resolution": "",
                "current_power_state": current_power_state,
            }
            row_id = "row_" + digest([source_hash, item])[:24]
            rows.append({
                **current, "row_id": row_id, "source_index": index, "file_id": "",
                "file_name": "上下电通告", "file_sha256": "", "page": 0, "source_row": index,
                "application_ids": [],
                "applicant": str(source.get("sender_name") or ""), "inference": inference,
                "exclude_notice_summary": False, "notice_removed": False,
                "original": copy.deepcopy(current), "edits": [], "status": "ready", "issues": [], "error": "",
                "operation_id": "batch_" + digest([source_hash, row_id])[:32],
            })
        return rows

    def create_from_notice(self, source):
        if not isinstance(source, dict):
            raise CabinetError("上下电通告待办来源无效")
        notice_type = str(source.get("notice_type") or "").strip()
        if notice_type not in ("上电通告", "下电通告"):
            raise CabinetError("仅支持上电通告或下电通告")
        target_record_id = str(source.get("target_record_id") or "").strip()
        if not target_record_id:
            raise CabinetError("上下电通告缺少目标记录ID")
        source_hash = hashlib.sha256(
            f"notice:v1:{notice_type}:{target_record_id}".encode("utf-8")
        ).hexdigest()
        existing = self.store.by_hash(source_hash)
        if existing:
            existing["duplicate_source"] = True
            return existing
        parsed, diagnostics = self._parse_notice_cabinets(source.get("cabinet"))
        if not parsed:
            raise CabinetError("通告柜号未识别到包间和机柜")
        batch_id = uuid.uuid4().hex
        rows = self._notice_rows(source, source_hash, parsed)
        scope = str(source.get("scope") or "").upper().replace("楼", "")
        source_notice = {
            "job_id": str(source.get("job_id") or ""),
            "target_record_id": target_record_id,
            "notice_type": notice_type,
            "title": str(source.get("title") or ""),
            "scope": scope,
            "start_time": self._notice_datetime(source.get("start_time")),
            "end_time": self._notice_datetime(source.get("end_time")),
            "cabinet": str(source.get("cabinet") or ""),
            "quantity": str(source.get("quantity") or ""),
            "declared_quantity": self._notice_quantity(source.get("quantity")),
            "sender_open_id": str(source.get("sender_open_id") or ""),
            "sender_name": str(source.get("sender_name") or ""),
            "sent_at": self._notice_datetime(source.get("sent_at")),
            "last_event_at": float(source.get("event_at") or 0),
            "ended_at": "",
            "deleted_at": "",
        }
        try:
            batch = self.store.create(
                {
                    "batch_id": batch_id,
                    "owner_id": str(source.get("owner_id") or source.get("sender_open_id") or "system"),
                    "status": "pending",
                    "source_hash": source_hash,
                    "scopes": [],
                    "source": "notice",
                    "source_notice": source_notice,
                    "parse_diagnostics": diagnostics,
                    "files": [],
                    "rows": rows,
                    "error": "",
                    "progress": {
                        "files_done": 0,
                        "files_total": 0,
                        "pages_done": 0,
                        "pages_total": 0,
                    },
                }
            )
        except sqlite3.IntegrityError:
            existing = self.store.by_hash(source_hash)
            if existing:
                existing["duplicate_source"] = True
                return existing
            raise
        return self._change(batch["batch_id"], lambda _batch: None, validate=True)

    def _sync_notice_rows(self, batch, source):
        cabinet_text = str(source.get("cabinet") or (batch.get("source_notice") or {}).get("cabinet") or "")
        parsed, diagnostics = self._parse_notice_cabinets(cabinet_text)
        if not parsed:
            raise CabinetError("最新通告柜号无法识别，待办保留原明细", 409)
        notice_type = str(source.get("notice_type") or batch["source_notice"]["notice_type"])
        direction_changed = notice_type != batch["source_notice"]["notice_type"]
        fresh = self._notice_rows({**source, "notice_type": notice_type}, batch["source_hash"], parsed)
        by_identity = {(row["scope"], row["room"], row["rack"]): row for row in fresh}

        def update(current):
            notice = current["source_notice"]
            if source.get("event_at"):
                notice["last_event_at"] = float(source["event_at"])
            notice["notice_type"] = notice_type
            if source.get("scope"):
                notice["scope"] = str(source["scope"]).upper().replace("楼", "")
            if source.get("title"):
                notice["title"] = str(source["title"])
            quantity = str(source.get("quantity") or notice.get("quantity") or "")
            notice.update(cabinet=cabinet_text, quantity=quantity,
                          declared_quantity=self._notice_quantity(quantity),
                          start_time=self._notice_datetime(source.get("start_time")) or notice.get("start_time", ""),
                          end_time=self._notice_datetime(source.get("end_time")) or notice.get("end_time", ""))
            current["parse_diagnostics"] = diagnostics
            matched = set()
            used_ids = {row["row_id"] for row in current["rows"]}
            for row in current["rows"]:
                identity = (row.get("scope"), row.get("room"), row.get("rack"))
                replacement = by_identity.get(identity)
                started = row.get("status") in LOCKED_ROW_STATUSES or bool(row.get("operation_started") and row.get("status") != "rolled_back")
                same = replacement is not None and identity not in matched and not (direction_changed and started)
                removed = not same
                if row.get("notice_removed") != removed:
                    row.setdefault("edits", []).append({"field": "notice_removed", "before": bool(row.get("notice_removed")),
                                                          "after": removed, "owner": "notice", "at": now()})
                row["notice_removed"] = removed
                if same:
                    matched.add(identity)
                    row["source_index"] = replacement["source_index"]
                    if direction_changed and row.get("status") not in LOCKED_ROW_STATUSES:
                        for key in ("action", "inference", "current_power_state"):
                            row[key] = replacement[key]
            for identity, row in by_identity.items():
                if identity in matched:
                    continue
                if row["row_id"] in used_ids:
                    index = 1
                    while True:
                        replacement_id = "row_" + digest([batch["source_hash"], identity, "replacement", index])[:24]
                        if replacement_id not in used_ids:
                            row["row_id"] = replacement_id
                            row["operation_id"] = "batch_" + digest([batch["source_hash"], replacement_id])[:32]
                            break
                        index += 1
                used_ids.add(row["row_id"])
                current["rows"].append(row)
        return self._change(batch["batch_id"], update, validate=True)

    def apply_notice_event(self, source):
        action = str(source.get("event_action") or "start").lower()
        record_id = str(source.get("target_record_id") or "").strip()
        batch = self.store.notice_by_target(record_id)
        if batch is None and source.get("prior_record_id"):
            batch = self.store.notice_by_target(str(source["prior_record_id"]))
        if action == "start":
            if batch is None:
                return self.create_from_notice(source)
            sent_at = self._notice_datetime(source.get("sent_at"))
            if sent_at and not (batch.get("source_notice") or {}).get("sent_at"):
                batch = self._change(batch["batch_id"], lambda current: current["source_notice"].update(sent_at=sent_at))
            return batch
        if batch is None:
            if action in {"update", "end"} and source.get("cabinet"):
                batch = self.create_from_notice({**source, "sent_at": self._notice_datetime(source.get("start_sent_at"))})
            else:
                raise CabinetError("来源通告待办尚未创建，请稍后重试", 409)
        event_at = float(source.get("event_at") or 0)
        if event_at and event_at < float((batch.get("source_notice") or {}).get("last_event_at") or 0):
            return batch
        batch_id = batch["batch_id"]
        start_sent_at = self._notice_datetime(source.get("start_sent_at"))
        if start_sent_at and not (batch.get("source_notice") or {}).get("sent_at"):
            batch = self._change(batch_id, lambda current: current["source_notice"].update(sent_at=start_sent_at))
        if action == "end" and not self._notice_datetime(source.get("sent_at")):
            raise CabinetError("结束通告缺少实际发送时间", 409)
        if action == "update" or action == "end" and source.get("cabinet_verified"):
            batch = self._sync_notice_rows(batch, source)
        if action in {"end", "undo_end", "delete", "undo_delete"}:
            def lifecycle(current):
                notice = current["source_notice"]
                if event_at:
                    notice["last_event_at"] = event_at
                if action == "end":
                    ended_at = self._notice_datetime(source.get("sent_at"))
                    if not ended_at:
                        raise CabinetError("结束通告缺少实际发送时间", 409)
                    if source.get("notice_type") in ("上电通告", "下电通告"):
                        notice["notice_type"] = source["notice_type"]
                    notice["ended_at"] = ended_at
                elif action == "undo_end":
                    notice["ended_at"] = ""
                elif action == "delete":
                    notice["deleted_at"] = self._notice_datetime(source.get("sent_at")) or now()
                else:
                    notice["deleted_at"] = ""
                    notice["target_record_id"] = record_id
                event_key = str(source.get("idempotency_key") or "")
                audit = notice.setdefault("lifecycle_audit", [])
                if not event_key or not any(item.get("event_key") == event_key for item in audit):
                    audit.append({"action": action, "at": now(), "record_id": record_id,
                                  "event_key": event_key})
            batch = self._change(batch_id, lifecycle)
        elif action != "update":
            raise CabinetError("未知通告联动动作", 400)

        if action in {"update", "end", "delete"}:
            if any(row.get("status") in {"queued", "writing"} and
                   (action == "delete" or row.get("notice_removed")) for row in batch["rows"]):
                self._change(batch_id, lambda current: current["source_notice"].update(
                    rollback_error="待办正在写入，完成后自动回退"))
                raise CabinetError("待办正在写入，完成后自动回退", 409)
            rollback_ids = [row["row_id"] for row in batch["rows"]
                            if (action == "delete" or row.get("notice_removed"))
                            and row.get("wrote_record") is not False
                            and row.get("status") in {"completed", "rollback_failed", "rollback_blocked"}]
            if rollback_ids:
                try:
                    batch = self.rollback(batch_id, {"version": batch["version"], "row_ids": rollback_ids}, "system", SCOPES, True)
                except Exception as exc:
                    self._change(batch_id, lambda current: current["source_notice"].update(rollback_error=str(exc)))
                    raise
            if (batch.get("source_notice") or {}).get("rollback_error"):
                batch = self._change(batch_id, lambda current: current["source_notice"].pop("rollback_error", None))
        return batch

    def _resume_notice_rollback_after_confirm(self, batch_id):
        batch = self.store.get(batch_id)
        if not batch or batch.get("source") != "notice":
            return
        notice = batch.get("source_notice") or {}
        if not notice.get("deleted_at") and not any(row.get("notice_removed") for row in batch["rows"]):
            return
        action = "delete" if notice.get("deleted_at") else "update"
        try:
            self.apply_notice_event({"event_action": action, "target_record_id": notice["target_record_id"],
                                     "notice_type": notice["notice_type"], "cabinet": notice.get("cabinet", ""),
                                     "quantity": notice.get("quantity", "")})
        except Exception:
            pass

    def notice_summary(self, scope, config):
        known = {(item["room"], item["rack"]): item for item in config["inventory"]}
        items = []
        projected = self.store.notice_summary_items(scope)
        template_hash = str((config.get("template_data") or {}).get("hash") or "")
        with _NOTICE_CUTOFFS_LOCK:
            cutoff = _NOTICE_CUTOFFS.get(template_hash)
        if cutoff is None:
            template_path = Path(config.get("path") or Path(__file__).with_name("templates") / "cabinet_power" / f"{scope}.xlsm")
            cutoff = notice_summary_baseline_date(template_path.read_bytes())
            with _NOTICE_CUTOFFS_LOCK:
                _NOTICE_CUTOFFS[template_hash] = cutoff
        frozen = self.store.notice_summary_baseline(
            scope, template_hash, cutoff,
            [f"{row['batch_id']}:{row['row_id']}" for row in projected if cutoff and row["sent_at"][:10] <= cutoff],
        )
        for row in projected:
            key = (row.get("room"), row.get("rack"))
            event_key = f"{row['batch_id']}:{row['row_id']}"
            if key not in known or event_key in frozen:
                continue
            sent_at = self._notice_datetime(row.get("sent_at"))
            if not sent_at:
                continue
            items.append({
                "batch_id": row["batch_id"], "row_id": row.get("row_id", ""),
                "room": key[0], "rack": key[1], "date": sent_at[:10], "sent_at": sent_at,
                "direction": "up" if row.get("notice_type") == "上电通告" else "down",
                "action": str(row.get("action") or ""),
                "rack_type": str(known[key].get("rack_type") or row.get("rack_type") or ""),
            })
        items.sort(key=lambda item: (item["sent_at"], item["batch_id"], item["row_id"]))
        return {"scope": scope, "version": digest([template_hash, cutoff, items]), "items": items,
                "template_hash": template_hash, "baseline_date": cutoff}

    @staticmethod
    def _notice_record_missing(error):
        normalized = re.sub(r"[\s_\-:：]+", "", str(error or "")).lower()
        return any(token in normalized for token in (
            "1254043", "recordidnotfound", "recordldnotfound", "记录id不存在",
        ))

    def reconcile_legacy_notice_end_times(self, fetch_record, force=False):
        checked = 0
        for batch in self.store.notice_batches():
            source = batch.get("source_notice") or {}
            if source.get("ended_at") or source.get("deleted_at") or not source.get("target_record_id"):
                continue
            prior_check = source.get("end_time_check") or {}
            prior_missing = self._notice_record_missing(prior_check.get("error"))
            if not prior_missing and not force and dt.datetime.now().timestamp() - float(prior_check.get("checked_at") or 0) < 3600:
                continue
            if prior_missing:
                state, error, ended_at = "deleted", "目标多维记录不存在，已按来源通告删除处理", ""
            else:
                try:
                    ok, record = fetch_record(source["target_record_id"], source["notice_type"])
                    if not ok or not isinstance(record, dict):
                        raise CabinetError(str(record or "目标多维读取失败"))
                    fields = record.get("fields") if isinstance(record.get("fields"), dict) else record
                    status = str(fields.get("上电状态", ""))
                    ended_at = self._notice_datetime(fields.get("实际结束时间"))
                    state = "resolved" if "结束" in status and ended_at else "pending"
                    error = "" if state == "resolved" else ("结束状态缺少实际结束时间" if "结束" in status else "目标通告尚未结束")
                except Exception as exc:
                    missing = self._notice_record_missing(exc)
                    state, error, ended_at = (
                        ("deleted", "目标多维记录不存在，已按来源通告删除处理", "")
                        if missing else ("failed", str(exc)[:200], "")
                    )
            def save(current):
                notice = current["source_notice"]
                if notice.get("ended_at") or notice.get("deleted_at"):
                    return
                notice["end_time_check"] = {"status": state, "error": error,
                                             "checked_at": dt.datetime.now().timestamp()}
                if state == "resolved":
                    notice["ended_at"] = ended_at
                    notice.setdefault("lifecycle_audit", []).append({"action": "legacy_end_reconciled", "at": now()})
                elif state == "deleted":
                    notice["deleted_at"] = now()
                    notice.setdefault("lifecycle_audit", []).append({
                        "action": "remote_record_missing", "at": now(),
                        "record_id": str(notice.get("target_record_id") or ""),
                    })
            try:
                self._change(batch["batch_id"], save)
                checked += state == "resolved"
            except Exception:
                continue
        return checked

    def get(self, batch_id):
        batch = self.store.get(batch_id)
        if batch is None:
            raise CabinetError("批次不存在", 404)
        if batch.get("status")=="partial" and not any(
            row.get("status") in ("completed","rolled_back") for row in batch.get("rows",[])
        ):
            self._refresh_summary(batch)
        if batch.get("source") == "notice" and batch.get("status") != "cancelled":
            source = batch.get("source_notice") or {}
            direction = "up" if source.get("notice_type") == "上电通告" else "down"
            cutoff = self._notice_datetime(source.get("sent_at")) or self._notice_datetime(source.get("start_time"))
            snapshots = {}
            updates = {}
            for row in batch.get("rows", []):
                if row.get("status") in LOCKED_ROW_STATUSES:
                    continue
                patch = {}
                if not row.get("result"):
                    patch["result"] = "成功"
                if not row.get("current_power_state") and row.get("scope") in SCOPES:
                    scope = str(row.get("scope") or "")
                    snapshot = snapshots.get(scope)
                    if snapshot is None:
                        snapshot = self.cabinet._snapshot(scope)
                        snapshots[scope] = snapshot
                    action, inference, state = self._infer_notice_action(
                        snapshot, row.get("room"), row.get("rack"), direction, cutoff
                    )
                    patch["current_power_state"] = state
                    action_was_edited = any(
                        edit.get("field") == "action" for edit in row.get("edits", [])
                    )
                    if not row.get("action") and not action_was_edited:
                        patch.update(action=action, inference=inference)
                if patch:
                    updates[row["row_id"]] = patch
            if updates:
                def apply_defaults(current):
                    for row in current.get("rows", []):
                        row.update(updates.get(row.get("row_id"), {}))
                batch = self._change(
                    batch_id,
                    apply_defaults,
                    expected_version=batch["version"],
                    validate=True,
                )
        return batch

    def visible(self, batch, owner, allowed, admin=False):
        if not admin and batch["owner_id"] != owner and not set(batch.get("scopes", [])) & set(allowed):
            raise CabinetError("无权查看该批次", 403)
        result = copy.deepcopy(batch)
        deleted_notice = bool((batch.get("source_notice") or {}).get("deleted_at"))
        if not admin and batch["owner_id"] != owner:
            result["rows"] = [row for row in result.get("rows", []) if row.get("scope") in allowed]
            result["stats"] = result.pop("_visible_stats", None) or self._stats(result["rows"])
            permitted_images = {
                image.get("image_id") for image in batch.get("images", [])
                if self._image_scopes(batch, image.get("image_id")) <= set(allowed)
            }
            result["images"] = [image for image in result.get("images", [])
                                if image.get("image_id") in permitted_images]
            for image in result["images"]:
                image["suggestions"] = [item for item in image.get("suggestions", [])
                                        if not item.get("scope") or item.get("scope") in allowed]
            result["files"] = []
        else:
            result.pop("_visible_stats", None)
        for row in result.get("rows", []):
            row["can_edit_notice_summary"] = bool(batch.get("source") == "notice" and
                (admin or row.get("scope") in allowed) and not (batch.get("source_notice") or {}).get("deleted_at"))
            row["editable"] = not deleted_notice and not row.get("notice_removed") and bool(admin or row.get("scope") in allowed) and batch.get("status") != "cancelled" and row.get("status") not in LOCKED_ROW_STATUSES and row.get("status") != "excluded_image" and (not row.get("operation_started") or row.get("status") == "rolled_back")
            row["confirmable"] = not deleted_notice and bool(admin or row.get("scope") in allowed) and not row.get("notice_removed") and row.get("status") in ACTIVE_ROW_STATUSES and not row.get("issues")
            row["rollbackable"] = bool(admin or row.get("scope") in allowed) and row.get("wrote_record") is not False and row.get("status") in {"completed", "rollback_failed", "rollback_blocked"}
            row["restorable"] = bool(admin or row.get("scope") in allowed) and row.get("status") in {"excluded_manual", "excluded_duplicate", "excluded_cancelled"} and not row.get("operation_started")
        result["can_download_files"] = bool(admin or batch["owner_id"] == owner)
        result["allowed_scopes"] = [scope for scope in batch.get("scopes", []) if admin or scope in allowed]
        result["can_confirm_all"] = bool(admin or set(batch.get("scopes", [])) <= set(allowed))
        return result

    def list(self, owner, allowed, admin=False, scope="", status="", date_from="", date_to="", page=1, page_size=20):
        items = []
        for batch in self.store.runtime_list():
            deleted_notice = bool((batch.get("source_notice") or {}).get("deleted_at"))
            if deleted_notice and status != "notice_rollback_error":
                continue
            if not admin and batch["owner_id"] != owner and not set(batch.get("scopes", [])) & set(allowed):
                continue
            if scope and scope not in batch.get("scopes", []) and not (
                batch.get("source") == "image" and not batch.get("rows") and batch.get("entry_scope") == scope
            ):
                continue
            if date_from and batch["created_at"][:10] < date_from:
                continue
            if date_to and batch["created_at"][:10] > date_to:
                continue
            visible_rows = batch.get("rows", []) if admin or batch["owner_id"] == owner else [
                row for row in batch.get("rows", []) if row.get("scope") in allowed
            ]
            rollback_error = any(
                row.get("status") in {"rollback_failed", "rollback_blocked"}
                and (deleted_notice or row.get("notice_removed")) for row in visible_rows
            ) or bool((batch.get("source_notice") or {}).get("rollback_error") and
                      (admin or batch["owner_id"] == owner or any(
                          row.get("status") in {"queued", "writing", "rollback_queued", "rolling_back"}
                          for row in visible_rows)))
            if status == "notice_rollback_error" and not rollback_error:
                continue
            pending_rows = sum(not row.get("notice_removed") and row.get("status") in {"ready", "invalid", "conflict", "duplicate", "failed",
                                                      "rolled_back", "queued", "writing", "rollback_queued",
                                                      "rolling_back", "rollback_failed", "rollback_blocked"}
                               for row in visible_rows)
            is_todo = bool(batch["status"] != "cancelled" and (
                pending_rows or batch["status"] == "recognizing" or
                batch.get("source") == "image" and not batch.get("rows") and batch["status"] == "pending"
            ))
            if status == "todo" and not is_todo or status and status not in {"todo", "notice_rollback_error"} and batch["status"] != status:
                continue
            active_images = [image for image in batch.get("images", []) if not image.get("deleted_at")]
            pending_label = ("图片识别中" if any(image.get("status") == "recognizing" for image in active_images)
                             else "待撤回或重传" if batch.get("source") == "image" and not active_images and batch.get("images")
                             else "待上传图片" if batch.get("source") == "image" and not active_images
                             else "待核对图片" if batch.get("source") == "image" else "解析中")
            items.append({**{key: batch.get(key) for key in ("batch_id", "owner_id", "status", "source", "scopes", "stats", "created_at", "updated_at", "error")},
                          "scopes": batch.get("scopes", []) if admin or batch["owner_id"] == owner else [scope for scope in batch.get("scopes", []) if scope in allowed],
                          "stats": batch.get("stats") if admin or batch["owner_id"] == owner else self._stats(visible_rows),
                          "pending_rows": pending_rows, "pending_label": pending_label, "is_todo": is_todo,
                          "notice_rollback_error": rollback_error})
        page_size = max(1, min(int(page_size), 100))
        pages = max(1, (len(items) + page_size - 1) // page_size)
        page = max(1, min(int(page), pages))
        return {"items": items[(page - 1) * page_size:page * page_size], "total": len(items), "page": page, "page_size": page_size,
                "pending_count": sum(item["is_todo"] for item in items)}

    def update(self, batch_id, payload, owner, allowed, admin=False):
        current = self.get(batch_id)
        if not admin and current["owner_id"] != owner and not set(current.get("scopes", [])) & set(allowed):
            raise CabinetError("无权查看该批次", 403)
        flag_only = bool(payload.get("rows")) and not payload.get("common") and not payload.get("acknowledge_warnings") and all(
            isinstance(patch, dict) and set(patch) == {"row_id", "exclude_notice_summary"}
            for patch in payload.get("rows", [])
        )
        if current["status"] == "cancelled" and not flag_only: raise CabinetError("已作废批次不能再修改",409)
        try:
            expected = int(payload.get("version"))
        except (TypeError, ValueError):
            raise CabinetError("缺少有效批次版本")
        patches = payload.get("rows", [])
        common = payload.get("common") or {}
        selected = set(str(value) for value in payload.get("row_ids", []))
        acknowledge_warnings = payload.get("acknowledge_warnings") is True
        if not isinstance(patches, list) or not isinstance(common, dict):
            raise CabinetError("批次修改格式无效")
        if acknowledge_warnings and not admin and not set(current.get("scopes", [])) <= set(allowed):
            raise CabinetError("核对整批异常需要拥有批次内全部楼栋权限", 403)
        notice_snapshots = {}

        def apply(batch):
            if batch.pop("validation_error",False): batch["error"]=""
            if acknowledge_warnings:
                batch["_warning_acknowledger"] = owner
            rows = {row["row_id"]: row for row in batch.get("rows", [])}
            requested = [(rows.get(str(patch.get("row_id"))), patch) for patch in patches if isinstance(patch, dict)]
            if common:
                requested.extend((row, common) for row in rows.values() if not selected or row["row_id"] in selected)
            for row, patch in requested:
                if row is None:
                    raise CabinetError("批次行不存在", 404)
                if not admin and row.get("scope") not in allowed:
                    raise CabinetError("无权修改该楼栋记录", 403)
                if "exclude_notice_summary" in patch:
                    if batch.get("source") != "notice" or (batch.get("source_notice") or {}).get("deleted_at"):
                        raise CabinetError("仅有效的通告待办可修改汇总标记", 409)
                    if type(patch["exclude_notice_summary"]) is not bool:
                        raise CabinetError("汇总标记必须为布尔值", 400)
                    before = bool(row.get("exclude_notice_summary"))
                    after = patch["exclude_notice_summary"]
                    if before != after:
                        row["exclude_notice_summary"] = after
                        row.setdefault("edits", []).append({"field": "exclude_notice_summary",
                            "before": before, "after": after, "owner": owner, "at": now()})
                    if set(patch) == {"row_id", "exclude_notice_summary"}:
                        continue
                if row.get("status") in LOCKED_ROW_STATUSES or row.get("operation_started") and row.get("status") != "rolled_back":
                    raise CabinetError("该行已开始正式提交，不能再修改内容", 409)
                before_scope = row.get("scope")
                action_was_edited = any(
                    edit.get("field") == "action" for edit in row.get("edits", [])
                )
                changed_fields = set()
                for field in EDITABLE_FIELDS:
                    if field not in patch:
                        continue
                    value = str(patch.get(field) or "").strip()
                    if value != str(row.get(field) or ""):
                        row.setdefault("edits", []).append({"field": field, "before": row.get(field, ""), "after": value, "owner": owner, "at": now()})
                        row[field] = value
                        changed_fields.add(field)
                if not admin and row.get("scope", "").upper().replace("楼", "") not in allowed:
                    row["scope"] = before_scope
                    raise CabinetError("无权将记录调整到该楼栋", 403)
                if batch.get("source") == "notice" and changed_fields & {"scope", "room", "rack"} and row.get("scope") in SCOPES:
                    scope = row["scope"]
                    snapshot = notice_snapshots.get(scope)
                    if snapshot is None:
                        snapshot = self.cabinet._snapshot(scope)
                        notice_snapshots[scope] = snapshot
                    notice = batch.get("source_notice") or {}
                    direction = "up" if notice.get("notice_type") == "上电通告" else "down"
                    cutoff = self._notice_datetime(notice.get("sent_at")) or self._notice_datetime(notice.get("start_time"))
                    action, inference, state = self._infer_notice_action(
                        snapshot, row.get("room"), row.get("rack"), direction, cutoff
                    )
                    row.update(current_power_state=state, inference=inference)
                    if "action" not in changed_fields and not action_was_edited:
                        row["action"] = action
                if "excluded" in patch:
                    excluded = bool(patch["excluded"])
                    was_excluded = str(row.get("status", "")).startswith("excluded_")
                    row["status"] = "excluded_manual" if excluded else "ready"
                    if was_excluded != excluded:
                        row.setdefault("edits", []).append({"field": "excluded", "before": was_excluded, "after": excluded, "owner": owner, "at": now()})
        updated = self._change(batch_id, apply, expected_version=expected, validate=True, partial_rows=True)
        if payload.get("response_mode") != "delta":
            return updated
        before = {row["row_id"]: row for row in current.get("rows", [])}
        changed = [row for row in updated.get("rows", []) if before.get(row["row_id"]) != row]
        delta = {key: copy.deepcopy(value) for key, value in updated.items()
                 if key not in {"rows", "images", "files"}}
        delta.update(rows=changed, images=[], files=[], partial_rows=True)
        if not admin and updated["owner_id"] != owner:
            delta["_visible_stats"] = self._stats(
                [row for row in updated.get("rows", []) if row.get("scope") in allowed]
            )
        return delta

    def clear_overlaps(self, batch_id, expected_version, owner, allowed, admin=False):
        current = self.get(batch_id)
        if not admin and current["owner_id"] != owner and not set(current.get("scopes", [])) & set(allowed):
            raise CabinetError("无权查看该批次", 403)
        if current["status"] == "cancelled": raise CabinetError("已作废批次不能再修改",409)
        def clear(batch):
            for row in batch.get("rows", []):
                if row.get("status") != "duplicate":
                    continue
                if not admin and row.get("scope") not in allowed:
                    continue
                row["status"] = "excluded_duplicate"
                row.setdefault("edits", []).append({"field": "excluded", "before": False, "after": True, "owner": owner, "at": now(), "reason": "overlap"})
        return self._change(batch_id, clear, expected_version=int(expected_version), validate=True)

    def _row_payload(self, batch, row):
        scope = row["scope"]
        if row.get("attempts") and self.store.later_completed(batch["batch_id"],scope,row["room"],row["rack"]):
            raise CabinetError("该机柜已有后续批次操作，不能再次确认",409)
        snap = self.cabinet._snapshot(scope)
        inventory = next((item for item in snap["config"]["inventory"] if (item["room"], item["rack"]) == (row["room"], row["rack"])),None)
        if inventory is None: raise CabinetError("机柜目录已变化，请刷新批次后核对",409)
        rack_type = inventory.get("rack_type", "") if row.get("type_resolution") == "keep_current" else row["rack_type"]
        image_refs = []
        for image_id in row.get("evidence_images", []):
            image = next((item for item in batch.get("images", []) if item["image_id"] == image_id), None)
            if image is None or image.get("deleted_at"):
                raise CabinetError("待办截图引用不存在", 409)
            token = str(image.get("cloud_file_token") or "")
            if not token:
                path = self.root / "evidence" / (image_id + image["extension"])
                if not path.is_file():
                    raise CabinetError("待办截图文件已丢失，不能确认", 409)
                remote = self.cabinet.remote_for(scope)
                remote.ensure_fields()
                token = remote.upload_attachment(path, image["name"])
                def remember(current):
                    target = next(item for item in current.get("images", []) if item["image_id"] == image_id)
                    target["cloud_file_token"] = token
                self._change(batch["batch_id"], remember)
                image["cloud_file_token"] = token
            image_refs.append({"image_id": image_id, "file_token": token, "extension": image["extension"]})
        evidence = {
            "batch_id": batch["batch_id"], "row_id": row["row_id"], "file_name": row.get("file_name", ""),
            "file_sha256": row.get("file_sha256", ""), "application_ids": row.get("application_ids", []),
            "source_page": row.get("page", 0), "source_row": row.get("source_row", 0),
            "source": batch.get("source", ""), "source_notice": batch.get("source_notice", {}),
            "inference": row.get("inference", ""), "original": row.get("original", {}),
            "edits": row.get("edits", []),
        }
        group_key = [batch["batch_id"], row["row_id"]]
        if row.get("attempts"):
            group_key.append(row["operation_id"])
        group = {"id": "event_" + digest(group_key)[:24], "action": row["action"],
                 "expected": row["expected"], "actual": row["actual"], "result": row["result"],
                 "failure_reason": row.get("failure_reason", "") if row["result"] == "失败" else "",
                 "evidence_images": image_refs}
        payload = {"room": row["room"], "rack": row["rack"], "rack_type": rack_type,
                   "result": row["result"], "groups": [group], "operation_id": row["operation_id"],
                   "category": "down" if row["action"].startswith("下") else "up", "batch_meta": evidence}
        exact = None
        for operation in snap["operations"]:
            if (operation["room"], operation["rack"]) != (row["room"], row["rack"]):
                continue
            if any((event["action"], event["actual"]) == (row["action"], row["actual"]) for event in operation["events"]):
                exact = operation
                break
        if exact:
            return None, exact["record_id"]
        if batch.get("source") in ("pdf", "image"):
            state = self._current_state(snap, row["room"], row["rack"])
            if row["action"] not in POWER_ACTIONS_BY_STATE.get(state, ()):
                raise CabinetError(f"该机柜当前为{POWER_STATE_LABELS.get(state, '状态待核实')}，与确认单操作不匹配，请重新核对",409)
        current = max(
            ((event, operation) for operation in snap["operations"]
             if (operation["room"], operation["rack"]) == (row["room"], row["rack"])
             for event in operation["events"] if completed_state_event(event)),
            key=lambda item: (item[0]["actual"], item[0]["id"]),
            default=None,
        )
        if scope in ("A", "B", "C") and current and not row["action"].startswith("上"):
            old = current[1]
            history = [copy.deepcopy(item) for item in old["groups"] if any(item.get(key) for key in ("action", "actual", "expected"))]
            payload["groups"] = [*history, group]
            if not row["action"].startswith("下"):
                payload["expected_version"] = old["version"]
                payload["source"] = old["source"]
                return (payload, old["record_id"]), ""
        if scope in ("D", "E"):
            old = next((operation for operation in snap["operations"] if (operation["room"], operation["rack"]) == (row["room"], row["rack"])), None)
            if old:
                payload["groups"] = copy.deepcopy(old["groups"]) + [group]
                payload["expected_version"] = old["version"]
                payload["source"] = old["source"]
                return (payload, old["record_id"]), ""
        return (payload, ""), ""

    def _confirm_scope(self, batch_id, scope, row_ids, owner):
        with self._scope_locks[scope]:
            rows={row["row_id"]:row for row in self.get(batch_id)["rows"] if row["row_id"] in row_ids}
            waves=[]; wave=[]; racks=set()
            for row_id in row_ids:
                row=rows[row_id]; rack=(row["room"],row["rack"])
                if rack in racks or len(wave)==100:
                    waves.append(wave); wave=[]; racks=set()
                wave.append(row_id); racks.add(rack)
            if wave: waves.append(wave)
            for wave in waves:
                pending=wave
                if len(wave)>1:
                    try:
                        batch=self.get(batch_id)
                        prepared=[]
                        for row_id in wave:
                            row=next(item for item in batch["rows"] if item["row_id"]==row_id)
                            request,existing_id=self._row_payload(batch,row)
                            if request is None: raise CabinetError("本组包含既有记录，逐条核验")
                            payload,record_id=request
                            prepared.append((row_id,payload,record_id))
                        def writing(current):
                            for row in current["rows"]:
                                if row["row_id"] in wave:
                                    row.update(status="writing",error="",operation_started=True)
                        self._change(batch_id,writing)
                        saved=self.cabinet.save_batch_operations(scope,prepared,owner)
                        completed_at = now()
                        def completed(current):
                            for row in current["rows"]:
                                if row["row_id"] in saved:
                                    row.update(status="completed", error="", record_id=saved[row["row_id"]],
                                               completed_at=completed_at, wrote_record=True)
                        self._change(batch_id,completed)
                        pending=[]
                    except Exception:
                        def retry(current):
                            for row in current["rows"]:
                                if row["row_id"] in wave and row.get("status")=="writing":
                                    row["status"]="queued"
                        self._change(batch_id,retry)
                for row_id in pending:
                    try:
                        def writing(batch):
                            row = next(item for item in batch["rows"] if item["row_id"] == row_id)
                            if row.get("status") not in ("queued", "failed"):
                                raise CabinetError("该行当前不可提交", 409)
                            row.update(status="writing", error="", operation_started=True)
                        batch = self._change(batch_id, writing)
                        row = next(item for item in batch["rows"] if item["row_id"] == row_id)
                        request, existing_record_id = self._row_payload(batch, row)
                        if request is None:
                            saved_record_id = existing_record_id
                        else:
                            payload, record_id = request
                            saved = self.cabinet.save_operation(scope, payload, owner, record_id)
                            saved_record_id = saved["record_id"]
                        def completed(current):
                            target = next(item for item in current["rows"] if item["row_id"] == row_id)
                            target.update(status="completed", error="", record_id=saved_record_id, completed_at=now(),wrote_record=request is not None)
                        self._change(batch_id, completed)
                    except Exception as exc:
                        def failed(current):
                            target = next((item for item in current.get("rows", []) if item["row_id"] == row_id), None)
                            if target and target.get("status") != "completed":
                                target.update(status="failed", error=str(exc), operation_started=True)
                        try:
                            self._change(batch_id, failed)
                        except Exception:
                            pass
        self._resume_notice_rollback_after_confirm(batch_id)

    def confirm(self, batch_id, payload, owner, allowed, admin=False):
        expected = payload.get("version")
        batch = self.get(batch_id)
        if (batch.get("source_notice") or {}).get("deleted_at"):
            raise CabinetError("来源通告已删除，不能继续确认机柜", 409)
        if expected is None: raise CabinetError("缺少有效批次版本")
        if not admin and batch["owner_id"] != owner and not set(batch.get("scopes", [])) & set(allowed):
            raise CabinetError("无权查看该批次", 403)
        if expected is not None and int(expected) != int(batch["version"]):
            raise CabinetError("批次已更新，请重新载入", 409)
        if batch.get("blocking_warnings") and not batch.get("warnings_acknowledged"):
            raise CabinetError("请先核对并确认通告数量或目录异常", 409)
        row_ids = set(str(value) for value in payload.get("row_ids", []))
        scope = str(payload.get("scope") or "").upper().replace("楼", "")
        whole = bool(payload.get("all"))
        if whole and not admin and not set(batch.get("scopes", [])) <= set(allowed):
            raise CabinetError("整批确认需要拥有批次内全部楼栋权限", 403)
        if scope and scope not in allowed and not admin:
            raise CabinetError("无权确认该楼栋", 403)
        if not admin and any(row["row_id"] in row_ids and row.get("scope") not in allowed for row in batch.get("rows", [])):
            raise CabinetError("选中记录包含无权操作的楼栋",403)
        batch = self._change(batch_id, lambda _current: None, expected_version=int(expected), validate=True)
        selected = []
        for row in batch.get("rows", []):
            wanted = whole or bool(scope and row.get("scope") == scope) or bool(row_ids and row["row_id"] in row_ids)
            if not wanted or row.get("notice_removed") or row.get("status") not in ACTIVE_ROW_STATUSES or row.get("issues"):
                continue
            if not admin and row.get("scope") not in allowed:
                raise CabinetError("选中记录包含无权操作的楼栋", 403)
            selected.append(row)
        if not selected:
            raise CabinetError("没有可确认的有效记录", 409)

        selected_ids = {row["row_id"] for row in selected}
        def queue(current):
            for row in current["rows"]:
                if row["row_id"] in selected_ids and row.get("status") in ACTIVE_ROW_STATUSES:
                    if row["status"] == "rolled_back":
                        attempts = row.setdefault("attempts", [])
                        attempts.append({key: row.get(key) for key in (
                            "operation_id", "record_id", "completed_at", "rolled_back_at", "wrote_record"
                        )})
                        row["operation_id"] = "batch_" + digest([batch_id, row["row_id"], len(attempts) + 1])[:32]
                        row.update(record_id="", wrote_record=False, operation_started=False)
                    row.update(status="queued", error="")
        queued = self._change(batch_id, queue, expected_version=batch["version"])
        grouped = {}
        for row in queued["rows"]:
            if row["row_id"] in selected_ids:
                grouped.setdefault(row["scope"], []).append(row)
        for building, rows in grouped.items():
            ordered = [row["row_id"] for row in sorted(rows, key=lambda item: (item.get("actual", ""), item.get("source_index", 0)))]
            self._confirm_pool.submit(self._confirm_scope, batch_id, building, ordered, owner)
        return queued

    def _rollback_scope(self,batch_id,scope,row_ids):
        with self._scope_locks[scope]:
            for row_id in row_ids:
                try:
                    def start(batch):
                        row=next(item for item in batch["rows"] if item["row_id"]==row_id)
                        if row.get("status")!="rollback_queued": raise CabinetError("该行当前不可回退",409)
                        row.update(status="rolling_back",error="")
                    batch=self._change(batch_id,start)
                    row=next(item for item in batch["rows"] if item["row_id"]==row_id)
                    newer_in_batch=any(
                        other["row_id"]!=row_id and other.get("wrote_record") is not False and other.get("status") in {"completed","rollback_queued","rolling_back","rollback_failed","rollback_blocked"}
                        and (other.get("scope"),other.get("room"),other.get("rack"))==(scope,row["room"],row["rack"])
                        and (other.get("actual", ""),other.get("source_index",0))>(row.get("actual", ""),row.get("source_index",0))
                        for other in batch["rows"]
                    )
                    if newer_in_batch or self.store.later_completed(batch_id,scope,row["room"],row["rack"]):
                        raise CabinetError("该机柜已有后续批次或本批后续操作，不能回退",409)
                    self.cabinet.rollback_batch_operation(scope,row["operation_id"],batch_id,row["record_id"])
                    def finished(current):
                        target=next(item for item in current["rows"] if item["row_id"]==row_id)
                        target.update(status="rolled_back",error="",rolled_back_at=now(),operation_started=False)
                    self._change(batch_id,finished)
                except Exception as exc:
                    def failed(current):
                        target=next((item for item in current["rows"] if item["row_id"]==row_id),None)
                        if target and target.get("status")!="rolled_back":
                            target.update(status="rollback_blocked" if isinstance(exc,CabinetError) and exc.status_code==409 else "rollback_failed",error=str(exc))
                    try: self._change(batch_id,failed)
                    except Exception: pass

    def rollback(self,batch_id,payload,owner,allowed,admin=False):
        batch=self.get(batch_id)
        if not admin and batch["owner_id"]!=owner and not set(batch.get("scopes",[])) & set(allowed): raise CabinetError("无权查看该批次",403)
        if int(payload.get("version",-1))!=batch["version"]: raise CabinetError("批次已更新，请重新载入",409)
        if any(row.get("status") in {"queued","writing","rollback_queued","rolling_back"} for row in batch["rows"]): raise CabinetError("批次仍在处理中，请完成后再回退",409)
        selected_ids={str(value) for value in payload.get("row_ids",[])}
        if payload.get("all") and not admin and not set(batch.get("scopes",[]))<=set(allowed): raise CabinetError("整批回退需要拥有全部楼栋权限",403)
        selected=[]
        for row in batch["rows"]:
            if not (payload.get("all") or row["row_id"] in selected_ids) or row.get("wrote_record") is False or row.get("status") not in {"completed","rollback_failed","rollback_blocked"}: continue
            if not admin and row.get("scope") not in allowed: raise CabinetError("选中记录包含无权操作的楼栋",403)
            selected.append(row)
        if not selected: raise CabinetError("没有可回退的已完成记录",409)
        ids={row["row_id"] for row in selected}
        def queue(current):
            for row in current["rows"]:
                if row["row_id"] in ids: row.update(status="rollback_queued",error="")
        queued=self._change(batch_id,queue,expected_version=batch["version"])
        grouped={}
        for row in selected: grouped.setdefault(row["scope"],[]).append(row)
        for scope,rows in grouped.items():
            ordered=[row["row_id"] for row in sorted(rows,key=lambda item:(item.get("actual",""),item.get("source_index",0)),reverse=True)]
            self._confirm_pool.submit(self._rollback_scope,batch_id,scope,ordered)
        return queued

    def cancel(self, batch_id, owner, admin=False, expected_version=None):
        batch = self.get(batch_id)
        if not admin and batch["owner_id"] != owner:
            raise CabinetError("仅上传者或管理员可作废批次", 403)
        def cancel_rows(current):
            if current["status"] == "cancelled":
                return
            if any(row.get("status") in ("queued", "writing", "rollback_queued", "rolling_back") for row in current.get("rows", [])):
                raise CabinetError("批次正在提交，暂不能作废", 409)
            if any(row.get("operation_started") and row.get("status") not in ("completed", "rolled_back") for row in current.get("rows", [])):
                raise CabinetError("存在结果未核验的上传记录，请先完成核验再作废", 409)
            if not any(row.get("status") not in ("completed", "rollback_failed", "rollback_blocked") for row in current.get("rows", [])) and not (
                current.get("status") == "recognizing" or current.get("source") == "image" and not current.get("rows")
            ):
                raise CabinetError("没有可作废的未提交行", 409)
            for row in current.get("rows", []):
                if row.get("status") not in ("completed", "rollback_failed", "rollback_blocked"):
                    row["cancelled_from_status"] = row.get("status", "")
                    row["status"] = "excluded_cancelled"
            for image in current.get("images", []):
                if image.get("status") == "recognizing":
                    image.update(status="cancelled", error="批次已作废，识别已停止")
            current["status"] = "cancelled"
        return self._change(batch_id, cancel_rows, expected_version=expected_version)

    def restore_rows(self, batch_id, payload, owner, allowed, admin=False):
        batch = self.get(batch_id)
        if not admin and batch["owner_id"] != owner and not set(batch.get("scopes", [])) & set(allowed):
            raise CabinetError("无权查看该批次", 403)
        row_ids = {str(value) for value in payload.get("row_ids", [])}
        if not row_ids:
            raise CabinetError("请选择要恢复的待办行", 400)
        try:
            version = int(payload.get("version"))
        except (TypeError, ValueError) as exc:
            raise CabinetError("缺少有效批次版本", 400) from exc

        def restore(current):
            rows = {row["row_id"]: row for row in current.get("rows", [])}
            if not row_ids <= rows.keys():
                raise CabinetError("选中的待办行不存在", 404)
            changed = 0
            for row_id in row_ids:
                row = rows[row_id]
                if not admin and row.get("scope") not in allowed:
                    raise CabinetError("选中记录包含无权操作的楼栋", 403)
                if row.get("status") not in {"excluded_manual", "excluded_duplicate", "excluded_cancelled"}:
                    continue
                if row.get("operation_started"):
                    raise CabinetError("该机柜存在未完成上传，不能直接恢复", 409)
                before = row["status"]
                prior = row.pop("cancelled_from_status", "")
                row["status"] = prior if prior in ("rolled_back", "excluded_image") else "ready"
                row.setdefault("edits", []).append({"field": "excluded", "before": before,
                                                    "after": False, "owner": owner, "at": now()})
                changed += 1
            if not changed:
                raise CabinetError("选中行中没有可恢复的记录", 409)
            if current["status"] == "cancelled":
                current["status"] = "pending"

        return self._change(batch_id, restore, expected_version=version, validate=True)

    def file_path(self, batch_id, file_id, owner, admin=False):
        batch = self.get(batch_id)
        if not admin and batch["owner_id"] != owner:
            raise CabinetError("无权下载原确认单", 403)
        meta = next((item for item in batch.get("files", []) if item["file_id"] == file_id), None)
        if meta is None:
            raise CabinetError("原确认单不存在", 404)
        path = self.import_root / batch_id / f"{file_id}.pdf"
        if not path.is_file():
            raise CabinetError("原确认单文件已清理", 410)
        return path, meta["name"]

    def cleanup_file(self,batch_id,file_id,owner,admin=False):
        batch=self.get(batch_id)
        if not admin and batch["owner_id"]!=owner: raise CabinetError("无权清理原确认单",403)
        if batch["status"] not in ("completed","cancelled"): raise CabinetError("批次完成或作废后才能清理原确认单",409)
        meta=next((item for item in batch.get("files",[]) if item["file_id"]==file_id),None)
        if meta is None: raise CabinetError("原确认单不存在",404)
        path=self.import_root/batch_id/f"{file_id}.pdf"
        if path.exists(): path.unlink()
        def cleaned(current):
            target=next(item for item in current["files"] if item["file_id"]==file_id)
            target["cleaned_at"]=now()
        return self._change(batch_id,cleaned)

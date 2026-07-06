# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import hashlib
import os
import queue
import re
import secrets
import sqlite3
import threading
import time
import uuid
from contextlib import closing
from pathlib import Path
from typing import Any

try:
    from upload_event_module.utils import get_data_file_path
except ModuleNotFoundError as exc:
    if exc.name != "upload_event_module":
        raise
    from ..upload_event_module.utils import get_data_file_path

try:
    from .migrations import run_schema_migrations, schema_health
    from .identity_utils import (
        canonical_source_record_id,
        canonical_target_record_id,
        is_local_record_id,
        normalize_notice_identity_payload,
    )
except ImportError:
    from lan_bitable_template_portal.migrations import (
        run_schema_migrations,
        schema_health,
    )
    from lan_bitable_template_portal.identity_utils import (
        canonical_source_record_id,
        canonical_target_record_id,
        is_local_record_id,
        normalize_notice_identity_payload,
    )


DEFAULT_STATE_DB_NAME = "lan_portal_state.sqlite3"
LEGACY_CHANGE_NOTICE_TYPE = "设备变更"
CANONICAL_CHANGE_NOTICE_TYPE = "变更通告"


class LanPortalStateStore:
    """SQLite-backed runtime state for the LAN portal.

    The database stores runtime data and user configuration. Legacy JSON files
    are migration inputs only and are never deleted or overwritten here.
    """

    SCHEMA_VERSION = 23
    SOURCE_SCOPE_TABLES = {
        "110": "source_records_110",
        "A": "source_records_a",
        "B": "source_records_b",
        "C": "source_records_c",
        "D": "source_records_d",
        "E": "source_records_e",
        "H": "source_records_h",
        "CAMPUS": "source_records_campus",
        "ALL": "source_records_all",
    }
    DOCUMENT_NAMESPACE_TABLES = {
        "notice_memory": "notice_memory",
        "notice_daily_summary": "daily_summary",
        "notice_work_status": "work_status",
        "notice_hidden_ongoing": "hidden_ongoing",
        "notice_action_job": "notice_actions",
        "qt_active_cache": "qt_active_cache",
        "qt_notice_history": "qt_notice_history",
        "runtime_state": "runtime_state",
    }
    REQUIRED_TABLES = [
        "meta",
        "settings",
        "auth_permissions",
        "permission_requests",
        "handover_links",
        "runtime_task_queue",
        "qt_active_items",
        "clipboard_candidates",
        "dialog_sessions",
        "repair_link_tasks",
        "source_snapshot_manifest",
        "source_snapshot_records",
        "event_month_snapshot_manifest",
        "event_month_snapshot_records",
        "notice_undo_actions",
        "notice_identity_map",
        "notice_work_type_overrides",
        "notice_upload_attachments",
        "mop_notice_bindings",
        "mop_fill_memory",
        "engineer_mop_local_files",
        "signature_link_tokens",
        "mop_temporary_signature_sessions",
        "mop_signature_usage_confirmations",
        "signature_crypto_migrations",
        "event_notice_operation_locks",
        "schema_migrations",
    ]
    REQUIRED_INDEXES = [
        "idx_runtime_task_queue_status",
        "idx_qt_active_items_record_id",
        "idx_source_snapshot_manifest_status_time",
        "idx_source_snapshot_records_scope_order",
        "idx_event_month_snapshot_manifest_month_status",
        "idx_event_month_snapshot_records_month_order",
        "idx_notice_undo_status_scope",
        "idx_notice_identity_active",
        "idx_notice_identity_source",
        "idx_notice_identity_target",
        "idx_notice_work_type_overrides_lookup",
        "idx_notice_upload_attachments_expiry",
        "idx_notice_upload_attachments_open",
        "idx_permission_requests_open_status",
        "idx_mop_notice_bindings_notice",
        "idx_mop_notice_bindings_template",
        "idx_mop_notice_bindings_mop",
        "idx_mop_fill_memory_updated",
        "idx_engineer_mop_local_notice",
        "idx_engineer_mop_local_status",
        "idx_signature_link_tokens_record",
        "idx_signature_link_tokens_expiry",
        "idx_mop_temp_signature_notice",
        "idx_mop_temp_signature_expiry",
        "idx_mop_signature_usage_notice",
        "idx_mop_signature_usage_token",
        "idx_signature_crypto_migrations_status",
        "idx_signature_crypto_migrations_updated",
        "idx_event_notice_operation_locks_expiry",
    ]

    def __init__(self, db_path: str | Path | None = None):
        self.db_path = Path(db_path or get_data_file_path(DEFAULT_STATE_DB_NAME))
        self._lock = threading.RLock()
        self._initialized = False
        self._wal_initialized = False
        self._write_queue: queue.Queue[dict[str, Any] | None] | None = None
        self._write_thread: threading.Thread | None = None
        self._write_worker_lock = threading.Lock()
        self._write_worker_started = False
        self._write_worker_stop = threading.Event()
        self._write_worker_stats: dict[str, Any] = {
            "enabled": False,
            "queued": 0,
            "written": 0,
            "failed": 0,
            "last_error": "",
            "last_flush_at": 0.0,
            "queue_size": 0,
        }

    def _connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.db_path), timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 5000")
        if not self._wal_initialized:
            conn.execute("PRAGMA journal_mode = WAL")
            self._wal_initialized = True
        conn.execute("PRAGMA synchronous = NORMAL")
        return conn

    @staticmethod
    def _write_worker_enabled() -> bool:
        return os.environ.get("CLIPFLOW_DISABLE_SQLITE_WRITE_WORKER") != "1"

    @staticmethod
    def _write_batch_window_seconds() -> float:
        raw = str(os.environ.get("CLIPFLOW_SQLITE_WRITE_BATCH_WINDOW_MS", "") or "").strip()
        try:
            value_ms = int(raw or "100")
        except Exception:
            value_ms = 100
        value_ms = max(0, min(value_ms, 1000))
        return value_ms / 1000.0

    def _ensure_write_worker(self) -> bool:
        if not self._write_worker_enabled():
            return False
        with self._write_worker_lock:
            if self._write_worker_started and self._write_thread and self._write_thread.is_alive():
                return True
            self._write_worker_stop.clear()
            self._write_queue = queue.Queue()
            self._write_thread = threading.Thread(
                target=self._run_write_worker,
                name="ClipFlowSQLiteWriteWorker",
                daemon=True,
            )
            self._write_worker_started = True
            self._write_worker_stats["enabled"] = True
            self._write_thread.start()
            return True

    def _submit_background_write(self, operation: str, payload: dict[str, Any]) -> bool:
        if not self._ensure_write_worker():
            return False
        write_queue = self._write_queue
        if write_queue is None:
            return False
        try:
            write_queue.put_nowait(
                {
                    "operation": str(operation or "").strip(),
                    "payload": dict(payload or {}),
                    "queued_at": time.time(),
                }
            )
            self._write_worker_stats["queued"] = int(
                self._write_worker_stats.get("queued") or 0
            ) + 1
            self._write_worker_stats["queue_size"] = write_queue.qsize()
            return True
        except Exception as exc:
            self._write_worker_stats["failed"] = int(
                self._write_worker_stats.get("failed") or 0
            ) + 1
            self._write_worker_stats["last_error"] = str(exc)
            return False

    def _run_write_worker(self) -> None:
        batch: list[dict[str, Any]] = []
        while not self._write_worker_stop.is_set():
            write_queue = self._write_queue
            if write_queue is None:
                break
            try:
                item = write_queue.get(timeout=0.2)
            except queue.Empty:
                item = None
            if item is None:
                if batch:
                    self._flush_background_writes(batch)
                    batch = []
                continue
            batch.append(item)
            deadline = time.monotonic() + self._write_batch_window_seconds()
            try:
                while len(batch) < 100:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    next_item = write_queue.get(timeout=remaining)
                    if next_item is not None:
                        batch.append(next_item)
                    else:
                        break
            except queue.Empty:
                pass
            self._flush_background_writes(batch)
            batch = []
        if batch:
            self._flush_background_writes(batch)
        self._write_worker_stats["queue_size"] = 0

    def _flush_background_writes(self, batch: list[dict[str, Any]]) -> None:
        if not batch:
            return
        now = time.time()
        effective_batch: list[dict[str, Any]] = []
        latest_backend_runtime: dict[str, dict[str, Any]] = {}
        for item in batch:
            operation = str(item.get("operation") or "").strip()
            payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
            if operation == "backend_runtime":
                key = self._text(payload.get("key"))
                if key:
                    latest_backend_runtime[key] = item
                    continue
            effective_batch.append(item)
        effective_batch.extend(latest_backend_runtime.values())
        try:
            with self._lock:
                with closing(self._connect()) as conn:
                    self._ensure_schema_locked(conn)
                    conn.execute("BEGIN IMMEDIATE")
                    for item in effective_batch:
                        operation = str(item.get("operation") or "").strip()
                        payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
                        if operation == "backend_runtime":
                            key = self._text(payload.get("key"))
                            if not key:
                                continue
                            value = payload.get("value")
                            normalized = dict(value) if isinstance(value, dict) else {}
                            conn.execute(
                                """
                                INSERT OR REPLACE INTO backend_runtime(key, value_json, updated_at)
                                VALUES (?, ?, ?)
                                """,
                                (key, self._json(normalized), now),
                            )
                        elif operation == "append_event":
                            source = self._text(payload.get("source")) or "unknown"
                            value = payload.get("value")
                            normalized = dict(value) if isinstance(value, dict) else {}
                            conn.execute(
                                """
                                INSERT INTO append_events(source, payload_json, created_at)
                                VALUES (?, ?, ?)
                                """,
                                (source, self._json(normalized), now),
                            )
                    conn.commit()
            self._write_worker_stats["written"] = int(
                self._write_worker_stats.get("written") or 0
            ) + len(batch)
            self._write_worker_stats["coalesced"] = int(
                self._write_worker_stats.get("coalesced") or 0
            ) + max(0, len(batch) - len(effective_batch))
            self._write_worker_stats["last_error"] = ""
            self._write_worker_stats["last_flush_at"] = now
            if self._write_queue is not None:
                self._write_worker_stats["queue_size"] = self._write_queue.qsize()
        except Exception as exc:
            self._write_worker_stats["failed"] = int(
                self._write_worker_stats.get("failed") or 0
            ) + len(batch)
            self._write_worker_stats["last_error"] = str(exc)
            if self._write_queue is not None:
                self._write_worker_stats["queue_size"] = self._write_queue.qsize()

    def shutdown_write_worker(self, *, timeout: float = 2.0) -> None:
        if not self._write_worker_started:
            return
        self._write_worker_stop.set()
        write_queue = self._write_queue
        if write_queue is not None:
            try:
                write_queue.put_nowait(None)
            except Exception:
                pass
        thread = self._write_thread
        if thread and thread.is_alive():
            thread.join(timeout=max(0.1, float(timeout or 0)))
        self._write_worker_stats["enabled"] = False

    def get_write_worker_stats(self) -> dict[str, Any]:
        stats = dict(self._write_worker_stats)
        queue_size = 0
        try:
            if self._write_queue is not None:
                queue_size = self._write_queue.qsize()
        except Exception:
            queue_size = 0
        stats["queue_size"] = queue_size
        stats["alive"] = bool(self._write_thread and self._write_thread.is_alive())
        stats["enabled"] = bool(stats.get("enabled")) and self._write_worker_enabled()
        return stats

    def acquire_event_notice_operation_lock(
        self,
        lock_key: str,
        *,
        owner: str = "",
        action: str = "",
        ttl_seconds: float = 180.0,
        payload: dict[str, Any] | None = None,
    ) -> tuple[bool, str]:
        lock_key = str(lock_key or "").strip()
        if not lock_key:
            return False, "缺少事件操作锁标识。"
        owner = str(owner or uuid.uuid4().hex).strip()
        now = time.time()
        lease_until = now + max(10.0, float(ttl_seconds or 180.0))
        payload_json = json.dumps(payload or {}, ensure_ascii=False)
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    """
                    SELECT owner, action, lease_until
                    FROM event_notice_operation_locks
                    WHERE lock_key=?
                    """,
                    (lock_key,),
                ).fetchone()
                if row and float(row["lease_until"] or 0) > now and row["owner"] != owner:
                    conn.rollback()
                    return (
                        False,
                        f"该事件正在处理，请稍后再试。当前动作：{row['action'] or '处理中'}",
                    )
                conn.execute(
                    """
                    INSERT OR REPLACE INTO event_notice_operation_locks(
                        lock_key, owner, action, payload_json, created_at, updated_at, lease_until
                    ) VALUES(
                        ?,
                        ?,
                        ?,
                        ?,
                        COALESCE((SELECT created_at FROM event_notice_operation_locks WHERE lock_key=?), ?),
                        ?,
                        ?
                    )
                    """,
                    (
                        lock_key,
                        owner,
                        str(action or ""),
                        payload_json,
                        lock_key,
                        now,
                        now,
                        lease_until,
                    ),
                )
                conn.commit()
        return True, owner

    def release_event_notice_operation_lock(self, lock_key: str, owner: str = "") -> bool:
        lock_key = str(lock_key or "").strip()
        if not lock_key:
            return False
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                if owner:
                    cursor = conn.execute(
                        "DELETE FROM event_notice_operation_locks WHERE lock_key=? AND owner=?",
                        (lock_key, str(owner or "").strip()),
                    )
                else:
                    cursor = conn.execute(
                        "DELETE FROM event_notice_operation_locks WHERE lock_key=?",
                        (lock_key,),
                    )
                conn.commit()
                return cursor.rowcount > 0

    def cleanup_event_notice_operation_locks(self) -> int:
        now = time.time()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                cursor = conn.execute(
                    "DELETE FROM event_notice_operation_locks WHERE lease_until<=?",
                    (now,),
                )
                conn.commit()
                return int(cursor.rowcount or 0)

    def _ensure_schema_locked(self, conn: sqlite3.Connection) -> None:
        if self._initialized:
            return
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ongoing_items (
                identity TEXT PRIMARY KEY,
                active_item_id TEXT,
                record_id TEXT,
                source_record_id TEXT,
                work_type TEXT,
                notice_type TEXT,
                title TEXT,
                building TEXT,
                building_codes_json TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                updated_at REAL NOT NULL,
                snapshot_id TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS json_documents (
                namespace TEXT NOT NULL,
                key TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY(namespace, key)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auth_permissions (
                open_id TEXT PRIMARY KEY,
                name TEXT,
                role TEXT,
                scopes_json TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                payload_json TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS permission_requests (
                request_id TEXT PRIMARY KEY,
                open_id TEXT NOT NULL,
                name TEXT,
                requested_scopes_json TEXT NOT NULL,
                reason TEXT,
                status TEXT NOT NULL,
                code_hash TEXT NOT NULL,
                code_salt TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 5,
                expires_at REAL NOT NULL,
                approved_at REAL,
                payload_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notice_upload_attachments (
                upload_id TEXT PRIMARY KEY,
                open_id TEXT,
                file_name TEXT,
                mime_type TEXT,
                size INTEGER NOT NULL,
                content BLOB NOT NULL,
                payload_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                used_at REAL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_notice_upload_attachments_expiry
            ON notice_upload_attachments(expires_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_notice_upload_attachments_open
            ON notice_upload_attachments(open_id, created_at)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mop_notice_bindings (
                binding_id TEXT PRIMARY KEY,
                notice_key TEXT NOT NULL,
                template_key TEXT,
                scope TEXT,
                notice_title TEXT,
                notice_status TEXT,
                source_record_id TEXT,
                target_record_id TEXT,
                active_item_id TEXT,
                mop_app_token TEXT,
                mop_table_id TEXT,
                mop_record_id TEXT NOT NULL,
                mop_title TEXT,
                mop_attachment_token TEXT,
                mop_attachment_name TEXT,
                selected_sheet TEXT,
                payload_json TEXT NOT NULL,
                updated_by TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                deleted_at REAL
            )
            """
        )
        self._ensure_column_locked(conn, "mop_notice_bindings", "template_key", "TEXT")
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_mop_notice_bindings_notice
            ON mop_notice_bindings(notice_key, scope, deleted_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_mop_notice_bindings_template
            ON mop_notice_bindings(template_key, scope, deleted_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_mop_notice_bindings_mop
            ON mop_notice_bindings(mop_record_id, mop_attachment_token, deleted_at)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mop_fill_memory (
                memory_key TEXT PRIMARY KEY,
                mop_title TEXT,
                mop_file_name TEXT,
                sheet_name TEXT,
                payload_json TEXT NOT NULL,
                updated_by TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_mop_fill_memory_updated
            ON mop_fill_memory(updated_at)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS engineer_mop_local_files (
                upload_id TEXT PRIMARY KEY,
                scope TEXT,
                source_record_id TEXT,
                notice_key TEXT,
                notice_title TEXT,
                original_file_name TEXT,
                local_file_path TEXT NOT NULL,
                file_size INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                detected_json TEXT NOT NULL,
                warnings_json TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_by_openid TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                deleted_at REAL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_engineer_mop_local_notice
            ON engineer_mop_local_files(source_record_id, notice_key, deleted_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_engineer_mop_local_status
            ON engineer_mop_local_files(status, updated_at, deleted_at)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signature_link_tokens (
                token_id TEXT PRIMARY KEY,
                record_id TEXT NOT NULL,
                token_hash TEXT NOT NULL UNIQUE,
                created_by TEXT,
                payload_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                used_at REAL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_signature_link_tokens_record
            ON signature_link_tokens(record_id, expires_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_signature_link_tokens_expiry
            ON signature_link_tokens(expires_at)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mop_temporary_signature_sessions (
                temp_id TEXT PRIMARY KEY,
                scope TEXT,
                notice_key TEXT,
                role TEXT NOT NULL,
                display_name TEXT NOT NULL,
                recipient_open_ids_json TEXT NOT NULL,
                status TEXT NOT NULL,
                token_hash TEXT NOT NULL UNIQUE,
                expires_at REAL NOT NULL,
                temporary_record_id TEXT,
                signature_file_token TEXT,
                created_by TEXT,
                payload_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_mop_temp_signature_notice
            ON mop_temporary_signature_sessions(scope, notice_key, role, status)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_mop_temp_signature_expiry
            ON mop_temporary_signature_sessions(expires_at)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mop_signature_usage_confirmations (
                confirmation_id TEXT PRIMARY KEY,
                scope TEXT,
                notice_key TEXT,
                role TEXT NOT NULL,
                signer_record_id TEXT NOT NULL,
                signer_open_id TEXT,
                signer_name TEXT,
                requested_by_openid TEXT,
                requested_by_name TEXT,
                status TEXT NOT NULL,
                token_hash TEXT NOT NULL UNIQUE,
                payload_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                confirmed_at REAL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_mop_signature_usage_notice
            ON mop_signature_usage_confirmations(scope, notice_key, signer_record_id, status)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_mop_signature_usage_token
            ON mop_signature_usage_confirmations(token_hash, expires_at)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signature_crypto_migrations (
                table_id TEXT NOT NULL,
                record_id TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                error TEXT,
                payload_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY(table_id, record_id)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_signature_crypto_migrations_status
            ON signature_crypto_migrations(status, updated_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_signature_crypto_migrations_updated
            ON signature_crypto_migrations(updated_at)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS event_notice_operation_locks (
                lock_key TEXT PRIMARY KEY,
                owner TEXT NOT NULL,
                action TEXT,
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                lease_until REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_event_notice_operation_locks_expiry
            ON event_notice_operation_locks(lease_until)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS handover_links (
                scope TEXT PRIMARY KEY,
                url TEXT,
                updated_by TEXT,
                payload_json TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        for table in sorted(set(self.DOCUMENT_NAMESPACE_TABLES.values())):
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    key TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{table}_updated_at ON {table}(updated_at)"
            )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS append_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_append_events_source_id ON append_events(source, id)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS backend_runtime (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS event_outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        self._ensure_column_locked(
            conn,
            "event_outbox",
            "attempts",
            "INTEGER NOT NULL DEFAULT 0",
        )
        self._ensure_column_locked(
            conn,
            "event_outbox",
            "last_error",
            "TEXT NOT NULL DEFAULT ''",
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_event_outbox_channel_status
            ON event_outbox(channel, status, id)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runtime_task_queue (
                queue_name TEXT NOT NULL,
                job_id TEXT NOT NULL,
                status TEXT NOT NULL,
                available_at REAL NOT NULL DEFAULT 0,
                lease_until REAL NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY(queue_name, job_id)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_runtime_task_queue_status
            ON runtime_task_queue(queue_name, status, available_at, updated_at)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS qt_active_items (
                active_item_id TEXT PRIMARY KEY,
                record_id TEXT,
                notice_type TEXT,
                section TEXT NOT NULL,
                sort_order INTEGER NOT NULL DEFAULT 0,
                origin TEXT,
                payload_json TEXT NOT NULL,
                updated_at REAL NOT NULL,
                deleted_at REAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS clipboard_candidates (
                candidate_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                content TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                source_event_id INTEGER,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_clipboard_candidates_status_updated
            ON clipboard_candidates(status, updated_at)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dialog_sessions (
                session_id TEXT PRIMARY KEY,
                session_type TEXT NOT NULL,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_dialog_sessions_status_updated
            ON dialog_sessions(status, updated_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_qt_active_items_record_id
            ON qt_active_items(record_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_qt_active_items_notice_section
            ON qt_active_items(notice_type, section)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_qt_active_items_deleted_at
            ON qt_active_items(deleted_at)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS repair_link_tasks (
                task_key TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                source_app_token TEXT NOT NULL,
                source_table_id TEXT NOT NULL,
                source_record_id TEXT NOT NULL,
                sync_app_token TEXT NOT NULL,
                sync_table_id TEXT NOT NULL,
                target_app_token TEXT NOT NULL,
                target_table_id TEXT NOT NULL,
                target_record_id TEXT NOT NULL,
                link_field_name TEXT NOT NULL,
                sync_record_id TEXT,
                due_at REAL NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 18,
                last_error TEXT,
                payload_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_repair_link_tasks_due
            ON repair_link_tasks(status, due_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_repair_link_tasks_source_target
            ON repair_link_tasks(source_record_id, target_record_id)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notice_undo_actions (
                undo_id TEXT PRIMARY KEY,
                identity_key TEXT NOT NULL,
                status TEXT NOT NULL,
                action_type TEXT NOT NULL,
                scope TEXT,
                work_type TEXT,
                notice_type TEXT,
                active_item_id TEXT,
                source_record_id TEXT,
                target_record_id TEXT,
                title TEXT,
                payload_json TEXT NOT NULL,
                error TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                applied_at REAL,
                expires_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_notice_undo_status_scope
            ON notice_undo_actions(status, scope, created_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_notice_undo_identity_status
            ON notice_undo_actions(identity_key, status, created_at)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notice_identity_map (
                identity_id TEXT PRIMARY KEY,
                work_type TEXT,
                notice_type TEXT,
                active_item_id TEXT,
                source_app_token TEXT,
                source_table_id TEXT,
                source_record_id TEXT,
                target_app_token TEXT,
                target_table_id TEXT,
                target_record_id TEXT,
                title TEXT,
                reason TEXT,
                building_codes_json TEXT NOT NULL DEFAULT '[]',
                start_time TEXT,
                end_time TEXT,
                status TEXT,
                origin TEXT,
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                deleted_at REAL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_notice_identity_active
            ON notice_identity_map(active_item_id, work_type, deleted_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_notice_identity_source
            ON notice_identity_map(source_record_id, work_type, deleted_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_notice_identity_target
            ON notice_identity_map(target_record_id, work_type, deleted_at)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notice_work_type_overrides (
                override_key TEXT PRIMARY KEY,
                source_work_type TEXT NOT NULL,
                target_work_type TEXT NOT NULL,
                normalized_title TEXT NOT NULL,
                title TEXT,
                payload_json TEXT NOT NULL DEFAULT '{}',
                enabled INTEGER NOT NULL DEFAULT 1,
                updated_by TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_notice_work_type_overrides_lookup
            ON notice_work_type_overrides(source_work_type, normalized_title, enabled)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS source_scope_snapshots (
                scope TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS source_snapshot_manifest (
                snapshot_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                started_at REAL NOT NULL,
                finished_at REAL,
                warnings_json TEXT NOT NULL,
                counts_json TEXT NOT NULL,
                meta_json TEXT NOT NULL,
                error TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_source_snapshot_manifest_status_time
            ON source_snapshot_manifest(status, updated_at)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS source_snapshot_records (
                snapshot_id TEXT NOT NULL,
                scope TEXT NOT NULL,
                record_key TEXT NOT NULL,
                record_kind TEXT NOT NULL,
                work_type TEXT,
                source_record_id TEXT,
                payload_json TEXT NOT NULL,
                sort_order INTEGER NOT NULL,
                created_at REAL NOT NULL,
                PRIMARY KEY(snapshot_id, scope, record_key)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_source_snapshot_records_scope_order
            ON source_snapshot_records(snapshot_id, scope, record_kind, sort_order)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_source_snapshot_records_work_source
            ON source_snapshot_records(snapshot_id, work_type, source_record_id)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS event_month_snapshot_manifest (
                snapshot_id TEXT PRIMARY KEY,
                month TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at REAL NOT NULL,
                finished_at REAL,
                counts_json TEXT NOT NULL,
                meta_json TEXT NOT NULL,
                error TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_event_month_snapshot_manifest_month_status
            ON event_month_snapshot_manifest(month, status, updated_at)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS event_month_snapshot_records (
                snapshot_id TEXT NOT NULL,
                month TEXT NOT NULL,
                source_key TEXT NOT NULL,
                source_record_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                sort_order INTEGER NOT NULL,
                created_at REAL NOT NULL,
                PRIMARY KEY(snapshot_id, source_key, source_record_id)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_event_month_snapshot_records_month_order
            ON event_month_snapshot_records(snapshot_id, month, sort_order)
            """
        )
        for table in self.SOURCE_SCOPE_TABLES.values():
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    record_key TEXT PRIMARY KEY,
                    record_kind TEXT NOT NULL,
                    work_type TEXT,
                    source_record_id TEXT,
                    payload_json TEXT NOT NULL,
                    sort_order INTEGER NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{table}_kind_order "
                f"ON {table}(record_kind, sort_order)"
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{table}_work_source "
                f"ON {table}(work_type, source_record_id)"
            )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_json_documents_updated_at "
            "ON json_documents(namespace, updated_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ongoing_work_type ON ongoing_items(work_type)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ongoing_active_item_id ON ongoing_items(active_item_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ongoing_record_id ON ongoing_items(record_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ongoing_source_record_id ON ongoing_items(source_record_id)"
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_permission_requests_open_status
            ON permission_requests(open_id, status)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_permission_requests_status_expiry
            ON permission_requests(status, expires_at)
            """
        )
        run_schema_migrations(conn, target_version=self.SCHEMA_VERSION)
        self._migrate_legacy_source_snapshot_locked(conn)
        self._migrate_legacy_change_notice_labels_locked(conn)
        self._repair_notice_identity_map_locked(conn)
        self._cleanup_invalid_notice_identity_targets_locked(conn)
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES('schema_version', ?)",
            (str(self.SCHEMA_VERSION),),
        )
        conn.commit()
        self._initialized = True

    @staticmethod
    def _ensure_column_locked(
        conn: sqlite3.Connection, table: str, column: str, ddl: str
    ) -> None:
        try:
            columns = {
                str(row[1] or "")
                for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
            }
            if column not in columns:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
        except sqlite3.OperationalError:
            pass

    def schema_health(self) -> dict[str, Any]:
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                return schema_health(
                    conn,
                    target_version=self.SCHEMA_VERSION,
                    required_tables=list(self.REQUIRED_TABLES),
                    required_indexes=list(self.REQUIRED_INDEXES),
                )

    def _migrate_legacy_source_snapshot_locked(self, conn: sqlite3.Connection) -> None:
        try:
            active_row = conn.execute(
                "SELECT value FROM meta WHERE key = 'active_source_snapshot_id'"
            ).fetchone()
            if active_row and str(active_row["value"] or "").strip():
                return
            legacy_meta_rows = conn.execute(
                "SELECT scope, payload_json, updated_at FROM source_scope_snapshots"
            ).fetchall()
        except sqlite3.OperationalError:
            return
        if not legacy_meta_rows:
            return
        snapshot_id = uuid.uuid4().hex
        now = time.time()
        started_at = min(
            [float(row["updated_at"] or now) for row in legacy_meta_rows] or [now]
        )
        counts: dict[str, dict[str, int]] = {}
        manifest_meta: dict[str, Any] = {}
        inserted = 0
        for meta_row in legacy_meta_rows:
            scope = self._normalize_source_scope(str(meta_row["scope"] or ""))
            table = self._source_scope_table(scope)
            meta_payload = self._loads(str(meta_row["payload_json"] or ""), {})
            if isinstance(meta_payload, dict) and not manifest_meta:
                manifest_meta = dict(meta_payload)
            rows = conn.execute(
                f"""
                SELECT record_key, record_kind, work_type, source_record_id,
                       payload_json, sort_order
                FROM {table}
                ORDER BY sort_order ASC, record_key ASC
                """
            ).fetchall()
            records_count = 0
            zhihang_count = 0
            for row in rows:
                record_kind = str(row["record_kind"] or "")
                if record_kind == "zhihang":
                    zhihang_count += 1
                else:
                    records_count += 1
                conn.execute(
                    """
                    INSERT OR REPLACE INTO source_snapshot_records(
                        snapshot_id,
                        scope,
                        record_key,
                        record_kind,
                        work_type,
                        source_record_id,
                        payload_json,
                        sort_order,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot_id,
                        scope,
                        str(row["record_key"] or ""),
                        record_kind,
                        str(row["work_type"] or ""),
                        str(row["source_record_id"] or ""),
                        str(row["payload_json"] or "{}"),
                        int(row["sort_order"] or 0),
                        now,
                    ),
                )
                inserted += 1
            counts[scope] = {
                "records": records_count,
                "zhihang_records": zhihang_count,
            }
        if not inserted:
            return
        conn.execute(
            """
            INSERT OR REPLACE INTO source_snapshot_manifest(
                snapshot_id,
                status,
                started_at,
                finished_at,
                warnings_json,
                counts_json,
                meta_json,
                error,
                created_at,
                updated_at
            ) VALUES (?, 'active', ?, ?, ?, ?, ?, '', ?, ?)
            """,
            (
                snapshot_id,
                started_at,
                now,
                self._json(manifest_meta.get("warnings") or []),
                self._json(counts),
                self._json(manifest_meta),
                now,
                now,
            ),
        )
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES('active_source_snapshot_id', ?)",
            (snapshot_id,),
        )
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES('active_source_snapshot_at', ?)",
            (str(now),),
        )

    @classmethod
    def _canonicalize_legacy_change_notice_text(cls, value: str) -> tuple[str, bool]:
        if value.strip() == LEGACY_CHANGE_NOTICE_TYPE:
            return CANONICAL_CHANGE_NOTICE_TYPE, True
        legacy_header = f"【{LEGACY_CHANGE_NOTICE_TYPE}】"
        if legacy_header in value:
            return (
                value.replace(legacy_header, f"【{CANONICAL_CHANGE_NOTICE_TYPE}】"),
                True,
            )
        return value, False

    @classmethod
    def _canonicalize_legacy_change_notice_payload(
        cls, value: Any
    ) -> tuple[Any, bool]:
        if isinstance(value, dict):
            changed = False
            result: dict[str, Any] = {}
            for key, item in value.items():
                normalized, item_changed = cls._canonicalize_legacy_change_notice_payload(
                    item
                )
                result[key] = normalized
                changed = changed or item_changed
            return result, changed
        if isinstance(value, list):
            changed = False
            result: list[Any] = []
            for item in value:
                normalized, item_changed = cls._canonicalize_legacy_change_notice_payload(
                    item
                )
                result.append(normalized)
                changed = changed or item_changed
            return result, changed
        if isinstance(value, str):
            return cls._canonicalize_legacy_change_notice_text(value)
        return value, False

    def _migrate_legacy_change_notice_labels_locked(
        self, conn: sqlite3.Connection
    ) -> None:
        """Canonicalize old local change-notice labels without accepting new old input.

        Previous versions persisted "设备变更" in SQLite/legacy JSON payloads.
        Runtime code now only understands "变更通告", so existing local rows must
        be normalized during startup. Business titles such as "网络设备变更" are not
        changed because only exact field values and bracketed notice headers are
        replaced.
        """

        def table_columns(table: str) -> set[str]:
            try:
                return {
                    str(row[1] or "")
                    for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
                }
            except sqlite3.OperationalError:
                return set()

        def update_json_table(
            table: str,
            *,
            payload_column: str = "payload_json",
            exact_columns: tuple[str, ...] = (),
            text_columns: tuple[str, ...] = (),
        ) -> int:
            columns = table_columns(table)
            if payload_column not in columns and not exact_columns and not text_columns:
                return 0
            selected_columns = ["rowid AS __rowid"]
            for column in (payload_column, *exact_columns, *text_columns):
                if column in columns:
                    selected_columns.append(column)
            if len(selected_columns) == 1:
                return 0
            try:
                rows = conn.execute(
                    f"SELECT {', '.join(selected_columns)} FROM {table}"
                ).fetchall()
            except sqlite3.OperationalError:
                return 0
            changed_rows = 0
            for row in rows:
                assignments: list[str] = []
                params: list[Any] = []
                if payload_column in columns:
                    payload = self._loads(str(row[payload_column] or ""), None)
                    normalized, changed = self._canonicalize_legacy_change_notice_payload(
                        payload
                    )
                    if changed:
                        assignments.append(f"{payload_column} = ?")
                        params.append(self._json(normalized))
                for column in exact_columns:
                    if column not in columns:
                        continue
                    raw = str(row[column] or "")
                    normalized, changed = self._canonicalize_legacy_change_notice_text(raw)
                    if changed:
                        assignments.append(f"{column} = ?")
                        params.append(normalized)
                for column in text_columns:
                    if column not in columns:
                        continue
                    raw = str(row[column] or "")
                    normalized, changed = self._canonicalize_legacy_change_notice_text(raw)
                    if changed:
                        assignments.append(f"{column} = ?")
                        params.append(normalized)
                if not assignments:
                    continue
                if "updated_at" in columns:
                    assignments.append("updated_at = ?")
                    params.append(time.time())
                params.append(row["__rowid"])
                conn.execute(
                    f"UPDATE {table} SET {', '.join(assignments)} WHERE rowid = ?",
                    tuple(params),
                )
                changed_rows += 1
            return changed_rows

        changed = 0
        changed += update_json_table(
            "qt_active_items",
            exact_columns=("notice_type",),
        )
        changed += update_json_table(
            "ongoing_items",
            exact_columns=("notice_type",),
        )
        changed += update_json_table(
            "notice_identity_map",
            exact_columns=("notice_type",),
        )
        changed += update_json_table(
            "notice_undo_actions",
            exact_columns=("notice_type",),
        )
        changed += update_json_table("runtime_task_queue")
        changed += update_json_table("event_outbox")
        changed += update_json_table("dialog_sessions")
        changed += update_json_table(
            "clipboard_candidates",
            text_columns=("content",),
        )
        for table in sorted(
            set(self.DOCUMENT_NAMESPACE_TABLES.values()) | {"json_documents"}
        ):
            changed += update_json_table(table)
        if changed:
            conn.execute(
                """
                INSERT OR REPLACE INTO meta(key, value)
                VALUES('legacy_change_notice_label_migration_v1', ?)
                """,
                (self._json({"changed_rows": changed, "at": time.time()}),),
            )

    def _notice_identity_candidate_payloads(self, payload: Any) -> list[dict[str, Any]]:
        if not isinstance(payload, dict):
            return []
        candidates: list[dict[str, Any]] = [payload]
        for key in (
            "request_payload",
            "prepared",
            "prepared_payload",
            "payload",
            "data",
            "data_dict",
            "result",
            "remote_result",
        ):
            value = payload.get(key)
            if isinstance(value, dict):
                candidates.append(value)
        return candidates

    def _repair_notice_identity_map_locked(self, conn: sqlite3.Connection) -> None:
        marker = conn.execute(
            "SELECT value FROM meta WHERE key = 'notice_identity_repair_v1_done'"
        ).fetchone()
        if marker:
            return
        repaired = 0

        def upsert_candidate(payload: dict[str, Any], *, origin: str) -> None:
            nonlocal repaired
            normalized = normalize_notice_identity_payload(payload)
            if not normalized:
                return
            if not (
                self._text(normalized.get("active_item_id"))
                or canonical_source_record_id(normalized)
                or canonical_target_record_id(normalized)
            ):
                return
            identity = self._upsert_notice_identity_locked(conn, normalized, origin=origin)
            if identity:
                repaired += 1

        try:
            rows = conn.execute(
                """
                SELECT active_item_id, record_id, origin, payload_json
                FROM qt_active_items
                ORDER BY updated_at DESC
                LIMIT 500
                """
            ).fetchall()
            for row in rows:
                payload = self._loads(str(row["payload_json"] or ""), {})
                if not isinstance(payload, dict):
                    continue
                payload.setdefault("active_item_id", str(row["active_item_id"] or ""))
                record_id = str(row["record_id"] or "").strip()
                if record_id and not is_local_record_id(record_id) and not payload.get("target_record_id"):
                    payload["target_record_id"] = str(row["record_id"] or "")
                upsert_candidate(payload, origin=str(row["origin"] or "qt_active_repair"))
        except Exception:
            pass

        try:
            rows = conn.execute(
                """
                SELECT active_item_id, record_id, source_record_id,
                       work_type, notice_type, payload_json
                FROM ongoing_items
                ORDER BY updated_at DESC
                LIMIT 500
                """
            ).fetchall()
            for row in rows:
                payload = self._loads(str(row["payload_json"] or ""), {})
                if not isinstance(payload, dict):
                    payload = {}
                payload.setdefault("active_item_id", str(row["active_item_id"] or ""))
                payload.setdefault("work_type", str(row["work_type"] or ""))
                payload.setdefault("notice_type", str(row["notice_type"] or ""))
                if row["source_record_id"] and not payload.get("source_record_id"):
                    payload["source_record_id"] = str(row["source_record_id"] or "")
                target_record_id = str(row["record_id"] or "")
                if is_local_record_id(target_record_id):
                    target_record_id = ""
                if target_record_id and not payload.get("target_record_id"):
                    payload["target_record_id"] = target_record_id
                upsert_candidate(payload, origin="ongoing_repair")
        except Exception:
            pass

        try:
            rows = conn.execute(
                """
                SELECT payload_json
                FROM notice_actions
                ORDER BY updated_at DESC
                LIMIT 500
                """
            ).fetchall()
            for row in rows:
                job = self._loads(str(row["payload_json"] or ""), {})
                for candidate in self._notice_identity_candidate_payloads(job):
                    upsert_candidate(candidate, origin="action_job_repair")
        except Exception:
            pass

        try:
            rows = conn.execute(
                """
                SELECT payload_json
                FROM work_status
                ORDER BY updated_at DESC
                LIMIT 500
                """
            ).fetchall()
            for row in rows:
                payload = self._loads(str(row["payload_json"] or ""), {})
                if isinstance(payload, list):
                    items = payload
                elif isinstance(payload, dict) and isinstance(payload.get("items"), list):
                    items = payload.get("items") or []
                else:
                    items = []
                for item in items:
                    if isinstance(item, dict):
                        upsert_candidate(item, origin="work_status_repair")
        except Exception:
            pass

        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES('notice_identity_repair_v1_done', ?)",
            (self._json({"repaired": repaired, "at": time.time()}),),
        )

    def _cleanup_invalid_notice_identity_targets_locked(self, conn: sqlite3.Connection) -> None:
        """Remove local/draft placeholders from target_record_id bindings."""

        marker_key = "notice_identity_strict_target_cleanup_v1_done"
        marker = conn.execute(
            "SELECT value FROM meta WHERE key = ?",
            (marker_key,),
        ).fetchone()
        if marker:
            self._delete_unbound_notice_identity_rows_locked(conn)
            return
        cleaned = 0
        try:
            rows = conn.execute(
                """
                SELECT identity_id, target_record_id, payload_json
                FROM notice_identity_map
                WHERE deleted_at IS NULL
                """
            ).fetchall()
            for row in rows:
                target_record_id = self._text(row["target_record_id"])
                if not is_local_record_id(target_record_id):
                    continue
                payload = self._loads(str(row["payload_json"] or ""), {})
                if not isinstance(payload, dict):
                    payload = {}
                payload.pop("target_record_id", None)
                if self._text(payload.get("record_id")) == target_record_id:
                    payload["record_id"] = self._text(payload.get("active_item_id"))
                conn.execute(
                    """
                    UPDATE notice_identity_map
                    SET target_record_id = '', payload_json = ?, updated_at = ?
                    WHERE identity_id = ?
                    """,
                    (self._json(payload), time.time(), row["identity_id"]),
                )
                cleaned += 1
        except Exception:
            pass
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)",
            (marker_key, self._json({"cleaned": cleaned, "at": time.time()})),
        )
        self._delete_unbound_notice_identity_rows_locked(conn)

    def _delete_unbound_notice_identity_rows_locked(self, conn: sqlite3.Connection) -> None:
        """Soft-delete legacy identity rows that cannot identify a source or target row."""

        marker_key = "notice_identity_unbound_cleanup_v1_done"
        marker = conn.execute(
            "SELECT value FROM meta WHERE key = ?",
            (marker_key,),
        ).fetchone()
        if marker:
            return
        now = time.time()
        deleted = 0
        try:
            cursor = conn.execute(
                """
                UPDATE notice_identity_map
                SET deleted_at = ?, updated_at = ?
                WHERE deleted_at IS NULL
                  AND COALESCE(TRIM(source_record_id), '') = ''
                  AND (
                        COALESCE(TRIM(target_record_id), '') = ''
                        OR target_record_id LIKE 'local_%'
                        OR target_record_id LIKE 'localid%'
                        OR target_record_id LIKE 'manual:%'
                        OR target_record_id LIKE 'draft:%'
                        OR target_record_id LIKE 'placeholder-%'
                  )
                """,
                (now, now),
            )
            deleted = int(cursor.rowcount or 0)
        except Exception:
            pass
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)",
            (marker_key, self._json({"deleted": deleted, "at": now})),
        )

    @staticmethod
    def _text(value: Any) -> str:
        return str(value or "").strip()

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def _stable_json(value: Any) -> str:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _loads(value: str, fallback: Any) -> Any:
        try:
            return json.loads(value)
        except Exception:
            return fallback

    @staticmethod
    def _event_created_at(payload: dict[str, Any], fallback: float) -> float:
        raw_ts = payload.get("ts") or payload.get("time") or payload.get("created_at")
        try:
            value = float(raw_ts)
        except Exception:
            return fallback
        if value > 10_000_000_000:
            return value / 1000.0
        if value > 0:
            return value
        return fallback

    @classmethod
    def _document_table_for_namespace(cls, namespace: str) -> str:
        return cls.DOCUMENT_NAMESPACE_TABLES.get(str(namespace or "").strip(), "")

    @classmethod
    def _normalize_source_scope(cls, scope: Any) -> str:
        text = str(scope or "ALL").strip().upper()
        if text in {"全部", "ALL", ""}:
            return "ALL"
        if text in {"园区", "CAMPUS", "PARK"}:
            return "CAMPUS"
        if "110" in text:
            return "110"
        return text[:1] if text[:1] in {"A", "B", "C", "D", "E", "H"} else "ALL"

    @classmethod
    def _source_scope_table(cls, scope: Any) -> str:
        return cls.SOURCE_SCOPE_TABLES[cls._normalize_source_scope(scope)]

    def _get_table_document_locked(
        self, conn: sqlite3.Connection, table: str, key: str
    ) -> dict[str, Any] | None:
        row = conn.execute(
            f"SELECT payload_json FROM {table} WHERE key = ?",
            (key,),
        ).fetchone()
        if not row:
            return None
        payload = self._loads(str(row["payload_json"] or ""), {})
        return payload if isinstance(payload, dict) else None

    def _put_table_document_locked(
        self, conn: sqlite3.Connection, table: str, key: str, payload: dict[str, Any]
    ) -> None:
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {table}(key, payload_json, updated_at)
            VALUES (?, ?, ?)
            """,
            (key, self._json(dict(payload or {})), time.time()),
        )

    def _delete_table_document_locked(
        self, conn: sqlite3.Connection, table: str, key: str
    ) -> None:
        conn.execute(f"DELETE FROM {table} WHERE key = ?", (key,))

    def _list_table_documents_locked(
        self, conn: sqlite3.Connection, table: str, key_prefix: str = ""
    ) -> list[dict[str, Any]]:
        if key_prefix:
            rows = conn.execute(
                f"""
                SELECT key, payload_json, updated_at
                FROM {table}
                WHERE key LIKE ?
                ORDER BY key ASC
                """,
                (f"{key_prefix}%",),
            ).fetchall()
        else:
            rows = conn.execute(
                f"""
                SELECT key, payload_json, updated_at
                FROM {table}
                ORDER BY key ASC
                """
            ).fetchall()
        documents: list[dict[str, Any]] = []
        for row in rows:
            payload = self._loads(str(row["payload_json"] or ""), {})
            if not isinstance(payload, dict):
                continue
            documents.append(
                {
                    "key": str(row["key"] or ""),
                    "payload": payload,
                    "updated_at": float(row["updated_at"] or 0),
                }
            )
        return documents

    def _get_legacy_json_document_locked(
        self, conn: sqlite3.Connection, namespace: str, key: str
    ) -> dict[str, Any] | None:
        row = conn.execute(
            """
            SELECT payload_json
            FROM json_documents
            WHERE namespace = ? AND key = ?
            """,
            (namespace, key),
        ).fetchone()
        if not row:
            return None
        payload = self._loads(str(row["payload_json"] or ""), {})
        return payload if isinstance(payload, dict) else None

    def _list_legacy_json_documents_locked(
        self, conn: sqlite3.Connection, namespace: str, key_prefix: str = ""
    ) -> list[dict[str, Any]]:
        if key_prefix:
            rows = conn.execute(
                """
                SELECT key, payload_json, updated_at
                FROM json_documents
                WHERE namespace = ? AND key LIKE ?
                ORDER BY key ASC
                """,
                (namespace, f"{key_prefix}%"),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT key, payload_json, updated_at
                FROM json_documents
                WHERE namespace = ?
                ORDER BY key ASC
                """,
                (namespace,),
            ).fetchall()
        documents: list[dict[str, Any]] = []
        for row in rows:
            payload = self._loads(str(row["payload_json"] or ""), {})
            if not isinstance(payload, dict):
                continue
            documents.append(
                {
                    "key": str(row["key"] or ""),
                    "payload": payload,
                    "updated_at": float(row["updated_at"] or 0),
                }
            )
        return documents

    def put_notice_upload_attachment(
        self,
        *,
        open_id: str = "",
        file_name: str = "",
        mime_type: str = "",
        content: bytes = b"",
        ttl_seconds: int = 86400,
        max_pending_bytes: int | None = None,
    ) -> dict[str, Any]:
        upload_id = uuid.uuid4().hex
        now = time.time()
        expires_at = now + max(60, int(ttl_seconds or 86400))
        content_bytes = bytes(content or b"")
        payload = {
            "upload_id": upload_id,
            "file_name": self._text(file_name) or "site_photo.png",
            "mime_type": self._text(mime_type) or "application/octet-stream",
            "size": len(content_bytes),
            "created_at": now,
            "expires_at": expires_at,
        }
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute("BEGIN IMMEDIATE")
                if max_pending_bytes is not None and int(max_pending_bytes or 0) > 0:
                    row = conn.execute(
                        """
                        SELECT COALESCE(SUM(size), 0) AS pending_bytes
                        FROM notice_upload_attachments
                        WHERE expires_at >= ? AND used_at IS NULL
                        """,
                        (now,),
                    ).fetchone()
                    pending_bytes = int((row or {})["pending_bytes"] or 0)
                    if pending_bytes + int(payload["size"]) > int(max_pending_bytes):
                        raise ValueError("现场照片暂存空间已满，请稍后重试或联系管理员清理。")
                conn.execute(
                    """
                    INSERT INTO notice_upload_attachments(
                        upload_id, open_id, file_name, mime_type, size,
                        content, payload_json, created_at, expires_at, used_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                    """,
                    (
                        upload_id,
                        self._text(open_id),
                        payload["file_name"],
                        payload["mime_type"],
                        int(payload["size"]),
                        content_bytes,
                        self._json(payload),
                        now,
                        expires_at,
                    ),
                )
                conn.commit()
        return payload

    def get_notice_upload_attachment(self, upload_id: str) -> dict[str, Any] | None:
        upload_id = self._text(upload_id)
        if not upload_id:
            return None
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(
                    """
                    SELECT upload_id, open_id, file_name, mime_type, size,
                           content, payload_json, created_at, expires_at, used_at
                    FROM notice_upload_attachments
                    WHERE upload_id = ?
                    """,
                    (upload_id,),
                ).fetchone()
                if not row:
                    return None
                if float(row["expires_at"] or 0) < time.time():
                    return None
                payload = self._loads(str(row["payload_json"] or ""), {})
                if not isinstance(payload, dict):
                    payload = {}
                payload.update(
                    {
                        "upload_id": str(row["upload_id"] or ""),
                        "open_id": str(row["open_id"] or ""),
                        "file_name": str(row["file_name"] or ""),
                        "mime_type": str(row["mime_type"] or ""),
                        "size": int(row["size"] or 0),
                        "content": bytes(row["content"] or b""),
                        "created_at": float(row["created_at"] or 0),
                        "expires_at": float(row["expires_at"] or 0),
                        "used_at": float(row["used_at"] or 0) if row["used_at"] else None,
                    }
                )
                return payload

    def mark_notice_upload_attachment_used(self, upload_id: str) -> bool:
        upload_id = self._text(upload_id)
        if not upload_id:
            return False
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                cursor = conn.execute(
                    "UPDATE notice_upload_attachments SET used_at = ? WHERE upload_id = ?",
                    (time.time(), upload_id),
                )
                conn.commit()
                return int(cursor.rowcount or 0) > 0

    def cleanup_notice_upload_attachments(
        self, *, now: float | None = None, used_grace_seconds: int = 3600
    ) -> int:
        cutoff = float(now if now is not None else time.time())
        used_cutoff = cutoff - max(60, int(used_grace_seconds or 3600))
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute("BEGIN IMMEDIATE")
                cursor = conn.execute(
                    """
                    DELETE FROM notice_upload_attachments
                    WHERE expires_at < ?
                       OR (used_at IS NOT NULL AND used_at < ?)
                    """,
                    (cutoff, used_cutoff),
                )
                deleted = int(cursor.rowcount or 0)
                conn.commit()
                return deleted

    def notice_upload_attachment_stats(self) -> dict[str, Any]:
        now = time.time()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(
                    """
                    SELECT COUNT(*) AS total,
                           COALESCE(SUM(size), 0) AS total_bytes,
                           SUM(CASE WHEN expires_at < ? THEN 1 ELSE 0 END) AS expired,
                           SUM(CASE WHEN used_at IS NOT NULL THEN 1 ELSE 0 END) AS used,
                           MIN(created_at) AS oldest_created_at,
                           MAX(created_at) AS newest_created_at
                    FROM notice_upload_attachments
                    """,
                    (now,),
                ).fetchone()
        total = int((row or {})["total"] or 0) if row else 0
        used = int((row or {})["used"] or 0) if row else 0
        expired = int((row or {})["expired"] or 0) if row else 0
        return {
            "total": total,
            "pending": max(0, total - used - expired),
            "used": used,
            "expired": expired,
            "total_bytes": int((row or {})["total_bytes"] or 0) if row else 0,
            "oldest_created_at": float((row or {})["oldest_created_at"] or 0) if row else 0.0,
            "newest_created_at": float((row or {})["newest_created_at"] or 0) if row else 0.0,
            "checked_at": now,
        }

    def _identity_for_item(self, item: dict[str, Any]) -> str:
        item = normalize_notice_identity_payload(item)
        work_type = self._text(item.get("work_type")) or "maintenance"
        target_record_id = canonical_target_record_id(item)
        if target_record_id:
            return f"{work_type}:target:{target_record_id}"
        source_record_id = canonical_source_record_id(item)
        if source_record_id:
            return f"{work_type}:source:{source_record_id}"
        active_item_id = self._text(item.get("active_item_id"))
        if active_item_id:
            return f"{work_type}:active:{active_item_id}"
        seed = self._json(
            [
                work_type,
                self._text(item.get("notice_type")),
                self._text(item.get("title")),
                self._text(item.get("building")),
                self._text(item.get("time")),
            ]
        )
        return f"generated:{uuid.uuid5(uuid.NAMESPACE_URL, seed).hex}"

    def _identity_keys_for_item(self, item: dict[str, Any]) -> set[str]:
        item = normalize_notice_identity_payload(item)
        work_type = self._text(item.get("work_type")) or "maintenance"
        keys: set[str] = set()
        target_record_id = canonical_target_record_id(item)
        source_record_id = canonical_source_record_id(item)
        active_item_id = self._text(item.get("active_item_id"))
        if target_record_id:
            keys.add(f"{work_type}:target:{target_record_id}")
        if source_record_id:
            keys.add(f"{work_type}:source:{source_record_id}")
        if active_item_id:
            keys.add(f"{work_type}:active:{active_item_id}")
        title = self._business_merge_text_key(
            item.get("title") or item.get("content") or item.get("name")
        )
        if title:
            seed = self._json(
                [
                    work_type,
                    self._text(item.get("notice_type")),
                    title,
                    self._business_merge_text_key(item.get("building")),
                    self._business_merge_text_key(item.get("maintenance_cycle")),
                    self._business_merge_time_key(item),
                    self._business_merge_text_key(item.get("location")),
                    self._business_merge_text_key(item.get("content")),
                    self._business_merge_text_key(item.get("reason")),
                    self._business_merge_text_key(item.get("impact")),
                ]
            )
            keys.add(f"{work_type}:exact:{hashlib.sha1(seed.encode('utf-8')).hexdigest()}")
        if not keys:
            keys.add(self._identity_for_item(item))
        return keys

    def _business_merge_keys_for_item(
        self, item: dict[str, Any], *, work_type: str = ""
    ) -> set[str]:
        item = normalize_notice_identity_payload(item or {})
        work_type = self._text(work_type or item.get("work_type")) or "maintenance"
        title_key = self._business_merge_text_key(
            item.get("title") or item.get("content") or item.get("name")
        )
        if not title_key:
            return set()
        building_key = self._business_merge_text_key(item.get("building"))
        reason_key = self._business_merge_text_key(item.get("reason"))
        time_key = self._business_merge_time_key(item)
        keys: set[str] = set()
        if building_key and reason_key:
            keys.add(f"{work_type}:business:title-building-reason:{building_key}:{title_key}:{reason_key}")
        if building_key and time_key and reason_key:
            keys.add(f"{work_type}:business:title-time-reason:{building_key}:{title_key}:{time_key}:{reason_key}")
        elif building_key and time_key:
            keys.add(f"{work_type}:business:title-time:{building_key}:{title_key}:{time_key}")
        return keys

    @staticmethod
    def _business_merge_text_key(value: Any) -> str:
        return re.sub(
            r"[\s,，;；:：。.【】（）()《》<>\"'“”‘’\-－_/\\]+",
            "",
            str(value or ""),
        ).strip().lower()

    @staticmethod
    def _business_merge_time_key(item: dict[str, Any]) -> str:
        parts = [
            str(item.get("start_time") or ""),
            str(item.get("time_str") or ""),
            str(item.get("time") or ""),
            str(item.get("end_time") or ""),
        ]
        digits = re.findall(r"\d+", "".join(parts))
        return "".join(chunk.zfill(2) if len(chunk) <= 2 else chunk for chunk in digits)

    @staticmethod
    def _notice_sections_from_text(text: Any) -> dict[str, str]:
        sections: dict[str, str] = {}
        current_key = ""
        buffer: list[str] = []
        for raw_line in str(text or "").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            match = re.match(r"^【([^】]+)】\s*(.*)$", line)
            if match:
                if current_key:
                    sections[current_key] = "\n".join(buffer).strip()
                current_key = match.group(1).strip()
                buffer = [match.group(2).strip()]
            elif current_key:
                buffer.append(line)
        if current_key:
            sections[current_key] = "\n".join(buffer).strip()
        return sections

    @staticmethod
    def _notice_section_first(
        sections: dict[str, str], names: tuple[str, ...], fallback: Any = ""
    ) -> str:
        for name in names:
            value = str(sections.get(name) or "").strip()
            if value:
                return value
        return str(fallback or "").strip()

    def _enrich_notice_payload_from_text(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload, _ = self._canonicalize_legacy_change_notice_payload(dict(payload or {}))
        payload = payload if isinstance(payload, dict) else {}
        sections = self._notice_sections_from_text(payload.get("text") or "")
        if not sections:
            return payload
        mapping = {
            "title": ("名称", "标题", "通告名称", "维修名称", "事件描述"),
            "time_str": ("时间", "计划时间", "维护时间"),
            "location": ("地点", "位置"),
            "content": ("内容",),
            "reason": ("原因", "故障原因", "故障维修原因"),
            "impact": ("影响", "影响范围"),
            "progress": ("进度", "完成情况"),
            "maintenance_cycle": ("维保周期", "维护周期"),
        }
        for field_name, labels in mapping.items():
            if payload.get(field_name) in (None, "", [], {}):
                value = self._notice_section_first(sections, labels)
                if value:
                    payload[field_name] = value
        if payload.get("start_time") in (None, "", [], {}) and payload.get("time_str"):
            parts = re.split(
                r"\s*(?:~|至|-|－|—)\s*",
                str(payload.get("time_str") or ""),
                maxsplit=1,
            )
            if parts:
                payload["start_time"] = parts[0].strip()
            if len(parts) > 1 and payload.get("end_time") in (None, "", [], {}):
                payload["end_time"] = parts[1].strip()
        return payload

    def _notice_payload_score(self, item: dict[str, Any]) -> int:
        item = normalize_notice_identity_payload(
            self._enrich_notice_payload_from_text(item or {})
        )
        score = 0
        if canonical_target_record_id(item):
            score += 12
        if canonical_source_record_id(item):
            score += 8
        if self._text(item.get("active_item_id")):
            score += 4
        for field in (
            "title",
            "building",
            "specialty",
            "maintenance_cycle",
            "start_time",
            "end_time",
            "location",
            "content",
            "reason",
            "impact",
            "progress",
            "text",
        ):
            if self._text(item.get(field)):
                score += 1
        if isinstance(item.get("extra_images"), list) and item.get("extra_images"):
            score += 1
        return score

    def _merge_notice_payload(self, existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
        existing = self._enrich_notice_payload_from_text(existing or {})
        incoming = self._enrich_notice_payload_from_text(incoming or {})
        primary, supplement = (
            (incoming, existing)
            if self._notice_payload_score(incoming) >= self._notice_payload_score(existing)
            else (existing, incoming)
        )
        merged = dict(primary)
        for key, value in supplement.items():
            current = merged.get(key)
            if current in (None, "", [], {}):
                merged[key] = value
        return normalize_notice_identity_payload(merged)

    def replace_ongoing_items(self, items: list[dict[str, Any]] | None) -> dict[str, Any]:
        now = time.time()
        snapshot_id = uuid.uuid4().hex
        normalized: list[dict[str, Any]] = []
        index_by_key: dict[str, int] = {}
        for item in (items or []):
            if not isinstance(item, dict):
                continue
            normalized_item = normalize_notice_identity_payload(dict(item))
            keys = self._identity_keys_for_item(normalized_item)
            match_index = None
            for key in keys:
                if key in index_by_key:
                    match_index = index_by_key[key]
                    break
            if match_index is None:
                match_index = len(normalized)
                normalized.append(normalized_item)
            else:
                normalized[match_index] = self._merge_notice_payload(
                    normalized[match_index],
                    normalized_item,
                )
            for key in keys | self._identity_keys_for_item(normalized[match_index]):
                index_by_key[key] = match_index
        comparable = [
            {"identity": self._identity_for_item(item), "payload": item}
            for item in normalized
        ]
        comparable.sort(key=lambda item: str(item.get("identity") or ""))
        snapshot_hash = hashlib.sha1(
            self._stable_json(comparable).encode("utf-8")
        ).hexdigest()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                existing_hash = conn.execute(
                    "SELECT value FROM meta WHERE key = 'ongoing_snapshot_hash'"
                ).fetchone()
                existing_count = conn.execute(
                    "SELECT value FROM meta WHERE key = 'ongoing_snapshot_count'"
                ).fetchone()
                if (
                    existing_hash
                    and str(existing_hash["value"] or "") == snapshot_hash
                    and existing_count
                    and str(existing_count["value"] or "") == str(len(normalized))
                ):
                    existing_at = conn.execute(
                        "SELECT value FROM meta WHERE key = 'ongoing_snapshot_at'"
                    ).fetchone()
                    try:
                        updated_at = float(existing_at["value"] if existing_at else 0)
                    except Exception:
                        updated_at = 0.0
                    return {
                        "snapshot_id": "",
                        "count": len(normalized),
                        "updated_at": updated_at,
                        "unchanged": True,
                    }
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("DELETE FROM ongoing_items")
                for item in normalized:
                    normalized_item = normalize_notice_identity_payload(item)
                    building_codes = item.get("building_codes")
                    if not isinstance(building_codes, list):
                        building_codes = []
                    normalized_item["building_codes"] = building_codes
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO ongoing_items(
                            identity,
                            active_item_id,
                            record_id,
                            source_record_id,
                            work_type,
                            notice_type,
                            title,
                            building,
                            building_codes_json,
                            payload_json,
                            updated_at,
                            snapshot_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            self._identity_for_item(item),
                            self._text(item.get("active_item_id")),
                            canonical_target_record_id(normalized_item),
                            canonical_source_record_id(normalized_item),
                            self._text(item.get("work_type")),
                            self._text(item.get("notice_type")),
                            self._text(item.get("title")),
                            self._text(item.get("building")),
                            self._json(building_codes),
                            self._json(item),
                            now,
                            snapshot_id,
                        ),
                    )
                    self._upsert_notice_identity_locked(
                        conn,
                        normalized_item,
                        origin=self._text(item.get("origin")) or "qt_snapshot",
                    )
                conn.execute(
                    "INSERT OR REPLACE INTO meta(key, value) VALUES('ongoing_snapshot_at', ?)",
                    (str(now),),
                )
                conn.execute(
                    "INSERT OR REPLACE INTO meta(key, value) VALUES('ongoing_snapshot_id', ?)",
                    (snapshot_id,),
                )
                conn.execute(
                    "INSERT OR REPLACE INTO meta(key, value) VALUES('ongoing_snapshot_count', ?)",
                    (str(len(normalized)),),
                )
                conn.execute(
                    "INSERT OR REPLACE INTO meta(key, value) VALUES('ongoing_snapshot_hash', ?)",
                    (snapshot_hash,),
                )
                conn.commit()
        return {"snapshot_id": snapshot_id, "count": len(normalized), "updated_at": now}

    def get_ongoing_snapshot(self) -> dict[str, Any]:
        if not self.db_path.exists():
            return {"exists": False, "items": [], "updated_at": 0.0, "count": 0}
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                meta_rows = conn.execute("SELECT key, value FROM meta").fetchall()
                meta = {str(row["key"]): str(row["value"]) for row in meta_rows}
                exists = "ongoing_snapshot_at" in meta
                rows = conn.execute(
                    "SELECT payload_json FROM ongoing_items ORDER BY updated_at DESC, identity ASC"
                ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            payload = self._loads(str(row["payload_json"] or ""), {})
            if isinstance(payload, dict):
                items.append(payload)
        try:
            updated_at = float(meta.get("ongoing_snapshot_at") or 0)
        except Exception:
            updated_at = 0.0
        return {
            "exists": bool(exists),
            "items": items,
            "updated_at": updated_at,
            "snapshot_id": meta.get("ongoing_snapshot_id", ""),
            "count": len(items),
            "path": os.fspath(self.db_path),
        }

    def get_ongoing_snapshot_meta(self) -> dict[str, Any]:
        if not self.db_path.exists():
            return {
                "exists": False,
                "updated_at": 0.0,
                "snapshot_id": "",
                "count": 0,
                "hash": "",
            }
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    """
                    SELECT key, value
                    FROM meta
                    WHERE key IN (
                        'ongoing_snapshot_at',
                        'ongoing_snapshot_id',
                        'ongoing_snapshot_count',
                        'ongoing_snapshot_hash'
                    )
                    """
                ).fetchall()
        meta = {str(row["key"]): str(row["value"]) for row in rows}
        try:
            updated_at = float(meta.get("ongoing_snapshot_at") or 0)
        except Exception:
            updated_at = 0.0
        try:
            count = int(float(meta.get("ongoing_snapshot_count") or 0))
        except Exception:
            count = 0
        return {
            "exists": "ongoing_snapshot_at" in meta,
            "updated_at": updated_at,
            "snapshot_id": meta.get("ongoing_snapshot_id", ""),
            "count": count,
            "hash": meta.get("ongoing_snapshot_hash", ""),
        }

    def replace_source_scope_snapshot(
        self,
        scope: str,
        *,
        records: list[dict[str, Any]] | None = None,
        zhihang_records: list[dict[str, Any]] | None = None,
        meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        scope = self._normalize_source_scope(scope)
        table = self._source_scope_table(scope)
        now = time.time()
        normalized_records = [
            dict(item) for item in (records or []) if isinstance(item, dict)
        ]
        normalized_zhihang = [
            dict(item) for item in (zhihang_records or []) if isinstance(item, dict)
        ]
        meta_payload = dict(meta or {})
        meta_payload.update(
            {
                "scope": scope,
                "records_count": len(normalized_records),
                "zhihang_records_count": len(normalized_zhihang),
                "updated_at": now,
            }
        )
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(f"DELETE FROM {table}")
                order_index = 0
                for kind, items in (
                    ("workbench", normalized_records),
                    ("zhihang", normalized_zhihang),
                ):
                    for item in items:
                        record_id = self._text(
                            item.get("record_id") or item.get("source_record_id")
                        )
                        work_type = self._text(item.get("work_type"))
                        record_key_seed = [
                            kind,
                            work_type,
                            record_id,
                            self._text(item.get("source_app_token")),
                            self._text(item.get("source_table_id")),
                        ]
                        record_key = hashlib.sha1(
                            self._stable_json(record_key_seed).encode("utf-8")
                        ).hexdigest()
                        conn.execute(
                            f"""
                            INSERT OR REPLACE INTO {table}(
                                record_key,
                                record_kind,
                                work_type,
                                source_record_id,
                                payload_json,
                                sort_order,
                                updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                record_key,
                                kind,
                                work_type,
                                record_id,
                                self._json(item),
                                order_index,
                                now,
                            ),
                        )
                        order_index += 1
                conn.execute(
                    """
                    INSERT OR REPLACE INTO source_scope_snapshots(
                        scope, payload_json, updated_at
                    ) VALUES (?, ?, ?)
                    """,
                    (scope, self._json(meta_payload), now),
                )
                conn.commit()
        return {
            "scope": scope,
            "records_count": len(normalized_records),
            "zhihang_records_count": len(normalized_zhihang),
            "updated_at": now,
        }

    def _source_snapshot_record_key(self, kind: str, item: dict[str, Any]) -> str:
        record_key_seed = [
            self._text(kind),
            self._text(item.get("work_type")),
            self._text(item.get("record_id") or item.get("source_record_id")),
            self._text(item.get("source_app_token")),
            self._text(item.get("source_table_id")),
        ]
        return hashlib.sha1(self._stable_json(record_key_seed).encode("utf-8")).hexdigest()

    def _write_legacy_source_scope_locked(
        self,
        conn: sqlite3.Connection,
        *,
        scope: str,
        records: list[dict[str, Any]],
        zhihang_records: list[dict[str, Any]],
        meta_payload: dict[str, Any],
        now: float,
    ) -> None:
        table = self._source_scope_table(scope)
        conn.execute(f"DELETE FROM {table}")
        order_index = 0
        for kind, items in (("workbench", records), ("zhihang", zhihang_records)):
            for item in items:
                record_id = self._text(item.get("record_id") or item.get("source_record_id"))
                work_type = self._text(item.get("work_type"))
                conn.execute(
                    f"""
                    INSERT OR REPLACE INTO {table}(
                        record_key,
                        record_kind,
                        work_type,
                        source_record_id,
                        payload_json,
                        sort_order,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self._source_snapshot_record_key(kind, item),
                        kind,
                        work_type,
                        record_id,
                        self._json(item),
                        order_index,
                        now,
                    ),
                )
                order_index += 1
        conn.execute(
            """
            INSERT OR REPLACE INTO source_scope_snapshots(
                scope, payload_json, updated_at
            ) VALUES (?, ?, ?)
            """,
            (scope, self._json(meta_payload), now),
        )

    def replace_all_source_scope_snapshots(
        self,
        snapshots_by_scope: dict[str, dict[str, Any]],
        *,
        meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        snapshot_id = uuid.uuid4().hex
        started_at = time.time()
        meta_payload = dict(meta or {})
        normalized: dict[str, dict[str, list[dict[str, Any]]]] = {}
        counts: dict[str, dict[str, int]] = {}
        for raw_scope, payload in (snapshots_by_scope or {}).items():
            scope = self._normalize_source_scope(raw_scope)
            payload = payload if isinstance(payload, dict) else {}
            records = [
                dict(item)
                for item in (payload.get("records") or [])
                if isinstance(item, dict)
            ]
            zhihang_records = [
                dict(item)
                for item in (payload.get("zhihang_records") or [])
                if isinstance(item, dict)
            ]
            normalized[scope] = {
                "records": records,
                "zhihang_records": zhihang_records,
            }
            counts[scope] = {
                "records": len(records),
                "zhihang_records": len(zhihang_records),
            }
        if not normalized:
            raise ValueError("source snapshot payload is empty")
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute("BEGIN IMMEDIATE")
                now = time.time()
                conn.execute(
                    """
                    INSERT INTO source_snapshot_manifest(
                        snapshot_id,
                        status,
                        started_at,
                        finished_at,
                        warnings_json,
                        counts_json,
                        meta_json,
                        error,
                        created_at,
                        updated_at
                    ) VALUES (?, 'building', ?, NULL, ?, ?, ?, '', ?, ?)
                    """,
                    (
                        snapshot_id,
                        started_at,
                        self._json(meta_payload.get("warnings") or []),
                        self._json(counts),
                        self._json(meta_payload),
                        now,
                        now,
                    ),
                )
                for scope in sorted(normalized):
                    records = normalized[scope]["records"]
                    zhihang_records = normalized[scope]["zhihang_records"]
                    scope_meta = dict(meta_payload)
                    scope_meta.update(
                        {
                            "scope": scope,
                            "snapshot_id": snapshot_id,
                            "records_count": len(records),
                            "zhihang_records_count": len(zhihang_records),
                            "updated_at": now,
                        }
                    )
                    order_index = 0
                    for kind, items in (("workbench", records), ("zhihang", zhihang_records)):
                        for item in items:
                            record_id = self._text(
                                item.get("record_id") or item.get("source_record_id")
                            )
                            conn.execute(
                                """
                                INSERT OR REPLACE INTO source_snapshot_records(
                                    snapshot_id,
                                    scope,
                                    record_key,
                                    record_kind,
                                    work_type,
                                    source_record_id,
                                    payload_json,
                                    sort_order,
                                    created_at
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    snapshot_id,
                                    scope,
                                    self._source_snapshot_record_key(kind, item),
                                    kind,
                                    self._text(item.get("work_type")),
                                    record_id,
                                    self._json(item),
                                    order_index,
                                    now,
                                ),
                            )
                            order_index += 1
                    self._write_legacy_source_scope_locked(
                        conn,
                        scope=scope,
                        records=records,
                        zhihang_records=zhihang_records,
                        meta_payload=scope_meta,
                        now=now,
                    )
                conn.execute(
                    "UPDATE source_snapshot_manifest SET status = 'retained', updated_at = ? WHERE status = 'active'",
                    (now,),
                )
                conn.execute(
                    """
                    UPDATE source_snapshot_manifest
                    SET status = 'active', finished_at = ?, updated_at = ?
                    WHERE snapshot_id = ?
                    """,
                    (now, now, snapshot_id),
                )
                conn.execute(
                    "INSERT OR REPLACE INTO meta(key, value) VALUES('active_source_snapshot_id', ?)",
                    (snapshot_id,),
                )
                conn.execute(
                    "INSERT OR REPLACE INTO meta(key, value) VALUES('active_source_snapshot_at', ?)",
                    (str(now),),
                )
                conn.commit()
        self.cleanup_source_snapshots()
        return {
            "snapshot_id": snapshot_id,
            "status": "active",
            "counts": counts,
            "updated_at": now,
        }

    def record_failed_source_snapshot(
        self, *, meta: dict[str, Any] | None = None, error: str = ""
    ) -> dict[str, Any]:
        snapshot_id = uuid.uuid4().hex
        now = time.time()
        meta_payload = dict(meta or {})
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute(
                    """
                    INSERT OR REPLACE INTO source_snapshot_manifest(
                        snapshot_id,
                        status,
                        started_at,
                        finished_at,
                        warnings_json,
                        counts_json,
                        meta_json,
                        error,
                        created_at,
                        updated_at
                    ) VALUES (?, 'failed', ?, ?, ?, '{}', ?, ?, ?, ?)
                    """,
                    (
                        snapshot_id,
                        now,
                        now,
                        self._json(meta_payload.get("warnings") or []),
                        self._json(meta_payload),
                        self._text(error),
                        now,
                        now,
                    ),
                )
                conn.commit()
        self.cleanup_source_snapshots()
        return {"snapshot_id": snapshot_id, "status": "failed", "updated_at": now}

    def get_source_scope_snapshot(self, scope: str) -> dict[str, Any]:
        scope = self._normalize_source_scope(scope)
        table = self._source_scope_table(scope)
        if not self.db_path.exists():
            return {
                "exists": False,
                "scope": scope,
                "records": [],
                "zhihang_records": [],
                "meta": {},
                "updated_at": 0.0,
            }
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                active_row = conn.execute(
                    "SELECT value FROM meta WHERE key = 'active_source_snapshot_id'"
                ).fetchone()
                active_snapshot_id = (
                    str(active_row["value"] or "").strip() if active_row else ""
                )
                if active_snapshot_id:
                    manifest_row = conn.execute(
                        """
                        SELECT *
                        FROM source_snapshot_manifest
                        WHERE snapshot_id = ? AND status = 'active'
                        """,
                        (active_snapshot_id,),
                    ).fetchone()
                    if manifest_row:
                        rows = conn.execute(
                            """
                            SELECT record_kind, payload_json
                            FROM source_snapshot_records
                            WHERE snapshot_id = ? AND scope = ?
                            ORDER BY sort_order ASC, record_key ASC
                            """,
                            (active_snapshot_id, scope),
                        ).fetchall()
                        meta = self._loads(str(manifest_row["meta_json"] or ""), {})
                        if not isinstance(meta, dict):
                            meta = {}
                        meta["snapshot_id"] = active_snapshot_id
                        try:
                            updated_at = float(manifest_row["finished_at"] or manifest_row["updated_at"] or 0)
                        except Exception:
                            updated_at = 0.0
                        records: list[dict[str, Any]] = []
                        zhihang_records: list[dict[str, Any]] = []
                        for row in rows:
                            payload = self._loads(str(row["payload_json"] or ""), {})
                            if not isinstance(payload, dict):
                                continue
                            if str(row["record_kind"] or "") == "zhihang":
                                zhihang_records.append(payload)
                            else:
                                records.append(payload)
                        return {
                            "exists": True,
                            "scope": scope,
                            "records": records,
                            "zhihang_records": zhihang_records,
                            "meta": meta,
                            "updated_at": updated_at,
                            "snapshot_id": active_snapshot_id,
                            "count": len(records),
                            "zhihang_count": len(zhihang_records),
                        }
                meta_row = conn.execute(
                    """
                    SELECT payload_json, updated_at
                    FROM source_scope_snapshots
                    WHERE scope = ?
                    """,
                    (scope,),
                ).fetchone()
                rows = conn.execute(
                    f"""
                    SELECT record_kind, payload_json
                    FROM {table}
                    ORDER BY sort_order ASC, record_key ASC
                    """
                ).fetchall()
        meta = {}
        updated_at = 0.0
        if meta_row:
            meta_payload = self._loads(str(meta_row["payload_json"] or ""), {})
            if isinstance(meta_payload, dict):
                meta = meta_payload
            try:
                updated_at = float(meta_row["updated_at"] or 0)
            except Exception:
                updated_at = 0.0
        records: list[dict[str, Any]] = []
        zhihang_records: list[dict[str, Any]] = []
        for row in rows:
            payload = self._loads(str(row["payload_json"] or ""), {})
            if not isinstance(payload, dict):
                continue
            if str(row["record_kind"] or "") == "zhihang":
                zhihang_records.append(payload)
            else:
                records.append(payload)
        return {
            "exists": bool(meta_row),
            "scope": scope,
            "records": records,
            "zhihang_records": zhihang_records,
            "meta": meta,
            "updated_at": updated_at,
            "count": len(records),
            "zhihang_count": len(zhihang_records),
        }

    def patch_active_source_record_fields(
        self,
        *,
        source_record_id: str,
        work_type: str = "",
        fields: dict[str, Any] | None = None,
    ) -> int:
        source_record_id = self._text(source_record_id)
        work_type = self._text(work_type)
        patch_fields = dict(fields or {})
        if not source_record_id or not patch_fields or not self.db_path.exists():
            return 0
        patched = 0
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute("BEGIN IMMEDIATE")
                active_row = conn.execute(
                    "SELECT value FROM meta WHERE key = 'active_source_snapshot_id'"
                ).fetchone()
                active_snapshot_id = (
                    str(active_row["value"] or "").strip() if active_row else ""
                )
                if active_snapshot_id:
                    rows = conn.execute(
                        """
                        SELECT snapshot_id, scope, record_key, payload_json
                        FROM source_snapshot_records
                        WHERE snapshot_id = ?
                          AND source_record_id = ?
                          AND (? = '' OR work_type = ?)
                        """,
                        (
                            active_snapshot_id,
                            source_record_id,
                            work_type,
                            work_type,
                        ),
                    ).fetchall()
                    for row in rows:
                        payload = self._loads(str(row["payload_json"] or ""), {})
                        if not isinstance(payload, dict):
                            continue
                        display_fields = payload.get("display_fields")
                        if not isinstance(display_fields, dict):
                            display_fields = {}
                        display_fields.update(patch_fields)
                        payload["display_fields"] = display_fields
                        conn.execute(
                            """
                            UPDATE source_snapshot_records
                            SET payload_json = ?
                            WHERE snapshot_id = ? AND scope = ? AND record_key = ?
                            """,
                            (
                                self._json(payload),
                                row["snapshot_id"],
                                row["scope"],
                                row["record_key"],
                            ),
                        )
                        patched += 1
                for _scope, table in SOURCE_SCOPE_TABLES.items():
                    rows = conn.execute(
                        f"""
                        SELECT record_key, payload_json
                        FROM {table}
                        WHERE source_record_id = ?
                          AND (? = '' OR work_type = ?)
                        """,
                        (source_record_id, work_type, work_type),
                    ).fetchall()
                    for row in rows:
                        payload = self._loads(str(row["payload_json"] or ""), {})
                        if not isinstance(payload, dict):
                            continue
                        display_fields = payload.get("display_fields")
                        if not isinstance(display_fields, dict):
                            display_fields = {}
                        display_fields.update(patch_fields)
                        payload["display_fields"] = display_fields
                        conn.execute(
                            f"""
                            UPDATE {table}
                            SET payload_json = ?, updated_at = ?
                            WHERE record_key = ?
                            """,
                            (self._json(payload), time.time(), row["record_key"]),
                        )
                conn.commit()
        return patched

    def source_snapshot_stats(self) -> dict[str, Any]:
        if not self.db_path.exists():
            return {}
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                active_row = conn.execute(
                    "SELECT value FROM meta WHERE key = 'active_source_snapshot_id'"
                ).fetchone()
                active_snapshot_id = (
                    str(active_row["value"] or "").strip() if active_row else ""
                )
                active = None
                if active_snapshot_id:
                    active = conn.execute(
                        """
                        SELECT snapshot_id, status, started_at, finished_at, warnings_json,
                               counts_json, meta_json, error, updated_at
                        FROM source_snapshot_manifest
                        WHERE snapshot_id = ?
                        """,
                        (active_snapshot_id,),
                    ).fetchone()
                failed = conn.execute(
                    """
                    SELECT snapshot_id, status, started_at, finished_at, warnings_json,
                           counts_json, meta_json, error, updated_at
                    FROM source_snapshot_manifest
                    WHERE status = 'failed'
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """
                ).fetchone()
                total = conn.execute(
                    "SELECT count(*) AS c FROM source_snapshot_manifest"
                ).fetchone()

        def row_payload(row: sqlite3.Row | None) -> dict[str, Any]:
            if not row:
                return {}
            meta = self._loads(str(row["meta_json"] or ""), {})
            counts = self._loads(str(row["counts_json"] or ""), {})
            warnings = self._loads(str(row["warnings_json"] or ""), [])
            return {
                "snapshot_id": str(row["snapshot_id"] or ""),
                "status": str(row["status"] or ""),
                "started_at": float(row["started_at"] or 0),
                "finished_at": float(row["finished_at"] or 0),
                "updated_at": float(row["updated_at"] or 0),
                "warnings": warnings if isinstance(warnings, list) else [],
                "counts": counts if isinstance(counts, dict) else {},
                "meta": meta if isinstance(meta, dict) else {},
                "error": str(row["error"] or ""),
            }

        return {
            "active": row_payload(active),
            "last_failed": row_payload(failed),
            "manifest_count": int(total["c"] or 0) if total else 0,
        }

    def active_source_snapshot_meta(self) -> dict[str, Any]:
        if not self.db_path.exists():
            return {"snapshot_id": "", "updated_at": 0.0}
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    """
                    SELECT key, value
                    FROM meta
                    WHERE key IN (
                        'active_source_snapshot_id',
                        'active_source_snapshot_at'
                    )
                    """
                ).fetchall()
        meta = {str(row["key"]): str(row["value"]) for row in rows}
        try:
            updated_at = float(meta.get("active_source_snapshot_at") or 0)
        except Exception:
            updated_at = 0.0
        return {
            "snapshot_id": meta.get("active_source_snapshot_id", ""),
            "updated_at": updated_at,
        }

    def source_snapshot_work_type_stats(self) -> dict[str, Any]:
        if not self.db_path.exists():
            return {}
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                active_row = conn.execute(
                    "SELECT value FROM meta WHERE key = 'active_source_snapshot_id'"
                ).fetchone()
                active_snapshot_id = (
                    str(active_row["value"] or "").strip() if active_row else ""
                )
                if not active_snapshot_id:
                    return {}
                rows = conn.execute(
                    """
                    SELECT scope, record_kind, COALESCE(NULLIF(work_type, ''), 'unknown') AS work_type,
                           COUNT(*) AS count
                    FROM source_snapshot_records
                    WHERE snapshot_id = ?
                    GROUP BY scope, record_kind, COALESCE(NULLIF(work_type, ''), 'unknown')
                    ORDER BY scope, record_kind, work_type
                    """,
                    (active_snapshot_id,),
                ).fetchall()
        scopes: dict[str, dict[str, Any]] = {}
        totals: dict[str, int] = {}
        for row in rows:
            scope = str(row["scope"] or "")
            record_kind = str(row["record_kind"] or "")
            work_type = str(row["work_type"] or "unknown")
            count = int(row["count"] or 0)
            scope_payload = scopes.setdefault(scope, {"records": {}, "zhihang_records": {}, "total": 0})
            if record_kind == "zhihang":
                bucket_name = "zhihang_records"
            else:
                bucket_name = "records"
            bucket = scope_payload.setdefault(bucket_name, {})
            bucket[work_type] = int(bucket.get(work_type) or 0) + count
            scope_payload["total"] = int(scope_payload.get("total") or 0) + count
            totals[work_type] = int(totals.get(work_type) or 0) + count
        return {
            "snapshot_id": active_snapshot_id,
            "scopes": scopes,
            "totals": totals,
            "scope_count": len(scopes),
            "checked_at": time.time(),
        }

    def cleanup_source_snapshots(
        self, *, keep_success: int = 3, keep_failed: int = 5
    ) -> dict[str, int]:
        if not self.db_path.exists():
            return {"deleted_manifests": 0, "deleted_records": 0}
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    """
                    SELECT snapshot_id, status
                    FROM source_snapshot_manifest
                    ORDER BY updated_at DESC
                    """
                ).fetchall()
                success_seen = 0
                failed_seen = 0
                delete_ids: list[str] = []
                for row in rows:
                    snapshot_id = str(row["snapshot_id"] or "")
                    status = str(row["status"] or "")
                    if status in {"active", "retained"}:
                        success_seen += 1
                        if success_seen > max(1, int(keep_success or 3)):
                            delete_ids.append(snapshot_id)
                    elif status == "failed":
                        failed_seen += 1
                        if failed_seen > max(0, int(keep_failed or 5)):
                            delete_ids.append(snapshot_id)
                    elif status != "building":
                        delete_ids.append(snapshot_id)
                deleted_records = 0
                deleted_manifests = 0
                if delete_ids:
                    conn.execute("BEGIN IMMEDIATE")
                    for snapshot_id in delete_ids:
                        cur = conn.execute(
                            "DELETE FROM source_snapshot_records WHERE snapshot_id = ?",
                            (snapshot_id,),
                        )
                        deleted_records += int(cur.rowcount or 0)
                        cur = conn.execute(
                            "DELETE FROM source_snapshot_manifest WHERE snapshot_id = ?",
                            (snapshot_id,),
                        )
                        deleted_manifests += int(cur.rowcount or 0)
                    conn.commit()
        return {
            "deleted_manifests": deleted_manifests,
            "deleted_records": deleted_records,
        }

    @staticmethod
    def _normalize_event_month(value: Any) -> str:
        text = str(value or "").strip()
        match = re.search(r"(\d{4})[-/年](\d{1,2})", text)
        if match:
            return f"{int(match.group(1)):04d}-{int(match.group(2)):02d}"
        return time.strftime("%Y-%m")

    @staticmethod
    def _active_event_month_snapshot_key(month: str) -> str:
        return f"active_event_month_snapshot_id:{month}"

    def replace_event_month_snapshot(
        self,
        month: str,
        records: list[dict[str, Any]],
        *,
        meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        month = self._normalize_event_month(month)
        snapshot_id = uuid.uuid4().hex
        started_at = time.time()
        meta_payload = dict(meta or {})
        normalized_records = [
            dict(item)
            for item in (records or [])
            if isinstance(item, dict)
        ]
        counts = {
            "records": len(normalized_records),
            "by_scope": {},
            "by_status": {},
            "high_level": 0,
        }
        for item in normalized_records:
            item_codes = item.get("building_codes")
            if not isinstance(item_codes, list):
                item_codes = []
            for code in item_codes:
                scope_counts = counts["by_scope"]
                scope_counts[code] = int(scope_counts.get(code) or 0) + 1
            status = str(item.get("status") or "未知").strip() or "未知"
            status_counts = counts["by_status"]
            status_counts[status] = int(status_counts.get(status) or 0) + 1
            if bool(item.get("high_level")):
                counts["high_level"] = int(counts.get("high_level") or 0) + 1
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute("BEGIN IMMEDIATE")
                now = time.time()
                conn.execute(
                    """
                    INSERT INTO event_month_snapshot_manifest(
                        snapshot_id,
                        month,
                        status,
                        started_at,
                        finished_at,
                        counts_json,
                        meta_json,
                        error,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, 'building', ?, NULL, ?, ?, '', ?, ?)
                    """,
                    (
                        snapshot_id,
                        month,
                        started_at,
                        self._json(counts),
                        self._json(meta_payload),
                        now,
                        now,
                    ),
                )
                for index, item in enumerate(normalized_records):
                    source_key = self._text(item.get("source_key") or "event_notice")
                    source_record_id = self._text(
                        item.get("source_record_id")
                        or item.get("record_id")
                        or item.get("target_record_id")
                    )
                    if not source_record_id:
                        source_record_id = hashlib.sha1(
                            self._stable_json(item).encode("utf-8")
                        ).hexdigest()
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO event_month_snapshot_records(
                            snapshot_id,
                            month,
                            source_key,
                            source_record_id,
                            payload_json,
                            sort_order,
                            created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            snapshot_id,
                            month,
                            source_key,
                            source_record_id,
                            self._json(item),
                            index,
                            now,
                        ),
                    )
                conn.execute(
                    """
                    UPDATE event_month_snapshot_manifest
                    SET status = 'retained', updated_at = ?
                    WHERE month = ? AND status = 'active'
                    """,
                    (now, month),
                )
                conn.execute(
                    """
                    UPDATE event_month_snapshot_manifest
                    SET status = 'active', finished_at = ?, updated_at = ?
                    WHERE snapshot_id = ?
                    """,
                    (now, now, snapshot_id),
                )
                conn.execute(
                    "INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)",
                    (self._active_event_month_snapshot_key(month), snapshot_id),
                )
                conn.commit()
        self.cleanup_event_month_snapshots()
        return {
            "snapshot_id": snapshot_id,
            "status": "active",
            "month": month,
            "counts": counts,
            "updated_at": now,
        }

    def record_failed_event_month_snapshot(
        self,
        month: str,
        *,
        meta: dict[str, Any] | None = None,
        error: str = "",
    ) -> dict[str, Any]:
        month = self._normalize_event_month(month)
        snapshot_id = uuid.uuid4().hex
        now = time.time()
        meta_payload = dict(meta or {})
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute(
                    """
                    INSERT OR REPLACE INTO event_month_snapshot_manifest(
                        snapshot_id,
                        month,
                        status,
                        started_at,
                        finished_at,
                        counts_json,
                        meta_json,
                        error,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, 'failed', ?, ?, '{}', ?, ?, ?, ?)
                    """,
                    (
                        snapshot_id,
                        month,
                        now,
                        now,
                        self._json(meta_payload),
                        self._text(error),
                        now,
                        now,
                    ),
                )
                conn.commit()
        self.cleanup_event_month_snapshots()
        return {
            "snapshot_id": snapshot_id,
            "status": "failed",
            "month": month,
            "updated_at": now,
        }

    def get_event_month_snapshot(self, month: str) -> dict[str, Any]:
        month = self._normalize_event_month(month)
        if not self.db_path.exists():
            return {
                "exists": False,
                "month": month,
                "records": [],
                "meta": {},
                "updated_at": 0.0,
                "last_failed": {},
            }
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                active_key = self._active_event_month_snapshot_key(month)
                active_row = conn.execute(
                    "SELECT value FROM meta WHERE key = ?",
                    (active_key,),
                ).fetchone()
                active_snapshot_id = (
                    str(active_row["value"] or "").strip() if active_row else ""
                )
                manifest_row = None
                records_rows: list[sqlite3.Row] = []
                if active_snapshot_id:
                    manifest_row = conn.execute(
                        """
                        SELECT *
                        FROM event_month_snapshot_manifest
                        WHERE snapshot_id = ? AND month = ? AND status = 'active'
                        """,
                        (active_snapshot_id, month),
                    ).fetchone()
                    if manifest_row:
                        records_rows = conn.execute(
                            """
                            SELECT payload_json
                            FROM event_month_snapshot_records
                            WHERE snapshot_id = ? AND month = ?
                            ORDER BY sort_order ASC, source_key ASC, source_record_id ASC
                            """,
                            (active_snapshot_id, month),
                        ).fetchall()
                failed_row = conn.execute(
                    """
                    SELECT *
                    FROM event_month_snapshot_manifest
                    WHERE month = ? AND status = 'failed'
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """,
                    (month,),
                ).fetchone()
        records: list[dict[str, Any]] = []
        for row in records_rows:
            payload = self._loads(str(row["payload_json"] or ""), {})
            if isinstance(payload, dict):
                records.append(payload)

        def manifest_payload(row: sqlite3.Row | None) -> dict[str, Any]:
            if not row:
                return {}
            meta = self._loads(str(row["meta_json"] or ""), {})
            counts = self._loads(str(row["counts_json"] or ""), {})
            return {
                "snapshot_id": str(row["snapshot_id"] or ""),
                "month": str(row["month"] or ""),
                "status": str(row["status"] or ""),
                "started_at": float(row["started_at"] or 0),
                "finished_at": float(row["finished_at"] or 0),
                "updated_at": float(row["updated_at"] or 0),
                "counts": counts if isinstance(counts, dict) else {},
                "meta": meta if isinstance(meta, dict) else {},
                "error": str(row["error"] or ""),
            }

        manifest = manifest_payload(manifest_row)
        return {
            "exists": bool(manifest_row),
            "month": month,
            "records": records,
            "meta": manifest.get("meta") or {},
            "updated_at": float(manifest.get("finished_at") or manifest.get("updated_at") or 0),
            "snapshot_id": manifest.get("snapshot_id") or "",
            "count": len(records),
            "manifest": manifest,
            "last_failed": manifest_payload(failed_row),
        }

    def event_month_snapshot_stats(self) -> dict[str, Any]:
        if not self.db_path.exists():
            return {}
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                active_rows = conn.execute(
                    """
                    SELECT month, snapshot_id, status, started_at, finished_at,
                           counts_json, meta_json, error, updated_at
                    FROM event_month_snapshot_manifest
                    WHERE status = 'active'
                    ORDER BY month DESC, updated_at DESC
                    LIMIT 12
                    """
                ).fetchall()
                failed = conn.execute(
                    """
                    SELECT month, snapshot_id, status, started_at, finished_at,
                           counts_json, meta_json, error, updated_at
                    FROM event_month_snapshot_manifest
                    WHERE status = 'failed'
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """
                ).fetchone()
                total = conn.execute(
                    "SELECT count(*) AS c FROM event_month_snapshot_manifest"
                ).fetchone()

        def row_payload(row: sqlite3.Row | None) -> dict[str, Any]:
            if not row:
                return {}
            meta = self._loads(str(row["meta_json"] or ""), {})
            counts = self._loads(str(row["counts_json"] or ""), {})
            return {
                "month": str(row["month"] or ""),
                "snapshot_id": str(row["snapshot_id"] or ""),
                "status": str(row["status"] or ""),
                "started_at": float(row["started_at"] or 0),
                "finished_at": float(row["finished_at"] or 0),
                "updated_at": float(row["updated_at"] or 0),
                "counts": counts if isinstance(counts, dict) else {},
                "meta": meta if isinstance(meta, dict) else {},
                "error": str(row["error"] or ""),
            }

        return {
            "active_months": [row_payload(row) for row in active_rows],
            "last_failed": row_payload(failed),
            "manifest_count": int(total["c"] or 0) if total else 0,
        }

    def cleanup_event_month_snapshots(
        self, *, keep_success: int = 3, keep_failed: int = 5
    ) -> dict[str, int]:
        if not self.db_path.exists():
            return {"deleted_manifests": 0, "deleted_records": 0}
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    """
                    SELECT snapshot_id, month, status
                    FROM event_month_snapshot_manifest
                    ORDER BY month DESC, updated_at DESC
                    """
                ).fetchall()
                success_seen: dict[str, int] = {}
                failed_seen = 0
                delete_ids: list[str] = []
                for row in rows:
                    snapshot_id = str(row["snapshot_id"] or "")
                    month = str(row["month"] or "")
                    status = str(row["status"] or "")
                    if status in {"active", "retained"}:
                        success_seen[month] = int(success_seen.get(month) or 0) + 1
                        if success_seen[month] > max(1, int(keep_success or 3)):
                            delete_ids.append(snapshot_id)
                    elif status == "failed":
                        failed_seen += 1
                        if failed_seen > max(0, int(keep_failed or 5)):
                            delete_ids.append(snapshot_id)
                    elif status != "building":
                        delete_ids.append(snapshot_id)
                deleted_records = 0
                deleted_manifests = 0
                if delete_ids:
                    conn.execute("BEGIN IMMEDIATE")
                    for snapshot_id in delete_ids:
                        cur = conn.execute(
                            "DELETE FROM event_month_snapshot_records WHERE snapshot_id = ?",
                            (snapshot_id,),
                        )
                        deleted_records += int(cur.rowcount or 0)
                        cur = conn.execute(
                            "DELETE FROM event_month_snapshot_manifest WHERE snapshot_id = ?",
                            (snapshot_id,),
                        )
                        deleted_manifests += int(cur.rowcount or 0)
                    conn.commit()
        return {
            "deleted_manifests": deleted_manifests,
            "deleted_records": deleted_records,
        }

    def list_source_scope_snapshot_meta(self) -> list[dict[str, Any]]:
        if not self.db_path.exists():
            return []
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    """
                    SELECT scope, payload_json, updated_at
                    FROM source_scope_snapshots
                    ORDER BY scope ASC
                    """
                ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            payload = self._loads(str(row["payload_json"] or ""), {})
            if not isinstance(payload, dict):
                payload = {}
            result.append(
                {
                    "scope": str(row["scope"] or ""),
                    "payload": payload,
                    "updated_at": float(row["updated_at"] or 0),
                }
            )
        return result

    def upsert_repair_link_task(self, task: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(task, dict):
            return {}
        task_key = self._text(task.get("task_key"))
        if not task_key:
            return {}
        now = time.time()
        normalized = dict(task)
        normalized.setdefault("status", "pending")
        normalized.setdefault("attempts", 0)
        normalized.setdefault("max_attempts", 18)
        normalized.setdefault("created_at", now)
        normalized["updated_at"] = now
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                existing = conn.execute(
                    """
                    SELECT payload_json, status
                    FROM repair_link_tasks
                    WHERE task_key = ?
                    """,
                    (task_key,),
                ).fetchone()
                if existing:
                    payload = self._loads(str(existing["payload_json"] or ""), {})
                    if isinstance(payload, dict):
                        status = str(existing["status"] or payload.get("status") or "")
                        if status == "linked":
                            payload["status"] = "linked"
                            return payload
                        payload.update(
                            {
                                key: value
                                for key, value in normalized.items()
                                if value not in (None, "")
                            }
                        )
                        normalized = payload
                created_at = float(normalized.get("created_at") or now)
                conn.execute(
                    """
                    INSERT OR REPLACE INTO repair_link_tasks(
                        task_key,
                        status,
                        source_app_token,
                        source_table_id,
                        source_record_id,
                        sync_app_token,
                        sync_table_id,
                        target_app_token,
                        target_table_id,
                        target_record_id,
                        link_field_name,
                        sync_record_id,
                        due_at,
                        attempts,
                        max_attempts,
                        last_error,
                        payload_json,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        task_key,
                        self._text(normalized.get("status")) or "pending",
                        self._text(normalized.get("source_app_token")),
                        self._text(normalized.get("source_table_id")),
                        self._text(normalized.get("source_record_id")),
                        self._text(normalized.get("sync_app_token")),
                        self._text(normalized.get("sync_table_id")),
                        self._text(normalized.get("target_app_token")),
                        self._text(normalized.get("target_table_id")),
                        self._text(normalized.get("target_record_id")),
                        self._text(normalized.get("link_field_name")) or "设备检修关联",
                        self._text(normalized.get("sync_record_id")),
                        float(normalized.get("due_at") or now),
                        int(normalized.get("attempts") or 0),
                        int(normalized.get("max_attempts") or 18),
                        self._text(normalized.get("last_error")),
                        self._json(normalized),
                        created_at,
                        now,
                    ),
                )
                conn.commit()
        return normalized

    def list_due_repair_link_tasks(
        self, *, now: float | None = None, limit: int = 3
    ) -> list[dict[str, Any]]:
        now = time.time() if now is None else float(now)
        limit = max(1, min(20, int(limit or 3)))
        if not self.db_path.exists():
            return []
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    """
                    SELECT task_key, payload_json
                    FROM repair_link_tasks
                    WHERE status = 'pending' AND due_at <= ?
                    ORDER BY due_at ASC, updated_at ASC
                    LIMIT ?
                    """,
                    (now, limit),
                ).fetchall()
        tasks: list[dict[str, Any]] = []
        for row in rows:
            payload = self._loads(str(row["payload_json"] or ""), {})
            if not isinstance(payload, dict):
                continue
            payload["task_key"] = str(payload.get("task_key") or row["task_key"] or "")
            tasks.append(payload)
        return tasks

    def claim_due_repair_link_tasks(
        self,
        *,
        now: float | None = None,
        limit: int = 3,
        lease_seconds: int = 300,
    ) -> list[dict[str, Any]]:
        now = time.time() if now is None else float(now)
        limit = max(1, min(20, int(limit or 3)))
        lease_until = now + max(60, min(3600, int(lease_seconds or 300)))
        if not self.db_path.exists():
            return []
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    """
                    SELECT task_key, payload_json
                    FROM repair_link_tasks
                    WHERE status IN ('pending', 'processing') AND due_at <= ?
                    ORDER BY due_at ASC, updated_at ASC
                    LIMIT ?
                    """,
                    (now, limit),
                ).fetchall()
                tasks: list[dict[str, Any]] = []
                for row in rows:
                    payload = self._loads(str(row["payload_json"] or ""), {})
                    if not isinstance(payload, dict):
                        continue
                    task_key = str(payload.get("task_key") or row["task_key"] or "")
                    if not task_key:
                        continue
                    payload["task_key"] = task_key
                    payload["status"] = "processing"
                    payload["due_at"] = lease_until
                    payload["claimed_at"] = now
                    payload["updated_at"] = now
                    conn.execute(
                        """
                        UPDATE repair_link_tasks
                        SET status = 'processing',
                            due_at = ?,
                            payload_json = ?,
                            updated_at = ?
                        WHERE task_key = ?
                        """,
                        (lease_until, self._json(payload), now, task_key),
                    )
                    tasks.append(dict(payload))
                conn.commit()
        return tasks

    def update_repair_link_task(self, task_key: str, patch: dict[str, Any]) -> None:
        task_key = self._text(task_key)
        if not task_key or not isinstance(patch, dict):
            return
        now = time.time()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(
                    """
                    SELECT payload_json
                    FROM repair_link_tasks
                    WHERE task_key = ?
                    """,
                    (task_key,),
                ).fetchone()
                if not row:
                    return
                payload = self._loads(str(row["payload_json"] or ""), {})
                payload = payload if isinstance(payload, dict) else {}
                payload.update(patch)
                payload["updated_at"] = now
                conn.execute(
                    """
                    UPDATE repair_link_tasks
                    SET status = ?,
                        sync_record_id = ?,
                        due_at = ?,
                        attempts = ?,
                        max_attempts = ?,
                        last_error = ?,
                        payload_json = ?,
                        updated_at = ?
                    WHERE task_key = ?
                    """,
                    (
                        self._text(payload.get("status")) or "pending",
                        self._text(payload.get("sync_record_id")),
                        float(payload.get("due_at") or now),
                        int(payload.get("attempts") or 0),
                        int(payload.get("max_attempts") or 18),
                        self._text(payload.get("last_error")),
                        self._json(payload),
                        now,
                        task_key,
                    ),
                )
                conn.commit()

    def get_document(self, namespace: str, key: str) -> dict[str, Any] | None:
        namespace = self._text(namespace)
        key = self._text(key)
        if not namespace or not key or not self.db_path.exists():
            return None
        table = self._document_table_for_namespace(namespace)
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                if table:
                    payload = self._get_table_document_locked(conn, table, key)
                    if isinstance(payload, dict):
                        return payload
                    legacy_payload = self._get_legacy_json_document_locked(
                        conn, namespace, key
                    )
                    if isinstance(legacy_payload, dict):
                        self._put_table_document_locked(conn, table, key, legacy_payload)
                        conn.commit()
                        return legacy_payload
                    return None
                row = conn.execute(
                    """
                    SELECT payload_json
                    FROM json_documents
                    WHERE namespace = ? AND key = ?
                    """,
                    (namespace, key),
                ).fetchone()
        if not row:
            return None
        payload = self._loads(str(row["payload_json"] or ""), {})
        return payload if isinstance(payload, dict) else None

    def put_document(
        self, namespace: str, key: str, payload: dict[str, Any] | None
    ) -> None:
        namespace = self._text(namespace)
        key = self._text(key)
        if not namespace or not key:
            return
        normalized = dict(payload or {})
        table = self._document_table_for_namespace(namespace)
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                if table:
                    self._put_table_document_locked(conn, table, key, normalized)
                else:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO json_documents(
                            namespace, key, payload_json, updated_at
                        ) VALUES (?, ?, ?, ?)
                        """,
                        (namespace, key, self._json(normalized), time.time()),
                    )
                conn.commit()

    def delete_document(self, namespace: str, key: str) -> None:
        namespace = self._text(namespace)
        key = self._text(key)
        if not namespace or not key or not self.db_path.exists():
            return
        table = self._document_table_for_namespace(namespace)
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                if table:
                    self._delete_table_document_locked(conn, table, key)
                else:
                    conn.execute(
                        "DELETE FROM json_documents WHERE namespace = ? AND key = ?",
                        (namespace, key),
                    )
                conn.commit()

    def list_documents(
        self, namespace: str, *, key_prefix: str = ""
    ) -> list[dict[str, Any]]:
        namespace = self._text(namespace)
        key_prefix = self._text(key_prefix)
        if not namespace or not self.db_path.exists():
            return []
        table = self._document_table_for_namespace(namespace)
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                if table:
                    documents = self._list_table_documents_locked(
                        conn, table, key_prefix
                    )
                    seen = {str(doc.get("key") or "") for doc in documents}
                    legacy_documents = self._list_legacy_json_documents_locked(
                        conn, namespace, key_prefix
                    )
                    migrated = False
                    for legacy in legacy_documents:
                        legacy_key = str(legacy.get("key") or "")
                        if not legacy_key or legacy_key in seen:
                            continue
                        payload = legacy.get("payload")
                        if not isinstance(payload, dict):
                            continue
                        self._put_table_document_locked(
                            conn, table, legacy_key, payload
                        )
                        documents.append(legacy)
                        seen.add(legacy_key)
                        migrated = True
                    if migrated:
                        conn.commit()
                    return sorted(documents, key=lambda item: str(item.get("key") or ""))
                if key_prefix:
                    rows = conn.execute(
                        """
                        SELECT key, payload_json, updated_at
                        FROM json_documents
                        WHERE namespace = ? AND key LIKE ?
                        ORDER BY key ASC
                        """,
                        (namespace, f"{key_prefix}%"),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT key, payload_json, updated_at
                        FROM json_documents
                        WHERE namespace = ?
                        ORDER BY key ASC
                        """,
                        (namespace,),
                    ).fetchall()
        documents: list[dict[str, Any]] = []
        for row in rows:
            payload = self._loads(str(row["payload_json"] or ""), {})
            if not isinstance(payload, dict):
                continue
            documents.append(
                {
                    "key": str(row["key"] or ""),
                    "payload": payload,
                    "updated_at": float(row["updated_at"] or 0),
                }
            )
        return documents

    def list_document_meta(
        self, namespace: str, *, key_prefix: str = ""
    ) -> list[dict[str, Any]]:
        namespace = self._text(namespace)
        key_prefix = self._text(key_prefix)
        if not namespace or not self.db_path.exists():
            return []
        table = self._document_table_for_namespace(namespace)
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                if table:
                    if key_prefix:
                        rows = conn.execute(
                            f"""
                            SELECT key, updated_at
                            FROM {table}
                            WHERE key LIKE ?
                            ORDER BY key ASC
                            """,
                            (f"{key_prefix}%",),
                        ).fetchall()
                    else:
                        rows = conn.execute(
                            f"""
                            SELECT key, updated_at
                            FROM {table}
                            ORDER BY key ASC
                            """
                        ).fetchall()
                elif key_prefix:
                    rows = conn.execute(
                        """
                        SELECT key, updated_at
                        FROM json_documents
                        WHERE namespace = ? AND key LIKE ?
                        ORDER BY key ASC
                        """,
                        (namespace, f"{key_prefix}%"),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT key, updated_at
                        FROM json_documents
                        WHERE namespace = ?
                        ORDER BY key ASC
                        """,
                        (namespace,),
                    ).fetchall()
        return [
            {
                "key": str(row["key"] or ""),
                "updated_at": float(row["updated_at"] or 0),
            }
            for row in rows
        ]

    def upsert_work_type_override(
        self,
        *,
        source_work_type: str,
        normalized_title: str,
        target_work_type: str,
        title: str = "",
        payload: dict[str, Any] | None = None,
        updated_by: str = "",
    ) -> dict[str, Any]:
        source_work_type = self._text(source_work_type) or "maintenance"
        target_work_type = self._text(target_work_type) or source_work_type
        normalized_title = self._text(normalized_title)
        if not normalized_title:
            raise ValueError("normalized_title is required")
        override_key = f"{source_work_type}:{normalized_title}"
        now = time.time()
        normalized_payload = dict(payload or {})
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                existing = conn.execute(
                    "SELECT created_at FROM notice_work_type_overrides WHERE override_key = ?",
                    (override_key,),
                ).fetchone()
                created_at = float(existing["created_at"]) if existing else now
                conn.execute(
                    """
                    INSERT OR REPLACE INTO notice_work_type_overrides(
                        override_key, source_work_type, target_work_type,
                        normalized_title, title, payload_json, enabled,
                        updated_by, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
                    """,
                    (
                        override_key,
                        source_work_type,
                        target_work_type,
                        normalized_title,
                        self._text(title),
                        self._json(normalized_payload),
                        self._text(updated_by),
                        created_at,
                        now,
                    ),
                )
                conn.commit()
        return {
            "override_key": override_key,
            "source_work_type": source_work_type,
            "target_work_type": target_work_type,
            "normalized_title": normalized_title,
            "title": self._text(title),
            "payload": normalized_payload,
            "enabled": True,
            "updated_by": self._text(updated_by),
            "created_at": created_at,
            "updated_at": now,
        }

    def disable_work_type_override(
        self,
        *,
        source_work_type: str,
        normalized_title: str,
        updated_by: str = "",
    ) -> bool:
        source_work_type = self._text(source_work_type) or "maintenance"
        normalized_title = self._text(normalized_title)
        if not normalized_title:
            return False
        override_key = f"{source_work_type}:{normalized_title}"
        now = time.time()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                cur = conn.execute(
                    """
                    UPDATE notice_work_type_overrides
                    SET enabled = 0, updated_by = ?, updated_at = ?
                    WHERE override_key = ? AND enabled = 1
                    """,
                    (self._text(updated_by), now, override_key),
                )
                conn.commit()
                return bool(cur.rowcount)

    def get_work_type_override(
        self, *, source_work_type: str, normalized_title: str
    ) -> dict[str, Any] | None:
        source_work_type = self._text(source_work_type) or "maintenance"
        normalized_title = self._text(normalized_title)
        if not normalized_title or not self.db_path.exists():
            return None
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(
                    """
                    SELECT *
                    FROM notice_work_type_overrides
                    WHERE source_work_type = ?
                      AND normalized_title = ?
                      AND enabled = 1
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """,
                    (source_work_type, normalized_title),
                ).fetchone()
        if not row:
            return None
        payload = self._loads(str(row["payload_json"] or ""), {})
        return {
            "override_key": str(row["override_key"] or ""),
            "source_work_type": str(row["source_work_type"] or ""),
            "target_work_type": str(row["target_work_type"] or ""),
            "normalized_title": str(row["normalized_title"] or ""),
            "title": str(row["title"] or ""),
            "payload": payload if isinstance(payload, dict) else {},
            "enabled": bool(row["enabled"]),
            "updated_by": str(row["updated_by"] or ""),
            "created_at": float(row["created_at"] or 0),
            "updated_at": float(row["updated_at"] or 0),
        }

    def list_work_type_overrides(
        self, *, source_work_type: str = "", enabled_only: bool = True
    ) -> list[dict[str, Any]]:
        source_work_type = self._text(source_work_type)
        if not self.db_path.exists():
            return []
        clauses = []
        params: list[Any] = []
        if source_work_type:
            clauses.append("source_work_type = ?")
            params.append(source_work_type)
        if enabled_only:
            clauses.append("enabled = 1")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    f"""
                    SELECT *
                    FROM notice_work_type_overrides
                    {where}
                    ORDER BY updated_at DESC
                    """,
                    params,
                ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            payload = self._loads(str(row["payload_json"] or ""), {})
            items.append(
                {
                    "override_key": str(row["override_key"] or ""),
                    "source_work_type": str(row["source_work_type"] or ""),
                    "target_work_type": str(row["target_work_type"] or ""),
                    "normalized_title": str(row["normalized_title"] or ""),
                    "title": str(row["title"] or ""),
                    "payload": payload if isinstance(payload, dict) else {},
                    "enabled": bool(row["enabled"]),
                    "updated_by": str(row["updated_by"] or ""),
                    "created_at": float(row["created_at"] or 0),
                    "updated_at": float(row["updated_at"] or 0),
                }
            )
        return items

    def _qt_active_item_key(self, payload: dict[str, Any], fallback: str = "") -> str:
        return (
            self._text(payload.get("active_item_id"))
            or self._text(payload.get("item_id"))
            or canonical_target_record_id(payload)
            or self._text(fallback)
        )

    def _normalize_qt_active_section(self, section: str) -> str:
        section = self._text(section)
        return section if section in {"event", "other"} else "other"

    def _qt_active_item_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
        payload = self._loads(str(row["payload_json"] or ""), {})
        payload = payload if isinstance(payload, dict) else {}
        return {
            "active_item_id": str(row["active_item_id"] or ""),
            "record_id": str(row["record_id"] or ""),
            "notice_type": str(row["notice_type"] or ""),
            "section": str(row["section"] or "other"),
            "sort_order": int(row["sort_order"] or 0),
            "origin": str(row["origin"] or ""),
            "payload": payload,
            "updated_at": float(row["updated_at"] or 0),
            "deleted_at": float(row["deleted_at"] or 0) if row["deleted_at"] is not None else None,
        }

    def list_qt_active_items(self, *, include_deleted: bool = False) -> list[dict[str, Any]]:
        if not self.db_path.exists():
            return []
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                where = "" if include_deleted else "WHERE deleted_at IS NULL"
                rows = conn.execute(
                    f"""
                    SELECT active_item_id, record_id, notice_type, section, sort_order,
                           origin, payload_json, updated_at, deleted_at
                    FROM qt_active_items
                    {where}
                    ORDER BY section ASC, sort_order ASC, updated_at DESC
                    """
                ).fetchall()
        items = [self._qt_active_item_from_row(row) for row in rows]
        return items

    def upsert_qt_active_item(
        self,
        payload: dict[str, Any] | None,
        *,
        section: str = "",
        sort_order: int = 0,
        origin: str = "",
    ) -> bool:
        if not isinstance(payload, dict):
            return False
        payload, _ = self._canonicalize_legacy_change_notice_payload(payload)
        payload = payload if isinstance(payload, dict) else {}
        normalized = normalize_notice_identity_payload(
            self._enrich_notice_payload_from_text(payload or {})
        )
        explicit_active_item_id = self._text(normalized.get("active_item_id"))
        active_item_id = self._qt_active_item_key(normalized)
        if not active_item_id:
            return False
        normalized.setdefault("active_item_id", active_item_id)
        record_id = canonical_target_record_id(normalized)
        notice_type = self._text(normalized.get("notice_type"))
        section = self._normalize_qt_active_section(
            section or ("event" if notice_type == "事件通告" else "other")
        )
        origin = self._text(origin) or self._text(normalized.get("origin"))
        if not origin and bool(normalized.get("lan_created_from_portal")):
            origin = "portal"
        now = time.time()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                existing_identity = self._notice_identity_lookup_locked(
                    conn,
                    work_type=self._text(normalized.get("work_type")),
                    active_item_id=active_item_id,
                    source_record_id=canonical_source_record_id(normalized),
                    target_record_id=record_id,
                )
                if existing_identity is not None:
                    existing_identity_data = self._notice_identity_from_row(existing_identity)
                    existing_active_item_id = self._text(
                        existing_identity_data.get("active_item_id")
                    )
                    if existing_active_item_id and not explicit_active_item_id:
                        active_item_id = existing_active_item_id
                        normalized["active_item_id"] = active_item_id
                    existing_target_record_id = self._text(
                        existing_identity_data.get("target_record_id")
                    )
                    if existing_target_record_id and not record_id:
                        record_id = existing_target_record_id
                        normalized["record_id"] = existing_target_record_id
                        normalized["target_record_id"] = existing_target_record_id
                        normalized["_is_placeholder_record"] = False
                conn.execute(
                    """
                    INSERT INTO qt_active_items(
                        active_item_id, record_id, notice_type, section, sort_order,
                        origin, payload_json, updated_at, deleted_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
                    ON CONFLICT(active_item_id) DO UPDATE SET
                        record_id = excluded.record_id,
                        notice_type = excluded.notice_type,
                        section = excluded.section,
                        sort_order = excluded.sort_order,
                        origin = excluded.origin,
                        payload_json = excluded.payload_json,
                        updated_at = excluded.updated_at,
                        deleted_at = NULL
                    """,
                    (
                        active_item_id,
                        record_id,
                        notice_type,
                        section,
                        int(sort_order or 0),
                        origin,
                        self._json(normalized),
                        now,
                    ),
                )
                self._upsert_notice_identity_locked(conn, normalized, origin=origin)
                conn.commit()
        return True

    def delete_qt_active_item(
        self, *, active_item_id: str = "", record_id: str = ""
    ) -> bool:
        active_item_id = self._text(active_item_id)
        record_id = self._text(record_id)
        if not active_item_id and not record_id:
            return False
        now = time.time()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                if active_item_id:
                    cursor = conn.execute(
                        """
                        UPDATE qt_active_items
                        SET deleted_at = ?, updated_at = ?
                        WHERE active_item_id = ? AND deleted_at IS NULL
                        """,
                        (now, now, active_item_id),
                    )
                else:
                    cursor = conn.execute(
                        """
                        UPDATE qt_active_items
                        SET deleted_at = ?, updated_at = ?
                        WHERE record_id = ? AND deleted_at IS NULL
                        """,
                        (now, now, record_id),
                    )
                conn.commit()
                return bool(cursor.rowcount)

    def _notice_identity_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
        payload = self._loads(str(row["payload_json"] or ""), {})
        payload = payload if isinstance(payload, dict) else {}
        target_record_id = str(row["target_record_id"] or "")
        if is_local_record_id(target_record_id):
            target_record_id = ""
            payload.pop("target_record_id", None)
        building_codes = self._loads(str(row["building_codes_json"] or "[]"), [])
        if not isinstance(building_codes, list):
            building_codes = []
        return {
            "identity_id": str(row["identity_id"] or ""),
            "work_type": str(row["work_type"] or ""),
            "notice_type": str(row["notice_type"] or ""),
            "active_item_id": str(row["active_item_id"] or ""),
            "source_app_token": str(row["source_app_token"] or ""),
            "source_table_id": str(row["source_table_id"] or ""),
            "source_record_id": str(row["source_record_id"] or ""),
            "target_app_token": str(row["target_app_token"] or ""),
            "target_table_id": str(row["target_table_id"] or ""),
            "target_record_id": target_record_id,
            "title": str(row["title"] or ""),
            "reason": str(row["reason"] or ""),
            "building_codes": building_codes,
            "start_time": str(row["start_time"] or ""),
            "end_time": str(row["end_time"] or ""),
            "status": str(row["status"] or ""),
            "origin": str(row["origin"] or ""),
            "payload": payload,
            "created_at": float(row["created_at"] or 0),
            "updated_at": float(row["updated_at"] or 0),
            "deleted_at": (
                float(row["deleted_at"] or 0)
                if row["deleted_at"] is not None
                else None
            ),
        }

    def _notice_identity_lookup_locked(
        self,
        conn: sqlite3.Connection,
        *,
        work_type: str = "",
        active_item_id: str = "",
        source_record_id: str = "",
        target_record_id: str = "",
    ) -> sqlite3.Row | None:
        work_type = self._text(work_type)
        source_record_id = "" if is_local_record_id(self._text(source_record_id)) else self._text(source_record_id)
        target_record_id = "" if is_local_record_id(self._text(target_record_id)) else self._text(target_record_id)
        lookups = (
            ("target_record_id", target_record_id),
            ("source_record_id", source_record_id),
            ("active_item_id", self._text(active_item_id)),
        )
        for column, value in lookups:
            if not value:
                continue
            clauses = [f"{column} = ?", "deleted_at IS NULL"]
            params: list[Any] = [value]
            if work_type:
                clauses.append("(work_type = ? OR work_type = '')")
                params.append(work_type)
            row = conn.execute(
                f"""
                SELECT *
                FROM notice_identity_map
                WHERE {' AND '.join(clauses)}
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                tuple(params),
            ).fetchone()
            if row:
                return row
        return None

    def _notice_identity_id_for_payload(
        self,
        *,
        work_type: str,
        active_item_id: str,
        source_record_id: str,
        target_record_id: str,
        title: str,
    ) -> str:
        work_type = self._text(work_type) or "maintenance"
        if target_record_id:
            return f"{work_type}:target:{target_record_id}"
        if source_record_id:
            return f"{work_type}:source:{source_record_id}"
        if active_item_id:
            return f"{work_type}:active:{active_item_id}"
        seed = self._json([work_type, self._text(title)])
        return f"{work_type}:generated:{uuid.uuid5(uuid.NAMESPACE_URL, seed).hex}"

    def _upsert_notice_identity_locked(
        self,
        conn: sqlite3.Connection,
        payload: dict[str, Any] | None,
        *,
        origin: str = "",
    ) -> dict[str, Any] | None:
        if not isinstance(payload, dict):
            return None
        payload, _ = self._canonicalize_legacy_change_notice_payload(payload)
        payload = payload if isinstance(payload, dict) else {}
        payload = normalize_notice_identity_payload(payload)
        notice_type = self._text(payload.get("notice_type"))
        if notice_type == "事件通告":
            work_type = "event"
        else:
            work_type = self._text(payload.get("work_type")) or "maintenance"
        active_item_id = self._text(payload.get("active_item_id"))
        source_record_id = canonical_source_record_id(payload)
        target_record_id = canonical_target_record_id(payload)
        record_id = self._text(payload.get("record_id"))
        if not (active_item_id or source_record_id or target_record_id):
            return None

        building_codes = payload.get("building_codes")
        if not isinstance(building_codes, list):
            building_codes = []
        title = self._text(payload.get("title") or payload.get("content"))
        reason = self._text(payload.get("reason"))
        source_app_token = self._text(payload.get("source_app_token"))
        source_table_id = self._text(payload.get("source_table_id"))
        target_app_token = self._text(payload.get("target_app_token"))
        target_table_id = self._text(payload.get("target_table_id"))
        start_time = self._text(payload.get("start_time") or payload.get("expected_time"))
        end_time = self._text(payload.get("end_time") or payload.get("fault_time"))
        status = self._text(payload.get("status"))
        origin = self._text(origin) or self._text(payload.get("origin"))
        existing_row = self._notice_identity_lookup_locked(
            conn,
            work_type=work_type,
            active_item_id=active_item_id,
            source_record_id=source_record_id,
            target_record_id=target_record_id,
        )
        existing = self._notice_identity_from_row(existing_row) if existing_row else {}
        existing_target_record_id = self._text(existing.get("target_record_id"))
        if is_local_record_id(existing_target_record_id):
            existing_target_record_id = ""
        identity_id = self._text(existing.get("identity_id")) or self._notice_identity_id_for_payload(
            work_type=work_type,
            active_item_id=active_item_id,
            source_record_id=source_record_id,
            target_record_id=target_record_id,
            title=title,
        )
        now = time.time()
        created_at = float(existing.get("created_at") or now)
        old_payload = existing.get("payload") if isinstance(existing.get("payload"), dict) else {}
        merged_payload = dict(old_payload)
        merged_payload.update(dict(payload))
        values = {
            "work_type": work_type or existing.get("work_type", ""),
            "notice_type": notice_type or existing.get("notice_type", ""),
            "active_item_id": active_item_id or existing.get("active_item_id", ""),
            "source_app_token": source_app_token or existing.get("source_app_token", ""),
            "source_table_id": source_table_id or existing.get("source_table_id", ""),
            "source_record_id": source_record_id or existing.get("source_record_id", ""),
            "target_app_token": target_app_token or existing.get("target_app_token", ""),
            "target_table_id": target_table_id or existing.get("target_table_id", ""),
            "target_record_id": target_record_id or existing_target_record_id,
            "title": title or existing.get("title", ""),
            "reason": reason or existing.get("reason", ""),
            "building_codes": building_codes or existing.get("building_codes", []),
            "start_time": start_time or existing.get("start_time", ""),
            "end_time": end_time or existing.get("end_time", ""),
            "status": status or existing.get("status", ""),
            "origin": origin or existing.get("origin", ""),
        }
        conn.execute(
            """
            INSERT OR REPLACE INTO notice_identity_map(
                identity_id, work_type, notice_type, active_item_id,
                source_app_token, source_table_id, source_record_id,
                target_app_token, target_table_id, target_record_id,
                title, reason, building_codes_json, start_time, end_time,
                status, origin, payload_json, created_at, updated_at, deleted_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                identity_id,
                values["work_type"],
                values["notice_type"],
                values["active_item_id"],
                values["source_app_token"],
                values["source_table_id"],
                values["source_record_id"],
                values["target_app_token"],
                values["target_table_id"],
                values["target_record_id"],
                values["title"],
                values["reason"],
                self._json(values["building_codes"]),
                values["start_time"],
                values["end_time"],
                values["status"],
                values["origin"],
                self._json(merged_payload),
                created_at,
                now,
            ),
        )
        row = conn.execute(
            "SELECT * FROM notice_identity_map WHERE identity_id = ?",
            (identity_id,),
        ).fetchone()
        return self._notice_identity_from_row(row) if row else None

    def upsert_notice_identity(
        self, payload: dict[str, Any] | None, *, origin: str = ""
    ) -> dict[str, Any] | None:
        if not isinstance(payload, dict):
            return None
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                identity = self._upsert_notice_identity_locked(
                    conn,
                    payload,
                    origin=origin,
                )
                conn.commit()
                return identity

    def resolve_notice_identity(
        self,
        *,
        work_type: str = "",
        active_item_id: str = "",
        source_record_id: str = "",
        target_record_id: str = "",
    ) -> dict[str, Any] | None:
        if not self.db_path.exists():
            return None
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = self._notice_identity_lookup_locked(
                    conn,
                    work_type=work_type,
                    active_item_id=active_item_id,
                    source_record_id=source_record_id,
                    target_record_id=target_record_id,
                )
        return self._notice_identity_from_row(row) if row else None

    def list_notice_identities(
        self,
        *,
        include_deleted: bool = False,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        if not self.db_path.exists():
            return []
        limit = max(1, min(int(limit or 0), 5000))
        clauses = []
        if not include_deleted:
            clauses.append("deleted_at IS NULL")
        sql = "SELECT * FROM notice_identity_map"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY updated_at DESC LIMIT ?"
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(sql, (limit,)).fetchall()
        return [self._notice_identity_from_row(row) for row in rows]

    def mark_notice_identity_deleted(
        self,
        *,
        work_type: str = "",
        active_item_id: str = "",
        source_record_id: str = "",
        target_record_id: str = "",
    ) -> bool:
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = self._notice_identity_lookup_locked(
                    conn,
                    work_type=work_type,
                    active_item_id=active_item_id,
                    source_record_id=source_record_id,
                    target_record_id=target_record_id,
                )
                if not row:
                    return False
                now = time.time()
                conn.execute(
                    """
                    UPDATE notice_identity_map
                    SET deleted_at = ?, updated_at = ?
                    WHERE identity_id = ?
                    """,
                    (now, now, str(row["identity_id"] or "")),
                )
                conn.commit()
                return True

    def create_notice_undo_action(self, payload: dict[str, Any]) -> str:
        if not isinstance(payload, dict):
            return ""
        undo_id = self._text(payload.get("undo_id")) or uuid.uuid4().hex
        identity_key = self._text(payload.get("identity_key"))
        action_type = self._text(payload.get("action_type"))
        if not identity_key or not action_type:
            return ""
        now = time.time()
        created_at = float(payload.get("created_at") or now)
        expires_at = float(payload.get("expires_at") or (created_at + 7 * 24 * 60 * 60))
        status = self._text(payload.get("status")) or "available"
        scope = self._text(payload.get("scope"))
        work_type = self._text(payload.get("work_type"))
        notice_type = self._text(payload.get("notice_type"))
        active_item_id = self._text(payload.get("active_item_id"))
        source_record_id = self._text(payload.get("source_record_id"))
        target_record_id = self._text(payload.get("target_record_id"))
        title = self._text(payload.get("title"))
        stored_payload = dict(payload)
        stored_payload["undo_id"] = undo_id
        stored_payload["identity_key"] = identity_key
        stored_payload["status"] = status
        stored_payload["created_at"] = created_at
        stored_payload["updated_at"] = now
        stored_payload["expires_at"] = expires_at
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute(
                    """
                    UPDATE notice_undo_actions
                    SET status = 'superseded', updated_at = ?
                    WHERE identity_key = ? AND status = 'available'
                    """,
                    (now, identity_key),
                )
                conn.execute(
                    """
                    INSERT OR REPLACE INTO notice_undo_actions(
                        undo_id, identity_key, status, action_type, scope,
                        work_type, notice_type, active_item_id, source_record_id,
                        target_record_id, title, payload_json, error, created_at,
                        updated_at, applied_at, expires_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        undo_id,
                        identity_key,
                        status,
                        action_type,
                        scope,
                        work_type,
                        notice_type,
                        active_item_id,
                        source_record_id,
                        target_record_id,
                        title,
                        self._json(stored_payload),
                        self._text(payload.get("error")),
                        created_at,
                        now,
                        None,
                        expires_at,
                    ),
                )
                conn.commit()
        return undo_id

    def _notice_undo_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
        payload = self._loads(str(row["payload_json"] or ""), {})
        payload = payload if isinstance(payload, dict) else {}
        payload.update(
            {
                "undo_id": str(row["undo_id"] or ""),
                "identity_key": str(row["identity_key"] or ""),
                "status": str(row["status"] or ""),
                "action_type": str(row["action_type"] or ""),
                "scope": str(row["scope"] or ""),
                "work_type": str(row["work_type"] or ""),
                "notice_type": str(row["notice_type"] or ""),
                "active_item_id": str(row["active_item_id"] or ""),
                "source_record_id": str(row["source_record_id"] or ""),
                "target_record_id": str(row["target_record_id"] or ""),
                "title": str(row["title"] or ""),
                "error": str(row["error"] or ""),
                "created_at": float(row["created_at"] or 0),
                "updated_at": float(row["updated_at"] or 0),
                "applied_at": (
                    float(row["applied_at"] or 0)
                    if row["applied_at"] is not None
                    else 0.0
                ),
                "expires_at": float(row["expires_at"] or 0),
            }
        )
        return payload

    def get_notice_undo_action(self, undo_id: str) -> dict[str, Any] | None:
        undo_id = self._text(undo_id)
        if not undo_id or not self.db_path.exists():
            return None
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(
                    """
                    SELECT *
                    FROM notice_undo_actions
                    WHERE undo_id = ?
                    """,
                    (undo_id,),
                ).fetchone()
        return self._notice_undo_from_row(row) if row else None

    def list_notice_undo_actions(
        self,
        *,
        status: str = "available",
        scope: str = "",
        include_expired: bool = False,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        if not self.db_path.exists():
            return []
        status = self._text(status) or "available"
        scope = self._text(scope)
        now = time.time()
        clauses = ["status = ?"]
        params: list[Any] = [status]
        if scope:
            clauses.append("(scope = ? OR scope = 'ALL' OR scope = '')")
            params.append(scope)
        if not include_expired:
            clauses.append("expires_at > ?")
            params.append(now)
        params.append(max(1, min(1000, int(limit or 200))))
        where_sql = " AND ".join(clauses)
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    f"""
                    SELECT *
                    FROM notice_undo_actions
                    WHERE {where_sql}
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    tuple(params),
                ).fetchall()
        return [self._notice_undo_from_row(row) for row in rows]

    def mark_notice_undo_action(
        self,
        undo_id: str,
        status: str,
        *,
        error: str = "",
        payload_patch: dict[str, Any] | None = None,
    ) -> bool:
        undo_id = self._text(undo_id)
        status = self._text(status)
        if not undo_id or not status:
            return False
        now = time.time()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(
                    "SELECT payload_json FROM notice_undo_actions WHERE undo_id = ?",
                    (undo_id,),
                ).fetchone()
                if not row:
                    return False
                payload = self._loads(str(row["payload_json"] or ""), {})
                payload = payload if isinstance(payload, dict) else {}
                if isinstance(payload_patch, dict):
                    payload.update(payload_patch)
                payload["status"] = status
                payload["updated_at"] = now
                payload["error"] = self._text(error)
                applied_at = now if status == "undone" else None
                conn.execute(
                    """
                    UPDATE notice_undo_actions
                    SET status = ?, error = ?, payload_json = ?, updated_at = ?,
                        applied_at = COALESCE(?, applied_at)
                    WHERE undo_id = ?
                    """,
                    (
                        status,
                        self._text(error),
                        self._json(payload),
                        now,
                        applied_at,
                        undo_id,
                    ),
                )
                conn.commit()
        return True

    def cleanup_notice_undo_actions(self, *, retain_days: int = 7) -> int:
        if not self.db_path.exists():
            return 0
        cutoff = time.time() - max(1, int(retain_days or 7)) * 24 * 60 * 60
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                cursor = conn.execute(
                    """
                    DELETE FROM notice_undo_actions
                    WHERE updated_at < ?
                       OR (status = 'available' AND expires_at < ?)
                    """,
                    (cutoff, time.time()),
                )
                conn.commit()
                return int(cursor.rowcount or 0)

    def replace_qt_active_items_from_payload(
        self, payload: dict[str, Any] | None
    ) -> dict[str, Any]:
        payload = payload if isinstance(payload, dict) else {}
        sections = ("event", "other")
        raw_items: list[dict[str, Any]] = []
        now = time.time()
        for section in sections:
            section_items = payload.get(section, [])
            if not isinstance(section_items, list):
                continue
            for sort_order, entry in enumerate(section_items):
                if not isinstance(entry, dict):
                    continue
                data = entry.get("data")
                if not isinstance(data, dict):
                    continue
                data, _ = self._canonicalize_legacy_change_notice_payload(data)
                data = data if isinstance(data, dict) else {}
                normalized = normalize_notice_identity_payload(data)
                active_item_id = self._qt_active_item_key(normalized)
                if not active_item_id:
                    continue
                normalized.setdefault("active_item_id", active_item_id)
                if self._text(normalized.get("active_item_id")) != active_item_id:
                    normalized["active_item_id"] = active_item_id
                notice_type = self._text(normalized.get("notice_type"))
                origin = self._text(normalized.get("origin"))
                if not origin and bool(normalized.get("lan_created_from_portal")):
                    origin = "portal"
                raw_items.append(
                    {
                        "active_item_id": active_item_id,
                        "record_id": canonical_target_record_id(normalized),
                        "notice_type": notice_type,
                        "section": self._normalize_qt_active_section(section),
                        "sort_order": int(sort_order),
                        "origin": origin,
                        "payload": normalized,
                        "updated_at": now,
                        "deleted_at": None,
                    }
                )
        rows: list[tuple[str, str, str, str, int, str, str, float]] = []
        identity_payloads: list[tuple[dict[str, Any], str]] = []
        seen: set[str] = set()
        for item in raw_items:
            active_item_id = self._text(item.get("active_item_id"))
            if not active_item_id:
                continue
            normalized = normalize_notice_identity_payload(
                item.get("payload") if isinstance(item.get("payload"), dict) else {}
            )
            normalized["active_item_id"] = active_item_id
            record_id = self._text(item.get("record_id")) or canonical_target_record_id(normalized)
            if record_id:
                normalized.setdefault("target_record_id", record_id)
                normalized.setdefault("record_id", record_id)
            seen.add(active_item_id)
            origin = self._text(item.get("origin"))
            identity_payloads.append((dict(normalized), origin))
            rows.append(
                (
                    active_item_id,
                    record_id,
                    self._text(item.get("notice_type")),
                    self._normalize_qt_active_section(self._text(item.get("section"))),
                    int(item.get("sort_order") or 0),
                    origin,
                    self._json(normalized),
                    now,
                )
            )
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                for row in rows:
                    conn.execute(
                        """
                        INSERT INTO qt_active_items(
                            active_item_id, record_id, notice_type, section, sort_order,
                            origin, payload_json, updated_at, deleted_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
                        ON CONFLICT(active_item_id) DO UPDATE SET
                            record_id = excluded.record_id,
                            notice_type = excluded.notice_type,
                            section = excluded.section,
                            sort_order = excluded.sort_order,
                            origin = excluded.origin,
                            payload_json = excluded.payload_json,
                            updated_at = excluded.updated_at,
                            deleted_at = NULL
                        """,
                        row,
                    )
                for identity_payload, origin in identity_payloads:
                    self._upsert_notice_identity_locked(
                        conn,
                        identity_payload,
                        origin=origin,
                    )
                if seen:
                    placeholders = ",".join("?" for _ in seen)
                    conn.execute(
                        f"""
                        UPDATE qt_active_items
                        SET deleted_at = ?, updated_at = ?
                        WHERE deleted_at IS NULL
                          AND active_item_id NOT IN ({placeholders})
                        """,
                        (now, now, *sorted(seen)),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE qt_active_items
                        SET deleted_at = ?, updated_at = ?
                        WHERE deleted_at IS NULL
                        """,
                        (now, now),
                    )
                conn.commit()
        return {
            "upserted": len(rows),
            "active": len(seen),
            "deduped": max(0, len(raw_items) - len(rows)),
            "updated_at": now,
        }

    def qt_active_items_stats(self) -> dict[str, Any]:
        if not self.db_path.exists():
            return {"active": 0, "deleted": 0, "by_notice_type": {}, "checked_at": time.time()}
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    """
                    SELECT
                        SUM(CASE WHEN deleted_at IS NULL THEN 1 ELSE 0 END) AS active_count,
                        SUM(CASE WHEN deleted_at IS NULL THEN 0 ELSE 1 END) AS deleted_count
                    FROM qt_active_items
                    """
                ).fetchone()
                type_rows = conn.execute(
                    """
                    SELECT notice_type, COUNT(*) AS count
                    FROM qt_active_items
                    WHERE deleted_at IS NULL
                    GROUP BY notice_type
                    """
                ).fetchall()
        return {
            "active": int(rows["active_count"] or 0) if rows else 0,
            "deleted": int(rows["deleted_count"] or 0) if rows else 0,
            "by_notice_type": {
                str(row["notice_type"] or ""): int(row["count"] or 0)
                for row in type_rows
            },
            "checked_at": time.time(),
        }

    def qt_active_items_meta(self) -> dict[str, Any]:
        if not self.db_path.exists():
            return {"active": 0, "deleted": 0, "updated_at": 0.0}
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(
                    """
                    SELECT
                        SUM(CASE WHEN deleted_at IS NULL THEN 1 ELSE 0 END) AS active_count,
                        SUM(CASE WHEN deleted_at IS NULL THEN 0 ELSE 1 END) AS deleted_count,
                        MAX(updated_at) AS max_updated_at
                    FROM qt_active_items
                    """
                ).fetchone()
        return {
            "active": int(row["active_count"] or 0) if row else 0,
            "deleted": int(row["deleted_count"] or 0) if row else 0,
            "updated_at": float(row["max_updated_at"] or 0) if row else 0.0,
        }

    def upsert_clipboard_candidate(
        self,
        candidate_id: str,
        *,
        content: str,
        payload: dict[str, Any] | None = None,
        status: str = "pending",
        source_event_id: int | None = None,
    ) -> bool:
        candidate_id = self._text(candidate_id)
        if not candidate_id:
            return False
        now = time.time()
        normalized = dict(payload or {})
        normalized.setdefault("candidate_id", candidate_id)
        normalized.setdefault("content", str(content or ""))
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute(
                    """
                    INSERT OR REPLACE INTO clipboard_candidates(
                        candidate_id, status, content, payload_json, source_event_id, created_at, updated_at
                    )
                    VALUES (
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        COALESCE((SELECT created_at FROM clipboard_candidates WHERE candidate_id = ?), ?),
                        ?
                    )
                    """,
                    (
                        candidate_id,
                        self._text(status) or "pending",
                        str(content or ""),
                        self._json(normalized),
                        int(source_event_id or 0) if source_event_id is not None else None,
                        candidate_id,
                        now,
                        now,
                    ),
                )
                conn.commit()
        return True

    def list_clipboard_candidates(
        self, *, status: str = "pending", limit: int = 100
    ) -> list[dict[str, Any]]:
        if not self.db_path.exists():
            return []
        limit = max(1, min(int(limit or 100), 500))
        status = self._text(status)
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                if status:
                    rows = conn.execute(
                        """
                        SELECT candidate_id, status, content, payload_json, source_event_id, created_at, updated_at
                        FROM clipboard_candidates
                        WHERE status = ?
                        ORDER BY updated_at ASC
                        LIMIT ?
                        """,
                        (status, limit),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT candidate_id, status, content, payload_json, source_event_id, created_at, updated_at
                        FROM clipboard_candidates
                        ORDER BY updated_at ASC
                        LIMIT ?
                        """,
                        (limit,),
                    ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            payload = self._loads(str(row["payload_json"] or ""), {})
            result.append(
                {
                    "candidate_id": str(row["candidate_id"] or ""),
                    "status": str(row["status"] or ""),
                    "content": str(row["content"] or ""),
                    "payload": payload if isinstance(payload, dict) else {},
                    "source_event_id": int(row["source_event_id"] or 0),
                    "created_at": float(row["created_at"] or 0),
                    "updated_at": float(row["updated_at"] or 0),
                }
            )
        return result

    def mark_clipboard_candidate(
        self, candidate_id: str, status: str, *, payload: dict[str, Any] | None = None
    ) -> bool:
        candidate_id = self._text(candidate_id)
        status = self._text(status)
        if not candidate_id or not status or not self.db_path.exists():
            return False
        now = time.time()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                current = conn.execute(
                    "SELECT payload_json FROM clipboard_candidates WHERE candidate_id = ?",
                    (candidate_id,),
                ).fetchone()
                if not current:
                    return False
                current_payload = self._loads(str(current["payload_json"] or ""), {})
                if not isinstance(current_payload, dict):
                    current_payload = {}
                if isinstance(payload, dict):
                    current_payload.update(payload)
                cursor = conn.execute(
                    """
                    UPDATE clipboard_candidates
                    SET status = ?, payload_json = ?, updated_at = ?
                    WHERE candidate_id = ?
                    """,
                    (status, self._json(current_payload), now, candidate_id),
                )
                conn.commit()
                return bool(cursor.rowcount)

    def cleanup_clipboard_candidates(
        self,
        *,
        done_retention_seconds: int = 3 * 24 * 3600,
        pending_retention_seconds: int = 7 * 24 * 3600,
        max_delete: int = 1000,
    ) -> dict[str, int]:
        done_retention_seconds = max(60, int(done_retention_seconds or 0))
        pending_retention_seconds = max(60, int(pending_retention_seconds or 0))
        max_delete = max(1, min(int(max_delete or 1000), 5000))
        now = time.time()
        removed_terminal = 0
        removed_pending = 0
        terminal_statuses = {"done", "failed", "cancelled", "ignored", "skipped"}
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    """
                    SELECT candidate_id, status, updated_at
                    FROM clipboard_candidates
                    ORDER BY updated_at ASC
                    LIMIT ?
                    """,
                    (max_delete,),
                ).fetchall()
                for row in rows:
                    status = self._text(row["status"])
                    updated_at = float(row["updated_at"] or 0)
                    retention = (
                        done_retention_seconds
                        if status in terminal_statuses
                        else pending_retention_seconds
                    )
                    if updated_at and now - updated_at < retention:
                        continue
                    conn.execute(
                        "DELETE FROM clipboard_candidates WHERE candidate_id = ?",
                        (str(row["candidate_id"] or ""),),
                    )
                    if status in terminal_statuses:
                        removed_terminal += 1
                    else:
                        removed_pending += 1
                conn.commit()
        return {
            "removed_terminal": removed_terminal,
            "removed_pending": removed_pending,
            "removed_total": removed_terminal + removed_pending,
        }

    def upsert_dialog_session(
        self,
        session_id: str,
        *,
        session_type: str,
        payload: dict[str, Any] | None = None,
        status: str = "pending",
    ) -> bool:
        session_id = self._text(session_id)
        if not session_id:
            return False
        now = time.time()
        normalized = dict(payload or {})
        normalized.setdefault("session_id", session_id)
        normalized.setdefault("session_type", str(session_type or "generic"))
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute(
                    """
                    INSERT OR REPLACE INTO dialog_sessions(
                        session_id, session_type, status, payload_json, created_at, updated_at
                    )
                    VALUES (
                        ?,
                        ?,
                        ?,
                        ?,
                        COALESCE((SELECT created_at FROM dialog_sessions WHERE session_id = ?), ?),
                        ?
                    )
                    """,
                    (
                        session_id,
                        str(session_type or "generic"),
                        self._text(status) or "pending",
                        self._json(normalized),
                        session_id,
                        now,
                        now,
                    ),
                )
                conn.commit()
        return True

    def get_dialog_session(self, session_id: str) -> dict[str, Any] | None:
        session_id = self._text(session_id)
        if not session_id or not self.db_path.exists():
            return None
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(
                    """
                    SELECT session_id, session_type, status, payload_json, created_at, updated_at
                    FROM dialog_sessions
                    WHERE session_id = ?
                    """,
                    (session_id,),
                ).fetchone()
        if not row:
            return None
        payload = self._loads(str(row["payload_json"] or ""), {})
        return {
            "session_id": str(row["session_id"] or ""),
            "session_type": str(row["session_type"] or ""),
            "status": str(row["status"] or ""),
            "payload": payload if isinstance(payload, dict) else {},
            "created_at": float(row["created_at"] or 0),
            "updated_at": float(row["updated_at"] or 0),
        }

    def list_dialog_sessions(
        self, *, status: str = "pending", limit: int = 100
    ) -> list[dict[str, Any]]:
        if not self.db_path.exists():
            return []
        limit = max(1, min(int(limit or 100), 500))
        status = self._text(status)
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                if status:
                    rows = conn.execute(
                        """
                        SELECT session_id, session_type, status, payload_json, created_at, updated_at
                        FROM dialog_sessions
                        WHERE status = ?
                        ORDER BY updated_at ASC
                        LIMIT ?
                        """,
                        (status, limit),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT session_id, session_type, status, payload_json, created_at, updated_at
                        FROM dialog_sessions
                        ORDER BY updated_at ASC
                        LIMIT ?
                        """,
                        (limit,),
                    ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            payload = self._loads(str(row["payload_json"] or ""), {})
            result.append(
                {
                    "session_id": str(row["session_id"] or ""),
                    "session_type": str(row["session_type"] or ""),
                    "status": str(row["status"] or ""),
                    "payload": payload if isinstance(payload, dict) else {},
                    "created_at": float(row["created_at"] or 0),
                    "updated_at": float(row["updated_at"] or 0),
                }
            )
        return result

    def mark_dialog_session(
        self, session_id: str, status: str, *, payload: dict[str, Any] | None = None
    ) -> bool:
        session_id = self._text(session_id)
        status = self._text(status)
        if not session_id or not status or not self.db_path.exists():
            return False
        now = time.time()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                current = conn.execute(
                    "SELECT payload_json FROM dialog_sessions WHERE session_id = ?",
                    (session_id,),
                ).fetchone()
                if not current:
                    return False
                current_payload = self._loads(str(current["payload_json"] or ""), {})
                if not isinstance(current_payload, dict):
                    current_payload = {}
                if isinstance(payload, dict):
                    current_payload.update(payload)
                cursor = conn.execute(
                    """
                    UPDATE dialog_sessions
                    SET status = ?, payload_json = ?, updated_at = ?
                    WHERE session_id = ?
                    """,
                    (status, self._json(current_payload), now, session_id),
                )
                conn.commit()
                return bool(cursor.rowcount)

    def cleanup_dialog_sessions(
        self,
        *,
        done_retention_seconds: int = 3 * 24 * 3600,
        pending_retention_seconds: int = 7 * 24 * 3600,
        max_delete: int = 1000,
    ) -> dict[str, int]:
        done_retention_seconds = max(60, int(done_retention_seconds or 0))
        pending_retention_seconds = max(60, int(pending_retention_seconds or 0))
        max_delete = max(1, min(int(max_delete or 1000), 5000))
        now = time.time()
        removed_terminal = 0
        removed_pending = 0
        terminal_statuses = {"completed", "done", "failed", "cancelled", "expired"}
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    """
                    SELECT session_id, status, updated_at
                    FROM dialog_sessions
                    ORDER BY updated_at ASC
                    LIMIT ?
                    """,
                    (max_delete,),
                ).fetchall()
                for row in rows:
                    status = self._text(row["status"])
                    updated_at = float(row["updated_at"] or 0)
                    retention = (
                        done_retention_seconds
                        if status in terminal_statuses
                        else pending_retention_seconds
                    )
                    if updated_at and now - updated_at < retention:
                        continue
                    conn.execute(
                        "DELETE FROM dialog_sessions WHERE session_id = ?",
                        (str(row["session_id"] or ""),),
                    )
                    if status in terminal_statuses:
                        removed_terminal += 1
                    else:
                        removed_pending += 1
                conn.commit()
        return {
            "removed_terminal": removed_terminal,
            "removed_pending": removed_pending,
            "removed_total": removed_terminal + removed_pending,
        }

    def get_settings(self) -> dict[str, Any] | None:
        if not self.db_path.exists():
            return None
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    "SELECT key, value_json FROM settings ORDER BY key ASC"
                ).fetchall()
        if not rows:
            return None
        result: dict[str, Any] = {}
        for row in rows:
            result[str(row["key"] or "")] = self._loads(
                str(row["value_json"] or ""), None
            )
        return result

    def put_settings(self, values: dict[str, Any]) -> None:
        if not isinstance(values, dict):
            return
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                now = time.time()
                for key, value in values.items():
                    text_key = self._text(key)
                    if not text_key:
                        continue
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO settings(key, value_json, updated_at)
                        VALUES (?, ?, ?)
                        """,
                        (text_key, self._json(value), now),
                    )
                conn.commit()

    @staticmethod
    def _signature_link_token_hash(token: str) -> str:
        return hashlib.sha256(str(token or "").encode("utf-8")).hexdigest()

    def create_signature_link_token(
        self,
        *,
        record_id: str,
        created_by: str = "",
        ttl_seconds: int = 3600,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        record_id = self._text(record_id)
        if not record_id:
            raise ValueError("签名链接缺少人员记录 ID。")
        ttl_seconds = max(300, min(int(ttl_seconds or 3600), 24 * 3600))
        token = secrets.token_urlsafe(32)
        token_hash = self._signature_link_token_hash(token)
        now = time.time()
        expires_at = now + ttl_seconds
        token_id = uuid.uuid4().hex
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "DELETE FROM signature_link_tokens WHERE expires_at < ?",
                    (now - 24 * 3600,),
                )
                conn.execute(
                    """
                    INSERT INTO signature_link_tokens(
                        token_id, record_id, token_hash, created_by,
                        payload_json, created_at, expires_at, used_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
                    """,
                    (
                        token_id,
                        record_id,
                        token_hash,
                        self._text(created_by),
                        self._json(payload or {}),
                        now,
                        expires_at,
                    ),
                )
                conn.commit()
        return {
            "token_id": token_id,
            "record_id": record_id,
            "token": token,
            "expires_at": expires_at,
            "created_at": now,
        }

    def validate_signature_link_token(self, *, record_id: str, token: str) -> bool:
        record_id = self._text(record_id)
        token = str(token or "").strip()
        if not record_id or not token:
            return False
        token_hash = self._signature_link_token_hash(token)
        now = time.time()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(
                    """
                    SELECT record_id, expires_at
                    FROM signature_link_tokens
                    WHERE token_hash = ?
                    """,
                    (token_hash,),
                ).fetchone()
        if not row:
            return False
        return self._text(row["record_id"]) == record_id and float(row["expires_at"] or 0) >= now

    def mark_signature_link_token_used(self, *, record_id: str, token: str) -> None:
        record_id = self._text(record_id)
        token = str(token or "").strip()
        if not record_id or not token:
            return
        token_hash = self._signature_link_token_hash(token)
        now = time.time()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute(
                    """
                    UPDATE signature_link_tokens
                    SET used_at = COALESCE(used_at, ?)
                    WHERE record_id = ? AND token_hash = ? AND expires_at >= ?
                    """,
                    (now, record_id, token_hash, now),
                )
                conn.commit()

    def create_mop_signature_usage_confirmation(
        self,
        *,
        scope: str,
        notice_key: str,
        role: str,
        signer_record_id: str,
        signer_open_id: str,
        signer_name: str = "",
        requested_by_openid: str = "",
        requested_by_name: str = "",
        ttl_seconds: int = 7 * 24 * 3600,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        role = self._text(role)
        signer_record_id = self._text(signer_record_id)
        signer_open_id = self._text(signer_open_id)
        if role not in {"implementer", "auditor"}:
            raise ValueError("签名确认角色无效。")
        if not signer_record_id or not signer_open_id:
            raise ValueError("签名确认缺少人员信息。")
        ttl_seconds = max(3600, min(int(ttl_seconds or 0), 30 * 24 * 3600))
        token = secrets.token_urlsafe(32)
        token_hash = self._signature_link_token_hash(token)
        confirmation_id = uuid.uuid4().hex
        now = time.time()
        expires_at = now + ttl_seconds
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "DELETE FROM mop_signature_usage_confirmations WHERE expires_at < ? AND status != 'confirmed'",
                    (now - 24 * 3600,),
                )
                conn.execute(
                    """
                    INSERT INTO mop_signature_usage_confirmations(
                        confirmation_id, scope, notice_key, role,
                        signer_record_id, signer_open_id, signer_name,
                        requested_by_openid, requested_by_name, status,
                        token_hash, payload_json, created_at, expires_at, confirmed_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, NULL)
                    """,
                    (
                        confirmation_id,
                        self._text(scope),
                        self._text(notice_key),
                        role,
                        signer_record_id,
                        signer_open_id,
                        self._text(signer_name),
                        self._text(requested_by_openid),
                        self._text(requested_by_name),
                        token_hash,
                        self._json(payload or {}),
                        now,
                        expires_at,
                    ),
                )
                conn.commit()
        return {
            "confirmation_id": confirmation_id,
            "token": token,
            "expires_at": expires_at,
            "created_at": now,
        }

    def _mop_signature_usage_result(self, row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        result = dict(row)
        result["payload"] = self._loads(self._text(result.get("payload_json")), {})
        return result

    def get_mop_signature_usage_confirmation(self, *, token: str) -> dict[str, Any]:
        token_hash = self._signature_link_token_hash(token)
        if not token_hash:
            raise ValueError("签名确认链接无效。")
        now = time.time()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(
                    """
                    SELECT * FROM mop_signature_usage_confirmations
                    WHERE token_hash = ?
                    """,
                    (token_hash,),
                ).fetchone()
        if not row:
            raise ValueError("签名确认链接无效或已过期。")
        status = self._text(row["status"]) or "pending"
        if status == "pending" and float(row["expires_at"] or 0) < now:
            raise ValueError("签名确认链接已过期。")
        return self._mop_signature_usage_result(row)

    def decide_mop_signature_usage(self, *, token: str, decision: str) -> dict[str, Any]:
        decision_text = self._text(decision).lower()
        if decision_text in {"confirm", "confirmed", "approve", "approved", "yes", "allow"}:
            final_status = "confirmed"
        elif decision_text in {"reject", "rejected", "deny", "denied", "no", "refuse"}:
            final_status = "rejected"
        else:
            raise ValueError("签名使用确认动作无效。")
        token_hash = self._signature_link_token_hash(token)
        if not token_hash:
            raise ValueError("签名确认链接无效。")
        now = time.time()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    """
                    SELECT * FROM mop_signature_usage_confirmations
                    WHERE token_hash = ?
                    """,
                    (token_hash,),
                ).fetchone()
                if not row:
                    conn.rollback()
                    raise ValueError("签名确认链接无效或已过期。")
                current_status = self._text(row["status"]) or "pending"
                if current_status == "pending" and float(row["expires_at"] or 0) < now:
                    conn.rollback()
                    raise ValueError("签名确认链接已过期。")
                if current_status == "pending":
                    conn.execute(
                        """
                        UPDATE mop_signature_usage_confirmations
                        SET status = ?, confirmed_at = COALESCE(confirmed_at, ?)
                        WHERE status = 'pending'
                          AND scope = ?
                          AND notice_key = ?
                          AND signer_record_id = ?
                          AND requested_by_openid = ?
                        """,
                        (
                            final_status,
                            now,
                            self._text(row["scope"]),
                            self._text(row["notice_key"]),
                            self._text(row["signer_record_id"]),
                            self._text(row["requested_by_openid"]),
                        ),
                    )
                conn.commit()
        result = self._mop_signature_usage_result(row)
        result["status"] = current_status if current_status != "pending" else final_status
        result["confirmed_at"] = result.get("confirmed_at") or now
        return result

    def confirm_mop_signature_usage(self, *, token: str) -> dict[str, Any]:
        return self.decide_mop_signature_usage(token=token, decision="confirmed")

    def reject_mop_signature_usage(self, *, token: str) -> dict[str, Any]:
        return self.decide_mop_signature_usage(token=token, decision="rejected")

    def mop_signature_usage_status(
        self,
        *,
        scope: str,
        notice_key: str,
        signer_record_id: str,
        requested_by_openid: str = "",
    ) -> str:
        signer_record_id = self._text(signer_record_id)
        if not signer_record_id:
            return ""
        now = time.time()
        clauses = [
            "scope = ?",
            "notice_key = ?",
            "signer_record_id = ?",
            "expires_at >= ?",
        ]
        params: list[Any] = [
            self._text(scope),
            self._text(notice_key),
            signer_record_id,
            now,
        ]
        if self._text(requested_by_openid):
            clauses.append("requested_by_openid = ?")
            params.append(self._text(requested_by_openid))
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(
                    f"""
                    SELECT status FROM mop_signature_usage_confirmations
                    WHERE {' AND '.join(clauses)}
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    params,
                ).fetchone()
        return self._text(row["status"]) if row else ""

    def has_confirmed_mop_signature_usage(
        self,
        *,
        scope: str,
        notice_key: str,
        signer_record_id: str,
        requested_by_openid: str = "",
    ) -> bool:
        signer_record_id = self._text(signer_record_id)
        if not signer_record_id:
            return False
        now = time.time()
        clauses = [
            "scope = ?",
            "notice_key = ?",
            "signer_record_id = ?",
            "status = 'confirmed'",
            "expires_at >= ?",
        ]
        params: list[Any] = [
            self._text(scope),
            self._text(notice_key),
            signer_record_id,
            now,
        ]
        if self._text(requested_by_openid):
            clauses.append("requested_by_openid = ?")
            params.append(self._text(requested_by_openid))
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(
                    f"""
                    SELECT confirmation_id FROM mop_signature_usage_confirmations
                    WHERE {' AND '.join(clauses)}
                    ORDER BY confirmed_at DESC
                    LIMIT 1
                    """,
                    params,
                ).fetchone()
        return bool(row)

    def create_mop_temporary_signature_session(
        self,
        *,
        scope: str,
        notice_key: str,
        role: str,
        display_name: str,
        recipient_open_ids: list[str],
        created_by: str = "",
        ttl_seconds: int = 3600,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        role = self._text(role)
        if role not in {"implementer", "auditor"}:
            raise ValueError("临时签名角色无效。")
        display_name = self._text(display_name) or "临时人员"
        ttl_seconds = max(300, min(int(ttl_seconds or 3600), 24 * 3600))
        token = secrets.token_urlsafe(32)
        token_hash = self._signature_link_token_hash(token)
        temp_id = uuid.uuid4().hex
        now = time.time()
        expires_at = now + ttl_seconds
        recipients = [
            self._text(item)
            for item in (recipient_open_ids or [])
            if self._text(item)
        ]
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "DELETE FROM mop_temporary_signature_sessions WHERE expires_at < ? AND status != 'signed'",
                    (now - 24 * 3600,),
                )
                conn.execute(
                    """
                    INSERT INTO mop_temporary_signature_sessions(
                        temp_id, scope, notice_key, role, display_name,
                        recipient_open_ids_json, status, token_hash, expires_at,
                        temporary_record_id, signature_file_token, created_by,
                        payload_json, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, '', '', ?, ?, ?, ?)
                    """,
                    (
                        temp_id,
                        self._text(scope),
                        self._text(notice_key),
                        role,
                        display_name,
                        self._json(recipients),
                        token_hash,
                        expires_at,
                        self._text(created_by),
                        self._json(payload or {}),
                        now,
                        now,
                    ),
                )
                conn.commit()
        return {
            "temp_id": temp_id,
            "token": token,
            "scope": self._text(scope),
            "notice_key": self._text(notice_key),
            "role": role,
            "display_name": display_name,
            "recipient_open_ids": recipients,
            "status": "pending",
            "expires_at": expires_at,
            "created_at": now,
        }

    def _mop_temp_signature_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
        payload = self._loads(str(row["payload_json"] or ""), {})
        recipients = self._loads(str(row["recipient_open_ids_json"] or "[]"), [])
        return {
            "temp_id": self._text(row["temp_id"]),
            "scope": self._text(row["scope"]),
            "notice_key": self._text(row["notice_key"]),
            "role": self._text(row["role"]),
            "display_name": self._text(row["display_name"]),
            "recipient_open_ids": recipients if isinstance(recipients, list) else [],
            "status": self._text(row["status"]),
            "expires_at": float(row["expires_at"] or 0),
            "temporary_record_id": self._text(row["temporary_record_id"]),
            "signature_file_token": self._text(row["signature_file_token"]),
            "created_by": self._text(row["created_by"]),
            "payload": payload if isinstance(payload, dict) else {},
            "created_at": float(row["created_at"] or 0),
            "updated_at": float(row["updated_at"] or 0),
        }

    def get_mop_temporary_signature_session(
        self,
        *,
        temp_id: str,
        token: str = "",
        require_valid_token: bool = False,
    ) -> dict[str, Any] | None:
        temp_id = self._text(temp_id)
        token = str(token or "").strip()
        if not temp_id:
            return None
        token_hash = self._signature_link_token_hash(token) if token else ""
        now = time.time()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(
                    """
                    SELECT *
                    FROM mop_temporary_signature_sessions
                    WHERE temp_id = ?
                    """,
                    (temp_id,),
                ).fetchone()
        if not row:
            return None
        if require_valid_token:
            if not token_hash or self._text(row["token_hash"]) != token_hash:
                return None
            if float(row["expires_at"] or 0) < now:
                return None
        return self._mop_temp_signature_from_row(row)

    def refresh_mop_temporary_signature_session_token(
        self,
        *,
        temp_id: str,
        ttl_seconds: int = 3600,
    ) -> dict[str, Any]:
        temp_id = self._text(temp_id)
        if not temp_id:
            raise ValueError("临时签名记录缺少 ID。")
        ttl_seconds = max(300, min(int(ttl_seconds or 3600), 24 * 3600))
        token = secrets.token_urlsafe(32)
        token_hash = self._signature_link_token_hash(token)
        now = time.time()
        expires_at = now + ttl_seconds
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(
                    "SELECT * FROM mop_temporary_signature_sessions WHERE temp_id = ?",
                    (temp_id,),
                ).fetchone()
                if not row:
                    raise ValueError("临时签名记录不存在。")
                conn.execute(
                    """
                    UPDATE mop_temporary_signature_sessions
                    SET token_hash = ?,
                        expires_at = ?,
                        updated_at = ?
                    WHERE temp_id = ?
                    """,
                    (token_hash, expires_at, now, temp_id),
                )
                conn.commit()
        session = self.get_mop_temporary_signature_session(temp_id=temp_id) or {}
        return {**session, "token": token}

    def list_mop_temporary_signature_sessions(
        self,
        *,
        scope: str = "",
        notice_key: str = "",
        created_by: str = "",
        include_expired: bool = False,
    ) -> list[dict[str, Any]]:
        now = time.time()
        clauses = ["1=1"]
        params: list[Any] = []
        scope = self._text(scope)
        notice_key = self._text(notice_key)
        created_by = self._text(created_by)
        if scope:
            clauses.append("scope = ?")
            params.append(scope)
        if notice_key:
            clauses.append("notice_key = ?")
            params.append(notice_key)
        if created_by:
            clauses.append("(created_by = ? OR created_by = '')")
            params.append(created_by)
        if not include_expired:
            clauses.append("(expires_at >= ? OR status = 'signed')")
            params.append(now)
        sql = (
            "SELECT * FROM mop_temporary_signature_sessions WHERE "
            + " AND ".join(clauses)
            + " ORDER BY created_at"
        )
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(sql, params).fetchall()
        return [self._mop_temp_signature_from_row(row) for row in rows]

    def update_mop_temporary_signature_session(
        self,
        *,
        temp_id: str,
        status: str = "",
        temporary_record_id: str = "",
        signature_file_token: str = "",
        payload_patch: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        temp_id = self._text(temp_id)
        if not temp_id:
            return None
        now = time.time()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(
                    "SELECT * FROM mop_temporary_signature_sessions WHERE temp_id = ?",
                    (temp_id,),
                ).fetchone()
                if not row:
                    return None
                payload = self._loads(str(row["payload_json"] or ""), {})
                if not isinstance(payload, dict):
                    payload = {}
                if isinstance(payload_patch, dict):
                    payload.update(payload_patch)
                next_status = self._text(status) or self._text(row["status"])
                next_record_id = self._text(temporary_record_id) or self._text(row["temporary_record_id"])
                next_file_token = self._text(signature_file_token) or self._text(row["signature_file_token"])
                conn.execute(
                    """
                    UPDATE mop_temporary_signature_sessions
                    SET status = ?,
                        temporary_record_id = ?,
                        signature_file_token = ?,
                        payload_json = ?,
                        updated_at = ?
                    WHERE temp_id = ?
                    """,
                    (
                        next_status,
                        next_record_id,
                        next_file_token,
                        self._json(payload),
                        now,
                        temp_id,
                    ),
                )
                conn.commit()
        return self.get_mop_temporary_signature_session(temp_id=temp_id)

    def cleanup_mop_temporary_signature_sessions(
        self,
        *,
        signed_retention_seconds: int = 30 * 24 * 3600,
        max_delete: int = 500,
    ) -> int:
        signed_retention_seconds = max(
            24 * 3600,
            min(int(signed_retention_seconds or 0), 180 * 24 * 3600),
        )
        max_delete = max(1, min(int(max_delete or 0), 5000))
        now = time.time()
        signed_cutoff = now - signed_retention_seconds
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                cursor = conn.execute(
                    """
                    DELETE FROM mop_temporary_signature_sessions
                    WHERE rowid IN (
                        SELECT rowid
                        FROM mop_temporary_signature_sessions
                        WHERE (status != 'signed' AND expires_at < ?)
                           OR (status = 'signed' AND updated_at < ?)
                        ORDER BY updated_at
                        LIMIT ?
                    )
                    """,
                    (now, signed_cutoff, max_delete),
                )
                removed = int(cursor.rowcount or 0)
                conn.commit()
        return removed

    def upsert_signature_crypto_migration(
        self,
        *,
        table_id: str,
        record_id: str,
        status: str,
        error: str = "",
        payload: dict[str, Any] | None = None,
    ) -> None:
        table_id = self._text(table_id)
        record_id = self._text(record_id)
        status = self._text(status) or "unknown"
        if not table_id or not record_id:
            return
        now = time.time()
        payload = dict(payload or {})
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(
                    """
                    SELECT attempts, created_at
                    FROM signature_crypto_migrations
                    WHERE table_id = ? AND record_id = ?
                    """,
                    (table_id, record_id),
                ).fetchone()
                attempts = int(row["attempts"] or 0) if row else 0
                created_at = float(row["created_at"] or now) if row else now
                if status in {"migrating", "failed"}:
                    attempts += 1
                conn.execute(
                    """
                    INSERT OR REPLACE INTO signature_crypto_migrations(
                        table_id, record_id, status, attempts, error, payload_json, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        table_id,
                        record_id,
                        status,
                        attempts,
                        self._text(error),
                        self._json(payload),
                        created_at,
                        now,
                    ),
                )
                conn.commit()

    def signature_crypto_migration_summary(self) -> dict[str, Any]:
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    """
                    SELECT status, COUNT(*) AS count
                    FROM signature_crypto_migrations
                    GROUP BY status
                    """
                ).fetchall()
                recent = conn.execute(
                    """
                    SELECT table_id, record_id, status, error, updated_at
                    FROM signature_crypto_migrations
                    ORDER BY updated_at DESC
                    LIMIT 10
                    """
                ).fetchall()
        counts = {str(row["status"] or ""): int(row["count"] or 0) for row in rows}
        return {
            "counts": counts,
            "recent": [
                {
                    "table_id": str(row["table_id"] or ""),
                    "record_id": str(row["record_id"] or ""),
                    "status": str(row["status"] or ""),
                    "error": str(row["error"] or ""),
                    "updated_at": float(row["updated_at"] or 0.0),
                }
                for row in recent
            ],
        }

    def _mop_binding_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
        payload = self._loads(str(row["payload_json"] or ""), {})
        payload = payload if isinstance(payload, dict) else {}
        return {
            "binding_id": str(row["binding_id"] or ""),
            "notice_key": str(row["notice_key"] or ""),
            "template_key": str(row["template_key"] or "") if "template_key" in row.keys() else "",
            "scope": str(row["scope"] or ""),
            "notice_title": str(row["notice_title"] or ""),
            "notice_status": str(row["notice_status"] or ""),
            "source_record_id": str(row["source_record_id"] or ""),
            "target_record_id": str(row["target_record_id"] or ""),
            "active_item_id": str(row["active_item_id"] or ""),
            "mop_app_token": str(row["mop_app_token"] or ""),
            "mop_table_id": str(row["mop_table_id"] or ""),
            "mop_record_id": str(row["mop_record_id"] or ""),
            "mop_title": str(row["mop_title"] or ""),
            "mop_attachment_token": str(row["mop_attachment_token"] or ""),
            "mop_attachment_name": str(row["mop_attachment_name"] or ""),
            "selected_sheet": str(row["selected_sheet"] or ""),
            "payload": payload,
            "updated_by": str(row["updated_by"] or ""),
            "created_at": float(row["created_at"] or 0),
            "updated_at": float(row["updated_at"] or 0),
            "deleted_at": float(row["deleted_at"] or 0) if row["deleted_at"] is not None else None,
        }

    def upsert_mop_notice_binding(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload = dict(payload or {})
        notice_key = self._text(payload.get("notice_key"))
        template_key = self._text(payload.get("template_key") or payload.get("mop_template_key"))
        mop_record_id = self._text(payload.get("mop_record_id"))
        if not notice_key or not mop_record_id:
            raise ValueError("MOP绑定缺少 notice_key 或 mop_record_id。")
        binding_seed = template_key or notice_key
        binding_id = self._text(payload.get("binding_id")) or f"mop:{uuid.uuid5(uuid.NAMESPACE_URL, binding_seed).hex}"
        now = time.time()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                existing = conn.execute(
                    "SELECT created_at FROM mop_notice_bindings WHERE binding_id = ?",
                    (binding_id,),
                ).fetchone()
                created_at = float(existing["created_at"] or now) if existing else now
                conn.execute(
                    """
                    INSERT OR REPLACE INTO mop_notice_bindings(
                        binding_id, notice_key, template_key, scope, notice_title, notice_status,
                        source_record_id, target_record_id, active_item_id,
                        mop_app_token, mop_table_id, mop_record_id, mop_title,
                        mop_attachment_token, mop_attachment_name, selected_sheet,
                        payload_json, updated_by, created_at, updated_at, deleted_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                    """,
                    (
                        binding_id,
                        notice_key,
                        template_key,
                        self._text(payload.get("scope")),
                        self._text(payload.get("notice_title")),
                        self._text(payload.get("notice_status")),
                        self._text(payload.get("source_record_id")),
                        self._text(payload.get("target_record_id")),
                        self._text(payload.get("active_item_id")),
                        self._text(payload.get("mop_app_token")),
                        self._text(payload.get("mop_table_id")),
                        mop_record_id,
                        self._text(payload.get("mop_title")),
                        self._text(payload.get("mop_attachment_token")),
                        self._text(payload.get("mop_attachment_name")),
                        self._text(payload.get("selected_sheet")),
                        self._json(payload),
                        self._text(payload.get("updated_by")),
                        created_at,
                        now,
                    ),
                )
                conn.commit()
                row = conn.execute(
                    "SELECT * FROM mop_notice_bindings WHERE binding_id = ?",
                    (binding_id,),
                ).fetchone()
        return self._mop_binding_from_row(row)

    def list_mop_notice_bindings(
        self,
        *,
        scope: str = "",
        notice_keys: list[str] | None = None,
        template_keys: list[str] | None = None,
        include_deleted: bool = False,
    ) -> list[dict[str, Any]]:
        if not self.db_path.exists():
            return []
        clauses = []
        params: list[Any] = []
        if not include_deleted:
            clauses.append("deleted_at IS NULL")
        scope = self._text(scope)
        if scope and scope != "ALL":
            clauses.append("(scope = ? OR scope = '' OR scope = 'ALL')")
            params.append(scope)
        match_clauses: list[str] = []
        match_params: list[Any] = []
        keys = [self._text(item) for item in (notice_keys or []) if self._text(item)]
        if keys:
            placeholders = ",".join("?" for _ in keys)
            match_clauses.append(f"notice_key IN ({placeholders})")
            match_params.extend(keys)
        template_keys_normalized = [
            self._text(item) for item in (template_keys or []) if self._text(item)
        ]
        if template_keys_normalized:
            placeholders = ",".join("?" for _ in template_keys_normalized)
            match_clauses.append(f"template_key IN ({placeholders})")
            match_params.extend(template_keys_normalized)
        if not match_clauses:
            return []
        if match_clauses:
            clauses.append(f"({' OR '.join(match_clauses)})")
            params.extend(match_params)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    f"""
                    SELECT *
                    FROM mop_notice_bindings
                    {where}
                    ORDER BY updated_at DESC
                    """,
                    tuple(params),
                ).fetchall()
        return [self._mop_binding_from_row(row) for row in rows]

    def _mop_fill_memory_from_row(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if not row:
            return None
        payload = self._loads(str(row["payload_json"] or "{}"), {})
        if not isinstance(payload, dict):
            payload = {}
        return {
            "memory_key": str(row["memory_key"] or ""),
            "mop_title": str(row["mop_title"] or ""),
            "mop_file_name": str(row["mop_file_name"] or ""),
            "sheet_name": str(row["sheet_name"] or ""),
            "payload": payload,
            "updated_by": str(row["updated_by"] or ""),
            "created_at": float(row["created_at"] or 0),
            "updated_at": float(row["updated_at"] or 0),
        }

    def upsert_mop_fill_memory(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload = dict(payload or {})
        memory_key = self._text(payload.get("memory_key"))
        if not memory_key:
            raise ValueError("MOP填写记忆缺少 memory_key。")
        now = time.time()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                existing = conn.execute(
                    "SELECT created_at FROM mop_fill_memory WHERE memory_key = ?",
                    (memory_key,),
                ).fetchone()
                created_at = float(existing["created_at"] or now) if existing else now
                conn.execute(
                    """
                    INSERT OR REPLACE INTO mop_fill_memory(
                        memory_key, mop_title, mop_file_name, sheet_name,
                        payload_json, updated_by, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        memory_key,
                        self._text(payload.get("mop_title")),
                        self._text(payload.get("mop_file_name")),
                        self._text(payload.get("sheet_name")),
                        self._json(payload.get("payload") if isinstance(payload.get("payload"), dict) else {}),
                        self._text(payload.get("updated_by")),
                        created_at,
                        now,
                    ),
                )
                conn.commit()
                row = conn.execute(
                    "SELECT * FROM mop_fill_memory WHERE memory_key = ?",
                    (memory_key,),
                ).fetchone()
        return self._mop_fill_memory_from_row(row) or {}

    def get_mop_fill_memory(self, memory_key: str) -> dict[str, Any] | None:
        memory_key = self._text(memory_key)
        if not memory_key or not self.db_path.exists():
            return None
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(
                    "SELECT * FROM mop_fill_memory WHERE memory_key = ?",
                    (memory_key,),
                ).fetchone()
        return self._mop_fill_memory_from_row(row)

    def _mop_local_file_from_row(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if not row:
            return None
        detected = self._loads(str(row["detected_json"] or "{}"), {})
        warnings = self._loads(str(row["warnings_json"] or "[]"), [])
        payload = self._loads(str(row["payload_json"] or "{}"), {})
        if not isinstance(detected, dict):
            detected = {}
        if not isinstance(warnings, list):
            warnings = []
        if not isinstance(payload, dict):
            payload = {}
        return {
            "upload_id": str(row["upload_id"] or ""),
            "scope": str(row["scope"] or ""),
            "source_record_id": str(row["source_record_id"] or ""),
            "notice_key": str(row["notice_key"] or ""),
            "notice_title": str(row["notice_title"] or ""),
            "original_file_name": str(row["original_file_name"] or ""),
            "local_file_path": str(row["local_file_path"] or ""),
            "file_size": int(row["file_size"] or 0),
            "status": str(row["status"] or ""),
            "detected": detected,
            "warnings": [str(item) for item in warnings],
            "payload": payload,
            "created_by_openid": str(row["created_by_openid"] or ""),
            "created_at": float(row["created_at"] or 0),
            "updated_at": float(row["updated_at"] or 0),
            "deleted_at": float(row["deleted_at"] or 0) if row["deleted_at"] is not None else None,
        }

    def upsert_engineer_mop_local_file(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload = dict(payload or {})
        upload_id = self._text(payload.get("upload_id")) or uuid.uuid4().hex
        local_file_path = self._text(payload.get("local_file_path"))
        if not local_file_path:
            raise ValueError("本地 MOP 文件缺少保存路径。")
        now = time.time()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                existing = conn.execute(
                    "SELECT created_at FROM engineer_mop_local_files WHERE upload_id = ?",
                    (upload_id,),
                ).fetchone()
                created_at = float(existing["created_at"] or now) if existing else now
                conn.execute(
                    """
                    INSERT OR REPLACE INTO engineer_mop_local_files(
                        upload_id, scope, source_record_id, notice_key, notice_title,
                        original_file_name, local_file_path, file_size, status,
                        detected_json, warnings_json, payload_json, created_by_openid,
                        created_at, updated_at, deleted_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        upload_id,
                        self._text(payload.get("scope")),
                        self._text(payload.get("source_record_id")),
                        self._text(payload.get("notice_key")),
                        self._text(payload.get("notice_title")),
                        self._text(payload.get("original_file_name")),
                        local_file_path,
                        int(payload.get("file_size") or 0),
                        self._text(payload.get("status")) or "ready",
                        self._json(payload.get("detected") if isinstance(payload.get("detected"), dict) else {}),
                        self._json(payload.get("warnings") if isinstance(payload.get("warnings"), list) else []),
                        self._json(payload.get("payload") if isinstance(payload.get("payload"), dict) else {}),
                        self._text(payload.get("created_by_openid")),
                        created_at,
                        now,
                        payload.get("deleted_at"),
                    ),
                )
                conn.commit()
                row = conn.execute(
                    "SELECT * FROM engineer_mop_local_files WHERE upload_id = ?",
                    (upload_id,),
                ).fetchone()
        return self._mop_local_file_from_row(row) or {}

    def get_engineer_mop_local_file(
        self,
        upload_id: str,
        *,
        include_deleted: bool = False,
    ) -> dict[str, Any] | None:
        upload_id = self._text(upload_id)
        if not upload_id or not self.db_path.exists():
            return None
        clauses = ["upload_id = ?"]
        if not include_deleted:
            clauses.append("deleted_at IS NULL")
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(
                    f"SELECT * FROM engineer_mop_local_files WHERE {' AND '.join(clauses)}",
                    (upload_id,),
                ).fetchone()
        return self._mop_local_file_from_row(row)

    def list_engineer_mop_local_files(
        self,
        *,
        source_record_id: str = "",
        notice_key: str = "",
        scope: str = "",
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        if not self.db_path.exists():
            return []
        clauses = ["deleted_at IS NULL"]
        params: list[Any] = []
        if self._text(source_record_id):
            clauses.append("source_record_id = ?")
            params.append(self._text(source_record_id))
        if self._text(notice_key):
            clauses.append("notice_key = ?")
            params.append(self._text(notice_key))
        if self._text(scope) and self._text(scope) != "ALL":
            clauses.append("(scope = ? OR scope = '' OR scope = 'ALL')")
            params.append(self._text(scope))
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    f"""
                    SELECT *
                    FROM engineer_mop_local_files
                    WHERE {' AND '.join(clauses)}
                    ORDER BY updated_at DESC
                    LIMIT ?
                    """,
                    (*params, max(1, int(limit or 20))),
                ).fetchall()
        return [item for item in (self._mop_local_file_from_row(row) for row in rows) if item]

    def mark_old_engineer_mop_local_files_deleted(self, *, older_than_ts: float) -> int:
        if not self.db_path.exists():
            return 0
        now = time.time()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                cursor = conn.execute(
                    """
                    UPDATE engineer_mop_local_files
                    SET deleted_at = ?, updated_at = ?
                    WHERE deleted_at IS NULL AND updated_at < ?
                    """,
                    (now, now, float(older_than_ts or 0)),
                )
                conn.commit()
                return int(cursor.rowcount or 0)

    def get_auth_permissions(self) -> dict[str, Any] | None:
        if not self.db_path.exists():
            return None
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    """
                    SELECT open_id, name, role, scopes_json, enabled, payload_json, updated_at
                    FROM auth_permissions
                    ORDER BY open_id ASC
                    """
                ).fetchall()
        if not rows:
            return None
        users: dict[str, Any] = {}
        meta: dict[str, Any] = {}
        updated_at = ""
        for row in rows:
            open_id = str(row["open_id"] or "")
            payload = self._loads(str(row["payload_json"] or ""), {})
            payload = payload if isinstance(payload, dict) else {}
            if open_id == "__meta__":
                meta = payload
                continue
            scopes = self._loads(str(row["scopes_json"] or "[]"), [])
            if not isinstance(scopes, list):
                scopes = []
            user_payload = dict(payload)
            user_payload.setdefault("name", str(row["name"] or ""))
            user_payload.setdefault("role", str(row["role"] or "building"))
            user_payload.setdefault("scopes", scopes)
            user_payload.setdefault("enabled", bool(row["enabled"]))
            users[open_id] = user_payload
            if not updated_at:
                updated_at = time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(float(row["updated_at"] or 0))
                )
        result = {
            "version": int(meta.get("version") or 1),
            "updated_at": str(meta.get("updated_at") or updated_at or ""),
            "default_scopes": meta.get("default_scopes")
            if isinstance(meta.get("default_scopes"), list)
            else [],
            "users": users,
        }
        return result

    def put_auth_permissions(self, payload: dict[str, Any]) -> None:
        if not isinstance(payload, dict):
            return
        users = payload.get("users") if isinstance(payload.get("users"), dict) else {}
        meta = {
            "version": int(payload.get("version") or 1),
            "updated_at": str(payload.get("updated_at") or ""),
            "default_scopes": payload.get("default_scopes")
            if isinstance(payload.get("default_scopes"), list)
            else [],
        }
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute("BEGIN IMMEDIATE")
                now = time.time()
                conn.execute("DELETE FROM auth_permissions")
                conn.execute(
                    """
                    INSERT INTO auth_permissions(
                        open_id, name, role, scopes_json, enabled, payload_json, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("__meta__", "", "meta", "[]", 1, self._json(meta), now),
                )
                for open_id, user_cfg in users.items():
                    text_open_id = str(open_id or "").strip()
                    if (
                        not text_open_id
                        or text_open_id == "__meta__"
                        or not isinstance(user_cfg, dict)
                    ):
                        continue
                    scopes = user_cfg.get("scopes") if isinstance(user_cfg.get("scopes"), list) else []
                    conn.execute(
                        """
                        INSERT INTO auth_permissions(
                            open_id, name, role, scopes_json, enabled, payload_json, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            text_open_id,
                            self._text(user_cfg.get("name")),
                            self._text(user_cfg.get("role")) or "building",
                            self._json(scopes),
                            0 if user_cfg.get("enabled") is False else 1,
                            self._json(user_cfg),
                            now,
                        ),
                    )
                conn.commit()

    def get_permission_request(self, request_id: str) -> dict[str, Any] | None:
        request_id = self._text(request_id)
        if not request_id or not self.db_path.exists():
            return None
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(
                    """
                    SELECT payload_json
                    FROM permission_requests
                    WHERE request_id = ?
                    """,
                    (request_id,),
                ).fetchone()
        if not row:
            return None
        payload = self._loads(str(row["payload_json"] or ""), {})
        return payload if isinstance(payload, dict) else None

    def get_active_permission_request(
        self, open_id: str, *, now: float | None = None
    ) -> dict[str, Any] | None:
        open_id = self._text(open_id)
        if not open_id or not self.db_path.exists():
            return None
        now = time.time() if now is None else float(now)
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(
                    """
                    SELECT payload_json
                    FROM permission_requests
                    WHERE open_id = ?
                      AND status = 'pending'
                      AND expires_at > ?
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (open_id, now),
                ).fetchone()
        if not row:
            return None
        payload = self._loads(str(row["payload_json"] or ""), {})
        return payload if isinstance(payload, dict) else None

    def get_latest_permission_request(
        self,
        open_id: str,
        *,
        statuses: list[str] | tuple[str, ...] = ("pending", "rejected"),
    ) -> dict[str, Any] | None:
        open_id = self._text(open_id)
        clean_statuses = [self._text(item) for item in statuses if self._text(item)]
        if not open_id or not clean_statuses or not self.db_path.exists():
            return None
        placeholders = ",".join("?" for _ in clean_statuses)
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(
                    f"""
                    SELECT payload_json
                    FROM permission_requests
                    WHERE open_id = ?
                      AND status IN ({placeholders})
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (open_id, *clean_statuses),
                ).fetchone()
        if not row:
            return None
        payload = self._loads(str(row["payload_json"] or ""), {})
        return payload if isinstance(payload, dict) else None

    def list_permission_requests(
        self, *, status: str = "pending", limit: int = 100
    ) -> list[dict[str, Any]]:
        if not self.db_path.exists():
            return []
        status = self._text(status)
        limit = max(1, min(int(limit or 100), 500))
        params: list[Any] = []
        where = ""
        if status and status.lower() != "all":
            where = "WHERE status = ?"
            params.append(status)
        params.append(limit)
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    f"""
                    SELECT payload_json
                    FROM permission_requests
                    {where}
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    tuple(params),
                ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            payload = self._loads(str(row["payload_json"] or ""), {})
            if isinstance(payload, dict):
                result.append(payload)
        return result

    def put_permission_request(self, payload: dict[str, Any]) -> None:
        if not isinstance(payload, dict):
            return
        request_id = self._text(payload.get("request_id"))
        open_id = self._text(payload.get("open_id"))
        if not request_id or not open_id:
            return
        requested_scopes = (
            payload.get("requested_scopes")
            if isinstance(payload.get("requested_scopes"), list)
            else []
        )
        now = time.time()
        created_at = float(payload.get("created_at_ts") or now)
        updated_at = float(payload.get("updated_at_ts") or now)
        expires_at = float(payload.get("expires_at_ts") or 0)
        approved_at_raw = payload.get("approved_at_ts")
        try:
            approved_at = float(approved_at_raw) if approved_at_raw else None
        except Exception:
            approved_at = None
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    """
                    INSERT OR REPLACE INTO permission_requests(
                        request_id, open_id, name, requested_scopes_json, reason,
                        status, code_hash, code_salt, attempts, max_attempts,
                        expires_at, approved_at, payload_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        request_id,
                        open_id,
                        self._text(payload.get("name")),
                        self._json(requested_scopes),
                        self._text(payload.get("reason")),
                        self._text(payload.get("status")) or "pending",
                        self._text(payload.get("code_hash")),
                        self._text(payload.get("code_salt")),
                        int(payload.get("attempts") or 0),
                        int(payload.get("max_attempts") or 5),
                        expires_at,
                        approved_at,
                        self._json(payload),
                        created_at,
                        updated_at,
                    ),
                )
                conn.commit()

    def mark_permission_requests_for_open_id(
        self,
        open_id: str,
        *,
        from_status: str = "pending",
        to_status: str = "superseded",
        exclude_request_id: str = "",
    ) -> None:
        open_id = self._text(open_id)
        if not open_id:
            return
        exclude_request_id = self._text(exclude_request_id)
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    """
                    SELECT payload_json
                    FROM permission_requests
                    WHERE open_id = ? AND status = ?
                    """,
                    (open_id, self._text(from_status)),
                ).fetchall()
                now = time.time()
                for row in rows:
                    payload = self._loads(str(row["payload_json"] or ""), {})
                    if not isinstance(payload, dict):
                        continue
                    if exclude_request_id and self._text(payload.get("request_id")) == exclude_request_id:
                        continue
                    payload["status"] = self._text(to_status) or "superseded"
                    payload["updated_at_ts"] = now
                    payload["updated_at"] = time.strftime(
                        "%Y-%m-%d %H:%M:%S", time.localtime(now)
                    )
                    conn.execute(
                        """
                        UPDATE permission_requests
                        SET status = ?, payload_json = ?, updated_at = ?
                        WHERE request_id = ?
                        """,
                        (
                            payload["status"],
                            self._json(payload),
                            now,
                            self._text(payload.get("request_id")),
                        ),
                    )
                conn.commit()

    def get_handover_payload(self) -> dict[str, Any] | None:
        if not self.db_path.exists():
            return None
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    """
                    SELECT scope, url, updated_by, payload_json, updated_at
                    FROM handover_links
                    ORDER BY scope ASC
                    """
                ).fetchall()
        if not rows:
            return None
        links: dict[str, str] = {}
        meta: dict[str, Any] = {}
        for row in rows:
            scope = str(row["scope"] or "")
            payload = self._loads(str(row["payload_json"] or ""), {})
            payload = payload if isinstance(payload, dict) else {}
            if scope == "__meta__":
                meta = payload
                continue
            links[scope] = str(row["url"] or payload.get("url") or "")
        result = dict(meta)
        result["links"] = links
        return result

    def put_handover_payload(self, payload: dict[str, Any]) -> None:
        if not isinstance(payload, dict):
            return
        links = payload.get("links") if isinstance(payload.get("links"), dict) else {}
        meta = dict(payload)
        meta.pop("links", None)
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute("BEGIN IMMEDIATE")
                now = time.time()
                conn.execute("DELETE FROM handover_links")
                conn.execute(
                    """
                    INSERT INTO handover_links(scope, url, updated_by, payload_json, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    ("__meta__", "", str(meta.get("updated_by") or ""), self._json(meta), now),
                )
                for scope, url in links.items():
                    scope_text = self._text(scope)
                    if not scope_text:
                        continue
                    row_payload = {"scope": scope_text, "url": str(url or "")}
                    conn.execute(
                        """
                        INSERT INTO handover_links(scope, url, updated_by, payload_json, updated_at)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            scope_text,
                            str(url or "").strip(),
                            str(meta.get("updated_by") or ""),
                            self._json(row_payload),
                            now,
                        ),
                    )
                conn.commit()

    def append_event(self, source: str, payload: dict[str, Any] | None) -> int:
        source = self._text(source) or "unknown"
        normalized = dict(payload or {})
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                cursor = conn.execute(
                    """
                    INSERT INTO append_events(source, payload_json, created_at)
                    VALUES (?, ?, ?)
                    """,
                    (source, self._json(normalized), time.time()),
                )
                conn.commit()
                return int(cursor.lastrowid or 0)

    def append_event_async(self, source: str, payload: dict[str, Any] | None) -> bool:
        return self._submit_background_write(
            "append_event",
            {
                "source": self._text(source) or "unknown",
                "value": dict(payload or {}),
            },
        )

    def put_backend_runtime(self, key: str, payload: dict[str, Any] | None) -> None:
        key = self._text(key)
        if not key:
            return
        normalized = dict(payload or {})
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute(
                    """
                    INSERT OR REPLACE INTO backend_runtime(key, value_json, updated_at)
                    VALUES (?, ?, ?)
                    """,
                    (key, self._json(normalized), time.time()),
                )
                conn.commit()

    def put_backend_runtime_async(
        self, key: str, payload: dict[str, Any] | None
    ) -> bool:
        key = self._text(key)
        if not key:
            return False
        return self._submit_background_write(
            "backend_runtime",
            {
                "key": key,
                "value": dict(payload or {}),
            },
        )

    def get_backend_runtime(self, key: str) -> dict[str, Any] | None:
        key = self._text(key)
        if not key or not self.db_path.exists():
            return None
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(
                    "SELECT value_json, updated_at FROM backend_runtime WHERE key = ?",
                    (key,),
                ).fetchone()
        if not row:
            return None
        payload = self._loads(str(row["value_json"] or ""), {})
        if not isinstance(payload, dict):
            payload = {}
        payload.setdefault("updated_at", float(row["updated_at"] or 0))
        return payload

    def get_database_stats(self) -> dict[str, Any]:
        db_path = self.db_path

        def file_size(path: Path) -> int:
            try:
                return int(path.stat().st_size)
            except OSError:
                return 0

        stats: dict[str, Any] = {
            "path": str(db_path),
            "exists": db_path.exists(),
            "db_bytes": file_size(db_path),
            "wal_bytes": file_size(Path(f"{db_path}-wal")),
            "shm_bytes": file_size(Path(f"{db_path}-shm")),
            "total_bytes": 0,
            "page_count": 0,
            "page_size": 0,
            "freelist_count": 0,
            "journal_mode": "",
            "busy_timeout_ms": 0,
            "table_counts": {},
            "checked_at": time.time(),
        }
        stats["total_bytes"] = (
            int(stats["db_bytes"]) + int(stats["wal_bytes"]) + int(stats["shm_bytes"])
        )
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                try:
                    stats["page_count"] = int(
                        (conn.execute("PRAGMA page_count").fetchone() or [0])[0] or 0
                    )
                    stats["page_size"] = int(
                        (conn.execute("PRAGMA page_size").fetchone() or [0])[0] or 0
                    )
                    stats["freelist_count"] = int(
                        (conn.execute("PRAGMA freelist_count").fetchone() or [0])[0] or 0
                    )
                    stats["journal_mode"] = str(
                        (conn.execute("PRAGMA journal_mode").fetchone() or [""])[0] or ""
                    )
                    stats["busy_timeout_ms"] = int(
                        (conn.execute("PRAGMA busy_timeout").fetchone() or [0])[0] or 0
                    )
                except sqlite3.Error:
                    pass
                counts: dict[str, int] = {}
                for table in (
                    "notice_actions",
                    "event_outbox",
                    "append_events",
                    "runtime_task_queue",
                    "backend_runtime",
                    "qt_active_items",
                    "daily_summary",
                    "work_status",
                    "source_records_all",
                    "source_snapshot_manifest",
                    "source_snapshot_records",
                    "notice_upload_attachments",
                ):
                    try:
                        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                        counts[table] = int((row or [0])[0] or 0)
                    except sqlite3.Error:
                        continue
                stats["table_counts"] = counts
        return stats

    def runtime_health_report(self) -> dict[str, Any]:
        schema = self.schema_health()
        database = self.get_database_stats()
        source_snapshot = self.source_snapshot_stats()
        write_worker = self.get_write_worker_stats()
        ok = bool(schema.get("ok")) and bool(database.get("exists"))
        return {
            "ok": ok,
            "schema": schema,
            "database": database,
            "source_snapshot": source_snapshot,
            "write_worker": write_worker,
            "checked_at": time.time(),
        }

    def checkpoint_database(self, *, truncate: bool = True) -> dict[str, Any]:
        before = self.get_database_stats()
        mode = "TRUNCATE" if truncate else "PASSIVE"
        checkpoint = {"busy": 0, "log": 0, "checkpointed": 0, "mode": mode}
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(f"PRAGMA wal_checkpoint({mode})").fetchone()
                if row:
                    checkpoint["busy"] = int(row[0] or 0)
                    checkpoint["log"] = int(row[1] or 0)
                    checkpoint["checkpointed"] = int(row[2] or 0)
        after = self.get_database_stats()
        return {
            "before": before,
            "after": after,
            "checkpoint": checkpoint,
            "reclaimed_bytes": max(
                0,
                int(before.get("total_bytes") or 0) - int(after.get("total_bytes") or 0),
            ),
        }

    def enqueue_outbox_event(
        self, channel: str, payload: dict[str, Any] | None
    ) -> int:
        channel = self._text(channel) or "default"
        normalized = dict(payload or {})
        now = time.time()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                cursor = conn.execute(
                    """
                    INSERT INTO event_outbox(
                        channel, status, payload_json, attempts, last_error, created_at, updated_at
                    )
                    VALUES (?, 'pending', ?, 0, '', ?, ?)
                    """,
                    (channel, self._json(normalized), now, now),
                )
                conn.commit()
                return int(cursor.lastrowid or 0)

    def list_outbox_events(
        self,
        channel: str,
        *,
        status: str = "pending",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        channel = self._text(channel)
        status = self._text(status) or "pending"
        if not channel or not self.db_path.exists():
            return []
        limit = max(1, min(int(limit or 100), 500))
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    """
                    SELECT id, channel, status, payload_json, attempts, last_error, created_at, updated_at
                    FROM event_outbox
                    WHERE channel = ? AND status = ?
                    ORDER BY id ASC
                    LIMIT ?
                    """,
                    (channel, status, limit),
                ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            payload = self._loads(str(row["payload_json"] or ""), {})
            result.append(
                {
                    "id": int(row["id"] or 0),
                    "channel": str(row["channel"] or ""),
                    "status": str(row["status"] or ""),
                    "payload": payload if isinstance(payload, dict) else {},
                    "attempts": int(row["attempts"] or 0),
                    "last_error": str(row["last_error"] or ""),
                    "created_at": float(row["created_at"] or 0),
                    "updated_at": float(row["updated_at"] or 0),
                }
            )
        return result

    def count_outbox_events(
        self, channel: str, *, stale_lease_seconds: int = 120
    ) -> dict[str, int]:
        channel = self._text(channel)
        if not channel or not self.db_path.exists():
            return {}
        stale_lease_seconds = max(5, int(stale_lease_seconds or 60))
        now = time.time()
        stale_before = now - stale_lease_seconds
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute(
                    """
                    UPDATE event_outbox
                    SET status = 'pending', updated_at = ?
                    WHERE channel = ? AND status = 'leased' AND updated_at < ?
                    """,
                    (now, channel, stale_before),
                )
                rows = conn.execute(
                    """
                    SELECT status, COUNT(*) AS count
                    FROM event_outbox
                    WHERE channel = ?
                    GROUP BY status
                    """,
                    (channel,),
                ).fetchall()
                conn.commit()
        return {
            str(row["status"] or ""): int(row["count"] or 0)
            for row in rows
            if str(row["status"] or "")
        }

    def lease_outbox_events(
        self,
        channel: str,
        *,
        limit: int = 1,
        lease_seconds: int = 30,
    ) -> list[dict[str, Any]]:
        channel = self._text(channel)
        if not channel or not self.db_path.exists():
            return []
        limit = max(1, min(int(limit or 1), 20))
        lease_seconds = max(5, int(lease_seconds or 30))
        now = time.time()
        stale_before = now - lease_seconds
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute(
                    """
                    UPDATE event_outbox
                    SET status = 'pending', updated_at = ?
                    WHERE channel = ? AND status = 'leased' AND updated_at < ?
                    """,
                    (now, channel, stale_before),
                )
                rows = conn.execute(
                    """
                    SELECT id, channel, status, payload_json, attempts, last_error, created_at, updated_at
                    FROM event_outbox
                    WHERE channel = ? AND status = 'pending'
                    ORDER BY id ASC
                    LIMIT ?
                    """,
                    (channel, limit),
                ).fetchall()
                ids = [int(row["id"] or 0) for row in rows if int(row["id"] or 0) > 0]
                if ids:
                    placeholders = ",".join("?" for _ in ids)
                    conn.execute(
                        f"""
                        UPDATE event_outbox
                        SET status = 'leased', updated_at = ?
                        WHERE id IN ({placeholders})
                        """,
                        (now, *ids),
                    )
                conn.commit()
        result: list[dict[str, Any]] = []
        for row in rows:
            payload = self._loads(str(row["payload_json"] or ""), {})
            result.append(
                {
                    "id": int(row["id"] or 0),
                    "channel": str(row["channel"] or ""),
                    "status": "leased",
                    "payload": payload if isinstance(payload, dict) else {},
                    "attempts": int(row["attempts"] or 0),
                    "last_error": str(row["last_error"] or ""),
                    "created_at": float(row["created_at"] or 0),
                    "updated_at": now,
                }
            )
        return result

    def mark_outbox_event(
        self,
        event_id: int,
        status: str,
        *,
        error: str = "",
        max_attempts: int = 3,
    ) -> dict[str, Any] | None:
        try:
            event_id = int(event_id or 0)
        except Exception:
            event_id = 0
        status = self._text(status) or "done"
        if event_id <= 0:
            return None
        error = self._text(error)
        max_attempts = max(1, int(max_attempts or 3))
        now = time.time()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                current = conn.execute(
                    """
                    SELECT id, channel, status, payload_json, attempts, last_error, created_at, updated_at
                    FROM event_outbox
                    WHERE id = ?
                    """,
                    (event_id,),
                ).fetchone()
                if not current:
                    return None
                attempts = int(current["attempts"] or 0)
                final_status = status
                if status == "pending":
                    attempts += 1
                    if attempts >= max_attempts:
                        final_status = "failed"
                elif status == "done":
                    error = ""
                conn.execute(
                    """
                    UPDATE event_outbox
                    SET status = ?, attempts = ?, last_error = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (final_status, attempts, error, now, event_id),
                )
                conn.commit()
                row = conn.execute(
                    """
                    SELECT id, channel, status, payload_json, attempts, last_error, created_at, updated_at
                    FROM event_outbox
                    WHERE id = ?
                    """,
                    (event_id,),
                ).fetchone()
        if not row:
            return None
        payload = self._loads(str(row["payload_json"] or ""), {})
        return {
            "id": int(row["id"] or 0),
            "channel": str(row["channel"] or ""),
            "status": str(row["status"] or ""),
            "payload": payload if isinstance(payload, dict) else {},
            "attempts": int(row["attempts"] or 0),
            "last_error": str(row["last_error"] or ""),
            "created_at": float(row["created_at"] or 0),
            "updated_at": float(row["updated_at"] or 0),
        }

    def cleanup_outbox_events(
        self,
        *,
        done_retention_seconds: int = 24 * 3600,
        failed_retention_seconds: int = 7 * 24 * 3600,
        max_delete: int = 1000,
    ) -> dict[str, int]:
        done_retention_seconds = max(60, int(done_retention_seconds or 0))
        failed_retention_seconds = max(60, int(failed_retention_seconds or 0))
        max_delete = max(1, min(int(max_delete or 1000), 5000))
        now = time.time()
        removed_done = 0
        removed_failed = 0
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    """
                    SELECT id, status, updated_at
                    FROM event_outbox
                    WHERE status IN ('done', 'failed', 'cancelled')
                    ORDER BY updated_at ASC
                    LIMIT ?
                    """,
                    (max_delete,),
                ).fetchall()
                for row in rows:
                    status = str(row["status"] or "")
                    updated_at = float(row["updated_at"] or 0)
                    retention = (
                        failed_retention_seconds
                        if status == "failed"
                        else done_retention_seconds
                    )
                    if updated_at and now - updated_at < retention:
                        continue
                    conn.execute(
                        "DELETE FROM event_outbox WHERE id = ?",
                        (int(row["id"] or 0),),
                    )
                    if status == "failed":
                        removed_failed += 1
                    else:
                        removed_done += 1
                conn.commit()
        return {
            "removed_done": removed_done,
            "removed_failed": removed_failed,
            "removed_total": removed_done + removed_failed,
        }

    def upsert_runtime_queue_item(
        self,
        queue_name: str,
        job_id: str,
        *,
        status: str = "queued",
        payload: dict[str, Any] | None = None,
        available_at: float | None = None,
        error: str = "",
    ) -> bool:
        queue_name = self._text(queue_name)
        job_id = self._text(job_id)
        status = self._text(status) or "queued"
        if not queue_name or not job_id:
            return False
        now = time.time()
        available = float(available_at if available_at is not None else now)
        normalized = dict(payload or {})
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute(
                    """
                    INSERT INTO runtime_task_queue(
                        queue_name, job_id, status, available_at, lease_until,
                        attempts, last_error, payload_json, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, 0, 0, ?, ?, ?, ?)
                    ON CONFLICT(queue_name, job_id) DO UPDATE SET
                        status = excluded.status,
                        available_at = excluded.available_at,
                        lease_until = 0,
                        last_error = excluded.last_error,
                        payload_json = excluded.payload_json,
                        updated_at = excluded.updated_at
                    """,
                    (
                        queue_name,
                        job_id,
                        status,
                        available,
                        self._text(error),
                        self._json(normalized),
                        now,
                        now,
                    ),
                )
                conn.commit()
        return True

    def mark_runtime_queue_item(
        self,
        queue_name: str,
        job_id: str,
        status: str,
        *,
        error: str = "",
    ) -> bool:
        queue_name = self._text(queue_name)
        job_id = self._text(job_id)
        status = self._text(status) or "done"
        if not queue_name or not job_id:
            return False
        now = time.time()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                cursor = conn.execute(
                    """
                    UPDATE runtime_task_queue
                    SET status = ?,
                        last_error = ?,
                        lease_until = CASE WHEN ? = 'processing' THEN lease_until ELSE 0 END,
                        updated_at = ?
                    WHERE queue_name = ? AND job_id = ?
                    """,
                    (status, self._text(error), status, now, queue_name, job_id),
                )
                conn.commit()
                return bool(cursor.rowcount)

    def lease_runtime_queue_items(
        self,
        queue_name: str,
        *,
        limit: int = 1,
        lease_seconds: float = 30.0,
    ) -> list[dict[str, Any]]:
        queue_name = self._text(queue_name)
        if not queue_name:
            return []
        limit = max(1, min(int(limit or 1), 50))
        now = time.time()
        lease_until = now + max(1.0, float(lease_seconds or 30.0))
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute("BEGIN IMMEDIATE")
                rows = conn.execute(
                    """
                    SELECT queue_name, job_id, status, available_at, lease_until,
                           attempts, last_error, payload_json, created_at, updated_at
                    FROM runtime_task_queue
                    WHERE queue_name = ?
                      AND (
                        (status = 'queued' AND available_at <= ?)
                        OR (status = 'processing' AND lease_until > 0 AND lease_until <= ?)
                      )
                    ORDER BY available_at ASC, updated_at ASC
                    LIMIT ?
                    """,
                    (queue_name, now, now, limit),
                ).fetchall()
                keys = [str(row["job_id"] or "") for row in rows if str(row["job_id"] or "")]
                for job_id in keys:
                    conn.execute(
                        """
                        UPDATE runtime_task_queue
                        SET status = 'processing',
                            lease_until = ?,
                            attempts = attempts + 1,
                            updated_at = ?
                        WHERE queue_name = ? AND job_id = ?
                        """,
                        (lease_until, now, queue_name, job_id),
                    )
                conn.commit()
        leased: list[dict[str, Any]] = []
        for row in rows:
            payload = self._loads(str(row["payload_json"] or ""), {})
            leased.append(
                {
                    "queue_name": str(row["queue_name"] or ""),
                    "job_id": str(row["job_id"] or ""),
                    "status": "processing",
                    "available_at": float(row["available_at"] or 0),
                    "lease_until": lease_until,
                    "attempts": int(row["attempts"] or 0) + 1,
                    "last_error": str(row["last_error"] or ""),
                    "payload": payload if isinstance(payload, dict) else {},
                    "created_at": float(row["created_at"] or 0),
                    "updated_at": now,
                }
            )
        return leased

    def list_runtime_queue_items(
        self,
        queue_name: str,
        *,
        statuses: tuple[str, ...] | list[str] = ("queued",),
        due_only: bool = False,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        queue_name = self._text(queue_name)
        if not queue_name:
            return []
        normalized_statuses = [
            self._text(status)
            for status in (statuses or [])
            if self._text(status)
        ]
        if not normalized_statuses:
            return []
        limit = max(1, min(int(limit or 100), 500))
        placeholders = ", ".join("?" for _ in normalized_statuses)
        params: list[Any] = [queue_name, *normalized_statuses]
        due_clause = ""
        if due_only:
            due_clause = " AND available_at <= ?"
            params.append(time.time())
        params.append(limit)
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    f"""
                    SELECT queue_name, job_id, status, available_at, lease_until,
                           attempts, last_error, payload_json, created_at, updated_at
                    FROM runtime_task_queue
                    WHERE queue_name = ?
                      AND status IN ({placeholders})
                      {due_clause}
                    ORDER BY available_at ASC, updated_at ASC
                    LIMIT ?
                    """,
                    tuple(params),
                ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            payload = self._loads(str(row["payload_json"] or ""), {})
            items.append(
                {
                    "queue_name": str(row["queue_name"] or ""),
                    "job_id": str(row["job_id"] or ""),
                    "status": str(row["status"] or ""),
                    "available_at": float(row["available_at"] or 0),
                    "lease_until": float(row["lease_until"] or 0),
                    "attempts": int(row["attempts"] or 0),
                    "last_error": str(row["last_error"] or ""),
                    "payload": payload if isinstance(payload, dict) else {},
                    "created_at": float(row["created_at"] or 0),
                    "updated_at": float(row["updated_at"] or 0),
                }
            )
        return items

    def lease_runtime_queue_item(
        self,
        queue_name: str,
        job_id: str,
        *,
        lease_seconds: float = 30.0,
    ) -> bool:
        queue_name = self._text(queue_name)
        job_id = self._text(job_id)
        if not queue_name or not job_id:
            return False
        now = time.time()
        lease_until = now + max(1.0, float(lease_seconds or 30.0))
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute("BEGIN IMMEDIATE")
                cursor = conn.execute(
                    """
                    UPDATE runtime_task_queue
                    SET status = 'processing',
                        lease_until = ?,
                        attempts = attempts + 1,
                        updated_at = ?
                    WHERE queue_name = ?
                      AND job_id = ?
                      AND (
                        (status = 'queued' AND available_at <= ?)
                        OR (status = 'processing' AND lease_until > 0 AND lease_until <= ?)
                      )
                    """,
                    (lease_until, now, queue_name, job_id, now, now),
                )
                conn.commit()
                return bool(cursor.rowcount)

    def requeue_runtime_queue_item(
        self,
        queue_name: str,
        job_id: str,
        *,
        available_at: float | None = None,
        error: str = "",
    ) -> bool:
        queue_name = self._text(queue_name)
        job_id = self._text(job_id)
        if not queue_name or not job_id:
            return False
        now = time.time()
        available = float(available_at if available_at is not None else now)
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                cursor = conn.execute(
                    """
                    UPDATE runtime_task_queue
                    SET status = 'queued',
                        available_at = ?,
                        lease_until = 0,
                        last_error = ?,
                        updated_at = ?
                    WHERE queue_name = ? AND job_id = ?
                    """,
                    (available, self._text(error), now, queue_name, job_id),
                )
                conn.commit()
                return bool(cursor.rowcount)

    def runtime_queue_counts(self) -> dict[str, dict[str, int]]:
        if not self.db_path.exists():
            return {}
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    """
                    SELECT queue_name, status, COUNT(*) AS count
                    FROM runtime_task_queue
                    GROUP BY queue_name, status
                    """
                ).fetchall()
        counts: dict[str, dict[str, int]] = {}
        for row in rows:
            queue_name = str(row["queue_name"] or "")
            status = str(row["status"] or "")
            if not queue_name or not status:
                continue
            counts.setdefault(queue_name, {})[status] = int(row["count"] or 0)
        return counts

    def runtime_queue_details(self) -> dict[str, dict[str, int]]:
        if not self.db_path.exists():
            return {}
        now = time.time()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    """
                    SELECT queue_name,
                           SUM(CASE WHEN status = 'queued' AND available_at <= ? THEN 1 ELSE 0 END) AS queued_due,
                           SUM(CASE WHEN status = 'queued' AND available_at > ? THEN 1 ELSE 0 END) AS queued_future,
                           SUM(CASE WHEN status = 'processing' AND lease_until > ? THEN 1 ELSE 0 END) AS processing_active,
                           SUM(CASE WHEN status = 'processing' AND lease_until > 0 AND lease_until <= ? THEN 1 ELSE 0 END) AS processing_expired,
                           SUM(CASE WHEN status IN ('done', 'failed', 'cancelled', 'interrupted') THEN 1 ELSE 0 END) AS terminal
                    FROM runtime_task_queue
                    GROUP BY queue_name
                    """,
                    (now, now, now, now),
                ).fetchall()
        details: dict[str, dict[str, int]] = {}
        for row in rows:
            queue_name = str(row["queue_name"] or "")
            if not queue_name:
                continue
            details[queue_name] = {
                "queued_due": int(row["queued_due"] or 0),
                "queued_future": int(row["queued_future"] or 0),
                "processing_active": int(row["processing_active"] or 0),
                "processing_expired": int(row["processing_expired"] or 0),
                "terminal": int(row["terminal"] or 0),
            }
        return details

    def reset_runtime_queue_incomplete(
        self,
        *,
        status: str = "interrupted",
        error: str = "程序重启，队列状态已重置。",
    ) -> int:
        final_status = self._text(status) or "interrupted"
        now = time.time()
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                cursor = conn.execute(
                    """
                    UPDATE runtime_task_queue
                    SET status = ?, last_error = ?, lease_until = 0, updated_at = ?
                    WHERE status NOT IN ('done', 'failed', 'cancelled', 'interrupted')
                    """,
                    (final_status, self._text(error), now),
                )
                conn.commit()
                return int(cursor.rowcount or 0)

    def cleanup_runtime_queue_items(
        self,
        *,
        retention_seconds: int = 86400,
        max_delete: int = 500,
    ) -> int:
        retention_seconds = max(60, int(retention_seconds or 0))
        max_delete = max(1, min(int(max_delete or 500), 2000))
        cutoff = time.time() - retention_seconds
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    """
                    SELECT queue_name, job_id
                    FROM runtime_task_queue
                    WHERE status IN ('done', 'failed', 'cancelled') AND updated_at < ?
                    ORDER BY updated_at ASC
                    LIMIT ?
                    """,
                    (cutoff, max_delete),
                ).fetchall()
                keys = [
                    (str(row["queue_name"] or ""), str(row["job_id"] or ""))
                    for row in rows
                ]
                for queue_name, job_id in keys:
                    if queue_name and job_id:
                        conn.execute(
                            """
                            DELETE FROM runtime_task_queue
                            WHERE queue_name = ? AND job_id = ?
                            """,
                            (queue_name, job_id),
                        )
                conn.commit()
        return len(keys)

    def import_jsonl_events_once(
        self, source: str, path: str | Path, *, marker: str = ""
    ) -> dict[str, Any]:
        source = self._text(source) or "unknown"
        file_path = Path(path)
        marker_key = marker or f"append_events_legacy_imported:{source}"
        result = {
            "source": source,
            "path": os.fspath(file_path),
            "exists": file_path.exists(),
            "imported": 0,
            "skipped": 0,
            "already_imported": False,
            "error": "",
        }
        if not result["exists"]:
            return result

        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(
                    "SELECT value FROM meta WHERE key = ?",
                    (marker_key,),
                ).fetchone()
                if row:
                    result["already_imported"] = True
                    return result

                conn.execute("BEGIN IMMEDIATE")
                fallback_ts = time.time()
                try:
                    with file_path.open("r", encoding="utf-8") as fh:
                        for line in fh:
                            raw_line = (line or "").strip()
                            if not raw_line:
                                continue
                            try:
                                payload = json.loads(raw_line)
                            except Exception:
                                result["skipped"] += 1
                                continue
                            if not isinstance(payload, dict):
                                result["skipped"] += 1
                                continue
                            conn.execute(
                                """
                                INSERT INTO append_events(source, payload_json, created_at)
                                VALUES (?, ?, ?)
                                """,
                                (
                                    source,
                                    self._json(payload),
                                    self._event_created_at(payload, fallback_ts),
                                ),
                            )
                            result["imported"] += 1
                    conn.execute(
                        "INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)",
                        (
                            marker_key,
                            self._json(
                                {
                                    "path": os.fspath(file_path),
                                    "imported": result["imported"],
                                    "skipped": result["skipped"],
                                    "time": fallback_ts,
                                }
                            ),
                        ),
                    )
                    conn.commit()
                except Exception as exc:
                    conn.rollback()
                    result["error"] = str(exc)
        return result

    def get_last_event_id(self, source: str) -> int:
        source = self._text(source)
        if not source or not self.db_path.exists():
            return 0
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                row = conn.execute(
                    "SELECT MAX(id) AS max_id FROM append_events WHERE source = ?",
                    (source,),
                ).fetchone()
        try:
            return int((row or {})["max_id"] or 0)
        except Exception:
            return 0

    def list_events_after(
        self, source: str, last_id: int = 0, *, limit: int = 200
    ) -> list[dict[str, Any]]:
        source = self._text(source)
        if not source or not self.db_path.exists():
            return []
        try:
            last_id = int(last_id or 0)
        except Exception:
            last_id = 0
        limit = max(1, min(int(limit or 200), 1000))
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                rows = conn.execute(
                    """
                    SELECT id, payload_json, created_at
                    FROM append_events
                    WHERE source = ? AND id > ?
                    ORDER BY id ASC
                    LIMIT ?
                    """,
                    (source, last_id, limit),
                ).fetchall()
        events: list[dict[str, Any]] = []
        for row in rows:
            payload = self._loads(str(row["payload_json"] or ""), {})
            if not isinstance(payload, dict):
                continue
            events.append(
                {
                    "id": int(row["id"] or 0),
                    "source": source,
                    "payload": payload,
                    "created_at": float(row["created_at"] or 0),
                }
            )
        return events

    def cleanup_append_events(
        self,
        *,
        source: str = "",
        retention_seconds: int = 3 * 24 * 3600,
        keep_latest: int = 5000,
        max_delete: int = 2000,
    ) -> dict[str, int]:
        source = self._text(source)
        retention_seconds = max(60, int(retention_seconds or 0))
        keep_latest = max(0, int(keep_latest or 0))
        max_delete = max(1, min(int(max_delete or 2000), 10000))
        now = time.time()
        cutoff = now - retention_seconds
        removed = 0
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                params: list[Any] = []
                source_clause = ""
                if source:
                    source_clause = "AND source = ?"
                    params.append(source)
                protected_min_id = 0
                if keep_latest > 0:
                    row = conn.execute(
                        f"""
                        SELECT MIN(id) AS min_id
                        FROM (
                            SELECT id
                            FROM append_events
                            WHERE 1=1 {source_clause}
                            ORDER BY id DESC
                            LIMIT ?
                        )
                        """,
                        (*params, keep_latest),
                    ).fetchone()
                    protected_min_id = int((row or {})["min_id"] or 0) if row else 0
                delete_params: list[Any] = [cutoff]
                delete_source_clause = ""
                if source:
                    delete_source_clause = "AND source = ?"
                    delete_params.append(source)
                if protected_min_id > 0:
                    delete_params.append(protected_min_id)
                    protected_clause = "AND id < ?"
                else:
                    protected_clause = ""
                rows = conn.execute(
                    f"""
                    SELECT id
                    FROM append_events
                    WHERE created_at < ? {delete_source_clause} {protected_clause}
                    ORDER BY id ASC
                    LIMIT ?
                    """,
                    (*delete_params, max_delete),
                ).fetchall()
                ids = [int(row["id"] or 0) for row in rows if int(row["id"] or 0) > 0]
                if ids:
                    placeholders = ",".join("?" for _ in ids)
                    conn.execute(
                        f"DELETE FROM append_events WHERE id IN ({placeholders})",
                        tuple(ids),
                    )
                    removed = len(ids)
                conn.commit()
        return {"removed": removed, "source": source or "all"}

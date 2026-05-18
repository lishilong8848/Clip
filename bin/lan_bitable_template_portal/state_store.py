# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
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


DEFAULT_STATE_DB_NAME = "lan_portal_state.sqlite3"


class LanPortalStateStore:
    """SQLite-backed runtime state for the LAN portal.

    The database stores runtime data and user configuration. Legacy JSON files
    are migration inputs only and are never deleted or overwritten here.
    """

    SCHEMA_VERSION = 3
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

    def __init__(self, db_path: str | Path | None = None):
        self.db_path = Path(db_path or get_data_file_path(DEFAULT_STATE_DB_NAME))
        self._lock = threading.RLock()
        self._initialized = False

    def _connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.db_path), timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 5000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        return conn

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
                raw_record_id TEXT,
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
            "INSERT OR REPLACE INTO meta(key, value) VALUES('schema_version', ?)",
            (str(self.SCHEMA_VERSION),),
        )
        conn.commit()
        self._initialized = True

    @staticmethod
    def _text(value: Any) -> str:
        return str(value or "").strip()

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

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

    def _identity_for_item(self, item: dict[str, Any]) -> str:
        active_item_id = self._text(item.get("active_item_id"))
        if active_item_id:
            return f"active:{active_item_id}"
        record_id = self._text(item.get("record_id") or item.get("raw_record_id"))
        if record_id:
            return f"record:{record_id}"
        source_record_id = self._text(item.get("source_record_id"))
        work_type = self._text(item.get("work_type")) or "maintenance"
        if source_record_id:
            return f"{work_type}:source:{source_record_id}"
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

    def replace_ongoing_items(self, items: list[dict[str, Any]] | None) -> dict[str, Any]:
        now = time.time()
        snapshot_id = uuid.uuid4().hex
        normalized = [dict(item) for item in (items or []) if isinstance(item, dict)]
        with self._lock:
            with closing(self._connect()) as conn:
                self._ensure_schema_locked(conn)
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("DELETE FROM ongoing_items")
                for item in normalized:
                    building_codes = item.get("building_codes")
                    if not isinstance(building_codes, list):
                        building_codes = []
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO ongoing_items(
                            identity,
                            active_item_id,
                            record_id,
                            raw_record_id,
                            source_record_id,
                            work_type,
                            notice_type,
                            title,
                            building,
                            building_codes_json,
                            payload_json,
                            updated_at,
                            snapshot_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            self._identity_for_item(item),
                            self._text(item.get("active_item_id")),
                            self._text(item.get("record_id")),
                            self._text(item.get("raw_record_id")),
                            self._text(item.get("source_record_id")),
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
                    if not str(open_id or "").strip() or not isinstance(user_cfg, dict):
                        continue
                    scopes = user_cfg.get("scopes") if isinstance(user_cfg.get("scopes"), list) else []
                    conn.execute(
                        """
                        INSERT INTO auth_permissions(
                            open_id, name, role, scopes_json, enabled, payload_json, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            str(open_id).strip(),
                            self._text(user_cfg.get("name")),
                            self._text(user_cfg.get("role")) or "building",
                            self._json(scopes),
                            0 if user_cfg.get("enabled") is False else 1,
                            self._json(user_cfg),
                            now,
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

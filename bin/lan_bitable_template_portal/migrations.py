# -*- coding: utf-8 -*-
from __future__ import annotations

import sqlite3
import time
from typing import Any


MIGRATIONS: tuple[tuple[int, str], ...] = (
    (1, "baseline_runtime_documents"),
    (2, "settings_and_permissions"),
    (3, "notice_state_documents"),
    (4, "append_events_and_runtime_state"),
    (5, "event_outbox"),
    (6, "runtime_task_queue"),
    (7, "qt_active_items"),
    (8, "clipboard_candidates"),
    (9, "dialog_sessions"),
    (10, "repair_link_tasks"),
    (11, "source_scope_snapshots"),
    (12, "source_snapshot_manifest"),
    (13, "notice_undo_actions"),
)


def run_schema_migrations(conn: sqlite3.Connection, *, target_version: int) -> dict[str, Any]:
    """Record idempotent schema migration state.

    Table creation remains intentionally idempotent in the state store for
    compatibility with existing user databases. This registry is the canonical
    version ledger for future schema changes and startup diagnostics.
    """

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at REAL NOT NULL
        )
        """
    )
    applied_rows = conn.execute("SELECT version FROM schema_migrations").fetchall()
    applied = {int(row[0]) for row in applied_rows}
    now = time.time()
    inserted: list[int] = []
    for version, name in MIGRATIONS:
        if version > int(target_version):
            continue
        if version in applied:
            continue
        conn.execute(
            """
            INSERT OR IGNORE INTO schema_migrations(version, name, applied_at)
            VALUES (?, ?, ?)
            """,
            (version, name, now),
        )
        inserted.append(version)
    conn.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES('schema_migration_version', ?)",
        (str(int(target_version)),),
    )
    return {
        "target_version": int(target_version),
        "applied_count": len(applied) + len(inserted),
        "inserted": inserted,
    }


def schema_health(
    conn: sqlite3.Connection,
    *,
    target_version: int,
    required_tables: list[str],
    required_indexes: list[str] | None = None,
) -> dict[str, Any]:
    tables = {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    indexes = {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()
    }
    migration_rows = conn.execute(
        "SELECT version FROM schema_migrations ORDER BY version"
    ).fetchall()
    migration_versions = [int(row[0]) for row in migration_rows]
    missing_tables = [name for name in required_tables if name not in tables]
    missing_indexes = [
        name for name in (required_indexes or []) if name and name not in indexes
    ]
    latest = max(migration_versions or [0])
    ok = (
        not missing_tables
        and not missing_indexes
        and latest >= int(target_version)
    )
    return {
        "ok": ok,
        "target_version": int(target_version),
        "latest_migration_version": latest,
        "missing_tables": missing_tables,
        "missing_indexes": missing_indexes,
        "migration_count": len(migration_versions),
    }

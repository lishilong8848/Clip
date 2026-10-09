# -*- coding: utf-8 -*-
"""Durable SQLite cache for the repair device ledger.

The cache keeps the last successful replacement of flat equipment dictionaries in a
dedicated SQLite file, indexed for filter fields and permission scopes. Replacements
are atomic: a generator failure, duplicate record_id, or a malformed record rolls
back and leaves the previous snapshot readable. Query/filter/option calls use
connection-per-call snapshots so readers never see a half-written replacement.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import unicodedata
from typing import Any, Iterable, Mapping

# Canonical flat equipment fields carried verbatim (besides record_id + scope_codes).
TEXT_FIELDS: tuple[str, ...] = (
    "设备编号",
    "机楼",
    "系统名称",
    "大设备类型",
    "设备名称",
    "产品其它参数",
    "品牌",
    "安装位置",
    "型号",
    "设备类型标识",
    "容量",
)

# Whitelisted filter/options keys, each with a dedicated normalized index.
FILTER_FIELDS: tuple[str, ...] = (
    "机楼",
    "系统名称",
    "大设备类型",
    "设备名称",
)

_FILTER_COLUMN: dict[str, str] = {
    "机楼": "building_norm",
    "系统名称": "system_name_norm",
    "大设备类型": "big_device_type_norm",
    "设备名称": "device_name_norm",
}

_OPTION_COLUMN: dict[str, str] = {
    "机楼": '"building"',
    "系统名称": '"system_name"',
    "大设备类型": '"big_device_type"',
    "设备名称": '"device_name"',
}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS records (
    record_id          TEXT PRIMARY KEY,
    payload            TEXT NOT NULL,
    scope_csv          TEXT NOT NULL,
    building           TEXT NOT NULL,
    system_name        TEXT NOT NULL,
    big_device_type    TEXT NOT NULL,
    device_name        TEXT NOT NULL,
    device_num         TEXT NOT NULL,
    building_norm      TEXT NOT NULL,
    system_name_norm   TEXT NOT NULL,
    big_device_type_norm TEXT NOT NULL,
    device_name_norm   TEXT NOT NULL,
    device_num_norm    TEXT NOT NULL,
    search_norm        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ledger_building   ON records(building_norm);
CREATE INDEX IF NOT EXISTS idx_ledger_system     ON records(system_name_norm);
CREATE INDEX IF NOT EXISTS idx_ledger_big_type   ON records(big_device_type_norm);
CREATE INDEX IF NOT EXISTS idx_ledger_device     ON records(device_name_norm);
CREATE INDEX IF NOT EXISTS idx_ledger_scope      ON records(scope_csv);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

_STAGE_DDL = """
CREATE TEMP TABLE ledger_stage (
    record_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    scope_csv TEXT NOT NULL,
    building TEXT NOT NULL,
    system_name TEXT NOT NULL,
    big_device_type TEXT NOT NULL,
    device_name TEXT NOT NULL,
    device_num TEXT NOT NULL,
    building_norm TEXT NOT NULL,
    system_name_norm TEXT NOT NULL,
    big_device_type_norm TEXT NOT NULL,
    device_name_norm TEXT NOT NULL,
    device_num_norm TEXT NOT NULL,
    search_norm TEXT NOT NULL
)
"""


def _norm(value: Any) -> str:
    return unicodedata.normalize("NFKC", str(value or "")).casefold().strip()


def _escape_like(token: str) -> str:
    """Escape SQL LIKE wildcards so caller-provided text is literal."""
    return token.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_").replace("[", "\\[")


def _coerce_scope_codes(value: Any) -> list[str]:
    """Return scope codes as a list of strings, rejecting scalar/string inputs."""
    if value is None:
        return []
    if isinstance(value, (str, bytes)) or not hasattr(value, "__iter__"):
        raise ValueError("scope_codes 必须是列表等可迭代对象，不能是字符串")
    out = [str(s) for s in value]
    return [s for s in out if s]


def _payload_dict(record: Mapping[str, Any], *, required_fields: tuple[str, ...]) -> dict[str, Any]:
    """Copy the flat record with a normalized record_id and list scope_codes."""
    out = dict(record)
    out["record_id"] = str(record["record_id"]).strip()
    for field in required_fields:
        out.setdefault(field, "")
    out["scope_codes"] = _coerce_scope_codes(record.get("scope_codes"))
    return out


def _json_same(a: Any, b: Any) -> int:
    """SQLite helper: 1 if two JSON payload strings describe equal content.

    ``json.loads`` dict equality is key-order independent while still comparing
    list order (e.g. ``scope_codes``) with order sensitivity, so reordered incoming
    dicts never count as changed but genuine scope/content edits do.
    """
    if a == b:
        return 1
    try:
        return 1 if json.loads(a) == json.loads(b) else 0
    except (TypeError, ValueError):
        return 0


class LedgerCatalog:
    """Durable equipment cache backed by one small SQLite file.

    ``path`` may be a :class:`pathlib.Path` or a ``str``; the parent directory is
    created on demand.  All public mutators use a single atomic transaction; readers
    use a short read snapshot so a mid-replace state is never visible.
    """

    def __init__(self, path: str | Any):
        self._path = str(path)
        os.makedirs(os.path.dirname(os.path.abspath(self._path)), exist_ok=True)
        self._schema_lock = threading.Lock()
        self._schema_ready = False

    # ------------------------------------------------------------------ plumbing
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.create_function("ledger_payload_same", 2, _json_same, deterministic=True)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        if not self._schema_ready:
            with self._schema_lock:
                if not self._schema_ready:
                    conn.executescript(_SCHEMA)
                    conn.commit()
                    self._schema_ready = True
        return conn

    # --------------------------------------------------------------- replacement
    def replace(self, records: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
        """Atomically replace the whole ledger with ``records``.

        ``records`` may be an iterator/generator; it is consumed inside the same
        transaction as the swap.  Any failure (bad ID, duplicate ID, generator
        exception) rolls the transaction back so the previous cache remains readable.
        An empty successful source is a valid ready cache.
        """
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DROP TABLE IF EXISTS ledger_stage")
            conn.execute(_STAGE_DDL)
            insert_sql = (
                "INSERT INTO ledger_stage(record_id,payload,scope_csv,building,"
                "system_name,big_device_type,device_name,device_num,building_norm,"
                "system_name_norm,big_device_type_norm,device_name_norm,device_num_norm,"
                "search_norm)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            )
            for raw in records:
                if not isinstance(raw, Mapping):
                    raise ValueError("每条记录必须是字典")
                record_id = raw.get("record_id")
                if record_id is None:
                    raise ValueError("记录缺少有效的 record_id")
                record_id = str(record_id).strip()
                if not record_id:
                    raise ValueError("记录缺少有效的 record_id")

                payload = _payload_dict(raw, required_fields=TEXT_FIELDS)
                normalized_scopes = [_norm(s) for s in payload["scope_codes"]]
                normalized_scopes = [s for s in normalized_scopes if s]
                if normalized_scopes:
                    scope_csv = " " + " ".join(sorted(set(normalized_scopes))) + " "
                else:
                    scope_csv = ""

                building = str(payload.get("机楼") or "")
                system_name = str(payload.get("系统名称") or "")
                big_device_type = str(payload.get("大设备类型") or "")
                device_name = str(payload.get("设备名称") or "")
                device_num = str(payload.get("设备编号") or "")
                building_norm = _norm(building)
                system_name_norm = _norm(system_name)
                big_device_type_norm = _norm(big_device_type)
                device_name_norm = _norm(device_name)
                device_num_norm = _norm(device_num)

                pieces = [_norm(payload.get(f) or "") for f in TEXT_FIELDS]
                pieces = [p for p in pieces if p]
                search_norm = " ".join(pieces)

                try:
                    conn.execute(
                        insert_sql,
                        (
                            record_id,
                            json.dumps(payload, ensure_ascii=False),
                            scope_csv,
                            building,
                            system_name,
                            big_device_type,
                            device_name,
                            device_num,
                            building_norm,
                            system_name_norm,
                            big_device_type_norm,
                            device_name_norm,
                            device_num_norm,
                            search_norm,
                        ),
                    )
                except sqlite3.IntegrityError:
                    raise ValueError(f"设备记录 record_id 重复: {record_id}") from None

            # Reconcile only after the complete source has validated; preserve rowids.
            conn.execute(
                """
                DELETE FROM records
                WHERE NOT EXISTS (
                    SELECT 1 FROM ledger_stage s WHERE s.record_id = records.record_id
                )
                """
            )
            conn.execute(
                """
                INSERT INTO records(record_id,payload,scope_csv,building,system_name,
                    big_device_type,device_name,device_num,building_norm,system_name_norm,
                    big_device_type_norm,device_name_norm,device_num_norm,search_norm)
                SELECT s.record_id,s.payload,s.scope_csv,s.building,s.system_name,
                    s.big_device_type,s.device_name,s.device_num,s.building_norm,s.system_name_norm,
                    s.big_device_type_norm,s.device_name_norm,s.device_num_norm,s.search_norm
                FROM ledger_stage s
                WHERE NOT EXISTS (
                    SELECT 1 FROM records r WHERE r.record_id = s.record_id
                )
                """
            )
            conn.execute(
                """
                UPDATE records
                SET payload = s.payload,
                    scope_csv = s.scope_csv,
                    building = s.building,
                    system_name = s.system_name,
                    big_device_type = s.big_device_type,
                    device_name = s.device_name,
                    device_num = s.device_num,
                    building_norm = s.building_norm,
                    system_name_norm = s.system_name_norm,
                    big_device_type_norm = s.big_device_type_norm,
                    device_name_norm = s.device_name_norm,
                    device_num_norm = s.device_num_norm,
                    search_norm = s.search_norm
                FROM ledger_stage s
                WHERE s.record_id = records.record_id
                  AND NOT ledger_payload_same(records.payload, s.payload)
                """
            )
            now = time.strftime("%Y-%m-%dT%H:%M:%S%z")
            conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", ("ready", "1"))
            conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", ("refreshed_at", now))
            conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", ("error", ""))
            conn.execute("DROP TABLE IF EXISTS ledger_stage")
            conn.commit()
        except BaseException:
            conn.rollback()
            try:
                conn.execute("DROP TABLE IF EXISTS ledger_stage")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
        return self.status()

    # -------------------------------------------------------------------- status
    def status(self) -> dict[str, Any]:
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            ready_row = conn.execute("SELECT value FROM meta WHERE key='ready'").fetchone()
            refreshed_row = conn.execute("SELECT value FROM meta WHERE key='refreshed_at'").fetchone()
            error_row = conn.execute("SELECT value FROM meta WHERE key='error'").fetchone()
            count = conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]
            conn.commit()
            return {
                "ready": bool(int(ready_row[0])) if ready_row else False,
                "record_count": int(count),
                "refreshed_at": refreshed_row[0] if refreshed_row else None,
                "error": (error_row[0] or None) if error_row else None,
            }
        finally:
            conn.close()

    def mark_error(self, message: str) -> dict[str, Any]:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)",
                ("error", str(message)),
            )
            conn.commit()
            return self.status()
        finally:
            conn.close()

    # --------------------------------------------------------------------- query
    def query(
        self,
        *,
        query: str = "",
        filters: Mapping[str, Any] | None = None,
        allowed_scopes: Iterable[str] | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            where_sql, params = self._build_where(query, filters, allowed_scopes)

            count = conn.execute(f"SELECT COUNT(*) FROM records WHERE {where_sql}", params).fetchone()[0]

            page_size = max(1, min(int(page_size), 100))
            page = max(1, int(page))
            offset = (page - 1) * page_size

            row_sql = (
                f"SELECT payload FROM records WHERE {where_sql} "
                "ORDER BY device_num_norm, record_id COLLATE NOCASE LIMIT ? OFFSET ?"
            )
            rows = conn.execute(row_sql, params + [page_size, offset]).fetchall()

            options = self._fetch_options(conn, allowed_scopes)
            conn.commit()
            return {
                "records": [json.loads(r[0]) for r in rows],
                "total": int(count),
                "page": page,
                "page_size": page_size,
                "options": options,
                "cache": self.status(),
            }
        finally:
            conn.close()

    def get_records(
        self,
        record_ids: Iterable[str],
        *,
        allowed_scopes: Iterable[str] | None = None,
    ) -> list[dict[str, Any]]:
        ids = [str(i) for i in record_ids]
        if not ids:
            return []
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            scope_sql, scope_params = self._scope_window(allowed_scopes)
            found: dict[str, dict[str, Any]] = {}
            chunk = 500
            for start in range(0, len(ids), chunk):
                part = ids[start:start + chunk]
                marks = ",".join("?" for _ in part)
                base = f"SELECT record_id, payload FROM records WHERE record_id IN ({marks})"
                if scope_sql:
                    base += f" AND {scope_sql}"
                rows = conn.execute(base, part + scope_params).fetchall()
                for row in rows:
                    found[str(row[0])] = json.loads(row[1])
            conn.commit()
            return [found[i] for i in ids if i in found]
        finally:
            conn.close()

    # --------------------------------------------------------------- internals
    def _build_where(
        self,
        query: str,
        filters: Mapping[str, Any] | None,
        allowed_scopes: Iterable[str] | None,
    ) -> tuple[str, list[Any]]:
        clauses: list[str] = []
        params: list[Any] = []

        scope_sql, scope_params = self._scope_window(allowed_scopes)
        if scope_sql:
            clauses.append(scope_sql)
            params.extend(scope_params)

        filters = filters or {}
        for key, value in filters.items():
            if key not in FILTER_FIELDS:
                raise ValueError(f"不支持的过滤字段: {key}")
            normalized = _norm(value)
            if not normalized:
                continue
            clauses.append(f"records.{_FILTER_COLUMN[key]} = ?")
            params.append(normalized)

        for keyword in (str(query or "").split()):
            normalized = _norm(keyword)
            if normalized:
                clauses.append("records.search_norm LIKE ? ESCAPE '\\'")
                params.append("%" + _escape_like(normalized) + "%")

        where_sql = " AND ".join(clauses) if clauses else "1"
        return where_sql, params

    def _scope_window(self, allowed_scopes: Iterable[str] | None) -> tuple[str, list[Any]]:
        """Return permission WHERE fragment. None => all; [] => none."""
        if allowed_scopes is None:
            return "", []
        as_list = list(allowed_scopes)
        codes = [_norm(s) for s in as_list]
        codes = [c for c in codes if c]
        pub_public = "(records.building_norm = '' AND records.scope_csv = '')"
        if not codes:
            return "(0)", []
        terms = ["INSTR(records.scope_csv, ?) > 0" for _ in codes]
        params = [" " + c + " " for c in codes]
        return f"( ( {' OR '.join(terms)} ) OR {pub_public} )", params

    def _fetch_options(self, conn: sqlite3.Connection, allowed_scopes: Iterable[str] | None) -> dict[str, list[str]]:
        scope_sql, scope_params = self._scope_window(allowed_scopes)
        base = "FROM records WHERE " + scope_sql if scope_sql else "FROM records WHERE 1"
        options: dict[str, list[str]] = {}
        for label in FILTER_FIELDS:
            quoted = _OPTION_COLUMN[label]
            sql = (
                f"SELECT DISTINCT {quoted} AS v {base} AND {quoted} <> '' "
                "ORDER BY v COLLATE NOCASE"
            )
            options[label] = [str(r[0]) for r in conn.execute(sql, scope_params).fetchall()]
        return options

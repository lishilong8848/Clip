"""Independent assistant JSON document store and one-time migration from LanPortalStateStore.

This module intentionally depends only on the Python standard library (sqlite3,
json, pathlib, hashlib, threading, shutil, os).  It does not import the giant
lan_bitable_template_portal package and does not launch any services.

An AssistantStore owns two things under ``root``:
  * ``root/assistant.sqlite3``  - WAL-mode SQLite database with ``documents`` and
    ``meta`` tables.
  * ``root/files``             - attachment bytes copied during migration,
    preserving the account-relative layout of the legacy ``lighthouse_assistant
    /files`` directory.

The optional migration is read-only against the previous LanPortalStateStore
database (``json_documents`` opened with SQLite ``mode=ro``, read inside a single
shared transaction) and only copies the assistant namespaces.  A completed
migration is identified by a stable canonical source path, so unrelated business
DB updates or deletion of the old database/file tree never cause a completed
migration to be re-run or to overwrite user edits.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import threading
import time
import uuid
from contextlib import closing
from pathlib import Path

MIGRATION_VERSION = 1
ASSISTANT_NAMESPACES = frozenset(
    {
        "lighthouse_ai",
        "lighthouse_model_capabilities",
        "lighthouse_runs",
        "lighthouse_messages",
        "lighthouse_agent_plans",
        "lighthouse_files",
        "lighthouse_appearance",
        "lighthouse_question_text",
        "lighthouse_installed_skills",
    }
)
# Active run statuses that get migration_interrupted=True.  Completed results are
# never modified and plans are never replayed.
ACTIVE_RUN_STATUSES = frozenset({"queued", "running"})
# Bounded SQLite busy timeout: callers wait at most one second on a lock.
_BUSY_TIMEOUT_SECONDS = 1.0
_MARKER_KEY = "migration_marker"
_HASH_CHUNK = 1024 * 1024


def _json_dumps(value) -> str:
    """Strict JSON encoding that rejects non-finite floats."""
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def _reject_constant(name: str):
    raise ValueError("Non-finite JSON numbers are not allowed in assistant documents")


def _json_loads_strict(text: str):
    """Parse source JSON strictly; NaN/Infinity are rejected, never silently dropped."""
    return json.loads(text, parse_constant=_reject_constant)


def _glob_escape(text: str) -> str:
    """Escape SQL GLOB wildcards for a literal, case-sensitive key prefix match."""
    out = []
    for ch in text:
        if ch == "*":
            out.append("[*]")
        elif ch == "?":
            out.append("[?]")
        elif ch == "[":
            out.append("[[]")
        elif ch == "]":
            out.append("[]]")
        else:
            out.append(ch)
    return "".join(out) + "*"


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path) -> str:
    """Streaming SHA256 so unbounded files are never read into memory at once."""
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            block = fh.read(_HASH_CHUNK)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _is_link(path) -> bool:
    """True for symlinks and Windows junctions (including a component node)."""
    try:
        if path.is_symlink():
            return True
    except OSError:
        pass
    try:
        if path.is_junction():
            return True
    except (OSError, AttributeError):
        pass
    return False


def _reject_link_components(root, path) -> None:
    """Reject any symlink/junction in the path, even if it eventually resolves in-root."""
    root = Path(root).resolve()
    path = Path(path)
    try:
        rel = path.relative_to(root)
    except ValueError:
        return
    probe = root
    if _is_link(probe):
        raise ValueError("A configured root path contains a link component")
    for part in rel.parts:
        probe = probe / part
        if _is_link(probe):
            raise ValueError("Attachment path contains a link component")


class AssistantStore:
    """Thread-safe JSON document store for assistant-only namespaces."""

    def __init__(
        self,
        root,
        *,
        legacy_db=None,
        legacy_files=None,
    ):
        self.root = Path(root).resolve()
        self.db_path = self.root / "assistant.sqlite3"
        self.files_root = self.root / "files"
        self.legacy_db = str(legacy_db) if legacy_db else None
        self.legacy_files = str(legacy_files) if legacy_files else None
        # Existing threading lock shared by all operations on this instance.
        self._lock = threading.RLock()
        self._prepare()
        if self.legacy_db:
            self._migrate()

    # ------------------------------------------------------------------ schema

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=_BUSY_TIMEOUT_SECONDS)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=%.0f" % (_BUSY_TIMEOUT_SECONDS * 1000.0))
        return conn

    def _prepare(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.files_root.mkdir(parents=True, exist_ok=True)
        with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS documents (
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
                    CREATE TABLE IF NOT EXISTS meta (
                        key TEXT PRIMARY KEY,
                        value_json TEXT NOT NULL,
                        updated_at REAL NOT NULL
                    )
                    """
                )
                conn.commit()

    # -------------------------------------------------------------- validation

    def _require_namespace(self, namespace) -> None:
        if namespace not in ASSISTANT_NAMESPACES:
            raise ValueError("Unknown assistant namespace")

    @staticmethod
    def _require_key(key) -> None:
        if not isinstance(key, str) or not key.strip():
            raise ValueError("Document key must be a non-empty string")

    # ------------------------------------------------------------------- API

    def get_document(self, namespace, key):
        self._require_namespace(namespace)
        self._require_key(key)
        with self._lock:
            with closing(self._connect()) as conn:
                row = conn.execute(
                    "SELECT payload_json FROM documents WHERE namespace=? AND key=?",
                    (namespace, key),
                ).fetchone()
        if row is None:
            return None
        try:
            payload = _json_loads_strict(str(row["payload_json"]))
        except (ValueError, TypeError) as exc:
            raise ValueError("Assistant document is corrupt; original data was preserved") from exc
        if not isinstance(payload, dict):
            raise ValueError("Assistant document is not an object; original data was preserved")
        return payload

    def put_document(self, namespace, key, payload):
        self._require_namespace(namespace)
        self._require_key(key)
        if not isinstance(payload, dict):
            raise ValueError("Payload must be a JSON object")
        try:
            payload_json = _json_dumps(payload)
        except (TypeError, ValueError) as exc:
            raise ValueError("Payload must be a finite JSON object") from exc
        now = time.time()
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    """
                    INSERT INTO documents(namespace, key, payload_json, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(namespace, key) DO UPDATE SET
                        payload_json = excluded.payload_json,
                        updated_at = excluded.updated_at
                    """,
                    (namespace, key, payload_json, now),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    def put_documents(self, namespace, payloads):
        self._require_namespace(namespace)
        if not isinstance(payloads, dict) or not payloads:
            raise ValueError("payloads must be a non-empty mapping")
        prepared = []
        now = time.time()
        for key, payload in payloads.items():
            self._require_key(key)
            if not isinstance(payload, dict):
                raise ValueError("Payload must be a JSON object")
            try:
                payload_json = _json_dumps(payload)
            except (TypeError, ValueError) as exc:
                raise ValueError("Payload must be a finite JSON object") from exc
            prepared.append((namespace, key, payload_json, now))
        # Validate everything before opening a transaction: the batch is atomic.
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.executemany(
                    """
                    INSERT INTO documents(namespace, key, payload_json, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(namespace, key) DO UPDATE SET
                        payload_json = excluded.payload_json,
                        updated_at = excluded.updated_at
                    """,
                    prepared,
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    def delete_document(self, namespace, key):
        self._require_namespace(namespace)
        self._require_key(key)
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "DELETE FROM documents WHERE namespace=? AND key=?",
                    (namespace, key),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    def list_documents(self, namespace, *, key_prefix=""):
        self._require_namespace(namespace)
        if key_prefix is None:
            key_prefix = ""
        if not isinstance(key_prefix, str):
            raise ValueError("key_prefix must be a string")
        with self._lock:
            with closing(self._connect()) as conn:
                if key_prefix:
                    pattern = _glob_escape(key_prefix)
                    rows = conn.execute(
                        """
                        SELECT key, payload_json, updated_at
                        FROM documents
                        WHERE namespace=? AND key GLOB ?
                        ORDER BY key ASC
                        """,
                        (namespace, pattern),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT key, payload_json, updated_at
                        FROM documents
                        WHERE namespace=?
                        ORDER BY key ASC
                        """,
                        (namespace,),
                    ).fetchall()
        result = []
        for row in rows:
            try:
                payload = _json_loads_strict(str(row["payload_json"]))
            except (ValueError, TypeError) as exc:
                raise ValueError("Assistant document is corrupt; original data was preserved") from exc
            if not isinstance(payload, dict):
                raise ValueError("Assistant document is not an object; original data was preserved")
            result.append(
                {
                    "key": str(row["key"]),
                    "payload": payload,
                    "updated_at": float(row["updated_at"] or 0),
                }
            )
        return result

    # --------------------------------------------------------------- migration

    def _legacy_source(self) -> str:
        """Stable canonical identity of the legacy DB path (case-normalized on Windows).

        Deliberately ignores file size/mtime and does not require the file to
        exist, so unrelated business DB changes or deletion of the old database
        never invalidate a completed migration for the same canonical source.
        """
        absolute = os.path.abspath(self.legacy_db)
        return os.path.normcase(absolute)

    def _read_marker(self):
        with self._lock:
            with closing(self._connect()) as conn:
                row = conn.execute(
                    "SELECT value_json FROM meta WHERE key=?", (_MARKER_KEY,)
                ).fetchone()
        if not row:
            return None
        try:
            marker = json.loads(str(row["value_json"]))
        except (ValueError, TypeError):
            return None
        return marker if isinstance(marker, dict) else None

    def _same_installation(self, marker, source):
        if marker.get('source') == source:
            return True
        old_root = marker.get('root')
        if not isinstance(old_root, str) or not Path(old_root).is_absolute():
            return False
        if os.path.normcase(str(self.root)) == os.path.normcase(old_root):
            return False
        try:
            previous = Path(marker['source']).relative_to(Path(old_root).parent)
            current = Path(source).relative_to(self.root.parent)
        except (ValueError, KeyError, TypeError):
            return False
        return os.path.normcase(str(previous)) == os.path.normcase(str(current))

    def _record_interrupted(self, message: str) -> None:
        """Best-effort interrupted marker so a later start can resume safely."""
        marker = {
            "version": MIGRATION_VERSION,
            "source": self._legacy_source(),
            "completed": False,
            "interrupted": True,
            "interrupted_at": time.time(),
            "interrupted_reason": "migration did not complete",
            "namespaces": sorted(ASSISTANT_NAMESPACES),
        }
        try:
            with self._lock:
                conn = self._connect()
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    conn.execute(
                        "INSERT OR REPLACE INTO meta(key, value_json, updated_at) VALUES (?, ?, ?)",
                        (_MARKER_KEY, _json_dumps(marker), time.time()),
                    )
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
                finally:
                    conn.close()
        except Exception:
            # The interrupted marker is advisory only; never mask the real error.
            pass

    def _migrate(self) -> None:
        if not self.legacy_db:
            return
        with self._lock:
            marker = self._read_marker()
            source = self._legacy_source()
            if marker:
                version = int(marker.get("version") or 0)
                if version > MIGRATION_VERSION:
                    raise ValueError(
                        "Assistant database contains newer service data; refusing to overwrite"
                    )
                if self._same_installation(marker, source) and version == MIGRATION_VERSION:
                    if marker.get("completed"):
                        # Already migrated from this canonical source; never overwrite
                        # edits and never require the old DB/file tree to still exist.
                        return
                else:
                    if marker.get("completed") and version >= MIGRATION_VERSION:
                        raise ValueError(
                            "Assistant database has migration data from a different source; refusing to overwrite"
                        )
            # Now the old source is required for an actual (re)run.
            if not Path(self.legacy_db).is_file():
                raise ValueError("legacy_db is missing and no completed migration exists")
            if self.legacy_files is None:
                raise ValueError("legacy_files is required for migration with attachments")
            # Guard against an unmarked assistant DB that already holds documents.
            with closing(self._connect()) as conn:
                existing = conn.execute(
                    "SELECT COUNT(*) AS n FROM documents WHERE namespace IN (%s)"
                    % ",".join("?" * len(ASSISTANT_NAMESPACES)),
                    tuple(sorted(ASSISTANT_NAMESPACES)),
                ).fetchone()
                if existing and int(existing["n"]) > 0 and marker is None:
                    raise ValueError(
                        "Assistant database already contains documents without a migration marker; refusing to overwrite"
                    )
            try:
                self._run_migration()
            except Exception as exc:
                # Do not log the underlying message (it may summarize filename or
                # payload state); record an opaque interrupted marker only.
                self._record_interrupted(str(type(exc).__name__))
                raise

    def _read_legacy_rows(self):
        """Read legacy json_documents in a single shared transaction (consistent snapshot)."""
        legacy_path = Path(self.legacy_db).resolve()
        uri = legacy_path.as_uri() + "?mode=ro"
        src = sqlite3.connect(uri, uri=True, timeout=_BUSY_TIMEOUT_SECONDS)
        src.row_factory = sqlite3.Row
        try:
            src.execute("PRAGMA busy_timeout=%.0f" % (_BUSY_TIMEOUT_SECONDS * 1000.0))
            src.execute("BEGIN")
            try:
                namespaces = sorted(ASSISTANT_NAMESPACES)
                result = {}
                for ns in namespaces:
                    rows = src.execute(
                        """
                        SELECT namespace, key, payload_json, updated_at
                        FROM json_documents
                        WHERE namespace=?
                        ORDER BY key ASC
                        """,
                        (ns,),
                    ).fetchall()
                    result[ns] = [
                        {
                            "namespace": str(row["namespace"]),
                            "key": str(row["key"]),
                            "payload_json": str(row["payload_json"]),
                            "updated_at": float(row["updated_at"] or 0),
                        }
                        for row in rows
                    ]
                return result
            finally:
                src.rollback()
        finally:
            src.close()

    def _resolve_attachment(self, payload):
        """Validate and resolve a lighthouse_files payload returning (source, relative, source_sha)."""
        raw_path = payload.get("path")
        if not isinstance(raw_path, str) or not raw_path.strip():
            raise ValueError("lighthouse_files document is missing its path")
        legacy_root = Path(self.legacy_files).resolve()
        candidate = Path(raw_path)
        absolute = candidate if candidate.is_absolute() else (legacy_root / candidate)
        # Reject any symlink/junction component even if it resolves inside root.
        _reject_link_components(legacy_root, absolute)
        resolved = absolute.resolve()
        if not resolved.is_relative_to(legacy_root):
            raise ValueError("Attachment path escapes the files root")
        _reject_link_components(legacy_root, resolved)
        if not resolved.is_file():
            raise ValueError("Attachment file is missing")
        source_sha = _sha256_file(resolved)
        provided_sha = payload.get("sha256")
        if provided_sha:
            if not isinstance(provided_sha, str):
                raise ValueError("Invalid attachment checksum")
            if source_sha != provided_sha.lower():
                raise ValueError("Attachment checksum mismatch")
        relative = resolved.relative_to(legacy_root)
        return resolved, relative, source_sha

    @staticmethod
    def _copy_verified(source, target, expected_sha, *, staging, created):
        """Copy source to target without overwriting/deleting existing files.

        * If target already exists, it is accepted only when byte-identical to
          the source (streaming hash); otherwise migration aborts.  Existing
          files are never overwritten or deleted.
        * Newly-created files go through a staged temporary then an atomic
          replace, and are tracked so they can be cleaned up on rollback.
        * Hash verification is streaming (chunked).
        Returns True if the target was newly created (tracked), False otherwise.
        """
        target = Path(target)
        if target.exists():
            if _sha256_file(target) != expected_sha:
                raise ValueError("Existing destination file has different content; aborting")
            return False
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.parent / (target.name + ".staging." + uuid.uuid4().hex)
        shutil.copyfile(source, tmp)
        staging.append(tmp)
        if _sha256_file(tmp) != expected_sha:
            raise ValueError("Copied attachment hash mismatch; aborting")
        os.replace(tmp, target)
        staging.remove(tmp)
        created.append(target)
        return True

    @staticmethod
    def _cleanup_files(created_lists, staging, protect_roots):
        """Remove only files this migration created; never touch pre-existing files.

        protect_roots: paths that must not themselves be removed (files_root).
        Newly-created empty directories are removed best-effort (rmdir only
        succeeds on empty dirs, so pre-existing files/dirs are never affected).
        """
        for item in staging:
            try:
                item.unlink(missing_ok=True)
            except OSError:
                pass
        all_created = []
        for lst in created_lists:
            all_created.extend(lst)
        for item in all_created:
            try:
                item.unlink(missing_ok=True)
            except OSError:
                pass
        # Best-effort prune empty directories (safety-checked rmdir) without ever
        # touching non-empty dirs or the protected persistent roots.
        protection = [str(Path(r).resolve()).lower() for r in protect_roots if r]
        for item in all_created:
            parent = item.parent
            while True:
                try:
                    resolved_parent = str(parent.resolve()).lower()
                except OSError:
                    break
                if resolved_parent in protection:
                    break
                try:
                    if parent.exists() and not list(parent.iterdir()):
                        parent.rmdir()
                    else:
                        break
                except OSError:
                    break
                parent = parent.parent

    def _run_migration(self) -> None:
        legacy_rows = self._read_legacy_rows()

        # ---- Pass 1: strict validation of every payload before any change ----
        inserts = []
        backup_docs = []
        file_plans = []  # (ns, key, source, relative, source_sha, new_payload)
        for ns in sorted(ASSISTANT_NAMESPACES):
            for row in legacy_rows[ns]:
                raw_json = row["payload_json"]
                try:
                    payload = _json_loads_strict(raw_json)
                except (ValueError, TypeError):
                    raise ValueError("Legacy assistant document is malformed JSON") from None
                if not isinstance(payload, dict):
                    raise ValueError("Legacy assistant document payload must be an object")
                new_payload = payload
                if ns == "lighthouse_runs" and payload.get("status") in ACTIVE_RUN_STATUSES:
                    # Flag only interrupted active runs; never fabricate success.
                    new_payload = dict(payload)
                    new_payload["migration_interrupted"] = True
                # A tombstone is retained for audit, never copied or made readable.
                if ns == "lighthouse_files" and not payload.get("deleted_at"):
                    source, relative, source_sha = self._resolve_attachment(payload)
                    new_payload = dict(payload)
                    new_payload["path"] = str(self.files_root / relative)
                    new_payload["relative_path"] = relative.as_posix()
                    file_plans.append((ns, row["key"], source, relative, source_sha, new_payload))
                try:
                    payload_json = _json_dumps(new_payload)
                except (ValueError, TypeError):
                    raise ValueError("Legacy assistant document contains non-finite values") from None
                inserts.append(
                    (
                        ns,
                        row["key"],
                        payload_json,
                        float(row["updated_at"] or time.time()),
                    )
                )
                backup_docs.append(
                    {
                        "namespace": ns,
                        "key": row["key"],
                        "payload_json": raw_json,
                        "updated_at": float(row["updated_at"] or time.time()),
                    }
                )

        # ---- Pass 2: copy attachments and write backup only after validation ----
        mig_id = "migration_%s_%s" % (
            time.strftime("%Y%m%d%H%M%S"),
            _sha256_bytes(self._legacy_source().encode("utf-8"))[:8],
        )
        backup_root = self.root / "backups" / mig_id
        staging = []       # staged temp files not yet moved
        created = []       # destination files we newly created under files_root
        backup_created = []  # files we newly created under backup_root
        try:
            file_plans.sort(key=lambda item: (item[0], item[1]))
            for _ns, _key, source, relative, source_sha, new_payload in file_plans:
                target = self.files_root / relative
                _reject_link_components(self.files_root, target)
                self._copy_verified(
                    source, target, source_sha,
                    staging=staging, created=created,
                )
                new_payload["path"] = str(target)

                backup_target = backup_root / "files" / relative
                _reject_link_components(backup_root, backup_target)
                self._copy_verified(
                    source, backup_target, source_sha,
                    staging=staging, created=backup_created,
                )

            backup_root.mkdir(parents=True, exist_ok=True)
            jsonl_target = backup_root / "assistant_documents.jsonl"
            jsonl_tmp = backup_root / ("assistant_documents.jsonl.staging." + uuid.uuid4().hex)
            jsonl_target.parent.mkdir(parents=True, exist_ok=True)
            jsonl_tmp.write_text(
                "".join(_json_dumps(doc) + "\n" for doc in backup_docs),
                encoding="utf-8",
            )
            staging.append(jsonl_tmp)
            os.replace(jsonl_tmp, jsonl_target)
            staging.remove(jsonl_tmp)
            backup_created.append(jsonl_target)

            # ---- Pass 3: atomic DB write including the marker ----
            marker = {
                "version": MIGRATION_VERSION,
                "source": self._legacy_source(),
                "root": str(self.root),
                "completed": True,
                "completed_at": time.time(),
                "interrupted": False,
                "namespaces": sorted(ASSISTANT_NAMESPACES),
            }
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.executemany(
                    """
                    INSERT INTO documents(namespace, key, payload_json, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(namespace, key) DO UPDATE SET
                        payload_json = excluded.payload_json,
                        updated_at = excluded.updated_at
                    """,
                    inserts,
                )
                conn.execute(
                    "INSERT OR REPLACE INTO meta(key, value_json, updated_at) VALUES (?, ?, ?)",
                    (_MARKER_KEY, _json_dumps(marker), time.time()),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()
        except Exception:
            # Never delete/overwrite pre-existing destination files, and never
            # fabricate; only remove files this migration actually created plus
            # any leftover staging (files_root itself is always preserved).
            self._cleanup_files(
                [created, backup_created], staging, protect_roots=[self.files_root]
            )
            raise

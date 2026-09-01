from __future__ import annotations

import contextlib
import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Iterator


TERMINAL_GROUP_STATES = {"completed", "cancelled", "stopped"}
INVALID_LINK_GROUP_STATES = {"cancelled", "stopped"}
ACTIVE_GROUP_STATES = {"active", "upload_pending"}
TERMINAL_COMMAND_STATES = {"succeeded", "rejected", "expired", "retryable_error"}
MAX_STEP_PHOTOS = 5
MAX_GROUP_PHOTOS = 100
MAX_GROUP_PHOTO_BYTES = 200 * 1024 * 1024


class RelayError(Exception):
    def __init__(self, status_code: int, code: str, message: str):
        super().__init__(message)
        self.status_code = int(status_code)
        self.code = str(code)
        self.message = str(message)


def stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(value: str) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def sha256_json(value: Any) -> str:
    return sha256_text(stable_json(value))


def _loads(value: str, default: Any) -> Any:
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


class RelayStore:
    def __init__(self, db_path: Path, upload_root: Path):
        self.db_path = Path(db_path).resolve()
        self.upload_root = Path(upload_root).resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.upload_root.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=10.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=10000")
        return connection

    @contextlib.contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    @contextlib.contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._connection() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS authority_leases (
                    authority_id TEXT PRIMARY KEY,
                    instance_id TEXT NOT NULL,
                    epoch INTEGER NOT NULL,
                    fencing_token TEXT NOT NULL,
                    fencing_hash TEXT NOT NULL,
                    expires_at REAL NOT NULL,
                    last_seen_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS replay_nonces (
                    authority_id TEXT NOT NULL,
                    nonce TEXT NOT NULL,
                    expires_at REAL NOT NULL,
                    PRIMARY KEY (authority_id, nonce)
                );
                CREATE TABLE IF NOT EXISTS groups (
                    public_group_id TEXT PRIMARY KEY,
                    authority_id TEXT NOT NULL,
                    registration_version INTEGER NOT NULL,
                    registration_hash TEXT NOT NULL,
                    state TEXT NOT NULL,
                    authority_version INTEGER NOT NULL,
                    projection_revision INTEGER NOT NULL,
                    projection_json TEXT NOT NULL,
                    projection_hash TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS links (
                    link_id TEXT PRIMARY KEY,
                    public_group_id TEXT NOT NULL REFERENCES groups(public_group_id),
                    role TEXT NOT NULL,
                    secret_hash TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    assigned_name TEXT NOT NULL,
                    active INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS links_group_role_generation
                    ON links(public_group_id, role, generation);
                CREATE TABLE IF NOT EXISTS sessions (
                    session_hash TEXT PRIMARY KEY,
                    link_id TEXT NOT NULL REFERENCES links(link_id),
                    public_group_id TEXT NOT NULL REFERENCES groups(public_group_id),
                    role TEXT NOT NULL,
                    csrf_hash TEXT NOT NULL,
                    expires_at REAL NOT NULL,
                    created_at REAL NOT NULL,
                    last_seen_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS sessions_group_idx
                    ON sessions(public_group_id, role);
                CREATE TABLE IF NOT EXISTS uploads (
                    upload_id TEXT PRIMARY KEY,
                    public_group_id TEXT NOT NULL REFERENCES groups(public_group_id),
                    role TEXT NOT NULL,
                    session_hash TEXT NOT NULL,
                    step_key TEXT NOT NULL,
                    expected_version INTEGER NOT NULL,
                    file_name TEXT NOT NULL,
                    content_type TEXT NOT NULL,
                    declared_size INTEGER NOT NULL,
                    declared_sha256 TEXT NOT NULL,
                    actual_size INTEGER NOT NULL DEFAULT 0,
                    actual_sha256 TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    path TEXT NOT NULL DEFAULT '',
                    expires_at REAL NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS uploads_group_idx
                    ON uploads(public_group_id, status);
                CREATE TABLE IF NOT EXISTS commands (
                    command_id TEXT PRIMARY KEY,
                    public_group_id TEXT NOT NULL REFERENCES groups(public_group_id),
                    role TEXT NOT NULL,
                    session_hash TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    request_hash TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    expected_version INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    expires_at REAL NOT NULL,
                    leased_by TEXT NOT NULL DEFAULT '',
                    lease_epoch INTEGER NOT NULL DEFAULT 0,
                    lease_until REAL NOT NULL DEFAULT 0,
                    result_json TEXT NOT NULL DEFAULT '{}',
                    ack_hash TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    UNIQUE(public_group_id, role, idempotency_key)
                );
                CREATE INDEX IF NOT EXISTS commands_queue_idx
                    ON commands(status, created_at);
                CREATE INDEX IF NOT EXISTS commands_group_idx
                    ON commands(public_group_id, status, created_at);
                """
            )
            lease_columns = {
                str(row[1]) for row in connection.execute("PRAGMA table_info(authority_leases)")
            }
            if "fencing_token" not in lease_columns:
                connection.execute(
                    "ALTER TABLE authority_leases ADD COLUMN fencing_token TEXT NOT NULL DEFAULT ''"
                )

    def readiness(self) -> dict[str, bool]:
        database_ready = False
        try:
            with self._connection() as connection:
                database_ready = connection.execute("SELECT 1").fetchone() is not None
        except Exception:
            database_ready = False
        upload_storage_ready = bool(
            self.upload_root.is_dir() and os.access(self.upload_root, os.W_OK)
        )
        return {
            "ready": database_ready and upload_storage_ready,
            "database_ready": database_ready,
            "upload_storage_ready": upload_storage_ready,
        }

    @staticmethod
    def _row(row: sqlite3.Row | None) -> dict[str, Any]:
        return dict(row) if row is not None else {}

    def consume_nonce(self, authority_id: str, nonce: str, expires_at: float) -> None:
        now = time.time()
        with self._transaction() as connection:
            connection.execute("DELETE FROM replay_nonces WHERE expires_at <= ?", (now,))
            try:
                connection.execute(
                    "INSERT INTO replay_nonces(authority_id, nonce, expires_at) VALUES(?,?,?)",
                    (authority_id, nonce, float(expires_at)),
                )
            except sqlite3.IntegrityError as exc:
                raise RelayError(401, "replayed_request", "内部请求已被使用。") from exc

    def issue_authority_lease(
        self,
        authority_id: str,
        instance_id: str,
        *,
        ttl_seconds: int,
    ) -> dict[str, Any]:
        now = time.time()
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM authority_leases WHERE authority_id=?",
                (authority_id,),
            ).fetchone()
            if existing and float(existing["expires_at"]) > now and str(existing["instance_id"]) != instance_id:
                raise RelayError(409, "authority_lease_busy", "另一内网实例仍持有权威租约。")
            same_live_instance = bool(
                existing
                and str(existing["instance_id"]) == instance_id
                and float(existing["expires_at"]) > now
                and str(existing["fencing_token"])
            )
            if same_live_instance:
                epoch = int(existing["epoch"])
                token = str(existing["fencing_token"])
            else:
                epoch = int(existing["epoch"] if existing else 0) + 1
                token = secrets.token_urlsafe(40)
            expires_at = now + max(10, int(ttl_seconds))
            connection.execute(
                """
                INSERT INTO authority_leases(authority_id, instance_id, epoch, fencing_token, fencing_hash, expires_at, last_seen_at)
                VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(authority_id) DO UPDATE SET
                    instance_id=excluded.instance_id,
                    epoch=excluded.epoch,
                    fencing_token=excluded.fencing_token,
                    fencing_hash=excluded.fencing_hash,
                    expires_at=excluded.expires_at,
                    last_seen_at=excluded.last_seen_at
                """,
                (authority_id, instance_id, epoch, token, sha256_text(token), expires_at, now),
            )
        return {
            "authority_id": authority_id,
            "instance_id": instance_id,
            "authority_epoch": epoch,
            "fencing_token": token,
            "expires_at": expires_at,
        }

    def verify_fencing_token(self, authority_id: str, token: str) -> dict[str, Any]:
        now = time.time()
        with self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM authority_leases WHERE authority_id=?",
                (authority_id,),
            ).fetchone()
        if not row or float(row["expires_at"]) <= now:
            raise RelayError(409, "authority_lease_expired", "内网权威租约已过期。")
        if not hmac.compare_digest(str(row["fencing_hash"]), sha256_text(token)):
            raise RelayError(403, "invalid_fencing_token", "权威 fencing token 无效。")
        return self._row(row)

    def authority_online(self, authority_id: str) -> bool:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT expires_at FROM authority_leases WHERE authority_id=?",
                (authority_id,),
            ).fetchone()
        return bool(row and float(row["expires_at"]) > time.time())

    def require_public_write_online(self, session: dict[str, Any]) -> None:
        with self._transaction() as connection:
            self._group_for_public_write(connection, session)

    @staticmethod
    def _group_from_row(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        item = dict(row)
        item["projection"] = _loads(item.pop("projection_json", "{}"), {})
        item.pop("registration_hash", None)
        item.pop("projection_hash", None)
        return item

    @staticmethod
    def _projection_photo_keys(projection: dict[str, Any]) -> set[tuple[str, str]]:
        candidates = (
            [projection.get("operator"), projection.get("reviewer")]
            if any(role in projection for role in ("operator", "reviewer"))
            else [projection]
        )
        return {
            (str(step.get("step_key") or ""), str(photo.get("sha256") or "").lower())
            for candidate in candidates
            if isinstance(candidate, dict)
            for step in candidate.get("steps") or []
            if isinstance(step, dict)
            for photo in step.get("photos") or []
            if isinstance(photo, dict)
            and str(step.get("step_key") or "")
            and str(photo.get("sha256") or "")
        }

    def _enrich_projection_photos(
        self,
        connection: sqlite3.Connection,
        public_group_id: str,
        projection: dict[str, Any],
    ) -> dict[str, Any]:
        rows = connection.execute(
            """
            SELECT upload_id, step_key, actual_sha256 FROM uploads
            WHERE public_group_id=? AND status='authority_attached'
            ORDER BY created_at DESC
            """,
            (public_group_id,),
        ).fetchall()
        photo_ids: dict[tuple[str, str], str] = {}
        for row in rows:
            photo_ids.setdefault(
                (str(row["step_key"]), str(row["actual_sha256"]).lower()),
                str(row["upload_id"]),
            )
        candidates = (
            [projection.get("operator"), projection.get("reviewer")]
            if any(role in projection for role in ("operator", "reviewer"))
            else [projection]
        )
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            for step in candidate.get("steps") or []:
                if not isinstance(step, dict):
                    continue
                step_key = str(step.get("step_key") or "")
                for photo in step.get("photos") or []:
                    if not isinstance(photo, dict):
                        continue
                    photo_id = photo_ids.get(
                        (step_key, str(photo.get("sha256") or "").lower())
                    )
                    if photo_id:
                        photo.update(
                            photo_id=photo_id,
                            preview_url=f"/api/v1/work-orders/photos/{photo_id}",
                            status="authority_attached",
                        )
        return projection

    def _discard_unreferenced_attached_uploads(
        self,
        connection: sqlite3.Connection,
        public_group_id: str,
        projection: dict[str, Any],
    ) -> list[Path]:
        referenced = self._projection_photo_keys(projection)
        rows = connection.execute(
            """
            SELECT upload_id, step_key, actual_sha256, path FROM uploads
            WHERE public_group_id=? AND status='authority_attached'
            """,
            (public_group_id,),
        ).fetchall()
        discarded = [
            row
            for row in rows
            if (
                str(row["step_key"]),
                str(row["actual_sha256"]).lower(),
            )
            not in referenced
        ]
        if discarded:
            connection.executemany(
                """
                UPDATE uploads SET status='cancelled', path='', updated_at=?
                WHERE upload_id=?
                """,
                [(time.time(), row["upload_id"]) for row in discarded],
            )
        return [
            Path(str(row["path"])).resolve()
            for row in discarded
            if str(row["path"])
        ]

    def _delete_upload_paths(self, paths: list[Path]) -> None:
        for path in paths:
            if path.is_file() and path.is_relative_to(self.upload_root):
                with contextlib.suppress(OSError):
                    path.unlink()

    def register_group(
        self,
        *,
        authority_id: str,
        public_group_id: str,
        registration_version: int,
        registration_payload: dict[str, Any],
        state: str,
        authority_version: int,
        projection_revision: int,
        projection: dict[str, Any],
        links: list[dict[str, Any]],
    ) -> dict[str, Any]:
        now = time.time()
        registration_hash = sha256_json(registration_payload)
        projection_hash = sha256_json(projection)
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM groups WHERE public_group_id=?",
                (public_group_id,),
            ).fetchone()
            if existing and str(existing["authority_id"]) != authority_id:
                raise RelayError(403, "wrong_authority", "该工单属于其他权威后端。")
            if existing and registration_version < int(existing["registration_version"]):
                raise RelayError(409, "stale_registration", "工单注册版本已过期。")
            if existing and registration_version == int(existing["registration_version"]):
                if not hmac.compare_digest(str(existing["registration_hash"]), registration_hash):
                    raise RelayError(409, "registration_conflict", "同一注册版本的内容不一致。")
                return self._group_from_row(existing)
            created_at = float(existing["created_at"]) if existing else now
            connection.execute(
                """
                INSERT INTO groups(
                    public_group_id, authority_id, registration_version, registration_hash,
                    state, authority_version, projection_revision, projection_json,
                    projection_hash, created_at, updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(public_group_id) DO UPDATE SET
                    registration_version=excluded.registration_version,
                    registration_hash=excluded.registration_hash,
                    state=excluded.state,
                    authority_version=excluded.authority_version,
                    projection_revision=excluded.projection_revision,
                    projection_json=excluded.projection_json,
                    projection_hash=excluded.projection_hash,
                    updated_at=excluded.updated_at
                """,
                (
                    public_group_id,
                    authority_id,
                    registration_version,
                    registration_hash,
                    state,
                    authority_version,
                    projection_revision,
                    stable_json(projection),
                    projection_hash,
                    created_at,
                    now,
                ),
            )
            connection.execute(
                "UPDATE links SET active=0, updated_at=? WHERE public_group_id=?",
                (now, public_group_id),
            )
            for link in links:
                occupied = connection.execute(
                    """
                    SELECT public_group_id, role, secret_hash, generation
                    FROM links WHERE link_id=?
                    """,
                    (link["link_id"],),
                ).fetchone()
                if occupied and (
                    str(occupied["public_group_id"]) != public_group_id
                    or str(occupied["role"]) != str(link["role"])
                ):
                    raise RelayError(409, "link_conflict", "角色链接标识已被其他工单占用。")
                if occupied and (
                    not hmac.compare_digest(
                        str(occupied["secret_hash"]),
                        str(link["secret_sha256"]),
                    )
                    or int(occupied["generation"])
                    != int(link.get("generation") or 1)
                ):
                    connection.execute(
                        "DELETE FROM sessions WHERE link_id=?",
                        (link["link_id"],),
                    )
                try:
                    connection.execute(
                        """
                        INSERT INTO links(
                            link_id, public_group_id, role, secret_hash, generation,
                            assigned_name, active, created_at, updated_at
                        ) VALUES(?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(link_id) DO UPDATE SET
                            secret_hash=excluded.secret_hash,
                            generation=excluded.generation,
                            assigned_name=excluded.assigned_name,
                            active=excluded.active,
                            updated_at=excluded.updated_at
                        """,
                        (
                            link["link_id"],
                            public_group_id,
                            link["role"],
                            link["secret_sha256"],
                            int(link.get("generation") or 1),
                            str(link.get("assigned_name") or ""),
                            1,
                            now,
                            now,
                        ),
                    )
                except sqlite3.IntegrityError as exc:
                    raise RelayError(409, "link_conflict", "角色链接标识已被其他工单占用。") from exc
            row = connection.execute(
                "SELECT * FROM groups WHERE public_group_id=?",
                (public_group_id,),
            ).fetchone()
        return self._group_from_row(row)

    def exchange_link(
        self,
        *,
        link_id: str,
        secret: str,
        session_ttl_seconds: int,
    ) -> dict[str, Any]:
        now = time.time()
        with self._transaction() as connection:
            row = connection.execute(
                """
                SELECT l.*, g.state, g.authority_id
                FROM links l JOIN groups g ON g.public_group_id=l.public_group_id
                WHERE l.link_id=?
                """,
                (link_id,),
            ).fetchone()
            if not row or not int(row["active"]):
                raise RelayError(403, "invalid_link", "工单链接无效或已失效。")
            if str(row["state"]) in INVALID_LINK_GROUP_STATES:
                raise RelayError(410, "group_terminal", "该工单已结束或已停止。")
            if not hmac.compare_digest(str(row["secret_hash"]), sha256_text(secret)):
                raise RelayError(403, "invalid_link", "工单链接无效或已失效。")
            session_token = secrets.token_urlsafe(40)
            csrf_token = secrets.token_urlsafe(32)
            expires_at = now + max(300, int(session_ttl_seconds))
            connection.execute(
                """
                INSERT INTO sessions(
                    session_hash, link_id, public_group_id, role, csrf_hash,
                    expires_at, created_at, last_seen_at
                ) VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    sha256_text(session_token),
                    link_id,
                    row["public_group_id"],
                    row["role"],
                    sha256_text(csrf_token),
                    expires_at,
                    now,
                    now,
                ),
            )
        return {
            "session_token": session_token,
            "csrf_token": csrf_token,
            "expires_at": expires_at,
            "public_group_id": str(row["public_group_id"]),
            "role": str(row["role"]),
            "assigned_name": str(row["assigned_name"]),
            "authority_id": str(row["authority_id"]),
        }

    def authenticate_session(
        self,
        session_token: str,
        *,
        touch: bool = True,
        ttl_seconds: int = 0,
    ) -> dict[str, Any]:
        session_hash = sha256_text(session_token)
        now = time.time()
        with self._transaction() as connection:
            row = connection.execute(
                """
                SELECT s.*, l.assigned_name, l.active AS link_active,
                       g.state, g.authority_id, g.authority_version,
                       g.projection_revision, g.projection_json
                FROM sessions s
                JOIN links l ON l.link_id=s.link_id
                JOIN groups g ON g.public_group_id=s.public_group_id
                WHERE s.session_hash=?
                """,
                (session_hash,),
            ).fetchone()
            if not row or float(row["expires_at"]) <= now:
                if row:
                    connection.execute("DELETE FROM sessions WHERE session_hash=?", (session_hash,))
                raise RelayError(401, "session_expired", "工单会话已过期，请重新打开角色链接。")
            if not int(row["link_active"]) or str(row["state"]) in {"cancelled", "stopped"}:
                raise RelayError(410, "group_terminal", "该工单已停止。")
            if touch:
                connection.execute(
                    "UPDATE sessions SET last_seen_at=?, expires_at=? WHERE session_hash=?",
                    (
                        now,
                        now + max(300, int(ttl_seconds))
                        if int(ttl_seconds or 0) > 0
                        else float(row["expires_at"]),
                        session_hash,
                    ),
                )
        item = self._row(row)
        item["session_hash"] = session_hash
        item["projection"] = _loads(item.pop("projection_json", "{}"), {})
        return item

    @staticmethod
    def verify_csrf(session: dict[str, Any], token: str) -> None:
        if not token or not hmac.compare_digest(str(session.get("csrf_hash") or ""), sha256_text(token)):
            raise RelayError(403, "invalid_csrf", "CSRF 校验失败。")

    def revoke_session(self, session_hash: str) -> None:
        with self._transaction() as connection:
            connection.execute("DELETE FROM sessions WHERE session_hash=?", (session_hash,))

    def public_snapshot(self, session: dict[str, Any]) -> dict[str, Any]:
        stored_projection = session.get("projection") or {}
        role_projection = (
            stored_projection.get(str(session["role"]))
            if isinstance(stored_projection, dict)
            and isinstance(stored_projection.get(str(session["role"])), dict)
            else stored_projection
        )
        projection = dict(role_projection or {})
        group_id = str(session["public_group_id"])
        with self._connection() as connection:
            pending = connection.execute(
                """
                SELECT command_id, kind, status, created_at, updated_at
                FROM commands
                WHERE public_group_id=? AND role=?
                  AND status IN ('pending','leased')
                ORDER BY created_at DESC LIMIT 20
                """,
                (group_id, session["role"]),
            ).fetchall()
        projection.update(
            {
                "public_group_id": group_id,
                "role": str(session["role"]),
                "role_label": "操作人" if session["role"] == "operator" else "现场审核人",
                "assigned_name": str(session.get("assigned_name") or ""),
                "state": str(session["state"]),
                "authority_online": self.authority_online(str(session["authority_id"])),
                "authority_version": int(session["authority_version"]),
                "projection_revision": int(session["projection_revision"]),
                "relay_server_time": time.time(),
                "pending_commands": [dict(row) for row in pending],
            }
        )
        return projection

    def _group_for_public_write(self, connection: sqlite3.Connection, session: dict[str, Any]) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM groups WHERE public_group_id=?",
            (session["public_group_id"],),
        ).fetchone()
        if not row:
            raise RelayError(404, "group_not_found", "工单不存在。")
        if str(row["state"]) in TERMINAL_GROUP_STATES:
            raise RelayError(410, "group_terminal", "工单已结束或已停止。")
        lease = connection.execute(
            "SELECT expires_at FROM authority_leases WHERE authority_id=?",
            (row["authority_id"],),
        ).fetchone()
        if not lease or float(lease["expires_at"]) <= time.time():
            raise RelayError(503, "authority_offline", "内网权威后端当前离线，请稍后重试。")
        return row

    def enqueue_command(
        self,
        *,
        session: dict[str, Any],
        idempotency_key: str,
        kind: str,
        payload: dict[str, Any],
        expected_version: int,
        ttl_seconds: int,
    ) -> tuple[dict[str, Any], bool]:
        now = time.time()
        request_hash = sha256_json({"kind": kind, "payload": payload, "expected_version": expected_version})
        with self._transaction() as connection:
            existing = connection.execute(
                """
                SELECT * FROM commands
                WHERE public_group_id=? AND role=? AND idempotency_key=?
                """,
                (session["public_group_id"], session["role"], idempotency_key),
            ).fetchone()
            if existing:
                if not hmac.compare_digest(str(existing["request_hash"]), request_hash):
                    raise RelayError(409, "idempotency_conflict", "幂等键已用于其他请求。")
                if kind == "attach_photo" and str(existing["status"]) in {
                    "expired",
                    "retryable_error",
                }:
                    group = self._group_for_public_write(connection, session)
                    if int(group["authority_version"]) != int(expected_version):
                        raise RelayError(409, "version_conflict", "工单状态已更新，请刷新后重试。")
                    upload = connection.execute(
                        "SELECT * FROM uploads WHERE upload_id=?",
                        (str(payload.get("upload_id") or ""),),
                    ).fetchone()
                    if (
                        upload
                        and str(upload["public_group_id"])
                        == str(session["public_group_id"])
                        and str(upload["role"]) == str(session["role"])
                        and str(upload["status"]) == "relay_ready"
                        and float(upload["expires_at"]) > now
                    ):
                        connection.execute(
                            """
                            UPDATE commands SET status='pending', expires_at=?, leased_by='',
                                lease_epoch=0, lease_until=0, result_json='{}', ack_hash='', updated_at=?
                            WHERE command_id=? AND status IN ('expired','retryable_error')
                            """,
                            (
                                now + max(15, int(ttl_seconds)),
                                now,
                                existing["command_id"],
                            ),
                        )
                        existing = connection.execute(
                            "SELECT * FROM commands WHERE command_id=?",
                            (existing["command_id"],),
                        ).fetchone()
                return self._command_public(existing), False
            group = self._group_for_public_write(connection, session)
            if int(group["authority_version"]) != int(expected_version):
                raise RelayError(409, "version_conflict", "工单状态已更新，请刷新后重试。")
            if kind == "attach_photo":
                upload_id = str(payload.get("upload_id") or "")
                upload = connection.execute(
                    "SELECT * FROM uploads WHERE upload_id=?",
                    (upload_id,),
                ).fetchone()
                if not upload or str(upload["public_group_id"]) != str(session["public_group_id"]):
                    raise RelayError(404, "upload_not_found", "照片暂存记录不存在。")
                if str(upload["role"]) != str(session["role"]):
                    raise RelayError(403, "wrong_upload_role", "不能使用另一角色上传的照片。")
                if str(upload["status"]) == "authority_attached":
                    raise RelayError(409, "photo_already_attached", "该照片已同步至内网。")
                if str(upload["status"]) != "relay_ready":
                    raise RelayError(409, "upload_not_ready", "照片尚未完成公网暂存。")
                if str(upload["step_key"]) != str(payload.get("step_key") or ""):
                    raise RelayError(409, "upload_step_mismatch", "照片与当前步骤不一致。")
            command_id = uuid.uuid4().hex
            expires_at = now + max(15, int(ttl_seconds))
            connection.execute(
                """
                INSERT INTO commands(
                    command_id, public_group_id, role, session_hash,
                    idempotency_key, request_hash, kind, payload_json,
                    expected_version, status, expires_at, created_at, updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,'pending',?,?,?)
                """,
                (
                    command_id,
                    session["public_group_id"],
                    session["role"],
                    session["session_hash"],
                    idempotency_key,
                    request_hash,
                    kind,
                    stable_json(payload),
                    int(expected_version),
                    expires_at,
                    now,
                    now,
                ),
            )
            row = connection.execute("SELECT * FROM commands WHERE command_id=?", (command_id,)).fetchone()
        return self._command_public(row), True

    @staticmethod
    def _command_public(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        item = dict(row)
        result = _loads(item.get("result_json", "{}"), {})
        return {
            "command_id": str(item["command_id"]),
            "type": str(item["kind"]),
            "status": str(item["status"]),
            "expires_at": float(item["expires_at"]),
            "created_at": float(item["created_at"]),
            "updated_at": float(item["updated_at"]),
            "result": result,
            "error_code": str(result.get("error_code") or ""),
            "error": str(result.get("error") or ""),
        }

    def command_for_session(self, session: dict[str, Any], command_id: str) -> dict[str, Any]:
        now = time.time()
        with self._transaction() as connection:
            connection.execute(
                """
                UPDATE commands SET status='expired', updated_at=?
                WHERE command_id=? AND status='pending' AND expires_at<=?
                """,
                (now, command_id, now),
            )
            connection.execute(
                """
                UPDATE commands SET status='expired', updated_at=?
                WHERE command_id=? AND status='leased'
                  AND lease_until<=? AND expires_at<=?
                """,
                (now, command_id, now, now),
            )
            row = connection.execute(
                """
                SELECT * FROM commands
                WHERE command_id=? AND public_group_id=? AND role=?
                """,
                (command_id, session["public_group_id"], session["role"]),
            ).fetchone()
        if not row:
            raise RelayError(404, "command_not_found", "命令不存在。")
        return self._command_public(row)

    def create_upload(
        self,
        *,
        session: dict[str, Any],
        step_key: str,
        expected_version: int,
        file_name: str,
        content_type: str,
        size: int,
        digest: str,
        ttl_seconds: int,
    ) -> dict[str, Any]:
        now = time.time()
        upload_id = uuid.uuid4().hex
        with self._transaction() as connection:
            group = self._group_for_public_write(connection, session)
            if str(group["state"]) != "active":
                raise RelayError(409, "group_not_active", "当前工单不能再上传操作照片。")
            if int(group["authority_version"]) != int(expected_version):
                raise RelayError(409, "version_conflict", "工单状态已更新，请刷新后重试。")
            stored_projection = _loads(str(group["projection_json"]), {})
            role_projection = (
                stored_projection.get(str(session["role"]))
                if isinstance(stored_projection, dict)
                and isinstance(stored_projection.get(str(session["role"])), dict)
                else stored_projection
            )
            current_step = next(
                (
                    item
                    for item in (role_projection or {}).get("steps") or []
                    if isinstance(item, dict)
                    and str(item.get("step_key") or "") == step_key
                    and str(item.get("position") or "") == "current"
                ),
                None,
            )
            if current_step is None:
                raise RelayError(409, "step_not_current", "只能给当前步骤上传操作照片。")
            duplicate = connection.execute(
                """
                SELECT * FROM uploads
                WHERE public_group_id=? AND role=? AND session_hash=?
                  AND step_key=? AND expected_version=? AND declared_sha256=?
                  AND expires_at>? AND status NOT IN ('cancelled','expired')
                ORDER BY created_at DESC LIMIT 1
                """,
                (
                    session["public_group_id"],
                    session["role"],
                    session["session_hash"],
                    step_key,
                    int(expected_version),
                    digest,
                    now,
                ),
            ).fetchone()
            if duplicate:
                return self._upload_initialization_public(duplicate)
            active_uploads = connection.execute(
                """
                SELECT step_key, declared_size, declared_sha256, status FROM uploads
                WHERE public_group_id=? AND expires_at>?
                  AND status NOT IN ('cancelled','expired')
                """,
                (session["public_group_id"], now),
            ).fetchall()
            referenced = self._projection_photo_keys(
                _loads(str(group["projection_json"]), {})
            )
            seen_attached: set[tuple[str, str]] = set()
            counted_uploads = []
            for item in active_uploads:
                if str(item["status"]) != "authority_attached":
                    counted_uploads.append(item)
                    continue
                key = (
                    str(item["step_key"]),
                    str(item["declared_sha256"]).lower(),
                )
                if key in referenced and key not in seen_attached:
                    seen_attached.add(key)
                    counted_uploads.append(item)
            active_uploads = counted_uploads
            if sum(1 for item in active_uploads if str(item["step_key"]) == step_key) >= MAX_STEP_PHOTOS:
                raise RelayError(409, "step_photo_limit", "每个步骤最多上传 5 张操作照片。")
            if len(active_uploads) >= MAX_GROUP_PHOTOS:
                raise RelayError(409, "group_photo_limit", "整个工单组最多上传 100 张操作照片。")
            if sum(int(item["declared_size"] or 0) for item in active_uploads) + int(size) > MAX_GROUP_PHOTO_BYTES:
                raise RelayError(413, "group_photo_bytes_limit", "整个工单组操作照片总大小不能超过 200MB。")
            connection.execute(
                """
                INSERT INTO uploads(
                    upload_id, public_group_id, role, session_hash, step_key,
                    expected_version, file_name, content_type, declared_size,
                    declared_sha256, status, expires_at, created_at, updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,'initialized',?,?,?)
                """,
                (
                    upload_id,
                    session["public_group_id"],
                    session["role"],
                    session["session_hash"],
                    step_key,
                    expected_version,
                    file_name,
                    content_type,
                    size,
                    digest,
                    now + max(300, int(ttl_seconds)),
                    now,
                    now,
                ),
            )
        return self._upload_initialization_public(
            {
                "upload_id": upload_id,
                "status": "initialized",
                "expires_at": now + max(300, int(ttl_seconds)),
            }
        )

    @staticmethod
    def _upload_initialization_public(
        row: sqlite3.Row | dict[str, Any],
    ) -> dict[str, Any]:
        item = dict(row)
        upload_id = str(item["upload_id"])
        return {
            "upload_id": upload_id,
            "status": str(item["status"]),
            "expires_at": float(item["expires_at"]),
            "content_url": f"/api/v1/work-orders/uploads/{upload_id}/content",
            "complete_url": f"/api/v1/work-orders/uploads/{upload_id}/complete",
        }

    def upload_for_session(self, session: dict[str, Any], upload_id: str) -> dict[str, Any]:
        with self._connection() as connection:
            row = connection.execute("SELECT * FROM uploads WHERE upload_id=?", (upload_id,)).fetchone()
        if not row or str(row["public_group_id"]) != str(session["public_group_id"]):
            raise RelayError(404, "upload_not_found", "照片暂存记录不存在。")
        if str(row["session_hash"]) != str(session["session_hash"]):
            raise RelayError(403, "wrong_upload_session", "照片暂存记录属于其他会话。")
        if float(row["expires_at"]) <= time.time():
            raise RelayError(410, "upload_expired", "照片暂存已过期。")
        return self._row(row)

    def mark_upload_content(
        self,
        *,
        upload_id: str,
        session_hash: str,
        path: Path,
        actual_size: int,
        actual_sha256: str,
    ) -> dict[str, Any]:
        now = time.time()
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM uploads WHERE upload_id=?", (upload_id,)).fetchone()
            if not row or str(row["session_hash"]) != session_hash:
                raise RelayError(404, "upload_not_found", "照片暂存记录不存在。")
            if str(row["status"]) not in {"receiving", "uploaded"}:
                raise RelayError(409, "upload_state_conflict", "照片暂存状态已变化。")
            connection.execute(
                """
                UPDATE uploads SET actual_size=?, actual_sha256=?, status='uploaded',
                    path=?, updated_at=? WHERE upload_id=?
                """,
                (actual_size, actual_sha256, str(path), now, upload_id),
            )
            updated = connection.execute("SELECT * FROM uploads WHERE upload_id=?", (upload_id,)).fetchone()
        return self._row(updated)

    def begin_upload_content(self, upload_id: str, session_hash: str) -> None:
        now = time.time()
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT status, session_hash FROM uploads WHERE upload_id=?",
                (upload_id,),
            ).fetchone()
            if not row or str(row["session_hash"]) != session_hash:
                raise RelayError(404, "upload_not_found", "照片暂存记录不存在。")
            if str(row["status"]) == "receiving":
                raise RelayError(409, "upload_in_progress", "照片正在由另一个请求上传。")
            if str(row["status"]) != "initialized":
                raise RelayError(409, "upload_state_conflict", "照片暂存状态已变化。")
            connection.execute(
                "UPDATE uploads SET status='receiving', updated_at=? WHERE upload_id=?",
                (now, upload_id),
            )

    def reset_upload_content(self, upload_id: str, session_hash: str) -> None:
        with self._transaction() as connection:
            connection.execute(
                """
                UPDATE uploads SET status='initialized', updated_at=?
                WHERE upload_id=? AND session_hash=? AND status='receiving'
                """,
                (time.time(), upload_id, session_hash),
            )

    def complete_upload(
        self,
        session: dict[str, Any],
        upload_id: str,
        *,
        ttl_seconds: int,
    ) -> dict[str, Any]:
        now = time.time()
        with self._transaction() as connection:
            self._group_for_public_write(connection, session)
            row = connection.execute("SELECT * FROM uploads WHERE upload_id=?", (upload_id,)).fetchone()
            if not row or str(row["session_hash"]) != str(session["session_hash"]):
                raise RelayError(404, "upload_not_found", "照片暂存记录不存在。")
            if str(row["status"]) in {"relay_ready", "authority_attached"}:
                return self._upload_public(row)
            if str(row["status"]) != "uploaded":
                raise RelayError(409, "upload_incomplete", "照片内容尚未上传完成。")
            path = Path(str(row["path"])).resolve()
            if not path.is_file() or not path.is_relative_to(self.upload_root):
                raise RelayError(409, "upload_file_missing", "照片暂存文件不存在。")
            if int(row["actual_size"]) != int(row["declared_size"]) or not hmac.compare_digest(
                str(row["actual_sha256"]), str(row["declared_sha256"])
            ):
                raise RelayError(409, "upload_checksum_mismatch", "照片大小或校验值不一致。")
            connection.execute(
                """
                UPDATE uploads SET status='relay_ready', expires_at=?, updated_at=?
                WHERE upload_id=?
                """,
                (now + max(300, int(ttl_seconds)), now, upload_id),
            )
            updated = connection.execute("SELECT * FROM uploads WHERE upload_id=?", (upload_id,)).fetchone()
        return self._upload_public(updated)

    @staticmethod
    def _upload_public(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        item = dict(row)
        return {
            "upload_id": str(item["upload_id"]),
            "public_photo_id": str(item["upload_id"]),
            "step_key": str(item["step_key"]),
            "file_name": str(item["file_name"]),
            "content_type": str(item["content_type"]),
            "size": int(item["actual_size"] or item["declared_size"]),
            "sha256": str(item["actual_sha256"] or item["declared_sha256"]),
            "status": str(item["status"]),
            "preview_url": f"/api/v1/work-orders/photos/{item['upload_id']}",
            "expires_at": float(item["expires_at"]),
        }

    def photo_for_session(self, session: dict[str, Any], photo_id: str) -> dict[str, Any]:
        with self._connection() as connection:
            row = connection.execute("SELECT * FROM uploads WHERE upload_id=?", (photo_id,)).fetchone()
        if not row or str(row["public_group_id"]) != str(session["public_group_id"]):
            raise RelayError(404, "photo_not_found", "照片不存在。")
        if str(row["status"]) not in {"relay_ready", "authority_attached"}:
            raise RelayError(404, "photo_not_ready", "照片尚未完成暂存。")
        return self._row(row)

    def lease_commands(
        self,
        *,
        authority_id: str,
        instance_id: str,
        authority_epoch: int,
        limit: int,
        lease_seconds: int,
    ) -> list[dict[str, Any]]:
        now = time.time()
        leased: list[dict[str, Any]] = []
        with self._transaction() as connection:
            connection.execute(
                """
                UPDATE commands SET status='expired', updated_at=?
                WHERE expires_at<=? AND status='pending'
                """,
                (now, now),
            )
            connection.execute(
                """
                UPDATE commands SET status='pending', leased_by='', lease_epoch=0,
                    lease_until=0, updated_at=?
                WHERE status='leased' AND lease_until<=? AND expires_at>?
                  AND public_group_id IN (
                      SELECT public_group_id FROM groups WHERE authority_id=?
                  )
                """,
                (now, now, now, authority_id),
            )
            connection.execute(
                """
                UPDATE commands SET status='expired', updated_at=?
                WHERE status='leased' AND lease_until<=? AND expires_at<=?
                  AND public_group_id IN (
                      SELECT public_group_id FROM groups WHERE authority_id=?
                  )
                """,
                (now, now, now, authority_id),
            )
            candidates = connection.execute(
                """
                SELECT c.* FROM commands c
                JOIN groups g ON g.public_group_id=c.public_group_id
                WHERE c.status='pending' AND c.expires_at>? AND g.authority_id=?
                ORDER BY c.created_at, c.command_id LIMIT ?
                """,
                (now, authority_id, max(1, min(100, int(limit))) * 4),
            ).fetchall()
            seen_groups: set[str] = set()
            for row in candidates:
                group_id = str(row["public_group_id"])
                if group_id in seen_groups:
                    continue
                active = connection.execute(
                    """
                    SELECT 1 FROM commands
                    WHERE public_group_id=? AND status='leased' LIMIT 1
                    """,
                    (group_id,),
                ).fetchone()
                if active:
                    continue
                connection.execute(
                    """
                    UPDATE commands SET status='leased', leased_by=?, lease_epoch=?,
                        lease_until=?, updated_at=? WHERE command_id=? AND status='pending'
                    """,
                    (
                        instance_id,
                        authority_epoch,
                        now + max(10, int(lease_seconds)),
                        now,
                        row["command_id"],
                    ),
                )
                refreshed = connection.execute(
                    "SELECT * FROM commands WHERE command_id=?",
                    (row["command_id"],),
                ).fetchone()
                item = dict(refreshed)
                item["payload"] = _loads(item.pop("payload_json"), {})
                item["type"] = str(item.get("kind") or "")
                item.pop("request_hash", None)
                item.pop("session_hash", None)
                item.pop("ack_hash", None)
                item.pop("result_json", None)
                if item["kind"] == "attach_photo":
                    upload_id = str(item["payload"].get("upload_id") or "")
                    upload = connection.execute(
                        "SELECT * FROM uploads WHERE upload_id=?",
                        (upload_id,),
                    ).fetchone()
                    if upload:
                        upload_metadata = {
                            "upload_id": upload_id,
                            "file_name": str(upload["file_name"]),
                            "content_type": str(upload["content_type"]),
                            "size": int(upload["actual_size"]),
                            "sha256": str(upload["actual_sha256"]),
                            "download_url": f"/api/v1/internal/uploads/{upload_id}/content",
                        }
                        item["upload"] = upload_metadata
                        item["payload"]["upload"] = upload_metadata
                leased.append(item)
                seen_groups.add(group_id)
                if len(leased) >= max(1, min(100, int(limit))):
                    break
        return leased

    def internal_upload(self, authority_id: str, upload_id: str) -> dict[str, Any]:
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT u.* FROM uploads u
                JOIN groups g ON g.public_group_id=u.public_group_id
                WHERE u.upload_id=? AND g.authority_id=?
                """,
                (upload_id, authority_id),
            ).fetchone()
        if not row or str(row["status"]) not in {"relay_ready", "authority_attached"}:
            raise RelayError(404, "upload_not_found", "照片暂存内容不存在。")
        path = Path(str(row["path"])).resolve()
        if not path.is_file() or not path.is_relative_to(self.upload_root):
            raise RelayError(404, "upload_file_missing", "照片暂存文件不存在。")
        return self._row(row)

    def _apply_projection(
        self,
        connection: sqlite3.Connection,
        *,
        row: sqlite3.Row,
        authority_version: int,
        projection_revision: int,
        projection: dict[str, Any],
        state: str,
        now: float,
    ) -> None:
        projection_hash = sha256_json(projection)
        if projection_revision < int(row["projection_revision"]):
            raise RelayError(409, "stale_projection", "工单投影版本已过期。")
        if projection_revision == int(row["projection_revision"]):
            if not hmac.compare_digest(str(row["projection_hash"]), projection_hash):
                raise RelayError(409, "projection_conflict", "同一投影版本内容不一致。")
            return
        if authority_version < int(row["authority_version"]):
            raise RelayError(409, "stale_authority_version", "工单权威版本已回退。")
        connection.execute(
            """
            UPDATE groups SET state=?, authority_version=?, projection_revision=?,
                projection_json=?, projection_hash=?, updated_at=?
            WHERE public_group_id=?
            """,
            (
                state,
                authority_version,
                projection_revision,
                stable_json(projection),
                projection_hash,
                now,
                row["public_group_id"],
            ),
        )

    def acknowledge_command(
        self,
        *,
        authority_id: str,
        command_id: str,
        authority_epoch: int,
        instance_id: str,
        acknowledgement: dict[str, Any],
        projection: dict[str, Any] | None,
    ) -> dict[str, Any]:
        now = time.time()
        discarded_upload_paths: list[Path] = []
        ack_hash = sha256_json(
            {"acknowledgement": acknowledgement, "projection": projection}
        )
        outcome = str(acknowledgement.get("outcome") or "")
        status_map = {
            "succeeded": "succeeded",
            "success": "succeeded",
            "conflict": "rejected",
            "rejected": "rejected",
            "retryable_error": "retryable_error",
        }
        status = status_map.get(outcome)
        if not status:
            raise RelayError(422, "invalid_outcome", "命令结果状态无效。")
        with self._transaction() as connection:
            command = connection.execute(
                """
                SELECT c.*, g.authority_id FROM commands c
                JOIN groups g ON g.public_group_id=c.public_group_id
                WHERE c.command_id=?
                """,
                (command_id,),
            ).fetchone()
            if not command or str(command["authority_id"]) != authority_id:
                raise RelayError(404, "command_not_found", "命令不存在。")
            if str(command["status"]) in TERMINAL_COMMAND_STATES:
                if not hmac.compare_digest(str(command["ack_hash"]), ack_hash):
                    raise RelayError(409, "ack_conflict", "该命令已有不同的执行结果。")
                return self._command_public(command)
            if (
                str(command["status"]) != "leased"
                or int(command["lease_epoch"]) != int(authority_epoch)
                or str(command["leased_by"]) != str(instance_id)
                or float(command["lease_until"]) <= now
            ):
                raise RelayError(409, "command_lease_lost", "命令租约已失效。")
            result = dict(acknowledgement.get("result") or {}) if isinstance(
                acknowledgement.get("result"), dict
            ) else {}
            if acknowledgement.get("error_code"):
                result["error_code"] = str(acknowledgement["error_code"])
            if acknowledgement.get("error"):
                result["error"] = str(acknowledgement["error"])
            connection.execute(
                """
                UPDATE commands SET status=?, result_json=?, ack_hash=?,
                    lease_until=0, updated_at=? WHERE command_id=?
                """,
                (status, stable_json(result), ack_hash, now, command_id),
            )
            if str(command["kind"]) == "attach_photo":
                payload = _loads(str(command["payload_json"]), {})
                upload_id = str(payload.get("upload_id") or "")
                if status == "succeeded":
                    connection.execute(
                        "UPDATE uploads SET status='authority_attached', updated_at=? WHERE upload_id=?",
                        (now, upload_id),
                    )
                else:
                    upload = connection.execute(
                        "SELECT path FROM uploads WHERE upload_id=?",
                        (upload_id,),
                    ).fetchone()
                    if upload and str(upload["path"]):
                        discarded_upload_paths.append(
                            Path(str(upload["path"])).resolve()
                        )
                    connection.execute(
                        "UPDATE uploads SET status='cancelled', path='', updated_at=? WHERE upload_id=?",
                        (now, upload_id),
                    )
            if projection is not None:
                projection = self._enrich_projection_photos(
                    connection,
                    str(command["public_group_id"]),
                    projection,
                )
                group = connection.execute(
                    "SELECT * FROM groups WHERE public_group_id=?",
                    (command["public_group_id"],),
                ).fetchone()
                projected_state = str(projection.get("state") or "")
                if not projected_state and isinstance(projection.get("operator"), dict):
                    projected_state = str(projection["operator"].get("state") or "")
                self._apply_projection(
                    connection,
                    row=group,
                    authority_version=int(acknowledgement.get("authority_version") or group["authority_version"]),
                    projection_revision=int(acknowledgement.get("projection_revision") or group["projection_revision"]),
                    projection=projection,
                    state=projected_state or str(group["state"]),
                    now=now,
                )
                if projected_state == "active":
                    discarded_upload_paths.extend(
                        self._discard_unreferenced_attached_uploads(
                            connection,
                            str(command["public_group_id"]),
                            projection,
                        )
                    )
            updated = connection.execute("SELECT * FROM commands WHERE command_id=?", (command_id,)).fetchone()
        self._delete_upload_paths(discarded_upload_paths)
        return self._command_public(updated)

    def update_projection(
        self,
        *,
        authority_id: str,
        public_group_id: str,
        authority_version: int,
        projection_revision: int,
        projection: dict[str, Any],
        state: str,
    ) -> dict[str, Any]:
        now = time.time()
        discarded_upload_paths: list[Path] = []
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM groups WHERE public_group_id=?", (public_group_id,)).fetchone()
            if not row:
                raise RelayError(404, "group_not_found", "工单不存在。")
            if str(row["authority_id"]) != authority_id:
                raise RelayError(403, "wrong_authority", "该工单属于其他权威后端。")
            projection = self._enrich_projection_photos(
                connection, public_group_id, projection
            )
            self._apply_projection(
                connection,
                row=row,
                authority_version=authority_version,
                projection_revision=projection_revision,
                projection=projection,
                state=state,
                now=now,
            )
            if state == "active":
                discarded_upload_paths = self._discard_unreferenced_attached_uploads(
                    connection, public_group_id, projection
                )
            updated = connection.execute("SELECT * FROM groups WHERE public_group_id=?", (public_group_id,)).fetchone()
        self._delete_upload_paths(discarded_upload_paths)
        return self._group_from_row(updated)

    def _purge_cancelled_group(self, public_group_id: str) -> bool:
        with self._connection() as connection:
            group = connection.execute(
                "SELECT state FROM groups WHERE public_group_id=?",
                (public_group_id,),
            ).fetchone()
            if not group:
                return True
            if str(group["state"]) != "cancelled":
                return False
            rows = connection.execute(
                "SELECT path FROM uploads WHERE public_group_id=? AND path<>''",
                (public_group_id,),
            ).fetchall()
        for row in rows:
            path = Path(str(row["path"])).resolve()
            if not path.is_relative_to(self.upload_root):
                return False
            if path.is_file():
                try:
                    path.unlink()
                except OSError:
                    return False
        with self._transaction() as connection:
            group = connection.execute(
                "SELECT state FROM groups WHERE public_group_id=?",
                (public_group_id,),
            ).fetchone()
            if not group:
                return True
            if str(group["state"]) != "cancelled":
                return False
            connection.execute("DELETE FROM sessions WHERE public_group_id=?", (public_group_id,))
            connection.execute("DELETE FROM commands WHERE public_group_id=?", (public_group_id,))
            connection.execute("DELETE FROM uploads WHERE public_group_id=?", (public_group_id,))
            connection.execute("DELETE FROM links WHERE public_group_id=?", (public_group_id,))
            connection.execute("DELETE FROM groups WHERE public_group_id=?", (public_group_id,))
        return True

    def cancel_group(self, authority_id: str, public_group_id: str, *, reason: str) -> dict[str, Any]:
        now = time.time()
        with self._transaction() as connection:
            group = connection.execute("SELECT * FROM groups WHERE public_group_id=?", (public_group_id,)).fetchone()
            if not group:
                raise RelayError(404, "group_not_found", "工单不存在。")
            if str(group["authority_id"]) != authority_id:
                raise RelayError(403, "wrong_authority", "该工单属于其他权威后端。")
            projection = _loads(str(group["projection_json"]), {})
            projection["state"] = "cancelled"
            projection["last_error"] = str(reason or "工单已取消。")
            connection.execute(
                """
                UPDATE groups SET state='cancelled', projection_json=?, projection_hash=?,
                    updated_at=? WHERE public_group_id=?
                """,
                (stable_json(projection), sha256_json(projection), now, public_group_id),
            )
            connection.execute("UPDATE links SET active=0, updated_at=? WHERE public_group_id=?", (now, public_group_id))
            connection.execute("DELETE FROM sessions WHERE public_group_id=?", (public_group_id,))
            connection.execute(
                """
                UPDATE commands SET status='rejected', result_json=?, updated_at=?
                WHERE public_group_id=? AND status IN ('pending','leased')
                """,
                (stable_json({"error_code": "group_cancelled", "error": str(reason or "工单已取消。")}), now, public_group_id),
            )
            connection.execute(
                "UPDATE uploads SET status='cancelled', updated_at=? WHERE public_group_id=?",
                (now, public_group_id),
            )
        purged = self._purge_cancelled_group(public_group_id)
        return {
            "public_group_id": public_group_id,
            "state": "cancelled",
            "reason": str(reason or ""),
            "purged": purged,
        }

    def cleanup(self) -> None:
        now = time.time()
        paths: list[Path] = []
        with self._transaction() as connection:
            connection.execute("DELETE FROM replay_nonces WHERE expires_at<=?", (now,))
            connection.execute("DELETE FROM sessions WHERE expires_at<=?", (now,))
            connection.execute(
                "UPDATE commands SET status='expired', updated_at=? WHERE expires_at<=? AND status='pending'",
                (now, now),
            )
            connection.execute(
                """
                UPDATE commands SET status='expired', updated_at=?
                WHERE status='leased' AND lease_until<=? AND expires_at<=?
                """,
                (now, now, now),
            )
            rows = connection.execute(
                """
                SELECT upload_id, path FROM uploads
                WHERE expires_at<=?
                  AND status IN ('initialized','receiving','uploaded','relay_ready')
                """,
                (now,),
            ).fetchall()
            paths = [Path(str(row["path"])).resolve() for row in rows if str(row["path"])]
            connection.executemany(
                "UPDATE uploads SET status='expired', path='', updated_at=? WHERE upload_id=?",
                [(now, row["upload_id"]) for row in rows],
            )
        for path in paths:
            if path.is_file() and path.is_relative_to(self.upload_root):
                with contextlib.suppress(OSError):
                    path.unlink()
        with self._connection() as connection:
            cancelled_group_ids = [
                str(row["public_group_id"])
                for row in connection.execute(
                    "SELECT public_group_id FROM groups WHERE state='cancelled'"
                ).fetchall()
            ]
        for public_group_id in cancelled_group_ids:
            self._purge_cancelled_group(public_group_id)

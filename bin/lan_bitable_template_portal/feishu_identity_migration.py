"""Offline identity ownership migration. Call before starting any state writers."""
from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import sqlite3
import time
import uuid
from contextlib import closing, contextmanager
from pathlib import Path


TARGET_APP_ID = "cli_aa42dc770a381cb3"
MARKER_TABLE = "feishu_identity_migrations"
QUARANTINE_TABLE = "feishu_identity_quarantine"
MANIFEST_NAME = "feishu_identity_profile.json"
_OPEN_ID = re.compile(r"ou_[A-Za-z0-9]+\Z")
_TOKEN = re.compile(r"ou_[A-Za-z0-9]+")
_LOCK_SECONDS = 300
_BLOCK_UNTIL = 253402300799.0
_OWNER_POLICY_VERSION = 2
_DEDICATED = {
    "notice_memory": "notice_memory", "daily_summary": "notice_daily_summary",
    "work_status": "notice_work_status", "hidden_ongoing": "notice_hidden_ongoing",
    "notice_actions": "notice_action_job", "qt_active_cache": "qt_active_cache",
    "qt_notice_history": "qt_notice_history", "runtime_state": "runtime_state",
}
_IDENTITIES = {
    "auth_permissions": ("open_id",), "permission_requests": ("open_id",),
    "notice_upload_attachments": ("open_id",),
    "engineer_mop_local_files": ("created_by_openid",),
    "mop_signature_usage_confirmations": ("signer_open_id", "requested_by_openid"),
    "mop_temporary_signature_sessions": ("created_by",),
    "signature_link_tokens": ("created_by",), "mop_notice_bindings": ("updated_by",),
    "mop_fill_memory": ("updated_by",), "handover_links": ("updated_by",),
    "notice_work_type_overrides": ("updated_by",),
    "business_operation_audits": ("actor_open_id",), "deletion_audit_outbox": ("actor_open_id",),
    "water_consumption_operations": ("actor_open_id",),
    "water_consumption_record_edits": ("actor_open_id",),
    "critical_guard_tasks": ("created_by_open_id",),
    "critical_guard_responses": ("submitted_by_open_id",),
    "critical_guard_signature_sets": ("updated_by_open_id",),
    "critical_guard_memories": ("updated_by_open_id",),
    "critical_guard_scope_files": ("uploaded_by_open_id",),
    "critical_guard_scope_templates": ("updated_by_open_id",),
    "critical_guard_weather_jobs": ("created_by_open_id",),
    "batches": ("owner_id",), "definitions": ("actor",), "tasks": ("actor_id",),
    "files": ("uploader_id",), "photo_preferences": ("actor_id",),
    "photo_previews": ("actor_id",),
    "photo_deliveries": ("recipient",),
    "work_handovers": ("owner", "recipient"), "work_handover_audit": ("actor",),
    "work_handover_outbox": ("target",), "risk_releases": ("owner",),
    "risk_people": ("person_id",), "risk_audit": ("actor", "person_id"), "risk_files": ("person_id",),
    "knowledge_groups": ("owner",),
}
_JSON_TABLES = frozenset(_IDENTITIES) | frozenset(_DEDICATED) | {
    "auth_sessions", "auth_oauth_states", "json_documents", "documents", "append_events",
    "ongoing_items", "qt_active_items", "clipboard_candidates", "dialog_sessions",
    "event_outbox", "runtime_task_queue", "notice_remote_operations", "repair_management_operations",
    "notice_undo_actions", "work_handover_versions", "work_handover_sources", "risk_people",
    "risk_jobs", "notice_handoffs", "batch_rows", "batch_images", "batch_runtime_status",
    "batch_list_views", "records", "inventory", "rosters", "photo_deliveries", "deliveries",
}
_PLAIN_JSON = {
    "documents": ("payload",), "records": ("payload",), "inventory": ("payload",),
    "definitions": ("items",), "rosters": ("people",),
    "tasks": ("items", "people", "answers", "evidence"),
    "work_handovers": ("payload",), "work_handover_audit": ("payload",),
    "work_handover_versions": ("payload",), "work_handover_sources": ("payload",),
    "work_handover_outbox": ("payload",), "risk_releases": ("data",),
    "risk_people": ("data",), "risk_audit": ("data",), "risk_jobs": ("payload",),
    "photo_previews": ("result",),
}
_CHAT = {"lighthouse_ai", "lighthouse_runs", "lighthouse_messages", "lighthouse_agent_plans",
         "lighthouse_files", "lighthouse_appearance", "workspace_layout"}
_DELIVERIES = {"event_outbox", "work_handover_outbox", "risk_jobs", "photo_deliveries", "deliveries"}
_TERMINAL = {"completed", "done", "sent", "stopped", "archived", "cancelled", "canceled",
             "revoked", "expired", "deleted", "success", "succeeded"}


class _Mappings(dict):
    def __init__(self, values, unresolved_old=()):
        super().__init__(values)
        self.unresolved_old = frozenset(unresolved_old) - set(values) - set(values.values())


class IdentityMigrationError(RuntimeError):
    def __init__(self, code, *, issues=None):
        self.code = code
        self.report = {"ready": False, "code": code, "issues": issues or [{"code": code}]}
        super().__init__("Feishu identity migration blocked: " + code)


def _fail(code, database=None, table=None):
    issue = {"code": code}
    if database is not None:
        issue["database"] = database
    if table is not None:
        issue["table"] = table
    raise IdentityMigrationError(code, issues=[issue])


def _dumps(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _loads(value):
    def pairs(items):
        result = {}
        for key, part in items:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = part
        return result
    return json.loads(value, object_pairs_hook=pairs,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError("invalid number")))


@contextmanager
def _process_lock(root):
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".feishu_identity_migration.lock").open("a+b") as handle:
        if handle.seek(0, os.SEEK_END) == 0:
            handle.write(b"\0")
            handle.flush()
        deadline = time.monotonic() + _LOCK_SECONDS
        while True:
            try:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    _fail("migration_lock_timeout")
                time.sleep(.05)
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _manifest(root, manifest_path, manifest):
    try:
        from .feishu_identity_manifest import PROFILE_PATH, validate_manifest
        if isinstance(manifest_path, dict) and manifest is None:
            manifest, manifest_path = manifest_path, None
        if manifest is None:
            path = Path(manifest_path) if manifest_path is not None else PROFILE_PATH
            if not path.is_file():
                _fail("manifest_missing")
            if path.stat().st_size > 4 * 1024 * 1024:
                _fail("manifest_invalid")
            manifest = validate_manifest(_loads(path.read_text(encoding="utf-8-sig")))
        else:
            manifest = validate_manifest(copy.deepcopy(manifest))
        mapping = _Mappings({row["old_open_id"]: row["new_open_id"] for row in manifest["mappings"]},
                            (identity for row in manifest["unresolved"] for identity in row["old_open_ids"]))
        if len(set(mapping.values())) != len(mapping) or set(mapping) & set(mapping.values()):
            _fail("manifest_mapping_conflict")
        stable = {"version": 1, "target_app_id": TARGET_APP_ID,
                  "mappings": sorted(mapping.items()), "policy_version": _OWNER_POLICY_VERSION,
                  "unresolved_old_open_ids": sorted(mapping.unresolved_old)}
        return mapping, hashlib.sha256(_dumps(stable).encode()).hexdigest(), len(manifest.get("unresolved", []))
    except IdentityMigrationError:
        raise
    except Exception:
        _fail("manifest_invalid")


def _databases(root):
    patterns = ("lan_portal_state.sqlite3", "lighthouse_openclaw/assistant.sqlite3",
                "learning/learning.sqlite3", "pre_shift_checks/checks.sqlite3",
                "cabinet_power/batches.sqlite3", "cabinet_power/buildings/*.sqlite3",
                "lighthouse_openclaw/knowledge_base/knowledge.sqlite3",
                "work_handover.sqlite3", "work_handovers.sqlite3", "work_handover/*.sqlite3",
                "risk_inspection.sqlite3", "risk_inspections.sqlite3", "risk_inspection/*.sqlite3")
    paths = set()
    for pattern in patterns:
        for path in root.glob(pattern):
            if path.is_file():
                if path.is_symlink() or not path.resolve().is_relative_to(root):
                    _fail("database_path_invalid")
                paths.add(path)
    return sorted(paths, key=lambda path: path.relative_to(root).as_posix())


def _connect(path, *, write=False):
    connection = sqlite3.connect(path.as_uri() + ("?mode=rw" if write else "?mode=ro"), uri=True, timeout=10)
    connection.row_factory = sqlite3.Row
    if not write:
        connection.execute("PRAGMA query_only=ON")
    return connection


def _quote(name):
    return '"' + name.replace('"', '""') + '"'


def _transform_document(namespace, key, payload, mapping):
    try:
        from .feishu_identity_documents import transform_document
    except ImportError:
        _fail("document_transformer_missing")
    try:
        return transform_document(namespace, key, copy.deepcopy(payload), mapping)
    except Exception:
        _fail("document_transform_failed", table=namespace)


def _mentions(value, identities):
    if isinstance(value, dict):
        return any(_mentions(key, identities) or _mentions(part, identities) for key, part in value.items())
    if isinstance(value, (list, tuple)):
        return any(_mentions(part, identities) for part in value)
    return isinstance(value, str) and bool(set(_TOKEN.findall(value)) & identities)


def _check_owner(owner, mapping, database, table):
    if isinstance(owner, dict):
        owner = owner.get("id") or owner.get("open_id")
    return owner if isinstance(owner, str) and owner in getattr(mapping, "unresolved_old", ()) else None


def _document_owners(namespace, key, payload, mapping, database):
    if namespace not in _CHAT:
        return set()
    unresolved = set()
    def collect(owner):
        if isinstance(owner, dict):
            owner = owner.get("id") or owner.get("open_id")
        if isinstance(owner, str) and _OPEN_ID.fullmatch(owner) and owner not in mapping and owner not in mapping.values():
            unresolved.add(owner)
    if isinstance(payload, dict):
        for field in ("owner", "owner_id"):
            collect(payload.get(field))
    if namespace in {"lighthouse_ai", "lighthouse_appearance"}:
        for identity in _TOKEN.findall(key):
            collect(identity)
    if namespace == "workspace_layout":
        try:
            collect(_loads(key)[0])
        except (ValueError, IndexError, TypeError):
            _fail("document_key_invalid", database, namespace)
    return unresolved


def _read_markers(connection, tables, mapping, plan_hash, database):
    if MARKER_TABLE not in tables:
        return False, mapping
    done = False
    previous_mapping = {}
    for row in connection.execute(f"SELECT plan_hash,target_app_id,version,mappings_json,counts_json FROM {MARKER_TABLE}"):
        previous = _loads(row["mappings_json"])
        if (not isinstance(previous, dict) or row["target_app_id"] != TARGET_APP_ID or row["version"] != 1
                or not isinstance(_loads(row["counts_json"]), dict)):
            _fail("migration_marker_invalid", database)
        if any(old in mapping and mapping[old] != new or old in previous_mapping and previous_mapping[old] != new
               for old, new in previous.items()):
            _fail("mapping_history_conflict", database)
        previous_mapping.update(previous)
        done |= row["plan_hash"] == plan_hash
    combined = _Mappings({**previous_mapping, **mapping}, getattr(mapping, "unresolved_old", ()))
    if len(set(combined.values())) != len(combined) or set(combined) & set(combined.values()):
        _fail("mapping_history_conflict", database)
    return done, combined


def _pause_delivery(table, namespace, original, payload):
    state = str(original.get("status") or original.get("state") or "")
    if namespace == "message_delivery":
        state = str(payload.get("status") or "")
        return state not in _TERMINAL and not (state == "queued" and not payload.get("parts"))
    if state in _TERMINAL or original.get("sent") == 1:
        return False
    if table in {"deliveries", "photo_deliveries"}:
        return bool(original.get("first_attempt") or original.get("lease") or original.get("image_key"))
    return state in {"running", "leased", "sending", "uncertain", "posting", "failed"} or bool(original.get("attempts"))


def _plan_database(connection, mapping, plan_hash, database, *, quarantines=None, inserts=None):
    tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    done, mapping = _read_markers(connection, tables, mapping, plan_hash, database)
    if done:
        return True, [], 0
    changes, oauth_count = [], 0
    quarantines = quarantines if quarantines is not None else []
    inserts = inserts if inserts is not None else []
    for table in sorted(tables & _JSON_TABLES):
        schema = list(connection.execute(f"PRAGMA table_info({_quote(table)})"))
        columns = {row["name"] for row in schema}
        primary = [row["name"] for row in sorted(schema, key=lambda row: row["pk"]) if row["pk"]]
        identities = set(_IDENTITIES.get(table, ())) & columns
        if table == "documents" and "owner" in columns:
            identities.add("owner")
        json_columns = {name for name in columns if name.endswith("_json")} | (set(_PLAIN_JSON.get(table, ())) & columns)
        if not primary:
            _fail("table_primary_key_missing", database, table)
        projected = set()
        for row in connection.execute(f"SELECT * FROM {_quote(table)}"):
            original = dict(row)
            updated = dict(original)
            unresolved_owners = set()
            unverified_owners = set()
            for field in identities:
                if table == "photo_deliveries" and original.get("sent") == 1:
                    continue
                if table == "auth_permissions" and field == "open_id":
                    if identity := _check_owner(original[field], mapping, database, table):
                        unresolved_owners.add(identity)
                    elif (_OPEN_ID.fullmatch(str(original[field])) and original[field] not in mapping
                          and original[field] not in mapping.values()):
                        unverified_owners.add(original[field])
                if original[field] in mapping:
                    updated[field] = mapping[original[field]]
            parsed = {}
            for field in sorted(json_columns):
                raw = original[field]
                if raw in (None, "") and not field.endswith("_json"):
                    continue
                try:
                    payload = _loads(raw)
                except (ValueError, TypeError):
                    _fail("invalid_json", database, table)
                parsed[field] = payload
                namespace = str(original.get("namespace") or _DEDICATED.get(table) or table)
                key = str(original.get("key", original[primary[0]]))
                if table == "auth_sessions" and field in {"user_json", "payload_json"}:
                    if not isinstance(payload, dict) or field == "payload_json" and not isinstance(payload.get("user"), dict):
                        _fail("session_json_invalid", database, table)
                    user = payload if field == "user_json" else payload.get("user", {}) if isinstance(payload, dict) else {}
                    if isinstance(user, dict):
                        if identity := _check_owner(user.get("open_id"), mapping, database, table):
                            unresolved_owners.add(identity)
                        elif (isinstance(user.get("open_id"), str) and _OPEN_ID.fullmatch(user["open_id"])
                              and user["open_id"] not in mapping and user["open_id"] not in mapping.values()):
                            unverified_owners.add(user["open_id"])
                        if user.get("open_id") in mapping:
                            user["open_id"] = mapping[user["open_id"]]
                if table == "documents" and original.get("kind") == "person":
                    namespace = "learning_person"
                for identity in _document_owners(namespace, key, payload, mapping, database):
                    if identity in getattr(mapping, "unresolved_old", ()):
                        unresolved_owners.add(identity)
                    else:
                        unverified_owners.add(identity)
                new_key, new_payload = _transform_document(namespace, key, payload, mapping)
                if namespace == "account_last_login":
                    new_key = mapping.get(key, key)
                if table == "risk_jobs" and field == "payload" and isinstance(new_payload, dict):
                    for identity_field in ("person_id", "subject_id"):
                        identity = new_payload.get(identity_field)
                        if isinstance(identity, str) and identity in mapping:
                            new_payload[identity_field] = mapping[identity]
                if field == "recipient_open_ids_json" and isinstance(new_payload, list):
                    new_payload = [mapping.get(identity, identity) for identity in new_payload]
                if not isinstance(new_key, str):
                    _fail("document_transform_invalid", database, table)
                if "key" in columns and field in {"payload_json", "payload"}:
                    updated["key"] = new_key
                encoded = _dumps(new_payload)
                if _loads(raw) != new_payload:
                    updated[field] = encoded
            if table == "auth_oauth_states":
                if not done:
                    oauth_count += 1
                continue
            row_key = _dumps([original[field] for field in primary])
            for identity in sorted(unverified_owners):
                quarantines.append((table, hashlib.sha256(row_key.encode()).hexdigest(),
                                    hashlib.sha256(identity.encode()).hexdigest(), "unverified_owner_retained"))
            for identity in unresolved_owners:
                quarantines.append((table, hashlib.sha256(row_key.encode()).hexdigest(),
                                    hashlib.sha256(identity.encode()).hexdigest(), "unresolved_ownership"))
            if table == "auth_sessions" and {"user_json", "payload_json"} <= set(parsed):
                if parsed["user_json"].get("open_id") != parsed["payload_json"]["user"].get("open_id"):
                    _fail("session_identity_conflict", database, table)
            namespace = original.get("namespace")
            if (table in _DELIVERIES or namespace == "message_delivery") and _mentions([original, parsed], set(mapping)):
                job_payload = parsed.get("payload_json") or parsed.get("payload") or {}
                if _pause_delivery(table, namespace, original, job_payload):
                    original_state = {name: original[name] for name in
                                      ("status", "state", "sent", "lease", "attempts", "first_attempt") if name in original}
                    quarantines.append((table, hashlib.sha256(row_key.encode()).hexdigest(), "",
                                        "pending_delivery_quarantined", _dumps(original_state)))
                    if table == "work_handover_outbox":
                        updated["state"] = "migration_blocked"
                    elif table in {"risk_jobs", "event_outbox"}:
                        updated["status"] = "migration_blocked"
                    elif table in {"deliveries", "photo_deliveries"} and "lease" in columns:
                        updated["lease"] = _BLOCK_UNTIL
                    payload_field = next((field for field in ("payload_json", "payload", "data") if field in parsed), None)
                    if payload_field:
                        value = _loads(updated[payload_field])
                        if isinstance(value, dict):
                            metadata = value.setdefault("identity_migration", {})
                            if not isinstance(metadata, dict):
                                _fail("document_metadata_invalid", database, table)
                            metadata.update(blocked=True, reason="pending_delivery_quarantined",
                                            original_state=original_state)
                            updated[payload_field] = _dumps(value)
                if table == "deliveries" and (original.get("sent") or original.get("first_attempt") or original.get("lease")):
                    prefix, separator, identity = str(original.get("id", "")).rpartition(":")
                    if separator and identity in mapping:
                        guard_key = prefix + ":" + mapping[identity]
                        existing_guard = connection.execute("SELECT * FROM deliveries WHERE id=?", (guard_key,)).fetchone()
                        if existing_guard:
                            owned_guard = (QUARANTINE_TABLE in tables and connection.execute(
                                f"SELECT 1 FROM {QUARANTINE_TABLE} WHERE table_name='deliveries' AND key_hash=? AND reason='delivery_replay_guard'",
                                (hashlib.sha256(_dumps([guard_key]).encode()).hexdigest(),)).fetchone())
                            if not owned_guard or existing_guard["sent"] != 0 or existing_guard["lease"] != _BLOCK_UNTIL:
                                _fail("delivery_guard_conflict", database, table)
                            continue
                        guard = {**original, "id": guard_key, "sent": 0, "lease": _BLOCK_UNTIL}
                        if "error" in guard:
                            guard["error"] = "identity_migration_requires_review"
                        for receipt_field in ("message_id", "sender_app_id"):
                            if receipt_field in guard:
                                guard[receipt_field] = ""
                        inserts.append((table, guard))
                        quarantines.append((table, hashlib.sha256(_dumps([guard_key]).encode()).hexdigest(), "",
                                            "delivery_replay_guard", _dumps({name: original[name] for name in
                                                ("sent", "lease", "first_attempt", "message_id") if name in original})))
            new_primary = tuple(updated[field] for field in primary)
            if new_primary in projected:
                _fail("target_key_conflict", database, table)
            projected.add(new_primary)
            if updated != original:
                changes.append((table, primary, original, updated))
    return done, changes, oauth_count


def _backup(path, directory):
    directory.mkdir(parents=True, exist_ok=True)
    target_path = directory / (path.name + "." + uuid.uuid4().hex + ".sqlite3.bak")
    temporary = target_path.with_suffix(".tmp")
    try:
        with closing(_connect(path)) as source, closing(sqlite3.connect(temporary)) as target:
            source.execute("BEGIN")
            source.execute("SELECT name FROM sqlite_master LIMIT 1").fetchone()
            source.backup(target)
            if [row[0] for row in target.execute("PRAGMA quick_check")] != ["ok"]:
                _fail("backup_invalid")
        os.replace(temporary, target_path)
    finally:
        temporary.unlink(missing_ok=True)


def _commit_database(path, root, mapping, plan_hash):
    database = path.relative_to(root).as_posix()
    with closing(_connect(path, write=True)) as connection:
        try:
            connection.execute("BEGIN IMMEDIATE")
            quarantines, inserts = [], []
            done, changes, oauth_count = _plan_database(connection, mapping, plan_hash, database,
                                                       quarantines=quarantines, inserts=inserts)
            if done:
                connection.rollback()
                saved = _loads(connection.execute(f"SELECT counts_json FROM {MARKER_TABLE} WHERE plan_hash=?", (plan_hash,)).fetchone()[0])
                return {"resumed": 1, "changed_rows": 0, "expired_oauth_states": 0,
                        "quarantined_rows": saved.get("quarantined_rows", 0), "issues": saved.get("issues", [])}
            _backup(path, root / "backups" / "feishu_identity" / plan_hash / path.relative_to(root).parent)
            for table, primary, original, updated in changes:
                fields = [name for name in updated if updated[name] != original[name]]
                sql = f"UPDATE {_quote(table)} SET " + ",".join(_quote(name) + "=?" for name in fields)
                sql += " WHERE " + " AND ".join(_quote(name) + " IS ?" for name in primary)
                if connection.execute(sql, [updated[name] for name in fields] + [original[name] for name in primary]).rowcount != 1:
                    _fail("row_changed_during_migration", database, table)
            for table, value in inserts:
                names = list(value)
                connection.execute(f"INSERT INTO {_quote(table)} (" + ",".join(map(_quote, names)) + ") VALUES ("
                                   + ",".join("?" for _ in names) + ")", [value[name] for name in names])
            if oauth_count:
                connection.execute("DELETE FROM auth_oauth_states")
            if quarantines:
                connection.execute(f"CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} ("
                                   "table_name TEXT NOT NULL,key_hash TEXT NOT NULL,identity_hash TEXT NOT NULL,"
                                   "reason TEXT NOT NULL,plan_hash TEXT NOT NULL,original_state_json TEXT NOT NULL DEFAULT '{}',"
                                   "PRIMARY KEY(table_name,key_hash,identity_hash))")
                if "original_state_json" not in {row[1] for row in connection.execute(f"PRAGMA table_info({QUARANTINE_TABLE})")}:
                    connection.execute(f"ALTER TABLE {QUARANTINE_TABLE} ADD COLUMN original_state_json TEXT NOT NULL DEFAULT '{{}}'")
                connection.executemany(f"INSERT OR IGNORE INTO {QUARANTINE_TABLE} VALUES(?,?,?,?,?,?)",
                                       [(*row[:4], plan_hash, row[4] if len(row) > 4 else "{}") for row in quarantines])
            if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (QUARANTINE_TABLE,)).fetchone():
                # Policy 1 retained ownership rows without usable auth guard hashes.
                connection.execute(f"DELETE FROM {QUARANTINE_TABLE} WHERE reason='unverified_owner_retained' AND identity_hash=''")
                connection.executemany(f"DELETE FROM {QUARANTINE_TABLE} WHERE identity_hash=? "
                                       "AND reason IN ('unverified_owner_retained','unresolved_ownership')",
                                       [(hashlib.sha256(identity.encode()).hexdigest(),) for identity in mapping.values()])
            connection.execute(f"CREATE TABLE IF NOT EXISTS {MARKER_TABLE} ("
                               "plan_hash TEXT PRIMARY KEY,target_app_id TEXT NOT NULL,version INTEGER NOT NULL,"
                               "mappings_json TEXT NOT NULL,counts_json TEXT NOT NULL,completed_at REAL NOT NULL)")
            counts = {"changed_rows": len(changes) + len(inserts), "expired_oauth_states": oauth_count,
                      "quarantined_rows": len(quarantines)}
            issues = [{"code": reason, "database": database, "rows": sum(row[3] == reason for row in quarantines)}
                      for reason in sorted({row[3] for row in quarantines})]
            connection.execute(f"INSERT INTO {MARKER_TABLE} VALUES(?,?,?,?,?,?)",
                               (plan_hash, TARGET_APP_ID, 1, _dumps(mapping), _dumps({**counts, "issues": issues}), time.time()))
            connection.commit()
            return {"resumed": 0, **counts, "issues": issues}
        except BaseException:
            connection.rollback()
            raise


def is_identity_quarantined(connection, identity):
    """Auth guards reject unverified identities while original ownership is retained."""
    if not connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (QUARANTINE_TABLE,)).fetchone():
        return False
    return connection.execute(f"SELECT 1 FROM {QUARANTINE_TABLE} WHERE identity_hash=? LIMIT 1",
                              (hashlib.sha256(identity.encode()).hexdigest(),)).fetchone() is not None


def is_operation_quarantined(connection, table, primary_key):
    """primary_key is the original SQL primary-key tuple, in declaration order."""
    if not connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (QUARANTINE_TABLE,)).fetchone():
        return False
    digest = hashlib.sha256(_dumps(list(primary_key)).encode()).hexdigest()
    return connection.execute(f"SELECT 1 FROM {QUARANTINE_TABLE} WHERE table_name=? AND key_hash=? LIMIT 1",
                              (table, digest)).fetchone() is not None


def _legacy_plan(root, mapping):
    path = root / "lan_portal_auth.json"
    if not path.is_file():
        return None
    if path.is_symlink() or not path.resolve().is_relative_to(root) or path.stat().st_size > 4 * 1024 * 1024:
        _fail("legacy_auth_invalid")
    try:
        value = _loads(path.read_text(encoding="utf-8-sig"))
        if not isinstance(value, dict) or not isinstance(value.get("users"), dict):
            _fail("legacy_auth_invalid")
        _, transformed = _transform_document("permissions", "legacy", value, mapping)
        if not isinstance(transformed, dict) or not isinstance(transformed.get("users"), dict):
            _fail("legacy_auth_invalid")
        return path, value, transformed
    except IdentityMigrationError:
        raise
    except Exception:
        _fail("legacy_auth_invalid")


def _atomic_file(path, content):
    temporary = path.with_name("." + path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _migrate_legacy(root, mapping, plan_hash):
    plan = _legacy_plan(root, mapping)
    if plan is None:
        return "absent"
    path, original, transformed = plan
    directory = root / "backups" / "feishu_identity" / plan_hash
    directory.mkdir(parents=True, exist_ok=True)
    backup = directory / "lan_portal_auth.json.bak"
    if not backup.exists():
        _atomic_file(backup, path.read_bytes())
    else:
        try:
            _loads(backup.read_text(encoding="utf-8-sig"))
        except Exception:
            _fail("legacy_auth_backup_invalid")
    database = root / "lan_portal_state.sqlite3"
    has_permissions = False
    if database.exists():
        with closing(_connect(database)) as connection:
            if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='auth_permissions'").fetchone():
                has_permissions = connection.execute("SELECT 1 FROM auth_permissions WHERE open_id<>'__meta__' LIMIT 1").fetchone() is not None
    if has_permissions:
        # Keep a recoverable input, but never leave it available for fallback import.
        os.replace(path, directory / ("lan_portal_auth." + uuid.uuid4().hex + ".json.bak"))
        action = "archived"
    else:
        if original != transformed:
            _atomic_file(path, (_dumps(transformed) + "\n").encode())
        action = "rewritten"
    marker = {"version": 1, "plan_hash": plan_hash, "action": action,
              "output_sha256": hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None}
    _atomic_file(directory / "legacy_auth_marker.json", (_dumps(marker) + "\n").encode())
    return action


def identity_migration_pending(data_root, manifest_path=None, *, manifest=None):
    """Read only existing database markers, before admitting migration with live jobs.

    False covers the current database inventory only. The caller must keep its
    job-admission lock held through the pending check and subsequent migration.
    Invalid/unreadable inputs raise IdentityMigrationError, never a false result.
    """
    root = Path(data_root).resolve()
    try:
        mapping, plan_hash, _ = _manifest(root, manifest_path, manifest)
        paths = _databases(root)
        for path in paths:
            with closing(_connect(path)) as connection:
                connection.execute("BEGIN")
                if not connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (MARKER_TABLE,)).fetchone():
                    return True
                done, _ = _read_markers(connection, {MARKER_TABLE}, mapping, plan_hash,
                                        path.relative_to(root).as_posix())
                if not done:
                    return True
        return _databases(root) != paths
    except IdentityMigrationError:
        raise
    except Exception:
        _fail("migration_pending_read_failed")


def migrate_identity_databases(data_root, manifest_path=None, *, manifest=None):
    """Return a ready report only after all existing stores complete the same plan.

    The caller must hold its startup gate until this returns. Exceptions keep the
    gate closed; committed databases resume independently after a process crash.
    No service, credential loader, business operation or inbox is initialized.
    """
    root = Path(data_root).resolve()
    report = {"ready": False, "databases": 0, "migrated_databases": 0, "resumed_databases": 0,
              "changed_rows": 0, "expired_oauth_states": 0, "quarantined_rows": 0, "issues": []}
    try:
        with _process_lock(root):
            mapping, plan_hash, unresolved = _manifest(root, manifest_path, manifest)
            report.update(plan_hash=plan_hash, unresolved_records=unresolved)
            paths = _databases(root)
            report["databases"] = len(paths)
            _legacy_plan(root, mapping)
            for path in paths:
                with closing(_connect(path)) as connection:
                    connection.execute("BEGIN")
                    _plan_database(connection, mapping, plan_hash, path.relative_to(root).as_posix())
            for path in paths:
                counts = _commit_database(path, root, mapping, plan_hash)
                report["issues"].extend(counts.pop("issues"))
                resumed = counts.pop("resumed")
                report["resumed_databases"] += resumed
                report["migrated_databases"] += 1 - resumed
                for name, count in counts.items():
                    report[name] += count
            report["legacy_auth"] = _migrate_legacy(root, mapping, plan_hash)
            if _databases(root) != paths:
                _fail("database_inventory_changed")
            report["ready"] = True
            return report
    except IdentityMigrationError as exc:
        exc.report = {**report, **exc.report}
        raise
    except Exception:
        exc = IdentityMigrationError("database_migration_failed")
        exc.report = {**report, **exc.report}
        raise exc from None

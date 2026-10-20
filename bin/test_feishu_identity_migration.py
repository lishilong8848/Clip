"""Offline acceptance tests; every writable database is under TemporaryDirectory."""
import copy
import hashlib
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing, contextmanager
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal import feishu_identity_migration as migration


OLD = "ou_fixturePrevious"
NEW = "ou_fixtureCurrent"
SECOND = "ou_fixturePreviousB"
SECOND_NEW = "ou_fixtureCurrentB"


def dumps(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


@contextmanager
def connect(path):
    with closing(sqlite3.connect(path)) as db, db:
        yield db


def manifest(*pairs, unresolved=None):
    return {"version": 1, "target_app_id": migration.TARGET_APP_ID,
            "mappings": [{"old_open_id": old, "new_open_id": new, "record_ids": ["recFixture" + str(index)],
                          "evidence": ["personnel_snapshot", "target_hr_person_field"]}
                         for index, (old, new) in enumerate(pairs or ((OLD, NEW),))],
            "unresolved": unresolved or []}


class IdentityMigrationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="identity-migration-fixture-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.profile = manifest()

    def database(self, relative="lan_portal_state.sqlite3", schema="", inserts=()):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        with connect(path) as db:
            db.executescript(schema)
            for sql, values in inserts:
                db.execute(sql, values)
        return path

    def main(self):
        return self.database(schema="""
            CREATE TABLE auth_permissions(open_id TEXT PRIMARY KEY,role TEXT,scopes_json TEXT,
                enabled INTEGER,payload_json TEXT);
            CREATE TABLE auth_sessions(session_hash TEXT PRIMARY KEY,user_json TEXT,payload_json TEXT,
                expires_at REAL,revoked_at REAL,created_at REAL);
            CREATE TABLE auth_oauth_states(state_hash TEXT PRIMARY KEY,payload_json TEXT,expires_at REAL);
            CREATE TABLE permission_requests(request_id TEXT PRIMARY KEY,open_id TEXT,payload_json TEXT,code_hash TEXT);
            CREATE TABLE notice_upload_attachments(upload_id TEXT PRIMARY KEY,open_id TEXT,payload_json TEXT);
            CREATE TABLE json_documents(namespace TEXT,key TEXT,payload_json TEXT,updated_at REAL,PRIMARY KEY(namespace,key));
            CREATE TABLE notice_actions(key TEXT PRIMARY KEY,payload_json TEXT,updated_at REAL);
        """)

    def permission(self, path, identity=OLD, scopes=None, enabled=1):
        value = {"role": "building", "scopes": scopes or ["A"], "enabled": bool(enabled),
                 "modules": ["assistant"], "custom": "preserved"}
        with connect(path) as db:
            db.execute("INSERT INTO auth_permissions VALUES(?,?,?,?,?)",
                       (identity, "building", dumps(value["scopes"]), enabled, dumps(value)))
        return value

    def document(self, path, namespace, key, payload):
        with connect(path) as db:
            db.execute("INSERT INTO json_documents VALUES(?,?,?,?)", (namespace, key, dumps(payload), 123))

    def migrate(self, **kwargs):
        return migration.migrate_identity_databases(self.root, manifest=kwargs.pop("manifest", self.profile), **kwargs)

    def rows(self, path, sql, values=()):
        with connect(path) as db:
            return db.execute(sql, values).fetchall()

    def test_permission_session_cookie_expiry_and_business_ids_are_preserved(self):
        path = self.main()
        permission = self.permission(path, enabled=0)
        user = {"open_id": OLD, "union_id": "on_fixture", "name": "Fixture"}
        session = {"user": user, "password_revision": "fixture-password-revision", "allowed_scopes": ["A"]}
        with connect(path) as db:
            db.execute("INSERT INTO auth_sessions VALUES(?,?,?,?,?,?)", ("cookie-sha256", dumps(user), dumps(session), 9999999999, None, 123))
            db.execute("INSERT INTO auth_oauth_states VALUES(?,?,?)", ("old-oauth-state", "{}", 9999999999))
            db.execute("INSERT INTO permission_requests VALUES(?,?,?,?)", ("request-id", OLD, dumps({"open_id": OLD, "approver_open_id": OLD}), "code-hash"))
            db.execute("INSERT INTO notice_upload_attachments VALUES(?,?,?)", ("attachment-id", OLD, dumps({"owner": OLD})))
        result = self.migrate()
        self.assertTrue(result["ready"])
        self.assertEqual(result["expired_oauth_states"], 1)
        self.assertEqual(self.rows(path, "SELECT open_id,role,scopes_json,enabled,payload_json FROM auth_permissions"),
                         [(NEW, "building", dumps(["A"]), 0, dumps(permission))])
        saved = self.rows(path, "SELECT * FROM auth_sessions")[0]
        self.assertEqual((saved[0], saved[3], saved[4], saved[5]), ("cookie-sha256", 9999999999, None, 123))
        self.assertEqual(json.loads(saved[1])["open_id"], NEW)
        self.assertEqual(json.loads(saved[2]), {**session, "user": {**user, "open_id": NEW}})
        request = self.rows(path, "SELECT * FROM permission_requests")[0]
        self.assertEqual((request[0], request[1], request[3]), ("request-id", NEW, "code-hash"))
        self.assertEqual(json.loads(request[2])["approver_open_id"], NEW)
        self.assertEqual(self.rows(path, "SELECT upload_id,open_id FROM notice_upload_attachments"), [("attachment-id", NEW)])

    def test_conversations_archive_keys_body_layout_and_preferences_survive(self):
        path = self.main()
        turn = {"operation_id": "opaque-operation", "question": "literal " + OLD, "answer": "unchanged", "status": "completed"}
        self.document(path, "lighthouse_ai", "conversation:" + OLD, {"id": "conversation-uuid", "turns": [turn], "execution_lanes": {OLD + ":lanehash": {"context": {"owner": OLD}}}})
        self.document(path, "lighthouse_messages", "conversation-uuid:opaque-operation", turn)
        self.document(path, "lighthouse_files", "file-uuid", {"owner": OLD, "relative_path": "opaque-owner-hash/file.bin"})
        self.document(path, "lighthouse_appearance", OLD, {"shape": "fixture", "enabled": False})
        layout_key = dumps([OLD, "phone", "/workspace"])
        self.document(path, "workspace_layout", layout_key, {"revision": 7, "value": {"columns": 2}})
        with connect(path) as db:
            db.execute("INSERT INTO notice_actions VALUES(?,?,?)", ("job-uuid", dumps({"phase": "wait_result", "request": {"_auth_open_id": OLD}}), 123))
        self.migrate()
        docs = {(ns, key): json.loads(raw) for ns, key, raw in self.rows(path, "SELECT namespace,key,payload_json FROM json_documents")}
        state = docs["lighthouse_ai", "conversation:" + NEW]
        self.assertEqual(state["id"], "conversation-uuid")
        self.assertEqual(state["turns"], [turn])
        self.assertIn(NEW + ":lanehash", state["execution_lanes"])
        self.assertEqual(docs["lighthouse_messages", "conversation-uuid:opaque-operation"], turn)
        self.assertEqual(docs["lighthouse_files", "file-uuid"]["relative_path"], "opaque-owner-hash/file.bin")
        self.assertIn(("lighthouse_appearance", NEW), docs)
        self.assertEqual(docs["workspace_layout", dumps([NEW, "phone", "/workspace"])]["revision"], 7)
        job = json.loads(self.rows(path, "SELECT payload_json FROM notice_actions")[0][0])
        self.assertEqual(job["phase"], "wait_result")
        self.assertEqual(job["request"]["_auth_open_id"], NEW)

    def test_real_session_scope_array_is_not_validated_as_a_user_object(self):
        path = self.main()
        with connect(path) as db:
            db.execute("ALTER TABLE auth_sessions ADD COLUMN allowed_scopes_json TEXT")
            db.execute("INSERT INTO auth_sessions VALUES(?,?,?,?,?,?,?)",
                       ("cookie-hash", dumps({"open_id": OLD}), dumps({"user": {"open_id": OLD}}),
                        9999999999, None, 123, '["A","B"]'))
        self.migrate()
        saved = self.rows(path, "SELECT session_hash,user_json,allowed_scopes_json FROM auth_sessions")[0]
        self.assertEqual((saved[0], saved[2]), ("cookie-hash", '["A","B"]'))
        self.assertEqual(json.loads(saved[1])["open_id"], NEW)

    def test_repeated_run_only_reads_markers_and_does_not_expire_new_oauth(self):
        path = self.main()
        self.permission(path)
        self.migrate()
        with connect(path) as db:
            db.execute("INSERT INTO auth_oauth_states VALUES('new-state','{}',9999999999)")
        original = migration._connect
        def guarded_connect(*args, **kwargs):
            db = original(*args, **kwargs)
            db.set_authorizer(lambda action, table, *rest: sqlite3.SQLITE_DENY
                              if action == sqlite3.SQLITE_READ and table in {"auth_permissions", "auth_sessions", "json_documents"}
                              else sqlite3.SQLITE_OK)
            return db
        with patch.object(migration, "_connect", side_effect=guarded_connect), patch.object(migration, "_backup", side_effect=AssertionError("no repeat backup")):
            result = self.migrate()
        self.assertEqual((result["resumed_databases"], result["changed_rows"]), (1, 0))
        self.assertEqual(self.rows(path, "SELECT count(*) FROM auth_oauth_states"), [(1,)])

    def test_target_permission_collision_rolls_back_without_union_or_backup(self):
        path = self.main()
        self.permission(path, OLD, ["A"])
        self.permission(path, NEW, ["B"])
        before = self.rows(path, "SELECT * FROM auth_permissions ORDER BY open_id")
        with self.assertRaises(migration.IdentityMigrationError) as caught:
            self.migrate()
        self.assertEqual(caught.exception.code, "target_key_conflict")
        self.assertEqual(self.rows(path, "SELECT * FROM auth_permissions ORDER BY open_id"), before)
        self.assertFalse((self.root / "backups").exists())

    def test_pending_missing_marker_is_readonly_and_does_not_lock_or_plan_payloads(self):
        path = self.main()
        self.permission(path)
        before = path.read_bytes()
        files = set(self.root.rglob("*"))
        with patch.object(migration, "_process_lock", side_effect=AssertionError("read-only check cannot lock")), \
                patch.object(migration, "_plan_database", side_effect=AssertionError("cannot scan payloads")):
            self.assertTrue(migration.identity_migration_pending(self.root, manifest=self.profile))
            self.assertTrue(migration.identity_migration_pending(self.root, self.profile))
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(set(self.root.rglob("*")), files)

    def test_pending_done_queries_only_metadata_and_never_opens_writable_connection(self):
        path = self.main()
        self.permission(path)
        self.migrate()
        original = migration._connect
        def guarded_connect(path, **kwargs):
            self.assertFalse(kwargs.get("write", False))
            db = original(path, **kwargs)
            db.set_authorizer(lambda action, table, *rest: sqlite3.SQLITE_DENY
                              if action == sqlite3.SQLITE_READ and table not in {"sqlite_master", migration.MARKER_TABLE}
                              else sqlite3.SQLITE_OK)
            return db
        with patch.object(migration, "_connect", side_effect=guarded_connect), \
                patch.object(migration, "_process_lock", side_effect=AssertionError("no file lock")), \
                patch.object(migration, "_plan_database", side_effect=AssertionError("no payload scan")), \
                patch.object(migration, "_backup", side_effect=AssertionError("no backup")):
            self.assertFalse(migration.identity_migration_pending(self.root, manifest=self.profile))

    def test_pending_new_database_after_completed_migration_returns_true(self):
        self.main()
        self.migrate()
        self.assertFalse(migration.identity_migration_pending(self.root, manifest=self.profile))
        self.database("learning/learning.sqlite3", "CREATE TABLE documents(kind TEXT,key TEXT,payload TEXT,PRIMARY KEY(kind,key));")
        self.assertTrue(migration.identity_migration_pending(self.root, manifest=self.profile))
        self.migrate()
        self.assertFalse(migration.identity_migration_pending(self.root, manifest=self.profile))

    def test_pending_changed_plan_returns_true_without_touching_completed_markers(self):
        path = self.main()
        self.permission(path)
        self.migrate()
        markers = self.rows(path, f"SELECT * FROM {migration.MARKER_TABLE}")
        self.assertTrue(migration.identity_migration_pending(self.root, manifest=manifest((OLD, NEW), (SECOND, SECOND_NEW))))
        self.assertEqual(self.rows(path, f"SELECT * FROM {migration.MARKER_TABLE}"), markers)

    def test_pending_empty_install_does_not_create_data_root_or_database(self):
        root = self.root / "not-created"
        self.assertFalse(migration.identity_migration_pending(root, manifest=self.profile))
        self.assertFalse(root.exists())

    def test_pending_missing_manifest_raises_before_creating_any_files(self):
        files = set(self.root.rglob("*"))
        with self.assertRaises(migration.IdentityMigrationError) as caught:
            migration.identity_migration_pending(self.root, self.root / "missing-profile.json")
        self.assertEqual(caught.exception.code, "manifest_missing")
        self.assertEqual(set(self.root.rglob("*")), files)

    def test_pending_invalid_marker_raises_instead_of_claiming_done(self):
        path = self.main()
        self.migrate()
        with connect(path) as db:
            db.execute(f"UPDATE {migration.MARKER_TABLE} SET counts_json='[]'")
        with self.assertRaises(migration.IdentityMigrationError) as caught:
            migration.identity_migration_pending(self.root, manifest=self.profile)
        self.assertEqual(caught.exception.code, "migration_marker_invalid")

    def test_target_conversation_conflict_keeps_both_histories(self):
        path = self.main()
        for identity in (OLD, NEW):
            self.document(path, "lighthouse_ai", "conversation:" + identity, {"id": identity, "turns": []})
        before = self.rows(path, "SELECT * FROM json_documents ORDER BY key")
        with self.assertRaises(migration.IdentityMigrationError):
            self.migrate()
        self.assertEqual(self.rows(path, "SELECT * FROM json_documents ORDER BY key"), before)

    def test_cross_database_interrupt_resumes_only_uncommitted_database(self):
        main = self.main()
        self.permission(main)
        assistant = self.database("lighthouse_openclaw/assistant.sqlite3", "CREATE TABLE documents(namespace TEXT,key TEXT,payload_json TEXT,PRIMARY KEY(namespace,key));")
        with connect(assistant) as db:
            db.execute("INSERT INTO documents VALUES(?,?,?)", ("lighthouse_runs", "run-uuid", dumps({"owner": OLD})))
        original = migration._commit_database
        class SimulatedCrash(BaseException):
            pass
        calls = []
        def commit(*args):
            if calls:
                raise SimulatedCrash()
            calls.append(1)
            return original(*args)
        with patch.object(migration, "_commit_database", side_effect=commit), self.assertRaises(SimulatedCrash):
            self.migrate()
        self.assertEqual(self.rows(main, "SELECT open_id FROM auth_permissions"), [(NEW,)])
        self.assertEqual(json.loads(self.rows(assistant, "SELECT payload_json FROM documents")[0][0])["owner"], OLD)
        self.assertTrue(migration.identity_migration_pending(self.root, manifest=self.profile))
        result = self.migrate()
        self.assertFalse(migration.identity_migration_pending(self.root, manifest=self.profile))
        self.assertEqual((result["resumed_databases"], result["migrated_databases"]), (1, 1))
        self.assertEqual(json.loads(self.rows(assistant, "SELECT payload_json FROM documents")[0][0])["owner"], NEW)

    def test_invalid_json_in_later_database_prevents_any_commit(self):
        main = self.main()
        self.permission(main)
        self.database("lighthouse_openclaw/assistant.sqlite3", "CREATE TABLE documents(namespace TEXT,key TEXT,payload_json TEXT,PRIMARY KEY(namespace,key));",
                      [("INSERT INTO documents VALUES(?,?,?)", ("lighthouse_runs", "run", '{"owner":'))])
        with self.assertRaises(migration.IdentityMigrationError) as caught:
            self.migrate()
        self.assertEqual(caught.exception.code, "invalid_json")
        self.assertEqual(self.rows(main, "SELECT open_id FROM auth_permissions"), [(OLD,)])
        self.assertFalse((self.root / "backups").exists())

    def test_backup_is_independent_valid_sqlite_with_original_ownership(self):
        path = self.main()
        self.permission(path)
        self.migrate()
        backups = list((self.root / "backups").rglob("*.sqlite3.bak"))
        self.assertEqual(len(backups), 1)
        self.assertEqual(self.rows(backups[0], "PRAGMA quick_check"), [("ok",)])
        self.assertEqual(self.rows(backups[0], "SELECT open_id FROM auth_permissions"), [(OLD,)])

    def test_unverified_owner_keeps_permission_session_and_history_but_is_auth_quarantined(self):
        path = self.main()
        identity = "ou_fixtureUnverifiedCurrent"
        permission = self.permission(path, identity, ["B"])
        self.document(path, "lighthouse_ai", "conversation:" + identity, {"id": "unknown-history", "turns": []})
        with connect(path) as db:
            db.execute("INSERT INTO auth_sessions VALUES(?,?,?,?,?,?)",
                       ("unknown-cookie-hash", dumps({"open_id": identity}),
                        dumps({"user": {"open_id": identity}, "source": "personnel_password", "password_revision": "unchanged"}),
                        9999999999, None, 123))
        original_permissions = self.rows(path, "SELECT * FROM auth_permissions")
        original_sessions = self.rows(path, "SELECT * FROM auth_sessions")
        original_history = self.rows(path, "SELECT * FROM json_documents")
        result = self.migrate()
        self.assertTrue(result["ready"])
        self.assertTrue(any(issue["code"] == "unverified_owner_retained" for issue in result["issues"]))
        self.assertEqual(json.loads(self.rows(path, "SELECT payload_json FROM auth_permissions")[0][0]), permission)
        self.assertEqual(self.rows(path, "SELECT * FROM auth_permissions"), original_permissions)
        self.assertEqual(self.rows(path, "SELECT * FROM auth_sessions"), original_sessions)
        self.assertEqual(self.rows(path, "SELECT * FROM json_documents"), original_history)
        with connect(path) as db:
            self.assertTrue(migration.is_identity_quarantined(db, identity))
            self.assertFalse(migration.is_identity_quarantined(db, NEW))
        hashes = self.rows(path, f"SELECT identity_hash FROM {migration.QUARANTINE_TABLE} WHERE reason='unverified_owner_retained'")
        self.assertEqual(set(hashes), {(hashlib.sha256(identity.encode()).hexdigest(),)})
        self.assertIn(("conversation:" + identity,), self.rows(path, "SELECT key FROM json_documents"))

    def test_unknown_session_without_permission_row_is_retained_and_auth_quarantined(self):
        path = self.main()
        identity = "ou_fixtureUnverifiedSession"
        with connect(path) as db:
            db.execute("INSERT INTO auth_sessions VALUES(?,?,?,?,?,?)",
                       ("unknown-cookie-hash", dumps({"open_id": identity}), dumps({"user": {"open_id": identity}}),
                        9999999999, None, 123))
        before = self.rows(path, "SELECT * FROM auth_sessions")
        self.assertTrue(self.migrate()["ready"])
        self.assertEqual(self.rows(path, "SELECT * FROM auth_sessions"), before)
        with connect(path) as db:
            self.assertTrue(migration.is_identity_quarantined(db, identity))

    def test_policy_upgrade_rebuilds_legacy_empty_hashes_then_keeps_marker_fast_path(self):
        path = self.main()
        identity = "ou_fixtureUnverifiedLegacy"
        self.permission(path, identity, ["B"])
        with patch.object(migration, "_OWNER_POLICY_VERSION", 1):
            previous = self.migrate()
        with connect(path) as db:
            db.execute(f"UPDATE {migration.QUARANTINE_TABLE} SET identity_hash='' WHERE reason='unverified_owner_retained'")
            self.assertFalse(migration.is_identity_quarantined(db, identity))
        before = self.rows(path, "SELECT * FROM auth_permissions")
        current = self.migrate()
        self.assertNotEqual(previous["plan_hash"], current["plan_hash"])
        self.assertEqual(self.rows(path, "SELECT * FROM auth_permissions"), before)
        self.assertEqual(self.rows(path, f"SELECT count(*) FROM {migration.QUARANTINE_TABLE} WHERE identity_hash='' AND reason='unverified_owner_retained'"), [(0,)])
        with connect(path) as db:
            self.assertTrue(migration.is_identity_quarantined(db, identity))
        with patch.object(migration, "_backup", side_effect=AssertionError("no repeated migration")):
            self.assertEqual(self.migrate()["resumed_databases"], 1)

    def test_verified_target_manifest_releases_only_identity_quarantine(self):
        path = self.main()
        identity = "ou_fixtureUnverifiedTarget"
        self.permission(path, identity, ["B"])
        self.migrate()
        before = self.rows(path, "SELECT * FROM auth_permissions")
        with connect(path) as db:
            self.assertTrue(migration.is_identity_quarantined(db, identity))
            db.execute(f"INSERT INTO {migration.QUARANTINE_TABLE} VALUES(?,?,?,?,?,?)",
                       ("event_outbox", "opaque-operation-hash", "", "pending_delivery_quarantined", "earlier-plan", "{}"))
        self.migrate(manifest=manifest((SECOND, identity)))
        self.assertEqual(self.rows(path, "SELECT * FROM auth_permissions"), before)
        with connect(path) as db:
            self.assertFalse(migration.is_identity_quarantined(db, identity))
        self.assertEqual(self.rows(path, f"SELECT reason FROM {migration.QUARANTINE_TABLE} WHERE key_hash='opaque-operation-hash'"), [("pending_delivery_quarantined",)])

    def test_explicit_unresolved_old_owner_is_preserved_and_quarantined(self):
        path = self.main()
        identity = "ou_fixtureUnresolvedPrevious"
        self.permission(path, identity)
        self.profile["unresolved"] = [{"reason": "old_evidence_conflict", "old_open_ids": [identity],
                                      "new_open_ids": [], "record_ids": ["recUnknown"], "evidence": []}]
        result = self.migrate()
        self.assertTrue(result["ready"])
        self.assertEqual(self.rows(path, "SELECT open_id,enabled FROM auth_permissions"), [(identity, 1)])
        with connect(path) as db:
            self.assertTrue(migration.is_identity_quarantined(db, identity))

    def test_unrelated_hr_unresolved_rows_do_not_block_verified_accounts(self):
        path = self.main()
        self.permission(path)
        self.profile["unresolved"] = [{"reason": "missing_or_ambiguous_target_person", "old_open_ids": [],
                                      "new_open_ids": [], "record_ids": ["recUnrelated"], "evidence": []}]
        result = self.migrate()
        self.assertTrue(result["ready"])
        self.assertEqual(result["quarantined_rows"], 0)

    def test_profile_evidence_and_record_validation_use_shared_validator(self):
        for modify in (lambda p: p["mappings"][0].update(evidence=["untrusted"]),
                       lambda p: p["mappings"][0].update(record_ids=[]),
                       lambda p: p.update(target_app_id="cli_fixtureOther")):
            with self.subTest(modify=modify):
                value = copy.deepcopy(self.profile)
                modify(value)
                with self.assertRaises(migration.IdentityMigrationError):
                    self.migrate(manifest=value)

    def test_missing_profile_is_a_clear_packaging_failure_even_for_empty_install(self):
        with self.assertRaises(migration.IdentityMigrationError) as caught:
            migration.migrate_identity_databases(self.root, self.root / "missing-profile.json")
        self.assertEqual(caught.exception.code, "manifest_missing")
        result = self.migrate()
        self.assertTrue(result["ready"])
        self.assertEqual(result["databases"], 0)
        self.assertEqual(list(self.root.rglob("*.sqlite3")), [])

    def test_order_and_evidence_do_not_change_plan_hash(self):
        first = manifest((OLD, NEW), (SECOND, SECOND_NEW))
        second = copy.deepcopy(first)
        second["mappings"].reverse()
        second["mappings"][0]["evidence"].reverse()
        self.assertEqual(self.migrate(manifest=first)["plan_hash"], self.migrate(manifest=second)["plan_hash"])

    def test_supplemental_plan_migrates_additional_verified_account(self):
        path = self.main()
        self.permission(path, OLD)
        self.permission(path, SECOND, ["B"])
        self.migrate()
        result = self.migrate(manifest=manifest((SECOND, SECOND_NEW)))
        self.assertEqual(result["migrated_databases"], 1)
        self.assertEqual(set(self.rows(path, "SELECT open_id FROM auth_permissions")), {(NEW,), (SECOND_NEW,)})
        self.assertEqual(self.rows(path, f"SELECT count(*) FROM {migration.MARKER_TABLE}"), [(2,)])

    def test_conflicting_supplemental_plan_cannot_move_completed_identity(self):
        path = self.main()
        self.permission(path)
        self.migrate()
        with self.assertRaises(migration.IdentityMigrationError) as caught:
            self.migrate(manifest=manifest((OLD, SECOND_NEW)))
        self.assertEqual(caught.exception.code, "mapping_history_conflict")
        self.assertEqual(self.rows(path, "SELECT open_id FROM auth_permissions"), [(NEW,)])

    def test_completed_opaque_message_receipts_and_job_id_remain(self):
        path = self.main()
        payload = {"id": "opaque-job-key", "owner": OLD, "status": "completed", "text": "unchanged",
                   "recipients": [{"open_id": OLD}], "files": [],
                   "parts": {"opaque-part-hash": {"status": "sent", "message_id": "remote-receipt", "message_uuid": "original-uuid"}}}
        self.document(path, "message_delivery", "opaque-job-key", payload)
        self.migrate()
        saved = json.loads(self.rows(path, "SELECT payload_json FROM json_documents")[0][0])
        self.assertEqual(saved["owner"], NEW)
        self.assertEqual(saved["id"], payload["id"])
        self.assertEqual(saved["parts"], payload["parts"])
        self.assertEqual(self.rows(path, "SELECT key FROM json_documents"), [("opaque-job-key",)])

    def test_queued_never_attempted_jobs_continue_but_uncertain_jobs_are_not_claimable(self):
        path = self.database(schema="""
            CREATE TABLE risk_jobs(id TEXT PRIMARY KEY,status TEXT,attempts INTEGER,payload TEXT);
            CREATE TABLE event_outbox(id INTEGER PRIMARY KEY,status TEXT,attempts INTEGER,payload_json TEXT);
            CREATE TABLE work_handover_outbox(id TEXT PRIMARY KEY,state TEXT,attempts INTEGER,target TEXT,payload TEXT);
        """)
        with connect(path) as db:
            for identity, state, attempts in (("queued-job", "queued", 0), ("uncertain-job", "running", 1)):
                db.execute("INSERT INTO risk_jobs VALUES(?,?,?,?)", (identity, state, attempts, dumps({"owner": OLD})))
            db.execute("INSERT INTO event_outbox VALUES(1,'leased',1,?)", (dumps({"owner": OLD}),))
            db.execute("INSERT INTO work_handover_outbox VALUES('handover-job','failed',1,?,?)", (OLD, dumps({"owner": OLD})))
        result = self.migrate()
        self.assertTrue(result["ready"])
        self.assertEqual(self.rows(path, "SELECT id FROM risk_jobs WHERE status IN ('queued','running')"), [("queued-job",)])
        self.assertEqual(self.rows(path, "SELECT id FROM event_outbox WHERE status IN ('pending','leased')"), [])
        self.assertEqual(self.rows(path, "SELECT id FROM work_handover_outbox WHERE state IN ('queued','failed')"), [])
        blocked = json.loads(self.rows(path, "SELECT payload FROM risk_jobs WHERE id='uncertain-job'")[0][0])
        self.assertEqual(blocked["identity_migration"]["original_state"]["status"], "running")

    def test_pre_shift_receipts_keep_old_ids_and_guard_new_recipient_lookup(self):
        path = self.database("pre_shift_checks/checks.sqlite3", """
            CREATE TABLE deliveries(id TEXT PRIMARY KEY,lease REAL,sent INTEGER,first_attempt REAL,error TEXT,message_id TEXT);
            CREATE TABLE photo_deliveries(file_id TEXT PRIMARY KEY,recipient TEXT,lease REAL,sent INTEGER,first_attempt REAL,image_key TEXT);
        """)
        with connect(path) as db:
            db.execute("INSERT INTO deliveries VALUES(?,0,1,123,'','receipt')", ("task:0:" + OLD,))
            db.execute("INSERT INTO photo_deliveries VALUES('file-id',?,0,0,123,'old-image-key')", (OLD,))
        self.migrate()
        self.assertEqual(self.rows(path, "SELECT sent,message_id FROM deliveries WHERE id=?", ("task:0:" + OLD,)), [(1, "receipt")])
        self.assertGreater(self.rows(path, "SELECT lease FROM deliveries WHERE id=?", ("task:0:" + NEW,))[0][0], 9999999999)
        self.assertEqual(self.rows(path, "SELECT sent,message_id FROM deliveries WHERE id=?", ("task:0:" + NEW,)), [(0, "")])
        self.assertEqual(self.rows(path, "SELECT file_id FROM photo_deliveries WHERE sent=0 AND lease<=9999999999"), [])
        result = self.migrate(manifest=manifest((OLD, NEW), (SECOND, SECOND_NEW)))
        self.assertTrue(result["ready"])
        self.assertEqual(self.rows(path, "SELECT count(*) FROM deliveries"), [(2,)])

    def test_learning_keeps_person_ids_scores_and_changes_only_login_ownership(self):
        path = self.database("learning/learning.sqlite3", """
            CREATE TABLE documents(kind TEXT,key TEXT,payload TEXT,person_id TEXT,PRIMARY KEY(kind,key));
            CREATE TABLE learning_results(paper_id TEXT PRIMARY KEY,person_id TEXT,correct INTEGER);
        """)
        with connect(path) as db:
            db.execute("INSERT INTO documents VALUES('person','learner-stable',?,'learner-stable')",
                       (dumps({"id": "learner-stable", "login_ids": [OLD], "score": 99}),))
            db.execute("INSERT INTO learning_results VALUES('paper-id','learner-stable',1)")
        self.migrate()
        kind, key, raw, person_id = self.rows(path, "SELECT * FROM documents")[0]
        self.assertEqual((kind, key, person_id), ("person", "learner-stable", "learner-stable"))
        self.assertEqual(json.loads(raw)["login_ids"], [NEW])
        self.assertEqual(self.rows(path, "SELECT * FROM learning_results"), [("paper-id", "learner-stable", 1)])

    def test_pre_shift_people_and_item_recipients_have_explicit_shapes(self):
        path = self.database("pre_shift_checks/checks.sqlite3", """
            CREATE TABLE rosters(day TEXT PRIMARY KEY,people TEXT);
            CREATE TABLE tasks(id TEXT PRIMARY KEY,people TEXT,items TEXT,answers TEXT,evidence TEXT,actor_id TEXT);
        """)
        with connect(path) as db:
            db.execute("INSERT INTO rosters VALUES('day',?)", (dumps([{"open_id": OLD}]),))
            db.execute("INSERT INTO tasks VALUES('task',?,?,?,?,?)",
                       (dumps([{"open_id": OLD}]), dumps([{"id": "item-stable", "recipient_ids": [OLD]}]),
                        dumps({"item-stable": "literal " + OLD}), "", OLD))
        self.migrate()
        row = self.rows(path, "SELECT people,items,answers,evidence,actor_id FROM tasks")[0]
        self.assertEqual(json.loads(row[0])[0]["open_id"], NEW)
        self.assertEqual(json.loads(row[1])[0]["recipient_ids"], [NEW])
        self.assertEqual(json.loads(row[2]), {"item-stable": "literal " + OLD})
        self.assertEqual((row[3], row[4]), ("", NEW))

    def test_risk_account_person_ids_change_but_learning_ids_and_job_ids_do_not(self):
        path = self.main()
        with connect(path) as db:
            db.executescript("""
                CREATE TABLE risk_people(release_id TEXT,person_id TEXT,data TEXT,PRIMARY KEY(release_id,person_id));
                CREATE TABLE risk_files(id TEXT PRIMARY KEY,person_id TEXT);
                CREATE TABLE risk_jobs(id TEXT PRIMARY KEY,status TEXT,attempts INTEGER,payload TEXT);
            """)
            db.execute("INSERT INTO risk_people VALUES('release',?,?)", (OLD, dumps({"id": OLD, "open_id": OLD})))
            db.execute("INSERT INTO risk_files VALUES('file-stable',?)", (OLD,))
            db.execute("INSERT INTO risk_jobs VALUES('opaque-operation','queued',0,?)", (dumps({"person_id": OLD, "subject_id": OLD}),))
        self.migrate()
        person_id, raw = self.rows(path, "SELECT person_id,data FROM risk_people")[0]
        self.assertEqual((person_id, json.loads(raw)["id"]), (NEW, NEW))
        self.assertEqual(self.rows(path, "SELECT * FROM risk_files"), [("file-stable", NEW)])
        key, raw = self.rows(path, "SELECT id,payload FROM risk_jobs")[0]
        self.assertEqual(key, "opaque-operation")
        self.assertEqual(json.loads(raw), {"person_id": NEW, "subject_id": NEW})

    def test_existing_business_store_whitelists_and_excluded_inbox_backup(self):
        cabinet = self.database("cabinet_power/batches.sqlite3", "CREATE TABLE batches(batch_id TEXT PRIMARY KEY,owner_id TEXT,payload_json TEXT);",
                                [("INSERT INTO batches VALUES(?,?,?)", ("batch-id", OLD, dumps({"owner_id": OLD})))])
        knowledge = self.database("lighthouse_openclaw/knowledge_base/knowledge.sqlite3", "CREATE TABLE documents(id TEXT PRIMARY KEY,owner TEXT);",
                                  [("INSERT INTO documents VALUES(?,?)", ("document-id", OLD))])
        for name in ("feishu_assistant.sqlite3", "backups/lan_portal_state.sqlite3"):
            self.database(name, "CREATE TABLE auth_permissions(open_id TEXT PRIMARY KEY,payload_json TEXT);",
                          [("INSERT INTO auth_permissions VALUES(?,?)", (OLD, "{}"))])
        result = self.migrate()
        self.assertEqual(result["databases"], 2)
        self.assertEqual(self.rows(cabinet, "SELECT batch_id,owner_id FROM batches"), [("batch-id", NEW)])
        self.assertEqual(self.rows(knowledge, "SELECT id,owner FROM documents"), [("document-id", NEW)])
        for name in ("feishu_assistant.sqlite3", "backups/lan_portal_state.sqlite3"):
            self.assertEqual(self.rows(self.root / name, "SELECT open_id FROM auth_permissions"), [(OLD,)])

    def test_legacy_auth_without_sql_permissions_is_rewritten_with_original_backup(self):
        path = self.root / "lan_portal_auth.json"
        original = {"version": 1, "users": {OLD: {"enabled": False, "scopes": ["A"], "modules": ["assistant"]}}}
        path.write_text(dumps(original), encoding="utf-8")
        result = self.migrate()
        self.assertEqual(result["legacy_auth"], "rewritten")
        self.assertEqual(json.loads(path.read_text())["users"], {NEW: original["users"][OLD]})
        backup = next((self.root / "backups").rglob("lan_portal_auth.json.bak"))
        self.assertEqual(json.loads(backup.read_text()), original)
        self.assertEqual(self.migrate()["legacy_auth"], "rewritten")

    def test_legacy_auth_with_sql_permissions_is_archived_not_merged(self):
        path = self.main()
        self.permission(path, scopes=["A"])
        legacy = self.root / "lan_portal_auth.json"
        legacy.write_text(dumps({"users": {OLD: {"role": "admin", "scopes": ["ALL"]}}}), encoding="utf-8")
        result = self.migrate()
        self.assertEqual(result["legacy_auth"], "archived")
        self.assertFalse(legacy.exists())
        self.assertEqual(self.rows(path, "SELECT role,scopes_json FROM auth_permissions"), [("building", dumps(["A"]))])
        self.assertTrue(list((self.root / "backups").rglob("lan_portal_auth*.bak")))

    def test_legacy_write_failure_keeps_original_and_resumes_after_fix(self):
        legacy = self.root / "lan_portal_auth.json"
        original = dumps({"users": {OLD: {"scopes": ["A"]}}})
        legacy.write_text(original, encoding="utf-8")
        atomic = migration._atomic_file
        def fail(path, content):
            if path == legacy:
                raise OSError("fixture-secret-must-not-leak")
            return atomic(path, content)
        with patch.object(migration, "_atomic_file", side_effect=fail), self.assertRaises(migration.IdentityMigrationError) as caught:
            self.migrate()
        self.assertNotIn("fixture-secret", str(caught.exception) + dumps(caught.exception.report))
        self.assertEqual(legacy.read_text(), original)
        self.assertTrue(self.migrate()["ready"])

    def test_cross_process_lock_excludes_another_startup(self):
        script = """
import json,sys
sys.path.insert(0,sys.argv[1])
from lan_bitable_template_portal import feishu_identity_migration as migration
migration._LOCK_SECONDS=.15
try:
    migration.migrate_identity_databases(sys.argv[2],manifest=json.loads(sys.argv[3]))
except migration.IdentityMigrationError as exc:
    print(exc.code)
else:
    raise SystemExit('startup entered locked migration')
"""
        with migration._process_lock(self.root):
            result = subprocess.run([sys.executable, "-B", "-c", script, str(Path(__file__).resolve().parent),
                                     str(self.root), dumps(self.profile)], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "migration_lock_timeout")


if __name__ == "__main__":
    unittest.main()

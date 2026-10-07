"""Isolated tests for bin.openclaw_service.store.AssistantStore.

All fixtures live in temporary directories; no production/cloud data is touched,
no packages are installed and no services are launched.  Migration is exercised
against freshly-built synthetic LanPortalStateStore databases.
"""

from __future__ import annotations

from contextlib import closing
import hashlib
import json
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from openclaw_service.store import (
    ASSISTANT_NAMESPACES,
    MIGRATION_VERSION,
    AssistantStore,
)

BUSINESS_NAMESPACES = (
    "cabinet_power",
    "plan_convergence",
    "notice_memory",
    "polling_work_order",
    "signature_management_person",
    "daily_work_report",
)


def _file_sha(path):
    with open(path, "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest()


def _make_legacy(root, *, include_business=True, file_bytes=None):
    """Build a synthetic LanPortalStateStore with json_documents + attachment bytes."""
    legacy = root / "portal"
    legacy_db = legacy / "lan_portal_state.sqlite3"
    files_root = legacy / "lighthouse_assistant" / "files"
    account_dir = files_root / "owneracc"
    account_dir.mkdir(parents=True, exist_ok=True)

    file_id = "f" + ("0" * 31)
    file_bytes = file_bytes if file_bytes is not None else b"legacy-attachment-bytes"
    attachment = account_dir / (file_id + ".txt")
    attachment.write_bytes(file_bytes)
    file_sha = hashlib.sha256(file_bytes).hexdigest()

    conn = sqlite3.connect(str(legacy_db))
    conn.execute(
        """
        CREATE TABLE json_documents (
            namespace TEXT NOT NULL,
            key TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            updated_at REAL NOT NULL,
            PRIMARY KEY(namespace, key)
        )
        """
    )
    rows = [
        (
            "lighthouse_ai",
            "model",
            json.dumps(
                {
                    "enabled": True,
                    "active_model_id": "default",
                    "models": [{"id": "default", "key_cipher": "AQAA-CIPHERTEXT-PRESERVED"}],
                }
            ),
            1010.0,
        ),
        (
            "lighthouse_model_capabilities",
            "cap-profile",
            json.dumps({"model": "WanWu/Deepseek-Auto", "vision": True, "tools": True}),
            1011.0,
        ),
        (
            "lighthouse_runs",
            "run-abc",
            json.dumps(
                {
                    "id": "run-abc",
                    "operation_id": "op-turn-42",
                    "status": "completed",
                    "answer": "done",
                    "finished_at": 1012.0,
                }
            ),
            1012.0,
        ),
        (
            "lighthouse_messages",
            "conversation:op-turn-42",
            json.dumps(
                {
                    "operation_id": "op-turn-42",
                    "run_id": "run-abc",
                    "status": "completed",
                    "answer": "final-answer",
                    "plan": {"id": "plan-x", "status": "completed"},
                }
            ),
            1013.0,
        ),
        (
            "lighthouse_agent_plans",
            "plan-x",
            json.dumps(
                {
                    "id": "plan-x",
                    "owner": "owneracc",
                    "status": "submitted",
                    "job_id": "job-77",
                    "result_ids": ["result-1", "result-2"],
                    "turn_id": "op-turn-42",
                }
            ),
            1014.0,
        ),
        (
            "lighthouse_files",
            file_id,
            json.dumps(
                {
                    "id": file_id,
                    "owner": "owneracc",
                    "name": "legacy.txt",
                    "size": len(file_bytes),
                    "path": str(attachment),
                    "sha256": file_sha,
                    "created_at": 1015.0,
                }
            ),
            1015.0,
        ),
        (
            "lighthouse_appearance",
            "owneracc",
            json.dumps({"color": "#123456", "expression": "happy"}),
            1016.0,
        ),
        (
            "lighthouse_question_text",
            "cached-material-digest",
            json.dumps({"text": "retained derived material text"}),
            1017.0,
        ),
    ]
    if include_business:
        rows.extend(
            [
                ("cabinet_power", "biz-key", json.dumps({"secret": "business-secret"}), 2000.0),
                ("plan_convergence", "pnc-key", json.dumps({"x": 1}), 2001.0),
                ("notice_memory", "mem", json.dumps({"note": "n"}), 2002.0),
            ]
        )
    for r in rows:
        conn.execute(
            "INSERT INTO json_documents(namespace, key, payload_json, updated_at) VALUES (?, ?, ?, ?)",
            r,
        )
    conn.commit()
    conn.close()
    return legacy_db, files_root, file_id, file_bytes, attachment


class AssistantStoreCRUDTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.store = AssistantStore(self.root)

    def tearDown(self):
        self._tmp.cleanup()

    def test_roundtrip_document(self):
        self.store.put_document("lighthouse_ai", "k1", {"a": 1})
        self.assertEqual(self.store.get_document("lighthouse_ai", "k1"), {"a": 1})
        self.store.put_document("lighthouse_ai", "k1", {"a": 2})
        self.assertEqual(self.store.get_document("lighthouse_ai", "k1"), {"a": 2})
        self.store.delete_document("lighthouse_ai", "k1")
        self.assertIsNone(self.store.get_document("lighthouse_ai", "k1"))

    def test_put_documents_is_atomic(self):
        with self.assertRaises(ValueError):
            self.store.put_documents(
                "lighthouse_ai",
                {"k1": {"ok": True}, "k2": ["not-an-object"]},
            )
        self.assertIsNone(self.store.get_document("lighthouse_ai", "k1"))
        self.assertIsNone(self.store.get_document("lighthouse_ai", "k2"))
        self.assertEqual(self.store.list_documents("lighthouse_ai"), [])

    def test_business_namespace_rejected(self):
        for ns in BUSINESS_NAMESPACES:
            with self.assertRaises(ValueError):
                self.store.get_document(ns, "x")
            with self.assertRaises(ValueError):
                self.store.put_document(ns, "x", {})
            with self.assertRaises(ValueError):
                self.store.put_documents(ns, {"x": {}})
            with self.assertRaises(ValueError):
                self.store.delete_document(ns, "x")
            with self.assertRaises(ValueError):
                self.store.list_documents(ns)

    def test_list_literal_key_prefix_matching(self):
        self.store.put_documents(
            "lighthouse_runs",
            {
                "job_100": {"v": 1},
                "job_101": {"v": 2},
                "job%evil": {"v": 3},
                "job_literal": {"v": 4},
                "other": {"v": 5},
            },
        )
        keys = [d["key"] for d in self.store.list_documents("lighthouse_runs", key_prefix="job_")]
        self.assertEqual(keys, ["job_100", "job_101", "job_literal"])
        self.assertNotIn("job%evil", keys)
        keys_literal = [d["key"] for d in self.store.list_documents("lighthouse_runs", key_prefix="job%")]
        self.assertEqual(keys_literal, ["job%evil"])
        # underscore must be literal, not a wildcard
        keys_ud = [d["key"] for d in self.store.list_documents("lighthouse_runs", key_prefix="job_1")]
        self.assertEqual(keys_ud, ["job_100", "job_101"])

    def test_corrupt_documents_are_not_reported_as_missing_or_skipped(self):
        for payload in ('{broken', '[1]', '{"value":NaN}'):
            with self.subTest(payload=payload):
                with closing(sqlite3.connect(self.store.db_path)) as connection:
                    connection.execute("INSERT OR REPLACE INTO documents VALUES (?,?,?,?)",
                        ('lighthouse_ai', 'corrupt', payload, time.time()))
                    connection.commit()
                with self.assertRaises(ValueError):
                    self.store.get_document('lighthouse_ai', 'corrupt')
                with self.assertRaises(ValueError):
                    self.store.list_documents('lighthouse_ai')
                with closing(sqlite3.connect(self.store.db_path)) as connection:
                    saved = connection.execute("SELECT payload_json FROM documents WHERE key='corrupt'").fetchone()[0]
                self.assertEqual(saved, payload)

    def test_key_prefix_case_sensitive(self):
        self.store.put_documents(
            "lighthouse_runs",
            {
                "Abc-op": {"v": 1},
                "abc-op": {"v": 2},
                "ABC-op": {"v": 3},
                "aBc%x": {"v": 4},
                "aBc_x": {"v": 5},
                "aBc\\y": {"v": 6},
            },
        )
        self.assertEqual(
            {d["key"] for d in self.store.list_documents("lighthouse_runs", key_prefix="aBc")},
            {"aBc%x", "aBc_x", "aBc\\y"},
        )
        self.assertEqual(
            {d["key"] for d in self.store.list_documents("lighthouse_runs", key_prefix="abc")},
            {"abc-op"},
        )
        self.assertEqual(
            {d["key"] for d in self.store.list_documents("lighthouse_runs", key_prefix="Abc")},
            {"Abc-op"},
        )
        # Prefix itself is matched case-sensitively (a vs A).
        lower_a = {d["key"] for d in self.store.list_documents("lighthouse_runs", key_prefix="a")}
        self.assertIn("abc-op", lower_a)
        self.assertNotIn("Abc-op", lower_a)
        self.assertNotIn("ABC-op", lower_a)
        upper_a = {d["key"] for d in self.store.list_documents("lighthouse_runs", key_prefix="A")}
        self.assertIn("Abc-op", upper_a)
        self.assertIn("ABC-op", upper_a)
        self.assertNotIn("abc-op", upper_a)

    def test_non_dict_payload_rejected(self):
        with self.assertRaises(ValueError):
            self.store.put_document("lighthouse_ai", "k", ["not", "a", "dict"])
        with self.assertRaises(ValueError):
            self.store.put_document("lighthouse_ai", "k", "scalar")


class AssistantStoreMigrationTests(unittest.TestCase):
    def test_joint_portable_relocation_retains_state_and_owned_files_without_remigration(self):
        from openclaw_service.assistant.lighthouse_files import LighthouseFiles
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            original, copied = base / 'original', base / 'copied'
            original.mkdir()
            legacy_db, legacy_files, file_id, content, _ = _make_legacy(original)
            service = AssistantStore(original / 'service', legacy_db=legacy_db, legacy_files=legacy_files)
            service.put_document('lighthouse_ai', 'conversation:owneracc', {'id': 'retained-id', 'turns': [{'answer': 'retained'}]})
            shutil.copytree(original, copied)
            moved = AssistantStore(copied / 'service', legacy_db=copied / 'portal/lan_portal_state.sqlite3',
                legacy_files=copied / 'portal/lighthouse_assistant/files')
            self.assertEqual(moved.get_document('lighthouse_ai', 'conversation:owneracc')['id'], 'retained-id')
            item = LighthouseFiles(moved, root=moved.files_root).get({'id': 'owneracc', 'scopes': ['D']}, file_id)
            self.assertTrue(Path(item['path']).is_relative_to(copied))
            self.assertEqual(Path(item['path']).read_bytes(), content)
            self.assertEqual(len(list((copied / 'service/backups').iterdir())), 1)

    def _fresh(self, **legacy_kwargs):
        tmp = tempfile.TemporaryDirectory()
        root = Path(tmp.name)
        legacy_db, files_root, file_id, file_bytes, attachment = _make_legacy(
            root, **legacy_kwargs
        )
        return tmp, root, legacy_db, files_root, file_id, file_bytes, attachment

    def test_migration_preserves_ciphertext_ids_and_no_business(self):
        tmp, root, legacy_db, files_root, file_id, file_bytes, attachment = self._fresh()
        try:
            store = AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
            model = store.get_document("lighthouse_ai", "model")
            self.assertEqual(model["models"][0]["key_cipher"], "AQAA-CIPHERTEXT-PRESERVED")
            run = store.get_document("lighthouse_runs", "run-abc")
            self.assertEqual(run["operation_id"], "op-turn-42")
            msg = store.get_document("lighthouse_messages", "conversation:op-turn-42")
            self.assertEqual(msg["operation_id"], "op-turn-42")
            plan = store.get_document("lighthouse_agent_plans", "plan-x")
            self.assertEqual(plan["status"], "submitted")
            self.assertEqual(plan["job_id"], "job-77")
            self.assertEqual(plan["result_ids"], ["result-1", "result-2"])
            # completed result preserved unchanged
            self.assertEqual(run["status"], "completed")
            self.assertEqual(run["answer"], "done")
            self.assertEqual(store.get_document('lighthouse_question_text', 'cached-material-digest'),
                {'text': 'retained derived material text'})
            # no business records copied into the assistant DB
            conn = sqlite3.connect(str(store.db_path))
            present = {
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT namespace FROM documents"
                ).fetchall()
            }
            conn.close()
            self.assertFalse(present & set(BUSINESS_NAMESPACES))
            self.assertTrue(present.issubset(ASSISTANT_NAMESPACES))

            namespaces = set()
            for ns in ASSISTANT_NAMESPACES:
                for doc in store.list_documents(ns):
                    namespaces.add(ns)
            self.assertEqual(namespaces, ASSISTANT_NAMESPACES - {'lighthouse_installed_skills'})
            self.assertEqual(store.list_documents('lighthouse_installed_skills'), [])
        finally:
            tmp.cleanup()

    def test_repeated_migration_does_not_overwrite_edits(self):
        tmp, root, legacy_db, files_root, file_id, file_bytes, attachment = self._fresh()
        try:
            store = AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
            store.put_document(
                "lighthouse_ai", "model", {"models": [{"key_cipher": "EDITED-BY-USER"}]}
            )
            store.put_document("lighthouse_agent_plans", "plan-x", {"edited": True})
            # Reopen (as on a later startup) with the same legacy source.
            reopened = AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
            self.assertEqual(
                reopened.get_document("lighthouse_ai", "model")["models"][0]["key_cipher"],
                "EDITED-BY-USER",
            )
            self.assertEqual(reopened.get_document("lighthouse_agent_plans", "plan-x"), {"edited": True})
        finally:
            tmp.cleanup()

    def test_migration_refuses_newer_or_foreign_service_data(self):
        tmp, root, legacy_db, files_root, file_id, file_bytes, attachment = self._fresh()
        try:
            store = AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
            # A different legacy source must not clobber already-migrated data.
            other_db = root / "other_portal.sqlite3"
            conn = sqlite3.connect(str(other_db))
            conn.execute(
                "CREATE TABLE json_documents (namespace TEXT, key TEXT, payload_json TEXT, updated_at REAL, PRIMARY KEY(namespace,key))"
            )
            conn.execute(
                "INSERT INTO json_documents VALUES (?, ?, ?, ?)",
                ("lighthouse_ai", "model", json.dumps({"foreign": True}), 1.0),
            )
            conn.commit()
            conn.close()
            with self.assertRaises(ValueError):
                AssistantStore(root, legacy_db=other_db, legacy_files=files_root)
        finally:
            tmp.cleanup()

    def test_missing_file_aborts_migration_with_rollback(self):
        tmp, root, legacy_db, files_root, file_id, file_bytes, attachment = self._fresh()
        try:
            # Break the attachment after creating the fixture.
            missing = files_root / "owneracc" / "missing.txt"
            conn = sqlite3.connect(str(legacy_db))
            conn.execute(
                "INSERT INTO json_documents VALUES (?, ?, ?, ?)",
                (
                    "lighthouse_files",
                    "missing-file",
                    json.dumps({"id": "missing-file", "path": str(missing), "sha256": "x" * 64}),
                    1.0,
                ),
            )
            conn.commit()
            conn.close()
            with self.assertRaises(ValueError):
                AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
            # Rollback: no assistant data committed, no files copied, source untouched.
            probe = AssistantStore(root)
            self.assertIsNone(probe.get_document("lighthouse_files", file_id))
            self.assertEqual(probe.list_documents("lighthouse_files"), [])
            self.assertFalse(list((root / "files").rglob("*")))
        finally:
            tmp.cleanup()

    def test_hash_mismatch_aborts_migration(self):
        tmp, root, legacy_db, files_root, file_id, file_bytes, attachment = self._fresh()
        try:
            conn = sqlite3.connect(str(legacy_db))
            conn.execute(
                "UPDATE json_documents SET payload_json=? WHERE namespace='lighthouse_files' AND key=?",
                (
                    json.dumps(
                        {
                            "id": file_id,
                            "path": str(attachment),
                            "sha256": "0" * 64,
                        }
                    ),
                    file_id,
                ),
            )
            conn.commit()
            conn.close()
            with self.assertRaises(ValueError):
                AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
        finally:
            tmp.cleanup()

    def test_path_escape_aborts_migration(self):
        tmp, root, legacy_db, files_root, file_id, file_bytes, attachment = self._fresh()
        try:
            outside = root / "outside_secret.bin"
            outside.write_bytes(b"do-not-copy")
            conn = sqlite3.connect(str(legacy_db))
            conn.execute(
                "INSERT INTO json_documents VALUES (?, ?, ?, ?)",
                (
                    "lighthouse_files",
                    "escaped-file",
                    json.dumps(
                        {"id": "escaped-file", "path": str(outside), "sha256": hashlib.sha256(b"do-not-copy").hexdigest()}
                    ),
                    1.0,
                ),
            )
            conn.commit()
            conn.close()
            with self.assertRaises(ValueError):
                AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
            # The outside file must never be copied.
            self.assertFalse(list((root / "files").rglob("*")))
        finally:
            tmp.cleanup()

    def test_symlink_escape_aborts_migration(self):
        tmp, root, legacy_db, files_root, file_id, file_bytes, attachment = self._fresh()
        try:
            outside = root / "outside_target.bin"
            outside.write_bytes(b"symlink-target")
            link = files_root / "owneracc" / "evil_link.txt"
            try:
                link.symlink_to(outside)
            except (OSError, NotImplementedError):
                self.skipTest("symlinks not available on this platform")
            conn = sqlite3.connect(str(legacy_db))
            conn.execute(
                "INSERT INTO json_documents VALUES (?, ?, ?, ?)",
                (
                    "lighthouse_files",
                    "symlink-file",
                    json.dumps(
                        {"id": "symlink-file", "path": str(link), "sha256": hashlib.sha256(b"symlink-target").hexdigest()}
                    ),
                    1.0,
                ),
            )
            conn.commit()
            conn.close()
            with self.assertRaises(ValueError):
                AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
        finally:
            tmp.cleanup()

    def test_file_preservation_and_sha(self):
        tmp, root, legacy_db, files_root, file_id, file_bytes, attachment = self._fresh()
        try:
            store = AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
            doc = store.get_document("lighthouse_files", file_id)
            new_path = Path(doc["path"])
            self.assertTrue(new_path.is_relative_to(root / "files"))
            rel = new_path.relative_to(root / "files")
            expected_rel = attachment.relative_to(files_root)
            self.assertEqual(rel, expected_rel)
            self.assertEqual(new_path.read_bytes(), file_bytes)
            self.assertEqual(doc["sha256"], hashlib.sha256(file_bytes).hexdigest())
        finally:
            tmp.cleanup()

    def test_invalid_json_aborts_migration(self):
        tmp, root, legacy_db, files_root, file_id, file_bytes, attachment = self._fresh()
        try:
            conn = sqlite3.connect(str(legacy_db))
            conn.execute(
                "INSERT INTO json_documents VALUES (?, ?, ?, ?)",
                ("lighthouse_ai", "bad", "this is not json", 1.0),
            )
            conn.commit()
            conn.close()
            with self.assertRaises(ValueError):
                AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
        finally:
            tmp.cleanup()

    def test_backup_contains_only_assistant_rows_and_files(self):
        tmp, root, legacy_db, files_root, file_id, file_bytes, attachment = self._fresh()
        try:
            AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
            backup_root = root / "backups"
            self.assertTrue(backup_root.exists())
            doc_files = list(backup_root.rglob("assistant_documents.jsonl"))
            self.assertEqual(len(doc_files), 1)
            lines = doc_files[0].read_text(encoding="utf-8").strip().splitlines()
            self.assertGreaterEqual(len(lines), 7)
            for line in lines:
                obj = json.loads(line)
                self.assertIn(obj["namespace"], ASSISTANT_NAMESPACES)
            backed_files = list((backup_root / doc_files[0].parent.name / "files").rglob("*"))
            self.assertTrue(any(p.is_file() for p in backed_files))
        finally:
            tmp.cleanup()

    def test_business_row_changed_after_migration_then_reopen_skips(self):
        tmp, root, legacy_db, files_root, file_id, file_bytes, attachment = self._fresh()
        try:
            store = AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
            store.put_document("lighthouse_ai", "model", {"models": [{"key_cipher": "EDITED"}]})
            # Change an unrelated business row (this alters the legacy file's stat).
            conn = sqlite3.connect(str(legacy_db))
            conn.execute(
                "UPDATE json_documents SET payload_json=? WHERE namespace='cabinet_power' AND key='biz-key'",
                (json.dumps({"updated": "yes", "secret": "s3"}),),
            )
            conn.commit()
            conn.close()
            # Reopen with the same canonical source: completed migration must be skipped.
            reopened = AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
            self.assertEqual(
                reopened.get_document("lighthouse_ai", "model")["models"][0]["key_cipher"],
                "EDITED",
            )
            # The business update remains in the (untouched) legacy DB.
            conn = sqlite3.connect(str(legacy_db))
            row = conn.execute(
                "SELECT payload_json FROM json_documents WHERE namespace='cabinet_power' AND key='biz-key'"
            ).fetchone()
            conn.close()
            self.assertEqual(json.loads(row[0])["updated"], "yes")
        finally:
            tmp.cleanup()

    def test_old_db_and_files_removed_after_migration_then_reopen_skips(self):
        tmp, root, legacy_db, files_root, file_id, file_bytes, attachment = self._fresh()
        try:
            import shutil

            store = AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
            store.put_document("lighthouse_appearance", "owneracc", {"color": "#000000"})
            self.assertEqual(
                store.get_document("lighthouse_appearance", "owneracc"), {"color": "#000000"}
            )
            # Remove the old DB and the legacy files tree entirely.
            legacy_db.unlink()
            shutil.rmtree(files_root)
            # Reopen: completed migration skips BEFORE requiring source existence.
            reopened = AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
            self.assertEqual(
                reopened.get_document("lighthouse_appearance", "owneracc"), {"color": "#000000"}
            )
            self.assertIs(reopened._read_marker().get("completed"), True)
        finally:
            tmp.cleanup()

    def test_existing_identical_destination_accepted_and_not_overwritten(self):
        tmp, root, legacy_db, files_root, file_id, file_bytes, attachment = self._fresh()
        try:
            rel = attachment.relative_to(files_root)
            target = root / "files" / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(file_bytes)  # identical existing file
            store = AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
            doc = store.get_document("lighthouse_files", file_id)
            self.assertEqual(Path(doc["path"]), target)
            self.assertEqual(target.read_bytes(), file_bytes)
        finally:
            tmp.cleanup()

    def test_existing_differing_destination_aborts_and_preserves(self):
        tmp, root, legacy_db, files_root, file_id, file_bytes, attachment = self._fresh()
        try:
            rel = attachment.relative_to(files_root)
            target = root / "files" / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"USER-OWNED-CONTENT")  # differs from legacy bytes
            with self.assertRaises(ValueError):
                AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
            # Pre-existing destination file is preserved byte-for-byte.
            self.assertEqual(target.read_bytes(), b"USER-OWNED-CONTENT")
            probe = AssistantStore(root)
            self.assertEqual(probe.list_documents("lighthouse_files"), [])
        finally:
            tmp.cleanup()

    def test_inner_symlink_component_rejected_even_in_root(self):
        tmp, root, legacy_db, files_root, file_id, file_bytes, attachment = self._fresh()
        try:
            # Directory symlink inside files root that resolves to an in-root dir.
            link_dir = files_root / "inner_link"
            try:
                link_dir.symlink_to(files_root / "owneracc", target_is_directory=True)
            except (OSError, NotImplementedError, TypeError):
                self.skipTest("symlinks not available on this platform")
            payload_path = link_dir / (file_id + ".txt")
            self.assertTrue(payload_path.resolve().is_relative_to(files_root))
            conn = sqlite3.connect(str(legacy_db))
            conn.execute(
                "INSERT INTO json_documents VALUES (?, ?, ?, ?)",
                (
                    "lighthouse_files",
                    "inroot-symlink",
                    json.dumps({"id": "inroot-symlink", "path": str(payload_path), "sha256": "0" * 64}),
                    1.0,
                ),
            )
            conn.commit()
            conn.close()
            with self.assertRaises(ValueError):
                AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
        finally:
            tmp.cleanup()

    def test_nonfinite_json_aborts_migration(self):
        tmp, root, legacy_db, files_root, file_id, file_bytes, attachment = self._fresh()
        try:
            conn = sqlite3.connect(str(legacy_db))
            conn.execute(
                "INSERT INTO json_documents VALUES (?, ?, ?, ?)",
                ("lighthouse_ai", "bad-nan", json.dumps({"x": float("nan")}), 1.0),
            )
            conn.commit()
            conn.close()
            with self.assertRaises(ValueError):
                AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
        finally:
            tmp.cleanup()

    def test_migration_interrupted_flag_on_active_runs_only(self):
        tmp, root, legacy_db, files_root, file_id, file_bytes, attachment = self._fresh()
        try:
            conn = sqlite3.connect(str(legacy_db))
            conn.execute(
                "INSERT INTO json_documents VALUES (?, ?, ?, ?)",
                (
                    "lighthouse_runs",
                    "run-active",
                    json.dumps(
                        {
                            "id": "run-active",
                            "operation_id": "op-active-9",
                            "status": "running",
                            "started_at": 2000.0,
                        }
                    ),
                    2000.0,
                ),
            )
            conn.commit()
            conn.close()
            store = AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
            active = store.get_document("lighthouse_runs", "run-active")
            self.assertIs(active["migration_interrupted"], True)
            self.assertEqual(active["operation_id"], "op-active-9")
            done = store.get_document("lighthouse_runs", "run-abc")
            self.assertEqual(done["status"], "completed")
            self.assertNotIn("migration_interrupted", done)
            # Plans are preserved verbatim (job/result ids intact, never replayed).
            plan = store.get_document("lighthouse_agent_plans", "plan-x")
            self.assertEqual(plan["job_id"], "job-77")
            self.assertEqual(plan["result_ids"], ["result-1", "result-2"])
            self.assertNotIn("migration_interrupted", plan)
        finally:
            tmp.cleanup()

    def test_error_messages_do_not_leak_paths_or_payload(self):
        tmp, root, legacy_db, files_root, file_id, file_bytes, attachment = self._fresh()
        try:
            secret = "SUPERSECRET-CREDENTIAL"
            outside = root / "outside_leak.bin"
            outside.write_bytes(b"x")
            conn = sqlite3.connect(str(legacy_db))
            conn.execute(
                "INSERT INTO json_documents VALUES (?, ?, ?, ?)",
                (
                    "lighthouse_files",
                    "leaky-key",
                    json.dumps(
                        {
                            "id": "leaky-id",
                            "path": str(outside),
                            "sha256": "0" * 64,
                            "nested_secret": secret,
                        }
                    ),
                    1.0,
                ),
            )
            conn.commit()
            conn.close()
            try:
                AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
                self.fail("expected migration to fail")
            except ValueError as exc:
                msg = str(exc)
            self.assertNotIn(secret, msg)
            self.assertNotIn("outside_leak.bin", msg)
            self.assertNotIn("leaky-key", msg)
            self.assertNotIn("leaky-id", msg)
            self.assertNotIn(str(outside), msg)
        finally:
            tmp.cleanup()


class AssistantStoreConcurrencyTests(unittest.TestCase):
    def test_concurrent_use_restart_and_no_source_changes(self):
        tmp = tempfile.TemporaryDirectory()
        root = Path(tmp.name)
        legacy_db, files_root, file_id, file_bytes, attachment = _make_legacy(root)
        try:
            before_db = _file_sha(legacy_db)
            before_file = _file_sha(attachment)

            store = AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)

            errors = []
            barrier = threading.Barrier(4)

            def worker(name):
                try:
                    barrier.wait()
                    local = AssistantStore(root)
                    for i in range(20):
                        key = f"thread-{name}-{i}"
                        local.put_document("lighthouse_runs", key, {"worker": name, "i": i})
                        local.get_document("lighthouse_runs", key)
                    local.list_documents("lighthouse_runs", key_prefix="thread-")
                except Exception as exc:  # pragma: no cover - diagnostic
                    errors.append(exc)

            threads = [threading.Thread(target=worker, args=(str(n),)) for n in range(4)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(30)
            self.assertFalse(errors, errors)

            # Restart: a new instance sees all committed data and migration is skipped.
            restarted = AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
            runs = restarted.list_documents("lighthouse_runs", key_prefix="thread-")
            self.assertEqual(len(runs), 80)
            self.assertEqual(len({d["key"] for d in runs}), 80)

            # Source DB and legacy attachment bytes remain unchanged.
            self.assertEqual(_file_sha(legacy_db), before_db)
            self.assertEqual(_file_sha(attachment), before_file)
        finally:
            tmp.cleanup()

    def test_assistant_store_busy_timeout_is_bounded_one_second(self):
        tmp = tempfile.TemporaryDirectory()
        root = Path(tmp.name)
        try:
            store = AssistantStore(root)
            locker = sqlite3.connect(str(store.db_path))
            locker.execute("BEGIN IMMEDIATE")
            try:
                start = time.monotonic()
                with self.assertRaises(sqlite3.OperationalError):
                    store.put_document("lighthouse_ai", "k", {"v": 1})
                elapsed = time.monotonic() - start
                self.assertLess(elapsed, 2.5)
            finally:
                locker.rollback()
                locker.close()
        finally:
            tmp.cleanup()

    def test_source_db_lock_times_out_boundedly(self):
        tmp = tempfile.TemporaryDirectory()
        root = Path(tmp.name)
        legacy_db, files_root, file_id, file_bytes, attachment = _make_legacy(root)
        try:
            store = AssistantStore(root)
            store.legacy_db, store.legacy_files = str(legacy_db), str(files_root)
            from openclaw_service.store import _BUSY_TIMEOUT_SECONDS
            self.assertEqual(_BUSY_TIMEOUT_SECONDS, 1.0)
            locker = sqlite3.connect(str(legacy_db))
            locker.execute("PRAGMA locking_mode=EXCLUSIVE")
            locker.execute("BEGIN EXCLUSIVE")
            try:
                start = time.monotonic()
                with self.assertRaises(sqlite3.OperationalError):
                    store._migrate()
                elapsed = time.monotonic() - start
                # Windows scheduling can exceed SQLite's 1s busy budget under
                # the full suite. Keep a wall-clock guard, not a tight benchmark.
                self.assertLess(elapsed, 5.0)
            finally:
                locker.rollback()
                locker.close()
            # After the lock is released the interrupted migration resumes cleanly.
            store = AssistantStore(root, legacy_db=str(legacy_db), legacy_files=str(files_root))
            self.assertIsNotNone(store.get_document("lighthouse_ai", "model"))
            self.assertIs(store._read_marker().get("completed"), True)
        finally:
            tmp.cleanup()


class AssistantStoreInterruptedTests(unittest.TestCase):
    def test_deleted_missing_attachment_preserves_tombstone_without_blocking_migration(self):
        from openclaw_service.assistant.lighthouse_files import LighthouseFiles
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy_db, files_root, file_id, file_bytes, attachment = _make_legacy(root)
            with closing(sqlite3.connect(legacy_db)) as connection:
                raw = connection.execute(
                    "SELECT payload_json FROM json_documents WHERE namespace='lighthouse_files' AND key=?",
                    (file_id,),
                ).fetchone()[0]
                payload = json.loads(raw)
                payload["deleted_at"] = 12345
                connection.execute(
                    "UPDATE json_documents SET payload_json=? WHERE namespace='lighthouse_files' AND key=?",
                    (json.dumps(payload), file_id),
                )
                connection.commit()
            attachment.unlink()
            before = legacy_db.read_bytes()
            store = AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
            self.assertEqual(store.get_document("lighthouse_files", file_id), payload)
            self.assertTrue(store._read_marker()["completed"])
            self.assertEqual(legacy_db.read_bytes(), before)
            self.assertEqual(list(store.files_root.rglob('*.txt')), [])
            files = LighthouseFiles(store, root=store.files_root)
            with self.assertRaises(Exception) as failure:
                files.get({"id": "owneracc", "scopes": ["A"]}, file_id)
            self.assertEqual(getattr(failure.exception, "status", None), 404)
            resumed = AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
            self.assertEqual(resumed.get_document("lighthouse_files", file_id), payload)

    def test_deleted_existing_attachment_is_not_resurrected_or_removed_from_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy_db, files_root, file_id, file_bytes, attachment = _make_legacy(root)
            with closing(sqlite3.connect(legacy_db)) as connection:
                connection.execute(
                    "UPDATE json_documents SET payload_json=json_set(payload_json,'$.deleted_at',12345) "
                    "WHERE namespace='lighthouse_files' AND key=?", (file_id,),
                )
                connection.commit()
            store = AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
            self.assertEqual(attachment.read_bytes(), file_bytes)
            self.assertEqual(store.get_document("lighthouse_files", file_id)["deleted_at"], 12345)
            self.assertEqual(list(store.files_root.rglob('*.txt')), [])

    def test_interrupted_marker_recorded_without_fabricating_success(self):
        tmp = tempfile.TemporaryDirectory()
        root = Path(tmp.name)
        legacy_db, files_root, file_id, file_bytes, attachment = _make_legacy(root)
        try:
            # First attempt fails: missing attachment.
            missing = files_root / "owneracc" / "gone.txt"
            conn = sqlite3.connect(str(legacy_db))
            conn.execute(
                "INSERT INTO json_documents VALUES (?, ?, ?, ?)",
                (
                    "lighthouse_files",
                    "gone-file",
                    json.dumps({"id": "gone-file", "path": str(missing)}),
                    1.0,
                ),
            )
            conn.commit()
            conn.close()
            with self.assertRaises(ValueError):
                AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)

            # Marker records interrupted=True, completed=False (no fabricated success).
            probe = AssistantStore(root)
            marker = probe._read_marker()
            self.assertIsNotNone(marker)
            self.assertIs(marker.get("completed"), False)
            self.assertIs(marker.get("interrupted"), True)
            self.assertEqual(marker.get("version"), MIGRATION_VERSION)
            self.assertEqual(probe.list_documents("lighthouse_files"), [])

            # Fix the source, then a later start resumes and completes.
            (files_root / "owneracc").mkdir(parents=True, exist_ok=True)
            gone = files_root / "owneracc" / "gone.txt"
            gone.write_bytes(b"now-fixed")
            conn = sqlite3.connect(str(legacy_db))
            conn.execute(
                "UPDATE json_documents SET payload_json=? WHERE namespace='lighthouse_files' AND key='gone-file'",
                (
                    json.dumps(
                        {
                            "id": "gone-file",
                            "path": str(gone),
                            "sha256": hashlib.sha256(b"now-fixed").hexdigest(),
                        }
                    ),
                ),
            )
            conn.commit()
            conn.close()

            resumed = AssistantStore(root, legacy_db=legacy_db, legacy_files=files_root)
            resumed_marker = resumed._read_marker()
            self.assertIs(resumed_marker.get("completed"), True)
            self.assertIs(resumed_marker.get("interrupted"), False)
            migrated_gone = resumed.get_document("lighthouse_files", "gone-file")
            self.assertTrue(
                Path(migrated_gone["path"]).is_relative_to(root / "files")
            )
            self.assertEqual(
                Path(migrated_gone["path"]).read_bytes(), b"now-fixed"
            )
        finally:
            tmp.cleanup()


if __name__ == "__main__":
    unittest.main()

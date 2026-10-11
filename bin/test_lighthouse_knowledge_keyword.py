"""Offline checks for full-text indexing, in-memory FAISS vectors and shared-file safety.

The knowledge base runs in the fixed local FAISS mode (BGE small-zh + FAISS in memory)
with an injected deterministic embedder and a real FAISS index.  These tests exercise
the interplay between persistent full-text and the in-memory vector index, plus all
security/shared-file guarantees.
"""
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_knowledge import (
    DIMENSIONS,
    MODE,
    MODEL_NAME,
    KnowledgeBase,
)
from bin.test_lighthouse_knowledge import FakeEmbedder, FakeMemoryIndex, actor, _protect


def _text(content, name="说明.txt"):
    return name, content.encode("utf-8")


class LocalTestCase(unittest.TestCase):
    """Base with a fresh temporary KnowledgeBase; default mode must be local_faiss."""

    def setUp(self):
        self.embedder = FakeEmbedder()
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.kb = KnowledgeBase(
            self._tmp.name,
            embedder=self.embedder,
            protect=_protect,
            start_worker=False,
            memory_index_factory=FakeMemoryIndex,
        )
        self.addCleanup(self.kb.close)
        self.admin = actor(admin=True)
        self.user = actor()

    def _local_save(self, origins=("https://llm.example.com",)):
        return self.kb.save_settings(self.admin, {"approved_origins": list(origins)})

    def _save_settings(self, endpoint="https://embed.example.com/v1/embeddings",
                       model="e-model", api_key="sk-secret",
                       origins=("https://llm.example.com",)):
        """Legacy remote payload; accepted for one-generation migration, creds ignored."""
        return self.kb.save_settings(
            self.admin,
            {"endpoint": endpoint, "model": model, "api_key": api_key,
             "approved_origins": list(origins)},
        )

    def _index(self, document_id):
        self.assertTrue(self.kb.process_one())
        return document_id


class FreshLocalDefaultsTests(LocalTestCase):
    def test_fresh_kb_defaults_local_faiss_and_uses_vectors(self):
        self.assertEqual(self.kb.configuration().get("mode"), MODE)
        result = self.kb.upload(self.admin, *_text("公司报销流程", "报销.txt"))
        self.assertTrue(self.kb.process_one())
        search = self.kb.search(self.user, "公司报销流程")
        self.assertEqual(search["mode"], MODE)
        self.assertTrue(search["items"])
        self.assertIn("vector", search["items"][0]["matches"])
        self.assertEqual(search["items"][0]["document_id"], result["id"])
        with self.kb.connect() as db:
            vectors = db.execute("SELECT count(*) FROM local_vectors").fetchone()[0]
            spans = db.execute("SELECT count(*) FROM chunks").fetchone()[0]
        self.assertEqual(vectors, spans)


class LocalSettingsTests(LocalTestCase):
    def test_local_save_needs_no_credentials_and_returns_fixed_local_values(self):
        result = self._local_save()
        self.assertEqual(result["mode"], MODE)
        self.assertTrue(result["configured"])
        self.assertEqual(result["approved_origins"], ["https://llm.example.com"])
        self.assertEqual(result["engine"], MODE)
        self.assertEqual(result["model"], MODEL_NAME)
        self.assertEqual(result["dimensions"], DIMENSIONS)
        self.assertNotIn("api_key", result)
        self.assertNotIn("key_cipher", result)
        self.assertNotIn("endpoint", result)

    def test_existing_domain_restrictions_stay_403(self):
        self._local_save()
        with self.assertRaises(AssistantError) as ctx:
            self.kb.search(self.user, "公司报销流程",
                           profile={"endpoint": "https://evil.example.com"})
        self.assertEqual(ctx.exception.status, 403)

    def test_fresh_local_library_uses_current_chat_profile_without_separate_settings(self):
        self.assertTrue(self.kb.settings(self.admin)["configured"])
        profile = {"endpoint": "https://wan.vnet.com/v1/chat/completions"}
        doc = self.kb.upload(self.admin, *_text("公司报销流程", "报销.txt"))
        self._index(doc["id"])
        result = self.kb.search(self.user, "公司报销流程", profile=profile)
        self.assertEqual(result["items"][0]["document_id"], doc["id"])
        self.assertTrue(self.kb.evidence_current(result["items"], profile))
        self.kb.change(self.admin, doc["id"], "delete", revision=1)
        self.assertFalse(self.kb.evidence_current(result["items"], profile))
        with self.assertRaises(AssistantError):
            self.kb.model_allowed({"endpoint": "http://invalid.example"})

    def test_guests_and_nonadmin_cannot_save_settings(self):
        for forbidden in (actor(guest=True), actor(), actor(role="guest")):
            with self.subTest(forbidden=forbidden):
                with self.assertRaises(AssistantError) as ctx:
                    self.kb.save_settings(
                        forbidden,
                        {"approved_origins": ["https://llm.example.com"]},
                    )
                self.assertEqual(ctx.exception.status, 403)


class LegacyMigrationTests(LocalTestCase):
    def test_legacy_payload_without_mode_stays_local_faiss(self):
        self._save_settings()
        result = self.kb.settings(self.admin)
        self.assertEqual(result["mode"], MODE)
        self.assertTrue(result["configured"])
        stored = self.kb.configuration()
        self.assertNotIn("key_cipher", stored)
        self.assertNotIn("endpoint", stored)

    def test_legacy_payload_to_local_keeps_history_and_reindexes_pending(self):
        self._local_save()
        v1 = self.kb.upload(self.admin, *_text("北极星内部准则甲", "甲文件.txt"))
        self._index(v1["id"])

        # Queue a second version while the embedder is failing.
        self.embedder.fail = True
        v2 = self.kb.upload(self.admin, *_text("南美洲保密守则乙", "乙文件.txt"),
                            document_id=v1["id"], revision=v1["version"])
        self.assertTrue(self.kb.process_one())
        with self.kb.connect() as db:
            self.assertEqual(
                db.execute("SELECT status FROM documents WHERE id=?", (v1["id"],)).fetchone()["status"], "failed")

        # Re-save with a legacy remote payload: must bypass the failed embedder only for
        # the switch metadata; actual reindex retries with a working embedder.
        before = len(self.embedder.calls)
        result = self._save_settings()
        self.assertEqual(result["mode"], MODE)
        self.embedder.fail = False
        self.assertTrue(self.kb.process_one())

        with self.kb.connect() as db:
            row = db.execute("SELECT status,active_version,pending_version FROM documents WHERE id=?",
                             (v1["id"],)).fetchone()
            versions = db.execute("SELECT version FROM versions WHERE document_id=? ORDER BY version",
                                  (v1["id"],)).fetchall()
        self.assertEqual(row["status"], "ready")
        self.assertEqual(row["active_version"], v2["version"])
        self.assertIsNone(row["pending_version"])
        self.assertEqual([v["version"] for v in versions], [1, 2])

        # Only the latest becomes searchable after the atomic local commit.
        old = self.kb.search(self.user, "北极星内部准则甲")
        self.assertEqual(old["items"], [])
        new = self.kb.search(self.user, "南美洲保密守则乙")
        self.assertEqual(new["items"][0]["version"], 2)
        self.assertTrue(self.kb.settings(self.admin)["ready"])


class SensitiveRemainBlockedTests(LocalTestCase):
    def test_blocked_sensitive_not_requeued_after_reindex(self):
        self._local_save()
        doc = self.kb.upload(self.admin, *_text("员工名单\n\n身份证号 110105199003071234", "名单.txt"))
        self.assertTrue(self.kb.process_one())
        with self.kb.connect() as db:
            row = db.execute("SELECT status FROM documents WHERE id=?", (doc["id"],)).fetchone()
        self.assertEqual(row["status"], "blocked")

        # Re-saving settings must not requeue blocked files.
        self._local_save()
        self.assertEqual(self.kb.process_one(), False)
        with self.kb.connect() as db:
            row = db.execute("SELECT status,active_version FROM documents WHERE id=?", (doc["id"],)).fetchone()
        self.assertEqual(row["status"], "blocked")
        self.assertIsNone(row["active_version"])
        self.assertEqual(self.kb.search(self.user, "员工名单")["items"], [])


class AtomicLocalReplacementTests(LocalTestCase):
    def test_local_replacement_commits_atomically_latest_only_searchable(self):
        v1 = self.kb.upload(self.admin, *_text("北极星内部准则甲", "甲文件.txt"))
        self._index(v1["id"])
        v2 = self.kb.upload(self.admin, *_text("南美洲保密守则乙", "乙文件.txt"),
                            document_id=v1["id"], revision=v1["version"])

        # Before commit, only v1 is searchable.
        self.assertEqual(self.kb.search(self.user, "北极星内部准则甲")["items"][0]["version"], 1)
        self.assertEqual(self.kb.search(self.user, "南美洲保密守则乙")["items"], [])

        self.assertTrue(self.kb.process_one())

        with self.kb.connect() as db:
            row = db.execute("SELECT active_version,pending_version,status FROM documents WHERE id=?",
                             (v1["id"],)).fetchone()
            versions = db.execute("SELECT version FROM versions WHERE document_id=? ORDER BY version",
                                  (v1["id"],)).fetchall()
        self.assertEqual(row["active_version"], v2["version"])
        self.assertIsNone(row["pending_version"])
        self.assertEqual(row["status"], "ready")
        self.assertEqual([v["version"] for v in versions], [1, 2])

        # Latest version only is searchable; history is preserved but hidden.
        self.assertEqual(self.kb.search(self.user, "北极星内部准则甲")["items"], [])
        self.assertEqual(self.kb.search(self.user, "南美洲保密守则乙")["items"][0]["version"], 2)

    def test_delete_excluded_immediately_and_restore_reindexes_with_vectors(self):
        doc = self._index(self.kb.upload(self.admin, *_text("公司报销流程", "报销.txt"))["id"])

        self.kb.change(self.admin, doc, "delete", revision=1)
        self.assertEqual(self.kb.search(self.user, "公司报销流程")["items"], [])
        with self.kb.connect() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM local_vectors").fetchone()[0], 0)

        row = self.kb.change(self.admin, doc, "restore", revision=2)
        self.assertEqual(row["status"], "queued")
        self.assertIsNone(row["active_version"])

        self._index(doc)

        search = self.kb.search(self.user, "公司报销流程")
        self.assertEqual(search["mode"], MODE)
        self.assertEqual(search["items"][0]["document_id"], doc)


class DuplicateIdempotentTests(LocalTestCase):
    def test_duplicate_upload_remains_idempotent(self):
        first = self.kb.upload(self.admin, *_text("公司报销流程", "报销.txt"))
        dup = self.kb.upload(self.admin, *_text("公司报销流程", "报销.txt"))
        self.assertTrue(dup["duplicate"])
        self.assertEqual(dup["id"], first["id"])


class LocalSearchTests(LocalTestCase):
    def test_two_char_chinese_term_inside_longer_natural_question(self):
        self._index(self.kb.upload(self.admin, *_text("报销制度要求真实凭证", "报销.txt"))["id"])
        search = self.kb.search(self.user, "我们公司怎么报销？")
        self.assertEqual(search["mode"], MODE)
        self.assertTrue(search["items"])
        self.assertIn("报销", search["items"][0]["text"])

    def test_title_and_filename_matching(self):
        name = "项目计划书.txt"  # content intentionally omits the filename/title keyword
        self._index(self.kb.upload(
            self.admin, name, "所有内部进度都在这里汇总并按周更新，正文不含标题词汇。".encode("utf-8"))["id"])
        search = self.kb.search(self.user, "项目计划书")
        self.assertEqual(search["mode"], MODE)
        self.assertTrue(search["items"])
        self.assertEqual(search["items"][0]["name"], name)

    def test_no_match_returns_empty(self):
        self._index(self.kb.upload(self.admin, *_text("公司报销流程", "报销.txt"))["id"])
        search = self.kb.search(self.user, "量子引力波实验")
        self.assertEqual(search["items"], [])

    def test_sql_syntax_text_treated_as_ordinary_query_and_database_intact(self):
        doc_id = self._index(self.kb.upload(self.admin, *_text("公司报销流程", "报销.txt"))["id"])
        injection = "'; DROP TABLE chunks;--"

        search = self.kb.search(self.user, injection)
        self.assertIn(search["mode"], (MODE, "keyword"))
        self.assertEqual(search["items"], [])

        with self.kb.connect() as db:
            docs = db.execute(
                "SELECT status,active_version,chunks FROM documents WHERE id=?",
                (doc_id,)).fetchone()
            chunk_count = db.execute(
                "SELECT count(*) FROM chunks WHERE document_id=?",
                (doc_id,)).fetchone()[0]
        self.assertEqual(docs["status"], "ready")
        self.assertIsNotNone(docs["active_version"])
        self.assertEqual(docs["chunks"], chunk_count)
        self.assertGreaterEqual(chunk_count, 1)

        normal = self.kb.search(self.user, "公司报销流程")
        self.assertEqual(normal["mode"], MODE)
        self.assertTrue(normal["items"])
        self.assertEqual(normal["items"][0]["document_id"], doc_id)

    def test_long_malformed_input_rejected(self):
        with self.assertRaises(AssistantError) as ctx:
            self.kb.search(self.user, "x" * 1001)
        self.assertEqual(ctx.exception.status, 400)

    def test_credentials_in_query_rejected(self):
        with self.assertRaises(AssistantError) as ctx:
            self.kb.search(self.user, "联系 sk-abcdefghijklmn")
        self.assertEqual(ctx.exception.status, 400)


if __name__ == "__main__":
    unittest.main(verbosity=2)
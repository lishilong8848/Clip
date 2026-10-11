"""FAISS lifecycle integration tests for the local knowledge base.

Covers the migration from the old keyword/hybrid (no sqlite_vec) install, serial
embedding under concurrency, delete/replace against the active in-memory snapshot,
restart recovery without re-embedding, singleflight snapshot building, failed
snapshot rebuilds that fall back to FTS while keeping the old version, and the
preserved document version history (报告 history view).  Everything runs
against temporary databases with the injected deterministic embedder and a real
FAISS in-memory index; the worker thread is disabled and no network is used.
"""
import json
from contextlib import closing
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from openclaw_service.assistant.lighthouse_knowledge import MODE, KnowledgeBase
from bin.test_lighthouse_knowledge import FakeEmbedder, FakeMemoryIndex, actor, _protect, _text


def _build_legacy_db(root, mode="keyword", doc_id="legacy1",
                     content="公司报销流程\n\n差旅报销需在三天内提交", name="报销.txt"):
    """Create a pre-migration keyword/hybrid DB (no local_vectors, no migration flag)."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    originals = root / "originals" / doc_id
    originals.mkdir(parents=True, exist_ok=True)
    (originals / "1.txt").write_bytes(content.encode("utf-8"))
    db_path = root / "knowledge.sqlite3"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript("""
            CREATE TABLE config(key TEXT PRIMARY KEY,payload TEXT NOT NULL);
            CREATE TABLE documents(
                id TEXT PRIMARY KEY,owner TEXT NOT NULL,owner_name TEXT NOT NULL,name TEXT NOT NULL,
                revision INTEGER NOT NULL DEFAULT 1,active_version INTEGER,pending_version INTEGER,
                deleted INTEGER NOT NULL DEFAULT 0,status TEXT NOT NULL,error TEXT NOT NULL DEFAULT '',
                updated_at REAL NOT NULL,size INTEGER NOT NULL,chunks INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE versions(
                document_id TEXT NOT NULL,version INTEGER NOT NULL,name TEXT NOT NULL,
                hash TEXT NOT NULL,path TEXT NOT NULL,size INTEGER NOT NULL,created_at REAL NOT NULL,
                PRIMARY KEY(document_id,version));
            CREATE TABLE chunks(
                id INTEGER PRIMARY KEY,document_id TEXT NOT NULL,version INTEGER NOT NULL,
                ordinal INTEGER NOT NULL,text TEXT NOT NULL,location TEXT NOT NULL);
            CREATE TABLE index_staging(
                document_id TEXT NOT NULL,ordinal INTEGER NOT NULL,embedding BLOB NOT NULL,
                PRIMARY KEY(document_id,ordinal));
            CREATE VIRTUAL TABLE chunks_fts USING fts5(text,tokenize='trigram');
        """)
        conn.execute(
            "INSERT INTO config(key,payload) VALUES('embedding',?)",
            (json.dumps({"mode": mode, "approved_origins": ["https://llm.example.com"], "generation": 1}),))
        conn.execute(
            "INSERT INTO documents(id,owner,owner_name,name,revision,active_version,pending_version,deleted,status,error,updated_at,size,chunks)"
            " VALUES(?,?,?,?,?,?,?,0,'ready','',0,?,0)",
            (doc_id, "legacy-owner", "旧用户", name, 1, 1, None, len(content)))
        conn.execute(
            "INSERT INTO versions(document_id,version,name,hash,path,size,created_at) VALUES(?,?,?,?,?,?,0)",
            (doc_id, 1, name, "hash", "originals/%s/1.txt" % doc_id, len(content)))
        conn.execute(
            "INSERT INTO chunks(document_id,version,ordinal,text,location) VALUES(?,?,?,?,?)",
            (doc_id, 1, 0, content, name + ":段落1"))
        conn.execute("INSERT INTO chunks_fts(rowid,text) VALUES(1,?)", (name + "\n" + content,))
        conn.commit()
    finally:
        conn.close()


class LegacyMigrationTests(unittest.TestCase):
    def test_settings_retry_wakes_worker_and_invalidates_document_status(self):
        with tempfile.TemporaryDirectory() as root:
            kb = KnowledgeBase(root, embedder=FakeEmbedder(), start_worker=False,
                               memory_index_factory=FakeMemoryIndex)
            self.addCleanup(kb.close)
            row = kb.upload(actor(), '制度.txt', '公司制度说明'.encode('utf-8'))
            with kb.connect() as db:
                db.execute("UPDATE documents SET status='failed',error='temporary' WHERE id=?", (row['id'],))
                revision = kb._revision(db)
            kb._wake.clear()
            with patch.object(kb, '_ensure_worker') as wake:
                kb.save_settings(actor(admin=True), {'approved_origins': ['https://llm.example.com']})
                wake.assert_called_once()
            self.assertTrue(kb._wake.is_set())
            with kb.connect() as db:
                self.assertGreater(kb._revision(db), revision)
                self.assertEqual(db.execute('SELECT status FROM documents WHERE id=?', (row['id'],)).fetchone()[0], 'queued')

    def _migrate(self, mode):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = temporary.name
        _build_legacy_db(root, mode=mode)
        kb = KnowledgeBase(root, embedder=FakeEmbedder(), protect=_protect,
                           start_worker=False, memory_index_factory=FakeMemoryIndex)
        self.addCleanup(kb.close)
        return root, kb

    def test_backup_timeout_leaves_legacy_database_unchanged(self):
        with tempfile.TemporaryDirectory() as root:
            _build_legacy_db(root)
            with patch('openclaw_service.assistant.lighthouse_knowledge._MAX_BACKUP_SECONDS', 0):
                with self.assertRaisesRegex(Exception, '备份超时'):
                    KnowledgeBase(root, embedder=FakeEmbedder(), start_worker=False)
            with closing(sqlite3.connect(Path(root) / 'knowledge.sqlite3')) as db:
                self.assertIsNone(db.execute("SELECT payload FROM config WHERE key='local_faiss_migrated'").fetchone())
                self.assertEqual(db.execute('SELECT status FROM documents').fetchone()[0], 'ready')
            self.assertEqual(list(Path(root).glob('knowledge.sqlite3.bak-*.sqlite3')), [])

    def test_migration_creates_backup_flag_and_requeues_legacy_ready_doc(self):
        for mode in ("keyword", "hybrid"):
            with self.subTest(mode=mode):
                root, kb = self._migrate(mode)
                backups = list(Path(root).glob("knowledge.sqlite3.bak-*.sqlite3"))
                self.assertEqual(len(backups), 1, "legacy migration must back up the DB")
                self.assertEqual(kb.configuration()["mode"], MODE)
                with kb.connect() as db:
                    flag = db.execute("SELECT payload FROM config WHERE key='local_faiss_migrated'").fetchone()
                    queued = db.execute(
                        "SELECT status,pending_version FROM documents WHERE id='legacy1'").fetchone()
                self.assertEqual(flag["payload"], "1")
                self.assertEqual(queued["status"], "queued")
                self.assertEqual(queued["pending_version"], 1)

    def test_migrated_doc_reindexes_into_faiss_and_becomes_searchable(self):
        root, kb = self._migrate("hybrid")
        self.assertTrue(kb.process_one())
        self.assertTrue(kb.settings(actor(admin=True))["ready"])
        with kb.connect() as db:
            vecs = db.execute("SELECT count(*) FROM local_vectors").fetchone()[0]
            row = db.execute("SELECT status,active_version FROM documents WHERE id='legacy1'").fetchone()
        self.assertEqual(row["status"], "ready")
        self.assertEqual(row["active_version"], 1)
        self.assertGreaterEqual(vecs, 1)
        search = kb.search(actor(), "公司报销流程")
        self.assertEqual(search["mode"], MODE)
        self.assertIn("vector", search["items"][0]["matches"])


class ConcurrencyTests(unittest.TestCase):
    def test_forty_accounts_index_serially_and_all_become_ready(self):
        embedder = FakeEmbedder()
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        kb = KnowledgeBase(tmp.name, embedder=embedder, protect=_protect,
                           start_worker=False, memory_index_factory=FakeMemoryIndex)
        self.addCleanup(kb.close)
        kb.save_settings(actor(admin=True), {"approved_origins": ["https://llm.example.com"]})

        count = 40
        for i in range(count):
            name = "doc%02d.txt" % i
            content = "公司报销流程第%d条\n\n需保留真实凭证并按时提交。".encode("utf-8") % i
            kb.upload(actor("user-%d" % i, name="用户%d" % i), name, content)

        # Process from eight worker threads; embedding must stay serialized under _index_lock.
        threads = [threading.Thread(target=lambda: [kb.process_one() for _ in range(5)]) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        with kb.connect() as db:
            statuses = [r["status"] for r in db.execute("SELECT status FROM documents").fetchall()]
            rows = db.execute("SELECT count(*) FROM documents").fetchone()[0]
        self.assertEqual(rows, count)
        self.assertEqual(set(statuses), {"ready"})
        self.assertEqual(embedder.max_active, 1, "embedding must be serialized")

        search = kb.search(actor(), "公司报销流程")
        self.assertEqual(search["mode"], MODE)
        self.assertTrue(search["items"])


class RestartRecoveryTests(unittest.TestCase):
    def test_restart_rebuilds_snapshot_from_blobs_without_reembedding(self):
        embedder = FakeEmbedder()
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        kb = KnowledgeBase(tmp.name, embedder=embedder, protect=_protect,
                           start_worker=False, memory_index_factory=FakeMemoryIndex)
        self.addCleanup(kb.close)
        kb.save_settings(actor(admin=True), {"approved_origins": ["https://llm.example.com"]})
        doc_id = kb.upload(actor(admin=True), *_text("公司报销流程", "报销.txt"))["id"]
        self.assertTrue(kb.process_one())
        calls_before = len(embedder.calls)
        self.assertGreater(kb._snapshot_count(), 0)

        # A new KnowledgeBase over the same directory must not re-embed existing chunks.
        kb2 = KnowledgeBase(tmp.name, embedder=embedder, protect=_protect,
                            start_worker=False, memory_index_factory=FakeMemoryIndex)
        self.addCleanup(kb2.close)
        self.assertEqual(len(embedder.calls), calls_before, "restart must not re-embed ready chunks")
        self.assertTrue(kb2.settings(actor(admin=True))["ready"])
        self.assertGreater(kb2._snapshot_count(), 0)

        search = kb2.search(actor(), "公司报销流程")
        self.assertEqual(search["mode"], MODE)
        self.assertEqual(search["items"][0]["document_id"], doc_id)


class SingletonSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.embedder = FakeEmbedder()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.kb = KnowledgeBase(self.tmp.name, embedder=self.embedder, protect=_protect,
                                start_worker=False, memory_index_factory=FakeMemoryIndex)
        self.addCleanup(self.kb.close)
        self.kb.save_settings(actor(admin=True), {"approved_origins": ["https://llm.example.com"]})
        self.admin = actor(admin=True)

    def _rev(self):
        with self.kb.connect() as db:
            return self.kb._vector_revision(db)

    def test_current_revision_is_not_rebuilt_and_new_revision_builds_once(self):
        doc1 = self.kb.upload(self.admin, *_text("公司报销流程", "报销.txt"))["id"]
        self.assertTrue(self.kb.process_one())
        calls = [0]
        original_factory = self.kb._new_index
        self.kb._new_index = lambda: (calls.__setitem__(0, calls[0] + 1), original_factory())[1]

        rev1 = self._rev()
        snap = self.kb._publish_snapshot(rev1)
        self.assertIsNotNone(snap)
        self.assertEqual(self.kb._ensure_snapshot(rev1), snap)
        self.assertEqual(calls[0], 0, "current snapshot must not be rebuilt")

        doc2 = self.kb.upload(self.admin, *_text("公司报销流程第二版", "报销2.txt"))
        self.assertTrue(self.kb.process_one())
        self.assertEqual(calls[0], 1, "a new revision must be built exactly once")
        self.assertEqual(self.kb._snapshot.revision, self._rev())
        self.assertEqual(self.kb._snapshot_count(), 2)


class DeleteReplaceSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.embedder = FakeEmbedder()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.kb = KnowledgeBase(self.tmp.name, embedder=self.embedder, protect=_protect,
                                start_worker=False, memory_index_factory=FakeMemoryIndex)
        self.addCleanup(self.kb.close)
        self.kb.save_settings(actor(admin=True), {"approved_origins": ["https://llm.example.com"]})
        self.admin = actor(admin=True)

    def test_delete_removes_vectors_and_restore_reindexes_snapshot(self):
        doc = self.kb.upload(self.admin, *_text("公司报销流程", "报销.txt"))["id"]
        self.assertTrue(self.kb.process_one())
        self.assertGreater(self.kb._snapshot_count(), 0)

        self.kb.change(self.admin, doc, "delete", revision=1)
        self.assertEqual(self.kb._snapshot_count(), 0, "deleted vectors must vanish from snapshot")
        self.assertEqual(self.kb.search(actor(), "公司报销流程")["items"], [])

        self.kb.change(self.admin, doc, "restore", revision=2)
        self.assertTrue(self.kb.process_one())
        self.assertGreater(self.kb._snapshot_count(), 0)
        search = self.kb.search(actor(), "公司报销流程")
        self.assertEqual(search["mode"], MODE)
        self.assertEqual(search["items"][0]["document_id"], doc)


class HistoryTests(unittest.TestCase):
    def setUp(self):
        self.embedder = FakeEmbedder()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.kb = KnowledgeBase(self.tmp.name, embedder=self.embedder, protect=_protect,
                                start_worker=False, memory_index_factory=FakeMemoryIndex)
        self.addCleanup(self.kb.close)
        self.kb.save_settings(actor(admin=True), {"approved_origins": ["https://llm.example.com"]})
        self.admin = actor(admin=True)

    def test_replace_preserves_history_old_viewable_but_not_searchable(self):
        v1 = self.kb.upload(self.admin, *_text("公司报销流程第一版", "报销.txt"))
        self.assertTrue(self.kb.process_one())
        self.kb.upload(self.admin, *_text("公司报销流程第二版", "报销.txt"),
                       document_id=v1["id"], revision=v1["version"])
        self.assertTrue(self.kb.process_one())

        with self.kb.connect() as db:
            versions = [r["version"] for r in db.execute(
                "SELECT version FROM versions WHERE document_id=? ORDER BY version", (v1["id"],)).fetchall()]
        self.assertEqual(versions, [1, 2])

        search = self.kb.search(actor(), "公司报销流程")
        self.assertTrue(search["items"])
        self.assertTrue(all(item["version"] == 2 for item in search["items"]),
                        "only the latest version may be surfaced")

        hist = self.kb.document(self.admin, v1["id"], {"version": "1"})
        self.assertTrue(hist["document"]["historical"])
        self.assertEqual(hist["document"]["view_version"], 1)
        self.assertTrue(hist["sections"])


class SnapshotFallbackTests(unittest.TestCase):
    def setUp(self):
        self.embedder = FakeEmbedder()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.kb = KnowledgeBase(self.tmp.name, embedder=self.embedder, protect=_protect,
                                start_worker=False, memory_index_factory=FakeMemoryIndex)
        self.addCleanup(self.kb.close)
        self.kb.save_settings(actor(admin=True), {"approved_origins": ["https://llm.example.com"]})
        self.admin = actor(admin=True)

    def _rev(self):
        with self.kb.connect() as db:
            return self.kb._vector_revision(db)

    def test_snapshot_rebuild_failure_falls_back_to_keyword_without_crash(self):
        doc = self.kb.upload(self.admin, *_text("公司报销流程", "报销.txt"))["id"]
        self.assertTrue(self.kb.process_one())
        rev = self._rev()

        self.kb._snapshot = None  # force a stale / missing snapshot
        self.kb._memory_index_factory = lambda: (_ for _ in ()).throw(RuntimeError("index build failed"))

        search = self.kb.search(actor(), "公司报销流程")
        self.assertEqual(search["mode"], "keyword")
        self.assertIn("索引尚未就绪", search["warning"])
        self.assertEqual(search["items"][0]["document_id"], doc, "FTS fallback still returns the doc")

        # Once the factory is healthy again the snapshot can be rebuilt in place.
        self.kb._memory_index_factory = FakeMemoryIndex
        search = self.kb.search(actor(), "公司报销流程")
        self.assertEqual(search["mode"], MODE)
        self.assertIn("vector", search["items"][0]["matches"])

    def test_failed_snapshot_publish_keeps_old_snapshot_and_falls_back(self):
        v1 = self.kb.upload(self.admin, *_text("公司报销流程第一版", "报销.txt"))["id"]
        self.assertTrue(self.kb.process_one())
        saved_rev = self.kb._snapshot.revision

        self.kb._memory_index_factory = lambda: (_ for _ in ()).throw(RuntimeError("index build failed"))
        v2 = self.kb.upload(self.admin, *_text("公司报销流程第二版", "报销.txt"),
                            document_id=v1, revision=1)
        self.assertTrue(self.kb.process_one())  # DB commit succeeds; only the snapshot build fails

        with self.kb.connect() as db:
            row = db.execute("SELECT status,active_version FROM documents WHERE id=?", (v1,)).fetchone()
        self.assertEqual(row["status"], "ready")
        self.assertEqual(row["active_version"], v2["version"])
        self.assertEqual(self.kb._snapshot.revision, saved_rev,
                         "failed snapshot build must keep the previous snapshot")
        # The snapshot is stale vs. the current vector revision, so search falls back
        # to FTS without crashing and still returns usable results.
        fallback = self.kb.search(self.admin, "公司报销流程")
        self.assertEqual(fallback["mode"], "keyword")
        self.assertIn("索引尚未就绪", fallback["warning"])
        self.assertTrue(fallback["items"])

        self.kb._memory_index_factory = FakeMemoryIndex
        search = self.kb.search(self.admin, "公司报销流程")
        self.assertEqual(search["mode"], MODE)
        self.assertTrue(all(item["version"] == 2 for item in search["items"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)

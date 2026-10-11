"""Stream-storage defects for KnowledgeBase.upload_file / _persist_content / process_one.

These tests exercise the REAL ``KnowledgeBase`` (SQLite + filesystem staging) with
injected ``FakeEmbedder``/``FakeMemoryIndex`` so no network/model is touched.  They
encode the *desired* streaming behaviour:

* the file body is copied to disk BEFORE the metadata write transaction is taken;
* ``_persist_content`` enforces the streamed per-chunk 100MiB limit and never reads
  unboundedly;
* duplicate / failed-read / empty / stale-revision uploads clean up their staging
  and final files;
* the source file-like object stays owned by the caller;
* Windows directory-backslash shells out to the basename;
* embeddings are staged incrementally (``index_staging``) instead of accumulated
  all in memory and packed at the end;
* old-version vectors are removed when a new version commits;
* 40 parallel searches of one unique question perform a single embedding
  (singleflight) and reads proceed while a document embedding is running
  (no outer ``_index_lock`` on the read path).

Run with ``bin/.venv/Scripts/python.exe -m unittest``.  Test failures are reported
honestly: assertions that still encode a defective behaviour will fail against the
current KnowledgeBase until Codex completes the production fixes.
"""
import math
import hashlib
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_knowledge import (
    DIMENSIONS,
    MODE,
    MAX_FILE_BYTES,
    _EMBED_BATCH,
    _STREAM_CHUNK,
    KnowledgeBase,
)
from openclaw_service.assistant.lighthouse_knowledge_local import LocalEmbeddings


def actor(uid="u1", *, admin=False, guest=False, role=None, name="用户"):
    role = role or ("guest" if guest else ("admin" if admin else "user"))
    return {"id": uid, "is_admin": admin, "is_guest": guest, "role": role, "name": name}


def _protect(value):
    return "CIPHER:" + value


class FakeEmbedder:
    """Deterministic, injectable, non-network embedder (512-dim unit vectors)."""

    def __init__(self, dim=DIMENSIONS):
        self.dim = dim
        self.calls = []
        self.fail = False
        self.closed = False

    def vector_for(self, text):
        digest = hashlib.sha256(text.encode("utf-8")).digest()
        vec = [(digest[i % len(digest)] / 255.0) * 2 - 1 for i in range(self.dim)]
        norm = math.sqrt(sum(v * v for v in vec)) or 1.0
        return [v / norm for v in vec]

    def embed(self, texts, config=None, *, query=False):
        self.calls.append((list(texts), dict(config or {}), query))
        if self.fail:
            raise AssistantError("模拟嵌入失败", 503)
        return [self.vector_for(text) for text in texts]

    def close(self):
        self.closed = True


class _StreamingCheckEmbedder(FakeEmbedder):
    """Fake embedder that requires prior batches to already be staged in
    ``index_staging`` before the next embed batch is requested."""

    def __init__(self, kb, dim=DIMENSIONS):
        super().__init__(dim)
        self.kb = kb
        self.doc_id = None
        self.embed_calls = 0
        self.missing_staging = False

    def embed(self, texts, config=None, *, query=False):
        self.embed_calls += 1
        if self.embed_calls >= 2 and self.kb is not None and self.doc_id and not query:
            with self.kb.connect() as db:
                n = db.execute(
                    "SELECT count(*) FROM index_staging WHERE document_id=?",
                    (self.doc_id,)).fetchone()[0]
            if n <= 0:
                self.missing_staging = True
        return [self.vector_for(t) for t in texts]


class FakeMemoryIndex:
    """Real FAISS cosine index implementing the local ``MemoryIndex`` contract."""

    def __init__(self):
        self._revision = 0
        self._ids = []
        self._index = None

    @property
    def revision(self):
        return self._revision

    @property
    def count(self):
        return len(self._ids)

    def begin_build(self, revision):
        self._builder_revision = int(revision)
        self._builder_ids = []
        self._builder_vectors = []

    def add_batch(self, rows):
        batch_arrays, batch_ids = [], []
        for chunk_id, blob in list(rows):
            import numpy as np
            arr = np.frombuffer(blob, dtype="<f4")
            if arr.size != DIMENSIONS:
                raise ValueError("bad embedding dimension")
            norm = np.linalg.norm(arr)
            if norm <= 0 or not np.isfinite(norm):
                raise ValueError("bad embedding norm")
            batch_arrays.append((arr / norm).astype("float32"))
            batch_ids.append(int(chunk_id))
        self._builder_ids.extend(batch_ids)
        self._builder_vectors.extend(batch_arrays)

    def end_build(self):
        self._build(self._builder_revision, self._builder_vectors, self._builder_ids)
        self._builder_ids = self._builder_vectors = None

    def cancel_build(self):
        self._builder_ids = self._builder_vectors = None

    def replace(self, revision, rows):
        arrays, ids = [], []
        for chunk_id, blob in rows:
            import numpy as np
            arr = np.frombuffer(blob, dtype="<f4")
            if arr.size != DIMENSIONS:
                raise ValueError("bad embedding dimension")
            norm = np.linalg.norm(arr)
            if norm <= 0 or not np.isfinite(norm):
                raise ValueError("bad embedding norm")
            arrays.append((arr / norm).astype("float32"))
            ids.append(int(chunk_id))
        self._build(revision, arrays, ids)

    def _build(self, revision, arrays, ids):
        self._revision = int(revision)
        self._ids = ids
        self._index = None
        if arrays:
            import faiss
            import numpy as np
            index = faiss.IndexFlatIP(DIMENSIONS)
            index.add(np.stack(arrays).astype("float32"))
            self._index = index

    def search(self, vector, k=40):
        if self._index is None or not self._ids:
            return []
        import faiss
        import numpy as np
        q = np.asarray(vector, dtype="float32")
        norm = np.linalg.norm(q)
        if norm <= 0 or not np.isfinite(norm):
            return []
        scores, idx = self._index.search((q / norm).astype("float32").reshape(1, -1), min(k, len(self._ids)))
        return [(self._ids[int(i)], float(s)) for i, s in zip(idx[0], scores[0]) if i >= 0]


class _BaseKBTest(unittest.TestCase):
    def setUp(self):
        self.embedder = FakeEmbedder()
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        self.kb = KnowledgeBase(
            self.root,
            embedder=self.embedder,
            protect=_protect,
            start_worker=False,
            memory_index_factory=FakeMemoryIndex,
        )

    def _save_settings(self, origins=("https://llm.example.com",)):
        self.kb.save_settings(actor(admin=True), {"approved_origins": list(origins)})

    def _text(self, content="公司报销流程 AABBCCDD", name="说明.txt"):
        return name, content.encode("utf-8")

    def _upload_index(self, name="说明.txt", content="公司报销流程 AABBCCDD", admin=None, **kw):
        admin = admin or actor(admin=True)
        body = content.encode("utf-8") if isinstance(content, str) else content
        doc = self.kb.upload(admin, name, body)
        self.assertTrue(self.kb.process_one())
        return doc

    def _all_physical_files(self):
        files = []
        originals = self.root / "originals"
        if originals.exists():
            files.extend(p for p in originals.rglob("*") if p.is_file())
        uploads = self.root / "uploads"
        if uploads.exists():
            files.extend(p for p in uploads.rglob("*") if p.is_file())
        return files


class SlowReader:
    """Blocks mid-stream after delivering the first chunk, until released."""

    def __init__(self, data, delivered=None, release=None):
        self.data = data
        self.delivered = delivered
        self.release = release
        self._sent = False
        self._done = False
        self.closed = False

    def read(self, n=-1):
        if not self._sent:
            self._sent = True
            if self.delivered is not None:
                self.delivered.set()
            return self.data[:n] if n is not None and n > 0 else self.data
        if not self._done:
            if self.release is not None:
                if not self.release.wait(timeout=10):
                    raise TimeoutError("slow reader release never signalled")
            self._done = True
            return b""
        return b""

    def close(self):
        self.closed = True


class CountingReader:
    """Records every ``read`` size so tests can assert bounded reads.

    ``chunk_size`` caps each ``read`` return so callers that read with a large
    buffer still observe incremental chunks from the stream.
    """

    def __init__(self, data, chunk_size=None):
        self.data = data
        self.chunk_size = chunk_size
        self.pos = 0
        self.read_sizes = []

    def read(self, n=-1):
        self.read_sizes.append(n)
        if n is None or n < 0:
            self.pos = len(self.data)
            return self.data
        limit = n if self.chunk_size is None else min(n, self.chunk_size)
        chunk = self.data[self.pos:self.pos + limit]
        self.pos += len(chunk)
        return chunk


class CopyLockTests(_BaseKBTest):
    def test_copy_happens_before_db_lock_other_writer_succeeds(self):
        # A DB write on another connection must finish quickly while a slow file
        # body is being streamed to staging (the body copy must NOT hold the write
        # transaction).
        delivered = threading.Event()
        release = threading.Event()
        src = SlowReader(b"x" * 2048, delivered=delivered, release=release)
        errors = []
        t = threading.Thread(target=lambda: self._safe_upload(src, errors))
        t.start()
        self.assertTrue(delivered.wait(2), "upload did not start copying")
        start = time.monotonic()
        with self.kb.connect() as db:
            db.execute("INSERT INTO config(key,payload) VALUES('probe','1') "
                       "ON CONFLICT(key) DO UPDATE SET payload='1'")
        elapsed = time.monotonic() - start
        release.set()
        t.join(10)
        self.assertEqual(errors, [])
        self.assertLess(elapsed, 1.0, "DB write blocked behind in-progress file copy")

    def _safe_upload(self, content, errors):
        try:
            self.kb.upload(actor(admin=True), "big.txt", content)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)


class PersistLimitTests(_BaseKBTest):
    def test_per_chunk_limit_aborts_before_reading_past_limit(self):
        # The 100MiB limit must abort the copy as soon as the running total
        # exceeds a (patched-small) limit, without draining the whole stream.
        original_max = MAX_FILE_BYTES
        from openclaw_service.assistant import lighthouse_knowledge as kbmod
        kbmod.MAX_FILE_BYTES = 512
        try:
            reader = CountingReader(b"z" * 4096, chunk_size=128)
            with self.assertRaises(AssistantError) as ctx:
                self.kb.upload(actor(admin=True), "too-big.txt", reader)
            self.assertEqual(ctx.exception.status, 413)
            # The bounded stream must not have consumed all 4KiB just because
            # the limit (512) was crossed mid-way.
            self.assertLess(
                reader.pos, 4096,
                "_persist_content must stop shortly after crossing the limit")
        finally:
            kbmod.MAX_FILE_BYTES = original_max
        self.assertEqual(self._all_physical_files(), [])


class CleanupTests(_BaseKBTest):
    def test_duplicate_returns_existing_and_only_one_physical_version(self):
        self._save_settings()
        admin = actor(admin=True)
        first = self._upload_index("报销.txt", "公司报销流程 AABBCCDD", admin=admin)
        self.assertEqual(len(self._all_physical_files()), 1)
        dup = self.kb.upload(admin, "报销.txt", "公司报销流程 AABBCCDD".encode("utf-8"))
        self.assertTrue(dup.get("duplicate"))
        self.assertEqual(dup["id"], first["id"])
        # The duplicate's staged copy must have been deleted: still exactly one
        # physical file for the single committed version.
        self.assertEqual(len(self._all_physical_files()), 1)
        versions = list((self.root / "originals" / first["id"]).glob("*.txt"))
        self.assertEqual(len(versions), 1)

    def test_oversize_raises_and_removes_staging_file(self):
        from openclaw_service.assistant import lighthouse_knowledge as kbmod
        original = kbmod.MAX_FILE_BYTES
        kbmod.MAX_FILE_BYTES = 128
        try:
            with self.assertRaises(AssistantError) as ctx:
                self.kb.upload(actor(admin=True), "big.txt", b"y" * 256)
            self.assertEqual(ctx.exception.status, 413)
        finally:
            kbmod.MAX_FILE_BYTES = original
        self.assertEqual(self._all_physical_files(), [])

    def test_empty_content_rejected_and_cleaned(self):
        with self.assertRaises(AssistantError) as ctx:
            self.kb.upload(actor(admin=True), "empty.txt", b"")
        self.assertEqual(ctx.exception.status, 413)
        self.assertEqual(self._all_physical_files(), [])

    def test_read_failure_mid_copy_cleans_staging(self):
        class BoomReader:
            def read(self, n=-1):
                raise OSError("simulated disk read failure")
        with self.assertRaises(OSError):
            self.kb.upload(actor(admin=True), "boom.txt", BoomReader())
        self.assertEqual(self._all_physical_files(), [])


class OptimisticRecheckTests(_BaseKBTest):
    def test_stale_revision_after_copy_cleans_up_and_rejects(self):
        self._save_settings()
        admin = actor(admin=True)
        doc = self._upload_index("报销.txt", "公司报销流程第一版", admin=admin)
        delivered = threading.Event()
        release = threading.Event()
        src = SlowReader(b"company replacement v2 " * 64, delivered=delivered, release=release)
        errors = []
        t = threading.Thread(target=lambda: self._safe_replace(doc, src, errors))
        t.start()
        self.assertTrue(delivered.wait(2), "replacement copy did not start")
        # While the copy is in progress (no DB lock held), delete the document.
        start = time.monotonic()
        self.kb.change(admin, doc["id"], "delete", revision=1)
        deleted_elapsed = time.monotonic() - start
        self.assertLess(deleted_elapsed, 1.0,
                        "delete blocked behind in-progress replacement copy")
        release.set()
        t.join(10)
        self.assertEqual(len(errors), 1)
        exc = errors[0]
        self.assertIsInstance(exc, AssistantError)
        self.assertEqual(exc.status, 409)
        # The failed replacement's staged copy must be gone and no v2 final file
        # may have been committed; the committed v1 original legitimately remains.
        uploads = self.root / "uploads"
        if uploads.exists():
            self.assertEqual(list(uploads.rglob("*.part")), [])
        originals = self.root / "originals" / doc["id"]
        self.assertEqual(sorted(p.name for p in originals.iterdir()), ["1.txt"])

    def _safe_replace(self, doc, content, errors):
        try:
            self.kb.upload(actor(admin=True), "报销.txt", content,
                           document_id=doc["id"], revision=1)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)


class HandleOwnershipAndSanitizeTests(_BaseKBTest):
    def test_source_handle_stays_open_owned_caller(self):
        with tempfile.NamedTemporaryFile("w+b", suffix=".txt", delete=False) as fh:
            fh.write(b"company note one two three")
            fh.flush()
        try:
            with open(fh.name, "rb") as src:
                self.kb.upload(actor(admin=True), "owned.txt", src)
                self.assertFalse(src.closed, "KnowledgeBase must not close the caller's file")
        finally:
            os.unlink(fh.name)
        # A committed upload legitimately persists one physical file; the point of
        # this assertion is only that the caller-owned source handle was NOT closed.

    def test_windows_backslash_filename_sanitized(self):
        name = "folder" + chr(92) + "file.txt"  # single Windows directory backslash
        result = self.kb.upload(actor(admin=True), name, b"company content here")
        self.assertEqual(result["name"], "file.txt")

    def test_bounded_1mib_reads_never_read_minus1(self):
        reader = CountingReader(b"a" * (3 * 1024))
        with tempfile.TemporaryDirectory() as d2:
            kb2 = KnowledgeBase(d2, embedder=FakeEmbedder(), protect=_protect, start_worker=False)
            try:
                kb2.upload(actor(admin=True), "bounded.txt", reader)
            finally:
                kb2.close()
        self.assertTrue(reader.read_sizes)
        self.assertLessEqual(max(reader.read_sizes), _STREAM_CHUNK,
                             "read() must be bounded to the 1MiB stream chunk")
        self.assertNotIn(-1, reader.read_sizes)
        self.assertNotIn(None, reader.read_sizes)


class VectorStagingAndVersionTests(_BaseKBTest):
    def test_vectors_staged_incrementally_not_accumulated_then_packed(self):
        # Streamed embeddings must arrive in index_staging batch-by-batch while
        # process_one runs, not be collected in a Python list and packed all at
        # the end.
        self._save_settings()
        embedder = _StreamingCheckEmbedder(self.kb)
        kb = KnowledgeBase(self.root, embedder=embedder, protect=_protect,
                           start_worker=False, memory_index_factory=FakeMemoryIndex)
        kb.save_settings(actor(admin=True), {"approved_origins": ["https://llm.example.com"]})

        def many_sections(_content, _name):
            return [{"text": "公司制度说明段落 %03d AABBCCDD" % i, "location": "p%d" % i}
                    for i in range(_EMBED_BATCH * 3)]
        kb.extractor = many_sections
        doc = kb.upload(actor(admin=True), "many.txt", b"padding" * 128)
        embedder.kb = kb
        embedder.doc_id = doc["id"]
        kb.process_one()
        self.assertFalse(embedder.missing_staging,
                         "earlier embed batches were not staged to index_staging "
                         "before the next batch was requested (all vectors "
                         "accumulated then packed)")

    def test_old_version_vectors_removed_on_replacement(self):
        self._save_settings()
        admin = actor(admin=True)
        v1 = self._upload_index("报销.txt", "公司报销流程第一版 AABBCCDD", admin=admin)
        v2 = self.kb.upload(admin, "报销.txt", "公司报销流程第二版 AABBCCDD".encode("utf-8"),
                            document_id=v1["id"], revision=1)
        self.assertTrue(self.kb.process_one())
        with self.kb.connect() as db:
            rows = db.execute('''
                SELECT c.version FROM local_vectors lv
                JOIN chunks c ON c.id = lv.chunk_id
                WHERE c.document_id = ? ORDER BY c.version
            ''', (v1["id"],)).fetchall()
        self.assertTrue(rows)
        self.assertEqual(set(row[0] for row in rows), {v2["version"]},
                         "old-version vectors were not removed after replacement")


class LocalSearchConcurrencyTests(_BaseKBTest):
    def test_40_parallel_searches_singleflight_one_embedding(self):
        class FakeModel:
            def __init__(self, stats):
                self.stats = stats
            def embed(self, batch, batch_size=None, parallel=None):
                # Doc embeddings arrive with batch_size>1 because the uploaded
                # document is long enough to split into several chunks; a query
                # embedding always arrives with exactly batch_size==1.
                is_query = batch_size == 1
                with self.stats["lock"]:
                    self.stats["queries" if is_query else "docs"] += 1
                # Keep the merged single-flight query embedding in-flight long
                # enough for all 40 parallel callers to join it.
                if is_query:
                    time.sleep(0.1)
                return [[1.0] + [0.0] * (DIMENSIONS - 1) for _ in batch]

        stats = {"lock": threading.Lock(), "queries": 0, "docs": 0}

        class Factory:
            def __init__(self):
                self.calls = 0
                self.lock = threading.Lock()
            def __call__(self):
                with self.lock:
                    self.calls += 1
                return FakeModel(stats)

        factory = Factory()
        # A document long enough to split into several embedded chunks, so the
        # document embedding is NOT misclassified as a query (batch_size==1).
        long_doc = ("公司报销流程 第一版 AABBCCDD 费用与单据规则详细说明。 " * 80)
        with tempfile.TemporaryDirectory() as d2:
            embedder = LocalEmbeddings(d2, model_factory=factory, start_worker=True)
            kb = KnowledgeBase(d2, embedder=embedder, protect=_protect,
                               start_worker=False, memory_index_factory=FakeMemoryIndex)
            try:
                kb.save_settings(actor(admin=True), {"approved_origins": ["https://llm.example.com"]})
                kb.upload(actor(admin=True), "报销.txt", long_doc.encode("utf-8"))
                self.assertTrue(kb.process_one())
                with stats["lock"]:
                    self.assertGreaterEqual(stats["docs"], 1,
                                            "expected at least one document embedding")

                results = []
                lock = threading.Lock()

                def run():
                    try:
                        r = kb.search(actor(), "同一个问题")
                        with lock:
                            results.append(r)
                    except Exception as exc:  # noqa: BLE001
                        with lock:
                            results.append(exc)

                threads = [threading.Thread(target=run) for _ in range(40)]
                for th in threads:
                    th.start()
                for th in threads:
                    th.join(timeout=30)

                self.assertEqual(len(results), 40)
                errors = [r for r in results if isinstance(r, Exception)]
                self.assertEqual(errors, [])
                with stats["lock"]:
                    self.assertEqual(stats["queries"], 1,
                                     "40 identical parallel searches must single-flight "
                                     "to exactly one query embedding")
                self.assertEqual(factory.calls, 1)
            finally:
                kb.close()
                embedder.close()

    def test_query_reads_proceed_while_doc_embedding_runs(self):
        class BlockableEmbedder(FakeEmbedder):
            def __init__(self, started, release):
                super().__init__()
                self.started = started
                self.release = release
                self.blocking = False
                self.query_calls = 0

            def embed(self, texts, config=None, *, query=False):
                if query:
                    self.query_calls += 1
                    return [self.vector_for(t) for t in texts]
                if self.blocking:
                    self.started.set()
                    if not self.release.wait(timeout=10):
                        raise TimeoutError("doc embedding release never signalled")
                return [self.vector_for(t) for t in texts]

        with tempfile.TemporaryDirectory() as d3:
            started = threading.Event()
            release = threading.Event()
            embedder = BlockableEmbedder(started, release)
            kb = KnowledgeBase(d3, embedder=embedder, protect=_protect,
                               start_worker=False, memory_index_factory=FakeMemoryIndex)
            try:
                kb.save_settings(actor(admin=True), {"approved_origins": ["https://llm.example.com"]})
                # Index one document fully so the read path has an active_version and
                # an available vector snapshot to read from while a second document
                # embedding is artificially blocked.
                kb.upload(actor(admin=True), "报销.txt", "公司报销流程 第一版 AABBCCDD".encode("utf-8"))
                self.assertTrue(kb.process_one())
                # Now start embedding a second document and block mid-embedding.
                kb.upload(actor(admin=True), "制度.txt", "公司制度说明 第二版 AABBCCDD".encode("utf-8"))
                embedder.blocking = True
                done = []
                t = threading.Thread(target=lambda: done.append(kb.process_one()))
                t.start()
                self.assertTrue(started.wait(3), "second document embedding did not start")
                # A read/search must not take the outer _index_lock and must
                # complete quickly while document embedding holds it.
                t0 = time.monotonic()
                result = kb.search(actor(), "公司报销流程")
                elapsed = time.monotonic() - t0
                self.assertGreaterEqual(embedder.query_calls, 1)
                self.assertLess(elapsed, 1.0, "search blocked behind document embedding")
                release.set()
                t.join(10)
                self.assertEqual(done, [True])
            finally:
                release.set()
                kb.close()


class UploadAliasTests(_BaseKBTest):
    def test_upload_alias_delegates_to_upload_file(self):
        result = self.kb.upload(actor(admin=True), "alias.txt", b"company alias content")
        self.assertEqual(result["name"], "alias.txt")


if __name__ == "__main__":
    unittest.main(verbosity=2)
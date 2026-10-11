"""Isolation tests (unittest only) for assistant.lighthouse_knowledge.

Everything runs against a temporary SQLite database with an injected deterministic
512-dimensional embedder (query keyword accepted) and a real FAISS in-memory index
backed by the peer ``MemoryIndex`` contract.  The worker thread is disabled.  No
production data, network, packages/dist/config or credentials are touched, and no
external model download is performed.
"""
import hashlib
import math
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import faiss
import numpy as np

from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_knowledge import (
    DIMENSIONS,
    MODE,
    MODEL_NAME,
    KnowledgeBase,
    check_actor,
    company_question,
    origin,
    sensitive_document,
)


def actor(uid="u1", *, admin=False, guest=False, role=None, name="用户"):
    role = role or ("guest" if guest else ("admin" if admin else "user"))
    return {"id": uid, "is_admin": admin, "is_guest": guest, "role": role, "name": name}


def _protect(value):
    return "CIPHER:" + value


def _unprotect(value):
    return value[len("CIPHER:"):] if isinstance(value, str) and value.startswith("CIPHER:") else value


class FakeEmbedder:
    """Deterministic, injectable, non-network embedder (serialized embedding)."""

    def __init__(self, dim=DIMENSIONS):
        self.dim = dim
        self.calls = []
        self.fail = False
        self.fail_message = "模拟嵌入失败"
        self.closed = False
        self._active = 0
        self.max_active = 0
        self.query_flags = []

    def vector_for(self, text):
        digest = hashlib.sha256(text.encode("utf-8")).digest()
        vec = [(digest[i % len(digest)] / 255.0) * 2 - 1 for i in range(self.dim)]
        norm = math.sqrt(sum(v * v for v in vec)) or 1.0
        return [v / norm for v in vec]

    def embed(self, texts, config=None, *, query=False):
        self.calls.append((list(texts), dict(config or {}), query))
        self.query_flags.append(query)
        self._active += 1
        self.max_active = max(self.max_active, self._active)
        try:
            if self.fail:
                raise AssistantError(self.fail_message, 503)
            return [self.vector_for(text) for text in texts]
        finally:
            self._active -= 1

    def close(self):
        self.closed = True


class FakeMemoryIndex:
    """Real FAISS cosine index implementing the local ``MemoryIndex`` contract."""

    def __init__(self):
        self._revision = None
        self._ids = []
        self._index = None
        self._builder_ids = None
        self._builder_vectors = None

    @property
    def revision(self):
        return self._revision or 0

    @property
    def count(self):
        return len(self._ids)

    def _build(self, revision, arrays, ids):
        self._revision = int(revision)
        self._ids = ids
        self._index = None
        if arrays:
            index = faiss.IndexFlatIP(DIMENSIONS)
            index.add(np.stack(arrays).astype("float32"))
            self._index = index

    def begin_build(self, revision):
        self._builder_ids = []
        self._builder_vectors = []
        self._builder_revision = int(revision)

    def add_batch(self, rows):
        batch_arrays, batch_ids = [], []
        for chunk_id, blob in list(rows):
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
        self._build(self._builder_revision, self._builder_vectors, list(self._builder_ids))
        self._builder_ids = self._builder_vectors = None

    def cancel_build(self):
        self._builder_ids = self._builder_vectors = None

    def replace(self, revision, rows):
        arrays, ids = [], []
        for chunk_id, blob in rows:
            arr = np.frombuffer(blob, dtype="<f4")
            if arr.size != DIMENSIONS:
                raise ValueError("bad embedding dimension")
            norm = np.linalg.norm(arr)
            if norm <= 0 or not np.isfinite(norm):
                raise ValueError("bad embedding norm")
            arrays.append((arr / norm).astype("float32"))
            ids.append(int(chunk_id))
        self._build(revision, arrays, ids)

    def search(self, vector, k=40):
        if self._index is None or not self._ids:
            return []
        q = np.asarray(vector, dtype="float32")
        norm = np.linalg.norm(q)
        if norm <= 0 or not np.isfinite(norm):
            return []
        scores, idx = self._index.search((q / norm).astype("float32").reshape(1, -1), min(k, len(self._ids)))
        return [(self._ids[int(i)], float(s)) for i, s in zip(idx[0], scores[0]) if i >= 0]


def _text(content, name="说明.txt"):
    return name, content.encode("utf-8")


class KnowledgeBaseTestCase(unittest.TestCase):
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

    def _save_settings(self, origins=("https://llm.example.com",)):
        return self.kb.save_settings(actor(admin=True), {"approved_origins": list(origins)})

    def _index(self, document_id, embedder=None):
        embedder = embedder or self.embedder
        self.assertTrue(self.kb.process_one())
        return document_id


class CompanyQuestionTests(unittest.TestCase):
    def test_excludes_weather_live_business_even_with_company(self):
        pos = ("我们公司的报销流程是什么", "公司内部制度有哪些", "知识库里的内容", "公司出差流程是什么", "公司的年假怎么请")
        for q in pos:
            with self.subTest(q=q):
                self.assertTrue(company_question(q))
        neg = ("今天公司天气如何", "咱们公司今天有几条通告", "公司里待发通告有几条",
               "今天公司维修工单有几条", "公司里维修任务进度", "灯塔怎么操作",
               "请查询外部网站", "翻译一段话")
        for q in neg:
            with self.subTest(q=q):
                self.assertFalse(company_question(q))


class OriginTests(unittest.TestCase):
    def test_valid_origins_normalize(self):
        self.assertEqual(origin("https://a.example.com/v1/embeddings"), "https://a.example.com")
        self.assertEqual(origin("https://a.example.com:8080/v1/embeddings"), "https://a.example.com:8080")
        self.assertEqual(origin("https://a.example.com:443/v1/embeddings"), "https://a.example.com")

    def test_invalid_origins_rejected(self):
        for bad in (
            "http://a.example.com/v1/embeddings",
            "https://localhost/v1/embeddings",
            "https://127.0.0.1/v1/embeddings",
            "https://a.example.com/v1/embeddings?key=1",
            "https://a.example.com/v1/embeddings#frag",
            "https://user:pass@a.example.com/v1/embeddings",
            "https://a example.com/v1/embeddings",
            "https://169.254.1.1/v1/embeddings",
            "https://0.0.0.0/v1/embeddings",
        ):
            with self.subTest(url=bad):
                with self.assertRaises(AssistantError):
                    origin(bad)


class SensitiveDocumentTests(unittest.TestCase):
    def test_patterns(self):
        self.assertTrue(sensitive_document("身份证 110105199003071234"))
        self.assertTrue(sensitive_document("sk-abcdefghijklmn"))
        self.assertTrue(sensitive_document("请联系 13800138000"))
        self.assertFalse(sensitive_document("普通公司报销流程说明"))


class SettingsTests(KnowledgeBaseTestCase):
    def test_admin_only_and_no_remote_credentials_stored(self):
        with self.assertRaises(AssistantError) as ctx:
            self.kb.save_settings(actor(admin=False), {"approved_origins": ["https://llm.example.com"]})
        self.assertEqual(ctx.exception.status, 403)

        result = self._save_settings()
        self.assertTrue(result["configured"])
        self.assertEqual(result["engine"], MODE)
        self.assertEqual(result["model"], MODEL_NAME)
        self.assertEqual(result["dimensions"], DIMENSIONS)
        self.assertEqual(result["approved_origins"], ["https://llm.example.com"])
        self.assertNotIn("key_cipher", result)
        self.assertNotIn("api_key", result)
        self.assertNotIn("endpoint", result)

        stored = self.kb.configuration()
        self.assertEqual(stored["mode"], MODE)
        self.assertNotIn("key_cipher", stored)
        self.assertNotIn("endpoint", stored)

    def test_legacy_remote_payload_accepted_but_credentials_never_used(self):
        result = self.kb.save_settings(actor(admin=True), {
            "endpoint": "https://embed.example.com/v1/embeddings",
            "model": "e-model",
            "api_key": "sk-secret",
            "approved_origins": ["https://llm.example.com"],
        })
        self.assertEqual(result["mode"], MODE)
        self.assertEqual(result["model"], MODEL_NAME)
        stored = self.kb.configuration()
        self.assertNotIn("key_cipher", stored)
        self.assertNotIn("endpoint", stored)
        self.assertNotIn("model", stored)

    def test_unsupported_mode_rejected(self):
        with self.assertRaises(AssistantError):
            self.kb.save_settings(actor(admin=True), {
                "mode": "remote_vec", "approved_origins": ["https://llm.example.com"]})

    def test_origins_validation(self):
        with self.assertRaises(AssistantError):
            self.kb.save_settings(actor(admin=True), {"approved_origins": []})
        with self.assertRaises(AssistantError):
            self.kb.save_settings(actor(admin=True), {"approved_origins": ["http://llm.example.com"]})

    def test_guest_cannot_read_or_write_settings(self):
        for g in (actor(guest=True), actor(role="guest")):
            with self.subTest(guest=g):
                with self.assertRaises(AssistantError) as ctx:
                    self.kb.settings(g)
                self.assertEqual(ctx.exception.status, 403)
                with self.assertRaises(AssistantError) as ctx:
                    self.kb.save_settings(g, {})
                self.assertEqual(ctx.exception.status, 403)


class UploadIndexSearchTests(KnowledgeBaseTestCase):
    def test_upload_index_search_real_vec(self):
        self._save_settings()
        name, content = _text("公司报销流程\n\n差旅报销需在三天内提交\n\n考勤制度", "报销.txt")
        result = self.kb.upload(actor(admin=True), name, content)
        doc_id = result["id"]
        self._index(doc_id)

        with self.kb.connect() as db:
            row = db.execute(
                "SELECT active_version,pending_version,status,chunks FROM documents WHERE id=?",
                (doc_id,)).fetchone()
            vec_count = db.execute(
                "SELECT count(*) FROM local_vectors lv JOIN chunks c ON c.id=lv.chunk_id WHERE c.document_id=?",
                (doc_id,)).fetchone()[0]
        self.assertIsNotNone(row["active_version"])
        self.assertIsNone(row["pending_version"])
        self.assertEqual(row["status"], "ready")
        self.assertGreaterEqual(row["chunks"], 1)
        self.assertEqual(vec_count, row["chunks"])

        search = self.kb.search(actor(), "公司报销流程",
                                profile={"endpoint": "https://llm.example.com"})
        self.assertEqual(search["mode"], MODE)
        self.assertTrue(search["items"])
        self.assertIn("vector", search["items"][0]["matches"])
        self.assertEqual(search["items"][0]["document_id"], doc_id)

    def test_search_requires_authorized_model(self):
        self._save_settings()
        name, content = _text("公司报销流程\n\n差旅说明", "报销.txt")
        doc_id = self._index(self.kb.upload(actor(admin=True), name, content)["id"])
        with self.assertRaises(AssistantError) as ctx:
            self.kb.search(actor(), "公司报销流程", profile={"endpoint": "https://evil.example.com"})
        self.assertEqual(ctx.exception.status, 403)
        # A signed-in user without a restricted profile can still search.
        search = self.kb.search(actor(), "公司报销流程")
        self.assertEqual(search["mode"], MODE)
        self.assertEqual(search["items"][0]["document_id"], doc_id)

    def test_search_embedder_failure_falls_back_to_keyword(self):
        self._save_settings()
        doc_id = self.kb.upload(actor(admin=True), *_text("公司报销流程", "报销.txt"))["id"]
        # index with working embedder
        self._index(doc_id)
        self.embedder.fail = True
        search = self.kb.search(actor(), "公司报销流程")
        self.assertEqual(search["mode"], "keyword")
        self.assertIn("仅使用关键词", search["warning"])
        self.assertEqual(search["items"][0]["document_id"], doc_id)


class AtomicUpdateTests(KnowledgeBaseTestCase):
    def test_failed_first_embedding_not_ready(self):
        self._save_settings()
        self.embedder.fail = True
        doc = self.kb.upload(actor(admin=True), *_text("公司报销流程", "报销.txt"))
        self.assertTrue(self.kb.process_one())
        with self.kb.connect() as db:
            row = db.execute(
                "SELECT status,active_version,pending_version,error FROM documents WHERE id=?",
                (doc["id"],)).fetchone()
        self.assertEqual(row["status"], "failed")
        self.assertIsNone(row["active_version"])
        self.assertIsNotNone(row["pending_version"])
        # no ready content exists, so search returns emptiness
        self.assertEqual(self.kb.search(actor(), "公司报销流程")["items"], [])

    def test_old_stays_ready_until_new_succeeds(self):
        self._save_settings()
        admin = actor(admin=True)
        v1 = self.kb.upload(admin, *_text("公司报销流程第一版", "报销.txt"))
        self._index(v1["id"])
        self.assertEqual(v1["version"], 1)

        v2 = self.kb.upload(admin, *_text("公司报销流程第二版", "报销.txt"),
                            document_id=v1["id"], revision=v1["version"])
        self.assertEqual(v2["version"], 2)
        # pending but not processed -> old version still searchable
        old = self.kb.search(actor(), "公司报销流程第一版")
        self.assertEqual(old["items"][0]["document_id"], v1["id"])
        self.assertEqual(old["items"][0]["version"], 1)

        # force failure on next index
        self.embedder.fail = True
        self.assertTrue(self.kb.process_one())
        with self.kb.connect() as db:
            row = db.execute(
                "SELECT active_version,pending_version,status,revision FROM documents WHERE id=?", (v1["id"],)).fetchone()
        self.assertEqual(row["status"], "failed")
        self.assertEqual(row["active_version"], 1)
        self.assertEqual(row["pending_version"], 2)
        # old data still available and searchable
        old = self.kb.search(actor(), "公司报销流程第一版")
        self.assertEqual(old["items"][0]["version"], 1)

        # retry after fixing embedder commits v2
        self.embedder.fail = False
        self.kb.change(admin, v1["id"], "retry", revision=row["revision"])
        self.assertTrue(self.kb.process_one())
        with self.kb.connect() as db:
            row = db.execute(
                "SELECT active_version,pending_version,status FROM documents WHERE id=?", (v1["id"],)).fetchone()
        self.assertEqual(row["status"], "ready")
        self.assertEqual(row["active_version"], 2)
        self.assertIsNone(row["pending_version"])
        new = self.kb.search(actor(), "公司报销流程第二版")
        self.assertEqual(new["items"][0]["version"], 2)


class DeletePermissionsTests(KnowledgeBaseTestCase):
    def test_delete_suppresses_search_and_evidence_immediately(self):
        self._save_settings()
        admin = actor(admin=True)
        doc_id = self._index(self.kb.upload(admin, *_text("公司报销流程", "报销.txt"))["id"])
        with self.kb.connect() as db:
            version = db.execute(
                "SELECT active_version FROM documents WHERE id=?", (doc_id,)).fetchone()["active_version"]

        profile = {"endpoint": "https://llm.example.com"}
        item = {"document_id": doc_id, "version": version}
        self.assertTrue(self.kb.evidence_current([item], profile))

        self.kb.change(admin, doc_id, "delete", revision=1)
        # vectors removed immediately
        with self.kb.connect() as db:
            count = db.execute(
                "SELECT count(*) FROM local_vectors lv JOIN chunks c ON c.id=lv.chunk_id WHERE c.document_id=?",
                (doc_id,)).fetchone()[0]
        self.assertEqual(count, 0)
        # citations invalidated
        self.assertFalse(self.kb.evidence_current([item], profile))
        # search suppressed immediately
        search = self.kb.search(actor(), "公司报销流程")
        self.assertEqual(search["items"], [])

    def test_owner_admin_replace_delete_but_other_user_shared_read_only(self):
        self._save_settings()
        owner = actor("alice", name="Alice")
        other = actor("bob", name="Bob")
        admin = actor(admin=True, name="Admin")
        guest = actor(guest=True)

        doc_id = self._index(self.kb.upload(owner, *_text("公司报销流程", "报销.txt"))["id"])
        with self.kb.connect() as db:
            row = db.execute(
                "SELECT active_version,revision FROM documents WHERE id=?", (doc_id,)).fetchone()

        # other signed-in user can read (shared)
        self.assertEqual(self.kb.list(other, {"page": 1})["items"][0]["id"], doc_id)
        self.assertFalse(self.kb.list(other, {"page": 1})["items"][0]["can_edit"])
        self.assertEqual(self.kb.document(other, doc_id, {"page": 1})["document"]["id"], doc_id)
        self.assertEqual(self.kb.search(other, "公司报销流程")["items"][0]["document_id"], doc_id)

        # other user cannot replace or delete
        with self.assertRaises(AssistantError) as ctx:
            self.kb.upload(other, *_text("公司报销流程改动", "报销.txt"),
                           document_id=doc_id, revision=row["revision"])
        self.assertEqual(ctx.exception.status, 403)
        with self.assertRaises(AssistantError) as ctx:
            self.kb.change(other, doc_id, "delete", revision=row["revision"])
        self.assertEqual(ctx.exception.status, 403)

        # guests denied everywhere
        for method in (lambda: self.kb.list(guest, {"page": 1}),
                       lambda: self.kb.upload(guest, *_text("任何人", "报销.txt")),
                       lambda: self.kb.search(guest, "公司报销流程"),
                       lambda: self.kb.document(guest, doc_id, {"page": 1}),
                       lambda: self.kb.settings(guest),
                       lambda: self.kb.save_settings(guest, {})):
            with self.subTest():
                with self.assertRaises(AssistantError) as ctx:
                    method()
                self.assertEqual(ctx.exception.status, 403)

        # owner replace then admin delete works
        self.kb.upload(owner, *_text("公司报销流程第二版", "报销.txt"),
                       document_id=doc_id, revision=row["revision"])
        with self.kb.connect() as db:
            latest = db.execute(
                "SELECT revision FROM documents WHERE id=?", (doc_id,)).fetchone()["revision"]
        self.kb.change(admin, doc_id, "delete", revision=latest)
        self.assertEqual(self.kb.list(admin, {"deleted": "1"})["items"][0]["id"], doc_id)


class DuplicateAndRevisionTests(KnowledgeBaseTestCase):
    def test_duplicate_content(self):
        self._save_settings()
        admin = actor(admin=True)
        first = self.kb.upload(admin, *_text("公司报销流程", "报销.txt"))
        dup = self.kb.upload(admin, *_text("公司报销流程", "报销.txt"))
        self.assertTrue(dup["duplicate"])
        self.assertEqual(dup["id"], first["id"])

    def test_stale_revision_conflicts(self):
        self._save_settings()
        admin = actor(admin=True)
        v1 = self.kb.upload(admin, *_text("公司报销流程第一版", "报销.txt"))
        self._index(v1["id"])
        v2 = self.kb.upload(admin, *_text("公司报销流程第二版", "报销.txt"),
                            document_id=v1["id"], revision=v1["version"])
        # stale revision no longer matches current revision
        with self.assertRaises(AssistantError) as ctx:
            self.kb.upload(admin, *_text("公司报销流程第三版", "报销.txt"),
                           document_id=v1["id"], revision=v1["version"])
        self.assertEqual(ctx.exception.status, 409)
        with self.assertRaises(AssistantError) as ctx:
            self.kb.change(admin, v1["id"], "delete", revision=v1["version"])
        self.assertEqual(ctx.exception.status, 409)


class SensitiveBlockTests(KnowledgeBaseTestCase):
    def test_sensitive_document_blocked_before_embed(self):
        self._save_settings()
        self.embedder.calls.clear()
        name, content = _text("员工名单\n\n身份证号 110105199003071234", "名单.txt")
        doc = self.kb.upload(actor(admin=True), name, content)
        self.assertTrue(self.kb.process_one())
        # embedder never called during indexing for blocked documents
        self.assertEqual(self.embedder.calls, [])
        with self.kb.connect() as db:
            row = db.execute(
                "SELECT status,active_version,error FROM documents WHERE id=?", (doc["id"],)).fetchone()
        self.assertEqual(row["status"], "blocked")
        self.assertIsNone(row["active_version"])
        self.assertEqual(row["error"], "文件包含不宜公开的信息，未入库。")
        # not searchable
        self.assertEqual(self.kb.search(actor(), "员工名单")["items"], [])

    def test_sensitive_filename_rejected(self):
        with self.assertRaises(AssistantError):
            self.kb.upload(actor(), "sk-abcdefghijklmn.txt", b"x" * 10)


class CheckActorTests(unittest.TestCase):
    def test_check_actor(self):
        check_actor(actor("u"))
        check_actor(actor("u", admin=True))
        for g in (actor(guest=True), actor(role="guest")):
            with self.assertRaises(AssistantError) as ctx:
                check_actor(g)
            self.assertEqual(ctx.exception.status, 403)
        with self.assertRaises(AssistantError):
            check_actor({})
        with self.assertRaises(AssistantError):
            check_actor({"id": ""})
        with self.assertRaises(AssistantError) as ctx:
            check_actor(actor("u"), admin=True)
        self.assertEqual(ctx.exception.status, 403)


if __name__ == "__main__":
    unittest.main(verbosity=2)
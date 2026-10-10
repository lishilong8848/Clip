"""Isolation tests (unittest only) for assistant.lighthouse_knowledge.

Everything runs against a temporary SQLite database with the real sqlite_vec
extension and a fake deterministic embedder; worker thread is disabled.  No
production data, network, packages/dist/config or credentials are touched.
"""
import hashlib
import math
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_knowledge import (
    Embeddings,
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
    """Deterministic, injectable, non-network embedder."""

    def __init__(self, dim=8):
        self.dim = dim
        self.calls = []
        self.fail = False
        self.fail_message = "模拟嵌入失败"
        self.closed = False

    def vector_for(self, text):
        digest = hashlib.sha256(text.encode("utf-8")).digest()
        vec = [(digest[i] / 255.0) * 2 - 1 for i in range(self.dim)]
        norm = math.sqrt(sum(v * v for v in vec)) or 1.0
        return [v / norm for v in vec]

    def embed(self, texts, config):
        self.calls.append((list(texts), dict(config)))
        if self.fail:
            raise AssistantError(self.fail_message, 503)
        return [self.vector_for(text) for text in texts]

    def close(self):
        self.closed = True


class FakeClient:
    def __init__(self, payload=None, exception=None):
        self.payload = payload
        self.exception = exception
        self.calls = []
        self.closed = False

    def request_json(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if self.exception is not None:
            raise self.exception
        return self.payload

    def close(self):
        self.closed = True


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
        )

    def _save_settings(self, endpoint="https://embed.example.com/v1/embeddings",
                       model="e-model", api_key="sk-secret", origins=("https://llm.example.com",)):
        return self.kb.save_settings(
            actor(admin=True),
            {"endpoint": endpoint, "model": model, "api_key": api_key,
             "approved_origins": list(origins)},
        )

    def _index(self, document_id, embedder=None):
        embedder = embedder or self.embedder
        self.assertTrue(self.kb.process_one())
        return document_id


class CompanyQuestionTests(unittest.TestCase):
    def test_excludes_weather_live_business_even_with_company(self):
        pos = ("我们公司的报销流程是什么", "公司内部制度有哪些", "知识库里的内容")
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
    def test_admin_only_and_key_redaction(self):
        with self.assertRaises(AssistantError) as ctx:
            self.kb.save_settings(actor(admin=False), {
                "endpoint": "https://embed.example.com/v1/embeddings",
                "model": "m", "api_key": "sk-a", "approved_origins": ["https://llm.example.com"]})
        self.assertEqual(ctx.exception.status, 403)

        result = self._save_settings()
        self.assertTrue(result["configured"])
        self.assertEqual(result["endpoint"], "https://embed.example.com/v1/embeddings")
        self.assertEqual(result["model"], "e-model")
        self.assertEqual(result["dimensions"], self.embedder.dim)
        self.assertEqual(result["approved_origins"], ["https://llm.example.com"])
        self.assertNotIn("key_cipher", result)
        self.assertNotIn("api_key", result)

        stored = self.kb.configuration()
        self.assertTrue(stored["key_cipher"].startswith("CIPHER:"))
        self.assertEqual(stored["key_cipher"], "CIPHER:sk-secret")

    def test_endpoint_must_be_https_embeddings(self):
        for endpoint in ("http://embed.example.com/v1/embeddings",
                         "https://embed.example.com/v1/chat/completions",
                         "https://embed.example.com/not-embeddings"):
            with self.subTest(endpoint=endpoint):
                with self.assertRaises(AssistantError):
                    self._save_settings(endpoint=endpoint)

    def test_origins_validation(self):
        with self.assertRaises(AssistantError):
            self.kb.save_settings(actor(admin=True), {
                "endpoint": "https://embed.example.com/v1/embeddings",
                "model": "m", "api_key": "sk-a", "approved_origins": []})
        with self.assertRaises(AssistantError):
            self.kb.save_settings(actor(admin=True), {
                "endpoint": "https://embed.example.com/v1/embeddings",
                "model": "m", "api_key": "sk-a",
                "approved_origins": ["http://llm.example.com"]})

    def test_changing_endpoint_without_key_rejected(self):
        self._save_settings()
        with self.assertRaises(AssistantError):
            self.kb.save_settings(actor(admin=True), {
                "endpoint": "https://other.example.com/v1/embeddings",
                "model": "e-model", "api_key": "",
                "approved_origins": ["https://llm.example.com"]})

    def test_guest_cannot_read_or_write_settings(self):
        for g in (actor(guest=True), actor(role="guest")):
            with self.subTest(guest=g):
                with self.assertRaises(AssistantError) as ctx:
                    self.kb.settings(g)
                self.assertEqual(ctx.exception.status, 403)
                with self.assertRaises(AssistantError) as ctx:
                    self.kb.save_settings(g, {})
                self.assertEqual(ctx.exception.status, 403)


class EmbedsValidationTests(unittest.TestCase):
    def _embeddings(self, payload=None, exception=None):
        client = FakeClient(payload=payload, exception=exception)
        return client, Embeddings(client=client, decrypt=lambda k: k)

    def test_embed_success_and_header(self):
        client, emb = self._embeddings({"data": [
            {"index": 1, "embedding": [0.0, 1.0]},
            {"index": 0, "embedding": [1.0, 0.0]},
        ]})
        result = emb.embed(["a", "b"], {"key_cipher": "secret", "endpoint": "https://x/v1/embeddings",
                                        "model": "m", "dimensions": 2})
        self.assertEqual(result, [[1.0, 0.0], [0.0, 1.0]])
        method, url, kwargs = client.calls[0]
        self.assertEqual(method, "POST")
        self.assertEqual(kwargs["headers"]["Authorization"], "Bearer secret")
        self.assertEqual(kwargs["json_payload"]["model"], "m")
        emb.close()
        self.assertTrue(client.closed)

    def test_requires_key_cipher(self):
        _, emb = self._embeddings()
        with self.assertRaises(AssistantError) as ctx:
            emb.embed(["a"], {})
        self.assertEqual(ctx.exception.status, 409)

    def test_network_error_maps_to_503(self):
        _, emb = self._embeddings(exception=RuntimeError("boom"))
        with self.assertRaises(AssistantError) as ctx:
            emb.embed(["a"], {"key_cipher": "k"})
        self.assertEqual(ctx.exception.status, 503)

    def _reject(self, payload, *, dims=None, texts=None):
        texts = texts or ["a", "b"]
        config = {"key_cipher": "k", "endpoint": "https://x/v1/embeddings", "model": "m"}
        if dims is not None:
            config["dimensions"] = dims
        _, emb = self._embeddings(payload)
        with self.assertRaises(AssistantError) as ctx:
            emb.embed(texts, config)
        return ctx.exception

    def test_malformed_responses(self):
        cases = [
            ("not a dict", {"data": "nope"}),
            ("wrong item count", {"data": [{"index": 0, "embedding": [1.0]}]}),
            ("index not int", {"data": [
                {"index": "0", "embedding": [1.0]},
                {"index": 1, "embedding": [1.0]},
            ]}),
            ("index out of range", {"data": [
                {"index": 0, "embedding": [1.0]},
                {"index": 5, "embedding": [1.0]},
            ]}),
            ("duplicate index", {"data": [
                {"index": 0, "embedding": [1.0]},
                {"index": 0, "embedding": [1.0]},
            ]}),
            ("vector not list", {"data": [
                {"index": 0, "embedding": "x"},
                {"index": 1, "embedding": [1.0]},
            ]}),
            ("vector too long", {"data": [
                {"index": 0, "embedding": [1.0] * 5000},
                {"index": 1, "embedding": [1.0] * 5000},
            ]}),
            ("vector empty", {"data": [
                {"index": 0, "embedding": []},
                {"index": 1, "embedding": [1.0]},
            ]}),
            ("non finite", {"data": [
                {"index": 0, "embedding": [float("nan")]},
                {"index": 1, "embedding": [1.0]},
            ]}),
            ("zero norm", {"data": [
                {"index": 0, "embedding": [0.0, 0.0]},
                {"index": 1, "embedding": [1.0, 0.0]},
            ]}),
        ]
        for label, payload in cases:
            with self.subTest(label=label):
                self.assertEqual(self._reject(payload).status, 502)

    def test_mixed_dims_raises_409(self):
        exc = self._reject({"data": [
            {"index": 0, "embedding": [1.0, 0.0]},
            {"index": 1, "embedding": [1.0, 0.0, 0.0]},
        ]})
        self.assertEqual(exc.status, 409)

    def test_dimension_mismatch_raises_409(self):
        exc = self._reject({"data": [
            {"index": 0, "embedding": [1.0, 0.0]},
            {"index": 1, "embedding": [0.0, 1.0]},
        ]}, dims=5)
        self.assertEqual(exc.status, 409)

    def test_empty_input_returns_without_client(self):
        client, emb = self._embeddings()
        self.assertEqual(emb.embed([], {"key_cipher": "k"}), [])
        self.assertEqual(client.calls, [])


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
        self.assertIsNotNone(row["active_version"])
        self.assertIsNone(row["pending_version"])
        self.assertEqual(row["status"], "ready")
        self.assertGreaterEqual(row["chunks"], 1)

        search = self.kb.search(actor(), "公司报销流程",
                                profile={"endpoint": "https://llm.example.com"})
        self.assertEqual(search["mode"], "hybrid")
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
        # Keyword fallback still works for a signed-in user without profile when embed available.
        search = self.kb.search(actor(), "公司报销流程")
        self.assertEqual(search["mode"], "hybrid")
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
            self.kb._vectors(db)
            count = db.execute("SELECT count(*) FROM vectors").fetchone()[0]
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
    def test_sensitive_document_blocked_before_external_embed(self):
        self._save_settings()
        self.embedder.calls.clear()
        name, content = _text("员工名单\n\n身份证号 110105199003071234", "名单.txt")
        doc = self.kb.upload(actor(admin=True), name, content)
        self.assertTrue(self.kb.process_one())
        # external embed never called during indexing
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
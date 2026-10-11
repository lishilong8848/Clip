"""Isolation tests (unittest only) for assistant.lighthouse_knowledge_local.

Covers the bounded embedding runtime with a fake model factory (no real model,
no network, no production data) plus real FAISS MemoryIndex read-concurrency
and snapshot-swap consistency when faiss is installed.  All times are bounded.
"""
import hashlib
import json
import math
import struct
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_knowledge_local import (
    DIMENSIONS,
    MemoryIndex,
    MODEL_NAME,
    MODE,
    LocalEmbeddings,
    _QUERY_INSTRUCTION,
)
import openclaw_service.assistant.lighthouse_knowledge_local as local_mod


def _faiss_available():
    try:
        import faiss  # noqa: F401
        return True
    except Exception:
        return False


_HAS_FAISS = _faiss_available()


class FakeModel:
    """Deterministic injectable model; tracks concurrency and optional blocking."""

    def __init__(self, dim=DIMENSIONS):
        self.dim = dim
        self.calls = []
        self.emitted = []
        self.active = 0
        self.max_active = 0
        self.lock = threading.Lock()
        self.block_first_embed = False
        self.first_embed_entered = threading.Event()
        self.release_first_embed = threading.Event()
        self.fail_on_embed = False

    @staticmethod
    def vector_for(text, dim=DIMENSIONS):
        digest = hashlib.sha256(text.encode("utf-8")).digest()
        values = [((digest[i % 32] / 255.0) * 2 - 1) for i in range(dim)]
        norm = math.sqrt(sum(v * v for v in values)) or 1.0
        return [v / norm for v in values]

    def embed(self, documents, batch_size=None, parallel=None):
        if isinstance(documents, str):
            documents = [documents]
        documents = list(documents)
        with self.lock:
            self.active += 1
            if self.active > self.max_active:
                self.max_active = self.active
        try:
            if self.fail_on_embed:
                raise AssistantError("模拟嵌入失败", 503)
            if self.block_first_embed:
                self.block_first_embed = False
                self.first_embed_entered.set()
                if not self.release_first_embed.wait(10):
                    raise AssistantError("测试阻塞超时", 503)
            vectors = [self.vector_for(text, self.dim) for text in documents]
            with self.lock:
                self.calls.append(list(documents))
                self.emitted.append([list(v) for v in vectors])
            return vectors
        finally:
            with self.lock:
                self.active -= 1


class _Base(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.fake = FakeModel()

    def make(self, **kwargs):
        kwargs.setdefault("model_factory", lambda: self.fake)
        kwargs.setdefault("query_timeout", 30.0)
        kwargs.setdefault("doc_timeout", 30.0)
        return LocalEmbeddings(self._tmp.name, **kwargs)


class ModuleAndLazyTests(_Base):
    def test_constants(self):
        self.assertEqual(MODEL_NAME, "BAAI/bge-small-zh-v1.5")
        self.assertEqual(DIMENSIONS, 512)
        self.assertEqual(MODE, "local_faiss")
        self.assertEqual(local_mod.MEMORY_BUDGET_BYTES, 2 * 1024**3)

    def test_import_does_not_load_optional_deps(self):
        # The module must not pull in fastembed or faiss at import/constructor.
        # Run in a clean subprocess (this test interpreter may already have
        # loaded faiss for the MemoryIndex suite).
        import subprocess
        import sys as _sys

        base = str(Path(__file__).resolve().parent)
        code = (
            "import sys, tempfile\n"
            "sys.path.insert(0, r'%s')\n"
            "from openclaw_service.assistant.lighthouse_knowledge_local import LocalEmbeddings\n"
            "with tempfile.TemporaryDirectory() as td:\n"
            "    LocalEmbeddings(td, start_worker=True)\n"
            "assert 'fastembed' not in sys.modules, 'fastembed imported'\n"
            "assert 'faiss' not in sys.modules, 'faiss imported'\n"
            "print('clean')\n"
        ) % base
        completed = subprocess.run(
            [_sys.executable, "-c", code],
            capture_output=True,
            text=True,
            timeout=120,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("clean", completed.stdout)

    def test_status_json_safe(self):
        emb = self.make()
        status = emb.status()
        json.dumps(status)  # must not raise
        self.assertEqual(status["model"], MODEL_NAME)
        self.assertEqual(status["dimensions"], DIMENSIONS)
        self.assertEqual(status["mode"], MODE)
        # No filesystem paths are exposed to ordinary clients.
        self.assertNotIn("model_dir", status)
        self.assertNotIn("model_dir", "\n".join(sorted(map(str, status.values()))))
        emb.close()


class QueryCachingTests(_Base):
    def test_singleflight_many_identical_queries_embed_once(self):
        emb = self.make()
        question = "公司报销流程是什么"
        n = 40
        results = [None] * n
        errors = []

        def runner(i):
            try:
                results[i] = emb.embed([question], query=True)
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=runner, args=(i,)) for i in range(n)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(20)
        self.assertEqual(errors, [])
        for result in results:
            self.assertEqual(len(result), 1)
            self.assertEqual(len(result[0]), DIMENSIONS)
        doc_calls = [call for call in self.fake.calls if not call[0].startswith(_QUERY_INSTRUCTION)]
        query_calls = [call for call in self.fake.calls if call and call[0].startswith(_QUERY_INSTRUCTION)]
        self.assertEqual(len(query_calls), 1)
        self.assertEqual(doc_calls, [])
        emb.close()

    def test_lru_recheck_inside_registry_lock_avoids_race_duplicate(self):
        # The LRU lookup must also happen under the registry lock so that a
        # caller arriving right after the first single-flight job finished and
        # populated the cache does not create a second embedding.
        emb = self.make()
        question = "竞态检查问题"
        first = emb.embed([question], query=True)
        self.assertEqual(len(self.fake.calls), 1)
        # Immediately re-embed from many threads; the cache is already warm so
        # only the initial call should have run.
        n = 30
        errors = []

        def runner(i):
            try:
                emb.embed([question], query=True)
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=runner, args=(i,)) for i in range(n)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
        self.assertEqual(errors, [])
        self.assertEqual(len(self.fake.calls), 1)
        emb.close()

    def test_query_lru_reuse_and_immutable_copy(self):
        emb = self.make()
        first = emb.embed(["重复问题"], query=True)
        self.assertEqual(len(self.fake.calls), 1)
        self.assertTrue(self.fake.calls[0][0].startswith(_QUERY_INSTRUCTION))
        second = emb.embed(["重复问题"], query=True)
        self.assertEqual(len(self.fake.calls), 1)  # served from LRU
        # returned lists are immutable copies
        before = list(second[0])
        second[0][0] = 12345.0
        self.assertNotEqual(second[0][0], first[0][0])
        self.assertEqual(first[0][0], before[0])
        emb.close()


class ConcurrencySerializationTests(_Base):
    def test_single_active_embed_across_many_callers(self):
        emb = self.make()
        results = []
        errors = []
        stop = threading.Event()

        def runner(i):
            while not stop.is_set():
                try:
                    if i % 2 == 0:
                        out = emb.embed(["文档文本%d" % i])
                    else:
                        out = emb.embed(["查询文本%d" % i], query=True)
                    results.append(out)
                    break
                except AssistantError:
                    break
                except Exception as exc:  # pragma: no cover
                    errors.append(exc)
                    break

        threads = [threading.Thread(target=runner, args=(i,)) for i in range(20)]
        started = time.monotonic()
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(20)
        stop.set()
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 20)
        # Model execution is serialized through a single worker: never >1 active.
        self.assertEqual(self.fake.max_active, 1)
        self.assertLess(time.monotonic() - started, 20)
        emb.close()

    def test_query_priority_over_document_and_batch_yield(self):
        emb = self.make()
        self.fake.block_first_embed = True
        doc_result = []
        query_result = []
        errors = []

        def doc_worker():
            try:
                doc_result.append(emb.embed(["chunk%d" % i for i in range(16)]))
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        def query_worker():
            try:
                query_result.append(emb.embed(["优先问题"], query=True))
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        dt = threading.Thread(target=doc_worker)
        dt.start()
        self.assertTrue(self.fake.first_embed_entered.wait(5))
        qt = threading.Thread(target=query_worker)
        qt.start()
        time.sleep(0.2)  # let the query be queued while doc is blocked
        self.fake.release_first_embed.set()
        dt.join(15)
        qt.join(15)
        self.assertEqual(errors, [])
        self.assertEqual(len(doc_result), 1)
        self.assertEqual(len(query_result), 1)
        # 16 chunks -> 2 doc sub-batches of 8, with the query served between them.
        self.assertEqual(len(self.fake.calls), 3)
        doc_calls = [call for call in self.fake.calls if call and not call[0].startswith(_QUERY_INSTRUCTION)]
        query_calls = [call for call in self.fake.calls if call and call[0].startswith(_QUERY_INSTRUCTION)]
        self.assertEqual(len(doc_calls), 2)
        self.assertEqual(len(query_calls), 1)
        self.assertEqual(doc_calls[0], ["chunk%d" % i for i in range(8)])
        self.assertEqual(doc_calls[1], ["chunk%d" % i for i in range(8, 16)])
        self.assertEqual(query_calls[0], [_QUERY_INSTRUCTION + "优先问题"])
        self.assertEqual(len(doc_result[0]), 16)
        emb.close()

    def test_document_batches_split_at_eight(self):
        emb = self.make()
        result = emb.embed(["item%d" % i for i in range(20)])
        self.assertEqual(len(result), 20)
        doc_calls = [call for call in self.fake.calls if call and not call[0].startswith(_QUERY_INSTRUCTION)]
        self.assertEqual(len(doc_calls), 3)
        self.assertEqual([len(call) for call in doc_calls], [8, 8, 4])
        emb.close()


class BoundedQueueTests(_Base):
    def test_timeout_queued_query_is_dropped_not_stuck(self):
        queue_max = 4
        emb = LocalEmbeddings(
            self._tmp.name,
            model_factory=lambda: self.fake,
            start_worker=False,
            queue_max=queue_max,
            query_timeout=0.2,
            doc_timeout=0.2,
        )
        outputs = [None] * queue_max
        errors = []

        def runner(i):
            try:
                outputs[i] = emb.embed(["查询%d" % i], query=True)
            except AssistantError as exc:
                errors.append(exc)
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=runner, args=(i,)) for i in range(queue_max)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(5)
        # Each unique waiter timed out; its queued job was dropped.
        self.assertEqual(len(errors), queue_max)
        self.assertEqual(emb.status()["queued"], 0)
        emb.close()

    def test_document_queue_full_fails_promptly(self):
        emb = LocalEmbeddings(
            self._tmp.name,
            model_factory=lambda: self.fake,
            start_worker=False,
            queue_max=1,
            query_timeout=0.2,
            doc_timeout=0.2,
        )
        # First doc job fills the queue and times out waiting.
        with self.assertRaises(AssistantError):
            emb.embed(["occupying"])
        with self.assertRaises(AssistantError) as ctx:
            emb.embed(["another"])
        self.assertEqual(ctx.exception.status, 503)
        emb.close()


class ModelRetryTests(_Base):
    def test_model_load_cooldown_prevents_per_query_redownload(self):
        factory_calls = []

        def failing_factory():
            factory_calls.append(1)
            raise RuntimeError("模拟模型加载失败")

        emb = LocalEmbeddings(
            self._tmp.name,
            model_factory=failing_factory,
            start_worker=True,
            model_retry_cooldown=5.0,
            query_timeout=5.0,
            doc_timeout=5.0,
        )
        with self.assertRaises(AssistantError) as ctx:
            emb.embed(["第一次"], query=True)
        self.assertEqual(ctx.exception.status, 503)
        self.assertEqual(len(factory_calls), 1)
        # Within the cooldown window a further call must fail with an
        # AssistantError WITHOUT retrying/redownloading the model.
        with self.assertRaises(AssistantError) as ctx:
            emb.embed(["第二次"], query=True)
        self.assertEqual(ctx.exception.status, 503)
        self.assertEqual(len(factory_calls), 1)
        emb.close()

    def test_factory_assistant_error_also_enters_cooldown(self):
        factory_calls = []

        def failing_factory():
            factory_calls.append(1)
            raise AssistantError("模拟模型加载失败", 503)

        emb = LocalEmbeddings(
            self._tmp.name,
            model_factory=failing_factory,
            start_worker=True,
            model_retry_cooldown=5.0,
            query_timeout=5.0,
            doc_timeout=5.0,
        )
        with self.assertRaises(AssistantError) as ctx:
            emb.embed(["第一次"], query=True)
        self.assertEqual(ctx.exception.status, 503)
        self.assertEqual(len(factory_calls), 1)
        # AssistantError is an "actual factory error" and must warm the cooldown.
        with self.assertRaises(AssistantError) as ctx:
            emb.embed(["第二次"], query=True)
        self.assertEqual(ctx.exception.status, 503)
        self.assertEqual(len(factory_calls), 1)
        emb.close()


class LoadBlockingTests(_Base):
    def test_status_nonblocking_while_model_loads(self):
        entered = threading.Event()
        release = threading.Event()

        def blocking_factory():
            entered.set()
            release.wait(15)
            return self.fake

        emb = LocalEmbeddings(
            self._tmp.name,
            model_factory=blocking_factory,
            start_worker=True,
            query_timeout=2.0,
            doc_timeout=2.0,
        )
        t = threading.Thread(target=lambda: emb.embed(["载入中"], query=True))
        t.start()
        self.assertTrue(entered.wait(5))
        started = time.monotonic()
        status = emb.status()
        self.assertLess(time.monotonic() - started, 1.0)
        self.assertFalse(status["ready"])
        self.assertTrue(status["loading"])
        release.set()
        t.join(10)
        emb.close()

    def test_close_prompt_when_blocking_factory(self):
        entered = threading.Event()
        release = threading.Event()

        def blocking_factory():
            entered.set()
            release.wait(30)
            raise RuntimeError("模拟卡住的模型加载")

        emb = LocalEmbeddings(
            self._tmp.name,
            model_factory=blocking_factory,
            start_worker=True,
            query_timeout=5.0,
            doc_timeout=5.0,
        )
        errors = []

        def embed_runner():
            try:
                emb.embed(["x"], query=True)
            except AssistantError as exc:
                errors.append(exc)
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        t = threading.Thread(target=embed_runner)
        t.start()
        self.assertTrue(entered.wait(5))
        started = time.monotonic()
        emb.close(timeout=0.5)
        elapsed = time.monotonic() - started
        # close() must not block unboundedly on a blocking model load.
        self.assertLess(elapsed, 3.0)
        release.set()
        t.join(5)
        # The embed waiter was rejected promptly once close() began.
        self.assertTrue(errors)
        # Worker still owns cleanup; nothing was closed while inference ran.
        self.assertFalse(emb.status()["ready"])


class TimeoutDropTests(_Base):
    def test_timeout_queued_never_runs(self):
        emb = self.make(query_timeout=0.3, doc_timeout=30.0)
        self.fake.block_first_embed = True
        first_result = []

        def first_runner():
            try:
                first_result.append(emb.embed(["阻塞输入"]))
            except Exception as exc:  # pragma: no cover
                first_result.append(exc)

        t = threading.Thread(target=first_runner)
        t.start()
        self.assertTrue(self.fake.first_embed_entered.wait(5))
        # While the worker is blocked, a queued query times out; its last waiter
        # drops it so it is never executed once the worker is released.
        with self.assertRaises(AssistantError) as ctx:
            emb.embed(["超时取消"], query=True)
        self.assertEqual(ctx.exception.status, 503)
        self.assertEqual(emb.status()["queued"], 0)
        self.fake.release_first_embed.set()
        t.join(15)
        query_calls = [call for call in self.fake.calls if call and call[0].startswith(_QUERY_INSTRUCTION)]
        self.assertEqual(query_calls, [])
        emb.close()

    def test_shared_waiters_survive_one_expired_caller(self):
        emb = self.make(query_timeout=5.0, doc_timeout=30.0)
        self.fake.block_first_embed = True
        results, errors = [], []
        first_waiting = threading.Event()
        second_waiting = threading.Event()
        first_expired = threading.Event()
        original_wait = emb._wait_job

        def bounded_wait(job, timeout):
            name = threading.current_thread().name
            if name == 'expires-first':
                first_waiting.set()
                return original_wait(job, 0.5)
            if name == 'still-waiting':
                second_waiting.set()
            return original_wait(job, timeout)

        def runner(query=False):
            try:
                value = emb.embed(["共享问题" if query else "阻塞文档"], query=query)
                results.append((threading.current_thread().name, value))
            except AssistantError as exc:
                errors.append((threading.current_thread().name, exc.status))
            finally:
                if threading.current_thread().name == 'expires-first':
                    first_expired.set()

        with mock.patch.object(emb, '_wait_job', side_effect=bounded_wait):
            document = threading.Thread(target=runner, name='document')
            first = threading.Thread(target=runner, args=(True,), name='expires-first')
            second = threading.Thread(target=runner, args=(True,), name='still-waiting')
            threads = []
            try:
                document.start(); threads.append(document)
                self.assertTrue(self.fake.first_embed_entered.wait(5))
                first.start(); threads.append(first)
                self.assertTrue(first_waiting.wait(5))
                second.start(); threads.append(second)
                self.assertTrue(second_waiting.wait(5))
                self.assertTrue(first_expired.wait(5))
                self.assertEqual(errors, [('expires-first', 503)])
                self.assertEqual(emb.status()['queued'], 1)
            finally:
                self.fake.release_first_embed.set()
                for thread in threads:
                    thread.join(10)
            self.assertEqual({name for name, _ in results}, {'document', 'still-waiting'})
            query_calls = [call for call in self.fake.calls if call[0].startswith(_QUERY_INSTRUCTION)]
            self.assertEqual(len(query_calls), 1)
        emb.close()


class CloseTests(_Base):
    def test_close_rejects_waiters_promptly(self):
        emb = LocalEmbeddings(
            self._tmp.name,
            model_factory=lambda: self.fake,
            start_worker=False,
            query_timeout=30.0,
            doc_timeout=30.0,
        )
        waiter_error = []
        done = threading.Event()

        def waiter():
            try:
                emb.embed(["等待中的问题"], query=True)
            except AssistantError as exc:
                waiter_error.append(exc)
            except Exception as exc:  # pragma: no cover
                waiter_error.append(exc)
            finally:
                done.set()

        thread = threading.Thread(target=waiter)
        thread.start()
        time.sleep(0.2)
        emb.close()
        started = time.monotonic()
        done.wait(5)
        self.assertLess(time.monotonic() - started, 3)
        thread.join(5)
        self.assertTrue(waiter_error)
        self.assertEqual(waiter_error[0].status, 503)

    def test_close_cancels_queued_jobs_without_compute(self):
        emb = self.make()
        self.fake.block_first_embed = True
        first_doc = []
        queued_errors = []
        state_lock = threading.Lock()

        def first_runner():
            try:
                with state_lock:
                    first_doc.append(emb.embed(["第一个文档"]))
            except Exception as exc:  # pragma: no cover
                with state_lock:
                    queued_errors.append(exc)

        def queued_query():
            try:
                emb.embed(["被取消的查询"], query=True)
            except Exception as exc:
                queued_errors.append(exc)

        def queued_doc():
            try:
                emb.embed(["被取消的文档"])
            except Exception as exc:
                queued_errors.append(exc)

        dt = threading.Thread(target=first_runner)
        dt.start()
        self.assertTrue(self.fake.first_embed_entered.wait(5))
        q1 = threading.Thread(target=queued_query)
        q2 = threading.Thread(target=queued_doc)
        q1.start()
        q2.start()
        time.sleep(0.2)  # ensure they logged into the queue

        closed = []
        closer = threading.Thread(target=lambda: (emb.close(), closed.append(True)))
        closer.start()
        time.sleep(0.2)
        self.fake.release_first_embed.set()

        dt.join(15)
        q1.join(15)
        q2.join(15)
        closer.join(15)
        self.assertTrue(closed)
        # close() rejects every waiter, including the in-flight doc's caller.
        self.assertEqual(first_doc, [])
        self.assertEqual(len(queued_errors), 3)
        # The in-flight first doc embed was already running and completed once;
        # the cancelled queued jobs performed no additional compute.
        self.assertEqual(len(self.fake.calls), 1)
        started_text = self.fake.calls[0][0]
        self.assertEqual(started_text, "第一个文档")


@unittest.skipUnless(_HAS_FAISS, "faiss is not installed")
class MemoryIndexTests(_Base):
    @staticmethod
    def _blob(values):
        return struct.pack("<%df" % len(values), *[float(v) for v in values])

    @staticmethod
    def _unit(values):
        norm = math.sqrt(sum(v * v for v in values)) or 1.0
        return [v / norm for v in values]

    def test_build_and_search_cosine(self):
        index = MemoryIndex()
        self.assertEqual(index.revision, 0)
        self.assertEqual(index.count, 0)
        self.assertEqual(index.search([1.0] + [0.0] * (DIMENSIONS - 1)), [])

        rows = [
            (1, self._blob(self._unit([1.0] + [0.0] * (DIMENSIONS - 1)))),
            (2, self._blob(self._unit([0.0, 1.0] + [0.0] * (DIMENSIONS - 2)))),
        ]
        index.replace(7, rows)
        self.assertEqual(index.revision, 7)
        self.assertEqual(index.count, 2)
        hits = index.search(self._unit([1.0] + [0.0] * (DIMENSIONS - 1)), k=2)
        self.assertEqual(hits[0][0], 1)
        self.assertAlmostEqual(hits[0][1], 1.0, places=5)
        self.assertEqual([chunk_id for chunk_id, _ in hits], [1, 2])
        self.assertEqual(sorted({chunk_id for chunk_id, _ in hits}), [1, 2])

    def test_search_normalizes_unnormalized_query(self):
        # Query input is not pre-normalized by callers; MemoryIndex.search must
        # normalize it so results are true cosine similarities.
        index = MemoryIndex()
        rows = [
            (1, self._blob(self._unit([1.0] + [0.0] * (DIMENSIONS - 1)))),
            (2, self._blob(self._unit([0.0, 1.0] + [0.0] * (DIMENSIONS - 2)))),
        ]
        index.replace(1, rows)
        hits = index.search([2.0] + [0.0] * (DIMENSIONS - 1), k=2)
        self.assertEqual(hits[0][0], 1)
        self.assertAlmostEqual(hits[0][1], 1.0, places=5)
        self.assertEqual([chunk_id for chunk_id, _ in hits], [1, 2])

    def test_replace_rejects_older_revision(self):
        index = MemoryIndex()
        rows = [(1, self._blob(self._unit([1.0] + [0.0] * (DIMENSIONS - 1))))]
        index.replace(5, rows)
        with self.assertRaises(AssistantError):
            index.replace(3, rows)
        self.assertEqual(index.revision, 5)
        self.assertEqual(index.count, 1)

    def test_invalid_rows_rejected(self):
        index = MemoryIndex()
        with self.assertRaises(AssistantError):
            index.replace(1, [(1, b"tooshort")])
        bad = self._blob([1.0] * (DIMENSIONS - 1))
        with self.assertRaises(AssistantError):
            index.replace(1, [(1, bad)])
        nan_blob = struct.pack("<%df" % DIMENSIONS, *([float("nan")] * DIMENSIONS))
        with self.assertRaises(AssistantError):
            index.replace(1, [(1, nan_blob)])

    def test_ids_must_be_unique_and_positive(self):
        index = MemoryIndex()
        vec = self._unit([1.0] + [0.0] * (DIMENSIONS - 1))
        blob = self._blob(vec)
        with self.assertRaises(AssistantError):
            index.replace(1, [(0, blob)])  # positive required
        with self.assertRaises(AssistantError):
            index.replace(1, [(1, blob), (1, blob)])  # unique required
        self.assertEqual(index.revision, 0)

    def test_memory_budget_reject_with_small_cap(self):
        # A wildly small budget (patched base) forces a reject before allocating.
        index = MemoryIndex(index_peak_budget_bytes=6 * 1024)
        rows = [(1, self._blob(self._unit([1.0] + [0.0] * (DIMENSIONS - 1))))]
        with mock.patch.object(local_mod, "_INDEX_BASE_BYTES", 1024):
            with self.assertRaises(AssistantError) as ctx:
                index.replace(1, rows)
        self.assertEqual(ctx.exception.status, 503)
        self.assertEqual(index.count, 0)

    def test_vector_limit_stops_before_consuming_whole_source(self):
        index = MemoryIndex()
        blob = self._blob([1.0] + [0.0] * (DIMENSIONS - 1))
        consumed = []
        def rows():
            for number in range(1, 100_001):
                consumed.append(number)
                yield number, blob
        with mock.patch.object(local_mod, '_MAX_VECTORS', 2):
            with self.assertRaises(AssistantError):
                index.replace(1, rows())
        self.assertLessEqual(len(consumed), local_mod._BUILD_BATCH)
        self.assertEqual(index.count, 0)

    def test_memory_index_safe_props(self):
        index = MemoryIndex()
        self.assertGreater(index.memory_budget_bytes, 0)
        self.assertEqual(index.count, 0)
        json.dumps({
            "budget": index.memory_budget_bytes,
            "count": index.count,
            "estimated_peak": index.estimated_peak_bytes,
        })
        rows = [(1, self._blob(self._unit([1.0] + [0.0] * (DIMENSIONS - 1))))]
        index.replace(1, rows)
        self.assertEqual(index.count, 1)
        self.assertGreater(index.estimated_peak_bytes, 0)

    def test_swap_consistency_under_concurrent_search_replace(self):
        index = MemoryIndex()
        stop = threading.Event()
        violations = []
        checks = []

        def build_rows(rev, base):
            rows = []
            for i, dim in enumerate((1, 2, 3)):
                vec = [0.0] * DIMENSIONS
                vec[dim] = 1.0
                rows.append((base + i, self._blob(self._unit(vec))))
            return rows

        def searcher():
            query = self._unit([1.0, 0.5, 0.25] + [0.0] * (DIMENSIONS - 3))
            while not stop.is_set():
                try:
                    hits = index.search(query, k=3)
                except Exception as exc:  # pragma: no cover
                    violations.append(exc)
                    continue
                if not hits:
                    continue
                ids = {chunk_id for chunk_id, _ in hits}
                if any(not (-1.0 <= score <= 1.0) for _, score in hits):
                    violations.append(("score", hits))
                checks.append(1)

        readers = [threading.Thread(target=searcher) for _ in range(8)]
        for thread in readers:
            thread.start()
        try:
            # Use strictly increasing revisions so older swaps are rejected and
            # the snapshot still never regresses under concurrent search/replace.
            for round_idx in range(10):
                index.replace(round_idx * 2 + 1, build_rows(round_idx * 2 + 1, 1))
                index.replace(round_idx * 2 + 2, build_rows(round_idx * 2 + 2, 101))
        finally:
            stop.set()
            for thread in readers:
                thread.join(10)
        self.assertEqual(violations, [])
        self.assertGreater(sum(checks), 0)
        self.assertEqual(index.revision, 20)


if __name__ == "__main__":
    unittest.main(verbosity=2)

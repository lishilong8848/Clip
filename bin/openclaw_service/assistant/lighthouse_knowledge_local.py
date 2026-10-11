# -*- coding: utf-8 -*-
"""Bounded local embedding runtime (fastembed CPU) + in-memory FAISS cosine index.

This module owns the *local* knowledge-base runtime. Model loading is lazy and
serialized inside a single worker thread; neither fastembed nor faiss is
imported at module import time or during construction.
"""
from __future__ import annotations

import heapq
import logging
from itertools import islice
import math
import os
import threading
import time
import uuid
from collections import OrderedDict
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence, Tuple

from .lighthouse_ai import AssistantError

MODEL_NAME = "BAAI/bge-small-zh-v1.5"
DIMENSIONS = 512
MODE = "local_faiss"

# Recommended BGE Chinese instruction for queries; documents are embedded as-is.
_QUERY_INSTRUCTION = "为这个句子生成表示以用于检索相关文章："
_QUERY_PRIORITY = 0
_DOC_PRIORITY = 1
_DEFAULT_QUEUE_MAX = 128
_DEFAULT_LRU_SIZE = 256
_DEFAULT_BATCH_SIZE = 8
_DEFAULT_QUERY_TIMEOUT = 10.0
_DEFAULT_DOC_TIMEOUT = 120.0
_DEFAULT_RETRY_COOLDOWN = 60.0
_WAKE_INTERVAL = 0.25
_MODEL_SEARCH_ADMISSION = 8
_MODEL_SEARCH_K_DEFAULT = 40

# Memory budget: 2 GiB total, shared with the ONNX model, tokenizer/parser and
# the query cache. The index rebuild is limited to a derived fraction so those
# resident pieces keep headroom.
MEMORY_BUDGET_BYTES = 2 * 1024**3
_MAX_VECTORS = 100_000
_BUILD_BATCH = 1024
_VECTOR_BYTES = DIMENSIONS * 4  # float32 per element
_INDEX_BASE_BYTES = 64 * 1024 * 1024  # base FAISS IndexFlatIP overhead
_BUILD_MULTIPLIER = 3  # temp numpy buffers + FAISS internal copies during batch add
_INDEX_PEAK_BUDGET_BYTES = MEMORY_BUDGET_BYTES // 2


class _Job:
    """A unit of work admitted to the priority queue.

    A query job contains exactly one text; a document job may carry many chunks
    that are embedded in sub-batches of at most ``_batch_size``.
    """

    __slots__ = (
        "kind", "key", "priority", "items", "cursor", "admitted",
        "_result", "_error", "_cancelled", "_done", "_lock", "_waiters",
    )

    def __init__(self, kind: str, key: str, priority: int, items: List[str]):
        self.kind = kind
        self.key = key
        self.priority = priority
        self.items = items
        self.cursor = 0
        self.admitted = False
        self._result: List[Tuple[float, ...]] = []
        self._error: Optional[AssistantError] = None
        self._cancelled = False
        self._done = threading.Event()
        self._lock = threading.Lock()
        self._waiters = 0

    def add_result(self, vector: Tuple[float, ...]) -> None:
        with self._lock:
            self._result.append(vector)

    def result_values(self) -> List[Tuple[float, ...]]:
        with self._lock:
            return list(self._result)

    def set_error(self, exc: AssistantError) -> None:
        with self._lock:
            self._error = exc
        self._done.set()

    def finish(self) -> None:
        self._done.set()

    def cancel(self) -> None:
        with self._lock:
            self._cancelled = True
        self._done.set()

    def is_cancelled(self) -> bool:
        with self._lock:
            return self._cancelled

    def error(self) -> Optional[AssistantError]:
        with self._lock:
            return self._error

    def is_done(self) -> bool:
        return self._done.is_set()

    def add_waiter(self) -> None:
        with self._lock:
            self._waiters += 1

    def remove_waiter(self) -> bool:
        """Decrement the waiter count; return whether this was the last waiter."""
        with self._lock:
            self._waiters -= 1
            if self._waiters < 0:
                self._waiters = 0
            return self._waiters <= 0


class _PriorityQueue:
    """Heap-based bounded priority queue guarded by a condition."""

    def __init__(self, max_waiting: int):
        self._max = max(1, int(max_waiting))
        self._heap: List[Tuple[int, int, _Job]] = []
        self._members = set()
        self._seq = 0
        self._cond = threading.Condition()

    def put(self, job: _Job) -> bool:
        with self._cond:
            if job in self._members:
                return True
            # Already-admitted jobs (a doc job resuming after a yield) may
            # re-enter even at capacity; brand-new jobs respect the bound.
            if not job.admitted and len(self._members) >= self._max:
                return False
            job.admitted = True
            self._seq += 1
            heapq.heappush(self._heap, (job.priority, self._seq, job))
            self._members.add(job)
            self._cond.notify_all()
            return True

    def full(self) -> bool:
        with self._cond:
            return len(self._members) >= self._max

    def count(self) -> int:
        with self._cond:
            return len(self._members)

    def snapshot_members(self) -> List[_Job]:
        with self._cond:
            return list(self._members)

    def discard(self, job: _Job) -> bool:
        with self._cond:
            if job not in self._members:
                return False
            self._members.discard(job)
            kept = []
            for entry in self._heap:
                if entry[2] is not job:
                    kept.append(entry)
            heapq.heapify(kept)
            self._heap = kept
            self._cond.notify_all()
            return True

    def take(self, timeout: float) -> Optional[_Job]:
        deadline = time.monotonic() + timeout
        with self._cond:
            while True:
                if self._heap:
                    _, _, job = heapq.heappop(self._heap)
                    self._members.discard(job)
                    return job
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                self._cond.wait(remaining)

    def wake_all(self) -> None:
        with self._cond:
            self._cond.notify_all()


class MemoryIndex:
    """In-memory FAISS cosine index with atomic snapshot swap.

    The index is built incrementally in bounded batches (``begin_build`` /
    ``add_batch`` / ``end_build``) or in one shot via ``replace``.  ``search``
    captures the current snapshot without holding a read mutex, so reads never
    block a rebuild.
    """

    def __init__(self, *, index_peak_budget_bytes: Optional[int] = None):
        self._state = _IndexState(None, 0, 0)
        self._swap_lock = threading.Lock()
        self._admit = threading.BoundedSemaphore(_MODEL_SEARCH_ADMISSION)
        self._peak_budget = int(index_peak_budget_bytes or _INDEX_PEAK_BUDGET_BYTES)
        self._builder: Optional["_IndexBuilder"] = None

    @property
    def revision(self) -> int:
        return self._state.revision

    @property
    def count(self) -> int:
        return self._state.count

    @property
    def memory_budget_bytes(self) -> int:
        return self._peak_budget

    @property
    def estimated_peak_bytes(self) -> int:
        count = self._state.count
        return _INDEX_BASE_BYTES + count * _VECTOR_BYTES * _BUILD_MULTIPLIER

    def begin_build(self, revision: int) -> None:
        if not isinstance(revision, int) or isinstance(revision, bool):
            raise AssistantError("向量索引版本无效。", 502)
        with self._swap_lock:
            if revision < self._state.revision:
                raise AssistantError("向量索引版本不得回退。", 502)
            if self._builder is not None:
                raise AssistantError("向量索引构建已在进行中。", 503)
            self._builder = _IndexBuilder(int(revision), self._peak_budget)

    def add_batch(self, rows: Iterable[Tuple[int, bytes]]) -> None:
        with self._swap_lock:
            builder = self._builder
            if builder is None:
                raise AssistantError("向量索引构建尚未开始。", 502)
            builder.add(rows)

    def end_build(self) -> int:
        with self._swap_lock:
            builder = self._builder
            if builder is None:
                raise AssistantError("向量索引构建尚未开始。", 502)
            if builder.revision < self._state.revision:
                self._builder = None
                raise AssistantError("向量索引版本不得回退。", 502)
            new_state = builder.finish()
            self._builder = None
            self._state = new_state
            return new_state.count

    def cancel_build(self) -> None:
        with self._swap_lock:
            self._builder = None

    def replace(self, revision: int, rows: Iterable[Tuple[int, bytes]]) -> None:
        self.begin_build(revision)
        try:
            self.add_batch(rows)
        except Exception:
            self.cancel_build()
            raise
        self.end_build()

    def search(self, vector: Sequence[float], k: int = _MODEL_SEARCH_K_DEFAULT) -> List[Tuple[int, float]]:
        if not self._admit.acquire(timeout=1.0):
            raise AssistantError("本地向量检索繁忙，请稍后重试。", 503)
        try:
            faiss = _import_faiss()
            import numpy as np

            values = [float(v) for v in vector]
            if len(values) != DIMENSIONS or not all(math.isfinite(v) for v in values):
                raise AssistantError("本地向量检索参数无效。", 502)
            norm = math.sqrt(sum(v * v for v in values))
            if not math.isfinite(norm) or norm <= 0.0:
                raise AssistantError("本地向量检索参数无效。", 502)
            state = self._state
            index = state.index
            if index is None or state.count == 0:
                return []
            k = max(1, min(int(k), 200))
            query = np.asarray([[v / norm for v in values]], dtype="float32")
            faiss.omp_set_num_threads(1)
            distances, ids = index.search(query, k)
            return [
                (int(chunk_id), float(score))
                for chunk_id, score in zip(ids[0], distances[0])
                if chunk_id != -1
            ]
        finally:
            self._admit.release()


class _IndexBuilder:
    """Streams validated, normalized vectors into a FAISS index in bounded batches.

    Ids are checked to be unique and positive across the whole build, and the
    projected peak memory (base overhead + raw vector bytes) is enforced before
    any batch numpy buffer is allocated.
    """

    __slots__ = ("revision", "peak_budget", "index", "ids_seen", "total")

    def __init__(self, revision: int, peak_budget: int):
        self.revision = revision
        self.peak_budget = peak_budget
        self.index = None
        self.ids_seen = set()
        self.total = 0

    def add(self, rows: Iterable[Tuple[int, bytes]]) -> None:
        source = iter(rows)
        while batch := list(islice(source, _BUILD_BATCH)):
            self._add_batch(batch)

    def _add_batch(self, rows_provided: List[Tuple[int, bytes]]) -> None:
        import numpy as np

        new_count = self.total + len(rows_provided)
        if new_count > _MAX_VECTORS:
            raise AssistantError("向量索引片段数量超出内存上限，请分批上传。", 503)
        estimated_peak = _INDEX_BASE_BYTES + new_count * _VECTOR_BYTES * _BUILD_MULTIPLIER
        if estimated_peak > self.peak_budget:
            raise AssistantError("向量索引重建超出内存预算，请分批上传后重试。", 503)

        validated: List[Tuple[int, bytes]] = []
        for chunk_id, blob in rows_provided:
            if isinstance(chunk_id, bool) or not isinstance(chunk_id, int):
                raise AssistantError("向量索引片段标识无效。", 502)
            if chunk_id <= 0:
                raise AssistantError("向量索引片段标识必须为正数。", 502)
            if chunk_id in self.ids_seen:
                raise AssistantError("向量索引片段标识重复。", 502)
            self.ids_seen.add(chunk_id)
            if not isinstance(blob, (bytes, bytearray, memoryview)) or len(blob) != _VECTOR_BYTES:
                raise AssistantError("向量索引片段内容无效。", 502)
            validated.append((int(chunk_id), bytes(blob)))

        if not validated:
            return
        if self.index is None:
            faiss = _import_faiss()
            faiss.omp_set_num_threads(1)
            self.index = faiss.IndexIDMap2(faiss.IndexFlatIP(DIMENSIONS))
        # Add in bounded sub-batches to cap peak memory.
        for start in range(0, len(validated), _BUILD_BATCH):
            batch = validated[start:start + _BUILD_BATCH]
            batch_ids = np.asarray([item[0] for item in batch], dtype="int64")
            batch_vecs = np.empty((len(batch), DIMENSIONS), dtype="float32")
            for row_idx, (_, blob) in enumerate(batch):
                arr = np.frombuffer(blob, dtype="<f4")
                if arr.size != DIMENSIONS or not bool(np.all(np.isfinite(arr))):
                    raise AssistantError("向量索引片段内容无效。", 502)
                norm = float(np.dot(arr, arr))
                if not math.isfinite(norm) or norm <= 0.0:
                    raise AssistantError("向量索引片段内容无效。", 502)
                batch_vecs[row_idx] = arr / math.sqrt(norm)
            self.index.add_with_ids(batch_vecs, batch_ids)
            self.total += len(batch)

    def finish(self) -> "_IndexState":
        if self.index is None:
            faiss = _import_faiss()
            faiss.omp_set_num_threads(1)
            self.index = faiss.IndexIDMap2(faiss.IndexFlatIP(DIMENSIONS))
        return _IndexState(self.index, self.revision, self.total)


class _IndexState:
    __slots__ = ("index", "revision", "count")

    def __init__(self, index, revision: int, count: int):
        self.index = index
        self.revision = revision
        self.count = count


class LocalEmbeddings:
    """Lazy, serialized local embeddings with bounded priority queue and LRU."""

    def __init__(
        self,
        root: Any,
        *,
        model_factory: Optional[Any] = None,
        start_worker: bool = True,
        queue_max: int = _DEFAULT_QUEUE_MAX,
        lru_size: int = _DEFAULT_LRU_SIZE,
        batch_size: int = _DEFAULT_BATCH_SIZE,
        query_timeout: float = _DEFAULT_QUERY_TIMEOUT,
        doc_timeout: float = _DEFAULT_DOC_TIMEOUT,
        model_retry_cooldown: float = _DEFAULT_RETRY_COOLDOWN,
    ):
        self.root = Path(root).resolve()
        self.model_dir = self.root / "models"
        try:
            self.model_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            raise AssistantError("本地嵌入模型目录不可写，请检查权限。", 503) from None
        self._model_factory = model_factory
        self._queue = _PriorityQueue(queue_max)
        self._lru: OrderedDict = OrderedDict()
        self._lru_size = max(1, int(lru_size))
        self._batch_size = max(1, min(int(batch_size), 8))
        self._query_timeout = float(query_timeout)
        self._doc_timeout = float(doc_timeout)
        self._cooldown = float(model_retry_cooldown)
        self._start_worker = bool(start_worker)

        self._query_jobs: dict = {}
        self._registry_lock = threading.Lock()
        self._lru_lock = threading.Lock()
        self._model_lock = threading.Lock()
        self._model: Optional[Any] = None
        self._ready = False
        self._loading = False
        self._cooldown_until: Optional[float] = None
        self._closed = False
        self._stop_event = threading.Event()
        self._worker: Optional[threading.Thread] = None
        self._worker_lock = threading.Lock()
        self._close_lock = threading.Lock()
        self._active_job: Optional[_Job] = None
        self._job_lock = threading.Lock()

        if start_worker:
            self._ensure_worker()

    # ------------------------------------------------------------------ API

    def embed(
        self,
        texts: Any,
        config: Optional[Any] = None,
        *,
        query: bool = False,
    ) -> List[List[float]]:
        del config  # local runtime takes no configuration payload
        if self._closed:
            raise AssistantError("本地嵌入服务已关闭，请稍后重试。", 503)
        if isinstance(texts, str):
            texts = [texts]
        if not isinstance(texts, (list, tuple)) or not texts:
            return []
        normalized: List[str] = []
        for item in texts:
            if not isinstance(item, str):
                raise AssistantError("本地嵌入文本格式无效。", 400)
            normalized.append(item)
        self._ensure_worker()
        if query:
            return [self._embed_query(text) for text in normalized]
        return self._embed_documents(normalized)

    def status(self) -> dict:
        with self._model_lock:
            ready = self._ready
            loading = self._loading
        with self._lru_lock:
            cache = len(self._lru)
        with self._registry_lock:
            inflight = len(self._query_jobs)
        return {
            "model": MODEL_NAME,
            "dimensions": DIMENSIONS,
            "mode": MODE,
            "ready": ready,
            "loading": loading,
            "queued": self._queue.count(),
            "inflight_queries": inflight,
            "cache": cache,
            "closed": self._closed,
        }

    def close(self, timeout: float = 25.0) -> None:
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
            self._stop_event.set()
            self._cancel_pending()
            self._queue.wake_all()
            worker = self._worker
            self._worker = None
        if worker is not None:
            worker.join(timeout)
        # The worker owns model cleanup in its finally block. If the worker is
        # still alive (blocking load/inference), we must not acquire the model
        # lock nor close the model under it.
        return

    # -------------------------------------------------------------- queries

    def _embed_query(self, text: str) -> List[float]:
        key = text.strip()
        cached = self._lru_get(key)
        if cached is not None:
            return list(cached)
        with self._registry_lock:
            # Recheck inside the registry lock: a just-finished single-flight job
            # may have populated the LRU immediately before being removed, which
            # would otherwise cause a duplicate embedding.
            cached = self._lru_get(key)
            if cached is not None:
                return list(cached)
            job = self._query_jobs.get(key)
            if job is None:
                if self._queue.full():
                    raise AssistantError("本地嵌入排队已满，本次查询仅使用关键词结果。", 503)
                job = _Job("query", key, _QUERY_PRIORITY, [text])
                if not self._queue.put(job):
                    raise AssistantError("本地嵌入排队已满，本次查询仅使用关键词结果。", 503)
                self._query_jobs[key] = job
            job.add_waiter()
        result = self._wait_job(job, self._query_timeout)
        return result[0]

    def _embed_documents(self, texts: List[str]) -> List[List[float]]:
        job = _Job("doc", "doc:" + uuid.uuid4().hex, _DOC_PRIORITY, list(texts))
        if not self._queue.put(job):
            raise AssistantError("本地嵌入排队已满，请稍后重试。", 503)
        job.add_waiter()
        return self._wait_job(job, self._doc_timeout)

    # -------------------------------------------------------------- worker

    def _ensure_worker(self) -> None:
        if not self._start_worker or self._closed or self._stop_event.is_set():
            return
        with self._worker_lock:
            if self._worker is not None and self._worker.is_alive():
                return
            thread = threading.Thread(
                target=self._run,
                name="LocalKnowledgeEmbedding",
                daemon=True,
            )
            self._worker = thread
            thread.start()

    def _run(self) -> None:
        try:
            from upload_event_module.services.process_lifetime import lower_current_thread_priority
            lower_current_thread_priority()
        except Exception:
            pass
        try:
            while True:
                job = self._queue.take(timeout=_WAKE_INTERVAL)
                if job is None:
                    if self._stop_event.is_set():
                        break
                    continue
                if job.is_cancelled():
                    self._finish_job(job, AssistantError("本地嵌入服务已关闭，请稍后重试。", 503))
                    continue
                with self._job_lock:
                    self._active_job = job
                try:
                    if job.kind == "query":
                        self._run_query_job(job)
                    else:
                        self._run_doc_job(job)
                except AssistantError as exc:
                    self._finish_job(job, exc)
                except Exception:
                    self._finish_job(job, AssistantError("本地嵌入任务失败，请稍后重试。", 503))
                finally:
                    with self._job_lock:
                        if self._active_job is job:
                            self._active_job = None
        finally:
            self._cleanup_model()

    def _run_query_job(self, job: _Job) -> None:
        model = self._ensure_model()
        if self._closed or job.is_cancelled():
            raise AssistantError("本地嵌入任务已取消。", 503)
        batch = [_QUERY_INSTRUCTION + job.items[0]]
        arrays = list(model.embed(batch, batch_size=1, parallel=None))
        if len(arrays) != 1:
            raise AssistantError("本地嵌入模型返回向量数量异常。", 502)
        vector = self._validate_vector(arrays[0])
        job.add_result(vector)
        self._lru_set(job.key, vector)
        self._finish_job(job)

    def _run_doc_job(self, job: _Job) -> None:
        model = self._ensure_model()
        while job.cursor < len(job.items):
            if self._stop_event.is_set() or job.is_cancelled():
                raise AssistantError("本地嵌入服务已关闭，请稍后重试。", 503)
            batch = job.items[job.cursor:job.cursor + self._batch_size]
            job.cursor += len(batch)
            arrays = list(model.embed(batch, batch_size=len(batch), parallel=None))
            if len(arrays) != len(batch):
                raise AssistantError("本地嵌入模型返回向量数量异常。", 502)
            for array in arrays:
                job.add_result(self._validate_vector(array))
            if job.cursor < len(job.items):
                # Yield to higher-priority (query) work before continuing.
                if not self._queue.put(job):
                    raise AssistantError("本地嵌入服务繁忙，请稍后重试。", 503)
                return
        self._finish_job(job)

    def _ensure_model(self) -> Any:
        now = time.monotonic()
        with self._model_lock:
            if self._model is not None:
                return self._model
            if self._cooldown_until is not None and now < self._cooldown_until:
                raise AssistantError("本地嵌入模型加载中，请稍后重试。", 503)
            if self._loading:
                raise AssistantError("本地嵌入模型加载中，请稍后重试。", 503)
            self._loading = True
        # The blocking model load must not hold ``_model_lock``.
        try:
            model = self._load_model()
        except AssistantError:
            self._set_cooldown()
            raise
        except Exception:
            self._set_cooldown()
            raise AssistantError("本地嵌入模型加载失败，请检查依赖并稍后重试。", 503) from None
        finally:
            with self._model_lock:
                self._loading = False
        if model is None:
            self._set_cooldown()
            raise AssistantError("本地嵌入模型不可用，请检查本地模型后重试。", 503)
        with self._model_lock:
            self._model = model
            self._ready = True
            self._cooldown_until = None
        return model

    def _set_cooldown(self) -> None:
        with self._model_lock:
            self._cooldown_until = time.monotonic() + self._cooldown

    def _load_model(self) -> Optional[Any]:
        if self._model_factory is not None:
            return self._model_factory()
        try:
            os.environ.setdefault('TOKENIZERS_PARALLELISM', 'false')
            from fastembed import TextEmbedding
        except (ImportError, OSError) as exc:
            logging.error('知识库嵌入依赖导入失败: %s', type(exc).__name__)
            raise AssistantError('本地嵌入依赖未就绪，请更新程序并查看依赖安装日志。', 503) from exc
        options = dict(model_name=MODEL_NAME, cache_dir=str(self.model_dir), threads=1,
                       providers=['CPUExecutionProvider'], lazy_load=False, enable_cpu_mem_arena=False)
        from .lighthouse_knowledge_model import BUNDLE, verify_model
        bundled = Path(__file__).resolve().parents[3] / BUNDLE
        try:
            if bundled.is_dir():
                verify_model(bundled)
                return TextEmbedding(**options, specific_model_path=str(bundled), local_files_only=True)
            try:
                return TextEmbedding(**options, local_files_only=True)
            except (ValueError, FileNotFoundError):
                logging.info('知识库本地模型未缓存，正在下载 BGE 模型；新版完整补丁内置此模型。')
                return TextEmbedding(**options)
        except AssistantError:
            raise
        except Exception as exc:
            logging.error('知识库模型加载失败: type=%s bundled=%s model_dir=%s', type(exc).__name__, bundled.is_dir(), self.model_dir)
            message = ('内置知识库模型加载失败，请重新更新完整补丁并核对运行依赖。' if bundled.is_dir()
                       else '本机知识库模型未就绪，自动下载或加载失败；请更新包含本地模型的完整补丁后重新索引。')
            raise AssistantError(message, 503) from exc

    # ------------------------------------------------------------- helpers

    def _wait_job(self, job: _Job, timeout: float) -> List[List[float]]:
        timed_out = False
        try:
            deadline = time.monotonic() + timeout
            while True:
                if self._closed or job.is_cancelled():
                    raise AssistantError("本地嵌入服务已关闭，请稍后重试。", 503)
                if job.is_done():
                    error = job.error()
                    if error is not None:
                        raise error
                    return [list(vector) for vector in job.result_values()]
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    timed_out = True
                    raise AssistantError("本地嵌入服务繁忙，本次任务已超时，请稍后重试。", 503)
                job._done.wait(min(remaining, 0.1))
        finally:
            if timed_out:
                self._drop_timed_out_job(job)
            else:
                job.remove_waiter()

    def _drop_timed_out_job(self, job: _Job) -> None:
        """Remove this waiter; if it was the last, drop a queued job."""
        if job.kind == "query":
            with self._registry_lock:
                if self._query_jobs.get(job.key) is not job:
                    job.remove_waiter()
                    return
                with job._lock:
                    job._waiters -= 1
                    if job._waiters < 0:
                        job._waiters = 0
                    last = job._waiters <= 0
                if not last or job.is_done():
                    return
                job.cancel()
                if self._queue.discard(job):
                    del self._query_jobs[job.key]
                    job.set_error(AssistantError(
                        "本地嵌入服务繁忙，本次任务已超时取消，请稍后重试。", 503))
            return
        if job.remove_waiter() and not job.is_done():
            job.cancel()
            if self._queue.discard(job):
                job.set_error(AssistantError(
                    "本地嵌入服务繁忙，本次任务已超时取消，请稍后重试。", 503))

    def _finish_job(self, job: _Job, error: Optional[AssistantError] = None) -> None:
        if error is not None:
            job.set_error(error)
        else:
            job.finish()
        if job.kind == "query":
            with self._registry_lock:
                if self._query_jobs.get(job.key) is job:
                    del self._query_jobs[job.key]

    def _cancel_pending(self) -> None:
        with self._job_lock:
            active = self._active_job
        if active is not None:
            active.cancel()
        with self._registry_lock:
            jobs = list(self._query_jobs.values())
            for job in jobs:
                job.cancel()
            self._query_jobs.clear()
        for job in self._queue.snapshot_members():
            job.cancel()

    def _cleanup_model(self) -> None:
        with self._model_lock:
            model = self._model
            self._model = None
            self._ready = False
            self._loading = False
            self._cooldown_until = None
        if model is not None:
            close = getattr(model, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass

    @staticmethod
    def _validate_vector(array) -> Tuple[float, ...]:
        values = list(array)
        if len(values) != DIMENSIONS:
            raise AssistantError("本地嵌入模型输出维度异常，请检查模型。", 502)
        norm = 0.0
        floats: List[float] = []
        for value in values:
            try:
                value = float(value)
            except (TypeError, ValueError):
                raise AssistantError("本地嵌入结果无效。", 502) from None
            if not math.isfinite(value):
                raise AssistantError("本地嵌入结果无效。", 502)
            floats.append(value)
            norm += value * value
        if norm <= 0.0 or not math.isfinite(norm):
            raise AssistantError("本地嵌入结果无效。", 502)
        inv = 1.0 / math.sqrt(norm)
        return tuple(value * inv for value in floats)

    def _lru_get(self, key: str) -> Optional[Tuple[float, ...]]:
        with self._lru_lock:
            vector = self._lru.get(key)
            if vector is None:
                return None
            self._lru.move_to_end(key)
            return vector

    def _lru_set(self, key: str, vector: Tuple[float, ...]) -> None:
        with self._lru_lock:
            self._lru[key] = vector
            self._lru.move_to_end(key)
            while len(self._lru) > self._lru_size:
                self._lru.popitem(last=False)


def _import_faiss():
    try:
        import faiss
    except Exception:
        raise AssistantError("本地向量检索依赖未就绪，请完成依赖更新后重试。", 503) from None
    return faiss

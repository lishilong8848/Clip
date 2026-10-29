"""Shared company documents, local full-text retrieval and in-memory FAISS vectors.

The knowledge base is fully local by default: BGE small-zh embeddings are produced by
``lighthouse_knowledge_local.LocalEmbeddings`` (serialized, no remote embedding
credentials) and stored persistently in an ordinary SQL table (``local_vectors``).  An
in-memory FAISS index (``lighthouse_knowledge_local.MemoryIndex``) is rebuilt from those
saved float32 blobs on startup and kept as an immutable snapshot that is swapped
atomically under a singleflight lock; reads never take that lock, so searches remain
parallel while only query embedding and index writes are serialized.
"""
from __future__ import annotations

from contextlib import contextmanager, suppress
import copy
import hashlib
import io
import ipaddress
import json
import logging
import math
import os
from pathlib import Path
import re
import sqlite3
import struct
import threading
import time
from urllib.parse import urlsplit
import uuid

from .lighthouse_ai import (AssistantError, ADDRESS, API_KEY, CONTACT, ID_NUMBER,
                            protect_key, safe_data, safe_text)
from .lighthouse_message_delivery import MESSAGE_INTENT
from .lighthouse_knowledge_extract import SUPPORTED_EXTENSIONS

MAX_FILE_BYTES = 100 * 1024 * 1024
MAX_BATCH_BYTES = 300 * 1024 * 1024
EXTENSIONS = SUPPORTED_EXTENSIONS

# Local FAISS configuration is fixed for every deployment.  No per-install
# embedding endpoint, model name or credential is required anymore.
MODE = 'local_faiss'
MODEL_NAME = 'BAAI/bge-small-zh-v1.5'
DIMENSIONS = 512

# Streaming upload copy granularity and bounded staging/atomic finalize.
_STREAM_CHUNK = 1024 * 1024
_EMBED_BATCH = 8
_MAX_ACTIVE_CHUNKS = 100_000
_MEMORY_BUDGET = 2 * 1024 * 1024 * 1024
_MAX_BACKUP_SECONDS = 20.0
_UNCHANGED_PARENT = object()

_CREDENTIAL = re.compile(r'(?:api[_ -]?key|password|authorization|access[_ -]?token|密钥|口令|密码)\s*[:：=]\s*\S{6,}', re.I)
_COMPANY = re.compile(r'知识库|公司(?:的)?(?:内部|资料|文件|文档|制度|规定|规范|流程|政策|要求|标准|报销|请假|假期|福利|考勤|出差|差旅|休假|年假|规章|审批|培训)|(?:本公司|我们公司|咱们公司|公司里|公司内|本单位|单位内部)|员工手册|内部(?:制度|规定|资料|流程|文档)', re.I)
_BUSINESS = re.compile(
    r'(?:发送|发起|结束|删除|修改|更新|撤销|新建|创建|填写|保存|上传|绑定|导出|审批|签名|签字).{0,20}(?:通告|维修|跟进|机柜|工单|演练|水耗|重保)'
    r'|(?:今天|今日|现在|当前|未结束|进行中|待发|未完成).{0,25}(?:通告|维修|事件|任务|工单|水耗|机柜|学练|答题|检修|演练|重保|收敛)'
    r'|(?:通告|维修|事件|任务|工单|水耗|机柜|学练|答题|检修|演练|重保|收敛).{0,15}(?:几条|多少|数量|进度|状态|数据|记录|列表)'
    r'|(?:在职|在岗|员工|人员).{0,10}(?:人数|多少人|名单|列表)'
    r'|(?:多少|几名|几位|几个).{0,6}(?:在职|在岗|员工|人员)|(?:公司|基地|楼).{0,6}(?:总人数|在职人数)'
    r'|(?:当前|现在).{0,8}(?:登录|登陆)|灯塔.{0,15}(?:怎么|如何|操作|使用|设置|界面)'
    r'|(?:上传|下载|删除|替换|更新).{0,8}(?:知识库|文件|文档)', re.I)
_EXTERNAL = re.compile(r'天气|天气预报|新闻|联网|外部网站|股价|汇率|股票|翻译|写代码|编程|(?:微软|苹果|谷歌|特斯拉|OpenAI|字节跳动|阿里巴巴)公司', re.I)
_KNOWLEDGE_META = re.compile(r'(?:什么是知识库|知识库是什么)[?？。！!\s]*$|知识库有什么(?:作用|功能|用途|原理)|知识库.{0,12}(?:怎么|如何|为何|为什么).{0,12}(?:上传|下载|删除|使用|配置|设置|加载|卡)|(?:不要|不用|不需要|无需|别).{0,8}(?:知识库|公司资料)', re.I)


def _load_local():
    """Return the local FAISS/embedding module or None when it is not installed yet."""
    try:
        from . import lighthouse_knowledge_local as module
        return module
    except Exception:
        return None


def company_question(question):
    text = str(question or '').strip()
    return bool(_COMPANY.search(text) and not _BUSINESS.search(text)
                and not _EXTERNAL.search(text) and not _KNOWLEDGE_META.search(text)
                and not MESSAGE_INTENT.search(text))


def sensitive_document(text):
    return any(pattern.search(text) for pattern in (ID_NUMBER, ADDRESS, API_KEY, CONTACT, _CREDENTIAL))


def origin(url):
    try:
        if not isinstance(url, str) or len(url) > 2000 or any(c.isspace() for c in url):
            raise ValueError()
        parsed = urlsplit(str(url))
        if parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError()
        if parsed.hostname.lower() == 'localhost':
            raise ValueError()
        try:
            address = ipaddress.ip_address(parsed.hostname)
        except ValueError:
            address = None
        if address and (address.is_loopback or address.is_link_local or address.is_unspecified or address.is_multicast):
            raise ValueError()
        port = parsed.port
        return 'https://' + parsed.hostname.lower() + (':' + str(port) if port and port != 443 else '')
    except (ValueError, TypeError):
        raise AssistantError('请填写有效的 HTTPS 模型接口，不含账号、密钥或查询参数。') from None


def check_actor(actor, *, admin=False):
    if not isinstance(actor, dict) or not actor.get('id') or actor.get('is_guest') or actor.get('role') == 'guest':
        raise AssistantError('请使用正式账号登录后访问公司知识库。', 403)
    if admin and not actor.get('is_admin'):
        raise AssistantError('仅管理员可修改知识库模型设置。', 403)


def _validate_group_name(name):
    """Validate and normalize a group display name (length bounded, no control chars)."""
    if not isinstance(name, str):
        raise AssistantError('分组名称无效。')
    name = name.strip()
    if not name:
        raise AssistantError('请填写分组名称。')
    if len(name) > 50:
        raise AssistantError('分组名称最长50个字符。')
    if any(ord(c) < 32 for c in name):
        raise AssistantError('分组名称包含无效字符。')
    return name


def _pack(vector, dim=DIMENSIONS):
    """Convert a validated float vector into a little-endian float32 blob."""
    if type(vector) is not list or len(vector) != dim:
        raise AssistantError('嵌入向量维度无效，未更新知识库。', 502)
    norm = math.sqrt(sum(v * v for v in vector))
    if not math.isfinite(norm) or norm <= 0:
        raise AssistantError('嵌入向量数值无效，未更新知识库。', 502)
    for v in vector:
        if type(v) not in (int, float) or not math.isfinite(v):
            raise AssistantError('嵌入向量数值无效，未更新知识库。', 502)
    return struct.pack('<%df' % dim, *vector)


class KnowledgeBase:
    def __init__(self, root, *, embedder=None, protect=protect_key, extractor=None,
                 start_worker=True, memory_index_factory=None, source_acl=None, source_current=None,
                 source_summarizer=None):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.db = self.root / 'knowledge.sqlite3'
        self.protect = protect
        self.extractor = extractor
        self.source_summarizer = source_summarizer
        self.source_acl = source_acl
        self.source_current = source_current
        self._evidence_key = os.urandom(32)
        self._worker_tasks = []
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._worker = None
        self._worker_lock = threading.Lock()
        self._index_lock = threading.Lock()
        self._settings_lock = threading.Lock()
        # Embedding serialization (single worker, bounded priority queue, query
        # singleflight) is owned by LocalEmbeddings; KnowledgeBase must NOT wrap
        # embed calls in a coarse '_embed_lock' that would serialize callers
        # before they reach the queue (breaking query priority and singleflight).
        self.start_worker = start_worker

        self._local = _load_local()
        if embedder is not None:
            self.embedder = embedder
        elif self._local is not None:
            self.embedder = self._local.LocalEmbeddings(self.root)
        else:
            self.embedder = None
        self._memory_index_factory = memory_index_factory
        # Immutable FAISS snapshot.  Readers grab this reference without any lock; the
        # rebuild path replaces it atomically under ``_snapshot_lock`` (singleflight).
        self._snapshot = None
        self._snapshot_lock = threading.Lock()

        # Commit schema + reconcile first, then WAL and a guarded lazy snapshot
        # build.  Never build the snapshot while the schema transaction is still
        # open (that could deadlock), and never let a missing faiss/corrupt
        # cached blob crash construction -- degrade to FTS instead.
        with self.connect() as db:
            self._migrate_and_schema(db)
            self._reconcile_queue(db)
        self._set_wal()
        self._bootstrap_snapshot_guarded()
        self._ensure_worker()

    # ------------------------------------------------------------------ schema
    @contextmanager
    def connect(self, *, readonly=False):
        if readonly:
            db = sqlite3.connect(self.db.as_uri() + '?mode=ro', uri=True, timeout=2)
        else:
            db = sqlite3.connect(self.db, timeout=1)
        db.row_factory = sqlite3.Row
        try:
            if readonly:
                yield db
            else:
                with db:
                    yield db
        finally:
            db.close()

    def _set_wal(self):
        """Configure WAL journal mode outside any write transaction."""
        try:
            conn = sqlite3.connect(self.db, timeout=2)
            try:
                conn.execute("PRAGMA journal_mode=WAL")
            finally:
                conn.close()
        except sqlite3.Error:
            # WAL is an optimization; a read-only media or unusual platform
            # should not prevent startup (FTS fallback still works).
            pass

    def _migrate_and_schema(self, db):
        """Create the local schema and migrate an existing keyword/hybrid DB once.

        The ``local_faiss_migrated`` flag is set exactly once for every database
        and ``generation`` stays stable across restarts (it only moves when the
        admin explicitly changes settings).  Backing up a legacy DB is bounded by
        size and a deadline and reads the source read-only so it never blocks
        live writers.
        """
        existed = self.db.exists()
        before = db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='config'").fetchone()
        migrated = db.execute("SELECT payload FROM config WHERE key='local_faiss_migrated'").fetchone() if before else None
        needs_backup = existed and before is not None and (not migrated)
        if needs_backup:
            stamp = time.strftime('%Y%m%d-%H%M%S')
            dst = self.db.with_name('knowledge.sqlite3.bak-%s.sqlite3' % stamp)
            start = time.monotonic()
            # Read the legacy source read-only; the destination is a fresh file,
            # so live writers of the main DB are never blocked by the copy.
            src = sqlite3.connect(self.db.as_uri() + '?mode=ro', uri=True)
            dest = sqlite3.connect(str(dst))
            complete = False
            try:
                def check_deadline(_status, _remaining, _total):
                    if time.monotonic() - start > _MAX_BACKUP_SECONDS:
                        raise AssistantError('知识库升级备份超时，原资料未改变，请稍后重试。', 503)
                with dest:
                    src.backup(dest, pages=256, progress=check_deadline, sleep=.025)
                complete = True
            finally:
                src.close()
                dest.close()
                if not complete:
                    with suppress(OSError):
                        dst.unlink(missing_ok=True)

        db.executescript('''
            CREATE TABLE IF NOT EXISTS config(key TEXT PRIMARY KEY,payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS documents(
                id TEXT PRIMARY KEY,owner TEXT NOT NULL,owner_name TEXT NOT NULL,name TEXT NOT NULL,
                revision INTEGER NOT NULL DEFAULT 1,active_version INTEGER,pending_version INTEGER,
                deleted INTEGER NOT NULL DEFAULT 0,status TEXT NOT NULL,error TEXT NOT NULL DEFAULT '',
                updated_at REAL NOT NULL,size INTEGER NOT NULL,chunks INTEGER NOT NULL DEFAULT 0);
            CREATE INDEX IF NOT EXISTS documents_queue ON documents(deleted,status,updated_at);
            CREATE TABLE IF NOT EXISTS versions(
                document_id TEXT NOT NULL,version INTEGER NOT NULL,name TEXT NOT NULL,
                hash TEXT NOT NULL,path TEXT NOT NULL,size INTEGER NOT NULL,created_at REAL NOT NULL,
                PRIMARY KEY(document_id,version));
            CREATE INDEX IF NOT EXISTS versions_hash ON versions(hash);
            CREATE TABLE IF NOT EXISTS document_analysis(
                document_id TEXT NOT NULL,version INTEGER NOT NULL,part INTEGER NOT NULL,
                text TEXT NOT NULL,location TEXT NOT NULL,summary TEXT NOT NULL,
                PRIMARY KEY(document_id,version,part));
            CREATE TABLE IF NOT EXISTS chunks(
                id INTEGER PRIMARY KEY,document_id TEXT NOT NULL,version INTEGER NOT NULL,
                ordinal INTEGER NOT NULL,text TEXT NOT NULL,location TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS chunks_document ON chunks(document_id,version,ordinal);
            CREATE TABLE IF NOT EXISTS index_staging(
                document_id TEXT NOT NULL,ordinal INTEGER NOT NULL,embedding BLOB NOT NULL,
                PRIMARY KEY(document_id,ordinal));
            CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(text,tokenize='trigram');
            CREATE TABLE IF NOT EXISTS local_vectors(
                chunk_id INTEGER PRIMARY KEY,embedding BLOB NOT NULL);
            CREATE INDEX IF NOT EXISTS local_vectors_chunk ON local_vectors(chunk_id);
            CREATE TABLE IF NOT EXISTS knowledge_groups(
                id TEXT PRIMARY KEY,name TEXT NOT NULL,owner TEXT NOT NULL,owner_name TEXT NOT NULL,created_at REAL NOT NULL);
            CREATE INDEX IF NOT EXISTS knowledge_groups_name ON knowledge_groups(lower(name));
            CREATE TABLE IF NOT EXISTS doc_group_members(
                document_id TEXT PRIMARY KEY,group_id TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS doc_group_members_group ON doc_group_members(group_id);
            CREATE TABLE IF NOT EXISTS document_sources(
                document_id TEXT PRIMARY KEY,source_key TEXT UNIQUE NOT NULL,
                payload TEXT NOT NULL,enabled INTEGER NOT NULL DEFAULT 1);
            CREATE TABLE IF NOT EXISTS source_versions(
                document_id TEXT NOT NULL,version INTEGER NOT NULL,payload TEXT NOT NULL,
                PRIMARY KEY(document_id,version));
            CREATE TABLE IF NOT EXISTS source_artifacts(id TEXT PRIMARY KEY,sources TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS source_retry(document_id TEXT PRIMARY KEY,attempts INTEGER NOT NULL,next_attempt REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS source_reindex_receipts(
                source_key TEXT NOT NULL,request_id TEXT NOT NULL,PRIMARY KEY(source_key,request_id));
        ''')
        if 'source_key' not in {row[1] for row in db.execute('PRAGMA table_info(documents)')}:
            db.execute("ALTER TABLE documents ADD COLUMN source_key TEXT NOT NULL DEFAULT ''")
        if 'index_token' not in {row[1] for row in db.execute('PRAGMA table_info(documents)')}:
            db.execute("ALTER TABLE documents ADD COLUMN index_token TEXT NOT NULL DEFAULT ''")
        # Additive folder-tree migration: nested folders are groups whose parent_id
        # points at another group.  Existing flat groups keep parent_id NULL and remain
        # roots, so the API and all existing consumers stay compatible.  Referential
        # integrity and cycle rejection are enforced in the service layer.
        group_columns = {row[1] for row in db.execute('PRAGMA table_info(knowledge_groups)')}
        if 'parent_id' not in group_columns:
            db.execute('ALTER TABLE knowledge_groups ADD COLUMN parent_id TEXT')
        db.execute('CREATE INDEX IF NOT EXISTS knowledge_groups_parent ON knowledge_groups(parent_id)')
        db.execute("UPDATE documents SET status='queued' WHERE status='indexing' AND deleted=0")
        # Older source failures may predate retry receipts; preserve any existing backoff.
        db.execute("""INSERT OR IGNORE INTO source_retry(document_id,attempts,next_attempt)
            SELECT d.id,0,0 FROM documents d JOIN document_sources s ON s.document_id=d.id
            WHERE d.deleted=0 AND d.source_key<>'' AND s.enabled=1
                AND d.status='failed' AND d.pending_version IS NOT NULL""")

        prior = self._get_config(db)
        base = {k: prior.get(k) for k in ('generation',)}
        base['mode'] = MODE
        base['approved_origins'] = prior.get('approved_origins', [])
        # generation must stay stable across restarts; it only changes when the
        # admin explicitly saves settings.
        base['generation'] = int(prior.get('generation', 0))
        db.execute("INSERT OR REPLACE INTO config(key,payload) VALUES('embedding',?)",
                   (json.dumps(base),))
        # Migration flag is set exactly once for every DB (fresh and legacy) so a
        # restart never re-triggers a legacy backup or re-migration.
        if not migrated:
            db.execute("INSERT OR REPLACE INTO config(key,payload) VALUES('local_faiss_migrated','1')")

    def _reconcile_queue(self, db):
        """Queue active/pending non-blocked docs that have no saved vectors yet.

        This is idempotent: docs that already have vectors in ``local_vectors`` for
        their latest active version are left untouched, so a restart rebuilds the
        in-memory index from stored blobs without re-embedding their text.
        """
        rows = db.execute(
            "SELECT id, active_version, pending_version FROM documents "
            "WHERE deleted=0 AND status!='blocked' AND (pending_version IS NOT NULL OR active_version IS NOT NULL)"
        ).fetchall()
        for row in rows:
            target = row['pending_version'] or row['active_version']
            if row['pending_version'] is not None:
                continue
            has = db.execute('''
                SELECT 1 FROM local_vectors lv JOIN chunks c ON c.id=lv.chunk_id
                WHERE c.document_id=? AND c.version=? LIMIT 1
            ''', (row['id'], target)).fetchone()
            if not has:
                db.execute("UPDATE documents SET pending_version=?, status='queued', error='' WHERE id=?",
                           (target, row['id']))

    def _bootstrap_snapshot_guarded(self):
        """Build the initial in-memory snapshot from saved blobs (no embedding).

        Runs AFTER schema/reconcile are committed.  A missing faiss dependency or
        a corrupt cached blob must never crash KB startup: leave the snapshot
        None so search degrades to FTS with a warning instead of false readiness.
        """
        try:
            with self.connect() as db:
                target = self._vector_revision(db)
            if target > 0 and self._index_available():
                built = self._build_snapshot(target)
                if built is not None:
                    self._snapshot = built
        except Exception:
            # Never report false readiness; FTS fallback stays usable.
            self._snapshot = None

    # ------------------------------------------------------------------ config
    @staticmethod
    def _get_config(db):
        row = db.execute("SELECT payload FROM config WHERE key='embedding'").fetchone()
        return json.loads(row[0]) if row else {}

    @staticmethod
    def _mode(config):
        mode = config.get('mode')
        return mode if mode in (MODE, 'keyword', 'hybrid') else MODE

    @staticmethod
    def _bump(db):
        db.execute("INSERT INTO config(key,payload) VALUES('revision','1') ON CONFLICT(key) DO UPDATE SET payload=CAST(CAST(payload AS INTEGER)+1 AS TEXT)")

    @staticmethod
    def _revision(db):
        row = db.execute("SELECT payload FROM config WHERE key='revision'").fetchone()
        return int(row[0]) if row else 0

    @staticmethod
    def _bump_vector(db):
        db.execute("INSERT INTO config(key,payload) VALUES('vector_revision','1') ON CONFLICT(key) DO UPDATE SET payload=CAST(CAST(payload AS INTEGER)+1 AS TEXT)")

    @staticmethod
    def _vector_revision(db):
        row = db.execute("SELECT payload FROM config WHERE key='vector_revision'").fetchone()
        return int(row[0]) if row else 0

    def configuration(self):
        with self.connect() as db:
            config = self._get_config(db)
            return {**config, 'mode': self._mode(config)}

    # ------------------------------------------------------------------ snapshot
    def _index_available(self):
        return self._memory_index_factory is not None or self._local is not None

    def _new_index(self):
        if self._memory_index_factory is not None:
            return self._memory_index_factory()
        if self._local is not None:
            return self._local.MemoryIndex()
        return None

    def _vector_mode(self, config):
        return self._mode(config) == MODE and self.embedder is not None and self._index_available()

    def _build_snapshot(self, target_rev):
        index = self._new_index()
        if index is None:
            return None
        with self.connect(readonly=True) as db:
            db.execute('BEGIN')
            if self._vector_revision(db) != target_rev:
                return None
            rows = db.execute('''SELECT c.id, lv.embedding FROM local_vectors lv
                JOIN chunks c ON c.id=lv.chunk_id JOIN documents d ON d.id=c.document_id
                WHERE d.deleted=0 AND d.active_version=c.version''')
            index.replace(target_rev, ((row[0], row[1]) for row in rows))
        return index

    def _publish_snapshot(self, target_rev):
        """Rebuild and atomically swap the snapshot; used by writers (background)."""
        if not self._index_available():
            return None
        with self._snapshot_lock:
            snapshot = self._snapshot
            if snapshot is not None and getattr(snapshot, 'revision', None) == target_rev:
                return snapshot
            try:
                built = self._build_snapshot(target_rev)
            except Exception:
                # Keep the previous snapshot usable; search will fall back to FTS until
                # a successful rebuild is possible.
                built = None
            if built is not None:
                with self.connect() as db:
                    if self._vector_revision(db) == target_rev:
                        self._snapshot = built
            return self._snapshot if (self._snapshot is not None and getattr(self._snapshot, 'revision', None) == target_rev) else None

    def _ensure_snapshot(self, target_rev):
        """Non-blocking singleflight rebuild for search; None means FTS fallback."""
        snapshot = self._snapshot
        if snapshot is not None and getattr(snapshot, 'revision', None) == target_rev:
            return snapshot
        if not self._snapshot_lock.acquire(blocking=False):
            return None
        try:
            snapshot = self._snapshot
            if snapshot is not None and getattr(snapshot, 'revision', None) == target_rev:
                return snapshot
            built = None
            try:
                built = self._build_snapshot(target_rev)
            except Exception:
                built = None
            if built is not None:
                with self.connect() as db:
                    if self._vector_revision(db) == target_rev:
                        self._snapshot = built
            return self._snapshot if (self._snapshot is not None and getattr(self._snapshot, 'revision', None) == target_rev) else None
        finally:
            self._snapshot_lock.release()

    def _snapshot_ready(self):
        snap = self._snapshot
        return snap is not None and getattr(snap, 'count', 0) > 0

    def _snapshot_count(self):
        snap = self._snapshot
        return getattr(snap, 'count', 0) if snap is not None else 0

    # ------------------------------------------------------------------ settings
    def _queued_count(self):
        try:
            with self.connect() as db:
                row = db.execute(
                    "SELECT count(*) FROM documents WHERE deleted=0 AND status IN ('queued','indexing') AND pending_version IS NOT NULL"
                ).fetchone()
                return int(row[0]) if row else 0
        except sqlite3.Error:
            return 0

    def _has_vectors(self):
        """True when there are any vector blobs for currently-active documents."""
        try:
            with self.connect() as db:
                row = db.execute('''SELECT 1 FROM local_vectors lv
                    JOIN chunks c ON c.id=lv.chunk_id
                    JOIN documents d ON d.id=c.document_id
                    WHERE d.deleted=0 AND d.active_version=c.version LIMIT 1''').fetchone()
                return row is not None
        except sqlite3.Error:
            return False

    def settings(self, actor):
        check_actor(actor)
        cfg = self.configuration()
        mode = self._mode(cfg)
        snapshot = self._snapshot
        vector_rev = -1
        try:
            with self.connect() as db:
                vector_rev = self._vector_revision(db)
        except sqlite3.Error:
            vector_rev = -1
        index_ready = (
            snapshot is not None
            and snapshot.revision == vector_rev
            and snapshot.count > 0
        )
        runtime = {}
        if self.embedder is not None and hasattr(self.embedder, 'status') and callable(self.embedder.status):
            try:
                runtime = self.embedder.status()  # nonblocking
            except Exception:
                runtime = {'ready': False, 'queued': 0}
        runtime_ready = bool(runtime.get('ready', True)) if self.embedder is not None else False
        with self.connect(readonly=True) as db:
            self._acl_sql(db, actor)
            queued = db.execute("SELECT count(*) FROM documents WHERE source_key='' AND deleted=0 AND status IN ('queued','indexing') AND pending_version IS NOT NULL AND knowledge_visible(id)=1").fetchone()[0]
            vectors = db.execute("SELECT count(*) FROM local_vectors lv JOIN chunks c ON c.id=lv.chunk_id JOIN documents d ON d.id=c.document_id WHERE d.source_key='' AND d.deleted=0 AND d.active_version=c.version AND knowledge_visible(d.id)=1").fetchone()[0] if snapshot is not None else 0
        warning = ''
        if mode == MODE:
            if not index_ready:
                warning = '向量索引重建中，关键词检索可用。'
            elif not runtime_ready:
                warning = '正在加载本地嵌入模型，关键词检索可用。' if runtime.get('loading') else '首次检索时自动加载本地模型。'
        return {
            'engine': MODE,
            'model': MODEL_NAME,
            'dimensions': DIMENSIONS,
            'mode': mode,
            'approved_origins': cfg.get('approved_origins', []),
            'configured': True,
            'ready': bool(index_ready and runtime_ready),
            'indexready': bool(index_ready),
            'queued': queued,
            'vectors': vectors,
            'budget': _MEMORY_BUDGET,
            'runtime': runtime,
            'warning': warning,
        }

    def save_settings(self, actor, payload):
        check_actor(actor, admin=True)
        if not isinstance(payload, dict) or set(payload) - {'mode', 'endpoint', 'model', 'api_key', 'approved_origins'}:
            raise AssistantError('知识库设置格式无效。')
        mode = payload.get('mode', MODE)
        if mode not in (MODE, 'keyword', 'hybrid'):
            raise AssistantError('知识库检索方式不受支持。')
        origins = payload.get('approved_origins')
        if not isinstance(origins, list) or not 1 <= len(origins) <= 20:
            raise AssistantError('请填写允许接收公司资料的聊天模型域名。')
        approved = sorted({origin(item) for item in origins})
        with self._settings_lock:
            prior = self.configuration()
            # Remote endpoint/model/api_key are accepted for one-generation migration
            # from the previous UI but are never stored or used again.
            changed = approved != prior.get('approved_origins', [])
            candidate = {
                'mode': MODE,
                'approved_origins': approved,
                'generation': int(prior.get('generation', 0)) + int(changed),
            }
            with self.connect() as db:
                db.execute('BEGIN IMMEDIATE')
                db.execute("INSERT OR REPLACE INTO config(key,payload) VALUES('embedding',?)",
                           (json.dumps(candidate),))
                # A legacy/local settings save signals admin intent to (re)index.  Requeue
                # only failed reindex attempts that still carry a pending version; blocked
                # sensitive files must never be requeued.
                requeued = db.execute("UPDATE documents SET status='queued',error='' WHERE status='failed' AND pending_version IS NOT NULL AND deleted=0").rowcount
                if changed or requeued:
                    self._bump(db)
        if requeued:
            self._ensure_worker()
            self._wake.set()
        return self.settings(actor)

    # ------------------------------------------------------------------ public rows
    def _acl_sql(self, db, actor):
        allowed = {row[0]: False for row in db.execute("SELECT id FROM documents WHERE source_key<>''")}
        for row in db.execute('SELECT document_id,payload,enabled FROM document_sources'):
            try:
                allowed[row['document_id']] = bool(actor and row['enabled'] and self.source_acl
                    and self.source_acl(actor, json.loads(row['payload'])))
            except Exception:
                allowed[row['document_id']] = False
        db.create_function('knowledge_visible', 1, lambda identity: int(allowed.get(identity, True)))
        stamp = hashlib.sha256(json.dumps(allowed, sort_keys=True).encode()).hexdigest()
        db.create_function('knowledge_acl_stamp', 0, lambda: stamp)
        return 'knowledge_visible(documents.id)=1'

    def result_current(self, actor, result):
        check_actor(actor)
        stamp = result.get('acl_revision') if isinstance(result, dict) else None
        if stamp is None:
            return True
        with self.connect(readonly=True) as db:
            self._acl_sql(db, actor)
            return stamp == db.execute('SELECT knowledge_acl_stamp()').fetchone()[0]

    def cached_turn_current(self, actor, turn):
        sources = [source for source in turn.get('sources', []) if source.get('kind') == 'company_knowledge']
        if not sources:
            return True
        with self.connect(readonly=True) as db:
            self._acl_sql(db, actor)
            for source in sources:
                data = source.get('data') or {}
                row = db.execute('SELECT source_key,deleted,active_version,knowledge_visible(id) AS visible FROM documents WHERE id=?', (data.get('document_id'),)).fetchone()
                if not row:
                    if data.get('source_kind') == 'document_library':
                        return False
                    continue
                if row['source_key'] and (not row['visible'] or row['deleted'] or row['active_version'] != data.get('version')):
                    return False
        return True

    def filter_cached_turn(self, actor, turn):
        if self.cached_turn_current(actor, turn):
            return turn
        result = copy.deepcopy(turn)
        result.update(answer='来源文件或当前授权已变化，请重新查询。', sources=[], output_files=[], downloads=[],
            process=[], file_ids=[], plan=None, answer_document=None, resume_available=False, knowledge_access_changed=True)
        result.pop('_draft_answer', None)
        return result

    def bind_source_artifacts(self, result):
        sources = [source for source in result.get('sources', []) if source.get('kind') == 'company_knowledge']
        if not sources:
            return
        identities = {item['id'] for item in result.get('output_files', []) if isinstance(item, dict) and item.get('id')}
        identities.update(item['id'] for item in result.get('downloads', []) if isinstance(item, dict) and item.get('id'))
        with self.connect() as db:
            for identity in identities:
                db.execute('INSERT OR REPLACE INTO source_artifacts VALUES(?,?)', (identity, json.dumps(sources, ensure_ascii=False)))

    def artifact_current(self, actor, identity):
        with self.connect(readonly=True) as db:
            row = db.execute('SELECT sources FROM source_artifacts WHERE id=?', (identity,)).fetchone()
        return not row or self.cached_turn_current(actor, {'sources': json.loads(row[0])})

    def _require_visible(self, db, actor, row):
        if not row or not db.execute('SELECT 1 FROM documents WHERE id=? AND ' + self._acl_sql(db, actor), (row['id'],)).fetchone():
            raise AssistantError('知识库文件不存在或当前无权访问。', 404)

    @staticmethod
    def _public(row, actor):
        result = {key: row[key] for key in ('id', 'name', 'owner_name', 'active_version', 'status', 'error', 'updated_at', 'size', 'chunks')} | {
            'version': row['revision'], 'can_edit': bool(actor.get('is_admin') or row['owner'] == actor['id'])}
        if 'group_id' in row.keys():
            result['group_id'] = row['group_id']
        if 'group_name' in row.keys():
            result['group_name'] = row['group_name']
        if 'source_key' in row.keys() and row['source_key']:
            result.update(can_edit=False, source_kind='document_library')
        return result

    @staticmethod
    def _editable(row, actor, revision):
        if not row:
            raise AssistantError('知识库文件不存在。', 404)
        if 'source_key' in row.keys() and row['source_key']:
            raise AssistantError('来源文件请在文件库中修改或替换版本。', 403)
        if not actor.get('is_admin') and row['owner'] != actor['id']:
            raise AssistantError('只有上传者或管理员可以修改此共享文件。', 403)
        if type(revision) is not int or revision != row['revision']:
            raise AssistantError('文件版本已变化，请刷新后重新操作。', 409)

    @staticmethod
    def _document_filter(actor, query):
        deleted = str(query.get('deleted', '0')) == '1'
        q = str(query.get('q') or '').strip()[:200]
        # Qualify every column against `documents`: the FROM joins knowledge_groups,
        # whose columns (name/owner_name/owner) overlap and would otherwise make the
        # predicates below ambiguous when q/trash filters are present.
        sql, values = "documents.source_key='' AND documents.deleted=?", [int(deleted)]
        if deleted and not actor.get('is_admin'):
            sql += ' AND documents.owner=?'
            values.append(actor['id'])
        if q:
            sql += " AND (instr(lower(documents.name),lower(?))>0 OR instr(lower(documents.owner_name),lower(?))>0)"
            values += [q, q]
        group_filter = str(query.get('group') or '').strip()
        if not deleted and group_filter and group_filter not in ('all', '*'):
            if group_filter in ('ungrouped', 'none', '0'):
                sql += ' AND NOT EXISTS (SELECT 1 FROM doc_group_members m WHERE m.document_id=documents.id)'
            else:
                sql += ' AND EXISTS (SELECT 1 FROM doc_group_members m WHERE m.document_id=documents.id AND m.group_id=?)'
                values.append(group_filter)
        return sql, values

    def _group_selection(self, db, actor, query):
        query = {'q': str(query.get('q') or '').strip()[:200], 'group': str(query.get('group') or '')}
        sql, values = self._document_filter(actor, query)
        sql += ' AND ' + self._acl_sql(db, actor)
        if not actor.get('is_admin'):
            sql += ' AND documents.owner=?'
            values.append(actor['id'])
        rows = db.execute('SELECT documents.id,documents.revision FROM documents WHERE ' + sql + ' ORDER BY documents.id', values).fetchall()
        ids = [row[0] for row in rows]
        fingerprint = hashlib.sha256(json.dumps(ids).encode()).hexdigest()
        revision_fingerprint = hashlib.sha256(json.dumps([list(row) for row in rows]).encode()).hexdigest()
        return ids, {**query, 'fingerprint': fingerprint, 'revision_fingerprint': revision_fingerprint, 'count': len(ids)}

    def list(self, actor, query):
        check_actor(actor)
        try:
            page = int(query.get('page', 1))
            if page < 1:
                raise ValueError()
        except (ValueError, TypeError):
            raise AssistantError('页码无效。') from None
        sql, values = self._document_filter(actor, query)
        selection = None
        with self.connect() as db:
            sql += ' AND ' + self._acl_sql(db, actor)
            if query.get('select_all') == '1' and str(query.get('deleted', '0')) != '1':
                _, selection = self._group_selection(db, actor, query)
            total = db.execute('SELECT count(*) FROM documents WHERE ' + sql, values).fetchone()[0]
            # Clamp to the last valid page so a document removed/deleted (by this user
            # or another) on the final page never leaves the UI on an empty page.
            last_page = max(1, -(-total // 20))
            if page > last_page:
                page = last_page
            rows = db.execute('SELECT documents.*, g.id AS group_id, g.name AS group_name FROM documents '
                              'LEFT JOIN doc_group_members gm ON gm.document_id=documents.id '
                              'LEFT JOIN knowledge_groups g ON g.id=gm.group_id '
                              'WHERE ' + sql + ' ORDER BY documents.updated_at DESC,documents.id LIMIT 20 OFFSET ?',
                              [*values, (page - 1) * 20]).fetchall()
            revision = self._revision(db)
            acl_revision = db.execute('SELECT knowledge_acl_stamp()').fetchone()[0]
        return {'items': [self._public(row, actor) for row in rows], 'total': total, 'page': page, 'page_size': 20,
                'is_admin': bool(actor.get('is_admin')), 'settings': self.settings(actor), 'revision': revision,
                'acl_revision': acl_revision,
                **({'selection': selection} if selection is not None else {})}

    def upload(self, actor, name, content, *, document_id='', revision=None):
        """Backward-compatible alias delegating to the streaming upload path."""
        return self.upload_file(actor, name, content, document_id=document_id, revision=revision)

    def upload_file(self, actor, name, content, *, document_id='', revision=None):
        return self._upload_file(actor, name, content, document_id=document_id, revision=revision)

    def upsert_source(self, source_key, source, name, content):
        """Internal ingestion only; source provenance never comes from an upload request."""
        if not isinstance(source_key, str) or not source_key or not isinstance(source, dict):
            raise AssistantError('来源文件信息无效。')
        identity = uuid.uuid5(uuid.NAMESPACE_URL, 'document-library:' + source_key).hex
        return self._upload_file({'id': '@document-library', 'name': 'Document Library'}, name, content,
            document_id=identity, source_key=source_key, source=source)

    def invalidate_sources(self, prefix):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            rows = db.execute('SELECT document_id,payload FROM document_sources WHERE substr(source_key,1,?)=? AND enabled=1', (len(prefix), prefix)).fetchall()
            for row in rows:
                if self.source_current is not None and self.source_current(json.loads(row['payload'])):
                    continue
                db.execute('UPDATE document_sources SET enabled=0 WHERE document_id=?', (row[0],))
                db.execute('UPDATE documents SET active_version=NULL,revision=revision+1 WHERE id=?', (row[0],))
            if rows:
                self._bump(db)

    def reindex_source(self, source_key, source, request_id):
        """Internally requeue an upserted source version once per durable request."""
        if not isinstance(source_key, str) or not source_key or not isinstance(source, dict):
            raise AssistantError('来源文件信息无效。')
        try:
            if not isinstance(request_id, str) or str(uuid.UUID(request_id)) != request_id.lower():
                raise ValueError()
        except (ValueError, AttributeError):
            raise AssistantError('重新入库请求标识必须为 UUID。') from None
        request_id = request_id.lower()
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT d.*,s.payload,s.enabled FROM document_sources s JOIN documents d ON d.id=s.document_id '
                'WHERE s.source_key=? AND d.source_key=s.source_key', (source_key,)).fetchone()
            if not row or row['deleted'] or row['owner'] != '@document-library':
                raise AssistantError('来源文件尚未入库或已移除。', 404)
            if (not row['enabled'] or json.loads(row['payload']) != source
                    or self.source_current is None or not self.source_current(source)):
                raise AssistantError('来源版本已变化，未重新入库。', 409)
            duplicate = db.execute('SELECT 1 FROM source_reindex_receipts WHERE source_key=? AND request_id=?',
                (source_key, request_id)).fetchone() is not None
            if not duplicate:
                version = db.execute('SELECT MAX(version) FROM versions WHERE document_id=?', (row['id'],)).fetchone()[0]
                if version is None:
                    raise AssistantError('来源文件版本缺失，请先同步文件。', 409)
                db.execute('INSERT INTO source_reindex_receipts VALUES(?,?)', (source_key, request_id))
                db.execute("UPDATE documents SET pending_version=?,status='queued',revision=revision+1,error='',updated_at=? WHERE id=?",
                    (version, time.time(), row['id']))
                db.execute('DELETE FROM index_staging WHERE document_id=?', (row['id'],))
                db.execute('DELETE FROM source_retry WHERE document_id=?', (row['id'],))
                self._bump(db)
            result = self._public(db.execute('SELECT * FROM documents WHERE id=?', (row['id'],)).fetchone(),
                {'id': '@document-library'})
        self._ensure_worker()
        self._wake.set()
        return {**result, 'duplicate': duplicate, 'request_id': request_id}

    def remove_sources(self, prefix):
        """Remove obsolete managed sources after canonical configuration removal."""
        if not isinstance(prefix, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,64}/(?:[A-Za-z0-9_-]{1,200}/)?', prefix):
            raise AssistantError('请指定完整来源表或记录前缀。')
        if self.source_current is None:
            raise AssistantError('无法核对来源状态，未清理文件。', 409)
        removed, skipped, originals = 0, 0, []
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            rows = db.execute('SELECT d.id,s.payload FROM document_sources s JOIN documents d ON d.id=s.document_id '
                "WHERE substr(s.source_key,1,?)=? AND d.source_key=s.source_key AND d.owner='@document-library'",
                (len(prefix), prefix)).fetchall()
            for row in rows:
                if self.source_current(json.loads(row['payload'])) is not False:
                    skipped += 1
                    continue
                identity = row['id']
                originals.extend(dict(version) for version in db.execute('SELECT * FROM versions WHERE document_id=?', (identity,)))
                db.execute('DELETE FROM chunks_fts WHERE rowid IN (SELECT id FROM chunks WHERE document_id=?)', (identity,))
                db.execute('DELETE FROM local_vectors WHERE chunk_id IN (SELECT id FROM chunks WHERE document_id=?)', (identity,))
                for table in ('chunks', 'index_staging', 'source_retry', 'doc_group_members', 'source_versions', 'document_analysis', 'versions', 'document_sources'):
                    db.execute(f'DELETE FROM {table} WHERE document_id=?', (identity,))
                db.execute('DELETE FROM documents WHERE id=?', (identity,))
                removed += 1
            if removed:
                self._bump(db)
                self._bump_vector(db)
            vector_revision = self._vector_revision(db)
        if removed:
            # Recheck under a writer lock: a new upsert may already reuse the same path.
            with suppress(OSError, ValueError), self.connect() as db:
                db.execute('BEGIN IMMEDIATE')
                referenced = {(self.root / row[0]).resolve() for row in db.execute('SELECT path FROM versions')}
                for version in originals:
                    expected = self.root / 'originals' / version['document_id'] / (str(version['version']) + Path(version['name']).suffix.lower())
                    with suppress(OSError, ValueError):
                        path = (self.root / version['path']).resolve()
                        if (re.fullmatch(r'[a-f0-9]{32}', version['document_id']) and path == expected
                                and path not in referenced and expected.parent.resolve() == expected.parent
                                and not expected.is_symlink() and expected.is_file()):
                            expected.unlink()
        # Deletion may already have committed on a failed attempt; retries must publish too.
        if self._index_available() and self._publish_snapshot(vector_revision) is None:
            raise AssistantError('来源索引已清理，向量快照尚未更新，请稍后重试。', 503)
        return {'removed': removed, 'skipped': skipped}

    def source_status(self, prefix):
        states = [item['status'] for item in self.source_states(prefix).values()]
        for state in ('blocked', 'failed', 'indexing', 'queued'):
            if state in states:
                return state
        return 'ready' if states else 'none'

    def source_states(self, prefix):
        with self.connect(readonly=True) as db:
            result = {}
            for row in db.execute('SELECT s.source_key,d.id,d.active_version,d.status,d.error FROM documents d JOIN document_sources s ON s.document_id=d.id WHERE substr(s.source_key,1,?)=? AND s.enabled=1 AND d.deleted=0', (len(prefix), prefix)):
                missing = row['status'] == 'ready' and not self._source_index_ready(db, row['id'], row['active_version'])
                result[row['source_key']] = {'status': 'queued' if missing else row['status'],
                    'error': '知识库索引缺失，等待补入。' if missing else row['error'], 'missing': missing}
            return result

    def _source_index_ready(self, db, identity, version):
        chunks, vectors = db.execute('SELECT count(*),count(lv.chunk_id) FROM chunks c LEFT JOIN local_vectors lv ON lv.chunk_id=c.id WHERE c.document_id=? AND c.version=?', (identity, version)).fetchone()
        return bool(chunks and (not self._vector_mode(self._get_config(db)) or chunks == vectors))

    def _upload_file(self, actor, name, content, *, document_id='', revision=None, source_key='', source=None):
        """Stage bytes before the short metadata transaction; never lock while copying."""
        check_actor(actor)
        name = str(name or '').replace('\\', '/').split('/')[-1].strip()
        if not name or len(name) > 180 or any(ord(c) < 32 for c in name) or Path(name).suffix.lower() not in EXTENSIONS:
            raise AssistantError('请选择 PDF、Word、Excel、文本或图片文件。')
        if sensitive_document(name):
            raise AssistantError('文件名包含疑似敏感信息，请修改后再上传。')
        if not isinstance(content, bytes) and not callable(getattr(content, 'read', None)):
            raise AssistantError('文件内容格式无效。')
        if document_id and not source_key:
            with self.connect() as db:
                self._editable(db.execute('SELECT * FROM documents WHERE id=?', (document_id,)).fetchone(), actor, revision)
        staging = self.root / 'uploads'
        staging.mkdir(exist_ok=True)
        temporary = staging / (uuid.uuid4().hex + '.part')
        target, committed = None, False
        try:
            digest = hashlib.sha256()
            size = self._persist_content(content, temporary, digest)
            hexdigest = digest.hexdigest()
            with self.connect() as db:
                db.execute('BEGIN IMMEDIATE')
                result, target = self._commit_upload(db, actor, name, temporary, size, hexdigest, document_id, revision,
                    source_key=source_key, source=source)
            committed = True
        finally:
            with suppress(OSError):
                temporary.unlink(missing_ok=True)
            if target is not None and not committed:
                with suppress(OSError):
                    target.unlink(missing_ok=True)
        self._ensure_worker()
        self._wake.set()
        return result

    def _commit_upload(self, db, actor, name, temporary, size, hexdigest, document_id, revision, *, source_key='', source=None):
        if source_key and (self.source_current is None or not self.source_current(source)):
            raise AssistantError('来源版本已变化，旧入库任务未覆盖当前文档。', 409)
        row = None
        if document_id:
            row = db.execute('SELECT * FROM documents WHERE id=?', (document_id,)).fetchone()
            if not source_key:
                self._editable(row, actor, revision)
            if row and row['deleted']:
                raise AssistantError('文件已删除，请先恢复。', 409)
        if not document_id:
            row = db.execute("SELECT d.* FROM documents d JOIN versions v ON v.document_id=d.id WHERE d.deleted=0 AND d.source_key='' AND v.hash=? AND v.version IN (d.active_version,d.pending_version) LIMIT 1", (hexdigest,)).fetchone()
        if source_key:
            prior_source = db.execute('SELECT payload,enabled FROM document_sources WHERE document_id=?', (document_id,)).fetchone()
            unchanged_source = bool(prior_source and prior_source['enabled'] and json.loads(prior_source['payload']) == source)
            db.execute('INSERT INTO document_sources VALUES(?,?,?,1) ON CONFLICT(document_id) DO UPDATE SET payload=excluded.payload,enabled=1',
                (document_id, source_key, json.dumps(source, ensure_ascii=False)))
            latest = db.execute('SELECT v.version,v.hash,v.name,s.payload FROM versions v LEFT JOIN source_versions s ON s.document_id=v.document_id AND s.version=v.version WHERE v.document_id=? ORDER BY v.version DESC LIMIT 1', (document_id,)).fetchone()
            policy_fields = ('table_key', 'record_id', 'origin', 'scopes', 'attachment_id', 'field_id')
            if row and latest and latest['payload'] and latest['hash'] == hexdigest and latest['name'] == name:
                prior = json.loads(latest['payload'])
                if all(prior.get(key) == source.get(key) for key in policy_fields):
                    ready = self._source_index_ready(db, document_id, latest['version'])
                    if not unchanged_source or row['status'] == 'ready' and (not ready or row['active_version'] != latest['version']):
                        db.execute('UPDATE documents SET active_version=?,pending_version=?,status=?,revision=revision+1,error=\'\',updated_at=? WHERE id=?',
                            (latest['version'] if ready else None, None if ready else latest['version'], 'ready' if ready else 'queued', time.time(), document_id))
                        self._bump(db)
                        if ready and row['active_version'] != latest['version']:
                            self._bump_vector(db)
                    return {**self._public(db.execute('SELECT * FROM documents WHERE id=?', (document_id,)).fetchone(), actor), 'duplicate': True}, None
        if row:
            identical = db.execute('SELECT 1 FROM versions WHERE document_id=? AND hash=? AND version IN (?,?)',
                (row['id'], hexdigest, row['active_version'], row['pending_version'])).fetchone()
            if identical and (not source_key or unchanged_source):
                return {**self._public(row, actor), 'duplicate': True}, None
        identity = document_id or uuid.uuid4().hex
        version = db.execute('SELECT COALESCE(MAX(version),0)+1 FROM versions WHERE document_id=?', (identity,)).fetchone()[0]
        relative = Path('originals') / identity / (str(version) + Path(name).suffix.lower())
        target = self.root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        stamp = time.time()
        db.execute('INSERT INTO versions VALUES(?,?,?,?,?,?,?)', (identity, version, name, hexdigest, str(relative), size, stamp))
        if source_key:
            db.execute('INSERT INTO source_versions VALUES(?,?,?)', (identity, version, json.dumps(source, ensure_ascii=False)))
        if row and document_id:
            db.execute("UPDATE documents SET name=?,pending_version=?,revision=revision+1,status='queued',error='',updated_at=?,size=? WHERE id=?",
                (name, version, stamp, size, identity))
        else:
            db.execute("INSERT INTO documents(id,owner,owner_name,name,pending_version,status,updated_at,size,source_key) VALUES(?,?,?,?,?,'queued',?,?,?)",
                (identity, actor['id'], safe_text(actor.get('name') or '用户', limit=100), name, version, stamp, size, source_key))
        if source_key:
            db.execute('UPDATE documents SET active_version=NULL WHERE id=?', (identity,))
        self._bump(db)
        result = self._public(db.execute('SELECT * FROM documents WHERE id=?', (identity,)).fetchone(), actor)
        os.replace(temporary, target)
        return result, target

    def _persist_content(self, content, target, digest):
        """Stream `content` (bytes or file-like) to `target` in bounded chunks,
        updating `digest` on the fly so peak RAM stays O(chunk)."""
        source = io.BytesIO(content) if isinstance(content, bytes) else content
        if isinstance(content, bytes) and len(content) > MAX_FILE_BYTES:
            raise AssistantError('单文件最大100MiB。', 413)
        size = 0
        with open(target, 'wb') as out:
            while True:
                chunk = source.read(_STREAM_CHUNK)
                if not chunk:
                    break
                if not isinstance(chunk, bytes):
                    raise AssistantError('文件内容格式无效。')
                size += len(chunk)
                if size > MAX_FILE_BYTES:
                    raise AssistantError('单文件最大100MiB。', 413)
                out.write(chunk)
                digest.update(chunk)
        if not size:
            raise AssistantError('文件内容不能为空。', 413)
        return size

    def _ensure_worker(self):
        if not self.start_worker or self._stop.is_set():
            return
        with self._worker_lock:
            if self._worker is None or not self._worker.is_alive():
                self._worker = threading.Thread(target=self._run, name='CompanyKnowledge', daemon=True)
                self._worker.start()

    def add_worker_task(self, task):
        with self._worker_lock:
            if task not in self._worker_tasks:
                self._worker_tasks.append(task)
        self._ensure_worker()
        self._wake.set()

    def _run(self):
        from upload_event_module.services.process_lifetime import lower_current_thread_priority
        lower_current_thread_priority()
        prepared_since_index = 0
        while not self._stop.is_set():
            try:
                worked = False
                preparing = False
                for task in tuple(self._worker_tasks):
                    if self._stop.is_set():
                        return
                    try:
                        prepare = getattr(getattr(task, '__self__', None), 'prepare_pending', None)
                        if callable(prepare) and not self._stop.is_set() and prepare(limit=1):
                            worked = preparing = True
                            continue
                        worked = bool(task()) or worked
                    except Exception:
                        logging.warning('Knowledge source synchronization failed; retry deferred')
                if self._stop.is_set():
                    return
                if preparing:
                    prepared_since_index += 1
                    if prepared_since_index < 4:
                        continue
                prepared_since_index = 0
                if self.process_one() or worked:
                    continue
            except (OSError, sqlite3.Error):
                pass
            self._wake.wait(30)
            self._wake.clear()

    def _path(self, relative):
        path = (self.root / relative).resolve()
        if not path.is_relative_to(self.root / 'originals') or not path.is_file():
            raise AssistantError('知识库原文件缺失，请重新上传文件。', 404)
        return path

    def _source_sections(self, doc, version, sections):
        if self.source_summarizer is None or not doc['source_key']:
            return sections
        with self.connect(readonly=True) as db:
            row = db.execute('SELECT payload FROM source_versions WHERE document_id=? AND version=?',
                (doc['id'], version['version'])).fetchone()
        if row is None or json.loads(row[0]).get('kind') != 'attachment':
            return sections
        from .lighthouse_knowledge_summary import summary_parts
        result = []
        for part, section in enumerate(summary_parts(sections)):
            if self._stop.is_set():
                return None
            with self.connect(readonly=True) as db:
                current = db.execute('SELECT revision,deleted,index_token FROM documents WHERE id=?', (doc['id'],)).fetchone()
                if not current or current['deleted'] or current['revision'] != doc['revision'] or current['index_token'] != doc['index_token']:
                    return None
                saved = db.execute('SELECT text,location,summary FROM document_analysis WHERE document_id=? AND version=? AND part=?',
                    (doc['id'], version['version'], part)).fetchone()
            summary = saved['summary'] if saved and saved['text'] == section['text'] and saved['location'] == section['location'] else ''
            if not summary:
                summary = self.source_summarizer(section['text'], version['name'], section['location'])
                if not isinstance(summary, str) or not summary.strip() or len(summary) > 12000:
                    raise AssistantError('文档摘要返回内容无效；原件已保留，将自动重试。', 503)
                if sensitive_document(summary):
                    raise AssistantError('文档摘要含疑似敏感信息，未公开入库。', 422, category='sensitive_document')
                with self.connect() as db:
                    db.execute('BEGIN IMMEDIATE')
                    current = db.execute('SELECT revision,deleted,index_token FROM documents WHERE id=?', (doc['id'],)).fetchone()
                    if not current or current['deleted'] or current['revision'] != doc['revision'] or current['index_token'] != doc['index_token']:
                        return None
                    db.execute('INSERT OR REPLACE INTO document_analysis VALUES(?,?,?,?,?,?)',
                        (doc['id'], version['version'], part, section['text'], section['location'], summary))
            result.append({'text': summary, 'location': section['location'] + ' · 模型摘要'})
        return result

    def process_one(self):
        # Indexing (including embedding and vector persistence) is serialized by this
        # lock.  Search never takes ``_index_lock`` so reads remain parallel.
        with self._index_lock:
            with self.connect() as db:
                db.execute('BEGIN IMMEDIATE')
                row = db.execute("SELECT * FROM documents WHERE deleted=0 AND pending_version IS NOT NULL AND (status='queued' OR (status='failed' AND source_key<>'' AND EXISTS (SELECT 1 FROM source_retry r JOIN document_sources s ON s.document_id=r.document_id WHERE r.document_id=documents.id AND r.next_attempt<=? AND s.enabled=1))) ORDER BY updated_at LIMIT 1", (time.time(),)).fetchone()
                if not row:
                    return False
                if row['status'] == 'failed':
                    provenance = db.execute('SELECT payload FROM document_sources WHERE document_id=?', (row['id'],)).fetchone()
                    if self.source_current is None or not self.source_current(json.loads(provenance[0])):
                        db.execute('UPDATE source_retry SET next_attempt=? WHERE document_id=?', (time.time() + 300, row['id']))
                        return False
                doc = dict(row)
                # A fresh claim token fences old work even after removal/recreation of the same source ID.
                doc['index_token'] = uuid.uuid4().hex
                cfg = self._get_config(db)
                vector_on = self._vector_mode(cfg)
                version = dict(db.execute('SELECT * FROM versions WHERE document_id=? AND version=?', (doc['id'], doc['pending_version'])).fetchone())
                db.execute("UPDATE documents SET status='indexing',error='',index_token=? WHERE id=?", (doc['index_token'], doc['id']))
                db.execute('DELETE FROM index_staging WHERE document_id=?', (doc['id'],))
            try:
                from .lighthouse_knowledge_extract import extract_sections, split_sections
                sections = (self.extractor or extract_sections)(self._path(version['path']).read_bytes(), version['name'])
                if any(sensitive_document(section['text']) for section in sections):
                    raise AssistantError('文件含疑似身份证、住址、联系方式或凭证，未公开入库。请脱敏后替换文件。', 422, category='sensitive_document')
                sections = self._source_sections(doc, version, sections)
                if sections is None:
                    return False if self._stop.is_set() else True
                chunks = split_sections(sections)
                if not chunks:
                    raise AssistantError('文件没有可检索的正文，未公开入库。', 422)
                with self.connect() as db:
                    active_count = db.execute('SELECT count(*) FROM chunks c JOIN documents d ON d.id=c.document_id WHERE d.deleted=0 AND d.active_version=c.version AND d.id<>?', (doc['id'],)).fetchone()[0]
                if active_count + len(chunks) > _MAX_ACTIVE_CHUNKS:
                    raise AssistantError('知识库已达到10万片段的内存预算，请清理旧资料后重试。', 413)
                if vector_on:
                    for start in range(0, len(chunks), _EMBED_BATCH):
                        if self._stop.is_set():
                            return False
                        with self.connect() as db:
                            current = db.execute('SELECT revision,deleted,index_token FROM documents WHERE id=?', (doc['id'],)).fetchone()
                            if not current or current['deleted'] or current['revision'] != doc['revision'] or current['index_token'] != doc['index_token']:
                                return True
                        texts = [chunk['text'] for chunk in chunks[start:start + _EMBED_BATCH]]
                        batch = self.embedder.embed(texts, cfg)
                        if not isinstance(batch, list) or len(batch) != len(texts):
                            raise AssistantError('嵌入接口未返回完整向量，原文档版本已保留，请重试。', 502)
                        packed = [(doc['id'], start + offset, _pack(vector)) for offset, vector in enumerate(batch)]
                        with self.connect() as db:
                            db.execute('BEGIN IMMEDIATE')
                            current = db.execute('SELECT revision,deleted,index_token FROM documents WHERE id=?', (doc['id'],)).fetchone()
                            if not current or current['deleted'] or current['revision'] != doc['revision'] or current['index_token'] != doc['index_token']:
                                return True
                            db.executemany('INSERT INTO index_staging VALUES(?,?,?)', packed)
                if self._stop.is_set():
                    return False
                with self.connect() as db:
                    db.execute('BEGIN IMMEDIATE')
                    current = db.execute('SELECT * FROM documents WHERE id=?', (doc['id'],)).fetchone()
                    if (not current or current['deleted'] or current['revision'] != doc['revision']
                            or current['index_token'] != doc['index_token'] or current['pending_version'] != version['version']):
                        return True
                    if vector_on and db.execute('SELECT count(*) FROM index_staging WHERE document_id=?', (doc['id'],)).fetchone()[0] != len(chunks):
                        raise AssistantError('嵌入向量不完整，原文档版本已保留，请重试。', 502)
                    active_count = db.execute('SELECT count(*) FROM chunks c JOIN documents d ON d.id=c.document_id WHERE d.deleted=0 AND d.active_version=c.version AND d.id<>?', (doc['id'],)).fetchone()[0]
                    if active_count + len(chunks) > _MAX_ACTIVE_CHUNKS:
                        raise AssistantError('当前单机知识库已达到10万片段上限，请管理员清理旧资料后重试。', 413)
                    old = [row[0] for row in db.execute('SELECT id FROM chunks WHERE document_id=? AND version=?', (doc['id'], version['version']))]
                    db.execute('DELETE FROM local_vectors WHERE chunk_id IN (SELECT id FROM chunks WHERE document_id=?)', (doc['id'],))
                    for identity in old:
                        db.execute('DELETE FROM chunks_fts WHERE rowid=?', (identity,))
                        db.execute('DELETE FROM local_vectors WHERE chunk_id=?', (identity,))
                    db.execute('DELETE FROM chunks WHERE document_id=? AND version=?', (doc['id'], version['version']))
                    for ordinal, chunk in enumerate(chunks):
                        cursor = db.execute('INSERT INTO chunks(document_id,version,ordinal,text,location) VALUES(?,?,?,?,?)',
                            (doc['id'], version['version'], ordinal, chunk['text'], chunk['location']))
                        identity = cursor.lastrowid
                        db.execute('INSERT INTO chunks_fts(rowid,text) VALUES(?,?)', (identity, version['name'] + '\n' + chunk['text']))
                        if vector_on:
                            db.execute('INSERT INTO local_vectors(chunk_id,embedding) SELECT ?,embedding FROM index_staging WHERE document_id=? AND ordinal=?', (identity, doc['id'], ordinal))
                    db.execute('DELETE FROM index_staging WHERE document_id=?', (doc['id'],))
                    db.execute('DELETE FROM source_retry WHERE document_id=?', (doc['id'],))
                    db.execute("UPDATE documents SET active_version=?,pending_version=NULL,status='ready',error='',chunks=?,updated_at=?,name=?,size=? WHERE id=?",
                        (version['version'], len(chunks), time.time(), version['name'], version['size'], doc['id']))
                    self._bump(db)
                    if vector_on:
                        self._bump_vector(db)
                        new_rev = self._vector_revision(db)
                    else:
                        new_rev = None
                if new_rev is not None:
                    self._publish_snapshot(new_rev)
                return True

            except Exception as exc:
                if self._stop.is_set():
                    return False
                message = str(exc) if isinstance(exc, AssistantError) else '文件解析或索引未完成，请检查格式后重试。'
                status = 'blocked' if getattr(exc, 'category', '') == 'sensitive_document' else 'failed'
                with self.connect() as db:
                    db.execute('BEGIN IMMEDIATE')
                    current = db.execute('SELECT revision,deleted,index_token,status FROM documents WHERE id=?', (doc['id'],)).fetchone()
                    if (not current or current['deleted'] or current['revision'] != doc['revision']
                            or current['index_token'] != doc['index_token'] or current['status'] != 'indexing'):
                        return True
                    db.execute('DELETE FROM index_staging WHERE document_id=?', (doc['id'],))
                    db.execute("UPDATE documents SET status=?,error=? WHERE id=? AND revision=? AND deleted=0 AND status='indexing'",
                        (status, safe_text(message, limit=600) or '文件包含不宜公开的信息，未入库。', doc['id'], doc['revision']))
                    if doc['source_key'] and status == 'failed':
                        previous = db.execute('SELECT attempts FROM source_retry WHERE document_id=?', (doc['id'],)).fetchone()
                        attempts = (previous[0] if previous else 0) + 1
                        db.execute('INSERT OR REPLACE INTO source_retry VALUES(?,?,?)', (doc['id'], attempts, time.time() + min(300, 30 * 2 ** min(attempts - 1, 4))))
                return True

    @staticmethod
    def _soft_delete(db, identity):
        for chunk in db.execute('SELECT id FROM chunks WHERE document_id=?', (identity,)).fetchall():
            db.execute('DELETE FROM local_vectors WHERE chunk_id=?', (chunk[0],))
        db.execute("UPDATE documents SET deleted=1,status='deleted',pending_version=NULL,revision=revision+1,updated_at=? WHERE id=?",
                   (time.time(), identity))
        db.execute('DELETE FROM index_staging WHERE document_id=?', (identity,))

    def bulk_delete(self, actor, payload):
        check_actor(actor)
        if not isinstance(payload, dict) or set(payload) not in ({'documents'}, {'selection'}):
            raise AssistantError('请选择要移入回收站的文件。')
        selection = payload.get('selection')
        documents = payload.get('documents')
        if 'selection' in payload:
            if (not isinstance(selection, dict) or not isinstance(selection.get('q'), str) or len(selection['q']) > 200
                    or not isinstance(selection.get('group'), str) or len(selection['group']) > 64
                    or type(selection.get('count')) is not int or selection['count'] < 1
                    or any(not re.fullmatch(r'[a-f0-9]{64}', str(selection.get(key) or ''))
                           for key in ('fingerprint', 'revision_fingerprint'))):
                raise AssistantError('全部选择信息无效，请重新选择。')
        elif (not isinstance(documents, list) or not documents or len(documents) > 100
              or any(not isinstance(item, dict) or set(item) != {'id', 'version'}
                     or not isinstance(item['id'], str) or not item['id'] or len(item['id']) > 64
                     or type(item['version']) is not int for item in documents)
              or len({item['id'] for item in documents}) != len(documents)):
            raise AssistantError('请选择1至100个文件并提供当前版本。')
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if selection is not None:
                ids, snapshot = self._group_selection(db, actor, selection)
                if any(snapshot[key] != selection[key] for key in ('fingerprint', 'revision_fingerprint', 'count')):
                    raise AssistantError('搜索结果、权限或文件版本已变化，未删除任何文件，请重新选择。', 409)
                excluded = selection.get('excluded_ids', [])
                available = set(ids)
                if not isinstance(excluded, list) or any(not isinstance(i, str) or i not in available for i in excluded):
                    raise AssistantError('取消选择的文件无效，请重新选择。')
                excluded = set(excluded)
                documents = [{'id': identity, 'version': db.execute('SELECT revision FROM documents WHERE id=?', (identity,)).fetchone()[0]}
                             for identity in ids if identity not in excluded]
                if not documents:
                    raise AssistantError('请选择要移入回收站的文件。')
            for item in documents:
                row = db.execute('SELECT * FROM documents WHERE id=?', (item['id'],)).fetchone()
                self._editable(row, actor, item['version'])
                self._require_visible(db, actor, row)
                if row['deleted']:
                    raise AssistantError('文件已在回收站，未删除任何文件，请重新选择。', 409)
            for item in documents:
                self._soft_delete(db, item['id'])
            self._bump_vector(db)
            self._bump(db)
            new_rev = self._vector_revision(db)
        self._publish_snapshot(new_rev)
        self._ensure_worker()
        self._wake.set()
        return {'deleted': len(documents)}

    def change(self, actor, identity, action, revision):
        check_actor(actor)
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM documents WHERE id=?', (identity,)).fetchone()
            self._editable(row, actor, revision)
            if action == 'delete':
                if row['deleted']:
                    raise AssistantError('文件已在回收站。', 409)
                self._soft_delete(db, identity)
                self._bump_vector(db)
                new_rev = self._vector_revision(db)
            elif action in {'restore', 'retry'}:
                if action == 'restore' and not row['deleted'] or action == 'retry' and (row['deleted'] or row['status'] != 'failed'):
                    raise AssistantError('文件状态已变化，请刷新后重试。', 409)
                version = row['pending_version'] or row['active_version'] or db.execute('SELECT MAX(version) FROM versions WHERE document_id=?', (identity,)).fetchone()[0]
                db.execute("UPDATE documents SET deleted=0,status='queued',pending_version=?,revision=revision+1,error='',updated_at=? WHERE id=?",
                    (version, time.time(), identity))
                if action == 'restore':
                    db.execute('UPDATE documents SET active_version=NULL,chunks=0 WHERE id=?', (identity,))
                new_rev = None
            else:
                raise AssistantError('文件操作无效。')
            self._bump(db)
            result = self._public(db.execute('SELECT * FROM documents WHERE id=?', (identity,)).fetchone(), actor)
        if new_rev is not None:
            self._publish_snapshot(new_rev)
        self._ensure_worker()
        self._wake.set()
        return result

    # ------------------------------------------------------------------ groups
    def _group_row(self, actor, row):
        return {'id': row['id'], 'name': row['name'], 'owner_name': row['owner_name'],
                'parent_id': row['parent_id'] if 'parent_id' in row.keys() else None,
                'created_at': row['created_at'], 'count': row['count'],
                'can_manage': bool(actor.get('is_admin') or row['owner'] == actor['id'])}

    def _normalize_parent(self, db, parent_id, *, exclude=None):
        """Validate and normalize an optional folder parent.

        ``''``/``root``/``ungrouped``/``0`` all mean "root folder" (NULL).
        Returns the normalized parent id or ``None`` for root.  Rejects a
        missing parent (404) and, when ``exclude`` is set, the parent being the
        moved folder itself or one of its descendants (cycle, 409).
        """
        if parent_id in (None, ''):
            return None
        text = str(parent_id).strip()
        if text in ('', 'root', 'ungrouped', '0'):
            return None
        if len(text) > 64:
            raise AssistantError('上级文件夹编号无效。')
        if not db.execute('SELECT 1 FROM knowledge_groups WHERE id=?', (text,)).fetchone():
            raise AssistantError('上级文件夹不存在。', 404)
        if exclude and text == exclude:
            raise AssistantError('文件夹不能移动到自身或其子文件夹内，否则会形成循环。', 409)
        if exclude:
            descendants = self._descendant_ids(db, exclude)
            if text in descendants:
                raise AssistantError('文件夹不能移动到自身或其子文件夹内，否则会形成循环。', 409)
        return text

    def _descendant_ids(self, db, group_id):
        """Iteratively collect every descendant folder id (cycle-safe, no recursion)."""
        seen = set()
        frontier = [group_id]
        while frontier:
            current = frontier.pop()
            for row in db.execute('SELECT id FROM knowledge_groups WHERE parent_id=?', (current,)):
                child = row['id']
                if child not in seen:
                    seen.add(child)
                    frontier.append(child)
        return seen

    def groups(self, actor):
        """List all organizational groups with their current (non-deleted) file counts.

        The flat list is deliberately kept (existing APIs/tests depend on it); the
        ``parent_id`` field lets callers build a nested folder tree while existing
        flat groups (parent_id NULL) remain roots.
        """
        check_actor(actor)
        with self.connect() as db:
            self._acl_sql(db, actor)
            acl_revision = db.execute('SELECT knowledge_acl_stamp()').fetchone()[0]
            rows = db.execute('''
                SELECT g.id, g.name, g.owner, g.owner_name, g.parent_id, g.created_at,
                       (SELECT count(*) FROM doc_group_members m
                        JOIN documents d ON d.id=m.document_id
                         WHERE m.group_id=g.id AND d.source_key='' AND d.deleted=0 AND knowledge_visible(d.id)=1) AS count
                FROM knowledge_groups g ORDER BY g.created_at, g.id''').fetchall()
            hidden = {row[0] for row in db.execute('SELECT DISTINCT m.group_id FROM doc_group_members m JOIN documents d ON d.id=m.document_id WHERE d.source_key<>\'\'')}
        visible = {row['id'] for row in rows if row['count'] or row['id'] not in hidden}
        parents = {row['id']: row['parent_id'] for row in rows}
        for identity in tuple(visible):
            parent = parents.get(identity)
            while parent and parent not in visible:
                visible.add(parent)
                parent = parents.get(parent)
        rows = [row for row in rows if row['id'] in visible]
        return {'groups': [self._group_row(actor, row) for row in rows], 'total': len(rows), 'acl_revision': acl_revision}

    def create_group(self, actor, name, parent_id=None):
        """Create a new organizational group/folder (any signed-in user can create).

        ``parent_id`` is optional: when omitted the group becomes a root folder;
        when provided it must reference an existing group (missing parent -> 404).
        """
        check_actor(actor)
        name = _validate_group_name(name)
        gid = uuid.uuid4().hex
        stamp = time.time()
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            normalized = self._normalize_parent(db, parent_id)
            if db.execute('SELECT 1 FROM knowledge_groups WHERE parent_id IS ? AND lower(name)=lower(?)', (normalized, name)).fetchone():
                raise AssistantError('同一文件夹下已存在同名分组。', 409)
            db.execute('INSERT INTO knowledge_groups(id,name,owner,owner_name,parent_id,created_at) VALUES(?,?,?,?,?,?)',
                       (gid, name, actor['id'], safe_text(actor.get('name') or '用户', limit=100), normalized, stamp))
            row = db.execute('SELECT id,name,owner,owner_name,parent_id,created_at,0 AS count FROM knowledge_groups WHERE id=?', (gid,)).fetchone()
        return self._group_row(actor, row)

    def rename_group(self, actor, group_id, name, parent_id=_UNCHANGED_PARENT):
        """Rename a group/folder (creator or admin only) or move it in the tree.

        ``parent_id`` (optional) is the new parent folder; passing it moves the
        folder.  Cycles (moving a folder under itself or a descendant) and missing
        parents are rejected server-side.
        """
        check_actor(actor)
        name = _validate_group_name(name)
        group_id = str(group_id or '').strip()
        if not group_id or len(group_id) > 64:
            raise AssistantError('分组编号无效。')
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM knowledge_groups WHERE id=?', (group_id,)).fetchone()
            self._acl_sql(db, actor)
            if not row:
                raise AssistantError('分组不存在。', 404)
            if not actor.get('is_admin') and row['owner'] != actor['id']:
                raise AssistantError('仅分组创建者或管理员可管理该分组。', 403)
            # Older callers rename only; omission must not move a nested folder.
            normalized = self._normalize_parent(db, row['parent_id'] if parent_id is _UNCHANGED_PARENT else parent_id, exclude=group_id)
            if db.execute('SELECT 1 FROM knowledge_groups WHERE id<>? AND parent_id IS ? AND lower(name)=lower(?)', (group_id, normalized, name)).fetchone():
                raise AssistantError('同一文件夹下已存在同名分组。', 409)
            if normalized == row['parent_id']:
                db.execute('UPDATE knowledge_groups SET name=? WHERE id=?', (name, group_id))
            else:
                db.execute('UPDATE knowledge_groups SET name=?,parent_id=? WHERE id=?', (name, normalized, group_id))
            count = db.execute("SELECT count(*) FROM doc_group_members m JOIN documents d ON d.id=m.document_id WHERE m.group_id=? AND d.source_key='' AND d.deleted=0 AND knowledge_visible(d.id)=1", (group_id,)).fetchone()[0]
            row = db.execute('SELECT id,name,owner,owner_name,parent_id,created_at FROM knowledge_groups WHERE id=?', (group_id,)).fetchone()
        return self._group_row(actor, dict(row, count=count))

    def delete_group(self, actor, group_id):
        """Delete a group/folder.

        Member files return to ungrouped (files/index untouched).  Child folders are
        reparented to the removed folder's own parent, so deleting a folder never
        silently deletes its children or the files inside them.
        """
        check_actor(actor)
        group_id = str(group_id or '').strip()
        if not group_id or len(group_id) > 64:
            raise AssistantError('分组编号无效。')
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM knowledge_groups WHERE id=?', (group_id,)).fetchone()
            if not row:
                raise AssistantError('分组不存在。', 404)
            if not actor.get('is_admin') and row['owner'] != actor['id']:
                raise AssistantError('仅分组创建者或管理员可管理该分组。', 403)
            parent = row['parent_id']
            if db.execute('''SELECT 1 FROM knowledge_groups child JOIN knowledge_groups sibling
                             ON lower(child.name)=lower(sibling.name) AND sibling.parent_id IS ?
                             WHERE child.parent_id=? AND sibling.id<>?''', (parent, group_id, group_id)).fetchone():
                raise AssistantError('子文件夹与上级目录存在同名分组，请先重命名或移动子文件夹，再删除。', 409)
            # Reparent direct child folders to this folder's own parent.
            db.execute('UPDATE knowledge_groups SET parent_id=? WHERE parent_id=?', (parent, group_id))
            # Files in this folder return to ungrouped; files/index untouched.
            db.execute('DELETE FROM doc_group_members WHERE group_id=?', (group_id,))
            db.execute('DELETE FROM knowledge_groups WHERE id=?', (group_id,))
        return {'deleted': True}

    def _normalize_document_ids(self, document_ids):
        if not isinstance(document_ids, (list, tuple)) or not document_ids:
            raise AssistantError('请选择要分组的文件。')
        if len(document_ids) > 100:
            raise AssistantError('单次最多操作100个文件。', 413)
        ids = [str(i).strip() for i in document_ids]
        ids = list(dict.fromkeys(i for i in ids if i))
        if not ids or any(len(i) > 64 for i in ids):
            raise AssistantError('文件编号无效。')
        return ids

    def set_documents_group(self, actor, group_id, document_ids=None, *, selection=None):
        """Assign documents to a group (atomic; all must be editable by actor)."""
        check_actor(actor)
        ids = self._normalize_document_ids(document_ids) if selection is None else None
        if selection is not None and (document_ids is not None or not isinstance(selection, dict)
                or not isinstance(selection.get('q'), str) or len(selection['q']) > 200
                or not isinstance(selection.get('group'), str) or len(selection['group']) > 64
                or not re.fullmatch(r'[a-f0-9]{64}', str(selection.get('fingerprint') or ''))):
            raise AssistantError('全部选择信息无效，请重新选择。')
        target = None if group_id in (None, '', 'ungrouped', 'none') else str(group_id or '').strip()
        if target is not None and (not target or len(target) > 64):
            raise AssistantError('分组编号无效。')
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if selection is not None:
                ids, snapshot = self._group_selection(db, actor, selection)
                if snapshot['fingerprint'] != selection['fingerprint']:
                    raise AssistantError('搜索结果或文件权限已变化，未移动任何文件，请重新全选。', 409)
                excluded = selection.get('excluded_ids', [])
                available = set(ids)
                if not isinstance(excluded, list) or any(not isinstance(value, str) or value not in available for value in excluded):
                    raise AssistantError('取消选择的文件无效，请重新选择。')
                excluded = set(excluded)
                ids = [identity for identity in ids if identity not in excluded]
                if not ids:
                    raise AssistantError('请选择要分组的文件。')
            if target is not None and not db.execute('SELECT 1 FROM knowledge_groups WHERE id=?', (target,)).fetchone():
                raise AssistantError('分组不存在。', 404)
            missing, forbidden = [], []
            for identity in ids:
                row = db.execute('SELECT owner,deleted,source_key FROM documents WHERE id=?', (identity,)).fetchone()
                if not row or row['deleted']:
                    missing.append(identity)
                elif row['source_key'] or not actor.get('is_admin') and row['owner'] != actor['id']:
                    forbidden.append(identity)
            if forbidden:
                raise AssistantError('存在无权操作的文件，未做任何修改。', 403)
            if missing:
                raise AssistantError('存在已删除或不存在的文件，未做任何修改。', 404)
            if target is not None:
                # A document belongs to exactly one group; assigning moves it.
                for identity in ids:
                    db.execute('DELETE FROM doc_group_members WHERE document_id=?', (identity,))
                    db.execute('INSERT OR REPLACE INTO doc_group_members(document_id,group_id) VALUES(?,?)', (identity, target))
            else:
                db.executemany('DELETE FROM doc_group_members WHERE document_id=?', [(identity,) for identity in ids])
        return {'updated': len(ids)}

    def remove_documents_group(self, actor, group_id, document_ids):
        """Remove documents from a group / unassign them (atomic, edit permission enforced)."""
        check_actor(actor)
        ids_ = self._normalize_document_ids(document_ids)
        group_id_str = str(group_id or '').strip()
        if group_id_str and len(group_id_str) > 64:
            raise AssistantError('分组编号无效。')
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if group_id_str and not db.execute('SELECT 1 FROM knowledge_groups WHERE id=?', (group_id_str,)).fetchone():
                raise AssistantError('分组不存在。', 404)
            forbidden, missing = [], []
            for identity in ids_:
                row = db.execute('SELECT owner,deleted,source_key FROM documents WHERE id=?', (identity,)).fetchone()
                if not row or row['deleted']:
                    missing.append(identity)
                elif row['source_key'] or not actor.get('is_admin') and row['owner'] != actor['id']:
                    forbidden.append(identity)
            if forbidden:
                raise AssistantError('存在无权操作的文件，未做任何修改。', 403)
            if missing:
                raise AssistantError('存在已删除或不存在的文件，未做任何修改。', 404)
            if group_id_str:
                db.executemany('DELETE FROM doc_group_members WHERE document_id=? AND group_id=?',
                               [(identity, group_id_str) for identity in ids_])
            else:
                db.executemany('DELETE FROM doc_group_members WHERE document_id=?', [(identity,) for identity in ids_])
        return {'updated': len(ids_)}

    def _document_version(self, db, actor, identity, version=None):
        row = db.execute('SELECT * FROM documents WHERE id=?', (identity,)).fetchone()
        if not row:
            raise AssistantError('知识库文件不存在。', 404)
        self._require_visible(db, actor, row)
        owner = row['owner'] == actor['id'] or actor.get('is_admin')
        if row['deleted']:
            raise AssistantError('该知识库文件已删除，不再作为回答依据。', 410)
        number = row['active_version'] if version in (None, '') else version
        if number is None:
            if not owner:
                raise AssistantError('文件尚未完成安全检查和索引入库。', 409)
            number = row['pending_version']
        try:
            number = int(number)
        except (TypeError, ValueError):
            raise AssistantError('文件版本无效。') from None
        if number != row['active_version'] and not owner:
            raise AssistantError('该引用已不是当前版本，请查看最新公司资料。', 409)
        original = db.execute('SELECT * FROM versions WHERE document_id=? AND version=?', (identity, number)).fetchone()
        if not original:
            raise AssistantError('文件版本不存在。', 404)
        if row['source_key']:
            provenance = db.execute('SELECT payload FROM source_versions WHERE document_id=? AND version=?', (identity, number)).fetchone()
            try:
                permitted = bool(provenance and self.source_acl and self.source_acl(actor, {**json.loads(provenance[0]), 'historical_version': True}))
            except Exception:
                permitted = False
            if not permitted:
                raise AssistantError('文件版本不存在或当前无权访问。', 404)
        return row, original

    def document(self, actor, identity, query):
        check_actor(actor)
        try:
            page = max(1, int(query.get('page', 1)))
            ordinal = int(query.get('chunk', -1))
            if ordinal >= 0:
                page = ordinal // 20 + 1
        except (TypeError, ValueError):
            raise AssistantError('页码或段落位置无效。') from None
        with self.connect() as db:
            row, original = self._document_version(db, actor, identity, query.get('version'))
            total = db.execute('SELECT count(*) FROM chunks WHERE document_id=? AND version=?', (identity, original['version'])).fetchone()[0]
            chunks = db.execute('SELECT ordinal,text,location FROM chunks WHERE document_id=? AND version=? ORDER BY ordinal LIMIT 20 OFFSET ?',
                (identity, original['version'], (page - 1) * 20)).fetchall()
            return {'document': {**self._public(row, actor), 'name': original['name'], 'view_version': original['version'],
                    'historical': original['version'] != row['active_version']},
                    'sections': [dict(chunk) for chunk in chunks], 'total': total, 'page': page, 'page_size': 20,
                    'acl_revision': db.execute('SELECT knowledge_acl_stamp()').fetchone()[0]}

    def file(self, actor, identity, version=None):
        check_actor(actor)
        with self.connect() as db:
            _, original = self._document_version(db, actor, identity, version)
            return self._path(original['path']), original['name']

    def answer_evidence(self, actor, items, *, profile, model, include_visual=True):
        """Read bounded current-version context; return items, warnings and coverage."""
        from .lighthouse_knowledge_evidence import answer_evidence
        return answer_evidence(self, actor, items, profile=profile, model=model,
                               include_visual=include_visual)

    def model_allowed(self, profile):
        endpoint_origin = origin(profile.get('endpoint', ''))
        config = self.configuration()
        approved = config.get('approved_origins', [])
        # Profiles come from the authenticated account's existing model settings, not browser input.
        return endpoint_origin in approved or not approved

    def export_photos(self, actor, items, *, profile, files, sources):
        from .lighthouse_knowledge_evidence import export_photos
        return export_photos(self, actor, items, profile=profile, files=files, sources=sources)

    @staticmethod
    def _search_context(db, item):
        # Use the search transaction and ACL, never a second search or a historical version.
        rows = db.execute('''SELECT c.ordinal,c.text,c.location FROM chunks c
            JOIN documents d ON d.id=c.document_id
            WHERE c.document_id=? AND c.version=? AND c.ordinal BETWEEN ? AND ?
                AND d.deleted=0 AND d.active_version=c.version AND knowledge_visible(d.id)=1
            ORDER BY c.ordinal LIMIT 3''',
            (item['document_id'], item['version'], item['ordinal'] - 1, item['ordinal'] + 1)).fetchall()
        context = []
        for row in rows:
            header = re.search(r'（列：([^）]+)）', row['location'])
            context.append({**dict(row), 'document_id': item['document_id'], 'version': item['version'],
                'relation': 'hit' if row['ordinal'] == item['ordinal'] else
                    'previous' if row['ordinal'] < item['ordinal'] else 'next',
                'table_header': header.group(1) if header else None})
        item['context'] = context
        # Existing answer consumers pass text and location, so include the bounded evidence there.
        if len(context) > 1 or any(part['table_header'] for part in context):
            item['text'] = '\n\n'.join(f"[{part['location']}]\n{part['text']}" for part in context)
        item['source_version'] = {'version': item['version'], 'sha256': item.pop('source_hash'),
            'created_at': item.pop('source_created_at')}
        item['coverage'] = {'scope': 'retrieved_context', 'complete': False,
            'ordinals': [part['ordinal'] for part in context], 'context_chunks': len(context),
            'indexed_document_chunks': item.pop('indexed_chunks'), 'history': 'unknown',
            'pending_version': item.pop('pending_version'), 'original_document_complete': None}
        item['content_trust'] = 'untrusted_data'

    @staticmethod
    def _filename_candidates(db, query):
        words = re.findall(r'[A-Za-z0-9][A-Za-z0-9._-]{1,79}|[\u4e00-\u9fff]{2,}', query.casefold())
        terms = set(words)
        for word in words:
            if re.fullmatch(r'[\u4e00-\u9fff]+', word):
                terms.update(word[i:i + size] for size in (1, 2, 3) for i in range(len(word) - size + 1))
        rows = db.execute('''SELECT d.id AS document_id,d.active_version AS version,v.name,d.source_key
            FROM documents d JOIN versions v ON v.document_id=d.id AND v.version=d.active_version
            WHERE d.deleted=0 AND d.active_version IS NOT NULL AND knowledge_visible(d.id)=1''').fetchall()
        terms = sorted(terms, key=lambda term: (-len(term), term))[:256]
        matched, frequency = [], {}
        for row in rows:
            name = row['name'].casefold()
            hits = {term for term in terms if term in name}
            matched.append((dict(row), hits))
            for term in hits:
                frequency[term] = frequency.get(term, 0) + 1
        ranked = []
        for row, hits in matched:
            if not hits:
                continue
            score = sum((.1 if len(term) == 1 else 1) / frequency[term] for term in hits)
            ranked.append((score, row))
        ranked.sort(key=lambda item: (-item[0], item[1]['document_id']))
        return [{**{key: row[key] for key in ('document_id', 'version', 'name')},
                 'source_kind': 'document_library' if row['source_key'] else 'shared'}
                for _, row in ranked[:12]]

    def find_documents(self, actor, query, *, profile=None):
        """Filename clues only; neither applicability nor document approval is inferred."""
        check_actor(actor)
        if not isinstance(query, str) or not 2 <= len(query.strip()) <= 1000 or sensitive_document(query):
            raise AssistantError('请输入2至1000字的检索内容，不要包含私人信息或凭证。')
        if profile is not None and not self.model_allowed(profile):
            raise AssistantError('当前聊天模型未获授权接收公司资料，请选择管理员批准的公司模型。', 403)
        with self.connect() as db:
            self._acl_sql(db, actor)
            items = self._filename_candidates(db, query.strip())
            revision = self._revision(db)
            acl_revision = db.execute('SELECT knowledge_acl_stamp()').fetchone()[0]
        return {'items': safe_data(items), 'revision': revision, 'acl_revision': acl_revision, 'complete': False,
                'note': '仅按文件名查找的候选，不是适用或已读取原文的结论。接着检索候选文件段落并核对；文件名不明确或依据不足时补查全库。'}

    def search(self, actor, query, *, profile=None, document_refs=None, prefer_local=False):
        check_actor(actor)
        if not isinstance(query, str) or not 2 <= len(query.strip()) <= 1000 or sensitive_document(query):
            raise AssistantError('请输入2至1000字的检索内容，不要包含私人信息或凭证。')
        query = query.strip()
        config = self.configuration()
        if profile is not None and not self.model_allowed(profile):
            raise AssistantError('当前聊天模型未获授权接收公司资料，请选择管理员批准的公司模型。', 403)
        if document_refs is not None:
            if (not isinstance(document_refs, list) or not 1 <= len(document_refs) <= 6 or any(
                    not isinstance(ref, dict) or not isinstance(ref.get('document_id'), str)
                    or type(ref.get('version')) is not int or ref['version'] < 1
                    or type(ref.get('ordinal')) is not int or ref['ordinal'] < 0 for ref in document_refs)):
                raise AssistantError('文档追问须包含至多6个有效引用。')
            from .lighthouse_knowledge_evidence import _current
            _current(self, actor, document_refs, profile or {})
        with self.connect() as db:
            if not db.execute('SELECT 1 FROM documents WHERE deleted=0 AND active_version IS NOT NULL LIMIT 1').fetchone():
                return {'items': [], 'mode': 'keyword', 'warning': '公司知识库暂无已完成入库的文件。',
                    'revision': self._revision(db), 'indexready': False, 'complete': False,
                    'coverage': {'scope': 'ranked_search_hits', 'complete': False, 'history': 'unknown'}}
        mode, warning, snapshot, hits = 'keyword', '', None, []
        file_candidates = []
        if document_refs is None:
            with self.connect() as db:
                self._acl_sql(db, actor)
                file_candidates = self._filename_candidates(db, query)
        indexready = False
        if document_refs is None and not prefer_local and self._vector_mode(config):
            try:
                with self.connect() as db:
                    vector_rev = self._vector_revision(db)
                snapshot = self._ensure_snapshot(vector_rev)
                if snapshot is not None and getattr(snapshot, 'revision', None) == vector_rev and getattr(snapshot, 'count', 0) > 0:
                    vector = self.embedder.embed([query], config, query=True)[0]
                    with self.connect() as db:
                        self._acl_sql(db, actor)
                        readable = {row[0] for row in db.execute('SELECT c.id FROM chunks c JOIN documents d ON d.id=c.document_id WHERE d.deleted=0 AND d.active_version=c.version AND knowledge_visible(d.id)=1')}
                    hits = [hit for hit in snapshot.search(vector, k=snapshot.count) if hit[0] in readable][:40]
                    mode = MODE
                    indexready = True
                else:
                    warning = '向量索引尚未就绪，本次仅使用关键词结果。'
                    snapshot = None
                    indexready = False
            except AssistantError as exc:
                warning = str(exc) + '本次仅使用关键词结果。'
                snapshot = None
                indexready = False
            except (RuntimeError, ValueError, OSError):
                logging.exception('Company knowledge vector search failed; falling back to local full text')
                mode, warning, snapshot, hits = 'keyword', '向量检索暂不可用，本次仅使用关键词结果。', None, []
                indexready = False

        keywords = re.sub(r'我们公司|咱们公司|本公司|公司内部|公司|请问|请告诉我|我想知道|怎么|如何|是什么|有哪些|什么|是否|可以|需要', ' ', query)
        terms = re.findall(r'[A-Za-z0-9][A-Za-z0-9._-]{1,79}|[\u4e00-\u9fff]{2,}', keywords)
        expanded = list(terms)
        for term in terms:
            if re.fullmatch(r'[\u4e00-\u9fff]{4,}', term):
                expanded.extend(term[i:i + size] for size in (2, 3) for i in range(min(len(term) - size + 1, 12)))
        expanded = list(dict.fromkeys(expanded))[:24]
        selected = {}

        def add(row, kind, rank):
            item = selected.setdefault(row['id'], {**dict(row), 'score': 0.0, 'matches': []})
            item['score'] += 1 / (60 + rank)
            if kind == 'filename':
                item['filename_match'] = True
                kind = 'keyword'
            if kind not in item['matches']:
                item['matches'].append(kind)

        try:
            with self.connect() as db:
                db.execute('BEGIN')
                self._acl_sql(db, actor)
                deadline = time.monotonic() + 3
                db.set_progress_handler(lambda: time.monotonic() > deadline, 1000)
                if config.get('generation') != self._get_config(db).get('generation'):
                    raise AssistantError('知识库模型刚发生变化，请重新查询。', 409)
                fields = ('c.id,c.document_id,c.version,c.ordinal,c.text,c.location,v.name,d.source_key,'
                    'v.hash AS source_hash,v.created_at AS source_created_at,d.chunks AS indexed_chunks,d.pending_version')
                joins = ' JOIN documents d ON d.id=c.document_id JOIN versions v ON v.document_id=c.document_id AND v.version=c.version '
                active = 'd.deleted=0 AND d.active_version=c.version AND knowledge_visible(d.id)=1'
                scope_args = []
                if document_refs is not None:
                    for ref in document_refs:
                        if not db.execute('SELECT 1 FROM chunks c' + joins + 'WHERE ' + active +
                                ' AND c.document_id=? AND c.version=? AND c.ordinal=?',
                                (ref['document_id'], ref['version'], ref['ordinal'])).fetchone():
                            raise AssistantError('已引用文档的版本、片段或授权已变化，请重新查询。', 409)
                    active += ' AND (c.document_id,c.version) IN (' + ','.join('(?,?)' for _ in document_refs) + ')'
                    scope_args = [value for ref in document_refs for value in (ref['document_id'], ref['version'])]
                if snapshot is not None and snapshot.revision != self._vector_revision(db):
                    hits = []
                    mode, indexready = 'keyword', False
                    warning = '资料已更新，本次仅使用当前全文结果。'
                if hits:
                    candidate = list(dict.fromkeys(chunk_id for chunk_id, _cos in hits))
                    row_by_id = {}
                    if candidate:
                        ph = ', '.join('?' * len(candidate))
                        for row in db.execute('SELECT ' + fields + ' FROM chunks c' + joins + 'WHERE ' + active + ' AND c.id IN (' + ph + ')', candidate):
                            row_by_id[row['id']] = row
                    for rank, (chunk_id, cosine) in enumerate(hits):
                        if cosine < 0.65:
                            continue
                        row = row_by_id.get(chunk_id)
                        if row is not None:
                            add(row, 'vector', rank)
                if expanded:
                    match = ' OR '.join('"' + term.replace('"', '""') + '"' for term in expanded)
                    if file_candidates:
                        passage_terms = list(dict.fromkeys([*expanded, *(term[i:i + 2] for term in terms
                            if re.fullmatch(r'[\u4e00-\u9fff]{3,}', term) for i in range(len(term) - 1))]))[:64]
                        condition = ' OR '.join('instr(lower(c.text),lower(?))>0' for _ in passage_terms)
                        relevance = ' + '.join('(CASE WHEN instr(lower(c.text),lower(?))>0 THEN ? ELSE 0 END)' for _ in passage_terms)
                        # Keep filename relevance, then rank passages inside each file.
                        # UUID order and a long first document must not displace the relevant file.
                        for document_rank, candidate in enumerate(file_candidates):
                            rows = db.execute('SELECT ' + fields + ' FROM chunks c' + joins +
                                'WHERE ' + active + ' AND c.document_id=? AND (' + condition +
                                ') ORDER BY (' + relevance + ') DESC,c.ordinal LIMIT 4',
                                [*scope_args, candidate['document_id'], *passage_terms,
                                 *[value for term in passage_terms for value in (term, len(term))]]).fetchall()
                            for rank, row in enumerate(rows):
                                add(row, 'filename', document_rank * 4 + rank)
                    rows = db.execute('SELECT ' + fields + ' FROM chunks_fts f JOIN chunks c ON c.id=f.rowid' + joins +
                        'WHERE chunks_fts MATCH ? AND ' + active + ' ORDER BY bm25(chunks_fts) LIMIT 40', [match, *scope_args]).fetchall()
                    for rank, row in enumerate(rows):
                        add(row, 'keyword', rank)
                    short = [term for term in terms if len(term) == 2][:24]
                    if not rows and short:
                        condition = ' OR '.join('(instr(lower(c.text),lower(?))>0 OR instr(lower(v.name),lower(?))>0)' for _ in short)
                        rows = db.execute('SELECT ' + fields + ' FROM chunks c' + joins + 'WHERE ' + active + ' AND (' + condition + ') LIMIT 20', [*scope_args, *[value for term in short for value in (term, term)]]).fetchall()
                        for rank, row in enumerate(rows):
                            add(row, 'keyword', rank)
                revision = self._revision(db)
                acl_revision = db.execute('SELECT knowledge_acl_stamp()').fetchone()[0]
                items = sorted(selected.values(), key=lambda item: (-item['score'], item['document_id'], item['ordinal']))[:8]
                for item in items:
                    self._search_context(db, item)
        except sqlite3.Error:
            raise AssistantError('知识库检索暂忙，未取得完整结果，请缩小问题范围后重试。', 503) from None
        if prefer_local and not items and document_refs is None and self._vector_mode(config):
            return self.search(actor, query, profile=profile)
        for item in items:
            item.pop('id', None)
            item['source_kind'] = 'document_library' if item.pop('source_key', '') else 'shared'
            item['score'] = round(item['score'], 6)
            item['url'] = f"/knowledge-base?document={item['document_id']}&version={item['version']}&chunk={item['ordinal']}"
        return {'items': items, 'mode': mode, 'warning': warning, 'revision': revision,
            'file_candidates': safe_data(file_candidates),
            'retrieval_strategy': 'referenced_document' if document_refs is not None else 'filename_then_passages_with_library_fallback',
            'indexready': indexready, 'acl_revision': acl_revision, 'complete': False,
            'coverage': {'scope': 'ranked_search_hits', 'complete': False, 'hit_limit': 8,
                'returned_hits': len(items), 'history': 'unknown', 'count_basis': 'retrieval_hits_not_business_records'},
            'statistics': {'tool': 'query_statistics', 'basis': 'authorized_structured_records'}}

    def evidence_current(self, items, profile, *, actor=None):
        if not self.model_allowed(profile):
            return False
        with self.connect() as db:
            self._acl_sql(db, actor)
            for item in items:
                row = db.execute('SELECT deleted,active_version FROM documents WHERE id=? AND knowledge_visible(id)=1', (item['document_id'],)).fetchone()
                if not row or row['deleted'] or row['active_version'] != item['version']:
                    return False
        return True

    def close(self):
        self._stop.set()
        self._wake.set()
        if self.embedder is not None:
            close = getattr(self.embedder, 'close', None)
            if close is not None:
                try:
                    close()
                except Exception:
                    pass
        if self._worker:
            self._worker.join(timeout=5)

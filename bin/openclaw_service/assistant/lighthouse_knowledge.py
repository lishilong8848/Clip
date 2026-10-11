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
import hashlib
import io
import ipaddress
import json
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
                            protect_key, safe_text)

MAX_FILE_BYTES = 100 * 1024 * 1024
MAX_BATCH_BYTES = 300 * 1024 * 1024
EXTENSIONS = {'.pdf', '.docx', '.xlsx', '.xlsm', '.txt', '.md', '.csv', '.png', '.jpg', '.jpeg', '.webp'}

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

_CREDENTIAL = re.compile(r'(?:api[_ -]?key|password|authorization|access[_ -]?token|密钥|口令|密码)\s*[:：=]\s*\S{6,}', re.I)
_COMPANY = re.compile(r'知识库|公司(?:的)?(?:内部|资料|文件|文档|制度|规定|规范|流程|政策|要求|标准|报销|请假|假期|福利|考勤|出差|差旅|休假|年假|规章|审批|培训)|(?:本公司|我们公司|咱们公司|公司里|公司内|本单位|单位内部)|员工手册|内部(?:制度|规定|资料|流程|文档)', re.I)
_BUSINESS = re.compile(r'(?:发送|发起|结束|删除|修改|更新|撤销|新建|创建|填写|保存|上传|绑定|导出|审批|签名|签字).{0,20}(?:通告|维修|跟进|机柜|工单|演练|水耗|重保)|(?:今天|今日|现在|当前|未结束|进行中|待发|未完成).{0,25}(?:通告|维修|事件|任务|水耗|机柜)|(?:通告|维修|事件|任务|水耗|机柜).{0,15}(?:几条|多少|数量|进度|状态)|(?:在职|在岗|员工|人员).{0,10}(?:人数|多少人)|灯塔.{0,15}(?:怎么|如何|操作|使用|设置|界面)', re.I)
_EXTERNAL = re.compile(r'天气|天气预报|新闻|联网|外部网站|股价|汇率|股票|翻译|写代码|编程|(?:微软|苹果|谷歌|特斯拉|OpenAI|字节跳动|阿里巴巴)公司', re.I)


def _load_local():
    """Return the local FAISS/embedding module or None when it is not installed yet."""
    try:
        from . import lighthouse_knowledge_local as module
        return module
    except Exception:
        return None


def company_question(question):
    text = str(question or '').strip()
    return bool(_COMPANY.search(text) and not _BUSINESS.search(text) and not _EXTERNAL.search(text))


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
                 start_worker=True, memory_index_factory=None):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.db = self.root / 'knowledge.sqlite3'
        self.protect = protect
        self.extractor = extractor
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
        ''')
        db.execute("UPDATE documents SET status='queued' WHERE status='indexing' AND deleted=0")

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
        queued = self._queued_count()
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
            'vectors': self._snapshot_count(),
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
    @staticmethod
    def _public(row, actor):
        return {key: row[key] for key in ('id', 'name', 'owner_name', 'active_version', 'status', 'error', 'updated_at', 'size', 'chunks')} | {
            'version': row['revision'], 'can_edit': bool(actor.get('is_admin') or row['owner'] == actor['id'])}

    @staticmethod
    def _editable(row, actor, revision):
        if not row:
            raise AssistantError('知识库文件不存在。', 404)
        if not actor.get('is_admin') and row['owner'] != actor['id']:
            raise AssistantError('只有上传者或管理员可以修改此共享文件。', 403)
        if type(revision) is not int or revision != row['revision']:
            raise AssistantError('文件版本已变化，请刷新后重新操作。', 409)

    def list(self, actor, query):
        check_actor(actor)
        try:
            page = int(query.get('page', 1))
            if page < 1:
                raise ValueError()
        except (ValueError, TypeError):
            raise AssistantError('页码无效。') from None
        deleted = str(query.get('deleted', '0')) == '1'
        q = str(query.get('q') or '').strip()[:200]
        sql, values = 'deleted=?', [int(deleted)]
        if deleted and not actor.get('is_admin'):
            sql += ' AND owner=?'
            values.append(actor['id'])
        if q:
            sql += " AND (instr(lower(name),lower(?))>0 OR instr(lower(owner_name),lower(?))>0)"
            values += [q, q]
        with self.connect() as db:
            total = db.execute('SELECT count(*) FROM documents WHERE ' + sql, values).fetchone()[0]
            rows = db.execute('SELECT * FROM documents WHERE ' + sql + ' ORDER BY updated_at DESC,id LIMIT 20 OFFSET ?', [*values, (page - 1) * 20]).fetchall()
            revision = self._revision(db)
        return {'items': [self._public(row, actor) for row in rows], 'total': total, 'page': page, 'page_size': 20,
                'is_admin': bool(actor.get('is_admin')), 'settings': self.settings(actor), 'revision': revision}

    def upload(self, actor, name, content, *, document_id='', revision=None):
        """Backward-compatible alias delegating to the streaming upload path."""
        return self.upload_file(actor, name, content, document_id=document_id, revision=revision)

    def upload_file(self, actor, name, content, *, document_id='', revision=None):
        """Stage bytes before the short metadata transaction; never lock while copying."""
        check_actor(actor)
        name = str(name or '').replace('\\', '/').split('/')[-1].strip()
        if not name or len(name) > 180 or any(ord(c) < 32 for c in name) or Path(name).suffix.lower() not in EXTENSIONS:
            raise AssistantError('请选择 PDF、Word、Excel、文本或图片文件。')
        if sensitive_document(name):
            raise AssistantError('文件名包含疑似敏感信息，请修改后再上传。')
        if not isinstance(content, bytes) and not callable(getattr(content, 'read', None)):
            raise AssistantError('文件内容格式无效。')
        if document_id:
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
                result, target = self._commit_upload(db, actor, name, temporary, size, hexdigest, document_id, revision)
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

    def _commit_upload(self, db, actor, name, temporary, size, hexdigest, document_id, revision):
        row = None
        if document_id:
            row = db.execute('SELECT * FROM documents WHERE id=?', (document_id,)).fetchone()
            self._editable(row, actor, revision)
            if row['deleted']:
                raise AssistantError('文件已删除，请先恢复。', 409)
        if not document_id:
            row = db.execute('SELECT d.* FROM documents d JOIN versions v ON v.document_id=d.id WHERE d.deleted=0 AND v.hash=? AND v.version IN (d.active_version,d.pending_version) LIMIT 1', (hexdigest,)).fetchone()
        if row:
            identical = db.execute('SELECT 1 FROM versions WHERE document_id=? AND hash=? AND version IN (?,?)',
                (row['id'], hexdigest, row['active_version'], row['pending_version'])).fetchone()
            if identical:
                return {**self._public(row, actor), 'duplicate': True}, None
        identity = document_id or uuid.uuid4().hex
        version = db.execute('SELECT COALESCE(MAX(version),0)+1 FROM versions WHERE document_id=?', (identity,)).fetchone()[0]
        relative = Path('originals') / identity / (str(version) + Path(name).suffix.lower())
        target = self.root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        stamp = time.time()
        db.execute('INSERT INTO versions VALUES(?,?,?,?,?,?,?)', (identity, version, name, hexdigest, str(relative), size, stamp))
        if document_id:
            db.execute("UPDATE documents SET name=?,pending_version=?,revision=revision+1,status='queued',error='',updated_at=?,size=? WHERE id=?",
                (name, version, stamp, size, identity))
        else:
            db.execute("INSERT INTO documents(id,owner,owner_name,name,pending_version,status,updated_at,size) VALUES(?,?,?,?,?,'queued',?,?)",
                (identity, actor['id'], safe_text(actor.get('name') or '用户', limit=100), name, version, stamp, size))
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

    def _run(self):
        from upload_event_module.services.process_lifetime import lower_current_thread_priority
        lower_current_thread_priority()
        while not self._stop.is_set():
            try:
                if self.process_one():
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

    def process_one(self):
        # Indexing (including embedding and vector persistence) is serialized by this
        # lock.  Search never takes ``_index_lock`` so reads remain parallel.
        with self._index_lock:
            with self.connect() as db:
                db.execute('BEGIN IMMEDIATE')
                row = db.execute("SELECT * FROM documents WHERE deleted=0 AND status='queued' AND pending_version IS NOT NULL ORDER BY updated_at LIMIT 1").fetchone()
                if not row:
                    return False
                doc = dict(row)
                cfg = self._get_config(db)
                vector_on = self._vector_mode(cfg)
                version = dict(db.execute('SELECT * FROM versions WHERE document_id=? AND version=?', (doc['id'], doc['pending_version'])).fetchone())
                db.execute("UPDATE documents SET status='indexing',error='' WHERE id=?", (doc['id'],))
                db.execute('DELETE FROM index_staging WHERE document_id=?', (doc['id'],))
            try:
                from .lighthouse_knowledge_extract import extract_sections, split_sections
                sections = (self.extractor or extract_sections)(self._path(version['path']).read_bytes(), version['name'])
                if any(sensitive_document(section['text']) for section in sections):
                    raise AssistantError('文件含疑似身份证、住址、联系方式或凭证，未公开入库。请脱敏后替换文件。', 422, category='sensitive_document')
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
                            current = db.execute('SELECT revision,deleted FROM documents WHERE id=?', (doc['id'],)).fetchone()
                            if current['deleted'] or current['revision'] != doc['revision']:
                                return True
                        texts = [chunk['text'] for chunk in chunks[start:start + _EMBED_BATCH]]
                        batch = self.embedder.embed(texts, cfg)
                        if not isinstance(batch, list) or len(batch) != len(texts):
                            raise AssistantError('嵌入接口未返回完整向量，原文档版本已保留，请重试。', 502)
                        packed = [(doc['id'], start + offset, _pack(vector)) for offset, vector in enumerate(batch)]
                        with self.connect() as db:
                            db.execute('BEGIN IMMEDIATE')
                            current = db.execute('SELECT revision,deleted FROM documents WHERE id=?', (doc['id'],)).fetchone()
                            if current['deleted'] or current['revision'] != doc['revision']:
                                return True
                            db.executemany('INSERT INTO index_staging VALUES(?,?,?)', packed)
                if self._stop.is_set():
                    return False
                with self.connect() as db:
                    db.execute('BEGIN IMMEDIATE')
                    current = db.execute('SELECT * FROM documents WHERE id=?', (doc['id'],)).fetchone()
                    if current['deleted'] or current['revision'] != doc['revision'] or current['pending_version'] != version['version']:
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
                    db.execute('DELETE FROM index_staging WHERE document_id=?', (doc['id'],))
                    db.execute("UPDATE documents SET status=?,error=? WHERE id=? AND revision=? AND deleted=0 AND status='indexing'",
                        (status, safe_text(message, limit=600) or '文件包含不宜公开的信息，未入库。', doc['id'], doc['revision']))
                return True

    def change(self, actor, identity, action, revision):
        check_actor(actor)
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM documents WHERE id=?', (identity,)).fetchone()
            self._editable(row, actor, revision)
            if action == 'delete':
                if row['deleted']:
                    raise AssistantError('文件已在回收站。', 409)
                for chunk in db.execute('SELECT id FROM chunks WHERE document_id=?', (identity,)).fetchall():
                    db.execute('DELETE FROM local_vectors WHERE chunk_id=?', (chunk[0],))
                db.execute("UPDATE documents SET deleted=1,status='deleted',pending_version=NULL,revision=revision+1,updated_at=? WHERE id=?",
                    (time.time(), identity))
                db.execute('DELETE FROM index_staging WHERE document_id=?', (identity,))
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

    def _document_version(self, db, actor, identity, version=None):
        row = db.execute('SELECT * FROM documents WHERE id=?', (identity,)).fetchone()
        if not row:
            raise AssistantError('知识库文件不存在。', 404)
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
                    'sections': [dict(chunk) for chunk in chunks], 'total': total, 'page': page, 'page_size': 20}

    def file(self, actor, identity, version=None):
        check_actor(actor)
        with self.connect() as db:
            _, original = self._document_version(db, actor, identity, version)
            return self._path(original['path']), original['name']

    def model_allowed(self, profile):
        endpoint_origin = origin(profile.get('endpoint', ''))
        config = self.configuration()
        approved = config.get('approved_origins', [])
        # Profiles come from the authenticated account's existing model settings, not browser input.
        return endpoint_origin in approved or not approved

    def search(self, actor, query, *, profile=None):
        check_actor(actor)
        if not isinstance(query, str) or not 2 <= len(query.strip()) <= 1000 or sensitive_document(query):
            raise AssistantError('请输入2至1000字的检索内容，不要包含私人信息或凭证。')
        query = query.strip()
        config = self.configuration()
        if profile is not None and not self.model_allowed(profile):
            raise AssistantError('当前聊天模型未获授权接收公司资料，请选择管理员批准的公司模型。', 403)
        with self.connect() as db:
            if not db.execute('SELECT 1 FROM documents WHERE deleted=0 AND active_version IS NOT NULL LIMIT 1').fetchone():
                return {'items': [], 'mode': 'keyword', 'warning': '公司知识库暂无已完成入库的文件。', 'revision': self._revision(db), 'indexready': False}
        mode, warning, snapshot, hits = 'keyword', '', None, []
        indexready = False
        if self._vector_mode(config):
            try:
                with self.connect() as db:
                    vector_rev = self._vector_revision(db)
                snapshot = self._ensure_snapshot(vector_rev)
                if snapshot is not None and getattr(snapshot, 'revision', None) == vector_rev and getattr(snapshot, 'count', 0) > 0:
                    vector = self.embedder.embed([query], config, query=True)[0]
                    hits = snapshot.search(vector, k=40)
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

        keywords = re.sub(r'我们公司|咱们公司|本公司|公司内部|公司|请问|请告诉我|我想知道|怎么|如何|是什么|有哪些|什么|是否|可以|需要', ' ', query)
        terms = re.findall(r'[A-Za-z0-9][A-Za-z0-9._-]{1,79}|[\u4e00-\u9fff]{2,}', keywords)
        expanded = list(terms)
        for term in terms:
            if re.fullmatch(r'[\u4e00-\u9fff]{4,}', term):
                expanded.extend(term[i:i + 3] for i in range(min(len(term) - 2, 12)))
        expanded = list(dict.fromkeys(expanded))[:24]
        selected = {}

        def add(row, kind, rank):
            item = selected.setdefault(row['id'], {**dict(row), 'score': 0.0, 'matches': []})
            item['score'] += 1 / (60 + rank)
            item['matches'].append(kind)

        try:
            with self.connect() as db:
                db.execute('BEGIN')
                deadline = time.monotonic() + 3
                db.set_progress_handler(lambda: time.monotonic() > deadline, 1000)
                if config.get('generation') != self._get_config(db).get('generation'):
                    raise AssistantError('知识库模型刚发生变化，请重新查询。', 409)
                fields = 'c.id,c.document_id,c.version,c.ordinal,c.text,c.location,v.name'
                joins = ' JOIN documents d ON d.id=c.document_id JOIN versions v ON v.document_id=c.document_id AND v.version=c.version '
                active = 'd.deleted=0 AND d.active_version=c.version'
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
                    rows = db.execute('SELECT ' + fields + ' FROM chunks_fts f JOIN chunks c ON c.id=f.rowid' + joins +
                        'WHERE chunks_fts MATCH ? AND ' + active + ' ORDER BY bm25(chunks_fts) LIMIT 40', (match,)).fetchall()
                    for rank, row in enumerate(rows):
                        add(row, 'keyword', rank)
                    short = [term for term in terms if len(term) == 2][:24]
                    if not rows and short:
                        condition = ' OR '.join('(instr(lower(c.text),lower(?))>0 OR instr(lower(v.name),lower(?))>0)' for _ in short)
                        rows = db.execute('SELECT ' + fields + ' FROM chunks c' + joins + 'WHERE ' + active + ' AND (' + condition + ') LIMIT 20', [value for term in short for value in (term, term)]).fetchall()
                        for rank, row in enumerate(rows):
                            add(row, 'keyword', rank)
                revision = self._revision(db)
        except sqlite3.Error:
            raise AssistantError('知识库检索暂忙，未取得完整结果，请缩小问题范围后重试。', 503) from None
        items = sorted(selected.values(), key=lambda item: (-item['score'], item['document_id'], item['ordinal']))[:8]
        for item in items:
            item.pop('id', None)
            item['score'] = round(item['score'], 6)
            item['url'] = f"/knowledge-base?document={item['document_id']}&version={item['version']}&chunk={item['ordinal']}"
        return {'items': items, 'mode': mode, 'warning': warning, 'revision': revision, 'indexready': indexready}

    def evidence_current(self, items, profile):
        if not self.model_allowed(profile):
            return False
        with self.connect() as db:
            for item in items:
                row = db.execute('SELECT deleted,active_version FROM documents WHERE id=?', (item['document_id'],)).fetchone()
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

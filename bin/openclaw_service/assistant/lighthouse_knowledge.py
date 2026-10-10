"""Shared company documents, versioned SQLite vectors and bounded background indexing."""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import ipaddress
import json
import math
from pathlib import Path
import re
import sqlite3
import threading
import time
from urllib.parse import urlsplit
import uuid

from .lighthouse_ai import (AssistantError, ADDRESS, API_KEY, CONTACT, ID_NUMBER,
                            protect_key, unprotect_key, safe_text)
from upload_event_module.services.http_client import FeishuHttpClient

MAX_FILE_BYTES = 20 * 1024 * 1024
EXTENSIONS = {'.pdf', '.docx', '.xlsx', '.xlsm', '.txt', '.md', '.csv', '.png', '.jpg', '.jpeg', '.webp'}
_CREDENTIAL = re.compile(r'(?:api[_ -]?key|password|authorization|access[_ -]?token|密钥|口令|密码)\s*[:：=]\s*\S{6,}', re.I)
_COMPANY = re.compile(r'知识库|公司(?:内部|资料|文件|文档|制度|规定|规范|流程|政策|要求|标准|报销|请假|假期|福利|考勤)|(?:本公司|我们公司|咱们公司|公司里|公司内|本单位|单位内部)|员工手册|内部(?:制度|规定|资料|流程|文档)', re.I)
_BUSINESS = re.compile(r'(?:发送|发起|结束|删除|修改|更新|撤销|新建|创建|填写|保存|上传|绑定|导出|审批|签名|签字).{0,20}(?:通告|维修|跟进|机柜|工单|演练|水耗|重保)|(?:今天|今日|现在|当前|未结束|进行中|待发|未完成).{0,25}(?:通告|维修|事件|任务|水耗|机柜)|(?:通告|维修|事件|任务|水耗|机柜).{0,15}(?:几条|多少|数量|进度|状态)|(?:在职|在岗|员工|人员).{0,10}(?:人数|多少人)|灯塔.{0,15}(?:怎么|如何|操作|使用|设置|界面)', re.I)
_EXTERNAL = re.compile(r'天气|天气预报|新闻|联网|外部网站|股价|汇率|股票|翻译|写代码|编程|(?:微软|苹果|谷歌|特斯拉|OpenAI|字节跳动|阿里巴巴)公司', re.I)


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


class Embeddings:
    def __init__(self, *, client=None, decrypt=unprotect_key):
        self.client = client or FeishuHttpClient(timeout=20, retries=0)
        self.decrypt = decrypt

    def embed(self, texts, config):
        if not texts:
            return []
        if not config.get('key_cipher'):
            raise AssistantError('请管理员先配置知识库嵌入模型；聊天模型配置不等于嵌入模型配置。', 409)
        try:
            data = self.client.request_json('POST', config['endpoint'],
                headers={'Authorization': 'Bearer ' + self.decrypt(config['key_cipher'])},
                json_payload={'model': config['model'], 'input': texts, 'encoding_format': 'float'})
        except Exception:
            raise AssistantError('嵌入模型连接未完成，原文件已保留，可重试索引。', 503) from None
        items = data.get('data') if isinstance(data, dict) else None
        if not isinstance(items, list) or len(items) != len(texts):
            raise AssistantError('嵌入接口未返回完整向量，请检查模型是否支持 embeddings。', 502)
        ordered = {}
        for item in items:
            if not isinstance(item, dict):
                raise AssistantError('嵌入接口返回了无效向量，未更新知识库。', 502)
            index = item.get('index')
            vector = item.get('embedding')
            if (type(index) is not int or not 0 <= index < len(texts) or index in ordered
                    or not isinstance(vector, list) or not 1 <= len(vector) <= 4096
                    or any(type(v) not in (int, float) or not math.isfinite(v) for v in vector)
                    or sum(v * v for v in vector) <= 0):
                raise AssistantError('嵌入接口返回了无效向量，未更新知识库。', 502)
            ordered[index] = vector
        vectors = [ordered[index] for index in range(len(texts))]
        size = len(vectors[0])
        if any(len(vector) != size for vector in vectors) or config.get('dimensions') not in (None, 0, size):
            raise AssistantError('嵌入模型的向量维度已变化，请管理员重新保存模型设置并重建索引。', 409)
        return vectors

    def close(self):
        self.client.close()


class KnowledgeBase:
    def __init__(self, root, *, embedder=None, protect=protect_key, extractor=None, start_worker=True):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.db = self.root / 'knowledge.sqlite3'
        self.embedder = embedder or Embeddings()
        self.protect = protect
        self.extractor = extractor
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._worker = None
        self._worker_lock = threading.Lock()
        self._index_lock = threading.Lock()
        self._settings_lock = threading.Lock()
        self.start_worker = start_worker
        with self.connect() as db:
            db.executescript('''
                PRAGMA journal_mode=WAL;
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
            ''')
            db.execute("UPDATE documents SET status='queued' WHERE status='indexing' AND deleted=0")
        self._ensure_worker()

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.db, timeout=1)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def _vectors(db):
        try:
            import sqlite_vec
            db.enable_load_extension(True)
            try:
                sqlite_vec.load(db)
            finally:
                db.enable_load_extension(False)
            return sqlite_vec
        except (ImportError, OSError, sqlite3.Error):
            raise AssistantError('向量库依赖未就绪，请完成程序依赖更新后重试。', 503) from None

    @staticmethod
    def _get_config(db):
        row = db.execute("SELECT payload FROM config WHERE key='embedding'").fetchone()
        return json.loads(row[0]) if row else {}

    def configuration(self):
        with self.connect() as db:
            return self._get_config(db)

    def settings(self, actor):
        check_actor(actor)
        cfg = self.configuration()
        return {key: cfg.get(key, [] if key == 'approved_origins' else 0 if key == 'dimensions' else '')
                for key in ('endpoint', 'model', 'dimensions', 'approved_origins')} | {'configured': bool(cfg.get('key_cipher'))}

    def save_settings(self, actor, payload):
        check_actor(actor, admin=True)
        if not isinstance(payload, dict) or set(payload) - {'endpoint', 'model', 'api_key', 'approved_origins'}:
            raise AssistantError('知识库设置格式无效。')
        endpoint = str(payload.get('endpoint') or '').strip()
        origin(endpoint)
        if not urlsplit(endpoint).path.rstrip('/').endswith('/embeddings'):
            raise AssistantError('请填写完整的 embeddings 接口地址，不是聊天接口。')
        model = str(payload.get('model') or '').strip()
        key = payload.get('api_key') or ''
        origins = payload.get('approved_origins')
        if not model or len(model) > 200 or not isinstance(key, str) or len(key) > 1000 or not isinstance(origins, list) or not 1 <= len(origins) <= 20:
            raise AssistantError('请填写嵌入模型和允许接收公司资料的模型域名。')
        approved = sorted({origin(item) for item in origins})
        with self._settings_lock:
            prior = self.configuration()
            if not key and prior.get('endpoint') != endpoint:
                raise AssistantError('更换接口地址时请重新填写 API Key，旧凭证不会发送到新地址。')
            cipher = self.protect(key.strip()) if key.strip() else prior.get('key_cipher', '')
            candidate = {'endpoint': endpoint, 'model': model, 'key_cipher': cipher, 'approved_origins': approved}
            vectors = self.embedder.embed(['公司知识库连接检查'], candidate)
            candidate['dimensions'] = len(vectors[0])
            changed = any(candidate.get(k) != prior.get(k) for k in ('endpoint', 'model', 'dimensions'))
            candidate['generation'] = int(prior.get('generation', 0)) + int(changed)
            with self.connect() as db:
                self._vectors(db)
                db.execute('BEGIN IMMEDIATE')
                if changed:
                    db.execute('DROP TABLE IF EXISTS vectors')
                    db.execute(f"CREATE VIRTUAL TABLE vectors USING vec0(embedding float[{candidate['dimensions']}] distance_metric=cosine)")
                    db.execute("UPDATE documents SET pending_version=COALESCE(pending_version,active_version),status='queued',error='' WHERE deleted=0 AND status!='blocked'")
                else:
                    db.execute("UPDATE documents SET status='queued',error='' WHERE deleted=0 AND status='failed' AND pending_version IS NOT NULL")
                db.execute("INSERT OR REPLACE INTO config(key,payload) VALUES('embedding',?)", (json.dumps(candidate),))
                self._bump(db)
            self._ensure_worker()
            self._wake.set()
        return self.settings(actor)

    @staticmethod
    def _bump(db):
        db.execute("INSERT INTO config(key,payload) VALUES('revision','1') ON CONFLICT(key) DO UPDATE SET payload=CAST(CAST(payload AS INTEGER)+1 AS TEXT)")

    @staticmethod
    def _revision(db):
        row = db.execute("SELECT payload FROM config WHERE key='revision'").fetchone()
        return int(row[0]) if row else 0

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
        check_actor(actor)
        name = str(name or '').replace('\\', '/').split('/')[-1].strip()
        if not name or len(name) > 180 or any(ord(c) < 32 for c in name) or Path(name).suffix.lower() not in EXTENSIONS:
            raise AssistantError('请选择 PDF、Word、Excel、文本或图片文件。')
        if not isinstance(content, bytes) or not 0 < len(content) <= MAX_FILE_BYTES:
            raise AssistantError('知识库单文件须为1字节至20MiB。', 413)
        if sensitive_document(name):
            raise AssistantError('文件名包含疑似敏感信息，请修改后再上传。')
        digest = hashlib.sha256(content).hexdigest()
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if document_id:
                row = db.execute('SELECT * FROM documents WHERE id=?', (document_id,)).fetchone()
                self._editable(row, actor, revision)
                if row['deleted']:
                    raise AssistantError('文件已删除，请先恢复。', 409)
            else:
                row = db.execute('SELECT d.* FROM documents d JOIN versions v ON v.document_id=d.id WHERE d.deleted=0 AND v.hash=? AND v.version IN (d.active_version,d.pending_version) LIMIT 1', (digest,)).fetchone()
            if row:
                identical = db.execute('SELECT 1 FROM versions WHERE document_id=? AND hash=? AND version IN (?,?)',
                    (row['id'], digest, row['active_version'], row['pending_version'])).fetchone()
                if identical:
                    return {**self._public(row, actor), 'duplicate': True}
            identity = document_id or uuid.uuid4().hex
            version = db.execute('SELECT COALESCE(MAX(version),0)+1 FROM versions WHERE document_id=?', (identity,)).fetchone()[0]
            relative = Path('originals') / identity / (str(version) + Path(name).suffix.lower())
            target = self.root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            stamp = time.time()
            db.execute('INSERT INTO versions VALUES(?,?,?,?,?,?,?)', (identity, version, name, digest, str(relative), len(content), stamp))
            if document_id:
                db.execute("UPDATE documents SET name=?,pending_version=?,revision=revision+1,status='queued',error='',updated_at=?,size=? WHERE id=?",
                    (name, version, stamp, len(content), identity))
            else:
                db.execute("INSERT INTO documents(id,owner,owner_name,name,pending_version,status,updated_at,size) VALUES(?,?,?,?,?,'queued',?,?)",
                    (identity, actor['id'], safe_text(actor.get('name') or '用户', limit=100), name, version, stamp, len(content)))
            self._bump(db)
            result = self._public(db.execute('SELECT * FROM documents WHERE id=?', (identity,)).fetchone(), actor)
        self._ensure_worker()
        self._wake.set()
        return result

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
        with self._index_lock:
            with self.connect() as db:
                db.execute('BEGIN IMMEDIATE')
                row = db.execute("SELECT * FROM documents WHERE deleted=0 AND status='queued' AND pending_version IS NOT NULL ORDER BY updated_at LIMIT 1").fetchone()
                if not row:
                    return False
                doc = dict(row)
                cfg = self._get_config(db)
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
                for start in range(0, len(chunks), 16):
                    if self._stop.is_set():
                        return False
                    with self.connect() as db:
                        current = db.execute('SELECT revision,deleted FROM documents WHERE id=?', (doc['id'],)).fetchone()
                        if current['deleted'] or current['revision'] != doc['revision'] or self._get_config(db).get('generation') != cfg.get('generation'):
                            return True
                    vectors = self.embedder.embed([chunk['text'] for chunk in chunks[start:start + 16]], cfg)
                    with self.connect() as db:
                        vec = self._vectors(db)
                        db.execute('BEGIN IMMEDIATE')
                        current = db.execute('SELECT revision,deleted FROM documents WHERE id=?', (doc['id'],)).fetchone()
                        if current['deleted'] or current['revision'] != doc['revision'] or self._get_config(db).get('generation') != cfg.get('generation'):
                            return True
                        db.executemany('INSERT INTO index_staging VALUES(?,?,?)',
                            [(doc['id'], start + offset, vec.serialize_float32(vector)) for offset, vector in enumerate(vectors)])
                with self.connect() as db:
                    vec = self._vectors(db)
                    db.execute('BEGIN IMMEDIATE')
                    current = db.execute('SELECT * FROM documents WHERE id=?', (doc['id'],)).fetchone()
                    if (current['deleted'] or current['revision'] != doc['revision'] or current['pending_version'] != version['version']
                            or self._get_config(db).get('generation') != cfg.get('generation')):
                        return True
                    # Only active-version vectors are searchable; all switches commit together.
                    ids = [row[0] for row in db.execute('SELECT id FROM chunks WHERE document_id=?', (doc['id'],))]
                    for identity in ids:
                        db.execute('DELETE FROM vectors WHERE rowid=?', (identity,))
                    if db.execute('SELECT count(*) FROM vectors').fetchone()[0] + len(chunks) > 100000:
                        raise AssistantError('当前单机知识库已达到10万片段上限，请管理员清理旧资料后重试。', 413)
                    old = [row[0] for row in db.execute('SELECT id FROM chunks WHERE document_id=? AND version=?', (doc['id'], version['version']))]
                    for identity in old:
                        db.execute('DELETE FROM chunks_fts WHERE rowid=?', (identity,))
                    db.execute('DELETE FROM chunks WHERE document_id=? AND version=?', (doc['id'], version['version']))
                    for staged in db.execute('SELECT ordinal,embedding FROM index_staging WHERE document_id=? ORDER BY ordinal', (doc['id'],)):
                        ordinal, vector = staged['ordinal'], staged['embedding']
                        chunk = chunks[ordinal]
                        cursor = db.execute('INSERT INTO chunks(document_id,version,ordinal,text,location) VALUES(?,?,?,?,?)',
                            (doc['id'], version['version'], ordinal, chunk['text'], chunk['location']))
                        identity = cursor.lastrowid
                        db.execute('INSERT INTO chunks_fts(rowid,text) VALUES(?,?)', (identity, chunk['text']))
                        db.execute('INSERT INTO vectors(rowid,embedding) VALUES(?,?)', (identity, vector))
                    db.execute('DELETE FROM index_staging WHERE document_id=?', (doc['id'],))
                    db.execute("UPDATE documents SET active_version=?,pending_version=NULL,status='ready',error='',chunks=?,updated_at=?,name=?,size=? WHERE id=?",
                        (version['version'], len(chunks), time.time(), version['name'], version['size'], doc['id']))
                    self._bump(db)
                return True

            except Exception as exc:
                message = str(exc) if isinstance(exc, AssistantError) else '文件解析或向量索引未完成，请检查格式后重试。'
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
                if self._get_config(db).get('dimensions'):
                    self._vectors(db)
                    for chunk in db.execute('SELECT id FROM chunks WHERE document_id=?', (identity,)).fetchall():
                        db.execute('DELETE FROM vectors WHERE rowid=?', (chunk[0],))
                db.execute("UPDATE documents SET deleted=1,status='deleted',pending_version=NULL,revision=revision+1,updated_at=? WHERE id=?", (time.time(), identity))
                db.execute('DELETE FROM index_staging WHERE document_id=?', (identity,))
            elif action in {'restore', 'retry'}:
                if action == 'restore' and not row['deleted'] or action == 'retry' and (row['deleted'] or row['status'] != 'failed'):
                    raise AssistantError('文件状态已变化，请刷新后重试。', 409)
                version = row['pending_version'] or row['active_version'] or db.execute('SELECT MAX(version) FROM versions WHERE document_id=?', (identity,)).fetchone()[0]
                db.execute("UPDATE documents SET deleted=0,status='queued',pending_version=?,revision=revision+1,error='',updated_at=? WHERE id=?", (version, time.time(), identity))
                if action == 'restore':
                    db.execute('UPDATE documents SET active_version=NULL,chunks=0 WHERE id=?', (identity,))
            else:
                raise AssistantError('文件操作无效。')
            self._bump(db)
            result = self._public(db.execute('SELECT * FROM documents WHERE id=?', (identity,)).fetchone(), actor)
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
                raise AssistantError('文件尚未完成安全检查和向量入库。', 409)
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
        return origin(profile.get('endpoint', '')) in self.configuration().get('approved_origins', [])

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
                return {'items': [], 'mode': 'keyword', 'warning': '公司知识库暂无已完成入库的文件。', 'revision': self._revision(db)}
        mode, warning, vector = 'keyword', '', None
        if config.get('key_cipher'):
            try:
                vector = self.embedder.embed([query], config)[0]
                mode = 'hybrid'
            except AssistantError as exc:
                warning = str(exc) + '本次仅使用关键词结果。'
        else:
            warning = '嵌入模型未配置，本次仅使用已有的关键词索引。'
        terms = re.findall(r'[A-Za-z0-9][A-Za-z0-9._-]{1,79}|[\u4e00-\u9fff]{2,}', query)
        expanded = list(terms)
        for term in terms:
            if re.fullmatch(r'[\u4e00-\u9fff]{4,}', term):
                expanded.extend(term[i:i + 3] for i in range(min(len(term) - 2, 12)))
        expanded = list(dict.fromkeys(expanded))[:24]
        selected = {}
        def collect(rows, kind):
            for rank, row in enumerate(rows):
                item = selected.setdefault(row['id'], {**dict(row), 'score': 0.0, 'matches': []})
                item['score'] += 1 / (60 + rank)
                item['matches'].append(kind)
        try:
            with self.connect() as db:
                vec = self._vectors(db) if vector is not None else None
                db.execute('BEGIN')
                deadline = time.monotonic() + 3
                db.set_progress_handler(lambda: time.monotonic() > deadline, 1000)
                if config.get('generation') != self._get_config(db).get('generation'):
                    raise AssistantError('知识库模型刚发生变化，请重新查询。', 409)
                fields = 'c.id,c.document_id,c.version,c.ordinal,c.text,c.location,v.name'
                joins = ' JOIN documents d ON d.id=c.document_id JOIN versions v ON v.document_id=c.document_id AND v.version=c.version '
                active = 'd.deleted=0 AND d.active_version=c.version'
                if vector is not None:
                    rows = db.execute('SELECT ' + fields + ' FROM (SELECT rowid,distance FROM vectors WHERE embedding MATCH ? AND k=40) n JOIN chunks c ON c.id=n.rowid' + joins +
                        'WHERE ' + active + ' AND n.distance<=0.65 ORDER BY n.distance', (vec.serialize_float32(vector),)).fetchall()
                    collect(rows, 'vector')
                if expanded:
                    match = ' OR '.join('"' + term.replace('"', '""') + '"' for term in expanded)
                    rows = db.execute('SELECT ' + fields + ' FROM chunks_fts f JOIN chunks c ON c.id=f.rowid' + joins +
                        'WHERE chunks_fts MATCH ? AND ' + active + ' ORDER BY bm25(chunks_fts) LIMIT 40', (match,)).fetchall()
                    collect(rows, 'keyword')
                    if not rows and len(query) < 3:
                        rows = db.execute('SELECT ' + fields + ' FROM chunks c' + joins + 'WHERE ' + active + ' AND instr(c.text,?)>0 LIMIT 20', (query,)).fetchall()
                        collect(rows, 'keyword')
                revision = self._revision(db)
        except sqlite3.Error:
            raise AssistantError('知识库检索暂忙，未取得完整结果，请缩小问题范围后重试。', 503) from None
        items = sorted(selected.values(), key=lambda item: (-item['score'], item['document_id'], item['ordinal']))[:8]
        for item in items:
            item.pop('id', None)
            item['score'] = round(item['score'], 6)
            item['url'] = f"/knowledge-base?document={item['document_id']}&version={item['version']}&chunk={item['ordinal']}"
        return {'items': items, 'mode': mode, 'warning': warning, 'revision': revision}

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
        if self._worker:
            self._worker.join(timeout=25)
        self.embedder.close()

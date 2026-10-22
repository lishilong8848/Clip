"""Configured Bitable documents, durable write receipts and source-scoped ingestion."""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import time
import unicodedata
from urllib.parse import parse_qs, urlsplit
import uuid

from openclaw_service.assistant.lighthouse_ai import AssistantError

SCOPES = frozenset(('A', 'B', 'C', 'D', 'E', 'H', '110'))
MAX_FILE = 100 * 1024 * 1024
_WRITABLE_TYPES = {1, 2, 3, 4, 5, 7, 11, 15}


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _digest(value):
    return hashlib.sha256(_json(value).encode('utf-8')).hexdigest()


def _scopes(values, *, empty=False):
    if not isinstance(values, list) or any(v not in SCOPES for v in values) or not empty and not values:
        raise AssistantError('楼栋范围配置无效。')
    return sorted(set(values))


def _member(actor):
    if not isinstance(actor, dict) or not actor.get('id') or actor.get('is_guest') or actor.get('role') == 'guest':
        raise AssistantError('请先登录。', 401)
    if 'knowledge' not in actor.get('modules', []):
        raise AssistantError('当前账号没有共享知识库权限。', 403)


def validate_source(payload, *, identity=None):
    if not isinstance(payload, dict):
        raise AssistantError('资料表配置无效。')
    allowed = {'id', 'name', 'url', 'base_token', 'table_id', 'module', 'display_fields',
        'attachment_fields', 'scope_field', 'fixed_scopes', 'read_scopes', 'write_scopes', 'enabled',
        'expected_revision', 'reason', 'operation_id'}
    if set(payload) - allowed:
        raise AssistantError('资料表配置包含未知字段。')
    result = {key: payload.get(key) for key in allowed - {'expected_revision', 'reason', 'operation_id'}}
    result['id'] = identity or result['id']
    if not isinstance(result['id'], str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', result['id']):
        raise AssistantError('资料表标识无效。')
    if not isinstance(result['name'], str) or not result['name'].strip() or len(result['name']) > 120:
        raise AssistantError('请填写资料表名称，长度不超过120个字符。')
    result['name'] = result['name'].strip()
    result['url'] = result['url'] or ''
    if result['url']:
        parsed = urlsplit(result['url'])
        if (parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password
                or parsed.hostname != 'feishu.cn' and not parsed.hostname.endswith('.feishu.cn')
                or parsed.port not in (None, 443)):
            raise AssistantError('仅支持 HTTPS 飞书多维表链接。')
        match = re.fullmatch(r'/base/([A-Za-z0-9]+)(?:/)?', parsed.path)
        if not match:
            raise AssistantError('请先读取 Wiki 链接，取得对应的多维表 Base 和数据表 ID。')
        base = match[1]
        table = (parse_qs(parsed.query).get('table') or [''])[0]
        if result['base_token'] and result['base_token'] != base or result['table_id'] and table and result['table_id'] != table:
            raise AssistantError('链接与数据表标识不一致。')
        result['base_token'] = result['base_token'] or base
        result['table_id'] = result['table_id'] or table
    if not re.fullmatch(r'[A-Za-z0-9]{8,200}', str(result['base_token'] or '')) or not re.fullmatch(r'tbl[A-Za-z0-9]+', str(result['table_id'] or '')):
        raise AssistantError('请填写有效的 Base Token 和数据表 ID。')
    if result['module'] not in (None, 'knowledge'):
        raise AssistantError('资料库仅使用共享知识库权限。')
    result['module'] = 'knowledge'
    for key in ('display_fields', 'attachment_fields'):
        result[key] = result[key] or []
        if not isinstance(result[key], list) or any(not isinstance(v, str) or not re.fullmatch(r'fld[A-Za-z0-9]+', v) for v in result[key]):
            raise AssistantError('请使用数据表的原始字段 ID 配置。')
        result[key] = list(dict.fromkeys(result[key]))
    result['scope_field'] = result['scope_field'] or ''
    if result['scope_field'] and not re.fullmatch(r'fld[A-Za-z0-9]+', result['scope_field']):
        raise AssistantError('楼栋字段 ID 无效。')
    result['fixed_scopes'] = _scopes(result['fixed_scopes'] or [], empty=bool(result['scope_field']))
    result['read_scopes'] = _scopes(payload.get('read_scopes', []), empty=True)
    result['write_scopes'] = _scopes(payload.get('write_scopes', []), empty=True)
    if set(result['write_scopes']) - set(result['read_scopes']):
        raise AssistantError('编辑范围不能超出读取范围。')
    result['enabled'] = payload.get('enabled', True)
    if not isinstance(result['enabled'], bool):
        raise AssistantError('启用状态无效。')
    return result


class DocumentLibraryService:
    _sleep_download = staticmethod(time.sleep)

    def __init__(self, data_root, *, remote_factory=None, knowledge=None, authorize_source=None,
                 refresh_actor=None, people_provider=None, clock=time.time):
        self.root = Path(data_root).resolve() / 'document_library'
        self.root.mkdir(parents=True, exist_ok=True)
        self.db_path = self.root / 'document_library.sqlite3'
        if remote_factory is None:
            from .document_library_remote import DocumentLibraryRemote
            remote_factory = DocumentLibraryRemote
        self.remote_factory = remote_factory
        self.authorize_source = authorize_source
        self.refresh_actor = refresh_actor
        self.people_provider = people_provider
        self.clock = clock
        self.knowledge = None
        self._ingest_next = False
        with self.connect() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS tables(id TEXT PRIMARY KEY,payload TEXT NOT NULL,revision TEXT NOT NULL,next_sync REAL NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS records(table_key TEXT NOT NULL,id TEXT NOT NULL,payload TEXT NOT NULL,revision TEXT NOT NULL,
                    deleted INTEGER NOT NULL DEFAULT 0,generation TEXT NOT NULL DEFAULT '',pending_operation TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY(table_key,id));
                CREATE TABLE IF NOT EXISTS versions(table_key TEXT NOT NULL,record_id TEXT NOT NULL,number INTEGER NOT NULL,
                    revision TEXT NOT NULL,payload TEXT NOT NULL,actor TEXT NOT NULL,reason TEXT NOT NULL,created_at REAL NOT NULL,
                    PRIMARY KEY(table_key,record_id,number));
                CREATE TABLE IF NOT EXISTS operations(id TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,table_key TEXT NOT NULL,
                    record_id TEXT NOT NULL,actor TEXT NOT NULL,status TEXT NOT NULL,payload TEXT NOT NULL,result TEXT,created_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS sync_jobs(id TEXT PRIMARY KEY,table_key TEXT NOT NULL,config_revision TEXT NOT NULL,
                    status TEXT NOT NULL,cursor TEXT NOT NULL DEFAULT '',seen_cursors TEXT NOT NULL DEFAULT '[]',
                    count INTEGER NOT NULL DEFAULT 0,attempts INTEGER NOT NULL DEFAULT 0,next_attempt REAL NOT NULL DEFAULT 0,
                    lease TEXT NOT NULL DEFAULT '',lease_until REAL NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS ingestion(source_key TEXT PRIMARY KEY,table_key TEXT NOT NULL,record_id TEXT NOT NULL,
                    record_revision TEXT NOT NULL,payload TEXT NOT NULL,status TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt REAL NOT NULL DEFAULT 0,lease TEXT NOT NULL DEFAULT '',lease_until REAL NOT NULL DEFAULT 0);
                CREATE INDEX IF NOT EXISTS ingestion_due ON ingestion(status,next_attempt);
                CREATE INDEX IF NOT EXISTS jobs_due ON sync_jobs(status,next_attempt);
                CREATE TABLE IF NOT EXISTS schema_cache(table_key TEXT PRIMARY KEY,config_revision TEXT NOT NULL,
                    payload TEXT NOT NULL,observed_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS maintenance(id TEXT PRIMARY KEY,kind TEXT NOT NULL,table_key TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,result TEXT NOT NULL,status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,next_attempt REAL NOT NULL DEFAULT 0,
                    lease TEXT NOT NULL DEFAULT '',lease_until REAL NOT NULL DEFAULT 0,error TEXT NOT NULL DEFAULT '');
            ''')
            db.execute('BEGIN IMMEDIATE')
            if 'download_after' not in {row[1] for row in db.execute('PRAGMA table_info(tables)')}:
                db.execute('ALTER TABLE tables ADD COLUMN download_after REAL NOT NULL DEFAULT 0')
            for table, column in (('records', 'observed_at'), ('sync_jobs', 'created_at')):
                if column not in {row[1] for row in db.execute('PRAGMA table_info(' + table + ')')}:
                    db.execute('ALTER TABLE ' + table + ' ADD COLUMN ' + column + ' REAL NOT NULL DEFAULT 0')
            if 'actor_name' not in {row[1] for row in db.execute('PRAGMA table_info(versions)')}:
                db.execute("ALTER TABLE versions ADD COLUMN actor_name TEXT NOT NULL DEFAULT ''")
            for column in ('error', 'error_code'):
                if column not in {row[1] for row in db.execute('PRAGMA table_info(sync_jobs)')}:
                    db.execute('ALTER TABLE sync_jobs ADD COLUMN ' + column + " TEXT NOT NULL DEFAULT ''")
            columns = {row[1] for row in db.execute('PRAGMA table_info(ingestion)')}
            for column, definition in (('stage', "TEXT NOT NULL DEFAULT 'prepare'"),
                    ('cached', 'INTEGER NOT NULL DEFAULT 0'), ('cache_size', 'INTEGER NOT NULL DEFAULT 0'),
                    ('error', "TEXT NOT NULL DEFAULT ''"), ('error_code', "TEXT NOT NULL DEFAULT ''"),
                    ('reindex_request', "TEXT NOT NULL DEFAULT ''")):
                if column not in columns:
                    db.execute('ALTER TABLE ingestion ADD COLUMN ' + column + ' ' + definition)
            if 'stage' not in columns:
                db.execute("UPDATE ingestion SET stage='index' WHERE status='synced'")
                db.execute("UPDATE ingestion SET status='queued',next_attempt=0 WHERE status IN ('retrying','unsupported')")
        if knowledge is not None:
            self.bind_knowledge(knowledge)

    @contextmanager
    def connect(self, *, readonly=False):
        db = sqlite3.connect(self.db_path.as_uri() + '?mode=ro', uri=True, timeout=3) if readonly else sqlite3.connect(self.db_path, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            yield db
            if not readonly:
                db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    @contextmanager
    def remote(self, source):
        client = self.remote_factory(source)
        try:
            yield client
        finally:
            close = getattr(client, 'close', None)
            if callable(close):
                close()

    def bind_knowledge(self, knowledge):
        if self.knowledge is knowledge:
            return
        self.knowledge = knowledge
        knowledge.source_acl = self.can_read_source
        knowledge.source_current = self.source_current
        knowledge.add_worker_task(self.process_one)

    def _actor(self, actor):
        if self.refresh_actor is not None:
            current = self.refresh_actor(actor)
            if not isinstance(current, dict) or current.get('id') != actor.get('id'):
                raise AssistantError('登录身份已变化，请重新登录。', 401)
            actor = {**current, 'scopes': sorted(set(current.get('scopes', [])) & set(actor.get('scopes', []))),
                'modules': sorted(set(current.get('modules', [])) & set(actor.get('modules', [])))}
        _member(actor)
        return actor

    def _table(self, key, db=None):
        if db is None:
            with self.connect(readonly=True) as connection:
                return self._table(key, connection)
        row = db.execute('SELECT * FROM tables WHERE id=?', (key,)).fetchone()
        if not row:
            raise AssistantError('资料表不存在或当前无权访问。', 404)
        return {**json.loads(row['payload']), 'revision': row['revision']}

    def _allowed(self, actor, source, scopes, *, write=False):
        actor = self._actor(actor)
        granted = set(actor.get('scopes', []))
        if 'ALL' in granted:
            granted = set(SCOPES)
        permitted = set(source['write_scopes'] if write else source['read_scopes'])
        from .assistant_shared_read import SHARED_READ
        shared = SHARED_READ.get()
        shared_read = not write and shared is not None and shared.active and shared.operator == actor['id']
        result = bool(source['enabled'] and scopes and (shared_read or set(scopes) <= granted & permitted))
        if result and self.authorize_source is not None:
            result = self.authorize_source(actor, source, list(scopes), 'write' if write else 'read') is True
        return result

    def _require(self, actor, source, scopes, *, write=False):
        if not self._allowed(actor, source, scopes, write=write):
            raise AssistantError('资料记录不存在或当前无权访问。', 404)

    def _record_scopes(self, source, fields):
        if not source['scope_field']:
            return source['fixed_scopes']
        value = fields.get(source['scope_field'])
        values = value if isinstance(value, list) else [value]
        if values and all(isinstance(v, dict) and v.get('type') == 'text'
                and isinstance(v.get('text'), str) for v in values):
            values = [''.join(v['text'] for v in values)]
        from upload_event_module.building_normalizer import extract_building_codes, normalize_building_name
        suffix = r'(?:\u697c|\u680b|\u5ea7|\u533a|\u673a\u623f|\u6570\u636e\u4e2d\u5fc3|DC)'
        unit = r'(?:\u5357\u901a)?(?:[A-EH]' + suffix + r'?|110(?:\u7ad9|\u697c|\u673a\u623f|\u6570\u636e\u4e2d\u5fc3|DC|KV)?)'
        span = r'[A-EH](?:\u697c|\u680b)?(?:-|~|\u2014|\u2013|\u81f3|\u5230)[A-EH](?:\u697c|\u680b)?'
        compact = r'[A-EH]{2,6}' + suffix
        atom = '(?:' + '|'.join((unit, span, compact)) + ')'
        separator = r'(?:,|/|;|\+|\u3001|\uff0c|\uff1b|\u53ca|\u548c|\u4e0e)'
        grammar = atom + '(?:' + separator + atom + ')*'
        result = set()
        for item in values:
            if isinstance(item, dict):
                if item.get('text') and item.get('name') and item['text'] != item['name']:
                    return []
                item = item.get('text') or item.get('name')
            if not isinstance(item, str) or not item.strip():
                return []
            text = ''.join(unicodedata.normalize('NFKC', item).split()).upper()
            text = normalize_building_name(text)
            # Extraction alone silently drops unknown portions and narrows ownership.
            if not re.fullmatch(grammar, text):
                return []
            for part in re.split(separator, text):
                codes = extract_building_codes(part)
                if not codes:
                    return []
                result.update(codes)
        if source['fixed_scopes'] and set(result) - set(source['fixed_scopes']):
            return []
        return sorted(result)

    def tables(self, actor):
        actor = self._actor(actor)
        with self.connect(readonly=True) as db:
            sources = [self._table(row[0], db) for row in db.execute('SELECT id FROM tables ORDER BY id')]
            cleanup = db.execute("SELECT count(*),MAX(error) FROM maintenance WHERE kind='remove' AND status<>'completed'").fetchone()
        items = []
        for source in sources:
            if actor.get('is_admin') or any(self._allowed(actor, source, [scope]) for scope in source['read_scopes']):
                candidates = set(source['write_scopes']) & set(source['fixed_scopes'] or SCOPES)
                can_create = (any(self._allowed(actor, source, [scope], write=True) for scope in candidates)
                    if source['scope_field'] else self._allowed(actor, source, source['fixed_scopes'], write=True))
                items.append({**source, 'capabilities': {'create': can_create,
                    'reindex': not actor.get('read_only', False),
                    'configure_tables': bool(actor.get('is_admin')), 'max_upload_bytes': MAX_FILE}})
        return {'items': items, 'can_configure': bool(actor.get('is_admin') and not actor.get('read_only')),
            **({'cleanup_pending': cleanup[0], 'cleanup_error': cleanup[1] or ''} if actor.get('is_admin') else {})}

    def save_table(self, actor, payload, *, key=None):
        actor = self._actor(actor)
        if not actor.get('is_admin') or actor.get('read_only'):
            raise AssistantError('仅管理员可配置资料表。', 403)
        if isinstance(payload, dict) and payload.get('url'):
            from .document_library_remote import parse_table_link
            link = parse_table_link(payload['url'], payload.get('table_id') or '')
            if link['kind'] == 'wiki':
                resolved = self.preview_table(actor, {'url': payload['url'], 'table_id': link['table_id']})['source']
                if not resolved:
                    raise AssistantError('请先选择具体的数据表。')
                if payload.get('base_token') and payload['base_token'] != resolved['base_token']:
                    raise AssistantError('Wiki 链接与多维表标识不一致。')
                payload = {**payload, 'base_token': resolved['base_token'], 'table_id': resolved['table_id'], 'url': resolved['url']}
        source = validate_source(payload, identity=key)
        revision = _digest(source)
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute("SELECT 1 FROM maintenance WHERE table_key=? AND kind='remove' AND status<>'completed'", (source['id'],)).fetchone():
                raise AssistantError('此标识的本地索引正在清理，请完成后重新接入。', 409)
            previous = db.execute('SELECT revision,payload FROM tables WHERE id=?', (source['id'],)).fetchone()
            if key and (not previous or payload.get('expected_revision') != previous[0]) or not key and previous:
                raise AssistantError('资料表配置已变化，请刷新后重试。', 409)
            if previous:
                old = json.loads(previous['payload'])
                if ((old['base_token'], old['table_id']) != (source['base_token'], source['table_id'])
                        and db.execute("SELECT 1 FROM operations WHERE table_key=? AND status IN ('pending','uncertain') LIMIT 1", (source['id'],)).fetchone()):
                    raise AssistantError('请先核验待确认的写入，再更换来源数据表。', 409)
            for row in db.execute('SELECT payload,id FROM tables WHERE id<>?', (source['id'],)):
                other = json.loads(row[0])
                if (other['base_token'], other['table_id']) == (source['base_token'], source['table_id']):
                    raise AssistantError('此数据表已接入，请勿重复配置。', 409)
            db.execute('INSERT INTO tables(id,payload,revision) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload,revision=excluded.revision,next_sync=0',
                (source['id'], _json(source), revision))
        # Canonical config is already changed, so old knowledge provenance fails closed.
        if self.knowledge is not None:
            self.knowledge.invalidate_sources(source['id'] + '/')
        self._enqueue_sync({**source, 'revision': revision}) if source['enabled'] else None
        return {**source, 'revision': revision}

    def preview_table(self, actor, payload):
        actor = self._actor(actor)
        if not actor.get('is_admin') or actor.get('read_only'):
            raise AssistantError('仅管理员可读取待接入数据表。', 403)
        if not isinstance(payload, dict) or set(payload) - {'url', 'table_id'}:
            raise AssistantError('数据表读取参数无效。')
        from .document_library_remote import DocumentLibraryRemote
        result = DocumentLibraryRemote.inspect_link(payload.get('url'), payload.get('table_id', ''))
        fresh = self._actor(actor)
        if not fresh.get('is_admin') or fresh.get('read_only'):
            raise AssistantError('读取过程中管理权限已变化，请重新读取。', 403)
        return result

    def schema(self, actor, key, *, refresh=False):
        source = self._table(key)
        if not any(self._allowed(actor, source, [scope]) for scope in source['read_scopes']):
            raise AssistantError('资料表不存在或当前无权访问。', 404)
        with self.connect(readonly=True) as db:
            cached = db.execute('SELECT payload FROM schema_cache WHERE table_key=? AND config_revision=?', (key, source['revision'])).fetchone()
        if cached and not refresh:
            fields = json.loads(cached[0])
        else:
            with self.remote(source) as remote:
                fields = remote.fields()
            self._cache_schema(source, fields)
        self._validate_schema(source, fields)
        if not any(self._allowed(actor, self._table(key), [scope]) for scope in source['read_scopes']):
            raise AssistantError('访问权限已变化，请重新读取。', 403)
        fields = [{**field, 'read_only': bool(field.get('is_computed') or field.get('type') not in _WRITABLE_TYPES | {17}
            or field.get('type') == 17 and field['field_id'] not in source['attachment_fields'])} for field in fields]
        people_fields = [field for field in fields if field.get('type') == 11]
        if people_fields:
            options, reason = [], ''
            try:
                options = self._people(actor, source)
                if not options:
                    reason = '当前楼栋权限范围内没有可选人员。'
            except AssistantError as exc:
                if exc.category != 'document_library_people_unavailable':
                    raise
                reason = str(exc)
            for field in people_fields:
                field.pop('options', None)
                field['property'] = {**(field.get('property') or {}), 'options': options}
                if reason:
                    field.update(read_only=True, read_only_reason=reason)
        return {'table': source, 'display_fields': source['display_fields'], 'attachment_fields': source['attachment_fields'],
            'fields': fields}

    def _cache_schema(self, source, fields):
        self._validate_schema(source, fields)
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if self._table(source['id'], db)['revision'] != source['revision']:
                raise AssistantError('资料表配置已变化，请重新读取。', 409)
            db.execute('INSERT OR REPLACE INTO schema_cache VALUES(?,?,?,?)',
                (source['id'], source['revision'], _json(fields), self.clock()))

    def _people(self, actor, source):
        actor = self._actor(actor)
        try:
            options = self.people_provider(actor, source) if self.people_provider else None
            if not isinstance(options, list):
                raise ValueError()
            return [{'id': person['id'], 'name': person['name']} for person in options]
        except Exception:
            raise AssistantError('人员目录暂未就绪，人员字段暂不可编辑，请稍后重新读取。', 503,
                category='document_library_people_unavailable') from None

    def _values(self, actor, source, values, by_id):
        values = dict(values)
        people_ids = None
        for fid, value in values.items():
            if by_id[fid].get('type') != 11:
                continue
            if (not isinstance(value, list) or any(not isinstance(person, dict)
                    or set(person) - {'id', 'name'} or not isinstance(person.get('id'), str)
                    or not re.fullmatch(r'ou_[A-Za-z0-9]+', person['id']) for person in value)):
                raise AssistantError('人员字段必须使用有效的飞书 OpenID 列表。')
            if (by_id[fid].get('property') or {}).get('multiple') is False and len(value) > 1:
                raise AssistantError('此人员字段仅允许选择一人。')
            ids = [person['id'] for person in value]
            if len(ids) != len(set(ids)):
                raise AssistantError('人员字段不能重复选择同一人。')
            if people_ids is None:
                people_ids = {person['id'] for person in self._people(actor, source)}
            if set(ids) - people_ids:
                raise AssistantError('所选人员已不可用或超出当前楼栋权限，请重新选择。', 403)
            values[fid] = [{'id': identity} for identity in ids]
        return values

    @staticmethod
    def _validate_schema(source, fields):
        if not isinstance(fields, list) or not fields or any(not isinstance(field, dict) or not field.get('field_id') or not field.get('field_name') for field in fields):
            raise AssistantError('来源资料字段信息不完整。', 503, category='bad_response')
        by_id = {field['field_id']: field for field in fields}
        required = [*source['display_fields'], *source['attachment_fields'], *([source['scope_field']] if source['scope_field'] else [])]
        if any(fid not in by_id for fid in required) or any(by_id[fid].get('type') != 17 for fid in source['attachment_fields']):
            raise AssistantError('配置字段与来源数据表不匹配，请检查表配置。', 409, category='document_library_schema')
        return by_id

    def _normalize(self, source, raw, fields, previous=None, replacement=None):
        by_name = {field['field_name']: field['field_id'] for field in fields}
        by_id = {field['field_id']: field for field in fields}
        values = {key if key in by_id else by_name.get(key, key): value for key, value in (raw.get('fields') or {}).items()}
        attachment_fields = [fid for fid, field in by_id.items() if field.get('type') == 17]
        old = {(a['field_id'], a['file_token']): a['attachment_id'] for a in (previous or {}).get('attachments', [])}
        attachments = []
        for fid in attachment_fields:
            for file in values.get(fid) or []:
                if not isinstance(file, dict) or not file.get('file_token'):
                    raise AssistantError('来源附件信息无效。', 503)
                token = file['file_token']
                identity = old.get((fid, token), uuid.uuid4().hex)
                if replacement and replacement[0] == fid and replacement[1] == token:
                    identity = replacement[2]
                attachments.append({'field_id': fid, 'attachment_id': identity, 'file_token': token,
                    'name': Path(str(file.get('name') or 'attachment').replace('\\', '/')).name,
                    'size': file.get('size', 0), 'type': file.get('type', '')})
        # Volatile signed URLs are not part of a source version or its conflict token.
        for fid in attachment_fields:
            values[fid] = [{'file_token': a['file_token'], 'name': a['name'], 'size': a['size'], 'type': a['type']}
                for a in attachments if a['field_id'] == fid]
        field_names = {fid: field['field_name'] for fid, field in by_id.items()}
        payload = {'record_id': raw['record_id'], 'fields': values, 'field_names': field_names, 'attachments': attachments,
            'scopes': self._record_scopes(source, values), 'config_revision': source['revision'],
            'origin': _digest([source['base_token'], source['table_id']])}
        payload['revision'] = _digest({'fields': values, 'field_names': field_names, 'table_revision': source['revision']})
        return payload

    @staticmethod
    def _record_text(source, record):
        names = record.get('field_names', {})
        attachment_fields = {item['field_id'] for item in record['attachments']}
        lines = [source['name'], record['record_id']]
        for fid in sorted(set(names) | set(record['fields'])):
            value = record['fields'].get(fid)
            if fid in attachment_fields:
                value = [{key: item[key] for key in ('name', 'size', 'type')}
                    for item in record['attachments'] if item['field_id'] == fid]
            lines.append(f"{names.get(fid, fid)} ({fid}): {value if isinstance(value, str) else _json(value)}")
        return '\n\n'.join(lines)

    def _cached(self, key, identity, db=None):
        if db is None:
            with self.connect(readonly=True) as connection:
                return self._cached(key, identity, connection)
        row = db.execute('SELECT * FROM records WHERE table_key=? AND id=?', (key, identity)).fetchone()
        return dict(row) if row else None

    @staticmethod
    def _source_entries(source, record):
        entries = [('record', {'kind': 'record', 'name': record['record_id'] + '.txt'}),
            *((a['field_id'] + '/' + a['attachment_id'], a) for a in record['attachments'])]
        for suffix, entry in entries:
            yield source['id'] + '/' + record['record_id'] + '/' + suffix, {
                'table_key': source['id'], 'record_id': record['record_id'], 'record_revision': record['revision'],
                'config_revision': source['revision'], 'origin': _digest([source['base_token'], source['table_id']]),
                'scopes': record['scopes'], **entry}

    def _remember(self, source, record, *, actor='', actor_name='', reason='source_sync', generation='', operation='', job=None):
        key, identity = source['id'], record['record_id']
        indexed = self.knowledge.source_states(key + '/' + identity + '/') if self.knowledge else None
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if self._table(key, db)['revision'] != source['revision']:
                raise AssistantError('资料表配置已变化，请重新读取。', 409)
            if job and not db.execute('SELECT 1 FROM sync_jobs WHERE id=? AND lease=? AND lease_until>?', (*job, self.clock())).fetchone():
                raise AssistantError('来源同步任务状态已变化，请稍后重试。', 409)
            previous = self._cached(key, identity, db)
            unconfirmed = db.execute("SELECT id FROM operations WHERE table_key=? AND record_id=? AND status IN ('pending','uncertain') AND id<>? LIMIT 1", (key, identity, operation)).fetchone()
            if unconfirmed:
                return False
            if previous and previous['pending_operation'] and previous['pending_operation'] != operation:
                return False
            if not previous or previous['revision'] != record['revision'] or previous['deleted']:
                number = db.execute('SELECT COALESCE(MAX(number),0)+1 FROM versions WHERE table_key=? AND record_id=?', (key, identity)).fetchone()[0]
                db.execute('INSERT INTO versions(table_key,record_id,number,revision,payload,actor,reason,created_at,actor_name) VALUES(?,?,?,?,?,?,?,?,?)',
                    (key, identity, number, record['revision'], _json(record), actor, reason, self.clock(), actor_name))
            db.execute('INSERT INTO records(table_key,id,payload,revision,generation,observed_at) VALUES(?,?,?,?,?,?) '
                'ON CONFLICT(table_key,id) DO UPDATE SET payload=excluded.payload,revision=excluded.revision,deleted=0,'
                "generation=CASE WHEN excluded.generation<>'' THEN excluded.generation ELSE records.generation END,pending_operation='',observed_at=excluded.observed_at",
                (key, identity, _json(record), record['revision'], generation, self.clock()))
            current_keys = set()
            # Rebuild desired tasks even for an unchanged snapshot, so older caches backfill.
            for source_key, payload in self._source_entries(source, record):
                current_keys.add(source_key)
                old = db.execute('SELECT payload,status FROM ingestion WHERE source_key=?', (source_key,)).fetchone()
                missing = indexed is not None and (source_key not in indexed or indexed[source_key].get('missing'))
                if not old or json.loads(old[0]) != payload or old['status'] == 'removed' or old['status'] == 'synced' and missing:
                    db.execute("INSERT INTO ingestion(source_key,table_key,record_id,record_revision,payload,status) VALUES(?,?,?,?,?,'queued') "
                        "ON CONFLICT(source_key) DO UPDATE SET record_revision=excluded.record_revision,payload=excluded.payload,status='queued',stage='prepare',cached=0,cache_size=0,error='',error_code='',attempts=0,next_attempt=0,lease='',lease_until=0,reindex_request=''",
                        (source_key, key, identity, record['revision'], _json(payload)))
            for row in db.execute('SELECT source_key FROM ingestion WHERE table_key=? AND record_id=?', (key, identity)).fetchall():
                if row[0] not in current_keys:
                    db.execute("UPDATE ingestion SET status='removed' WHERE source_key=?", (row[0],))
        if self.knowledge is not None:
            if not previous or previous['revision'] != record['revision'] or previous['deleted']:
                self.knowledge.invalidate_sources(key + '/' + identity + '/')
            self.knowledge._wake.set()
        return True

    def _public(self, source, record, actor):
        result = json.loads(_json(record))
        for attachment in result['attachments']:
            attachment.pop('file_token', None)
            attachment['download_url'] = f"/api/document-library/tables/{source['id']}/records/{record['record_id']}/attachments/{attachment['attachment_id']}/file"
        for fid in set(source['attachment_fields']) | {a['field_id'] for a in result['attachments']}:
            result['fields'][fid] = [a for a in result['attachments'] if a['field_id'] == fid]
        with self.connect(readonly=True) as db:
            tasks = db.execute('SELECT * FROM ingestion WHERE table_key=? AND record_id=? AND status<>?',
                (source['id'], record['record_id'], 'removed')).fetchall()
        from .document_library_index import read_index_states
        knowledge_db = self.knowledge.db if self.knowledge is not None else self.root.parent / 'lighthouse_openclaw/knowledge_base/knowledge.sqlite3'
        indexed = read_index_states(knowledge_db, source['id'] + '/' + record['record_id'] + '/', self.source_current)
        items = []
        tasks = {task['source_key']: task for task in tasks}
        for source_key, expected in self._source_entries(source, record):
            task = tasks.get(source_key)
            if not task or json.loads(task['payload']) != expected:
                items.append({**{k: expected[k] for k in ('name', 'field_id', 'attachment_id') if k in expected},
                    'kind': expected.get('kind', 'attachment'), 'status': 'queued', 'stage': 'prepare',
                    'cached': expected.get('kind') == 'record' or self._cache_size(expected.get('file_token', '')) > 0,
                    'vectorized': False, 'error': '等待补入本地索引。', 'error_code': ''})
                continue
            payload = json.loads(task['payload'])
            if payload.get('record_revision') != record['revision'] or payload.get('config_revision') != source['revision']:
                continue
            state = (indexed or {}).get(task['source_key'], {}) if task['status'] == 'synced' else {}
            status = state.get('status') or ('queued' if task['status'] in {'running', 'synced'} else task['status'])
            cached = payload.get('kind') == 'record' or self._cache_size(payload.get('file_token', '')) > 0
            stage = task['stage']
            if task['status'] == 'synced' and indexed is None:
                status = 'unavailable'
            elif stage == 'prepare' and status in {'queued', 'running'}:
                status = 'caching'
            items.append({**{k: payload[k] for k in ('name', 'field_id', 'attachment_id') if k in payload},
                'kind': payload.get('kind', 'attachment'), 'status': status, 'stage': stage, 'cached': cached,
                'vectorized': bool(state.get('vectorized') and status == 'ready'),
                'error_code': task['error_code'],
                'error': state.get('error', '') or task['error'] or ('原件已缓存，此格式暂不支持正文索引。' if status == 'unsupported'
                    else '暂时无法读取实际索引状态，请稍后刷新。' if status == 'unavailable'
                    else '知识库索引缺失，等待补入。' if task['status'] == 'synced' and not state else '')})
        result['knowledge_items'] = items
        states = {item['status'] for item in items}
        ready = sum(bool(item['vectorized']) for item in items)
        result['knowledge_summary'] = {'total': len(items), 'ready': ready,
            'cached': sum(bool(item['cached']) for item in items), 'vectorized': bool(items and ready == len(items))}
        result['knowledge_status'] = next((state for state in ('blocked', 'failed', 'retrying', 'unavailable', 'caching', 'queued', 'indexing', 'unsupported', 'text_only', 'ready')
            if state in states), 'none')
        result['config_revision'] = source['revision']
        editable = self._allowed(actor, source, record['scopes'], write=True)
        result['capabilities'] = {'edit': editable, 'upload': editable, 'reindex': not actor.get('read_only', False), 'max_upload_bytes': MAX_FILE}
        return result

    def list_records(self, actor, key, query):
        source = self._table(key)
        actor = self._actor(actor)
        if not any(self._allowed(actor, source, [scope]) for scope in source['read_scopes']):
            raise AssistantError('资料表不存在或当前无权访问。', 404)
        try:
            size = int(query.get('page_size', 50))
            offset = int(query.get('cursor') or 0)
            if not 1 <= size <= 100 or offset < 0:
                raise ValueError()
        except (ValueError, TypeError):
            raise AssistantError('分页参数无效。') from None
        scope, q = query.get('scope', ''), str(query.get('q') or '').strip()[:200].casefold()
        if scope and scope not in SCOPES:
            raise AssistantError('楼栋范围无效。')
        with self.connect(readonly=True) as db:
            db.execute('BEGIN')
            rows = db.execute('SELECT payload,observed_at FROM records WHERE table_key=? AND deleted=0 AND pending_operation=\'\' ORDER BY id', (key,)).fetchall()
            completed = db.execute("SELECT 1 FROM sync_jobs WHERE table_key=? AND config_revision=? AND status='completed' LIMIT 1", (key, source['revision'])).fetchone()
            job = db.execute('SELECT status,error FROM sync_jobs WHERE table_key=? AND config_revision=? ORDER BY created_at DESC,rowid DESC LIMIT 1', (key, source['revision'])).fetchone()
        rows = [row for row in rows if json.loads(row[0]).get('config_revision') == source['revision']]
        if not completed and not rows:
            if job and job['status'] == 'retrying':
                raise AssistantError(job['error'] or '来源资料同步未完成，请重试同步；持续失败时请检查飞书鉴权和数据表权限。', 503,
                    category='document_library_sync_failed')
            raise AssistantError('正在后台同步来源资料，完成后将自动显示列表。', 503,
                category='document_library_sync_pending')
        records = []
        for row in rows:
            record = json.loads(row[0])
            if (self._allowed(actor, source, record['scopes']) and (not scope or scope in record['scopes'])
                    and (not q or q in _json(record['fields']).casefold())):
                records.append(record)
        items = []
        for record in records[offset:offset + size]:
            public = self._public(source, record, actor)
            items.append(public)
        return {'items': items, 'total': len(records), 'has_more': len(records) > offset + size,
            'cursor': str(offset + size) if len(records) > offset + size else '', 'page_size': size,
            'config_revision': source['revision'], 'sync_status': job['status'] if job else 'pending',
            'cache': {'complete': bool(completed), 'updated_at': max((row['observed_at'] for row in rows), default=0),
                'refreshing': bool(job and job['status'] in ('queued', 'running')), 'error': job['error'] if job else ''}}

    def cached_record(self, actor, key, identity, *, refresh=False):
        source, cached = self._table(key), self._cached(key, identity)
        refresh = refresh or not cached or json.loads(cached['payload']).get('config_revision') != source['revision']
        return self.record(actor, key, identity, refresh=refresh)

    def record(self, actor, key, identity, *, refresh=True):
        source = self._table(key)
        cached = self._cached(key, identity)
        if cached and (cached['deleted'] or cached['pending_operation']):
            raise AssistantError('资料记录正在等待写入核验，暂不可用。', 409)
        if cached:
            self._require(actor, source, json.loads(cached['payload'])['scopes'])
        if refresh:
            with self.remote(source) as remote:
                fields, raw = remote.fields(), remote.get(identity)
            self._validate_schema(source, fields)
            self._cache_schema(source, fields)
            if not raw:
                self._retire(key, identity, config_revision=source['revision'])
                raise AssistantError('资料记录不存在。', 404)
            record = self._normalize(source, raw, fields, json.loads(cached['payload']) if cached else None)
            if not self._remember(source, record):
                raise AssistantError('资料记录正在等待写入核验。', 409)
            self._require(actor, self._table(key), record['scopes'])
        elif cached:
            record = json.loads(cached['payload'])
            if record['config_revision'] != source['revision']:
                raise AssistantError('资料表配置已变化，请重新读取。', 409)
        else:
            raise AssistantError('资料记录不存在。', 404)
        self._require(actor, self._table(key), record['scopes'])
        return self._public(source, record, actor)

    def _retire(self, key, identity, *, config_revision=None, job=None):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if config_revision and self._table(key, db)['revision'] != config_revision:
                raise AssistantError('来源配置已变化，请重新读取。', 409)
            if job and not db.execute('SELECT 1 FROM sync_jobs WHERE id=? AND lease=? AND lease_until>?', (*job, self.clock())).fetchone():
                raise AssistantError('来源同步任务状态已变化，请稍后重试。', 409)
            db.execute('UPDATE records SET deleted=1 WHERE table_key=? AND id=? AND pending_operation=\'\'', (key, identity))
            db.execute("UPDATE ingestion SET status='removed' WHERE table_key=? AND record_id=?", (key, identity))
        if self.knowledge is not None:
            self.knowledge.invalidate_sources(key + '/' + identity + '/')

    def can_read_source(self, actor, provenance):
        try:
            key, identity = provenance['table_key'], provenance['record_id']
            source = self._table(key)
            cached = self._cached(key, identity)
            historical = provenance.get('historical_version') is True
            if (not cached or cached['deleted'] or cached['pending_operation']
                    or provenance.get('origin') != _digest([source['base_token'], source['table_id']])
                    or not historical and source['revision'] != provenance['config_revision']):
                return False
            record = json.loads(cached['payload'])
            if not provenance.get('historical_version') and cached['revision'] != provenance['record_revision']:
                return False
            if actor.get('_shared_business_queries'):
                return bool(source['enabled'] and record['scopes'] and provenance['scopes'])
            return self._allowed(actor, source, record['scopes']) and self._allowed(actor, source, provenance['scopes'])
        except Exception:
            return False

    def source_current(self, provenance):
        try:
            source = self._table(provenance['table_key'])
            row = self._cached(provenance['table_key'], provenance['record_id'])
            return bool(source['enabled'] and row and not row['deleted'] and not row['pending_operation']
                and provenance.get('origin') == _digest([source['base_token'], source['table_id']])
                and row['revision'] == provenance['record_revision'] and source['revision'] == provenance['config_revision'])
        except Exception:
            return False

    def versions(self, actor, key, identity):
        current = self.record(actor, key, identity)
        source = self._table(key)
        with self.connect(readonly=True) as db:
            rows = db.execute('SELECT * FROM versions WHERE table_key=? AND record_id=? ORDER BY number DESC', (key, identity)).fetchall()
        items = []
        for row in rows:
            record = json.loads(row['payload'])
            if record.get('origin') == _digest([source['base_token'], source['table_id']]) and self._allowed(actor, source, record['scopes']):
                public = self._public(source, record, actor)
                for attachment in public['attachments']:
                    attachment['download_url'] += '?version=' + str(row['number'])
                items.append({**public, 'number': row['number'], 'reason': row['reason'], 'created_at': row['created_at'],
                    'created_by': row['actor_name'] or row['actor'] or 'source_sync', 'actor_id': row['actor']})
        return {'items': items, 'revision': current['revision']}

    def download(self, actor, key, identity, attachment_id, *, version=None, refresh=True):
        self.record(actor, key, identity, refresh=refresh)
        source = self._table(key)
        cached = self._cached(key, identity)
        record = json.loads(cached['payload'])
        if version is not None:
            with self.connect(readonly=True) as db:
                row = db.execute('SELECT payload FROM versions WHERE table_key=? AND record_id=? AND number=?', (key, identity, int(version))).fetchone()
            if not row:
                raise AssistantError('资料版本不存在。', 404)
            record = json.loads(row[0])
        if record.get('origin') != _digest([source['base_token'], source['table_id']]):
            raise AssistantError('附件版本不属于当前来源。', 404)
        self._require(actor, source, record['scopes'])
        attachment = next((a for a in record['attachments'] if a['attachment_id'] == attachment_id), None)
        if not attachment:
            raise AssistantError('附件不存在或当前无权访问。', 404)
        path = self._content_path(source, attachment['file_token'], record_id=identity, field_id=attachment['field_id'])
        fresh_source = self._table(key)
        self._require(actor, fresh_source, record['scopes'])
        if fresh_source['revision'] != source['revision']:
            raise AssistantError('下载过程中资料表配置已变化，请重新读取。', 409)
        current = self._cached(key, identity)
        if not current or current['deleted'] or current['pending_operation'] or current['revision'] != cached['revision']:
            raise AssistantError('下载过程中来源资料已变化，请重新读取。', 409)
        return path, attachment['name']

    def _cache_size(self, token):
        if not token:
            return 0
        path = self.root / 'files' / hashlib.sha256(token.encode()).hexdigest()
        try:
            size = path.stat().st_size
            return size if path.is_file() and not path.is_symlink() and 0 < size <= MAX_FILE else 0
        except OSError:
            return 0

    def _content_path(self, source, token, *, record_id=None, field_id=None):
        directory = self.root / 'files'
        directory.mkdir(exist_ok=True)
        path = directory / hashlib.sha256(token.encode()).hexdigest()
        if not self._cache_size(token):
            temporary = path.with_name(uuid.uuid4().hex + '.part')
            try:
                self._reserve_download(source['id'])
                with self.remote(source) as remote:
                    remote.download_to_file(token, temporary, record_id=record_id, field_id=field_id)
                if not temporary.is_file() or not 0 < temporary.stat().st_size <= MAX_FILE:
                    raise AssistantError('附件无效或超过大小限制。', 413, category='too_large')
                temporary.replace(path)
            finally:
                temporary.unlink(missing_ok=True)
        return path

    def _reserve_download(self, key):
        # Shared by portal and AI processes; cached files never consume a download slot.
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            now = self.clock()
            after = db.execute('SELECT COALESCE(MAX(download_after),0) FROM tables').fetchone()[0]
            delay = max(0, after - now)
            if delay > 1:
                raise AssistantError('附件下载正在排队，稍后重试。', 429, category='rate_limit')
            db.execute('UPDATE tables SET download_after=? WHERE id=?', (max(now, after) + .25, key))
        if delay:
            self._sleep_download(delay)

    def check_response(self, actor, key, result, *, configuration=False, preview=False, removal=False):
        actor = self._actor(actor)
        if configuration or preview or removal:
            if not actor.get('is_admin') or actor.get('read_only'):
                raise AssistantError('仅管理员可配置资料表。', 403)
            if preview or removal:
                return
            if result.get('revision') != self._table(key or result['id'])['revision']:
                raise AssistantError('资料表配置已变化，请重新读取。', 409)
            return
        if not key:
            return
        source = self._table(key)
        expected = result.get('config_revision') or (result.get('table') or {}).get('revision')
        if expected and expected != source['revision']:
            raise AssistantError('请求过程中资料表配置已变化，请重新读取。', 409)
        if not any(self._allowed(actor, source, [scope]) for scope in source['read_scopes']):
            raise AssistantError('资料表访问权限已变化。', 403)
        for record in result['items'] if isinstance(result.get('items'), list) else [result]:
            if 'scopes' in record:
                self._require(actor, source, record['scopes'])
                cached = self._cached(key, record['record_id'])
                if not cached or cached['deleted']:
                    raise AssistantError('资料记录已移除，请重新读取。', 404)
                current = json.loads(cached['payload'])
                self._require(actor, source, current['scopes'])
                if (cached['pending_operation'] or current.get('config_revision') != source['revision']
                        or 'number' not in record and current['revision'] != record.get('revision')):
                    raise AssistantError('请求过程中资料记录已变化，请重新读取。', 409)

    @staticmethod
    def _write_metadata(payload):
        operation = payload.get('operation_id')
        reason = payload.get('reason')
        if not isinstance(operation, str) or not re.fullmatch(r'[A-Za-z0-9_-]{8,128}', operation):
            raise AssistantError('请提供有效且可复用的操作标识 operation_id。')
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 500:
            raise AssistantError('请填写修改原因，长度为1至500个字符。')
        return operation, reason.strip()

    def _claim_write(self, actor, source, identity, payload, *, fields, upload=None):
        operation, reason = self._write_metadata(payload)
        fingerprint = _digest({'actor': actor['id'], 'table': source['id'], 'record': identity,
            'source_origin': _digest([source['base_token'], source['table_id']]), 'fields': payload.get('fields'),
            'expected_revision': payload.get('expected_revision'), 'reason': reason, 'upload': upload})
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            previous = db.execute('SELECT * FROM operations WHERE id=?', (operation,)).fetchone()
            if previous:
                if previous['fingerprint'] != fingerprint:
                    raise AssistantError('该操作标识已用于其他请求，请重新读取后核验。', 409)
                return dict(previous), False
            if self._table(source['id'], db)['revision'] != source['revision']:
                raise AssistantError('资料表配置已变化，请重新读取。', 409)
            if identity:
                record = self._cached(source['id'], identity, db)
                if not record or record['deleted'] or record['pending_operation']:
                    raise AssistantError('资料记录不可用或正在等待写入核验。', 409)
                if payload.get('expected_revision') != record['revision']:
                    raise AssistantError('资料记录已变化，请刷新后重试。', 409)
                db.execute('UPDATE records SET pending_operation=? WHERE table_key=? AND id=?', (operation, source['id'], identity))
            data = {'fields': fields, 'reason': reason, 'config_revision': source['revision'], 'upload': upload,
                'actor_name': str(actor.get('name') or '')[:120]}
            db.execute("INSERT INTO operations VALUES(?,?,?,?,?,'pending',?,NULL,?)",
                (operation, fingerprint, source['id'], identity or '', actor['id'], _json(data), self.clock()))
            return dict(db.execute('SELECT * FROM operations WHERE id=?', (operation,)).fetchone()), True

    @staticmethod
    def _verify_fields(record, expected, fields):
        types = {field['field_id']: field.get('type') for field in fields}
        for fid, value in expected.items():
            actual = record['fields'].get(fid)
            if types.get(fid) == 17:
                if [item['file_token'] for item in actual or []] != [item['file_token'] for item in value or []]:
                    return False
            elif types.get(fid) == 11:
                if (not isinstance(actual or [], list) or any(not isinstance(item, dict)
                        or not isinstance(item.get('id'), str) for item in actual or [])
                        or sorted(item['id'] for item in actual or []) != sorted(item['id'] for item in value)):
                    return False
            elif actual != value:
                return False
        return True

    def _complete_write(self, actor, source, operation, fields, raw, *, replacement=None):
        data = json.loads(operation['payload'])
        old = self._cached(source['id'], raw['record_id'])
        record = self._normalize(source, raw, fields, json.loads(old['payload']) if old else None, replacement)
        if data['fields'] is None or not self._verify_fields(record, data['fields'], fields):
            raise AssistantError('回读来源未能确认写入结果，系统不会自动重复提交。', 503,
                category='document_library_uncertain')
        if not self._remember(source, record, actor=operation['actor'], actor_name=data.get('actor_name', ''), reason=data['reason'], operation=operation['id']):
            raise AssistantError('来源写入结果需要核验。', 503)
        with self.connect() as db:
            db.execute("UPDATE operations SET status='completed',record_id=?,result=? WHERE id=?",
                (record['record_id'], _json({'revision': record['revision']}), operation['id']))
        self._require(actor, self._table(source['id']), record['scopes'])
        return {**self._public(source, record, actor), 'operation_id': operation['id']}

    def _recover_write(self, actor, source, operation, fields):
        if operation['status'] == 'failed':
            raise AssistantError('原写入已在发送前被阻止，系统不会自动重复提交。', 403,
                category='write_blocked')
        if operation['status'] == 'completed':
            return {**self.record(actor, source['id'], operation['record_id']), 'operation_id': operation['id']}
        data = json.loads(operation['payload'])
        if operation['record_id'] and data['fields'] is not None:
            with self.remote(source) as remote:
                raw = remote.get(operation['record_id'])
            if raw:
                replacement = data.get('replacement')
                return self._complete_write(actor, source, operation, fields, raw, replacement=replacement)
        raise AssistantError('上次操作结果尚未确认，系统未重复提交，请核验来源记录。', 503,
            category='document_library_uncertain')

    def _uncertain(self, operation):
        with self.connect() as db:
            db.execute("UPDATE operations SET status='uncertain' WHERE id=? AND status<>'completed'", (operation['id'],))

    def _abort_blocked(self, operation):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute("UPDATE operations SET status='failed' WHERE id=? AND status<>'completed'", (operation['id'],))
            db.execute("UPDATE records SET pending_operation='' WHERE pending_operation=?", (operation['id'],))

    def save_record(self, actor, key, payload, *, identity=None):
        actor = self._actor(actor)
        if not isinstance(payload, dict) or set(payload) - {'fields', 'expected_revision', 'operation_id', 'reason'}:
            raise AssistantError('资料记录请求无效。')
        self._write_metadata(payload)
        source = self._table(key)
        values = payload.get('fields')
        if not isinstance(values, dict) or not values:
            raise AssistantError('请提供保留原始类型的字段值。')
        with self.remote(source) as remote:
            fields = remote.fields()
            by_id = self._validate_schema(source, fields)
            if any(fid not in by_id or by_id[fid].get('is_computed') or by_id[fid].get('type') not in _WRITABLE_TYPES for fid in values):
                raise AssistantError('字段不存在或为只读；附件请通过上传功能提交。')
            # Check an existing receipt before refreshing a pending local record.
            with self.connect(readonly=True) as db:
                previous = db.execute('SELECT * FROM operations WHERE id=?', (payload['operation_id'],)).fetchone()
            if previous:
                operation, _ = self._claim_write(actor, source, identity, payload, fields=values)
                return self._recover_write(actor, source, operation, fields)
            current = None
            if identity:
                self.record(actor, key, identity)
                current = json.loads(self._cached(key, identity)['payload'])
                self._require(actor, source, current['scopes'], write=True)
            values = self._values(actor, source, values, by_id)
            candidate = {**(current['fields'] if current else {}), **values}
            self._require(actor, source, self._record_scopes(source, candidate), write=True)
            operation, fresh = self._claim_write(actor, source, identity, payload, fields=values)
            if not fresh:
                return self._recover_write(actor, source, operation, fields)
            try:
                self._require(actor, self._table(key), self._record_scopes(source, candidate), write=True)
                wire = {by_id[fid]['field_name']: value for fid, value in values.items()}
                result = remote.update(identity, wire) if identity else remote.create(wire, operation['id'])
                identity = result['record_id']
                with self.connect() as db:
                    db.execute('UPDATE operations SET record_id=? WHERE id=?', (identity, operation['id']))
                operation['record_id'] = identity
                raw = remote.get(identity)
                if not raw:
                    raise AssistantError('无法核验来源写入结果。', 503)
                return self._complete_write(actor, source, operation, fields, raw)
            except Exception as exc:
                if isinstance(exc, AssistantError) and exc.category == 'write_blocked':
                    self._abort_blocked(operation)
                    raise
                self._uncertain(operation)
                # One read-back can confirm a timed-out update, never repeat its mutation.
                if operation['record_id']:
                    try:
                        return self._recover_write(actor, source, operation, fields)
                    except Exception:
                        pass
                raise AssistantError('无法确认写入结果，系统不会自动重复提交，请核验来源记录。', 503,
                    category='document_library_uncertain') from None

    def upload_attachment(self, actor, key, identity, payload, name, content):
        actor = self._actor(actor)
        if not isinstance(payload, dict) or set(payload) - {'field_id', 'expected_revision', 'operation_id', 'reason', 'replace_attachment_id'}:
            raise AssistantError('附件请求无效。')
        self._write_metadata(payload)
        name = Path(str(name or '').replace('\\', '/')).name
        if not name or len(name) > 180 or any(ord(ch) < 32 for ch in name):
            raise AssistantError('附件文件名无效。')
        upload_dir = self.root / 'uploads'
        upload_dir.mkdir(exist_ok=True)
        path = upload_dir / (uuid.uuid4().hex + '.part')
        try:
            digest, size = hashlib.sha256(), 0
            with path.open('wb') as output:
                while True:
                    chunk = content.read(1024 * 1024)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > MAX_FILE:
                        raise AssistantError('单个文件不能超过100 MiB。', 413)
                    output.write(chunk)
                    digest.update(chunk)
            if not size:
                raise AssistantError('不能上传空文件。')
            upload = {'field_id': payload.get('field_id'), 'replace_attachment_id': payload.get('replace_attachment_id'),
                'name': name, 'size': size, 'hash': digest.hexdigest()}
            source = self._table(key)
            with self.remote(source) as remote:
                fields = remote.fields()
                self._validate_schema(source, fields)
                fid = payload.get('field_id')
                if fid not in source['attachment_fields']:
                    raise AssistantError('该字段未配置为附件字段。')
                with self.connect(readonly=True) as db:
                    previous = db.execute('SELECT * FROM operations WHERE id=?', (payload['operation_id'],)).fetchone()
                if previous:
                    operation, _ = self._claim_write(actor, source, identity, payload, fields=None, upload=upload)
                    return self._recover_write(actor, source, operation, fields)
                self.record(actor, key, identity)
                current = json.loads(self._cached(key, identity)['payload'])
                self._require(actor, source, current['scopes'], write=True)
                replace = payload.get('replace_attachment_id')
                if replace and not any(a['attachment_id'] == replace and a['field_id'] == fid for a in current['attachments']):
                    raise AssistantError('待替换的附件不存在。', 404)
                operation, fresh = self._claim_write(actor, source, identity, payload, fields=None, upload=upload)
                if not fresh:
                    return self._recover_write(actor, source, operation, fields)
                try:
                    self._require(actor, self._table(key), current['scopes'], write=True)
                    added = remote.upload(path, name)
                    target = replace or uuid.uuid4().hex
                    attachments = [{'file_token': a['file_token']} for a in current['attachments'] if a['field_id'] == fid and a['attachment_id'] != replace]
                    attachments.append({'file_token': added['file_token']})
                    expected = {fid: attachments}
                    data = json.loads(operation['payload'])
                    data.update(fields=expected, replacement=[fid, added['file_token'], target])
                    operation['payload'] = _json(data)
                    with self.connect() as db:
                        db.execute('UPDATE operations SET payload=? WHERE id=?', (operation['payload'], operation['id']))
                    self._require(actor, self._table(key), current['scopes'], write=True)
                    field_name = next(field['field_name'] for field in fields if field['field_id'] == fid)
                    remote.update(identity, {field_name: attachments})
                    raw = remote.get(identity)
                    if not raw:
                        raise AssistantError('无法核验来源附件。', 503)
                    return self._complete_write(actor, source, operation, fields, raw, replacement=data['replacement'])
                except Exception as exc:
                    if isinstance(exc, AssistantError) and exc.category == 'write_blocked':
                        self._abort_blocked(operation)
                        raise
                    self._uncertain(operation)
                    try:
                        return self._recover_write(actor, source, operation, fields)
                    except Exception:
                        raise AssistantError('无法确认附件提交结果，系统不会自动重复提交，请核验来源记录。', 503,
                            category='document_library_uncertain') from None
        finally:
            path.unlink(missing_ok=True)

    @staticmethod
    def _maintenance_request(actor, key, payload, kind):
        operation = payload.get('operation_id')
        try:
            if not isinstance(operation, str) or str(uuid.UUID(operation)) != operation.lower():
                raise ValueError()
        except (ValueError, AttributeError):
            raise AssistantError('请提供有效且可复用的 UUID 操作标识。') from None
        return operation, _digest({'actor': actor['id'], 'table': key, 'kind': kind, 'payload': payload})

    def reindex(self, actor, key, payload):
        actor = self._actor(actor)
        if actor.get('read_only'):
            raise AssistantError('只读会话不能重新向量化。', 403)
        if set(payload) - {'operation_id', 'expected_revision', 'record_ids', 'all', 'q', 'scope'}:
            raise AssistantError('重新向量化参数无效。')
        ids, all_records = payload.get('record_ids'), payload.get('all', False)
        if type(all_records) is not bool or (all_records and ids is not None) or (not all_records and
                (not isinstance(ids, list) or not ids or len(ids) > 1000 or any(not isinstance(v, str) or not v for v in ids))):
            raise AssistantError('请选择记录或全部匹配记录。')
        scope, q = payload.get('scope', ''), str(payload.get('q') or '').strip()[:200].casefold()
        if scope and scope not in SCOPES:
            raise AssistantError('楼栋范围无效。')
        operation, fingerprint = self._maintenance_request(actor, key, payload, 'reindex')
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            actor = self._actor(actor)
            if actor.get('read_only'):
                raise AssistantError('只读会话不能重新向量化。', 403)
            source = self._table(key, db)
            if not any(self._allowed(actor, source, [s]) for s in source['read_scopes']):
                raise AssistantError('当前无权访问此资料表。', 403)
            if source['revision'] != payload.get('expected_revision'):
                raise AssistantError('资料表配置已变化，请刷新后重试。', 409)
            previous = db.execute('SELECT * FROM maintenance WHERE id=?', (operation,)).fetchone()
            if previous:
                if previous['fingerprint'] != fingerprint:
                    raise AssistantError('操作标识已用于其他请求。', 409)
                return json.loads(previous['result'])
            if all_records and not db.execute("SELECT 1 FROM sync_jobs WHERE table_key=? AND config_revision=? AND status='completed'", (key, source['revision'])).fetchone():
                raise AssistantError('首次同步尚未完成，可先选择已显示的记录进行向量化。', 409)
            selected = []
            for row in db.execute("SELECT payload FROM records WHERE table_key=? AND deleted=0 AND pending_operation='' ORDER BY id", (key,)):
                record = json.loads(row[0])
                if record['config_revision'] != source['revision'] or not self._allowed(actor, source, record['scopes']):
                    continue
                if ((all_records and (not scope or scope in record['scopes']) and (not q or q in _json(record['fields']).casefold()))
                        or not all_records and record['record_id'] in ids):
                    selected.append(record)
            if not all_records and {r['record_id'] for r in selected} != set(ids):
                raise AssistantError('部分记录已变化、正在写入或无权访问，请重新选择。', 409)
            count = 0
            for record in selected:
                for source_key, provenance in self._source_entries(source, record):
                    db.execute("INSERT INTO ingestion(source_key,table_key,record_id,record_revision,payload,status,reindex_request) VALUES(?,?,?,?,?,'queued',?) "
                        "ON CONFLICT(source_key) DO UPDATE SET record_revision=excluded.record_revision,payload=excluded.payload,status='queued',stage='prepare',reindex_request=excluded.reindex_request,attempts=0,next_attempt=0,error='',error_code=''",
                        (source_key, key, record['record_id'], record['revision'], _json(provenance), operation))
                    count += 1
            result = {'operation_id': operation, 'status': 'queued', 'records': len(selected), 'items': count}
            db.execute("INSERT INTO maintenance(id,kind,table_key,fingerprint,result,status) VALUES(?,'reindex',?,?,?,'accepted')",
                (operation, key, fingerprint, _json(result)))
        if self.knowledge is not None:
            self.knowledge._wake.set()
        return result

    def remove_table(self, actor, key, payload):
        actor = self._actor(actor)
        if not actor.get('is_admin') or actor.get('read_only'):
            raise AssistantError('仅管理员可移除接入资料表。', 403)
        if set(payload) - {'operation_id', 'expected_revision'}:
            raise AssistantError('移除参数无效。')
        operation, fingerprint = self._maintenance_request(actor, key, payload, 'remove')
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            actor = self._actor(actor)
            if not actor.get('is_admin') or actor.get('read_only'):
                raise AssistantError('管理权限已变化，未移除接入资料表。', 403)
            previous = db.execute('SELECT * FROM maintenance WHERE id=?', (operation,)).fetchone()
            if previous:
                if previous['fingerprint'] != fingerprint:
                    raise AssistantError('操作标识已用于其他请求。', 409)
                return {**json.loads(previous['result']), 'cleanup_status': 'completed' if previous['status'] == 'completed' else 'pending'}
            source = self._table(key, db)
            if source['revision'] != payload.get('expected_revision'):
                raise AssistantError('资料表配置已变化，请刷新后重试。', 409)
            if db.execute("SELECT 1 FROM operations WHERE table_key=? AND status IN ('pending','uncertain')", (key,)).fetchone():
                raise AssistantError('此表仍有待确认的写入，请核验完成后再移除。', 409)
            result = {'operation_id': operation, 'removed': True, 'cleanup_status': 'pending'}
            db.execute("INSERT INTO maintenance(id,kind,table_key,fingerprint,result,status) VALUES(?,'remove',?,?,?,'queued')",
                (operation, key, fingerprint, _json(result)))
            # Removing the authority first immediately excludes this source from every knowledge query.
            db.execute('DELETE FROM tables WHERE id=?', (key,))
            db.execute("UPDATE ingestion SET status='removed',lease='',lease_until=0 WHERE table_key=?", (key,))
            db.execute("UPDATE sync_jobs SET status='superseded',lease='',lease_until=0 WHERE table_key=?", (key,))
            db.execute('DELETE FROM schema_cache WHERE table_key=?', (key,))
        if self.knowledge is not None:
            self.knowledge._wake.set()
        return result

    def _cleanup_removed(self):
        if self.knowledge is None:
            return False
        task = self._claim('maintenance')
        if not task:
            return False
        try:
            result = self.knowledge.remove_sources(task['table_key'] + '/')
            if result.get('skipped'):
                raise AssistantError('部分来源索引尚未清理。', 409)
            with self.connect() as db:
                db.execute('BEGIN IMMEDIATE')
                if db.execute('SELECT 1 FROM tables WHERE id=?', (task['table_key'],)).fetchone():
                    raise AssistantError('来源配置已重新接入，已停止清理。', 409)
                for table in ('records', 'versions', 'ingestion', 'sync_jobs'):
                    db.execute('DELETE FROM ' + table + ' WHERE table_key=?', (task['table_key'],))
                db.execute("UPDATE maintenance SET status='completed',lease='',lease_until=0,error='' WHERE id=? AND lease=?", (task['id'], task['lease']))
        except Exception:
            with self.connect() as db:
                db.execute("UPDATE maintenance SET status='retrying',attempts=attempts+1,next_attempt=?,lease='',lease_until=0,error=? WHERE id=? AND lease=?",
                    (self.clock() + min(300, 5 * 2 ** min(task['attempts'], 6)), '本地索引清理未完成，将自动重试；飞书原记录未改变。', task['id'], task['lease']))
        return True

    def refresh_cache_if_due(self, actor, key):
        source = self._table(key)
        if not any(self._allowed(actor, source, [s]) for s in source['read_scopes']):
            raise AssistantError('当前无权访问此资料表。', 403)
        with self.connect(readonly=True) as db:
            due = db.execute('SELECT next_sync FROM tables WHERE id=?', (key,)).fetchone()
        if due and due[0] <= self.clock():
            return self._enqueue_sync(source)

    def request_sync(self, actor, key):
        source = self._table(key)
        if not any(self._allowed(actor, source, [scope]) for scope in source['read_scopes']):
            raise AssistantError('资料表不存在或当前无权访问。', 404)
        return self._enqueue_sync(source, retry=True)

    def ensure_initial_sync(self, actor, key):
        source = self._table(key)
        if not any(self._allowed(actor, source, [scope]) for scope in source['read_scopes']):
            raise AssistantError('资料表不存在或当前无权访问。', 404)
        return self._enqueue_sync(source, only_initial=True)

    def _enqueue_sync(self, source, *, retry=False, only_initial=False):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if self._table(source['id'], db)['revision'] != source['revision']:
                raise AssistantError('资料表配置已变化，请重新读取。', 409)
            if only_initial and db.execute("SELECT 1 FROM sync_jobs WHERE table_key=? AND config_revision=? AND status='completed' LIMIT 1",
                    (source['id'], source['revision'])).fetchone():
                return None
            row = db.execute("SELECT id,status FROM sync_jobs WHERE table_key=? AND config_revision=? AND status IN ('queued','running','retrying') LIMIT 1", (source['id'], source['revision'])).fetchone()
            if row:
                identity, status = row['id'], row['status']
                if retry and status == 'retrying':
                    db.execute("UPDATE sync_jobs SET status='queued',next_attempt=0 WHERE id=?", (identity,))
                    status = 'queued'
            else:
                identity, status = uuid.uuid4().hex, 'queued'
                db.execute("INSERT INTO sync_jobs(id,table_key,config_revision,status,created_at) VALUES(?,?,?,'queued',?)", (identity, source['id'], source['revision'], self.clock()))
            db.execute('UPDATE tables SET next_sync=? WHERE id=?', (self.clock() + 300, source['id']))
        if self.knowledge is not None:
            self.knowledge._wake.set()
        return {'job_id': identity, 'status': status}

    def sync_status(self, actor, key, identity):
        source = self._table(key)
        if not any(self._allowed(actor, source, [scope]) for scope in source['read_scopes']):
            raise AssistantError('资料表不存在或当前无权访问。', 404)
        with self.connect(readonly=True) as db:
            row = db.execute('SELECT id,status,error,error_code FROM sync_jobs WHERE id=? AND table_key=? AND config_revision=?', (identity, key, source['revision'])).fetchone()
        if not row:
            raise AssistantError('同步任务不存在。', 404)
        # A table-wide source scan never exposes counts from another building.
        return {'job_id': row['id'], 'status': row['status'], 'error': row['error'], 'error_code': row['error_code']}

    def process_sync(self, key):
        # Portal requests can advance the same leased queue without starting the AI/index worker.
        task = self._claim('sync_jobs', table_key=key)
        if task:
            self._scan_page(task)
        return bool(task)

    def prepare_pending(self, *, table_key=None, limit=16):
        worked = False
        for _ in range(limit):
            if self._cleanup_removed():
                worked = True
                continue
            task = self._claim('sync_jobs', table_key=table_key)
            if task:
                self._scan_page(task)
            else:
                task = self._claim('ingestion', table_key=table_key, stage='prepare')
                if task:
                    self._prepare(task)
            if not task:
                break
            worked = True
        return worked

    def process_one(self):
        with self.connect(readonly=True) as db:
            due = db.execute('SELECT id FROM tables WHERE next_sync<=? ORDER BY next_sync LIMIT 1', (self.clock(),)).fetchone()
        if due:
            source = self._table(due[0])
            if source['enabled']:
                self._enqueue_sync(source)
            else:
                with self.connect() as db:
                    db.execute('UPDATE tables SET next_sync=? WHERE id=?', (self.clock() + 300, due[0]))
        if self.prepare_pending(limit=1):
            return True
        if self.knowledge is not None:
            task = self._claim('ingestion', stage='index')
            if task:
                self._ingest(task)
                return True
        return False

    def _claim(self, table, *, table_key=None, stage=None):
        identity_column = 'source_key' if table == 'ingestion' else 'id'
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            condition = ' AND table_key=?' if table_key is not None else ''
            params = (self.clock(), self.clock()) + ((table_key,) if table_key is not None else ())
            if stage is not None:
                condition += ' AND stage=?'
                params += (stage,)
            row = db.execute(f"SELECT * FROM {table} WHERE status IN ('queued','running','retrying') AND next_attempt<=? AND lease_until<=?{condition} ORDER BY next_attempt LIMIT 1", params).fetchone()
            if not row:
                return None
            lease = uuid.uuid4().hex
            db.execute(f"UPDATE {table} SET status='running',lease=?,lease_until=? WHERE {identity_column}=?", (lease, self.clock() + 180, row[identity_column]))
            return {**dict(row), 'lease': lease}

    def _retry(self, table, task, error=None):
        key = 'id' if table == 'sync_jobs' else 'source_key'
        with self.connect() as db:
            if table == 'sync_jobs':
                code, message = self._sync_error(error)
                db.execute('UPDATE sync_jobs SET error=?,error_code=? WHERE id=? AND lease=?',
                    (message, code, task['id'], task['lease']))
            else:
                code, message = self._ingestion_error(error, task['stage'])
                db.execute('UPDATE ingestion SET error=?,error_code=? WHERE source_key=? AND lease=?',
                    (message, code, task['source_key'], task['lease']))
            db.execute(f"UPDATE {table} SET status='retrying',attempts=attempts+1,next_attempt=?,lease='',lease_until=0 WHERE {key}=? AND lease=?",
                (self.clock() + min(300, 5 * 2 ** min(task['attempts'], 6)), task[key], task['lease']))

    @staticmethod
    def _sync_error(error):
        # Never persist raw transport exceptions, request URLs, tokens or provider record values.
        code = error.category if isinstance(error, AssistantError) else ''
        messages = {
            'credential_unavailable': '飞书鉴权尚未就绪，请检查 Qt 飞书鉴权配置后重试同步。',
            'credential_changed': '同步期间飞书鉴权发生变化，请重试同步。',
            'token_unavailable': '未能取得飞书访问令牌，请检查 Qt 飞书鉴权和网络后重试同步。',
            'unauthorized': '飞书拒绝读取来源资料，请检查应用鉴权和此数据表的读取权限后重试。',
            'transport': '读取来源资料时网络连接失败或超时，请检查网络后重试同步。',
            'bad_response': '来源资料返回不完整，未将空列表作为同步成功，请重试同步。',
            'pagination': '来源资料分页信息无效，请重试同步；持续失败时请联系管理员。',
            'document_library_schema': '资料表字段已变化，请重新读取字段并核对资料表配置后同步。',
        }
        if isinstance(error, AssistantError) and error.status in (401, 403):
            code = code if code in messages else 'unauthorized'
        elif isinstance(error, (OSError, TimeoutError)):
            code = 'transport'
        message = messages.get(code, '来源资料同步失败，请重试同步；持续失败时请联系管理员检查数据表配置和后台日志。')
        provider_code = re.search(r'code[=:]\s*(\d{1,12})', str(error)) if isinstance(error, AssistantError) else None
        if provider_code:
            message += '（飞书错误码：' + provider_code[1] + '）'
        return (code if code in messages or code == 'provider_error' else 'sync_error'), message

    def _scan_page(self, task):
        try:
            source = self._table(task['table_key'])
            if not source['enabled'] or source['revision'] != task['config_revision']:
                with self.connect() as db:
                    db.execute("UPDATE sync_jobs SET status='superseded',lease_until=0 WHERE id=? AND lease=?", (task['id'], task['lease']))
                return
            with self.remote(source) as remote:
                fields, page = remote.fields(), remote.records(cursor=task['cursor'], page_size=100)
            self._validate_schema(source, fields)
            self._cache_schema(source, fields)
            if (not isinstance(page, dict) or not isinstance(page.get('items'), list) or type(page.get('has_more')) is not bool
                    or any(not isinstance(row, dict) or not isinstance(row.get('record_id'), str) or not row['record_id']
                        or not isinstance(row.get('fields'), dict) for row in page['items'])):
                raise AssistantError('来源资料记录信息不完整。', 503, category='bad_response')
            seen = json.loads(task['seen_cursors'])
            if len(seen) >= 10000:
                raise AssistantError('来源分页超过安全上限。', 413)
            token = page.get('page_token') or ''
            if page.get('has_more') and (not token or token == task['cursor'] or token in seen):
                raise AssistantError('来源分页信息无效。', 503, category='pagination')
            for raw in page.get('items', []):
                old = self._cached(source['id'], raw['record_id'])
                record = self._normalize(source, raw, fields, json.loads(old['payload']) if old else None)
                self._remember(source, record, generation=task['id'], job=(task['id'], task['lease']))
            if not page.get('has_more'):
                with self.connect(readonly=True) as db:
                    missing = [row[0] for row in db.execute("SELECT id FROM records WHERE table_key=? AND generation<>? AND deleted=0 AND pending_operation='' AND observed_at<=?", (source['id'], task['id'], task['created_at']))]
                for identity in missing:
                    self._retire(source['id'], identity, config_revision=source['revision'], job=(task['id'], task['lease']))
            with self.connect() as db:
                db.execute("UPDATE sync_jobs SET status=?,cursor=?,seen_cursors=?,count=count+?,attempts=0,next_attempt=0,lease='',lease_until=0,error='',error_code='' WHERE id=? AND lease=?",
                    ('queued' if page.get('has_more') else 'completed', token, _json([*seen, task['cursor']]), len(page.get('items', [])), task['id'], task['lease']))
        except Exception as exc:
            self._retry('sync_jobs', task, exc)

    def _ingest(self, task):
        try:
            provenance = json.loads(task['payload'])
            source = self._table(task['table_key'])
            cached = self._cached(task['table_key'], task['record_id'])
            if not self.source_current(provenance):
                with self.connect() as db:
                    db.execute("UPDATE ingestion SET status='removed',lease_until=0 WHERE source_key=? AND lease=?", (task['source_key'], task['lease']))
                return
            public_source = {key: provenance[key] for key in ('table_key', 'record_id', 'record_revision', 'config_revision', 'origin', 'scopes', 'attachment_id', 'field_id', 'kind') if key in provenance}
            if provenance.get('kind') == 'record':
                content = self._record_text(source, json.loads(cached['payload'])).encode('utf-8')
                self.knowledge.upsert_source(task['source_key'], public_source, provenance['name'], content)
            else:
                if not self._cache_size(provenance['file_token']):
                    with self.connect() as db:
                        db.execute("UPDATE ingestion SET stage='prepare',status='queued',cached=0,lease='',lease_until=0 WHERE source_key=? AND lease=?",
                            (task['source_key'], task['lease']))
                    return
                path = self.root / 'files' / hashlib.sha256(provenance['file_token'].encode()).hexdigest()
                with path.open('rb') as content:
                    self.knowledge.upsert_source(task['source_key'], public_source, provenance['name'], content)
            if task.get('reindex_request'):
                self.knowledge.reindex_source(task['source_key'], public_source, task['reindex_request'])
            with self.connect() as db:
                # An in-flight task finishes its lease but must not consume a newer reindex request.
                db.execute("UPDATE ingestion SET status=CASE WHEN reindex_request=? THEN 'synced' ELSE 'queued' END,stage='index',error='',error_code='',lease='',lease_until=0 WHERE source_key=? AND lease=?",
                    (task.get('reindex_request', ''), task['source_key'], task['lease']))
        except Exception as exc:
            self._retry('ingestion', task, exc)

    @staticmethod
    def _ingestion_error(error, stage):
        code = getattr(error, 'category', '')
        messages = {
            'unauthorized': '附件读取权限不足，请检查飞书应用及多维表高级权限。',
            'token_rejected': '飞书拒绝附件下载鉴权，请检查 Qt 飞书鉴权。',
            'rate_limit': '飞书附件下载限流，系统将退避后重试。',
            'unsafe_redirect': '附件下载跳转未通过安全校验，未保存为成功缓存。',
            'too_large': '附件为空或超过100 MiB上限，未缓存或索引。',
            'bad_response': '附件响应不完整或不是原文件，未保存为成功缓存。',
            'local_storage': '本地原件保存失败，请检查磁盘空间和目录权限。',
            'transport': '附件下载连接失败或超时，系统将重试。',
        }
        if code not in messages:
            if isinstance(error, OSError):
                code = 'local_storage'
            elif stage == 'index':
                from .document_library_index import index_error
                return 'index_submit', index_error(str(error) if isinstance(error, AssistantError) else '') or '本地原件已保留，提交知识库未完成，请稍后重试。'
            else:
                code, message = DocumentLibraryService._sync_error(error)
                return code, message
        return code, messages[code]

    def _prepare(self, task):
        try:
            provenance = json.loads(task['payload'])
            if not self.source_current(provenance):
                with self.connect() as db:
                    db.execute("UPDATE ingestion SET status='removed',lease='',lease_until=0 WHERE source_key=? AND lease=?", (task['source_key'], task['lease']))
                return
            if provenance.get('kind') == 'record':
                size = len(self._cached(task['table_key'], task['record_id'])['payload'].encode('utf-8'))
            else:
                source = self._table(task['table_key'])
                path = self._content_path(source, provenance['file_token'], record_id=task['record_id'], field_id=provenance['field_id'])
                size = path.stat().st_size
            if not self.source_current(provenance):
                raise AssistantError('缓存期间来源版本已变化。', 409, category='source_changed')
            from openclaw_service.assistant.lighthouse_knowledge import EXTENSIONS
            supported = Path(provenance['name']).suffix.lower() in EXTENSIONS
            with self.connect() as db:
                db.execute("UPDATE ingestion SET stage='index',status=?,cached=1,cache_size=?,error='',error_code='',attempts=0,next_attempt=0,lease='',lease_until=0 WHERE source_key=? AND lease=?",
                    ('queued' if supported else 'unsupported', size, task['source_key'], task['lease']))
            if self.knowledge is not None:
                self.knowledge._wake.set()
        except Exception as exc:
            self._retry('ingestion', task, exc)

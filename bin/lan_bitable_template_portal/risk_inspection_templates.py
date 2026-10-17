"""Reusable publication definitions, never reconstructed from inspector answers."""
from copy import deepcopy
import json
import re
import time

from .risk_inspection import FIELD_TYPES, SCOPES, SYSTEM_NAMES, RiskError, can_scope, digest, manager, text, validate_values


def clean_fields(fields):
    if not isinstance(fields, list) or not 1 <= len(fields) <= 80:
        raise RiskError('模板需保留1至80个字段。')
    result, keys, names = [], set(), set()
    for f in fields:
        if not isinstance(f, dict):
            raise RiskError('模板字段格式无效。')
        key, name, kind = text(f.get('key'), 100, True), text(f.get('field_name'), 80, True), f.get('type')
        role = f.get('role', 'input')
        if not re.fullmatch(r'[A-Za-z0-9_-]+', key) or key in keys or name in names or name.startswith('灯塔'):
            raise RiskError('模板字段标识或名称重复、无效。')
        if type(kind) is not int or kind not in FIELD_TYPES or not isinstance(role, str) or role not in {'input', *SYSTEM_NAMES.values()}:
            raise RiskError('模板字段类型或填写方式无效。')
        if role != 'input' and kind != (5 if role == 'reviewed_at' else 1):
            raise RiskError('系统字段需使用文本类型，复核日期需使用日期类型。')
        prop = f.get('property') or {}
        if not isinstance(prop, dict):
            raise RiskError('字段属性格式无效。')
        if kind in {3, 4}:
            options = prop.get('options', [])
            if not isinstance(options, list) or len(options) > 100 or any(not isinstance(o, dict) for o in options):
                raise RiskError('字段选项格式无效。')
            options = [text(o.get('name'), 100, True) for o in options]
            if len(set(options)) != len(options):
                raise RiskError('选项不能重复。')
            if f.get('required') and not options:
                raise RiskError(name + '为必填字段，请先配置选项。')
            prop = {'options': [{'name': o} for o in options]}
        elif kind == 5:
            prop = {'auto_fill': False, 'date_formatter': 'yyyy/MM/dd'}
        else:
            prop = {}
        result.append({'key': key, 'field_id': key, 'field_name': name, 'type': kind, 'role': role,
                       'is_primary': f.get('is_primary') is True, 'required': f.get('required') is True and role == 'input', 'property': prop})
        keys.add(key); names.add(name)
    if sum(f['is_primary'] for f in result) != 1 or any(f['is_primary'] and f['type'] != 1 for f in result):
        raise RiskError('必须保留一个文本主字段。')
    return result


def reusable_items(fields, items):
    if not isinstance(items, list) or not 1 <= len(items) <= 100:
        raise RiskError('模板需包含1至100个排查事项。')
    allowed = {f['key'] for f in fields if f['role'] == 'input' and f['type'] not in {5, 17}}
    result = []
    for item in items:
        if not isinstance(item, dict) or not isinstance(item.get('values', {}), dict):
            raise RiskError('排查事项格式无效。')
        values = {k: v for k, v in item.get('values', {}).items() if k in allowed}
        result.append({'title': text(item.get('title'), 300, True), 'values': validate_values(fields, values)})
    return result


class RiskTemplates:
    def __init__(self, store):
        self.store = store
        with store.connect(True) as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS risk_history_templates(id TEXT PRIMARY KEY,name TEXT NOT NULL,updated REAL NOT NULL,data TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS risk_history_catalog(id INTEGER PRIMARY KEY CHECK(id=1),data TEXT NOT NULL);
            ''')

    def allowed(self, actor, data):
        return (manager(actor) and bool(data['scopes']) and all(can_scope(actor, s) for s in data['scopes'])
                and (actor['id'] == data['creator']['id'] or self.store.superior(actor, data['creator']['id'])))

    @staticmethod
    def read(db, tid):
        row = db.execute('SELECT data FROM risk_history_templates WHERE id=?', (tid,)).fetchone()
        if not row:
            raise RiskError('历史排查模板不存在。', 404)
        return json.loads(row[0])

    @staticmethod
    def put(db, data):
        data['version'] = str(data['revision']) + ':' + digest([data['name'], data['enabled'], data['fields'], data['items']])
        db.execute('INSERT OR REPLACE INTO risk_history_templates VALUES(?,?,?,?)',
                   (data['id'], data['name'], data['updated'], json.dumps(data, ensure_ascii=False)))

    def create(self, db, release, people):
        tid = release['id']
        if db.execute('SELECT 1 FROM risk_history_templates WHERE id=?', (tid,)).fetchone():
            return
        legacy = 'original_items' not in release
        if legacy:
            # Older releases have no original prefill: recover titles only, never answers.
            unique = {}
            for person in people:
                for item in person['items']:
                    unique.setdefault(item['id'], {'title': item['title'], 'values': {}})
            items = list(unique.values())
        else:
            items = release['original_items']
        fields = deepcopy(release['fields'])
        data = {'id': tid, 'source_release_id': tid, 'source_name': release['name'],
                'source_url': release.get('cloud', {}).get('url', ''), 'name': release['name'],
                'revision': 1, 'enabled': True, 'fields': fields, 'items': reusable_items(fields, items),
                'scopes': sorted({p['scope'] for p in people}), 'legacy': legacy,
                'created': release['created'], 'updated': time.time(),
                'creator': {k: release['owner'][k] for k in ('id', 'name')},
                'updated_by': release['owner']['id'], 'cloud': {}}
        self.put(db, data)
        self.store.enqueue(db, tid, 'template_sync', {'template_id': tid, 'revision': 1}, 'template:' + tid + ':1')

    def backfill(self):
        with self.store.connect(True) as db:
            rows = db.execute("SELECT r.data FROM risk_releases r WHERE json_extract(r.data,'$.status')='active' AND NOT EXISTS(SELECT 1 FROM risk_history_templates h WHERE h.id=r.id)").fetchall()
            for row in rows:
                release = json.loads(row[0])
                people = [json.loads(r[0]) for r in db.execute('SELECT data FROM risk_people WHERE release_id=?', (release['id'],))]
                self.create(db, release, people)

    def get(self, actor, tid, *, usable=False, kind=None):
        self.store.check(actor)
        with self.store.connect() as db:
            data = self.read(db, tid)
            if kind and data.get('kind', 'history') != kind:
                raise RiskError('模板类型不匹配，请重新选择。', 409)
            if not self.allowed(actor, data):
                raise RiskError('没有该历史模板涉及楼栋的管理权限。', 403)
            if usable and not data['enabled']:
                raise RiskError('该历史模板已停用，请选择其它模板。', 409)
            public = deepcopy(data)
            public['kind'] = data.get('kind', 'history')
            public.pop('updated_by', None)
            public['creator'] = data['creator']['name']
            public['cloud'] = {'synced': data['cloud'].get('revision') == data['revision']}
            job = db.execute('SELECT status,error FROM risk_jobs WHERE id=?', (f'template:{tid}:{data["revision"]}',)).fetchone()
            public['sync_status'] = dict(job) if job else {'status': 'queued', 'error': ''}
            return public

    def list(self, actor, page=1, search='', kind='history'):
        self.store.check(actor)
        if not manager(actor):
            raise RiskError('仅管理员或楼栋主管可使用历史模板。', 403)
        if kind not in {'base', 'history'}:
            raise RiskError('模板类型无效。')
        if kind == 'history':
            self.backfill()
        try:
            page = min(100000, max(1, int(page)))
        except (ValueError, TypeError):
            raise RiskError('页码无效。') from None
        search = text(search, 120)
        scopes = [s for s in SCOPES if can_scope(actor, s)]
        with self.store.connect() as db:
            owners = [actor['id'], *(row[0] for row in db.execute("SELECT DISTINCT json_extract(data,'$.creator.id') FROM risk_history_templates")
                                     if row[0] != actor['id'] and self.store.superior(actor, row[0]))]
            sql = " FROM risk_history_templates h WHERE COALESCE(json_extract(h.data,'$.kind'),'history')=? AND json_extract(h.data,'$.creator.id') IN (" + ','.join('?' for _ in owners) + ") AND json_array_length(h.data,'$.scopes')>0 AND NOT EXISTS(SELECT 1 FROM json_each(h.data,'$.scopes') s WHERE s.value NOT IN (" + ','.join('?' for _ in scopes) + ')) AND instr(h.name,?)>0'
            args = [kind, *owners, *scopes, search]
            total = db.execute('SELECT COUNT(*)' + sql, args).fetchone()[0]
            rows = db.execute('SELECT h.id' + sql + ' ORDER BY h.updated DESC,h.id LIMIT 20 OFFSET ?', args + [(page-1)*20]).fetchall()
        items = []
        for row in rows:
            data = self.get(actor, row[0])
            items.append({**{k: data[k] for k in ('id', 'name', 'revision', 'enabled', 'created', 'updated', 'creator', 'scopes', 'legacy', 'sync_status')},
                          'field_count': len(data['fields']), 'item_count': len(data['items'])})
        return {'items': items, 'page': page, 'total': total}

    def needs_base(self, actor):
        with self.store.connect() as db:
            return not db.execute("SELECT 1 FROM risk_history_templates WHERE json_extract(data,'$.kind')='base' AND json_extract(data,'$.creator.id')=? LIMIT 1",
                                  (actor['id'],)).fetchone()

    def seed_base(self, actor, template):
        return self.add_base(actor, {'id': digest(['risk-base', actor['id']])[:32], 'name': '默认基础模板',
                                    'enabled': True, 'fields': template['fields'], 'items': []})

    def add_base(self, actor, body):
        self.store.check(actor)
        if not manager(actor):
            raise RiskError('仅管理员或楼栋主管可创建基础模板。', 403)
        if not isinstance(body, dict) or set(body) != {'id', 'name', 'enabled', 'fields', 'items'} or type(body['enabled']) is not bool:
            raise RiskError('基础模板参数无效。')
        tid = text(body['id'], 32, True)
        if not re.fullmatch('[a-f0-9]{32}', tid):
            raise RiskError('基础模板标识无效。')
        fields = clean_fields(body['fields'])
        items = reusable_items(fields, body['items']) if body['items'] != [] else []
        data = {'id': tid, 'kind': 'base', 'source_release_id': '', 'source_name': '', 'source_url': '',
                'name': text(body['name'], 120, True), 'revision': 1, 'enabled': body['enabled'], 'fields': fields,
                'items': items, 'scopes': [s for s in SCOPES if can_scope(actor, s)], 'legacy': False,
                'created': time.time(), 'updated': time.time(), 'creator': {'id': actor['id'], 'name': actor['name']},
                'updated_by': actor['id'], 'request_hash': digest(body), 'cloud': {}}
        if not data['scopes']:
            raise RiskError('没有可用于基础模板的楼栋权限。', 403)
        with self.store.connect(True) as db:
            if db.execute('SELECT 1 FROM risk_history_templates WHERE id=?', (tid,)).fetchone():
                existing = self.read(db, tid)
                if existing.get('kind') != 'base' or existing['creator']['id'] != actor['id'] or existing.get('request_hash') != digest(body):
                    raise RiskError('模板创建标识冲突，请重新读取原模板。', 409)
            else:
                self.put(db, data)
                self.store._audit(db, tid, '', actor, '创建基础模板', {'name': data['name']})
                self.store.enqueue(db, tid, 'template_sync', {'template_id': tid, 'revision': 1}, 'template:' + tid + ':1')
        return self.get(actor, tid, kind='base')

    def update(self, actor, tid, body):
        self.store.check(actor)
        if not isinstance(body, dict) or set(body) != {'version', 'name', 'enabled', 'fields', 'items'} or type(body['enabled']) is not bool:
            raise RiskError('历史模板保存参数无效。')
        fields = clean_fields(body['fields'])
        with self.store.connect(True) as db:
            data = self.read(db, tid)
            items = [] if data.get('kind') == 'base' and body['items'] == [] else reusable_items(fields, body['items'])
            if not self.allowed(actor, data):
                raise RiskError('无权修改该模板。', 403)
            if data['version'] != body['version']:
                raise RiskError('历史模板已被修改，请重新读取后核对。', 409)
            data.update(name=text(body['name'], 120, True), enabled=body['enabled'], fields=fields, items=items,
                        revision=data['revision']+1, updated=time.time(), updated_by=actor['id'])
            self.put(db, data)
            self.store._audit(db, tid, '', actor, '修改历史模板', {'revision': data['revision'], 'enabled': data['enabled'], 'fields': fields, 'items': items})
            self.store.enqueue(db, tid, 'template_sync', {'template_id': tid, 'revision': data['revision']}, f'template:{tid}:{data["revision"]}')
        return self.get(actor, tid)

    def retry(self, actor, tid):
        self.get(actor, tid)
        with self.store.connect(True) as db:
            db.execute("UPDATE risk_jobs SET status='queued',next_at=0,attempts=0 WHERE id=? AND status='failed' AND kind='template_sync'",
                       (f'template:{tid}:{self.read(db, tid)["revision"]}',))
        return self.get(actor, tid)

    def raw(self, tid):
        with self.store.connect() as db:
            return self.read(db, tid)

    def checkpoint(self, tid, **values):
        with self.store.connect(True) as db:
            data = self.read(db, tid)
            data['cloud'].update(values)
            self.put(db, data)

    def catalog(self, **values):
        with self.store.connect(bool(values)) as db:
            row = db.execute('SELECT data FROM risk_history_catalog WHERE id=1').fetchone()
            data = json.loads(row[0]) if row else {}
            if values:
                data.update(values)
                db.execute('INSERT OR REPLACE INTO risk_history_catalog VALUES(1,?)', (json.dumps(data),))
            return data

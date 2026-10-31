"""Personal inspection assignments, immutable release schemas and durable cloud work."""
from contextlib import contextmanager
from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import time
import uuid

MODULE = 'risk_inspection'
SCOPES = ('110', 'A', 'B', 'C', 'D', 'E', 'H')
FIELD_TYPES = {1, 2, 3, 4, 5, 7, 13, 15, 17}
SYSTEM_NAMES = {'排查任务编号': 'number', '排查进度': 'status', '楼栋': 'scope',
                '排查人': 'person', '复核人': 'reviewer', '复核结果': 'review',
                '复核日期': 'reviewed_at', '最终推送': 'sync', '排查日期': 'inspected_at'}
STATUS = {'pending': '未提交', 'submitted': '待审核', 'approved': '审核通过', 'returned': '退回修改', 'revoked': '已撤销'}
REMINDER_SECONDS = 2 * 60 * 60


class RiskError(Exception):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


def text(value, size=500, required=False):
    if not isinstance(value, str) or len(value) > size or '\x00' in value:
        raise RiskError('文字内容格式或长度不正确。')
    value = value.strip()
    if required and not value:
        raise RiskError('请补齐必填内容。')
    return value


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def can_scope(actor, scope):
    return MODULE in actor.get('modules', []) and scope in SCOPES and bool({'ALL', scope} & set(actor.get('scope_rules', [])))


def manager(actor):
    return MODULE in actor.get('modules', []) and actor.get('level') in {'L0', 'L1', 'L3'}


def final_manager(actor):
    return manager(actor) and actor.get('category') == 'manager'


def reviewer_list(release):
    return release.get('reviewers') or ([release['reviewer']] if release.get('reviewer') else [])


def person_scopes(person):
    return list(dict.fromkeys(person.get('scopes') or [person['scope']]))


def item_scope(person, item):
    return item.get('scope') or person['scope']


def outstanding_items(person):
    return [i for i in person['items'] if not person['revoked'] and i['status'] in {'pending', 'returned'}]


def scoped_people(people):
    """Expand only for presentation; the persisted person and item IDs never change."""
    return [{**p, 'scope': scope, 'scopes': [scope],
             'items': [i for i in p['items'] if item_scope(p, i) == scope]}
            for p in people for scope in person_scopes(p)]


def system_fields(fields):
    """Keep historical keys, but never let template settings make reserved fields editable."""
    result = deepcopy(fields)
    for field in result:
        field['role'] = SYSTEM_NAMES.get(field['field_name'], field.get('role', 'input'))
        if not isinstance(field['role'], str) or field['role'] not in {'input', *SYSTEM_NAMES.values()}:
            raise RiskError('模板字段填写方式无效。')
        if field['role'] != 'input':
            field['required'] = False
    roles = {f['role'] for f in result}
    keys = {f['key'] for f in result}
    for name, role in SYSTEM_NAMES.items():
        if role in roles:
            continue
        key = 'system_' + role
        while key in keys:
            key += '_'
        kind = 5 if role in {'reviewed_at', 'inspected_at'} else 1
        result.append({'key': key, 'field_id': key, 'field_name': name, 'type': kind,
                       'role': role, 'required': False, 'is_primary': False,
                       'property': {'auto_fill': False, 'date_formatter': 'yyyy/MM/dd HH:mm'} if kind == 5 else {}})
        keys.add(key)
    return result


def completion_key(people):
    items = [(p['id'], i['id'], i.get('reviewed_at'), i.get('scope', '')) for p in people if not p['revoked']
             for i in p['items'] if i['status'] != 'revoked']
    if not items or any(i['status'] != 'approved' for p in people if not p['revoked']
                        for i in p['items'] if i['status'] != 'revoked'):
        return ''
    # Keep already queued legacy completion keys valid.
    if not any(i[3] for i in items):
        items = [i[:3] for i in items]
    return digest(sorted(items))[:24]


def system_values(release, person, item):
    scope = item_scope(person, item)
    active = [i for i in person['items'] if i['status'] != 'revoked' and item_scope(person, i) == scope]
    submitted = sum(i['status'] in {'submitted', 'approved'} for i in active)
    progress = f'{round(100 * submitted / len(active)) if active else 0}%（{submitted}/{len(active)}项已反馈）'
    targets = [p for p in release.get('final_managers', []) if scope in p['scopes']]
    final = release['cloud'].get('final_push', {})
    if 'scope_versions' in final:
        version = digest(sorted((scope, i['id'], i.get('reviewed_at')) for i in active))
        saved = final['scope_versions'].get(person['id'] + ':' + scope)
    else:
        version = digest(sorted((i['id'], i.get('reviewed_at')) for i in active))
        saved = final.get('person_versions', {}).get(person['id']) if len(person_scopes(person)) == 1 else None
    current = bool(active) and all(i['status'] == 'approved' for i in active) and saved == version
    sent = [p['name'] for p in targets if current and p['id'] in final.get('delivered', [])]
    push = ('已推送：' + '、'.join(sent) if targets and len(sent) == len(targets)
            else '部分已推送，等待重试' if sent else '待全部复核通过后推送' if targets else '未配置接收经理')
    return {'number': release['id'] + ':' + person['id'] + ':' + item['id'],
            'status': progress, 'scope': scope + ('站' if scope == '110' else '楼'),
            'person': person['name'], 'reviewer': item.get('reviewed_by') or '、'.join(p['name'] for p in reviewer_list(release)),
            'review': (STATUS[item['status']] + ('：' + item['reason'] if item.get('reason') else '')) if item.get('reviewed_at') else '',
            'reviewed_at': int(item.get('reviewed_at', 0) * 1000) or None,
            'inspected_at': int(item.get('submitted_at', 0) * 1000) or None, 'sync': push}


def schema_fields(fields, settings=None):
    settings = settings or {}
    result = []
    for source in fields:
        field = {k: deepcopy(source[k]) for k in ('field_id', 'field_name', 'type', 'property', 'is_primary') if k in source}
        field['key'] = field['field_id']
        options = settings.get(field['key'], {})
        field['role'] = SYSTEM_NAMES.get(field['field_name']) or options.get('role') or 'input'
        field['required'] = field['role'] == 'input' and bool(options.get('required', field['field_name'] == '排查结果'))
        if field['role'] == 'input' and field['type'] not in FIELD_TYPES:
            field['unsupported'] = True
        result.append(field)
    if not result or len(result) > 80:
        raise RiskError('模板字段数量必须在1至80之间。')
    return result


def validate_values(fields, values, *, complete=False, file_check=None):
    if not isinstance(values, dict) or set(values) - {f['key'] for f in fields if f['role'] == 'input'}:
        raise RiskError('存在不属于本次模板的填写字段。')
    clean = {}
    for f in fields:
        if f['role'] != 'input':
            continue
        key, kind = f['key'], f['type']
        v = values.get(key)
        empty = v is None or v == [] or isinstance(v, str) and not v.strip()
        if complete and f.get('required') and empty:
            raise RiskError('请填写：' + f['field_name'])
        if empty:
            continue
        if kind in {1, 13}:
            v = text(v, 10000)
        elif kind == 15:
            v = text(v, 2000)
            if not re.match(r'^https?://[^\s]+$', v):
                raise RiskError(f['field_name'] + '需填写完整的HTTP或HTTPS链接。')
        elif kind in {2, 5}:
            if type(v) not in {int, float} or not math.isfinite(v):
                raise RiskError(f['field_name'] + '格式不正确。')
            if kind == 5 and not 0 <= v <= 32503680000000:
                raise RiskError('日期超出有效范围。')
        elif kind == 7:
            if type(v) is not bool:
                raise RiskError('勾选值无效。')
        elif kind in {3, 4}:
            available = {o['name'] for o in (f.get('property') or {}).get('options', [])}
            options = [v] if kind == 3 else v
            if not isinstance(options, list) or len(options) > 100 or any(not isinstance(x, str) or x not in available for x in options):
                raise RiskError(f['field_name'] + '含有模板以外的选项。')
            v = v if kind == 3 else list(dict.fromkeys(v))
        elif kind == 17:
            if not isinstance(v, list) or len(v) > 20 or any(not isinstance(x, str) or not re.fullmatch('[a-f0-9]{32}', x) for x in v):
                raise RiskError('附件列表无效。')
            v = list(dict.fromkeys(v))
            if not file_check or any(not file_check(key, fid) for fid in v):
                raise RiskError('附件不属于当前人员的本项排查，请重新上传。', 403)
        else:
            raise RiskError('当前字段类型暂不能填写：' + f['field_name'])
        clean[key] = v
    return clean


class RiskInspections:
    def __init__(self, db_path, person_for):
        self.db_path = Path(db_path)
        self.person_for = person_for
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect(True) as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS risk_releases(id TEXT PRIMARY KEY,owner TEXT NOT NULL,created REAL NOT NULL,data TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS risk_release_owner ON risk_releases(owner);
                CREATE TABLE IF NOT EXISTS risk_people(release_id TEXT NOT NULL,person_id TEXT NOT NULL,scope TEXT NOT NULL,data TEXT NOT NULL,PRIMARY KEY(release_id,person_id));
                CREATE INDEX IF NOT EXISTS risk_people_person ON risk_people(person_id,release_id);
                CREATE INDEX IF NOT EXISTS risk_people_scope ON risk_people(scope,release_id);
                CREATE TABLE IF NOT EXISTS risk_jobs(id TEXT PRIMARY KEY,release_id TEXT NOT NULL,kind TEXT NOT NULL,payload TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'queued',attempts INTEGER NOT NULL DEFAULT 0,next_at REAL NOT NULL DEFAULT 0,lease REAL NOT NULL DEFAULT 0,error TEXT NOT NULL DEFAULT '',created REAL NOT NULL);
                CREATE INDEX IF NOT EXISTS risk_jobs_ready ON risk_jobs(status,next_at,created);
                CREATE INDEX IF NOT EXISTS risk_jobs_release ON risk_jobs(release_id,status);
                CREATE TABLE IF NOT EXISTS risk_audit(id INTEGER PRIMARY KEY,release_id TEXT NOT NULL,person_id TEXT NOT NULL,actor TEXT NOT NULL,action TEXT NOT NULL,at REAL NOT NULL,data TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS risk_audit_person ON risk_audit(release_id,person_id,id);
                CREATE TABLE IF NOT EXISTS risk_files(id TEXT PRIMARY KEY,release_id TEXT NOT NULL,person_id TEXT NOT NULL,item_id TEXT NOT NULL,field_id TEXT NOT NULL,name TEXT NOT NULL,size INTEGER NOT NULL,created REAL NOT NULL);
                CREATE INDEX IF NOT EXISTS risk_files_person ON risk_files(release_id,person_id,item_id);
                CREATE TABLE IF NOT EXISTS risk_template(id INTEGER PRIMARY KEY CHECK(id=1),data TEXT NOT NULL);
            ''')
        from .risk_inspection_templates import RiskTemplates
        self.history = RiskTemplates(self)

    @contextmanager
    def connect(self, write=False):
        db = sqlite3.connect(self.db_path, timeout=20)
        db.row_factory = sqlite3.Row
        try:
            if write:
                db.execute('BEGIN IMMEDIATE')
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def check(actor):
        if (not actor.get('id') or not actor.get('open_id') or actor.get('role') in {'guest', 'temporary'}
                or actor.get('explicit_disabled') or actor.get('inactive')):
            raise RiskError('当前账号没有风险排查权限。', 403)

    def template(self):
        with self.connect() as db:
            row = db.execute('SELECT data FROM risk_template WHERE id=1').fetchone()
            return json.loads(row[0]) if row else None

    def save_template(self, data):
        with self.connect(True) as db:
            db.execute('INSERT OR REPLACE INTO risk_template(id,data) VALUES(1,?)', (json.dumps(data, ensure_ascii=False),))

    @staticmethod
    def _release(db, rid, *, include_deleted=False):
        row = db.execute('SELECT data FROM risk_releases WHERE id=?', (rid,)).fetchone()
        if not row:
            raise RiskError('排查任务不存在。', 404)
        release = json.loads(row[0])
        if release.get('status') == 'deleted' and not include_deleted:
            raise RiskError('排查任务已删除，不能继续填写或操作。', 410)
        return release

    @staticmethod
    def _person(db, rid, pid):
        row = db.execute('SELECT data FROM risk_people WHERE release_id=? AND person_id=?', (rid, pid)).fetchone()
        if not row:
            raise RiskError('排查人员不存在。', 404)
        return json.loads(row[0])

    @staticmethod
    def _put_person(db, rid, data):
        db.execute('INSERT OR REPLACE INTO risk_people VALUES(?,?,?,?)', (rid, data['id'], data['scope'], json.dumps(data, ensure_ascii=False)))

    @staticmethod
    def _put_release(db, data):
        db.execute('INSERT OR REPLACE INTO risk_releases VALUES(?,?,?,?)', (data['id'], data['owner']['id'], data['created'], json.dumps(data, ensure_ascii=False)))

    @staticmethod
    def _audit(db, rid, pid, actor, action, data):
        db.execute('INSERT INTO risk_audit(release_id,person_id,actor,action,at,data) VALUES(?,?,?,?,?,?)',
                   (rid, pid, actor['id'], action, time.time(), json.dumps({'name': actor['name'], **data}, ensure_ascii=False)))

    @staticmethod
    def enqueue(db, rid, kind, payload, key):
        db.execute('INSERT OR IGNORE INTO risk_jobs(id,release_id,kind,payload,created) VALUES(?,?,?,?,?)',
                   (key, rid, kind, json.dumps(payload, ensure_ascii=False), time.time()))

    @staticmethod
    def brief(actor):
        return {k: actor.get(k, '') for k in ('id', 'name', 'open_id')}

    def _reviewers(self, body, default=''):
        ids = body.get('reviewer_ids', [body.get('reviewer_id') or default])
        if (not isinstance(ids, list) or not 1 <= len(ids) <= 500
                or any(not isinstance(pid, str) or not pid.strip() for pid in ids)
                or len(set(ids)) != len(ids)):
            raise RiskError('请选择有效且不重复的审核人。')
        result = []
        for pid in ids:
            person = self.person_for(text(pid, 100, True))
            self.check(person)
            result.append(self.brief(person))
        if len({p['id'] for p in result}) != len(result):
            raise RiskError('审核人不能重复。')
        return result

    def publish(self, actor, body, template):
        self.check(actor)
        if not manager(actor):
            raise RiskError('仅管理员或授权楼栋主管可发布排查。', 403)
        if not isinstance(body, dict) or set(body) - {'id', 'name', 'deadline', 'reviewer_id', 'reviewer_ids', 'manager_ids', 'people', 'items', 'template_version', 'history_template_id', 'base_template_id'}:
            raise RiskError('发布参数无效。')
        rid = text(body.get('id'), 32, True)
        if not re.fullmatch('[a-f0-9]{32}', rid):
            raise RiskError('发布标识无效。')
        with self.connect() as db:
            row = db.execute('SELECT owner,data FROM risk_releases WHERE id=?', (rid,)).fetchone()
            if row:
                if row['owner'] != actor['id']:
                    raise RiskError('发布标识冲突。', 409)
                if json.loads(row['data'])['request_hash'] != digest(body):
                    raise RiskError('这次发布已受理，不能用原标识更改内容。', 409)
                return self.get(actor, rid)
        history_id, base_id = body.get('history_template_id'), body.get('base_template_id')
        if history_id and base_id:
            raise RiskError('每次发布只能选择一个模板来源。')
        template_id = history_id or base_id
        if template_id:
            template = self.history.get(actor, text(template_id, 32, True), usable=True,
                                        kind='base' if base_id else 'history')
        if not template or body.get('template_version') != template['version']:
            raise RiskError('模板已变化，请重新读取后发布。', 409)
        if any(f.get('unsupported') for f in template['fields']):
            raise RiskError('模板包含不支持独立复制的关联或公式字段，请先调整模板。')
        from .risk_inspection_templates import clean_fields
        fields = clean_fields(template['fields'])
        recipients = body.get('people')
        items = body.get('items')
        if not isinstance(recipients, list) or not 1 <= len(recipients) <= 500 or not isinstance(items, list) or not 1 <= len(items) <= 100:
            raise RiskError('请选择1至500名人员，并设置1至100项排查事项。')
        if any(not isinstance(p, dict) or set(p) - {'id', 'scope', 'scopes'} or not isinstance(p.get('id'), str)
               or not ('scope' in p or 'scopes' in p) for p in recipients) or len({p['id'] for p in recipients}) != len(recipients):
            raise RiskError('人员不能重复分配。')
        reviewers = self._reviewers(body, default=actor['id'])
        deadline = body.get('deadline')
        if type(deadline) not in {int, float} or not math.isfinite(deadline) or not time.time() * 1000 < deadline <= 32503680000000:
            raise RiskError('截止时间需晚于当前时间。')
        base_items = []
        for item in items:
            if not isinstance(item, dict) or set(item) - {'title', 'values'}:
                raise RiskError('排查事项格式无效。')
            base_items.append({'id': uuid.uuid4().hex, 'title': text(item.get('title'), 300, True),
                               'values': validate_values(fields, item.get('values', {})), 'status': 'pending', 'reason': ''})
        people = []
        for selected in recipients:
            person = self.person_for(text(selected['id'], 100, True))
            self.check(person)
            if any(p['id'] == person['id'] for p in people):
                raise RiskError('人员不能重复分配。')
            scopes = selected.get('scopes', [selected.get('scope')])
            if (not isinstance(scopes, list) or not scopes or len(scopes) > len(SCOPES)
                    or any(not isinstance(s, str) or s not in SCOPES for s in scopes)
                    or len(set(scopes)) != len(scopes)):
                raise RiskError('请选择有效且不重复的接收楼栋。')
            if not all(can_scope(actor, s) for s in scopes):
                raise RiskError('发布人需有全部接收楼栋的风险排查权限。', 403)
            people.append({**self.brief(person), 'scope': scopes[0], 'scopes': scopes, 'version': 1, 'revoked': False,
                           'items': [{**deepcopy(i), 'id': uuid.uuid4().hex, 'scope': s}
                                     for s in scopes for i in base_items], 'updated': time.time()})
        manager_ids = body.get('manager_ids', [])
        if not isinstance(manager_ids, list) or len(manager_ids) > 50 or any(not isinstance(pid, str) for pid in manager_ids) or len(set(manager_ids)) != len(manager_ids):
            raise RiskError('接收经理列表无效。')
        if 'manager_ids' in body and not manager_ids:
            raise RiskError('请选择最终汇总的接收经理。')
        final_managers = []
        for pid in manager_ids:
            target = self.person_for(text(pid, 100, True))
            self.check(target)
            scopes = sorted({item_scope(p, i) for p in people for i in p['items'] if can_scope(target, item_scope(p, i))}, key=SCOPES.index)
            if not final_manager(target) or not scopes:
                raise RiskError('接收经理需有对应楼栋的风险排查权限。', 403)
            final_managers.append({**self.brief(target), 'scopes': scopes})
        if manager_ids and any(not any(item_scope(p, i) in m['scopes'] for m in final_managers) for p in people for i in p['items']):
            raise RiskError('请为每个接收楼栋选择至少一名有权限的经理。')
        data = {'id': rid, 'name': text(body.get('name'), 120, True), 'deadline': deadline,
                'owner': self.brief(actor), 'reviewer': reviewers[0], 'reviewers': reviewers,
                'task_grants_version': 2, 'created': time.time(),
                'status': 'publishing', 'fields': fields, 'template_version': template['version'],
                'original_items': [{'title': i['title'], 'values': deepcopy(i['values'])} for i in base_items],
                'source_template': {'id': template_id, 'kind': 'base' if base_id else 'history',
                                    'revision': template['revision'], 'name': template['name']} if template_id else None,
                'final_managers': final_managers, 'cloud': {}, 'request_hash': digest(body)}
        with self.connect(True) as db:
            if template_id:
                current = self.history.read(db, template_id)
                if not current['enabled'] or current['version'] != body['template_version'] or not self.history.allowed(actor, current):
                    raise RiskError('来源历史模板已变化，请重新读取后发布。', 409)
            row = db.execute('SELECT data FROM risk_releases WHERE id=?', (rid,)).fetchone()
            if row:
                existing = json.loads(row[0])
                if existing['owner']['id'] == actor['id'] and existing['request_hash'] == digest(body):
                    return self.get(actor, rid)
                raise RiskError('发布标识冲突，请刷新查看原任务。', 409)
            self._put_release(db, data)
            for person in people:
                self._put_person(db, rid, person)
            self.enqueue(db, rid, 'publish', {}, 'publish:' + rid)
            self._audit(db, rid, '', actor, '发布', {'people': [p['id'] for p in people]})
        return self.get(actor, rid)

    def superior(self, actor, identity):
        if not manager(actor):
            return False
        try:
            target = self.person_for(identity)
        except (RiskError, KeyError):
            return False
        from .account_policy import level_rank, MANAGED_CATEGORIES
        return (target.get('level') in {'L0', 'L1', 'L2', 'L3', 'L4'}
                and level_rank(actor.get('level')) < level_rank(target.get('level'))
                and (actor['level'] in {'L0', 'L1'} or actor['level'] == 'L3'
                     and target.get('level') == 'L4' and target.get('category') in MANAGED_CATEGORIES))

    def controls(self, actor, release):
        return manager(actor) and (actor['id'] == release['owner']['id']
                                   or release.get('task_grants_version') != 2 and any(p['id'] == actor['id'] for p in reviewer_list(release))
                                   or self.superior(actor, release['owner']['id']))

    def view_scopes(self, actor, release, person):
        try:
            self.check(actor)
        except RiskError:
            return []
        if release.get('status') == 'deleted':
            return []
        scopes = list(dict.fromkeys(item_scope(person, i) for i in person['items']))
        if actor['id'] == person['id'] or any(p['id'] == actor['id'] for p in reviewer_list(release)):
            return scopes
        return [s for s in scopes if can_scope(actor, s)] if self.controls(actor, release) else []

    def _visible(self, actor, release, person, item=None):
        scopes = self.view_scopes(actor, release, person)
        return item_scope(person, item) in scopes if item is not None else bool(scopes)

    def has_task_access(self, actor, rid=''):
        try:
            self.check(actor)
        except RiskError:
            return False
        with self.connect() as db:
            rows = db.execute("SELECT id,data FROM risk_releases WHERE json_extract(data,'$.status')='active'"
                              + (' AND id=?' if rid else ''), (rid,) if rid else ())
            for row in rows:
                release = json.loads(row['data'])
                if any(p['id'] == actor['id'] for p in reviewer_list(release)):
                    return True
                person = db.execute('SELECT data FROM risk_people WHERE release_id=? AND person_id=?', (row['id'], actor['id'])).fetchone()
                if person:
                    person = json.loads(person[0])
                    if not person['revoked'] and any(i['status'] != 'revoked' for i in person['items']):
                        return True
        return False

    def get(self, actor, rid):
        self.check(actor)
        with self.connect() as db:
            release = self._release(db, rid)
            people = [json.loads(r[0]) for r in db.execute('SELECT data FROM risk_people WHERE release_id=? ORDER BY scope,person_id', (rid,))]
            controls = self.controls(actor, release)
            visible = []
            for person in people:
                scopes = self.view_scopes(actor, release, person)
                items = [i for i in person['items'] if item_scope(person, i) in scopes]
                if items:
                    visible.append({**person, 'items': items, 'scopes': scopes, 'scope': scopes[0]})
            if not visible:
                raise RiskError('没有查看本次排查的权限。', 403)
            full_control = controls and all(can_scope(actor, item_scope(p, i)) for p in people for i in p['items'])
            is_reviewer = any(p['id'] == actor['id'] for p in reviewer_list(release))
            public = {k: deepcopy(release[k]) for k in ('id', 'name', 'created', 'deadline', 'status', 'fields')}
            public['fields'] = system_fields(public['fields'])
            input_keys = {f['key'] for f in public['fields'] if f['role'] == 'input'}
            public['final_managers'] = [{k: p[k] for k in ('id', 'name')} for p in release.get('final_managers', [])
                                        if any(s in p['scopes'] for v in visible for s in person_scopes(v))]
            public.update({'owner': release['owner']['name'], 'reviewer': reviewer_list(release)[0]['name'], 'people': visible,
                           'reviewers': [{k: p[k] for k in ('id', 'name')} for p in reviewer_list(release)],
                           'reviewer_ids': [p['id'] for p in reviewer_list(release)],
                           'url': release['cloud'].get('url', '') if full_control else '',
                           'is_manager': controls, 'is_reviewer': is_reviewer, 'self_id': actor['id']})
            public['can_claim'] = (controls and actor['id'] == release['owner']['id']
                                   and release['status'] == 'active' and all(p['id'] != actor['id'] for p in people))
            public['claim_scopes'] = sorted({s for p in visible for s in person_scopes(p) if can_scope(actor, s)}, key=SCOPES.index) if public['can_claim'] else []
            public['reviewer_id'] = reviewer_list(release)[0]['id']
            public['reviewer_version'] = release.get('reviewer_version', 1)
            visible_ids = {p['id'] for p in visible}
            placeholders = ','.join('?' for _ in visible_ids)
            files = {}
            for row in db.execute('SELECT id,name,size,field_id,person_id,item_id FROM risk_files WHERE release_id=? AND person_id IN (' + placeholders + ')', [rid, *visible_ids]):
                files.setdefault((row['person_id'], row['item_id']), []).append({k: row[k] for k in ('id', 'name', 'size', 'field_id')})
            for p in public['people']:
                p.pop('open_id', None)
                p.pop('_reminder', None)
                for i in p['items']:
                    i['files'] = files.get((p['id'], i['id']), [])
                    automatic = system_values(release, p, i)
                    i['system_values'] = {f['key']: automatic[f['role']] for f in public['fields'] if f['role'] != 'input'}
                    i['values'] = {k: v for k, v in i['values'].items() if k in input_keys}
            jobs = db.execute("SELECT id,kind,status,error,json_extract(payload,'$.item_id') AS item_id,COALESCE(json_extract(payload,'$.subject_id'),json_extract(payload,'$.person_id')) AS subject FROM risk_jobs WHERE release_id=? AND status!='done' AND NOT(kind='reminder' AND status='cancelled')", (rid,))
            public['can_manage_release'] = full_control
            public['can_delete_release'] = public['can_manage_release']
            public['jobs'] = []
            for job in jobs:
                subject = next((p for p in visible if p['id'] == job['subject']), None)
                whole_subject = subject and len(subject['items']) == len(next(p for p in people if p['id'] == subject['id'])['items'])
                if (full_control or subject and (whole_subject or job['item_id'] in {i['id'] for i in subject['items']})
                        and (manager(actor) or is_reviewer or job['kind'] != 'review_notice')):
                    public['jobs'].append({**{k: job[k] for k in ('id', 'kind', 'status', 'error')},
                                           'kind': 'notify' if job['kind'] == 'reminder' else 'review_notice' if job['kind'] == 'review_reminder' else job['kind']})
            audit_ids = [*visible_ids, *([''] if public['can_manage_release'] else [])]
            public['audit'] = []
            visible_items = {p['id']: {i['id'] for i in p['items']} for p in visible}
            for row in db.execute('SELECT person_id,action,at,data FROM risk_audit WHERE release_id=? AND person_id IN (' + ','.join('?' for _ in audit_ids) + ') ORDER BY id DESC', [rid, *audit_ids]):
                audit = dict(row)
                data = json.loads(audit['data'])
                if audit['person_id']:
                    allowed = visible_items[audit['person_id']]
                    if data.get('item_id') and data['item_id'] not in allowed:
                        continue
                    if not data.get('item_id') and len(allowed) != len(next(p for p in people if p['id'] == audit['person_id'])['items']):
                        continue
                    if 'items' in data:
                        data['items'] = [i for i in data['items'] if i['id'] in allowed]
                audit['data'] = json.dumps(data, ensure_ascii=False)
                public['audit'].append(audit)
                if len(public['audit']) == 100:
                    break
            return public

    def list(self, actor, page=1, scope='', view='all', q='', state=''):
        self.check(actor)
        try:
            page = max(1, int(page))
        except (TypeError, ValueError):
            raise RiskError('页码无效。') from None
        if view not in {'all', 'published', 'mine', 'review'}:
            raise RiskError('排查列表范围无效。')
        if state not in {'', 'active', 'review', 'overdue', 'completed', 'publishing'}:
            raise RiskError('排查任务状态无效。')
        query = str(q or '').strip().casefold()
        filtering = bool(query or state)
        now = time.time() * 1000
        if view == 'published' and not manager(actor):
            view = 'mine'
        if scope and scope != 'ALL' and scope not in SCOPES:
            raise RiskError('没有该楼栋风险排查的查看权限。', 403)
        result = []
        with self.connect() as db:
            owners = [actor['id']]
            if manager(actor) and view in {'all', 'published'}:
                owners += [row[0] for row in db.execute('SELECT DISTINCT owner FROM risk_releases')
                           if row[0] != actor['id'] and self.superior(actor, row[0])]
            scopes = [s for s in SCOPES if can_scope(actor, s)]
            item_scope_sql = "COALESCE(json_extract(i.value,'$.scope'),p.scope)"
            scope_sql = item_scope_sql + ' IN (' + ','.join('?' for _ in scopes) + ')'
            control_sql = 'r.owner IN (' + ','.join('?' for _ in owners) + ')' if manager(actor) else '0'
            control_args = owners if manager(actor) else []
            reviewer_sql = "EXISTS(SELECT 1 FROM json_each(COALESCE(json_extract(r.data,'$.reviewers'),json_array(json_extract(r.data,'$.reviewer')))) rv WHERE json_extract(rv.value,'$.id')=?)"
            grants = '(p.person_id=? OR ' + reviewer_sql + ')'
            grant_args = [actor['id'], actor['id']]
            managed = '(' + scope_sql + ' AND ' + control_sql + ')'
            if view == 'mine':
                visible, args = 'p.person_id=?', [actor['id']]
                candidate, candidate_args = visible, args
            elif view == 'review':
                visible, args = reviewer_sql, [actor['id']]
                candidate, candidate_args = visible, args
            elif view == 'published':
                visible, args = managed, [*scopes, *control_args]
                candidate, candidate_args = visible, args
            else:
                visible, args = '(' + grants + ' OR ' + managed + ')', [*grant_args, *scopes, *control_args]
                candidate, candidate_args = visible, args
            if scope and scope != 'ALL':
                authorized = db.execute("SELECT 1 FROM risk_people p JOIN risk_releases r ON r.id=p.release_id,json_each(p.data,'$.items') i WHERE json_extract(r.data,'$.status')!='deleted' AND "
                                        + grants + ' AND ' + item_scope_sql + '=? LIMIT 1', [*grant_args, scope]).fetchone()
                if not can_scope(actor, scope) and not authorized:
                    raise RiskError('没有该楼栋风险排查的查看权限。', 403)
                visible += ' AND ' + item_scope_sql + '=?'
                candidate += ' AND ' + item_scope_sql + '=?'
                args = [*args, scope]
                candidate_args = [*candidate_args, scope]
            sql = " FROM risk_releases r WHERE json_extract(r.data,'$.status')!='deleted' AND EXISTS(SELECT 1 FROM risk_people p,json_each(p.data,'$.items') i WHERE p.release_id=r.id AND " + candidate + ')'
            total = db.execute('SELECT COUNT(*)' + sql, candidate_args).fetchone()[0]
            # Search only the same permission/view-filtered candidates. Paginate
            # after filtering their visible counts so totals never include hidden work.
            rows = db.execute('SELECT r.data' + sql + ' ORDER BY r.created DESC,r.id'
                              + ('' if filtering else ' LIMIT 20 OFFSET ?'),
                              candidate_args if filtering else [*candidate_args, (page - 1) * 20]).fetchall()
            for row in rows:
                data = json.loads(row[0])
                reviewer_names = '、'.join(p['name'] for p in reviewer_list(data))
                if query and query not in (data['name'] + ' ' + reviewer_names).casefold():
                    continue
                counts = db.execute("""SELECT COUNT(*) AS total, COUNT(DISTINCT p.person_id) AS people_count,
                    COALESCE(SUM(json_extract(i.value,'$.status') IN ('submitted','approved')),0) AS submitted,
                    COALESCE(SUM(json_extract(i.value,'$.status')='approved'),0) AS approved
                    FROM risk_people p JOIN risk_releases r ON r.id=p.release_id,json_each(p.data,'$.items') i
                    WHERE p.release_id=? AND NOT json_extract(p.data,'$.revoked')
                    AND json_extract(i.value,'$.status')!='revoked' AND """ + visible,
                    [data['id'], *args]).fetchone()
                if state and not {
                    'active': data['status'] == 'active' and counts['approved'] < counts['total'],
                    'review': counts['submitted'] > counts['approved'],
                    'overdue': data['status'] == 'active' and counts['total'] > counts['approved'] and data['deadline'] < now,
                    'completed': counts['total'] > 0 and counts['approved'] == counts['total'],
                    'publishing': data['status'] == 'publishing',
                }[state]:
                    continue
                full_control = self.controls(actor, data) and all(can_scope(actor, r[0]) for r in db.execute(
                    "SELECT DISTINCT COALESCE(json_extract(i.value,'$.scope'),p.scope) FROM risk_people p,json_each(p.data,'$.items') i WHERE p.release_id=?", (data['id'],)))
                result.append({**{k: data[k] for k in ('id', 'name', 'deadline', 'status')},
                               'owner': data['owner']['name'], 'reviewer': reviewer_names, **dict(counts),
                               'reviewers': [{k: p[k] for k in ('id', 'name')} for p in reviewer_list(data)],
                               'is_reviewer': any(p['id'] == actor['id'] for p in reviewer_list(data)),
                               'can_manage_release': full_control, 'can_delete_release': full_control,
                               'can_claim': manager(actor) and data['owner']['id'] == actor['id']
                                   and data['status'] == 'active' and not db.execute('SELECT 1 FROM risk_people WHERE release_id=? AND person_id=?', (data['id'], actor['id'])).fetchone()})
        if filtering:
            total = len(result)
            page = min(page, max(1, (total + 19) // 20))
            result = result[(page - 1) * 20:page * 20]
        return {'items': result, 'total': total, 'page': page, 'view': view, 'is_manager': manager(actor),
                'can_edit_template': manager(actor) and actor.get('level') in {'L0', 'L1'}}

    def delete(self, actor, rid, body):
        self.check(actor)
        if not isinstance(body, dict) or body:
            raise RiskError('删除参数无效。')
        with self.connect(True) as db:
            release = self._release(db, rid, include_deleted=True)
            scopes = [row[0] for row in db.execute("SELECT DISTINCT COALESCE(json_extract(i.value,'$.scope'),p.scope) FROM risk_people p,json_each(p.data,'$.items') i WHERE p.release_id=?", (rid,))]
            if not self.controls(actor, release) or not all(can_scope(actor, scope) for scope in scopes):
                raise RiskError('没有删除本次排查任务的权限。', 403)
            if release['status'] == 'deleted':
                return {'id': rid, 'status': 'deleted'}
            if db.execute("SELECT 1 FROM risk_jobs WHERE release_id=? AND kind!='template_sync' AND status='running' AND lease>=? LIMIT 1", (rid, time.time())).fetchone():
                raise RiskError('本任务正在同步或发送通知，请稍后再删除；原任务与反馈保持不变。', 409)
            release.update(status='deleted', deleted_at=time.time(), deleted_by=self.brief(actor))
            self._put_release(db, release)
            # Keep evidence and result tables; only stop work that has not started.
            db.execute("UPDATE risk_jobs SET status='cancelled',lease=0,error='排查任务已删除，停止后续执行' WHERE release_id=? AND kind!='template_sync' AND status!='done'", (rid,))
            self._audit(db, rid, '', actor, '删除排查任务', {})
        return {'id': rid, 'status': 'deleted'}

    def action(self, actor, rid, pid, body):
        self.check(actor)
        if not isinstance(body, dict) or set(body) - {'action', 'version', 'item_id', 'values', 'reason'}:
            raise RiskError('操作参数无效。')
        action = body.get('action')
        if not isinstance(action, str):
            raise RiskError('操作参数无效。')
        with self.connect(True) as db:
            release, person = self._release(db, rid), self._person(db, rid, pid)
            if not self._visible(actor, release, person):
                raise RiskError('无权处理这份排查。', 403)
            if type(body.get('version')) is not int or person['version'] != body['version']:
                raise RiskError('内容已更新，请刷新后核对，未覆盖他人的提交。', 409)
            if release['status'] != 'active' or person['revoked']:
                raise RiskError('任务尚未发布完成或已撤销，不能填写。', 409)
            item = next((i for i in person['items'] if i['id'] == body.get('item_id')), None)
            if action != 'revoke_person' and (not item or item['status'] == 'revoked'):
                raise RiskError('该排查事项不存在或已撤销。', 409)
            if item is not None and not self._visible(actor, release, person, item=item):
                raise RiskError('无权处理该楼栋的排查事项。', 403)
            reason = text(body.get('reason', ''), 2000)
            if action in {'save', 'submit'}:
                if actor['id'] != pid or item['status'] not in {'pending', 'returned'}:
                    raise RiskError('仅本人可填写未提交或退回的事项。', 403)
                def file_check(field, fid):
                    return bool(db.execute('SELECT 1 FROM risk_files WHERE id=? AND release_id=? AND person_id=? AND item_id=? AND field_id=?', (fid, rid, pid, item['id'], field)).fetchone())
                item['values'] = validate_values(system_fields(release['fields']), body.get('values', {}), complete=action == 'submit', file_check=file_check)
                if action == 'submit':
                    item.update(status='submitted', submitted_at=time.time(), submitted_by=actor['name'], reason='')
                    item.pop('reviewed_at', None)
                    item.pop('reviewed_by', None)
            elif action in {'approve', 'return'}:
                if not any(p['id'] == actor['id'] for p in reviewer_list(release)) or item['status'] != 'submitted':
                    raise RiskError('仅指定审核人可审核当前待审反馈。', 403)
                if action == 'return' and not reason:
                    raise RiskError('退回时请说明需要补充或修改的内容。')
                item.update(status='approved' if action == 'approve' else 'returned', reason=reason,
                            reviewed_at=time.time(), reviewed_by=actor['name'])
            elif action in {'revoke_person', 'revoke_item'}:
                if not self.controls(actor, release) or not reason:
                    raise RiskError('仅管理员可撤销，请填写原因。', 403)
                affected = person['items'] if action == 'revoke_person' else [item]
                if not all(can_scope(actor, item_scope(person, i)) for i in affected):
                    raise RiskError('撤销需要覆盖相关事项的全部楼栋。', 403)
                if action == 'revoke_person':
                    person['revoked'] = True
                    for i in person['items']:
                        i.update(previous_status=i['status'], status='revoked', reason=reason)
                else:
                    item.update(previous_status=item['status'], status='revoked', reason=reason)
            else:
                raise RiskError('操作无效。')
            person['version'] += 1
            if action in {'submit', 'return', 'approve'}:
                item['notice_version'] = person['version']
            person['updated'] = time.time()
            self._reminder_state(db, release, person)
            self._put_person(db, rid, person)
            self._audit(db, rid, pid, actor, action, {'item_id': body.get('item_id'), 'reason': reason, 'version': person['version'], 'items': person['items']})
            if action != 'save':
                snapshot = deepcopy(person)
                snapshot['items'] = [i for i in snapshot['items'] if self._visible(actor, release, person, item=i)]
                self.enqueue(db, rid, 'sync', {'person_id': pid, 'person': snapshot, 'actor': self.brief(actor)}, f'sync:{rid}:{pid}:{person["version"]}')
            if action in {'submit', 'return', 'approve'}:
                targets = [p['id'] for p in reviewer_list(release)] if action == 'submit' else [pid]
                for target in targets:
                    self.enqueue(db, rid, 'review_notice', {'person_id': target, 'subject_id': pid, 'action': action,
                                'item_id': item['id'], 'version': person['version'], 'reviewer_version': release.get('reviewer_version', 1)},
                                 f'review:{rid}:{pid}:{person["version"]}:{target}')
            if action in {'approve', 'revoke_item', 'revoke_person'} and release.get('final_managers'):
                people = [json.loads(r[0]) for r in db.execute('SELECT data FROM risk_people WHERE release_id=?', (rid,))]
                batch = completion_key(people)
                if batch:
                    self.enqueue(db, rid, 'final_push', {'batch': batch}, f'final:{rid}:{batch}')
        return self.get(actor, rid)

    def change_reviewer(self, actor, rid, body):
        self.check(actor)
        if not manager(actor):
            raise RiskError('仅有权限的管理员或主管可更换审核人。', 403)
        if (not isinstance(body, dict) or set(body) - {'reviewer_id', 'reviewer_ids', 'version'}
                or not ('reviewer_id' in body or 'reviewer_ids' in body) or type(body.get('version')) is not int):
            raise RiskError('审核人变更参数无效。')
        reviewers = self._reviewers(body)
        with self.connect(True) as db:
            release = self._release(db, rid)
            if not self.controls(actor, release):
                raise RiskError('无权管理他人发布的排查。', 403)
            people = [json.loads(r[0]) for r in db.execute('SELECT data FROM risk_people WHERE release_id=?', (rid,))]
            if not all(can_scope(actor, item_scope(p, i)) for p in people for i in p['items']):
                raise RiskError('更换审核人需要覆盖本次排查的全部楼栋。', 403)
            if release['status'] != 'active' or release.get('reviewer_version', 1) != body['version']:
                raise RiskError('任务或审核人已变化，请重新读取核对。', 409)
            if [p['id'] for p in reviewers] == [p['id'] for p in reviewer_list(release)]:
                return self.get(actor, rid)
            previous = reviewer_list(release)
            release.update(reviewer=reviewers[0], reviewers=reviewers, reviewer_version=body['version'] + 1)
            self._put_release(db, release)
            self._audit(db, rid, '', actor, '更换审核人', {'previous': previous, 'reviewers': reviewers})
            self.enqueue(db, rid, 'registry', {'actor': self.brief(actor)}, f'registry:{rid}:{release["reviewer_version"]}')
            for person in people:
                if person['revoked']:
                    continue
                for item in person['items']:
                    if item['status'] == 'submitted':
                        for reviewer in reviewers:
                            self.enqueue(db, rid, 'review_notice', {'person_id': reviewer['id'], 'subject_id': person['id'],
                                'action': 'submit', 'item_id': item['id'], 'version': item.get('notice_version', person['version']),
                                'reviewer_version': release['reviewer_version']},
                                f'reviewer:{rid}:{release["reviewer_version"]}:{person["id"]}:{item["id"]}:{reviewer["id"]}')
        return self.get(actor, rid)

    def claim(self, actor, rid, body):
        self.check(actor)
        if not isinstance(body, dict) or set(body) != {'scope'}:
            raise RiskError('请选择本次领取的楼栋。')
        with self.connect(True) as db:
            release = self._release(db, rid)
            if not manager(actor) or actor['id'] != release['owner']['id']:
                raise RiskError('仅发布人可领取本人的排查任务。', 403)
            scope = body['scope']
            scopes = {r[0] for r in db.execute("SELECT DISTINCT COALESCE(json_extract(i.value,'$.scope'),p.scope) FROM risk_people p,json_each(p.data,'$.items') i WHERE p.release_id=?", (rid,))}
            if not isinstance(scope, str) or scope not in scopes or not can_scope(actor, scope):
                raise RiskError('没有该楼栋的领取权限。', 403)
            if release['status'] != 'active':
                raise RiskError('排查尚未发布完成，请等待原任务恢复后再领取。', 409)
            existing = db.execute('SELECT data FROM risk_people WHERE release_id=? AND person_id=?', (rid, actor['id'])).fetchone()
            if existing:
                if json.loads(existing[0])['revoked']:
                    raise RiskError('本人任务已被撤销，不能重复领取。', 409)
            else:
                originals = release.get('original_items')
                if originals is None:
                    originals = [{'title': i['title'], 'values': {}} for i in self._person(db, rid,
                        db.execute('SELECT person_id FROM risk_people WHERE release_id=? LIMIT 1', (rid,)).fetchone()[0])['items']]
                person = {**self.brief(actor), 'scope': scope, 'scopes': [scope], 'version': 1, 'revoked': False, 'updated': time.time(), 'assigned_at': time.time(),
                          'items': [{'id': uuid.uuid4().hex, 'scope': scope, 'title': i['title'], 'values': deepcopy(i['values']), 'status': 'pending', 'reason': ''} for i in originals]}
                self._put_person(db, rid, person)
                self._audit(db, rid, actor['id'], actor, '领取本人任务', {})
                self.enqueue(db, rid, 'registry', {'actor': self.brief(actor)}, f'claim-registry:{rid}:{actor["id"]}')
        return self.get(actor, rid)

    def checkpoint(self, rid, **values):
        with self.connect(True) as db:
            data = self._release(db, rid)
            previous_messages = data['cloud'].get('messages', {})
            data['cloud'].update(values)
            prefix = f'notify:{rid}:'
            for key, receipt in values.get('messages', {}).items():
                if key.startswith(prefix) and receipt.get('message_id') and receipt != previous_messages.get(key):
                    person = self._person(db, rid, key[len(prefix):])
                    self._reminder_state(db, data, person)
                    self._put_person(db, rid, person)
            self._put_release(db, data)

    def activate(self, rid):
        with self.connect(True) as db:
            release = self._release(db, rid)
            release['status'] = 'active'
            release.setdefault('activated_at', time.time())
            self._put_release(db, release)
            people = [json.loads(row[0]) for row in db.execute('SELECT data FROM risk_people WHERE release_id=?', (rid,))]
            self.history.create(db, release, people)
            for row in db.execute('SELECT data FROM risk_people WHERE release_id=?', (rid,)).fetchall():
                person = json.loads(row[0])
                self._reminder_state(db, release, person)
                self._put_person(db, rid, person)
                self.enqueue(db, rid, 'notify', {'person_id': person['id']}, f'notify:{rid}:{person["id"]}')

    def raw(self, rid):
        with self.connect() as db:
            return self._release(db, rid), [json.loads(r[0]) for r in db.execute('SELECT data FROM risk_people WHERE release_id=?', (rid,))]

    @staticmethod
    def _reminder_state(db, release, person):
        items = outstanding_items(person)
        state = person.get('_reminder')
        if release['status'] != 'active' or not items:
            if state:
                db.execute("UPDATE risk_jobs SET status='cancelled',lease=0 WHERE id=? AND status NOT IN ('done','cancelled')", (state['id'],))
                person.pop('_reminder', None)
            return
        initial = release['cloud'].get('messages', {}).get(f'notify:{release["id"]}:{person["id"]}', {})
        initial_sent = initial.get('sent_at', 0) if initial.get('message_id') else 0
        if state:
            receipt = release['cloud'].get('messages', {}).get(state['id'], {})
            if not receipt.get('message_id'):
                due = max(state['due'], initial_sent + REMINDER_SECONDS)
                if due != state['due']:
                    # Keep the same delivery UUID and move its queued work with the timer.
                    state['due'] = due
                    db.execute("UPDATE risk_jobs SET next_at=MAX(next_at,?) WHERE id=? AND status='queued'", (due, state['id']))
                return
            due = max(receipt['sent_at'], initial_sent) + REMINDER_SECONDS
        else:
            assigned = max(person.get('assigned_at', release.get('activated_at', release['created'])), initial_sent)
            due = min(i.get('reviewed_at', assigned) if i['status'] == 'returned' else assigned for i in items) + REMINDER_SECONDS
        person['_reminder'] = {'id': f'remind:{release["id"]}:{person["id"]}:{uuid.uuid4().hex}', 'due': due}

    def enqueue_reminders(self):
        """Queue the existing two-hour work reminders and one-day review reminders durably."""
        with self.connect(True) as db:
            for row in db.execute("SELECT data FROM risk_releases WHERE json_extract(data,'$.status')='active'"):
                release = json.loads(row[0])
                people = db.execute('SELECT data FROM risk_people WHERE release_id=?', (release['id'],)).fetchall()
                for row in people:
                    person = json.loads(row[0])
                    before = deepcopy(person.get('_reminder'))
                    self._reminder_state(db, release, person)
                    state = person.get('_reminder')
                    if before != state:
                        self._put_person(db, release['id'], person)
                    if state:
                        db.execute("UPDATE risk_jobs SET status='queued' WHERE id=? AND kind='notify' AND status='failed'",
                                   (f'notify:{release["id"]}:{person["id"]}',))
                    if state and state['due'] <= time.time():
                        self.enqueue(db, release['id'], 'reminder', {'person_id': person['id']}, state['id'])
                    if person.get('revoked'):
                        continue
                    for item in person['items']:
                        submitted = item.get('submitted_at')
                        if (item['status'] != 'submitted' or not isinstance(submitted, (int, float))
                                or submitted <= 0 or time.time() < submitted + 86400):
                            continue
                        for reviewer in reviewer_list(release):
                            version = item.get('notice_version', submitted)
                            reviewers = release.get('reviewer_version', 1)
                            self.enqueue(db, release['id'], 'review_reminder', {
                                'person_id': reviewer['id'], 'subject_id': person['id'], 'item_id': item['id'],
                                'version': version, 'reviewer_version': reviewers},
                                f'review-reminder:{release["id"]}:{person["id"]}:{item["id"]}:{version}:{reviewers}:{reviewer["id"]}')

    def review_reminder(self, job):
        with self.connect() as db:
            payload = job['payload']
            row = db.execute('''SELECT r.data AS release,p.data AS person FROM risk_releases r
                JOIN risk_people p ON p.release_id=r.id WHERE r.id=? AND p.person_id=?''',
                (job['release_id'], payload['subject_id'])).fetchone()
            if not row:
                return None
            release, person = json.loads(row['release']), json.loads(row['person'])
            item = next((i for i in person['items'] if i['id'] == payload['item_id']), None)
            if (release['status'] == 'active' and not person.get('revoked') and item
                    and item['status'] == 'submitted' and item.get('submitted_at', float('inf')) + 86400 <= time.time()
                    and item.get('notice_version', item.get('submitted_at')) == payload['version']
                    and release.get('reviewer_version', 1) == payload['reviewer_version']
                    and any(p['id'] == payload['person_id'] for p in reviewer_list(release))):
                return release, person, item
        return None

    def reminder(self, job):
        """Recheck the current assignment and cycle immediately before delivery."""
        with self.connect() as db:
            row = db.execute('''SELECT r.data AS release,p.data AS person FROM risk_releases r
                JOIN risk_people p ON p.release_id=r.id WHERE r.id=? AND p.person_id=?''',
                (job['release_id'], job['payload']['person_id'])).fetchone()
            if row:
                release, person = json.loads(row['release']), json.loads(row['person'])
                state = person.get('_reminder') or {}
                if (release['status'] == 'active' and state.get('id') == job['id']
                        and state['due'] <= time.time() and outstanding_items(person)):
                    return release, person
        return None

    def claim_job(self):
        with self.connect(True) as db:
            row = db.execute("""SELECT j.* FROM risk_jobs j
                WHERE (j.status='queued' AND j.next_at<=? OR j.status='running' AND j.lease<?)
                AND (j.kind='template_sync' OR EXISTS(SELECT 1 FROM risk_releases r WHERE r.id=j.release_id AND json_extract(r.data,'$.status')!='deleted'))
                AND NOT EXISTS(SELECT 1 FROM risk_jobs owner WHERE owner.status='running' AND owner.lease>=?)
                AND NOT EXISTS(SELECT 1 FROM risk_jobs earlier WHERE earlier.release_id=j.release_id
                    AND earlier.status NOT IN ('done','cancelled') AND earlier.id!=j.id AND
                    (earlier.kind='publish' OR (j.kind='reminder' AND earlier.id='notify:'||j.release_id||':'||json_extract(j.payload,'$.person_id'))
                    OR (j.kind='final_push' AND earlier.kind IN ('sync','registry')) OR (j.kind='sync' AND earlier.kind='sync'
                    AND json_extract(earlier.payload,'$.person.version')<json_extract(j.payload,'$.person.version')
                    AND json_extract(earlier.payload,'$.person_id')=json_extract(j.payload,'$.person_id'))))
                ORDER BY j.created,j.id LIMIT 1""", (time.time(), time.time(), time.time())).fetchone()
            if not row:
                return None
            lease = time.time() + 900
            db.execute("UPDATE risk_jobs SET status='running',lease=? WHERE id=?", (lease, row['id']))
            return {**dict(row), 'status': 'running', 'lease': lease, 'payload': json.loads(row['payload'])}

    def renew_job(self, job):
        with self.connect(True) as db:
            lease = time.time() + 900
            changed = db.execute("UPDATE risk_jobs SET lease=? WHERE id=? AND status='running' AND lease=?",
                                 (lease, job['id'], job['lease'])).rowcount
            if not changed:
                raise RiskError('任务已由其他执行进程接管，停止本次执行。', 409)
            job['lease'] = lease

    def finish_job(self, job, error=''):
        attempts = job['attempts'] + bool(error)
        status = 'done' if not error else 'failed' if attempts >= 6 and job['kind'] not in {'reminder', 'review_reminder', 'notify'} else 'queued'
        with self.connect(True) as db:
            db.execute("UPDATE risk_jobs SET status=?,error=?,attempts=?,next_at=?,lease=0 WHERE id=? AND status='running' AND lease=?",
                       (status, error[:700], attempts, time.time() + min(600, 15 * 2 ** min(attempts, 6)), job['id'], job['lease']))

    def retry(self, actor, rid, *, template_only=False):
        detail = self.get(actor, rid)
        if not detail['can_manage_release']:
            raise RiskError('仅管理员可重试云端同步。', 403)
        _, people = self.raw(rid)
        if not all(can_scope(actor, item_scope(p, i)) for p in people for i in p['items']):
            raise RiskError('不能重试超出楼栋权限的任务。', 403)
        with self.connect(True) as db:
            if template_only:
                db.execute("UPDATE risk_jobs SET status='queued',next_at=0,attempts=0 WHERE release_id=? AND status='failed' AND kind='template_sync'", (rid,))
            else:
                for row in db.execute("SELECT id,payload FROM risk_jobs WHERE release_id=? AND status='failed' AND kind='sync'", (rid,)).fetchall():
                    payload = json.loads(row['payload'])
                    payload['retry_authorized_by'] = self.brief(actor)
                    db.execute('UPDATE risk_jobs SET payload=? WHERE id=?', (json.dumps(payload, ensure_ascii=False), row['id']))
                    self._audit(db, rid, payload['person_id'], actor, '授权同步重试', {'job_id': row['id']})
                db.execute("UPDATE risk_jobs SET status='queued',next_at=0,attempts=0 WHERE release_id=? AND status='failed'", (rid,))
        return self.get(actor, rid)

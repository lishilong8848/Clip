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
                '复核日期': 'reviewed_at', '最终推送': 'sync'}


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


def schema_fields(fields, settings=None):
    settings = settings or {}
    result = []
    for source in fields:
        field = {k: deepcopy(source[k]) for k in ('field_id', 'field_name', 'type', 'property', 'is_primary') if k in source}
        field['key'] = field['field_id']
        options = settings.get(field['key'], {})
        field['role'] = options.get('role') or SYSTEM_NAMES.get(field['field_name'], 'input')
        field['required'] = bool(options.get('required', field['field_name'] in {'排查结果', '排查日期'}))
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
        if not actor.get('id') or not actor.get('open_id') or MODULE not in actor.get('modules', []) or actor.get('role') in {'guest', 'temporary'}:
            raise RiskError('当前账号没有风险排查权限。', 403)

    def template(self):
        with self.connect() as db:
            row = db.execute('SELECT data FROM risk_template WHERE id=1').fetchone()
            return json.loads(row[0]) if row else None

    def save_template(self, data):
        with self.connect(True) as db:
            db.execute('INSERT OR REPLACE INTO risk_template(id,data) VALUES(1,?)', (json.dumps(data, ensure_ascii=False),))

    @staticmethod
    def _release(db, rid):
        row = db.execute('SELECT data FROM risk_releases WHERE id=?', (rid,)).fetchone()
        if not row:
            raise RiskError('排查任务不存在。', 404)
        return json.loads(row[0])

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

    def publish(self, actor, body, template):
        self.check(actor)
        if not manager(actor):
            raise RiskError('仅管理员或授权楼栋主管可发布排查。', 403)
        if set(body) - {'id', 'name', 'deadline', 'reviewer_id', 'people', 'items', 'template_version', 'history_template_id'}:
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
        history_id = body.get('history_template_id')
        if history_id:
            template = self.history.get(actor, text(history_id, 32, True), usable=True)
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
        if any(not isinstance(p, dict) or set(p) != {'id', 'scope'} or not isinstance(p['id'], str) or not isinstance(p['scope'], str) for p in recipients) or len({p['id'] for p in recipients}) != len(recipients):
            raise RiskError('人员不能重复分配。')
        reviewer = self.person_for(text(body.get('reviewer_id'), 100, True))
        self.check(reviewer)
        if not manager(reviewer):
            raise RiskError('审核人需为有权限的管理员或楼栋主管。')
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
            scope = selected['scope']
            if not all(can_scope(a, scope) for a in (actor, reviewer, person)):
                raise RiskError('发布人、审核人及接收人必须都有对应楼栋的风险排查权限。', 403)
            if person['id'] == reviewer['id']:
                raise RiskError('审核人不能审核自己的排查，请选择其他审核人。')
            people.append({**self.brief(person), 'scope': scope, 'version': 1, 'revoked': False,
                           'items': deepcopy(base_items), 'updated': time.time()})
        data = {'id': rid, 'name': text(body.get('name'), 120, True), 'deadline': deadline,
                'owner': self.brief(actor), 'reviewer': self.brief(reviewer), 'created': time.time(),
                'status': 'publishing', 'fields': fields, 'template_version': template['version'],
                'original_items': [{'title': i['title'], 'values': deepcopy(i['values'])} for i in base_items],
                'source_template': {'id': history_id, 'revision': template['revision'], 'name': template['name']} if history_id else None,
                'cloud': {}, 'request_hash': digest(body)}
        with self.connect(True) as db:
            if history_id:
                current = self.history.read(db, history_id)
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

    def _visible(self, actor, release, person):
        return can_scope(actor, person['scope']) and (actor['id'] == person['id'] or manager(actor))

    def get(self, actor, rid):
        self.check(actor)
        with self.connect() as db:
            release = self._release(db, rid)
            people = [json.loads(r[0]) for r in db.execute('SELECT data FROM risk_people WHERE release_id=? ORDER BY scope,person_id', (rid,))]
            visible = [p for p in people if self._visible(actor, release, p)]
            if not visible:
                raise RiskError('没有查看本次排查的权限。', 403)
            public = {k: deepcopy(release[k]) for k in ('id', 'name', 'created', 'deadline', 'status', 'fields')}
            public.update({'owner': release['owner']['name'], 'reviewer': release['reviewer']['name'], 'people': visible,
                           'url': release['cloud'].get('url', '') if manager(actor) and len(visible) == len(people) else '',
                           'is_manager': manager(actor), 'is_reviewer': manager(actor) and actor['id'] == release['reviewer']['id'], 'self_id': actor['id']})
            public['reviewer_id'] = release['reviewer']['id']
            public['reviewer_version'] = release.get('reviewer_version', 1)
            visible_ids = {p['id'] for p in visible}
            placeholders = ','.join('?' for _ in visible_ids)
            files = {}
            for row in db.execute('SELECT id,name,size,field_id,person_id,item_id FROM risk_files WHERE release_id=? AND person_id IN (' + placeholders + ')', [rid, *visible_ids]):
                files.setdefault((row['person_id'], row['item_id']), []).append({k: row[k] for k in ('id', 'name', 'size', 'field_id')})
            for p in public['people']:
                p.pop('open_id', None)
                for i in p['items']:
                    i['files'] = files.get((p['id'], i['id']), [])
            jobs = db.execute("SELECT id,kind,status,error,COALESCE(json_extract(payload,'$.subject_id'),json_extract(payload,'$.person_id')) AS subject FROM risk_jobs WHERE release_id=? AND status!='done'", (rid,))
            public['can_manage_release'] = manager(actor) and len(visible) == len(people)
            public['jobs'] = []
            for job in jobs:
                if (job['subject'] in visible_ids and (manager(actor) or job['kind'] != 'review_notice')
                        or public['can_manage_release']):
                    public['jobs'].append({k: job[k] for k in ('id', 'kind', 'status', 'error')})
            audit_ids = [*visible_ids, *([''] if public['can_manage_release'] else [])]
            public['audit'] = [dict(r) for r in db.execute('SELECT person_id,action,at,data FROM risk_audit WHERE release_id=? AND person_id IN (' + ','.join('?' for _ in audit_ids) + ') ORDER BY id DESC LIMIT 100', [rid, *audit_ids])]
            return public

    def list(self, actor, page=1, scope=''):
        self.check(actor)
        try:
            page = max(1, int(page))
        except (TypeError, ValueError):
            raise RiskError('页码无效。') from None
        scopes = [s for s in SCOPES if can_scope(actor, s)]
        if scope and scope != 'ALL':
            if scope not in scopes:
                raise RiskError('没有该楼栋风险排查的查看权限。', 403)
            scopes = [scope]
        visible = 'p.scope IN (' + ','.join('?' for _ in scopes) + ')'
        args = list(scopes)
        if not manager(actor):
            visible += ' AND p.person_id=?'
            args.append(actor['id'])
        result = []
        with self.connect() as db:
            sql = ' FROM risk_releases r WHERE EXISTS(SELECT 1 FROM risk_people p WHERE p.release_id=r.id AND ' + visible + ')'
            total = db.execute('SELECT COUNT(*)' + sql, args).fetchone()[0]
            rows = db.execute('SELECT r.data' + sql + ' ORDER BY r.created DESC,r.id LIMIT 20 OFFSET ?', args + [(page - 1) * 20]).fetchall()
            for row in rows:
                data = json.loads(row[0])
                counts = db.execute("""SELECT COUNT(*) AS total, COUNT(DISTINCT p.person_id) AS people_count,
                    COALESCE(SUM(json_extract(i.value,'$.status') IN ('submitted','approved')),0) AS submitted,
                    COALESCE(SUM(json_extract(i.value,'$.status')='approved'),0) AS approved
                    FROM risk_people p,json_each(p.data,'$.items') i
                    WHERE p.release_id=? AND NOT json_extract(p.data,'$.revoked')
                    AND json_extract(i.value,'$.status')!='revoked' AND """ + visible, [data['id'], *args]).fetchone()
                result.append({**{k: data[k] for k in ('id', 'name', 'deadline', 'status')},
                               'owner': data['owner']['name'], 'reviewer': data['reviewer']['name'], **dict(counts)})
        return {'items': result, 'total': total, 'page': page, 'is_manager': manager(actor),
                'can_edit_template': actor.get('level') in {'L0', 'L1'}}

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
            reason = text(body.get('reason', ''), 2000)
            if action in {'save', 'submit'}:
                if actor['id'] != pid or item['status'] not in {'pending', 'returned'}:
                    raise RiskError('仅本人可填写未提交或退回的事项。', 403)
                def file_check(field, fid):
                    return bool(db.execute('SELECT 1 FROM risk_files WHERE id=? AND release_id=? AND person_id=? AND item_id=? AND field_id=?', (fid, rid, pid, item['id'], field)).fetchone())
                item['values'] = validate_values(release['fields'], body.get('values', {}), complete=action == 'submit', file_check=file_check)
                if action == 'submit':
                    item.update(status='submitted', submitted_at=time.time(), submitted_by=actor['name'], reason='')
                    item.pop('reviewed_at', None)
                    item.pop('reviewed_by', None)
            elif action in {'approve', 'return'}:
                if not manager(actor) or actor['id'] != release['reviewer']['id'] or actor['id'] == pid or item['status'] != 'submitted':
                    raise RiskError('仅指定审核人可审核当前待审反馈，不能自审。', 403)
                if action == 'return' and not reason:
                    raise RiskError('退回时请说明需要补充或修改的内容。')
                item.update(status='approved' if action == 'approve' else 'returned', reason=reason,
                            reviewed_at=time.time(), reviewed_by=actor['name'])
            elif action in {'revoke_person', 'revoke_item'}:
                if not manager(actor) or not reason:
                    raise RiskError('仅管理员可撤销，请填写原因。', 403)
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
            self._put_person(db, rid, person)
            self._audit(db, rid, pid, actor, action, {'item_id': body.get('item_id'), 'reason': reason, 'version': person['version'], 'items': person['items']})
            if action != 'save':
                self.enqueue(db, rid, 'sync', {'person_id': pid, 'person': deepcopy(person), 'actor': self.brief(actor)}, f'sync:{rid}:{pid}:{person["version"]}')
            if action in {'submit', 'return', 'approve'}:
                target = release['reviewer']['id'] if action == 'submit' else pid
                self.enqueue(db, rid, 'review_notice', {'person_id': target, 'subject_id': pid, 'action': action,
                            'item_id': item['id'], 'version': person['version']}, f'review:{rid}:{pid}:{person["version"]}')
        return self.get(actor, rid)

    def change_reviewer(self, actor, rid, body):
        self.check(actor)
        if not manager(actor):
            raise RiskError('仅有权限的管理员或主管可更换审核人。', 403)
        if not isinstance(body, dict) or set(body) != {'reviewer_id', 'version'} or type(body['version']) is not int:
            raise RiskError('审核人变更参数无效。')
        reviewer = self.person_for(text(body['reviewer_id'], 100, True))
        self.check(reviewer)
        if not manager(reviewer):
            raise RiskError('请选择有对应权限的管理员或主管作为审核人。', 403)
        with self.connect(True) as db:
            release = self._release(db, rid)
            people = [json.loads(r[0]) for r in db.execute('SELECT data FROM risk_people WHERE release_id=?', (rid,))]
            if not all(can_scope(actor, p['scope']) and can_scope(reviewer, p['scope']) for p in people):
                raise RiskError('更换审核人需要覆盖本次排查的全部楼栋。', 403)
            if release['status'] != 'active' or release.get('reviewer_version', 1) != body['version']:
                raise RiskError('任务或审核人已变化，请重新读取核对。', 409)
            if any(p['id'] == reviewer['id'] and not p['revoked'] for p in people):
                raise RiskError('审核人不能同时作为本次排查接收人。')
            if reviewer['id'] == release['reviewer']['id']:
                return self.get(actor, rid)
            previous = release['reviewer']
            release.update(reviewer=self.brief(reviewer), reviewer_version=body['version'] + 1)
            self._put_release(db, release)
            self._audit(db, rid, '', actor, '更换审核人', {'previous': previous, 'reviewer': self.brief(reviewer)})
            self.enqueue(db, rid, 'registry', {'actor': self.brief(actor)}, f'registry:{rid}:{release["reviewer_version"]}')
            for person in people:
                if person['revoked']:
                    continue
                for item in person['items']:
                    if item['status'] == 'submitted':
                        self.enqueue(db, rid, 'review_notice', {'person_id': reviewer['id'], 'subject_id': person['id'],
                            'action': 'submit', 'item_id': item['id'], 'version': item.get('notice_version', person['version'])},
                            f'reviewer:{rid}:{release["reviewer_version"]}:{person["id"]}:{item["id"]}')
        return self.get(actor, rid)

    def checkpoint(self, rid, **values):
        with self.connect(True) as db:
            data = self._release(db, rid)
            data['cloud'].update(values)
            self._put_release(db, data)

    def activate(self, rid):
        with self.connect(True) as db:
            release = self._release(db, rid)
            release['status'] = 'active'
            self._put_release(db, release)
            people = [json.loads(row[0]) for row in db.execute('SELECT data FROM risk_people WHERE release_id=?', (rid,))]
            self.history.create(db, release, people)
            for row in db.execute('SELECT data FROM risk_people WHERE release_id=?', (rid,)).fetchall():
                person = json.loads(row[0])
                self.enqueue(db, rid, 'notify', {'person_id': person['id']}, f'notify:{rid}:{person["id"]}')

    def raw(self, rid):
        with self.connect() as db:
            return self._release(db, rid), [json.loads(r[0]) for r in db.execute('SELECT data FROM risk_people WHERE release_id=?', (rid,))]

    def claim_job(self):
        with self.connect(True) as db:
            row = db.execute("""SELECT j.* FROM risk_jobs j
                WHERE (j.status='queued' AND j.next_at<=? OR j.status='running' AND j.lease<?)
                AND NOT EXISTS(SELECT 1 FROM risk_jobs owner WHERE owner.status='running' AND owner.lease>=?)
                AND NOT EXISTS(SELECT 1 FROM risk_jobs earlier WHERE earlier.release_id=j.release_id
                    AND earlier.status!='done' AND earlier.id!=j.id AND
                    (earlier.kind='publish' OR (j.kind='sync' AND earlier.kind='sync'
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
        status = 'done' if not error else 'failed' if attempts >= 6 else 'queued'
        with self.connect(True) as db:
            db.execute("UPDATE risk_jobs SET status=?,error=?,attempts=?,next_at=?,lease=0 WHERE id=? AND status='running' AND lease=?",
                       (status, error[:700], attempts, time.time() + min(600, 15 * 2 ** min(attempts, 6)), job['id'], job['lease']))

    def retry(self, actor, rid, *, template_only=False):
        self.get(actor, rid)
        if not manager(actor):
            raise RiskError('仅管理员可重试云端同步。', 403)
        _, people = self.raw(rid)
        if not all(can_scope(actor, p['scope']) for p in people):
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

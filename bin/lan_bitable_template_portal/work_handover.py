"""Personnel handovers: immutable sent versions and independent follow-up checks."""
from __future__ import annotations

import copy
import datetime as dt
import json
import re
import secrets
import sqlite3
from contextlib import contextmanager
from pathlib import Path

SCOPES = ('110', 'A', 'B', 'C', 'D', 'E', 'H', 'CAMPUS')
KINDS = {'ongoing', 'todo', 'history', 'reference'}
MODULE = 'work_handover'


class HandoverError(Exception):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


def timestamp():
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).isoformat(timespec='seconds')


def text(value, maximum, required=False):
    if not isinstance(value, str) or len(value) > maximum or required and not value.strip():
        raise HandoverError('填写内容为空、格式无效或超过长度限制。')
    return value.strip()


def authorized(actor, scope, module=''):
    return (scope in SCOPES and (scope in actor.get('scope_rules', []) or 'ALL' in actor.get('scope_rules', []))
            and (not module or module in actor.get('modules', [])))


class Handovers:
    def __init__(self, db_path, people, actor_by_person):
        self.db_path = Path(db_path)
        self.people, self.actor_by_person = people, actor_by_person
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS work_handovers(
                    id TEXT PRIMARY KEY, owner TEXT NOT NULL, recipient TEXT NOT NULL,
                    status TEXT NOT NULL, version INTEGER NOT NULL, updated TEXT NOT NULL,
                    payload TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS handover_owner ON work_handovers(owner,updated);
                CREATE INDEX IF NOT EXISTS handover_recipient ON work_handovers(recipient,status,updated);
                CREATE TABLE IF NOT EXISTS work_handover_audit(
                    seq INTEGER PRIMARY KEY, handover_id TEXT NOT NULL, actor TEXT NOT NULL,
                    action TEXT NOT NULL, time TEXT NOT NULL, payload TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS handover_audit ON work_handover_audit(handover_id,seq);
                CREATE TABLE IF NOT EXISTS work_handover_versions(
                    handover_id TEXT NOT NULL, version INTEGER NOT NULL, payload TEXT NOT NULL,
                    PRIMARY KEY(handover_id,version));
                CREATE TABLE IF NOT EXISTS work_handover_sources(
                    id TEXT PRIMARY KEY, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS work_handover_outbox(
                    id TEXT PRIMARY KEY, handover_id TEXT NOT NULL, version INTEGER NOT NULL,
                    kind TEXT NOT NULL, target TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'queued',
                    attempts INTEGER NOT NULL DEFAULT 0, next_at REAL NOT NULL DEFAULT 0,
                    error TEXT NOT NULL DEFAULT '', message_id TEXT NOT NULL DEFAULT '', payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS work_handover_files(
                    id TEXT PRIMARY KEY, handover_id TEXT NOT NULL, name TEXT NOT NULL,
                    size INTEGER NOT NULL, time TEXT NOT NULL, visible INTEGER NOT NULL DEFAULT 1);
            ''')

    @contextmanager
    def connect(self, write=False):
        db = sqlite3.connect(self.db_path, timeout=15)
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
    def check_actor(actor):
        if not actor.get('id') or MODULE not in actor.get('modules', []) or actor.get('role') in {'guest', 'temporary'}:
            raise HandoverError('当前账号没有人员工作交接权限。', 403)

    @staticmethod
    def _read(db, identity):
        row = db.execute('SELECT payload FROM work_handovers WHERE id=?', (identity,)).fetchone()
        if not row:
            raise HandoverError('交接单不存在。', 404)
        return json.loads(row[0])

    def _access(self, data, actor):
        self.check_actor(actor)
        if data['status'] == 'deleted':
            raise HandoverError('交接草稿已删除。' if actor['id'] == data['owner']['id'] else '无权查看此交接单。',
                                410 if actor['id'] == data['owner']['id'] else 403)
        if actor['id'] != data['owner']['id'] and not (actor['id'] == data['recipient_id'] and data['status'] not in {'draft', 'withdrawn'}):
            raise HandoverError('只能查看本人发出或收到的交接单。', 403)

    def _save(self, db, data, actor, action, detail=None):
        data['updated_at'] = timestamp()
        db.execute('INSERT INTO work_handovers VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET recipient=excluded.recipient,status=excluded.status,version=excluded.version,updated=excluded.updated,payload=excluded.payload',
                   (data['id'], data['owner']['id'], data['recipient_id'], data['status'], data['version'], data['updated_at'], json.dumps(data, ensure_ascii=False)))
        db.execute('INSERT INTO work_handover_audit(handover_id,actor,action,time,payload) VALUES(?,?,?,?,?)',
                   (data['id'], actor['name'], action, data['updated_at'], json.dumps(detail or {}, ensure_ascii=False)))

    @staticmethod
    def _queue(db, data, kind, target):
        key = f"handover:{data['id']}:{data['version']}:{kind}"
        snapshot = {k:v for k,v in data.items() if k != 'sent_snapshot'}
        snapshot['_delivery'] = {'sender_app_id': '', 'status': 'not_sent'}
        db.execute('INSERT OR IGNORE INTO work_handover_outbox(id,handover_id,version,kind,target,payload) VALUES(?,?,?,?,?,?)',
                   (key, data['id'], data['version'], kind, target, json.dumps(snapshot,ensure_ascii=False)))

    def _public(self, db, data, actor):
        self._access(data, actor)
        result = copy.deepcopy(data)
        result.pop('card_token', None)
        result.pop('sent_snapshot', None)
        result['is_owner'] = actor['id'] == data['owner']['id']
        result['is_recipient'] = actor['id'] == data['recipient_id']
        for item in result['items']:
            allowed = authorized(actor, item['scope'], item.get('module', ''))
            if not allowed:
                item.update(title='该事项的业务权限已变更', notes='', source_url='', restricted=True)
                item.pop('source_title', None)
            elif item.get('source_id'):
                row = db.execute('SELECT payload FROM work_handover_sources WHERE id=?', (item['source_id'],)).fetchone()
                if row:
                    current = json.loads(row[0])
                    item['business_status'] = current['business_status']
                    item['source_checked_at'] = current['source_checked_at']
        if any(i.get('restricted') for i in result['items']):
            result.update(title='部分交接事项权限已变更',notes='')
        result['files'] = []
        if all(authorized(actor,i['scope'],i.get('module','')) for i in data['items']):
            if result['is_owner']:
                result['files'] = [dict(row) for row in db.execute('SELECT id,name,size,time FROM work_handover_files WHERE handover_id=? AND visible=1 ORDER BY time', (data['id'],))]
            else:
                row = db.execute('SELECT payload FROM work_handover_versions WHERE handover_id=? AND version=?', (data['id'],data.get('sent_version',0))).fetchone()
                archive = json.loads(row[0]) if row else {}
                if archive.get('recipient_id') == actor['id']:
                    result['files'] = archive.get('files',[])
        result['audit'] = [dict(row) for row in db.execute('SELECT actor,action,time,payload FROM work_handover_audit WHERE handover_id=? ORDER BY seq DESC LIMIT 100', (data['id'],))]
        result['notification'] = dict(db.execute("SELECT state,error,message_id FROM work_handover_outbox WHERE handover_id=? AND kind='invite' ORDER BY version DESC LIMIT 1", (data['id'],)).fetchone() or {})
        result['notification_failures'] = [row[0] for row in db.execute("SELECT kind FROM work_handover_outbox WHERE handover_id=? AND state='failed' AND json_extract(payload,'$.sent_version')=?", (data['id'],data.get('sent_version',0)))]
        return result

    def get(self, actor, identity):
        with self.connect() as db:
            return self._public(db, self._read(db, identity), actor)

    def list(self, actor, view='received', page=1):
        self.check_actor(actor)
        page = max(1, min(int(page), 10000))
        clause = "owner=? AND status!='deleted'" if view == 'sent' else "recipient=? AND status NOT IN ('draft','withdrawn','deleted')"
        if view == 'pending':
            clause += " AND status='pending'"
        with self.connect() as db:
            total = db.execute('SELECT COUNT(*) FROM work_handovers WHERE '+clause, (actor['id'],)).fetchone()[0]
            rows = db.execute('SELECT payload FROM work_handovers WHERE '+clause+' ORDER BY updated DESC LIMIT 30 OFFSET ?', (actor['id'], (page-1)*30))
            items = []
            for row in rows:
                data = json.loads(row[0])
                items.append({key: data[key] for key in ('id','title','owner','recipient_id','recipient_name','status','version','updated_at')})
                if any(not authorized(actor,i['scope'],i.get('module','')) for i in data['items']):
                    items[-1]['title']='部分交接事项权限已变更'
                items[-1]['remaining'] = sum(not item.get('removed') and not item.get('done') and item['kind'] in {'ongoing','todo'} for item in data['items'])
                items[-1]['total'] = sum(not item.get('removed') for item in data['items'])
            return {'items': items, 'total': total, 'page': page}

    def create(self, actor, identity):
        self.check_actor(actor)
        if not re.fullmatch(r'[a-f0-9]{32}', str(identity)):
            raise HandoverError('交接单标识无效。')
        with self.connect(True) as db:
            existing = db.execute('SELECT id FROM work_handovers WHERE id=?', (identity,)).fetchone()
            if existing:
                return self._public(db, self._read(db, identity), actor)
            data = {'id': identity, 'title': '工作交接', 'owner': {k: actor[k] for k in ('id','name','open_id')},
                    'recipient_id': '', 'recipient_name': '', 'status': 'draft', 'version': 1,
                    'notes': '', 'items': [], 'created_at': timestamp(), 'updated_at': timestamp()}
            self._save(db, data, actor, '创建草稿')
            return self._public(db, data, actor)

    def cache_sources(self, rows):
        with self.connect(True) as db:
            for row in rows:
                db.execute('INSERT OR REPLACE INTO work_handover_sources VALUES(?,?)', (row['id'], json.dumps(row, ensure_ascii=False)))

    @staticmethod
    def _active_sources(items):
        return {i['source_id'] for i in items if i.get('source_id') and i['kind'] in {'ongoing','todo'} and not i.get('done') and not i.get('removed')}

    def _check_conflicts(self, db, identity, items, actor):
        sources = self._active_sources(items)
        if not sources:
            return
        for row in db.execute("SELECT payload FROM work_handovers WHERE status IN ('pending','accepted') AND id<>?",(identity,)):
            other = json.loads(row[0])
            if sources & self._active_sources(other['items']):
                target = ('，接收人：'+other['recipient_name']) if actor['id'] in {other['owner']['id'],other['recipient_id']} else ''
                hint = '请先处理或撤回原交接。' if other['status']=='pending' else '如需转交，请先由原接收人移除原清单项，再发起新交接。'
                raise HandoverError('有事项已在另一份未完成的交接清单中'+target+'。'+hint,409)

    def save(self, actor, identity, version, payload):
        self.check_actor(actor)
        if set(payload) != {'title','recipient_id','notes','items'} or not isinstance(payload['items'], list) or len(payload['items']) > 500:
            raise HandoverError('交接清单格式无效，单份最多500项。')
        person = self.actor_by_person(text(payload['recipient_id'], 120)) if payload['recipient_id'] else None
        if person and person['id'] == actor['id']:
            raise HandoverError('接收人不能是本人。')
        with self.connect(True) as db:
            data = self._read(db, identity)
            self._access(data, actor)
            if data['owner']['id'] != actor['id'] or data['status'] not in {'draft','returned','withdrawn'}:
                raise HandoverError('请先撤回待接收交接单后再修改。', 409)
            if version != data['version']:
                raise HandoverError('交接单已更新，请重新读取后修改。', 409)
            items, ids, sources = [], set(), set()
            for raw in payload['items']:
                if not isinstance(raw, dict) or set(raw)-{'id','title','notes','scope','kind','source_id'}:
                    raise HandoverError('事项字段无效。')
                item = {key: text(raw.get(key, ''), size, key in {'id','title','scope','kind'}) for key, size in
                        [('id',64),('title',240),('notes',4000),('scope',20),('kind',20),('source_id',64)]}
                if not re.fullmatch(r'[a-f0-9]{32}', item['id']) or item['id'] in ids or item['kind'] not in KINDS:
                    raise HandoverError('事项重复或分类无效。')
                ids.add(item['id'])
                if item['source_id']:
                    row = db.execute('SELECT payload FROM work_handover_sources WHERE id=?', (item['source_id'],)).fetchone()
                    if not row or item['source_id'] in sources:
                        raise HandoverError('关联事项已失效或重复，请重新选择。')
                    source = json.loads(row[0])
                    for key in ('scope','module','source_url','business_status','source_checked_at','period'):
                        item[key] = source[key]
                    item['source_title'] = source['title']
                    sources.add(item['source_id'])
                if not authorized(actor, item['scope'], item.get('module','')):
                    raise HandoverError('包含超出本人权限的交接事项。', 403)
                items.append(item)
            data.update(title=text(payload['title'],200,True), notes=text(payload['notes'],6000),
                        recipient_id=person['id'] if person else '', recipient_name=person['name'] if person else '',
                        items=items, version=data['version']+1, status='draft')
            self._save(db, data, actor, '保存草稿')
            return self._public(db, data, actor)

    def action(self, actor, identity, version, action, *, reason='', item_id='', token=''):
        self.check_actor(actor)
        reason = text(reason,1000)
        target = None
        if action == 'send':
            # Directory refresh may touch the shared database; do it before taking the write lock.
            with self.connect() as db:
                preview = self._read(db,identity)
                self._access(preview,actor)
                if preview['owner']['id'] != actor['id']:
                    raise HandoverError('只能发送本人创建的交接单。',403)
            if preview['recipient_id']:
                target = self.actor_by_person(preview['recipient_id'])
        with self.connect(True) as db:
            data = self._read(db, identity)
            if action == 'delete_draft' and data['status'] == 'deleted' and actor['id'] == data['owner']['id']:
                return {'id': identity, 'deleted': True}
            self._access(data, actor)
            owner = actor['id'] == data['owner']['id']
            recipient = actor['id'] == data['recipient_id']
            if action in {'accept','return'}:
                if not recipient or token and not secrets.compare_digest(token, str(data.get('card_token',''))):
                    raise HandoverError('只有本次指定接收人能确认交接。',403)
                if data.get('sent_version') == version and data['status'] == ('accepted' if action=='accept' else 'returned'):
                    return self._public(db, data, actor)
            if data['version'] != version:
                raise HandoverError('交接内容已变化，请打开最新交接单核对。',409)
            if action == 'delete_draft':
                if not owner:
                    raise HandoverError('只能删除本人创建的草稿。', 403)
                if data['status'] != 'draft' or data.get('sent_version') or db.execute(
                        'SELECT 1 FROM work_handover_versions WHERE handover_id=? LIMIT 1', (identity,)).fetchone():
                    raise HandoverError('仅未发送的交接草稿可删除，已发送的交接和历史记录保留。', 409)
                data.update(status='deleted', version=data['version'] + 1, deleted_at=timestamp())
                db.execute('UPDATE work_handover_files SET visible=0 WHERE handover_id=?', (identity,))
                self._save(db, data, actor, '删除草稿')
                return {'id': identity, 'deleted': True}
            if action == 'send':
                if not owner or data['status'] not in {'draft','returned','withdrawn'}:
                    raise HandoverError('当前交接单不能重复发送。',409)
                if not data['items'] or not data['recipient_id']:
                    raise HandoverError('请选择接收人并添加交接事项。')
                if not target or target['id'] != data['recipient_id']:
                    raise HandoverError('接收人已变更，请重新核对交接单。',409)
                self.check_actor(target)
                if any(not authorized(target, i['scope'], i.get('module','')) or not authorized(actor,i['scope'],i.get('module','')) for i in data['items']):
                    raise HandoverError('双方的楼栋或业务权限不足，请先由管理员配置对应权限。',403)
                self._check_conflicts(db,identity,data['items'],actor)
                data.update(status='pending', card_token=secrets.token_urlsafe(32), sent_snapshot=copy.deepcopy(data['items']), sent_at=timestamp())
                data['version'] += 1
                data['sent_version'] = data['version']
                archive={k:v for k,v in data.items() if k not in {'card_token','sent_snapshot'}}
                archive['files']=[dict(r) for r in db.execute('SELECT id,name,size,time FROM work_handover_files WHERE handover_id=? AND visible=1',(identity,))]
                db.execute('INSERT INTO work_handover_versions VALUES(?,?,?)',(identity,data['version'],json.dumps(archive,ensure_ascii=False)))
                self._queue(db,data,'invite',data['recipient_id'])
            elif action == 'withdraw':
                if not owner or data['status'] != 'pending':
                    raise HandoverError('只能撤回本人尚未被接收的交接单。',409)
                data['status'] = 'withdrawn'
                data['version'] += 1
                self._queue(db,data,'refresh',data['recipient_id'])
            elif action in {'accept','return'}:
                if not recipient or data['status'] != 'pending':
                    raise HandoverError('交接单已处理，不能重复确认。',409)
                if action == 'accept' and any(not authorized(actor,i['scope'],i.get('module','')) for i in data['items']):
                    raise HandoverError('当前权限不足，不能接收全部交接事项，请联系管理员。',403)
                if action == 'accept':
                    self._check_conflicts(db,identity,data['items'],actor)
                if action == 'return' and not reason:
                    raise HandoverError('请填写需要补充的内容。')
                data.update(status='accepted' if action=='accept' else 'returned', decision_at=timestamp(), decision_by=actor['name'], decision_reason=reason)
                data['version'] += 1
                self._queue(db,data,'result',data['owner']['id'])
                self._queue(db,data,'refresh',data['recipient_id'])
            elif action in {'complete','reopen','remove','restore'}:
                if not recipient or data['status'] != 'accepted':
                    raise HandoverError('接收后由接收人维护接续清单。',403)
                item = next((i for i in data['items'] if i['id']==item_id),None)
                if not item or not authorized(actor,item['scope'],item.get('module','')):
                    raise HandoverError('事项不存在或没有访问权限。',403)
                if action in {'complete','reopen'}:
                    if item['kind'] not in {'ongoing','todo'} or item.get('removed'):
                        raise HandoverError('仅待处理事项需要确认完成。')
                    item.update(done=action=='complete',confirmed_at=timestamp(),confirmed_by=actor['name'])
                else:
                    item.update(removed=action=='remove',removed_at=timestamp(),removed_by=actor['name'])
                if action in {'reopen','restore'}:
                    self._check_conflicts(db,identity,[item],actor)
                data['version'] += 1
            elif action == 'retry-notification':
                updated = db.execute("UPDATE work_handover_outbox SET state='queued',attempts=0,next_at=0,error='' WHERE handover_id=? AND json_extract(payload,'$.sent_version')=? AND state='failed'",(identity,data.get('sent_version',0)))
                if not updated.rowcount:
                    raise HandoverError('当前没有失败通知需要重试，请刷新查看最新状态。',409)
            else:
                raise HandoverError('不支持的交接操作。')
            labels={'send':'发送交接','withdraw':'撤回交接','accept':'确认接收','return':'退回补充','complete':'确认事项完成','reopen':'恢复未完成','remove':'移除事项','restore':'恢复事项','retry-notification':'重试飞书通知'}
            self._save(db,data,actor,labels[action],{'item_id':item_id,'reason':reason})
            return self._public(db,data,actor)

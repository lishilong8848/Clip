"""Risk inspection cloud adapter. Uses only the separately configured agent app."""
from copy import deepcopy
import hashlib
import io
import json
from pathlib import Path
import re
import threading
import time
import uuid

import httpx

from upload_event_module.services.http_client import FeishuHttpClient

from .risk_inspection import RiskError, SCOPES, SYSTEM_NAMES, can_scope, digest, manager, schema_fields, text

API = 'https://open.feishu.cn/open-apis/'
TEMPLATE_NODE = 'VOSVwomkaiFM3OkSXU5csVU8nBe'
TEMPLATE_APP = 'DnkxbUBfUaoGSnsTDZjcdEjpnuh'
TEMPLATE_TABLE = 'tbldVIlNetjlCkuI'
REGISTRY_APP = 'MliKbC3fXa8PXrsndKscmxjdn1g'
REGISTRY_TABLE = 'tblbmtDSsRksyhYb'
KEY = '灯塔排查标识'
HISTORY_TABLE_NAME = '风险排查模板库'
HISTORY_FIELDS = [{'field_name': name, 'type': kind} for name, kind in (
    ('模板编号', 1), ('模板名称', 1), ('版本', 2), ('来源发布', 1), ('来源结果表链接', 1),
    ('创建人', 1), ('创建时间', 5), ('更新时间', 5), ('启用状态', 1), ('适用楼栋', 1),
    ('配置快照', 17), ('快照摘要', 1), (KEY, 1))]
STATUS = {'pending': '未提交', 'submitted': '待审核', 'approved': '审核通过', 'returned': '退回修改', 'revoked': '已撤销'}


class CloudRejected(RiskError):
    """A definitive rejection, unlike an unknown transport or response outcome."""


def field_spec(field):
    result = {'field_name': field['field_name'], 'type': field['type']}
    prop = deepcopy(field.get('property') or {})
    if field['type'] in {3, 4}:
        prop = {'options': [{'name': o['name']} for o in prop.get('options', [])]}
    else:
        prop = {k: v for k, v in prop.items() if k in {'date_formatter', 'formatter', 'multiple', 'auto_fill'}}
    if prop:
        result['property'] = prop
    return result


class RiskCloud:
    def __init__(self, store, config_path, recipient, base_url):
        self.store, self.config_path = store, Path(config_path)
        self.recipient, self.base_url = recipient, base_url
        self.http = FeishuHttpClient(timeout=httpx.Timeout(30, connect=10), retries=0)
        self.token, self.expiry, self.config_hash = '', 0, ''
        self.lock = threading.RLock()
        self.work = threading.local()
        self.identities, self.identity_expiry = {}, 0

    def close(self):
        self.http.close()

    def identity(self, person):
        if not str(person['id']).startswith('rec'):
            return self.recipient(person)  # Shared duty accounts have no HR row.
        from .portal_service import MaintenancePortalService, PERMISSION_DIRECTORY_APP_TOKEN, PERMISSION_DIRECTORY_TABLE_ID
        with self.lock:
            if time.time() >= self.identity_expiry:
                identities, seen, seen_rows, cursor = {}, set(), set(), ''
                path = self.table_path(PERMISSION_DIRECTORY_APP_TOKEN, PERMISSION_DIRECTORY_TABLE_ID) + '/records/search'
                for _ in range(20):
                    params = {'user_id_type': 'union_id', 'page_size': 500}
                    if cursor:
                        params['page_token'] = cursor
                    data = self.request('POST', path, params=params, json={'field_names': ['员工姓名', '离职/异动情况']})
                    if not isinstance(data.get('items'), list) or type(data.get('has_more')) is not bool:
                        raise RiskError('飞书人员身份目录读取不完整，未发送消息。', 503)
                    for row in data['items']:
                        rid = row.get('record_id')
                        if not isinstance(rid, str) or rid in seen_rows:
                            raise RiskError('人员目录出现重复或无效记录，未使用不完整身份信息。', 503)
                        seen_rows.add(rid)
                        fields = row.get('fields') or {}
                        if MaintenancePortalService._signature_person_inactive(fields.get('离职/异动情况')):
                            continue
                        users = fields.get('员工姓名') or []
                        if isinstance(users, list) and len(users) == 1 and isinstance(users[0], dict) and re.fullmatch(r'on_[A-Za-z0-9]+', str(users[0].get('id', ''))):
                            identities[rid] = users[0]['id']
                    if not data['has_more']:
                        break
                    cursor = data.get('page_token')
                    if not cursor or cursor in seen:
                        raise RiskError('人员目录分页异常，暂不发送消息。', 503)
                    seen.add(cursor)
                else:
                    raise RiskError('人员目录未读取完成。', 503)
                from collections import Counter
                counts = Counter(identities.values())
                self.identities = {rid: uid for rid, uid in identities.items() if counts[uid] == 1}
                self.identity_expiry = time.time() + 300
            value = self.identities.get(person['id'])
            if not value:
                raise RiskError('人员表中未找到唯一的在岗飞书身份：' + person['name'] + '。请核对员工姓名人员字段。', 409)
            return value

    def headers(self):
        from openclaw_service.assistant.lighthouse_ai import unprotect_key
        config = json.loads(self.config_path.read_text(encoding='utf-8-sig'))
        if config.get('app_id') != 'cli_aa42dc770a381cb3' or not config.get('secret_cipher'):
            raise CloudRejected('风险排查需要已配置的飞书智能体应用；不会回退使用登录应用。', 503)
        signature = digest([config['app_id'], config['secret_cipher']])
        if self.token and self.expiry > time.time() and self.config_hash == signature:
            return {'Authorization': 'Bearer ' + self.token}
        reply = self.http.request('POST', API + 'auth/v3/tenant_access_token/internal', json={
            'app_id': config['app_id'], 'app_secret': unprotect_key(config['secret_cipher'])}, timeout=httpx.Timeout(25, connect=10)).json()
        if reply.get('code') or not reply.get('tenant_access_token'):
            raise CloudRejected('飞书智能体身份验证失败，请检查应用配置。', 503)
        self.token, self.expiry, self.config_hash = reply['tenant_access_token'], time.time() + reply.get('expire', 7200) - 90, signature
        return {'Authorization': 'Bearer ' + self.token}

    def request(self, method, path, **kwargs):
        if method != 'GET' and not path.endswith('/records/search'):
            from .portal_service import external_real_write_guard
            if not external_real_write_guard()['real_write_allowed']:
                raise CloudRejected('隔离测试模式不写入真实飞书数据。', 403)
        with self.lock:
            job = getattr(self.work, 'job', None)
            if job:
                self.store.renew_job(job)
            response = self.http.request(method, API + path, headers=self.headers(), **kwargs)
            if job:
                self.store.renew_job(job)
            if response.status_code in {400, 401, 403, 404, 405, 413, 415, 422, 429}:
                if response.status_code == 401:
                    self.expiry = 0
                raise CloudRejected(f'飞书拒绝请求（HTTP {response.status_code}），请核对权限、参数或稍后重试。', 503)
            response.raise_for_status()
            result = response.json()
            if result.get('code'):
                if result.get('code') in {99991663, 99991664, 99991668}:
                    self.expiry = 0
                raise CloudRejected('飞书未完成请求（代码 ' + str(result.get('code')) + '），请检查应用权限或稍后重试。', 503)
            data = result.get('data', {} if method == 'DELETE' else None)
            if not isinstance(data, dict):
                raise RiskError('飞书返回不完整，保留任务等待核对。', 503)
            return data

    def all(self, path, **params):
        rows, seen = [], set()
        for _ in range(200):
            data = self.request('GET', path, params={'page_size': 50, **params})
            if not isinstance(data.get('items'), list) or type(data.get('has_more')) is not bool:
                raise RiskError('飞书列表未完整返回，已停止操作。', 503)
            rows.extend(data['items'])
            if not data['has_more']:
                return rows
            cursor = data.get('page_token')
            if not cursor or cursor in seen:
                break
            seen.add(cursor)
            params['page_token'] = cursor
        raise RiskError('飞书列表分页异常，不能使用不完整数据。', 503)

    @staticmethod
    def table_path(app, table):
        return f'bitable/v1/apps/{app}/tables/{table}'

    def read_template(self):
        with self.lock:
            fields = self.all(self.table_path(TEMPLATE_APP, TEMPLATE_TABLE) + '/fields')
            previous = self.store.template() or {}
            settings = previous.get('settings', {})
            data = {'fields': schema_fields(fields, settings), 'settings': settings, 'read_at': time.time()}
            data['version'] = digest(data['fields'])
            self.store.save_template(data)
            return data

    def edit_field(self, actor, body):
        if actor.get('level') not in {'L0', 'L1'}:
            raise RiskError('公共发布模板仅全楼管理员可修改。', 403)
        if not isinstance(body, dict) or set(body) - {'version', 'action', 'field_id', 'field_name', 'type', 'options', 'required', 'role', 'confirmed'}:
            raise RiskError('字段参数无效。')
        with self.lock:
            current = self.read_template()
            if current['version'] != body.get('version'):
                raise RiskError('模板已被修改，请重新读取。', 409)
            action, fid = body.get('action'), body.get('field_id')
            old = next((f for f in current['fields'] if f['key'] == fid), None)
            path = self.table_path(TEMPLATE_APP, TEMPLATE_TABLE) + '/fields'
            if action not in {'add', 'update', 'delete'} or action != 'add' and not old:
                raise RiskError('字段不存在或操作无效。')
            if action == 'delete':
                if old.get('is_primary'):
                    raise RiskError('不能删除多维表主字段。')
                if body.get('confirmed') is not True:
                    raise RiskError('删除会移除模板中的该列及其数据，请确认后继续。')
                self.request('DELETE', path + '/' + fid)
                current['settings'].pop(fid, None)
            else:
                name, kind = text(body.get('field_name'), 80, True), body.get('type')
                if type(kind) is not int or kind not in {1, 2, 3, 4, 5, 7, 13, 15, 17}:
                    raise RiskError('请选择支持的字段类型。')
                if name.startswith('灯塔') or any(f['field_name'] == name and f['key'] != fid for f in current['fields']):
                    raise RiskError('字段名重复或使用了系统保留名称。')
                if old and old.get('is_primary') and kind != 1:
                    raise RiskError('主字段需保留文本类型。')
                if old and old['type'] != kind and body.get('confirmed') is not True:
                    raise RiskError('更改类型可能转换或清空模板数据，请确认后继续。')
                role = body.get('role', 'input')
                if not isinstance(role, str) or role not in {'input', *SYSTEM_NAMES.values()}:
                    raise RiskError('字段用途无效。')
                if role != 'input' and kind != (5 if role == 'reviewed_at' else 1):
                    raise RiskError('系统字段需使用文本类型，复核日期需使用日期类型。')
                spec = {'field_name': name, 'type': kind}
                if kind in {3, 4}:
                    options = body.get('options', [])
                    if not isinstance(options, list) or not 1 <= len(options) <= 100:
                        raise RiskError('请选择字段至少设置一个选项。')
                    options = [text(x, 100, True) for x in options]
                    if len(set(options)) != len(options):
                        raise RiskError('选项不能重复。')
                    spec['property'] = {'options': [{'name': x} for x in options]}
                elif kind == 5:
                    spec['property'] = {'date_formatter': 'yyyy/MM/dd', 'auto_fill': False}
                if action == 'add':
                    result = self.request('POST', path, json=spec)
                    fid = result['field']['field_id']
                else:
                    self.request('PUT', path + '/' + fid, json=spec)
                current['settings'][fid] = {'role': role, 'required': body.get('required') is True}
            self.store.save_template(current)
            return self.read_template()

    def ensure_fields(self, app, table, specs):
        path = self.table_path(app, table) + '/fields'
        present = {f['field_name']: f for f in self.all(path)}
        for spec in specs:
            old = present.get(spec['field_name'])
            if old and old['type'] != spec['type']:
                raise RiskError('云端字段类型不一致：' + spec['field_name'] + '。未覆盖原数据。', 409)
            if not old:
                result = self.request('POST', path, json=spec)
                present[spec['field_name']] = result['field']
        return present

    def records(self, app, table):
        return self.all(self.table_path(app, table) + '/records', user_id_type='union_id')

    @staticmethod
    def field_text(value):
        return ''.join(str(v.get('text', '')) for v in value) if isinstance(value, list) else str(value or '')

    def upsert(self, app, table, key, fields):
        path = self.table_path(app, table) + '/records'
        result = self.request('POST', path + '/search', params={'user_id_type': 'union_id', 'page_size': 20},
                              json={'filter': {'conjunction': 'and', 'conditions': [{'field_name': KEY, 'operator': 'is', 'value': [key]}]}})
        if not isinstance(result.get('items'), list) or result.get('has_more') is not False:
            raise RiskError('云端排查标识检索不完整，停止写入。', 503)
        records = result['items']
        if any(self.field_text(r.get('fields', {}).get(KEY)) != key for r in records):
            raise RiskError('云端标识检索结果不一致，停止写入。', 409)
        if len(records) > 1:
            raise RiskError('云端存在重复排查标识，已停止写入，请管理员核对。', 409)
        params = {'user_id_type': 'union_id'}
        if records:
            return self.request('PUT', path + '/' + records[0]['record_id'], params=params, json={'fields': {KEY: key, **fields}})['record']['record_id']
        params['client_token'] = str(uuid.uuid5(uuid.NAMESPACE_URL, app + ':' + table + ':' + key))
        return self.request('POST', path, params=params, json={'fields': {KEY: key, **fields}})['record']['record_id']

    def create_release(self, release, people):
        owner = self.store.person_for(release['owner']['id'])
        if not manager(owner) or not all(can_scope(owner, p['scope']) for p in people):
            raise RiskError('发布人权限已变化，暂停发布，请管理员核对。', 403)
        reviewer = self.store.person_for(release['reviewer']['id'])
        if not manager(reviewer) or not all(can_scope(reviewer, p['scope']) for p in people):
            raise RiskError('审核人权限已变化，暂停发布，请管理员核对。', 403)
        for person in people:
            if not can_scope(self.store.person_for(person['id']), person['scope']):
                raise RiskError('接收人权限已变化，暂停发布，请管理员核对。', 403)
        cloud = release['cloud']
        rid = release['id']
        specs = [field_spec(f) for f in sorted(release['fields'], key=lambda f: not f.get('is_primary'))] + [
            {'field_name': n, 'type': t} for n, t in [(KEY, 1), ('灯塔排查事项', 1), ('灯塔人员标识', 1), ('灯塔审核状态', 1), ('灯塔审核意见', 1), ('灯塔提交时间', 5)]]
        if not cloud.get('table'):
            path = f'bitable/v1/apps/{TEMPLATE_APP}/tables'
            # The visible release suffix makes a timed-out create discoverable without guessing by title.
            title = release['name'] + ' · ' + rid[:12]
            candidates = [table for table in self.all(path) if table.get('name') == title]
            if len(candidates) > 1 or candidates and candidates[0]['table_id'] == TEMPLATE_TABLE:
                raise RiskError('多维表内存在同名冲突，暂停创建。', 409)
            if candidates and not cloud.get('create_attempted'):
                raise RiskError('同名结果表不是本次创建操作的回执，未写入该表，请核对名称。', 409)
            if not candidates:
                if cloud.get('create_attempted'):
                    raise RiskError('上次建表结果尚未确认，未再次创建；请管理员核对多维表内的数据表后联系维护人员处理。', 409)
                self.store.checkpoint(rid, create_attempted=True)
                try:
                    target = self.request('POST', path, json={'table': {'name': title, 'default_view_name': '排查反馈', 'fields': specs}})
                except CloudRejected:
                    # A rejected API response confirms no table was created; transport failures do not.
                    self.store.checkpoint(rid, create_attempted=False)
                    raise
            else:
                target = candidates[0]
            cloud.update(app=TEMPLATE_APP, table=target['table_id'], table_name=title,
                         url='https://vnet.feishu.cn/wiki/' + TEMPLATE_NODE + '?table=' + target['table_id'])
            if target.get('default_view_id'):
                cloud['url'] += '&view=' + target['default_view_id']
            self.store.checkpoint(rid, **cloud)
        if cloud['app'] != TEMPLATE_APP or cloud['table'] == TEMPLATE_TABLE:
            raise RiskError('发布目标不能指向模板表或其它多维表。', 409)
        self.ensure_fields(cloud['app'], cloud['table'], specs)
        self.registry(release, people)
        self.store.activate(rid)

    def registry(self, release, people, finished_job=''):
        specs = [{'field_name': n, 'type': t} for n, t in [(KEY, 1), ('灯塔审核人', 1), ('灯塔提交人数', 2), ('灯塔通过人数', 2), ('灯塔任务状态', 1), ('灯塔截止时间', 5), ('来源模板编号', 1), ('来源模板版本', 2), ('历史模板编号', 1)]]
        self.ensure_fields(REGISTRY_APP, REGISTRY_TABLE, specs)
        active = [p for p in people if not p['revoked'] and any(i['status'] != 'revoked' for i in p['items'])]
        items = [i for p in active for i in p['items'] if i['status'] != 'revoked']
        approved = sum(i['status'] == 'approved' for i in items)
        submitted = sum(any(i['status'] in {'submitted', 'approved'} for i in p['items']) for p in active)
        completed = [p for p in active if all(i['status'] in {'approved', 'revoked'} for i in p['items'])]
        scopes = sorted({p['scope'] for p in active}, key=lambda s: SCOPES.index(s))
        label = lambda s: '110站' if s == '110' else s + '楼'
        # User fields use union_id, never an open_id from another application.
        identities = dict(release['cloud'].get('identities', {}))
        for person in [release['owner'], *active]:
            if person['id'] not in identities:
                identities[person['id']] = self.identity(person)
        if identities != release['cloud'].get('identities'):
            self.store.checkpoint(release['id'], identities=identities)
        recipients = [{'id': identities[p['id']]} for p in active]
        with self.store.connect() as db:
            pending = db.execute("SELECT 1 FROM risk_jobs WHERE release_id=? AND kind='sync' AND status!='done' AND id!=? LIMIT 1", (release['id'], finished_job)).fetchone()
        self.upsert(REGISTRY_APP, REGISTRY_TABLE, release['id'], {
            '文本': release['name'], '排查表名': release['cloud'].get('table_name', release['name']), '链接': release['cloud']['url'],
            '排查发布时间': int(release['created'] * 1000), '发布人': [{'id': identities[release['owner']['id']]}],
            '现场排查人': recipients, '楼栋': [label(s) for s in scopes],
            '任务完成情况': approved / len(items) if items else 0,
            '已完成楼栋': [label(s) for s in scopes if all(p in completed for p in active if p['scope'] == s)],
            '灯塔审核人': release['reviewer']['name'], '灯塔提交人数': submitted, '灯塔通过人数': len(completed),
            '来源模板编号': (release.get('source_template') or {}).get('id', ''),
            '来源模板版本': (release.get('source_template') or {}).get('revision'), '历史模板编号': release['id'],
            '灯塔任务状态': '结果待同步' if pending else '已撤销' if not items else '已完成' if approved == len(items) else '进行中', '灯塔截止时间': release['deadline']})

    def history_table(self):
        with self.lock:
            catalog = self.store.history.catalog()
            table = catalog.get('table')
            if not table:
                path = f'bitable/v1/apps/{TEMPLATE_APP}/tables'
                matches = [t for t in self.all(path) if t.get('name') == HISTORY_TABLE_NAME]
                if len(matches) > 1:
                    raise RiskError('存在多个同名历史模板库，请管理员核对。', 409)
                if matches:
                    table = matches[0]['table_id']
                    fields = self.all(self.table_path(TEMPLATE_APP, table) + '/fields')
                    expected = {f['field_name']: f['type'] for f in HISTORY_FIELDS}
                    if any({f['field_name']: f['type'] for f in fields}.get(k) != expected[k] for k in (KEY, '模板编号', '配置快照')):
                        raise RiskError('同名数据表不是灯塔模板库，未改动它，请先核对名称。', 409)
                else:
                    if catalog.get('create_attempted'):
                        raise RiskError('模板库创建结果尚未确认，未重复创建，请核对多维表。', 409)
                    self.store.history.catalog(create_attempted=True)
                    try:
                        result = self.request('POST', path, json={'table': {'name': HISTORY_TABLE_NAME, 'default_view_name': '全部历史模板', 'fields': HISTORY_FIELDS}})
                    except CloudRejected:
                        self.store.history.catalog(create_attempted=False)
                        raise
                    table = result['table_id']
                self.store.history.catalog(table=table)
            if table == TEMPLATE_TABLE:
                raise RiskError('历史模板库不能指向基础模板。', 409)
            self.ensure_fields(TEMPLATE_APP, table, HISTORY_FIELDS)
            return table

    def sync_history(self, job):
        history = self.store.history
        data = history.raw(job['payload']['template_id'])
        if data['revision'] != job['payload']['revision']:
            return  # A queued old edit must not overwrite a newer template version.
        actor = self.store.person_for(data['updated_by'])
        if not history.allowed(actor, data):
            raise RiskError('模板修改人权限已变化，定义保留，请有权限的管理员核对后保存。', 403)
        table = self.history_table()
        snapshot = {k: data[k] for k in ('id', 'name', 'revision', 'enabled', 'source_release_id', 'fields', 'items', 'scopes', 'created', 'creator', 'legacy')}
        content = json.dumps({'format': 'lighthouse-risk-template-v1', **snapshot}, ensure_ascii=False, sort_keys=True).encode('utf-8')
        if len(content) > 2 * 1024 * 1024:
            raise RiskError('模板快照超过2MB，请精简预填内容后重新保存。', 413)
        checksum = hashlib.sha256(content).hexdigest()
        token = data['cloud'].get('file_token') if data['cloud'].get('checksum') == checksum else None
        if not token:
            name = f'risk-template-{data["id"]}-v{data["revision"]}.json'
            result = self.request('POST', 'drive/v1/medias/upload_all', data={'file_name': name, 'parent_type': 'bitable_file', 'parent_node': TEMPLATE_APP, 'size': str(len(content))}, files={'file': (name, io.BytesIO(content), 'application/json')})
            token = result['file_token']
            history.checkpoint(data['id'], file_token=token, checksum=checksum)
        if history.raw(data['id'])['revision'] != data['revision']:
            return
        record = self.upsert(TEMPLATE_APP, table, data['id'], {
            '模板编号': data['id'], '模板名称': data['name'], '版本': data['revision'],
            '来源发布': data['source_name'], '来源结果表链接': data['source_url'],
            '创建人': data['creator']['name'], '创建时间': int(data['created']*1000), '更新时间': int(data['updated']*1000),
            '启用状态': '启用' if data['enabled'] else '停用', '适用楼栋': '、'.join(data['scopes']),
            '配置快照': [{'file_token': token}], '快照摘要': checksum})
        history.checkpoint(data['id'], revision=data['revision'], record_id=record, table=table)

    def attachment(self, release, fid):
        cloud = self.store.raw(release['id'])[0]['cloud']
        tokens = dict(cloud.get('files', {}))
        if fid in tokens:
            return {'file_token': tokens[fid]}
        with self.store.connect() as db:
            row = db.execute('SELECT name,size FROM risk_files WHERE id=? AND release_id=?', (fid, release['id'])).fetchone()
        if not row:
            raise RiskError('附件元数据不存在，停止同步。', 409)
        path = self.store.db_path.parent / 'risk_inspection_files' / fid
        if not path.is_file() or path.stat().st_size != row['size']:
            raise RiskError('本地附件不完整，反馈已保留，请联系管理员。', 409)
        with path.open('rb') as stream:
            result = self.request('POST', 'drive/v1/medias/upload_all', data={'file_name': row['name'], 'parent_type': 'bitable_file', 'parent_node': release['cloud']['app'], 'size': str(row['size'])}, files={'file': (row['name'], stream, 'application/octet-stream')})
        tokens[fid] = result['file_token']
        self.store.checkpoint(release['id'], files=tokens)
        return {'file_token': tokens[fid]}

    def sync(self, release, person):
        for item in person['items']:
            if item['status'] == 'pending':
                continue
            key = release['id'] + ':' + person['id'] + ':' + item['id']
            values = {}
            system = {'number': key, 'status': STATUS[item['status']], 'scope': person['scope'] + ('站' if person['scope'] == '110' else '楼'),
                      'person': person['name'], 'reviewer': item.get('reviewed_by', ''),
                      'review': (STATUS[item['status']] + ('：' + item['reason'] if item.get('reason') else '')) if item.get('reviewed_at') else '',
                      'reviewed_at': int(item.get('reviewed_at', 0) * 1000) or None, 'sync': '已落表'}
            for field in release['fields']:
                value = system.get(field['role']) if field['role'] != 'input' else item['values'].get(field['key'])
                if field['role'] == 'input' and value is not None:
                    if field['type'] == 17:
                        value = [self.attachment(release, fid) for fid in value]
                    elif field['type'] == 15:
                        value = {'link': value, 'text': value}
                values[field['field_name']] = value
            values.update({'灯塔排查事项': item['title'], '灯塔人员标识': person['id'], '灯塔审核状态': STATUS[item['status']],
                           '灯塔审核意见': item.get('reason', ''), '灯塔提交时间': int(item.get('submitted_at', 0) * 1000) or None})
            self.upsert(release['cloud']['app'], release['cloud']['table'], key, values)

    def process(self, job):
        self.work.job = job if job.get('status') == 'running' else None
        try:
            if self.work.job:
                self.store.renew_job(job)
            self._process(job)
        finally:
            self.work.job = None

    def _process(self, job):
        if job['kind'] == 'template_sync':
            self.sync_history(job)
            return
        release, people = self.store.raw(job['release_id'])
        if job['kind'] == 'publish':
            self.create_release(release, people)
        elif job['kind'] == 'registry':
            actor = self.store.person_for(job['payload']['actor']['id'])
            if not manager(actor) or not all(can_scope(actor, p['scope']) for p in people):
                raise RiskError('操作人权限已变化，暂未更新登记信息。', 403)
            self.registry(release, people)
        elif job['kind'] == 'sync':
            actor = self.store.person_for((job['payload'].get('retry_authorized_by') or job['payload']['actor'])['id'])
            person = job['payload']['person']
            if not can_scope(actor, person['scope']) or actor['id'] != person['id'] and not manager(actor):
                raise RiskError('原操作人楼栋权限已撤销，反馈保留，待管理员核对。', 403)
            self.sync(release, person)
            self.registry(*self.store.raw(release['id']), finished_job=job['id'])
        elif job['kind'] == 'notify':
            person = next(p for p in people if p['id'] == job['payload']['person_id'])
            target = self.store.person_for(person['id'])
            if person['revoked'] or not any(i['status'] in {'pending', 'returned'} for i in person['items']):
                return
            if not can_scope(target, person['scope']):
                raise RiskError('接收人权限已变化，暂不发送排查消息。', 403)
            self.notify(job, release, target)
        elif job['kind'] == 'review_notice':
            payload = job['payload']
            if payload['action'] == 'submit' and payload['person_id'] != release['reviewer']['id']:
                return
            person = next(p for p in people if p['id'] == payload['subject_id'])
            item = next(i for i in person['items'] if i['id'] == payload['item_id'])
            expected = {'submit': 'submitted', 'return': 'returned', 'approve': 'approved'}[payload['action']]
            if person['revoked'] or item.get('notice_version', person['version']) != payload['version'] or item['status'] != expected:
                return
            target = self.store.person_for(payload['person_id'])
            if not can_scope(target, person['scope']) or payload['action'] == 'submit' and not manager(target):
                raise RiskError('通知接收人权限已变化。', 403)
            self.notify(job, release, target, note=person['name'] + ' · ' + item['title'] + '：' + STATUS[item['status']])

    def notify(self, job, release, target, note=''):
        cloud = self.store.raw(release['id'])[0]['cloud']
        receipts = dict(cloud.get('messages', {}))
        receipt_key = job['id']
        if receipts.get(receipt_key, {}).get('message_id'):
            return
        recipient = self.identity(target)
        url = self.base_url().rstrip('/') + '/risk-inspections?id=' + release['id']
        attempted_before = bool(receipts.get(receipt_key))
        attempt = receipts.get(receipt_key, {}).get('attempted_at', time.time())
        if time.time() - attempt > 20 * 3600:
            raise RiskError('消息投递结果需人工核验，已停止自动重发以避免重复通知。', 409)
        receipts[receipt_key] = {'attempted_at': attempt}
        self.store.checkpoint(release['id'], messages=receipts)
        card = {'config': {'wide_screen_mode': True}, 'header': {'title': {'tag': 'plain_text', 'content': '风险排查 · ' + release['name']}, 'template': 'blue'},
                'elements': [{'tag': 'div', 'text': {'tag': 'plain_text', 'content': note or '请完成分配给你的排查事项，并在灯塔提交反馈供审核。'}},
                             {'tag': 'action', 'actions': [{'tag': 'button', 'type': 'primary', 'text': {'tag': 'plain_text', 'content': '查看排查' if note else '开始排查'}, 'url': url}]}]}
        try:
            result = self.request('POST', 'im/v1/messages', params={'receive_id_type': 'union_id'}, json={'receive_id': recipient, 'msg_type': 'interactive', 'content': json.dumps(card, ensure_ascii=False), 'uuid': hashlib.sha256(job['id'].encode()).hexdigest()[:32]})
        except CloudRejected:
            if not attempted_before:
                receipts.pop(receipt_key, None)
                self.store.checkpoint(release['id'], messages=receipts)
            raise
        if not result.get('message_id'):
            raise RiskError('飞书未返回消息回执，不能标记发送成功。', 503)
        receipts[receipt_key].update(message_id=result['message_id'], sent_at=time.time())
        self.store.checkpoint(release['id'], messages=receipts)

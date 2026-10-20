"""Risk inspection cloud adapter using the agent credential mirror of Qt settings."""
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
from upload_event_module.services.feishu_service import _stable_uuid4_client_token

from .risk_inspection import (RiskError, SCOPES, SYSTEM_NAMES, can_scope, completion_key,
                              digest, final_manager, item_scope, manager, reviewer_list, scoped_people,
                              schema_fields, system_fields, system_values, text)
from .risk_inspection_templates import clean_fields

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
    ('配置快照', 17), ('快照摘要', 1), ('模板类型', 1), (KEY, 1))]
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


def result_specs(release):
    return [field_spec(f) for f in sorted(system_fields(release['fields']), key=lambda f: not f.get('is_primary'))] + [
        {'field_name': n, 'type': t} for n, t in ((KEY, 1), ('灯塔排查事项', 1), ('灯塔人员标识', 1),
                                                ('灯塔审核状态', 1), ('灯塔审核意见', 1), ('灯塔提交时间', 5))]


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
        from openclaw_service.assistant.lighthouse_ai import AssistantError, unprotect_key
        from .feishu_assistant_settings import read_config, validate_app_id
        try:
            config, revision = read_config(self.config_path)
            validate_app_id(config.get('app_id'))
        except AssistantError as exc:
            raise CloudRejected(str(exc), 503) from None
        if not re.fullmatch(r'cli_[A-Za-z0-9]+', str(config.get('app_id', ''))) or not config.get('secret_cipher'):
            raise CloudRejected('请在 Qt 设置的飞书鉴权中配置应用；风险排查不使用独立或备用应用。', 503)
        signature = digest([config['app_id'], config['secret_cipher']])
        if self.token and self.expiry > time.time() and self.config_hash == signature:
            return {'Authorization': 'Bearer ' + self.token}
        reply = self.http.request('POST', API + 'auth/v3/tenant_access_token/internal', json={
            'app_id': config['app_id'], 'app_secret': unprotect_key(config['secret_cipher'])}, timeout=httpx.Timeout(25, connect=10)).json()
        try:
            current_revision = read_config(self.config_path)[1]
        except AssistantError as exc:
            raise CloudRejected(str(exc), 503) from None
        if current_revision != revision:
            raise CloudRejected('Qt 飞书鉴权已变化，已丢弃旧应用令牌，请重试。', 503)
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
            try:
                result = response.json()
            except (ValueError, UnicodeError):
                result = {}
            if not isinstance(result, dict):
                result = {}
            code = result.get('code')
            if response.status_code in {400, 401, 403, 404, 405, 413, 415, 422, 429} or code and response.status_code < 500:
                if response.status_code == 401:
                    self.expiry = 0
                if code in {99991663, 99991664, 99991668}:
                    self.expiry = 0
                stage = ('发送通知' if path == 'im/v1/messages' else '登记排查任务' if path.startswith(self.table_path(REGISTRY_APP, REGISTRY_TABLE))
                         else '配置数据表字段' if '/fields' in path else '写入排查记录' if '/records' in path else '创建排查表')
                hints = {1254037: '记录幂等标识格式无效，请更新程序后重试原任务。',
                         1254015: '字段类型与填写内容不一致，请核对结果表字段。',
                         1254060: '文本字段收到非文本内容，请核对字段配置。',
                         1254291: '数据表写入冲突，原任务会稍后重试。',
                         99991400: '请求频率受限，原任务会稍后重试。'}
                raise CloudRejected(f'{stage}失败（HTTP {response.status_code}' + (f'，飞书代码 {code}' if code else '') + '）。' +
                                    hints.get(code, '请核对飞书智能体的应用权限和目标表字段；保留原任务重试，不要重新发布。'), 503)
            response.raise_for_status()
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
            data = {'fields': clean_fields(schema_fields(fields, settings)), 'settings': settings, 'read_at': time.time()}
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
            if old and (old['field_name'] in SYSTEM_NAMES or old['role'] != 'input'):
                raise RiskError('固定系统字段不可修改或删除。')
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
                if name in SYSTEM_NAMES or name.startswith('灯塔') or any(f['field_name'] == name and f['key'] != fid for f in current['fields']):
                    raise RiskError('字段名重复或使用了系统保留名称。')
                if old and old.get('is_primary') and kind != 1:
                    raise RiskError('主字段需保留文本类型。')
                if old and old['type'] != kind and body.get('confirmed') is not True:
                    raise RiskError('更改类型可能转换或清空模板数据，请确认后继续。')
                role = body.get('role', 'input')
                if role != 'input':
                    raise RiskError('自定义字段只能用于填写。')
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

    def ensure_result_fields(self, release):
        specs = result_specs(release)
        signature = digest(specs)
        cloud = release['cloud']
        if cloud.get('result_fields') != signature:
            self.ensure_fields(cloud['app'], cloud['table'], specs)
            self.store.checkpoint(release['id'], result_fields=signature)
            cloud['result_fields'] = signature

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
        params['client_token'] = _stable_uuid4_client_token(app + ':' + table + ':' + key)
        return self.request('POST', path, params=params, json={'fields': {KEY: key, **fields}})['record']['record_id']

    def create_release(self, release, people):
        release, people = self.store.raw(release['id'])
        owner = self.store.person_for(release['owner']['id'])
        self.store.check(owner)
        if not manager(owner) or not all(can_scope(owner, item_scope(p, i)) for p in people for i in p['items']):
            raise RiskError('发布人权限已变化，暂停发布，请管理员核对。', 403)
        for reviewer in reviewer_list(release):
            self.store.check(self.store.person_for(reviewer['id']))
        for person in people:
            self.store.check(self.store.person_for(person['id']))
        cloud = release['cloud']
        rid = release['id']
        specs = result_specs(release)
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
        self.ensure_result_fields(release)
        if not cloud.get('initialized'):
            for person in people:
                self.sync(release, person)
            self.store.checkpoint(rid, initialized=True)
            cloud['initialized'] = True
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
        expanded = [p for p in scoped_people(active) if any(i['status'] != 'revoked' for i in p['items'])]
        scopes = sorted({p['scope'] for p in expanded}, key=SCOPES.index)
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
            '已完成楼栋': [label(s) for s in scopes if all(i['status'] in {'approved', 'revoked'} for p in expanded if p['scope'] == s for i in p['items'])],
            '灯塔审核人': '、'.join(p['name'] for p in reviewer_list(release)), '灯塔提交人数': submitted, '灯塔通过人数': len(completed),
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
        snapshot['kind'] = data.get('kind', 'history')
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
            '模板类型': '基础模板' if data.get('kind') == 'base' else '历史模板',
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

    def sync(self, release, person, *, system_only=False):
        fields = system_fields(release['fields'])
        for item in person['items']:
            key = release['id'] + ':' + person['id'] + ':' + item['id']
            values = {}
            system = system_values(release, person, item)
            for field in fields:
                if (system_only or item['status'] == 'pending') and field['role'] == 'input':
                    continue
                value = system.get(field['role']) if field['role'] != 'input' else item['values'].get(field['key'])
                if field['role'] == 'input' and value is not None:
                    if field['type'] == 17:
                        value = [self.attachment(release, fid) for fid in value]
                    elif field['type'] == 15:
                        value = {'link': value, 'text': value}
                values[field['field_name']] = value
            if not system_only:
                values.update({'灯塔排查事项': item['title'], '灯塔人员标识': person['id'], '灯塔审核状态': STATUS[item['status']],
                               '灯塔审核意见': item.get('reason', ''), '灯塔提交时间': int(item.get('submitted_at', 0) * 1000) or None})
            self.upsert(release['cloud']['app'], release['cloud']['table'], key, values)

    def final_cards(self, release, people, target):
        self.store.check(target)
        if not final_manager(target):
            raise RiskError('接收经理身份或权限已变化。', 403)
        selected = next((p for p in release.get('final_managers', []) if p['id'] == target['id']), None)
        people = [p for p in scoped_people(people) if not p['revoked'] and can_scope(target, p['scope'])
                  and (selected is None or p['scope'] in selected['scopes'])
                  and any(i['status'] != 'revoked' for i in p['items'])]
        if release.get('task_grants_version') == 2:
            people.sort(key=lambda p: (SCOPES.index(p['scope']), p['id']))
        total = sum(i['status'] != 'revoked' for p in people for i in p['items'])
        approved = sum(i['status'] == 'approved' for p in people for i in p['items'])
        summary = f'共{len({p["id"] for p in people})}人 · {total}项 · 已复核{approved}项\n' + '\n'.join(
            ('110站' if scope == '110' else scope + '楼') + '：' +
            str(sum(p['scope'] == scope for p in people)) + '人 / ' +
            str(sum(i['status'] != 'revoked' for p in people if p['scope'] == scope for i in p['items'])) + '项'
            for scope in SCOPES if any(p['scope'] == scope for p in people))
        button = {'tag': 'action', 'actions': [{'tag': 'button', 'type': 'primary',
            'text': {'tag': 'plain_text', 'content': '查看排查结果'},
            'url': self.base_url().rstrip('/') + '/risk-inspections?id=' + release['id']}]}

        def card(elements, part, *, last=False, measure=False):
            heading = summary + f'\n明细第{part}片，' + ('本片为末片。' if last else '后续明细见下一片。')
            return {'config': {'wide_screen_mode': True}, 'header': {'title': {'tag': 'plain_text',
                    'content': '风险排查汇总 · ' + release['name']}, 'template': 'green'},
                    'elements': [{'tag': 'div', 'text': {'tag': 'plain_text', 'content': heading}}, *elements,
                                 *([button] if measure or self.store.controls(target, release) else [])]}

        elements, part = [], 1
        fields = [f for f in system_fields(release['fields']) if f['role'] == 'input']
        for person in people:
            for item in person['items']:
                if item['status'] == 'revoked':
                    continue
                feedback = []
                for field in fields:
                    value = item['values'].get(field['key'])
                    if value is None:
                        continue
                    if field['type'] == 17:
                        value = str(len(value)) + '个附件'
                    elif isinstance(value, bool):
                        value = '是' if value else '否'
                    elif isinstance(value, list):
                        value = '、'.join(str(v) for v in value)
                    feedback.append(field['field_name'] + '：' + str(value))
                review = system_values(release, person, item)
                prefix = review['scope'] + ' · ' + person['name'] + '\n事项：' + item['title'] + '\n'
                content = ('反馈：' + ('\n'.join(feedback) or '无填写内容') + '\n复核人：' + review['reviewer'] +
                           '\n复核：' + review['review'])
                # Split long answers as well as large assignments; never silently truncate feedback.
                for start in range(0, len(content), 2400):
                    element = {'tag': 'div', 'text': {'tag': 'plain_text',
                               'content': prefix + ('（接上段）\n' if start else '') + content[start:start + 2400]}}
                    candidate = card([*elements, element], part, measure=True)
                    if elements and (len(elements) >= 20 or len(json.dumps(candidate, ensure_ascii=False).encode('utf-8')) >= 24000):
                        yield card(elements, part)
                        elements, part = [], part + 1
                    elements.append(element)
        yield card(elements, part, last=True)

    def final_push(self, job):
        release, people = self.store.raw(job['release_id'])
        batch = job['payload']['batch']
        if release.get('status') == 'deleted' or not batch or completion_key(people) != batch or not release.get('final_managers'):
            return
        active = [p for p in people if not p['revoked'] and any(i['status'] != 'revoked' for i in p['items'])]
        expanded = [p for p in scoped_people(active) if any(i['status'] != 'revoked' for i in p['items'])]
        owner = self.store.person_for(release['owner']['id'])
        self.store.check(owner)
        if not manager(owner) or not all(can_scope(owner, p['scope']) for p in expanded):
            raise RiskError('发布人权限已变化，暂停最终推送。', 403)
        final = release['cloud'].get('final_push', {})
        if final.get('batch') != batch:
            final = {'batch': batch, 'delivered': [], 'person_versions': {
                p['id']: digest(sorted((i['id'], i.get('reviewed_at')) for i in p['items'] if i['status'] != 'revoked'))
                for p in active}, 'scope_versions': {
                    p['id'] + ':' + p['scope']: digest(sorted((p['scope'], i['id'], i.get('reviewed_at'))
                        for i in p['items'] if i['status'] != 'revoked')) for p in expanded},
                'status': 'pending', 'sent_at': None}
        final = deepcopy(final)
        self.store.checkpoint(release['id'], final_push=final)
        try:
            for selected in release['final_managers']:
                target = self.store.person_for(selected['id'])
                self.store.check(target)
                visible = [p for p in expanded if p['scope'] in selected['scopes']]
                if (not final_manager(target) or not any(can_scope(target, s) for s in selected['scopes'])
                        or not all(can_scope(target, p['scope']) for p in visible)):
                    raise RiskError('接收经理身份或楼栋权限已变化，暂停最终推送。', 403)
                if target['id'] in final['delivered']:
                    continue
                for part, card in enumerate(self.final_cards(release, visible, target), 1):
                    fresh, fresh_people = self.store.raw(release['id'])
                    if fresh.get('status') == 'deleted' or completion_key(fresh_people) != batch:
                        return
                    owner = self.store.person_for(release['owner']['id'])
                    target = self.store.person_for(selected['id'])
                    self.store.check(owner)
                    self.store.check(target)
                    if not manager(owner) or not all(can_scope(owner, p['scope']) for p in expanded):
                        raise RiskError('发布人权限已变化，暂停最终推送。', 403)
                    if (not final_manager(target) or not any(can_scope(target, s) for s in selected['scopes'])
                            or not all(can_scope(target, p['scope']) for p in visible)):
                        raise RiskError('接收经理身份或楼栋权限已变化，暂停最终推送。', 403)
                    if not self.store.controls(target, release):
                        card['elements'] = [e for e in card['elements'] if e['tag'] != 'action']
                    self.notify({**job, 'id': f'{job["id"]}:{target["id"]}:{part}'}, release, target, card=card)
                final['delivered'].append(target['id'])
                self.store.checkpoint(release['id'], final_push=final)
            fresh, fresh_people = self.store.raw(release['id'])
            if fresh.get('status') == 'deleted' or completion_key(fresh_people) != batch:
                return
            final.update(status='sent', sent_at=final.get('sent_at') or time.time())
            self.store.checkpoint(release['id'], final_push=final)
        finally:
            fresh, fresh_people = self.store.raw(release['id'])
            if fresh.get('status') != 'deleted' and completion_key(fresh_people) == batch:
                self.ensure_result_fields(fresh)
                for person in fresh_people:
                    if not person['revoked']:
                        self.sync(fresh, person, system_only=True)

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
        if job['kind'] == 'final_push':
            self.final_push(job)
            return
        release, people = self.store.raw(job['release_id'])
        if job['kind'] == 'publish':
            self.create_release(release, people)
        elif job['kind'] == 'registry':
            actor = self.store.person_for(job['payload']['actor']['id'])
            self.store.check(actor)
            if not self.store.controls(actor, release) or not all(can_scope(actor, item_scope(p, i)) for p in people for i in p['items']):
                raise RiskError('操作人权限已变化，暂未更新登记信息。', 403)
            self.registry(release, people)
            self.ensure_result_fields(release)
            for person in people:
                self.sync(release, person, system_only=True)
        elif job['kind'] == 'sync':
            actor = self.store.person_for((job['payload'].get('retry_authorized_by') or job['payload']['actor'])['id'])
            self.store.check(actor)
            person = job['payload']['person']
            current = next((p for p in people if p['id'] == person['id']), None)
            if (current is None or not person['items']
                    or not all(self.store._visible(actor, release, current, item=i) for i in person['items'])):
                raise RiskError('原操作人楼栋权限已撤销，反馈保留，待管理员核对。', 403)
            self.ensure_result_fields(release)
            self.sync(release, person)
            self.registry(*self.store.raw(release['id']), finished_job=job['id'])
        elif job['kind'] == 'notify':
            person = next(p for p in people if p['id'] == job['payload']['person_id'])
            target = self.store.person_for(person['id'])
            if person['revoked'] or not any(i['status'] in {'pending', 'returned'} for i in person['items']):
                return
            self.store.check(target)
            self.notify(job, release, target)
        elif job['kind'] == 'review_notice':
            payload = job['payload']
            if payload['action'] == 'submit' and (payload['person_id'] not in {p['id'] for p in reviewer_list(release)}
                    or payload.get('reviewer_version', 1) != release.get('reviewer_version', 1)):
                return
            person = next(p for p in people if p['id'] == payload['subject_id'])
            item = next(i for i in person['items'] if i['id'] == payload['item_id'])
            expected = {'submit': 'submitted', 'return': 'returned', 'approve': 'approved'}[payload['action']]
            if person['revoked'] or item.get('notice_version', person['version']) != payload['version'] or item['status'] != expected:
                return
            target = self.store.person_for(payload['person_id'])
            self.store.check(target)
            if not self.store._visible(target, release, person, item=item):
                raise RiskError('通知接收人权限已变化。', 403)
            self.notify(job, release, target, note=item_scope(person, item) + ' · ' + person['name'] + ' · ' + item['title'] + '：' + STATUS[item['status']])

    def notify(self, job, release, target, note='', card=None):
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
        if card is None:
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

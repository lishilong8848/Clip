"""Private assistant materials, never native business forms or live business memory.

Integration: Workspace(store, files).dispatch(actor, payload, *, confirmed=False).
Actor must be freshly authenticated; identity, topic and permissions come ONLY
from actor, never payload. Pass confirmed=True for memory.save/delete only after
a direct UI action or explicit confirmation in the latest direct user message.
Never pass a model's claim of confirmation as this keyword argument.

Actions and payload fields (in addition to action):
  document.list / package.list / memory.list: offset=0, limit=20
  package.list also accepts keyword='' (title/instructions/attachment names)
  document.create: title, content
  document.read: id, version=None (historical revision)
  document.edit: id, revision, old_text, new_text
  document.versions: id, offset=0, limit=20
  document.diff: id, from_version, to_version=None (current)
  document.restore: id, revision, version
  document.export: id, version=None; returns LighthouseFiles.public upload
  document.delete / package.delete / memory.delete: id, revision
  package.create: title, instructions='', file_ids=[], generated_file_ids=[],
                  document_ids=[], sources=[]
  package.read: id OR name (exact title; ambiguous names fail)
  package.update: id, revision, any of the package.create fields
  memory.save: content, kind='preference'|'writing_rule', id=None, revision=None

Sources are PARENT-ONLY inert plaintext copies: {query_ref, content, recorded_at,
scopes, modules}. Parent MUST resolve current-run query_refs, strip model/UI
sources, and inject the resolved sources before dispatch. No payload field can
authorize access. Source IDs are server assigned; on package.update, {id}
retains an existing snapshot. Editing a snapshot is prohibited. No query is run.
Lists return {items, total, offset, next_offset}. Objects have id/revision.
Content is returned verbatim with content_type=text/plain: consumers must use a
text renderer, not innerHTML. No model or network client is invoked here.
"""

import copy
import difflib
import hashlib
import json
import re
import threading
import time
import unicodedata
import uuid

from .lighthouse_ai import API_KEY, CONTACT, AssistantError, private_identifier


NAMESPACE = 'lighthouse_ai'
KEY_PREFIX = 'workspace:v1:'
MAX_DOCUMENTS = 40
MAX_PACKAGES = 40
MAX_MEMORIES = 100
MAX_VERSIONS = 100
MAX_CONTENT = 100000
MAX_REFERENCES = 40
MAX_SOURCES = 10
MAX_STATE_BYTES = 8 * 1024 * 1024
MAX_PAGE = 50
MAX_OFFSET = 10000
MAX_DIFF_INPUT = 40000
MAX_DIFF_LINES = 1000
MAX_DIFF_OUTPUT = 20000
MAX_DIFF_OUTPUT_LINES = 200

# shortcut: serializes threads in one process, use store-level CAS before multi-process writers.
_LOCK = threading.RLock()
_ID = re.compile(r'[a-f0-9]{32}')
_CREDENTIAL = re.compile(
    r'password|passwd|passphrase|secret|authorization|bearer\s|api[\s_-]*key|'
    r'(?:access|refresh|session)[\s_-]*token|\bcookie\b|\btoken\b|'
    r'private[\s_-]*key|-----BEGIN .*KEY|\b(?:AKIA|ASIA)[A-Z0-9]{16}\b|'
    r'\bgh[pousr]_[A-Za-z0-9_]+|\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.|'
    r'密码|密钥|口令|凭据|凭证|手机号|电话|邮箱|联系方式|phone\s*(?:number)?|\be-?mail\b', re.I)
_CREDENTIAL_VALUE = re.compile(
    r'(?:password|passwd|secret|authorization|api[\s_-]*key|(?:access|refresh|session)[\s_-]*token|'
    r'\btoken|\bcookie|密码|密钥|口令)[\x22\x27]?\s*[:：=]\s*\S+|\bbearer\s+\S{8,}|'
    r'[?&](?:token|ticket|signature|api_key|key)=[^&\s]+|'
    r'-----BEGIN .*PRIVATE KEY|\b(?:AKIA|ASIA)[A-Z0-9]{16}\b|'
    r'\bgh[pousr]_[A-Za-z0-9_]{8,}|\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.', re.I)
_AUTH_MEMORY = re.compile(
    r'权限|授权|管理员|角色|身份|登录|认证|鉴权|\b(?:admin(?:istrator)?|role|'
    r'permissions?|auth(?:entication|orization)?|credentials?|login|access\s+rights?)\b|'
    r'\b(?:i\s+am|i[\x27\u2019]m|i\s+work\s+as)\s+(?:(?:a|an|the)\s+)?'
    r'(?:\w+\s+){0,3}(?:engineer|operator|manager|supervisor|owner|lead)\b|'
    r'(?:我|本人)(?:是|担任|负责|管理).{0,24}(?:工程师|经理|主管|负责人|运维|[ABCDEH]楼|110站)|'
    r'我的(?:岗位|职务|负责楼栋|管辖范围)|'
    r'(?:ignore|override|bypass).{0,40}(?:system|rules|safety|confirmation)|'
    r'(?:忽略|绕过|覆盖).{0,20}(?:系统|规则|确认)', re.I)
_BUSINESS = re.compile(
    r'工单|维修|检修|维保|通告|事件|库存|机柜|设备|水耗|上电|下电|值班|容量|告警|重保|'
    r'work\s*orders?|maintenance|repairs?|incidents?|inventory|devices?|cabinets?|capacity|alarms?|shifts?|'
    r'water\s*(?:usage|consumption)|\b(?:UPS|HVDC|BMS|building)\b|[ABCDEH]\u697c|110\u7ad9', re.I)
_DYNAMIC = re.compile(
    r'当前|现在|今天|今日|昨天|本周|本月|状态|进度|未完成|待处理|进行中|已完成|已关闭|'
    r'已结束|未结束|故障|数量|总数|剩余|(?:有|共|剩)\s*[一二三四五六七八九十\d]+|'
    r'\d+\s*(?:条|台|个|件|项|kW|kWh|升|立方米)|'
    r'\b(?:current(?:ly)?|today|yesterday|tomorrow|status|progress|pending|open|closed|'
    r'failed|broken|completed|remaining|total|count|running|active)\b', re.I)


def _text(value, name, maximum, *, nonempty=False):
    if not isinstance(value, str) or '\x00' in value:
        raise AssistantError('请输入有效的纯文本：' + name + '。')
    if len(value) > maximum:
        raise AssistantError('文字超过资料长度上限：' + name + '。', 413)
    if nonempty and not value.strip():
        raise AssistantError('请填写必填文字：' + name + '。')
    try:
        value.encode('utf-8')
    except UnicodeError:
        raise AssistantError('文字包含无效字符，请检查后重试。') from None
    return value


def _normalized(value):
    value = unicodedata.normalize('NFKC', value)
    return ''.join(c for c in value if unicodedata.category(c) != 'Cf')


def _material_text(value, name, maximum, *, nonempty=False):
    value = _text(value, name, maximum, nonempty=nonempty)
    normalized = _normalized(value)
    if (private_identifier(normalized) or CONTACT.search(normalized)
            or API_KEY.search(normalized) or _CREDENTIAL_VALUE.search(normalized)):
        raise AssistantError('资料含私密身份信息、联系方式或凭据，请移除后重试。', 422)
    return value


def _integer(value, name, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        raise AssistantError('请输入范围内的整数：' + name + '。')
    return value


def _strings(value, name, maximum=40):
    if not isinstance(value, list) or len(value) > maximum:
        raise AssistantError('列表格式无效或超过数量上限：' + name + '。')
    for item in value:
        _text(item, name, 256, nonempty=True)
    if len(set(value)) != len(value):
        raise AssistantError('列表存在重复项：' + name + '。')
    return list(value)


def _identity(value):
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise AssistantError('资料不存在或不属于当前账号、话题。', 404)
    return value


def _page(items, payload):
    offset = _integer(payload.get('offset', 0), 'offset', 0, MAX_OFFSET)
    limit = _integer(payload.get('limit', 20), 'limit', 1, MAX_PAGE)
    end = offset + limit
    return {'items': items[offset:end], 'total': len(items), 'offset': offset,
            'next_offset': end if end < len(items) else None}


class Workspace:
    """One atomic JSON bucket per exact (account, topic); no personal fallback."""

    def __init__(self, store, files):
        self.store = store
        self.files = files

    @staticmethod
    def _actor(actor):
        if not isinstance(actor, dict):
            raise AssistantError('请先登录。', 401)
        owner = _text(actor.get('id'), 'actor.id', 256, nonempty=True)
        scopes = _strings(actor.get('_personal_scopes', actor.get('scopes', [])), 'actor.scopes')
        if 'allowed_scopes' in actor:
            scopes = sorted(set(scopes) & set(_strings(actor['allowed_scopes'], 'actor.allowed_scopes')))
        modules = _strings(actor.get('_personal_modules', actor.get('modules', [])), 'actor.modules', 100)
        if (actor.get('_topic_capabilities') or actor.get('_shared_plan')) and (
                '_personal_scopes' not in actor or '_personal_modules' not in actor):
            raise AssistantError('当前话题缺少账号自身权限，请重新登录后重试。', 403)
        if 'assistant' not in modules:
            raise AssistantError('当前账号已无助手使用权限。', 403)
        topic = actor.get('_feishu_topic')
        channel = actor.get('channel', '')
        if not isinstance(channel, str):
            raise AssistantError('当前会话身份无效。', 403)
        if topic is not None:
            if not isinstance(topic, dict) or not channel.startswith('feishu:'):
                raise AssistantError('当前话题身份无效。', 403)
            topic = _text(topic.get('id'), 'actor.topic', 512, nonempty=True)
        # Strip shared-plan grants and delegated discovery capabilities before file ACL checks.
        own = {'id': owner, 'scopes': scopes, 'allowed_scopes': scopes,
               'modules': modules, 'channel': channel}
        if topic is not None:
            own['_feishu_topic'] = {'id': topic}
        return own, topic

    @staticmethod
    def _key(actor, topic):
        encoded = json.dumps([actor['id'], topic], ensure_ascii=True, separators=(',', ':'))
        return KEY_PREFIX + hashlib.sha256(encoded.encode('ascii')).hexdigest()

    def _load(self, actor, topic):
        state = self.store.get_document(NAMESPACE, self._key(actor, topic))
        if state is None:
            return {'schema_version': 1, 'owner': actor['id'], 'topic_id': topic,
                    'documents': {}, 'packages': {}, 'memories': {}}
        if (state.get('schema_version') != 1 or state.get('owner') != actor['id']
                or state.get('topic_id') != topic
                or any(not isinstance(state.get(k), dict) for k in ('documents', 'packages', 'memories'))):
            raise AssistantError('资料身份或格式不匹配，原资料已保留。', 409)
        return copy.deepcopy(state)

    def _save(self, actor, topic, state):
        if len(json.dumps(state, ensure_ascii=False, allow_nan=False).encode('utf-8')) > MAX_STATE_BYTES:
            raise AssistantError('工作资料空间已满，原资料已保留。', 413)
        self.store.put_document(NAMESPACE, self._key(actor, topic), state)

    @staticmethod
    def _allowed(actor, item):
        return (set(item['access_scopes']) <= set(actor['scopes'])
                and set(item['access_modules']) <= set(actor['modules']))

    def _get(self, state, actor, kind, identity):
        item = state[kind].get(_identity(identity))
        if item is None:
            raise AssistantError('资料不存在或不属于当前账号、话题。', 404)
        if not self._allowed(actor, item):
            raise AssistantError('当前账号已无权访问该资料。', 403)
        return item

    @staticmethod
    def _revision(item, payload):
        revision = _integer(payload.get('revision'), 'revision', 1, 2 ** 53 - 1)
        if revision != item['revision']:
            raise AssistantError('资料已被修改，请刷新后重试。', 409,
                                 category='workspace_conflict')

    @staticmethod
    def _access(item, actor):
        item['access_scopes'] = sorted(set(item.get('access_scopes', [])) | set(actor['scopes']))
        item['access_modules'] = sorted(set(item.get('access_modules', [])) | set(actor['modules']))

    def _new(self, state, actor, kind, maximum):
        if len(state[kind]) >= maximum:
            raise AssistantError('资料数量已达上限，请先删除不再需要的资料。', 413)
        for _ in range(4):
            identity = uuid.uuid4().hex
            if all(identity not in state[k] for k in ('documents', 'packages', 'memories')):
                break
        else:
            raise AssistantError('资料编号冲突，请重新创建。', 409)
        now = time.time()
        item = {'id': identity, 'revision': 1, 'created_at': now, 'updated_at': now}
        self._access(item, actor)
        state[kind][identity] = item
        return item

    @staticmethod
    def _view(item):
        return copy.deepcopy({k: v for k, v in item.items() if k != 'versions'})

    def _version(self, item, number=None):
        number = item['revision'] if number is None else _integer(number, 'version', 1, MAX_VERSIONS)
        version = next((v for v in item['versions'] if v['revision'] == number), None)
        if version is None:
            raise AssistantError('文档版本不存在。', 404)
        return version

    def _document_view(self, item, number=None):
        version = self._version(item, number)
        return {**self._view(item), **copy.deepcopy(version), 'current_revision': item['revision'],
                'version_count': len(item['versions']), 'content_type': 'text/plain'}

    def _append_version(self, item, actor, content, *, restored_from=None):
        if len(item['versions']) >= MAX_VERSIONS:
            raise AssistantError('文档版本已达上限，历史版本已保留。', 413)
        item['revision'] += 1
        item['updated_at'] = time.time()
        self._access(item, actor)
        version = {'revision': item['revision'], 'title': item['title'],
                   'content': content, 'created_at': item['updated_at']}
        if restored_from is not None:
            version['restored_from'] = restored_from
        item['versions'].append(version)

    def _file(self, actor, topic, identity):
        item = self.files.get(actor, _identity(identity))
        if item.get('owner') != actor['id'] or item.get('topic_id') != topic:
            raise AssistantError('附件不属于当前账号、话题。', 404)
        if (set(item.get('source_scopes', [])) - set(actor['scopes'])
                or set(item.get('source_modules', [])) - set(actor['modules'])):
            raise AssistantError('当前账号已无权读取该附件。', 403)
        _material_text(item['name'], '附件名称', 180)
        return self.files.public(item)

    def _package_view(self, state, actor, topic, item):
        files = [self._file(actor, topic, i) for i in item['file_ids']]
        generated = [self._file(actor, topic, i) for i in item['generated_file_ids']]
        documents = [self._document_view(self._get(state, actor, 'documents', identity))
                     for identity in item['document_ids']]
        for source in item['sources']:
            if not self._allowed(actor, source):
                raise AssistantError('当前账号已无权读取该查询快照。', 403)
        return {**self._view(item), 'files': files, 'generated_files': generated,
                'documents': documents, 'content_type': 'text/plain', 'private': True,
                'historical_note': '查询结果为 recorded_at 时刻的私有只读历史快照；'
                '当前业务状态请重新查询有权限的业务工具。'}

    def _sources(self, actor, values, previous):
        if not isinstance(values, list) or len(values) > MAX_SOURCES:
            raise AssistantError('每个材料包最多保存10条查询快照。')
        old = {s['id']: s for s in previous}
        result, seen = [], set()
        query_refs = set()
        for value in values:
            if not isinstance(value, dict):
                raise AssistantError('查询快照格式无效。')
            if 'id' in value:
                identity = _identity(value['id'])
                if identity not in old:
                    raise AssistantError('查询快照不存在。', 404)
                snapshot = old[identity]
                if value != {'id': identity} and value != snapshot:
                    raise AssistantError('查询快照只读，请重新查询并添加新快照。', 409)
            else:
                if set(value) != {'query_ref', 'content', 'recorded_at', 'scopes', 'modules'}:
                    raise AssistantError('查询快照须包含查询引用、正文、记录时间及楼栋、模块范围。')
                scopes = _strings(value['scopes'], 'source.scopes')
                modules = _strings(value['modules'], 'source.modules', 100)
                if set(scopes) - set(actor['scopes']) or set(modules) - set(actor['modules']):
                    raise AssistantError('查询快照超出当前账号自身权限。', 403)
                recorded_at = value['recorded_at']
                if type(recorded_at) not in (int, float) or not 0 < recorded_at <= time.time() + 60:
                    raise AssistantError('查询快照记录时间无效。')
                identity = uuid.uuid4().hex
                if identity in old:
                    raise AssistantError('查询快照编号冲突，请重试。', 409)
                snapshot = {'id': identity, 'query_ref': _text(value['query_ref'], 'query_ref', 200, nonempty=True),
                            'content': _material_text(value['content'], '查询正文', 6000),
                            'recorded_at': recorded_at, 'scopes': sorted(scopes), 'modules': sorted(modules),
                            'access_scopes': sorted(set(scopes) | set(actor['scopes'])),
                            'access_modules': sorted(set(modules) | set(actor['modules'])),
                            'captured_at': time.time(), 'read_only': True, 'live': False,
                            'untrusted_content': True, 'content_type': 'text/plain'}
            if snapshot['id'] in seen or snapshot['query_ref'] in query_refs:
                raise AssistantError('查询快照重复，请检查查询引用。')
            seen.add(snapshot['id'])
            query_refs.add(snapshot['query_ref'])
            result.append(copy.deepcopy(snapshot))
        return result

    def _package_fields(self, state, actor, topic, payload, item):
        for name, maximum in (('title', 200), ('instructions', 12000)):
            if name in payload:
                item[name] = _material_text(payload[name], name, maximum, nonempty=name == 'title')
        for name in ('file_ids', 'generated_file_ids', 'document_ids'):
            if name in payload:
                item[name] = _strings(payload[name], name, MAX_REFERENCES)
                for identity in item[name]:
                    _identity(identity)
        if set(item['file_ids']) & set(item['generated_file_ids']):
            raise AssistantError('同一附件不能同时列为输入文件和生成文件。')
        if sum(len(item[k]) for k in ('file_ids', 'generated_file_ids', 'document_ids')) > MAX_REFERENCES:
            raise AssistantError('材料包附件和文档合计超过数量上限。', 413)
        if 'sources' in payload:
            item['sources'] = self._sources(actor, payload['sources'], item['sources'])
        self._package_view(state, actor, topic, item)

    @staticmethod
    def _memory_content(value):
        content = _text(value, 'content', 2000, nonempty=True)
        normalized = _normalized(content)
        if (private_identifier(normalized) or CONTACT.search(normalized) or API_KEY.search(normalized)
                or _CREDENTIAL.search(normalized) or _AUTH_MEMORY.search(normalized)):
            raise AssistantError('长期记忆不能保存私密信息、凭据、角色或权限。', 422)
        if _BUSINESS.search(normalized) and _DYNAMIC.search(normalized):
            raise AssistantError('动态业务状态请保存为查询快照，不能作为长期记忆。', 422)
        return content

    def dispatch(self, actor, payload, *, confirmed=False):
        """Dispatch a strict payload; `confirmed` is a trusted SERVER keyword only."""
        if not isinstance(payload, dict) or not isinstance(payload.get('action'), str):
            raise AssistantError('请提供有效的工作资料操作。')
        action = payload['action']
        fields = {
            'document.list': {'offset', 'limit'},
            'document.create': {'title', 'content'},
            'document.read': {'id', 'version'},
            'document.edit': {'id', 'revision', 'old_text', 'new_text'},
            'document.versions': {'id', 'offset', 'limit'},
            'document.diff': {'id', 'from_version', 'to_version'},
            'document.restore': {'id', 'revision', 'version'},
            'document.export': {'id', 'version'},
            'document.delete': {'id', 'revision'},
            'package.list': {'offset', 'limit', 'keyword'},
            'package.create': {'title', 'instructions', 'file_ids', 'generated_file_ids', 'document_ids', 'sources'},
            'package.read': {'id', 'name'},
            'package.update': {'id', 'revision', 'title', 'instructions', 'file_ids', 'generated_file_ids', 'document_ids', 'sources'},
            'package.delete': {'id', 'revision'},
            'memory.list': {'offset', 'limit'},
            'memory.save': {'id', 'revision', 'content', 'kind'},
            'memory.delete': {'id', 'revision'},
        }
        if action not in fields or set(payload) - fields.get(action, set()) - {'action'}:
            raise AssistantError('工作资料操作或参数不受支持。')
        actor, topic = self._actor(actor)
        if action in {'memory.save', 'memory.delete'} and confirmed is not True:
            raise AssistantError('修改长期记忆须由用户在当前消息或界面中明确确认。', 403)
        with _LOCK:
            state = self._load(actor, topic)
            group, operation = action.split('.')
            kind = {'document': 'documents', 'package': 'packages', 'memory': 'memories'}[group]
            if operation == 'list':
                items = []
                keyword = _text(payload.get('keyword', ''), 'keyword', 200).casefold()
                for item in state[kind].values():
                    if not self._allowed(actor, item):
                        continue
                    if group == 'package':
                        try:
                            view = self._package_view(state, actor, topic, item)
                        except AssistantError as exc:
                            if exc.status not in {403, 404}:
                                raise
                            continue
                        searchable = [item['title'], item['instructions']]
                        searchable.extend(f['name'] for f in view['files'] + view['generated_files'])
                        if keyword and not any(keyword in text.casefold() for text in searchable):
                            continue
                    view = self._view(item)
                    if group == 'document':
                        view['version_count'] = len(item['versions'])
                    elif group == 'package':
                        view.pop('sources')
                        view['source_count'] = len(item['sources'])
                    items.append(view)
                items.sort(key=lambda i: (-i['updated_at'], i['id']))
                return _page(items, payload)

            if action == 'document.create':
                title = _material_text(payload.get('title'), '标题', 200, nonempty=True)
                content = _material_text(payload.get('content'), '正文', MAX_CONTENT)
                item = self._new(state, actor, kind, MAX_DOCUMENTS)
                item.update(title=title, versions=[{'revision': 1, 'title': title,
                            'content': content, 'created_at': item['created_at']}])
                result = self._document_view(item)
            elif action == 'package.create':
                _material_text(payload.get('title'), '标题', 200, nonempty=True)
                item = self._new(state, actor, kind, MAX_PACKAGES)
                item.update(instructions='', file_ids=[], generated_file_ids=[], document_ids=[], sources=[])
                self._package_fields(state, actor, topic, payload, item)
                result = self._package_view(state, actor, topic, item)
            elif action == 'memory.save':
                content = self._memory_content(payload.get('content'))
                if 'id' in payload:
                    item = self._get(state, actor, kind, payload['id'])
                    self._revision(item, payload)
                    item['revision'] += 1
                    item['updated_at'] = time.time()
                    self._access(item, actor)
                else:
                    if 'revision' in payload:
                        raise AssistantError('新建长期记忆时无需提供修订号。')
                    item = self._new(state, actor, kind, MAX_MEMORIES)
                memory_kind = payload.get('kind', item.get('kind', 'preference'))
                if not isinstance(memory_kind, str) or memory_kind not in {'preference', 'writing_rule'}:
                    raise AssistantError('长期记忆仅支持偏好或稳定写作规则。')
                item.update(content=content, kind=memory_kind, confirmed_at=time.time(), content_type='text/plain')
                result = self._view(item)
            else:
                if action == 'package.read' and 'name' in payload:
                    if 'id' in payload:
                        raise AssistantError('请仅通过编号或名称中的一种选择材料包。')
                    name = _text(payload['name'], 'name', 200, nonempty=True)
                    matches = []
                    for candidate in state[kind].values():
                        if candidate['title'] != name or not self._allowed(actor, candidate):
                            continue
                        try:
                            matches.append(self._package_view(state, actor, topic, candidate))
                        except AssistantError as exc:
                            if exc.status not in {403, 404}:
                                raise
                    if not matches:
                        raise AssistantError('资料不存在或不属于当前账号、话题。', 404)
                    if len(matches) > 1:
                        raise AssistantError('材料包存在重名，请通过编号选择。', 409)
                    return matches[0]
                item = self._get(state, actor, kind, payload.get('id'))
                if operation == 'delete':
                    self._revision(item, payload)
                    if group == 'document' and any(item['id'] in p['document_ids'] for p in state['packages'].values()):
                        raise AssistantError('文档仍被材料包引用，请先移除引用。', 409)
                    del state[kind][item['id']]
                    result = {'id': item['id'], 'revision': item['revision'], 'deleted': True}
                elif action == 'package.read':
                    return self._package_view(state, actor, topic, item)
                elif action == 'package.update':
                    self._revision(item, payload)
                    self._access(item, actor)
                    self._package_fields(state, actor, topic, payload, item)
                    item['revision'] += 1
                    item['updated_at'] = time.time()
                    result = self._package_view(state, actor, topic, item)
                elif action == 'document.read':
                    return self._document_view(item, payload.get('version'))
                elif action == 'document.versions':
                    return _page([{k: v for k, v in version.items() if k != 'content'}
                                  for version in reversed(item['versions'])], payload)
                elif action == 'document.edit':
                    self._revision(item, payload)
                    old = _text(payload.get('old_text'), 'old_text', MAX_CONTENT)
                    new = _text(payload.get('new_text'), 'new_text', MAX_CONTENT)
                    current = self._version(item)['content']
                    start = current.find(old)
                    if (not old and current) or (old and (start < 0 or current.find(old, start + 1) >= 0)):
                        raise AssistantError('待替换文字须唯一且完全匹配，原文已保留。', 409,
                                             category='workspace_conflict')
                    content = current.replace(old, new, 1) if old else new
                    _material_text(content, '正文', MAX_CONTENT)
                    self._append_version(item, actor, content)
                    result = self._document_view(item)
                elif action == 'document.restore':
                    self._revision(item, payload)
                    number = _integer(payload.get('version'), 'version', 1, MAX_VERSIONS)
                    version = self._version(item, number)
                    self._append_version(item, actor, version['content'], restored_from=version['revision'])
                    result = self._document_view(item)
                elif action == 'document.diff':
                    if 'from_version' not in payload:
                        raise AssistantError('请选择比较的起始版本。')
                    number = _integer(payload['from_version'], 'from_version', 1, MAX_VERSIONS)
                    before = self._version(item, number)
                    after = self._version(item, payload.get('to_version'))
                    for version in (before, after):
                        if len(version['content']) > MAX_DIFF_INPUT or len(version['content'].splitlines()) > MAX_DIFF_LINES:
                            raise AssistantError('版本内容超过差异比较上限，请分别读取版本。', 413)
                    lines, size, line_count, truncated = [], 0, 0, False
                    for line in difflib.unified_diff(before['content'].splitlines(keepends=True),
                            after['content'].splitlines(keepends=True), fromfile=str(before['revision']),
                            tofile=str(after['revision'])):
                        if not line.endswith('\n'):
                            line += '\n\\ No newline at end of file\n'
                        count = len(line.splitlines())
                        if line_count + count > MAX_DIFF_OUTPUT_LINES or size + len(line) > MAX_DIFF_OUTPUT:
                            truncated = True
                            break
                        lines.append(line)
                        size += len(line)
                        line_count += count
                    return {'id': item['id'], 'revision': item['revision'], 'from_version': before['revision'],
                            'to_version': after['revision'], 'diff': ''.join(lines), 'truncated': truncated,
                            'content_type': 'text/plain'}
                elif action == 'document.export':
                    version = self._version(item, payload.get('version'))
                    name = re.sub(r'[\\/:*?"<>|\x00-\x1f]', '_', item['title'])[:150] + '.txt'
                    return self.files.upload(actor, name, version['content'].encode('utf-8'),
                                             extract=False, source_scopes=item['access_scopes'],
                                             source_modules=item['access_modules'])
                else:
                    raise AssistantError('不支持此工作资料操作。')
            self._save(actor, topic, state)
            return result

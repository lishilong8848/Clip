"""Publish absolute daily counts from the active caller. Import and construction are offline."""
import datetime as dt
import json
import re
import threading
import uuid


WIKI = 'HScCwZt9QiqPCUkSrHjcbBL1ngb'
TABLE = 'tbl283P5x8cOhKDx'
VIEW = 'vew3MLzBIn'
KEY = '文本'  # Existing text primary field on this specific table.
DAY, MODULE, PAGE, CLIENT, COUNT = '统计日期', '功能模块', '页面', '客户端', '统计数量'
SCHEMA = {KEY: 1, DAY: 5, MODULE: 1, PAGE: 1, CLIENT: 1, COUNT: 2}
UI_TYPES = {1: 'Text', 2: 'Number', 5: 'DateTime'}
BEIJING = dt.timezone(dt.timedelta(hours=8))
CLIENT_LABELS = {'web_v2': '新版网页', 'web_legacy': '旧版网页',
                 'web_standalone': '独立网页', 'qt': 'Qt 桌面'}


class _Rejected(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__(f'飞书使用统计请求失败（代码 {code}）')


def _require_writer():
    from .portal_service import external_real_write_guard
    if not external_real_write_guard()['real_write_allowed']:
        raise RuntimeError('当前调用端未启用真实飞书写入')


def _request(method, path, **kwargs):
    from upload_event_module.services import feishu_service as fs
    try:
        token = fs.ensure_feishu_token()
        if not token:
            raise RuntimeError('auth')
        reply = fs._feishu_request(method, path, token, **kwargs)
        if reply.code in fs.TOKEN_ERROR_CODES:
            token = fs.refresh_feishu_token()
            if token:
                reply = fs._feishu_request(method, path, token, **kwargs)
        if not reply.success():
            raise _Rejected(reply.code)
        data = json.loads(reply.raw.content)['data']
        if not isinstance(data, dict):
            raise ValueError('data')
        return data
    except _Rejected:
        raise
    except Exception:
        raise RuntimeError('飞书鉴权、连接或响应未确认，请保留统计待重试') from None


def _record_id(value):
    if not isinstance(value, str) or not re.fullmatch(r'rec[A-Za-z0-9]+', value):
        raise ValueError('使用统计记录 ID 无效')
    return value


def _identity(row):
    key = uuid.UUID(row['key'])
    if key.version != 4 or str(key) != row['key'].lower():
        raise ValueError('使用统计键必须为标准 UUIDv4')
    day = dt.date.fromisoformat(row['day'])
    if day.isoformat() != row['day']:
        raise ValueError('使用统计日期须为 YYYY-MM-DD')
    if row['client'] not in CLIENT_LABELS:
        raise ValueError('使用统计客户端无效')
    for name in ('module_label', 'page_label'):
        if not isinstance(row[name], str) or not row[name].strip():
            raise ValueError('使用统计缺少功能或页面名称')
    if type(row['count']) is not int or not 0 <= row['count'] < 2**53:
        raise ValueError('使用统计数量必须是可精确表示的非负整数')
    return {KEY: str(key), DAY: int(dt.datetime.combine(day, dt.time.min, BEIJING).timestamp() * 1000),
            MODULE: row['module_label'], PAGE: row['page_label'], CLIENT: CLIENT_LABELS[row['client']]}


def _text(value):
    if isinstance(value, list) and all(isinstance(v, dict) and isinstance(v.get('text'), str) for v in value):
        return ''.join(v['text'] for v in value)
    return value


def _items(method, path, *, size, **kwargs):
    params = {'page_size': size}
    seen = set()
    while True:
        data = _request(method, path, params=params.copy(), **kwargs)
        if not isinstance(data.get('items'), list) or type(data.get('has_more')) is not bool:
            raise RuntimeError('飞书使用统计列表不完整')
        yield from data['items']
        if not data['has_more']:
            return
        cursor = data.get('page_token')
        if not isinstance(cursor, str) or not cursor or cursor in seen:
            raise RuntimeError('飞书使用统计分页异常')
        seen.add(cursor)
        params['page_token'] = cursor


class UsageBitableSink:
    def __init__(self):
        self._table = ''
        self._schema_ready = False
        self._lock = threading.Lock()

    def _target(self):
        if not self._table:
            node = _request('GET', 'wiki/v2/spaces/get_node', params={'token': WIKI, 'obj_type': 'wiki'})['node']
            if (node.get('node_token') != WIKI or node.get('obj_type') != 'bitable'
                    or not re.fullmatch('[A-Za-z0-9]+', node.get('obj_token') or '')):
                raise RuntimeError('未能确认指定的使用统计多维表格节点')
            path = f'bitable/v1/apps/{node["obj_token"]}/tables/{TABLE}'
            if _request('GET', path + '/views/' + VIEW)['view'].get('view_id') != VIEW:
                raise RuntimeError('未能确认指定的使用统计视图')
            self._table = path
        return self._table

    def _ensure_schema(self):
        if self._schema_ready:
            return
        path = self._target() + '/fields'
        fields = {f['field_name']: f for f in _items('GET', path, size=100)}
        if fields.get(KEY, {}).get('is_primary') is not True:
            raise RuntimeError('使用统计主字段已变化')
        for name, kind in SCHEMA.items():
            field = fields.get(name)
            if field and (field['type'] != kind or field.get('ui_type', UI_TYPES[kind]) != UI_TYPES[kind]):
                raise RuntimeError(f'使用统计字段「{name}」类型冲突')
        for name, kind in SCHEMA.items():
            if name not in fields:
                spec = {'field_name': name, 'type': kind}
                if name == DAY:
                    spec['property'] = {'date_formatter': 'yyyy-MM-dd', 'auto_fill': False}
                result = _request('POST', path, body=spec)['field']
                if result.get('field_name') != name or result.get('type') != kind:
                    raise RuntimeError('飞书使用统计字段创建结果未确认')
        self._schema_ready = True

    def _find(self, records, key):
        # A view filter may hide an existing row. Check uniqueness across the target table.
        body = {'field_names': [KEY], 'filter': {'conjunction': 'and', 'conditions': [
            {'field_name': KEY, 'operator': 'is', 'value': [key]}]}}
        found = ''
        for item in _items('POST', records + '/search', size=2, body=body):
            if found:
                raise RuntimeError('云端有重复统计键，请核对记录')
            if _text(item['fields'].get(KEY)) != key:
                raise RuntimeError('云端搜索结果与统计键不符')
            found = _record_id(item['record_id'])
        return found

    @staticmethod
    def _read(records, rid):
        try:
            result = _request('GET', records + '/' + rid)['record']
        except _Rejected as exc:
            if exc.code == 1254043:
                return None
            raise
        if result.get('record_id') != rid or not isinstance(result.get('fields'), dict):
            raise RuntimeError('云端使用统计记录回执不完整')
        return result['fields']

    def upsert(self, row) -> str:
        identity = _identity(row)
        count, key = row['count'], identity[KEY]
        receipt = row.get('record_id')
        if receipt not in ('', None):
            _record_id(receipt)
        _require_writer()
        # The caller owns the SQLite export lock across processes and constructs one sink per round.
        with self._lock:
            self._ensure_schema()
            records = self._table + '/records'
            found = self._find(records, key)
            rid = found or receipt
            if found and receipt and found != receipt and self._read(records, receipt) is not None:
                raise RuntimeError('旧回执与云端统计键不一致')
            current = self._read(records, rid) if rid else None
            if current is None:
                if found:
                    raise RuntimeError('搜索与回读结果不一致，待重试')
                token = key
                if receipt:
                    from upload_event_module.services.feishu_service import _stable_uuid4_client_token
                    token = _stable_uuid4_client_token(f'usage:{key}:after:{receipt}')
                # Keep the entire create request stable even if the pending count grows.
                result = _request('POST', records, params={'client_token': token},
                                  body={'fields': {**identity, COUNT: 0}})
                rid = _record_id(result['record']['record_id'])
                current = self._read(records, rid)
                if current is None:
                    raise RuntimeError('云端新记录尚未可读，待重试')

            if any(_text(current.get(name)) != value for name, value in identity.items()):
                raise RuntimeError('统计键或日期、功能、页面、客户端不一致')
            previous = current.get(COUNT)
            if type(previous) not in (int, float) or not 0 <= previous < 2**53 or int(previous) != previous:
                raise RuntimeError('云端统计数量异常')
            if previous < count:
                result = _request('PUT', records + '/' + rid, body={'fields': {COUNT: count}})['record']
                written = result.get('fields', {}).get(COUNT)
                if result.get('record_id') != rid or type(written) not in (int, float) or written != count:
                    raise RuntimeError('云端统计数量更新未确认')
            return rid

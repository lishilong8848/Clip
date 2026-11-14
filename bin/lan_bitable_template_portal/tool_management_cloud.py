"""Read the existing tool inventory and write only the configured ledgers."""

import datetime as dt
import hashlib
import json
import logging
import math
from pathlib import Path
import re
from tempfile import TemporaryDirectory
import threading
import time
from urllib.parse import parse_qs, quote, urlencode, urlsplit

from upload_event_module.services.http_client import FeishuHTTPError
from upload_event_module.services.feishu_service import _stable_uuid4_client_token

from .risk_inspection import RiskError
from .risk_inspection_cloud import API
from .tool_management import DESTINATIONS, MAX_REPORT, OUTCOMES
from .tool_self_check import BUILDINGS, MAX_FILE, TZ


BASE = 'Ix2Db9hMfaZacms9cxdcBdnJnMc'
INVENTORY = 'tbl8pYoYy88r2OSx'
ORGANIZATION = 'tblxLMZDeBbA6Wn2'
TEMPLATE_TABLE = 'tbl5zn6iPyKOF3kR'
TEMPLATE_NAME = '文本'
TEMPLATE_FILE = '模板文件'
TEMPLATE_HASH = '文件摘要'
TEMPLATE_LINK = '与记录表格模板关联关系'
TABLES = {'maintenance': 'tblutFXoTA0mPkt1', 'inspection': 'tblHtl0cVXn6zci6',
          'repair': 'tblTasmHUxgdtZQd', 'scrap': 'tbl71QoG0R0Q3esn', 'change': 'tblEwY3AbwiT021v'}
KEY = '灯塔工具工单号'
STATE = '灯塔处理状态'
DETAIL = '灯塔处理说明'
SOURCE_FIELDS = ['楼栋', '工具名称', '规格型号', '放置区域', '一级分类', '区域对应工具数量',
                 '维护周期', '送检周期', '当前维护日期', '当前送检日期', '是否报废', '品牌', TEMPLATE_LINK]


def plain(value):
    if isinstance(value, list):
        return ''.join(map(plain, value))
    if isinstance(value, dict):
        return plain(value.get('value', value.get('text', value.get('name', ''))))
    return str(value or '').strip()


def source_date(value):
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        try:
            return dt.datetime.fromtimestamp(value / 1000, TZ).date().isoformat()
        except (OverflowError, OSError, ValueError):
            pass
    return ''


def epoch(day):
    return int(dt.datetime.combine(dt.date.fromisoformat(day), dt.time.min, TZ).timestamp() * 1000)


def record_id(value):
    return value if isinstance(value, str) and re.fullmatch(r'rec[A-Za-z0-9]+', value) else ''


def _template_text(value):
    if value is None:
        return ''
    if isinstance(value, dict):
        return _template_text(value.get('text', value.get('value', str(value))))
    if isinstance(value, list):
        segments = [_template_text(part) for part in value]
        # Two record IDs in one cell are invalid even when displayed together.
        return ' '.join(segments) if sum(bool(record_id(part.strip())) for part in segments) > 1 else ''.join(segments)
    return str(value)


def _link_ids(value):
    if isinstance(value, list):
        return [identity for part in value for identity in _link_ids(part)]
    if isinstance(value, dict):
        for key in ('record_ids', 'link_record_ids', 'record_id', 'id'):
            if key in value:
                return _link_ids(value[key])
        return ['']
    return [record_id(value)]


def _attachment_tokens(value):
    if not isinstance(value, list):
        return []
    return [item.get('file_token') if isinstance(item, dict) else None for item in value]


_WORKSPACE_FIELDS = {
    'self': ('maintenance', '维护工具', '计划维护日期', '实际维护日期', '维护记录单'),
    'external': ('inspection', '送检工具', '计划检测日期', '实际检测日期', '送检报告'),
}
_QUALIFICATION = '工具检测是否合格'
# Keep retries within the same conservative window as the portal Bitable sync.
WORKSPACE_CREATE_RETRY_SECONDS = 600


class ToolCloud:
    def __init__(self, cloud):
        self.cloud = cloud
        self.fields = {}
        self.workspace_lock = threading.RLock()

    def inventory_fields(self):
        allowed={'楼栋','工具名称','规格型号','放置区域','一级分类','二级分类','区域对应工具数量','品牌','计数单位','维护周期','送检周期','当前维护日期','当前送检日期','清洁情况','外观完好无破损','配件齐全','功能正常','检查结论'}
        fields=self.cloud.all(self.cloud.table_path(BASE, INVENTORY)+'/fields')
        return [{'name':field['field_name'],'type':field['type'],
                 'options':[str(item.get('name')) for item in ((field.get('property') or {}).get('options') or []) if isinstance(item,dict) and item.get('name')]}
                for field in fields if field.get('field_name') in allowed and field.get('type') in (1,2,3,5,7)]

    def create_inventory(self, fields, operation):
        result=self.cloud.request('POST',self.cloud.table_path(BASE,INVENTORY)+'/records',
            params={'client_token':_stable_uuid4_client_token('tool-inventory:'+operation)},json={'fields':fields})
        record=result.get('record') or {}
        if not re.fullmatch(r'rec[A-Za-z0-9]+',str(record.get('record_id') or '')):
            raise RiskError('工具新增结果未确认，请先核对多维表，勿重复新增。',503)
        return record['record_id']

    def inventory(self):
        path = self.cloud.table_path(BASE, INVENTORY) + '/records/search'
        params, seen, records = {'page_size': 500}, set(), {}
        for _ in range(100):
            page = self.cloud.request('POST', path, params=params, json={'field_names': SOURCE_FIELDS})
            if not isinstance(page.get('items'), list) or type(page.get('has_more')) is not bool:
                raise RiskError('工具底表未完整返回，保留上一份库存。', 503)
            for row in page['items']:
                identity, fields = row.get('record_id'), row.get('fields')
                if (not isinstance(identity, str) or not re.fullmatch(r'rec[A-Za-z0-9]+', identity)
                        or not isinstance(fields, dict) or identity in records):
                    raise RiskError('工具底表存在无效或重复记录，暂缓排期。', 503)
                records[identity] = fields
            if not page['has_more']:
                break
            cursor = page.get('page_token')
            if not cursor or cursor in seen:
                raise RiskError('工具底表分页不完整，暂缓排期。', 503)
            seen.add(cursor)
            params['page_token'] = cursor
        else:
            raise RiskError('工具底表读取未完成，暂缓排期。', 503)
        result = []
        for identity, fields in records.items():
            if plain(fields.get('一级分类')) != 'B工具类':
                continue
            building = re.fullmatch(r'(?:南通)?([ABCDEH])楼', plain(fields.get('楼栋')))
            name = plain(fields.get('工具名称'))
            quantity = fields.get('区域对应工具数量')
            if not building or not name:
                continue
            result.append({'id': identity, 'name': name, 'scope': building[1], 'model': plain(fields.get('规格型号')),
                'brand': plain(fields.get('品牌')), 'template_id': record_id(_template_text(fields.get(TEMPLATE_LINK)).strip()),
                'location': plain(fields.get('放置区域')), 'quantity': int(quantity) if type(quantity) in (int, float) else 0,
                'retired': plain(fields.get('是否报废')) in {'是', '已报废', 'true', 'True'},
                'self_cycle': plain(fields.get('维护周期')), 'external_cycle': plain(fields.get('送检周期')),
                'self_last': source_date(fields.get('当前维护日期')), 'external_last': source_date(fields.get('当前送检日期'))})
        leaders = {}
        for row in self.cloud.all(self.cloud.table_path(BASE, ORGANIZATION) + '/records', user_id_type='open_id'):
            fields = row.get('fields') or {}
            scope = re.fullmatch(r'(?:南通)?([ABCDEH])楼', plain(fields.get('楼栋')))
            if not scope:
                continue
            if scope[1] in leaders:
                raise RiskError(f'组织架构表中 {scope[1]} 楼重复配置，暂缓排期。', 503)
            leaders[scope[1]] = sorted({person['id'] for person in fields.get('楼长（借用报废审批）', [])
                if isinstance(person, dict) and re.fullmatch(r'ou_[A-Za-z0-9]+', str(person.get('id', '')))})
        return result, leaders

    def _read_record(self, path, identity, **kwargs):
        if not record_id(identity):
            raise RiskError('飞书返回无效的工具记录编号，请核对原任务。', 503)
        data = self.cloud.request('GET', path + '/records/' + identity, **kwargs)
        row = data.get('record')
        if (not isinstance(row, dict) or row.get('record_id') != identity
                or not isinstance(row.get('fields'), dict)):
            raise RiskError('飞书未完整返回指定工具记录，请稍后重试。', 503)
        return row

    def _inventory_template_id(self, path, asset_id):
        value = _template_text(self._read_record(path, asset_id)['fields'].get(TEMPLATE_LINK)).strip()
        identity = record_id(value)
        if value and not identity:
            raise RiskError('工具底表现有模板关联不是单个有效记录编号，请核对原值。', 409)
        return identity

    @staticmethod
    def _template_entry(row):
        identity = record_id(row.get('record_id'))
        fields = row.get('fields')
        if not identity or not isinstance(fields, dict):
            raise RiskError('飞书模板表包含无效记录，请核对原表。', 503)
        name = plain(fields.get(TEMPLATE_NAME))
        files = fields.get(TEMPLATE_FILE) or []
        if not name or not files:
            return None
        if not isinstance(files, list) or len(files) != 1 or not isinstance(files[0], dict):
            raise RiskError('每条工具模板记录应包含一个附件，请核对飞书模板表。', 503)
        token = files[0].get('file_token')
        if not isinstance(token, str) or not token.strip():
            raise RiskError('飞书未返回有效的工具模板附件令牌。', 503)
        size = files[0].get('size')
        return {'id': identity, 'name': name, 'file_token': token,
                'filename': plain(files[0].get('name')) or name, 'size': size if type(size) is int and size >= 0 else 0,
                'digest': plain(fields.get(TEMPLATE_HASH))}

    def templates(self):
        """List complete template rows. Empty placeholders are omitted."""
        path = self.cloud.table_path(BASE, TEMPLATE_TABLE)
        result, seen = [], set()
        for row in self.cloud.all(path + '/records'):
            identity = record_id(row.get('record_id')) if isinstance(row, dict) else ''
            if not identity or identity in seen:
                raise RiskError('飞书模板列表包含重复或无效记录，请核对原表。', 503)
            entry = self._template_entry(row)
            seen.add(identity)
            if entry:
                result.append(entry)
        return result

    def download_template(self, template):
        """Return at most 2 MiB of bytes from the template's current attachment."""
        identity = record_id(template.get('id'))
        if not identity:
            raise RiskError('工具模板记录编号无效。')
        path = self.cloud.table_path(BASE, TEMPLATE_TABLE)
        row = self._read_record(path, identity)
        current = self._template_entry(row)
        if not current or any(current[key] != template.get(key, '') for key in ('file_token', 'digest')):
            raise RiskError('飞书模板附件已变化，请重新选择模板。', 409)
        if current['size'] > MAX_FILE:
            raise RiskError('工具模板附件不得超过 2 MB。', 413)
        metadata = next((field for field in self.cloud.all(path + '/fields')
                         if field.get('field_name') == TEMPLATE_FILE), {})
        field_id = metadata.get('field_id')
        if metadata.get('type') != 17 or not field_id:
            raise RiskError('飞书模板表缺少可读的模板文件附件字段。', 503)
        token = current['file_token']
        # Use the fixed Drive endpoint and grant context from the verified record.
        attachment = row['fields'][TEMPLATE_FILE][0]
        reported = parse_qs(urlsplit(plain(attachment.get('url'))).query).get('extra', [''])[0]
        extra = reported or json.dumps({'bitablePerm': {'tableId': TEMPLATE_TABLE,
                                       'attachments': {field_id: {identity: [token]}}}}, separators=(',', ':'))
        url = API + 'drive/v1/medias/' + quote(token, safe='') + '/download?' + urlencode({'extra': extra})
        with self.cloud.lock:
            headers = self.cloud.headers()
        try:
            with TemporaryDirectory(prefix='tool-template-') as temp:
                output = Path(temp) / 'template'
                self.cloud.http.download_to_file(url, output, headers=headers, max_bytes=MAX_FILE)
                content = output.read_bytes()
        except FeishuHTTPError as exc:
            if exc.category in {'permission', 'token'}:
                raise RiskError('当前 Qt 飞书应用无法下载该工具模板，请核对鉴权和附件访问权限。', 503) from None
            if exc.category == 'business':
                raise RiskError('工具模板附件下载受限，请核对文件是否超过 2 MB。', 413) from None
            raise RiskError('工具模板附件下载暂未完成，请稍后重试。', 503) from None
        except OSError:
            raise RiskError('本机无法暂存工具模板，请核对临时目录写入权限。', 503) from None
        if not content or len(content) > MAX_FILE or current['size'] and len(content) != current['size']:
            raise RiskError('工具模板文件为空、过大或下载不完整，请核对飞书附件。', 503)
        if current['digest'] and hashlib.sha256(content).hexdigest() != current['digest'].lower():
            raise RiskError('工具模板下载内容与文件摘要不符，请核对飞书附件。', 503)
        return content

    def bind_template(self, asset_id, template_id, expected_template_id):
        """Update the text link after checking its current value; return the bound ID."""
        if (not record_id(asset_id) or not record_id(template_id)
                or expected_template_id != '' and not record_id(expected_template_id)):
            raise RiskError('工具或模板记录编号无效。')
        path = self.cloud.table_path(BASE, INVENTORY)
        with self.workspace_lock:
            before = self._inventory_template_id(path, asset_id)
            if before == template_id:
                return template_id
            if before != expected_template_id:
                raise RiskError('工具的关联模板已变化，请刷新后重试。', 409)
            template_path = self.cloud.table_path(BASE, TEMPLATE_TABLE)
            if not self._template_entry(self._read_record(template_path, template_id)):
                raise RiskError('所选飞书模板尚未配置完整。', 409)
            actual = self._inventory_template_id(path, asset_id)
            if actual == template_id:
                return template_id
            if actual != before:
                raise RiskError('工具的关联模板已变化，请刷新后重试。', 409)
            self.cloud.request('PUT', path + '/records/' + asset_id, json={'fields': {TEMPLATE_LINK: template_id}})
            actual = self._inventory_template_id(path, asset_id)
            if actual == template_id:
                return template_id
            if actual != before:
                raise RiskError('工具的关联模板已变化，请刷新后重试。', 409)
            raise RiskError('飞书尚未读回工具模板关联，请重试原操作。', 503)

    def schema(self, dest, path):
        cached = self.fields.get(dest)
        if cached and time.monotonic() < cached[0]:
            return cached[1]
        fields = {field['field_name']: field for field in self.cloud.all(path + '/fields')}
        for name in (KEY, STATE, DETAIL):
            if name not in fields:
                self.cloud.request('POST', path + '/fields', json={'field_name': name, 'type': 1})
                fields[name] = {'type': 1}
            if fields[name]['type'] != 1:
                raise RiskError(f'{DESTINATIONS[dest]}的「{name}」应为文本字段。', 503)
        self.fields[dest] = (time.monotonic() + 300, fields)
        return fields

    def find(self, path, key):
        data = self.cloud.request('POST', path + '/records/search', params={'page_size': 20},
            json={'field_names': [KEY], 'filter': {'conjunction': 'and', 'conditions': [
                {'field_name': KEY, 'operator': 'is', 'value': [key]}]}})
        rows = data.get('items')
        if not isinstance(rows, list) or data.get('has_more') is not False or len(rows) > 1:
            raise RiskError('飞书工具工单流水号重复或未完整返回，等待核对。', 503)
        if not rows:
            return ''
        identity = rows[0].get('record_id')
        if not identity or plain((rows[0].get('fields') or {}).get(KEY)) != key:
            raise RiskError('飞书返回的工具工单流水号不匹配。', 503)
        return identity

    def upload(self, name, report):
        data = self.cloud.request('POST', 'drive/v1/medias/upload_all',
            data={'file_name': name, 'parent_type': 'bitable_file', 'parent_node': BASE, 'size': str(len(report))},
            files={'file': (name, report, 'application/octet-stream')})
        token = data.get('file_token')
        if not isinstance(token, str) or not token:
            raise RiskError('飞书未确认送检报告上传。', 503)
        return token

    def write_workspace_item(self, session_id, item, report_name, report_bytes, actual,
                             file_token='', remote_id='', *, save_file=None, attempted_at=None, save_attempt=None):
        """Publish and verify one immutable result. Callbacks persist receipts before creation.

        attempted_at is a Unix timestamp in seconds, or None/0 for the first attempt.
        save_attempt(timestamp) stores the first attempt and must raise if persistence fails.
        """
        if not isinstance(session_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', session_id):
            raise RiskError('工具工作台单号无效。')
        asset_id, scope, kind = item.get('asset_id'), item.get('scope'), item.get('kind')
        if not record_id(asset_id) or scope not in BUILDINGS or kind not in _WORKSPACE_FIELDS:
            raise RiskError('工具记录、楼栋或工单类型无效。')
        if remote_id and not record_id(remote_id):
            raise RiskError('工具台账回执编号无效。')
        qualified = item.get('qualified')
        if kind == 'self' and type(qualified) is not bool:
            raise RiskError('请选择工具检测是否合格。')
        if (attempted_at is not None and (type(attempted_at) not in (int, float)
                or not math.isfinite(attempted_at) or attempted_at < 0)):
            raise RiskError('工具台账首次写入时间无效。')
        try:
            planned, completed = epoch(item.get('due')), epoch(actual)
        except (TypeError, ValueError, OverflowError):
            raise RiskError('计划日期或实际日期无效。') from None
        if (not isinstance(report_name, str) or not report_name.strip() or len(report_name) > 180
                or re.search(r'[\\/:*?"<>|\x00-\x1f]', report_name)
                or not isinstance(report_bytes, bytes) or not 0 < len(report_bytes) <= MAX_REPORT):
            raise RiskError('每条工具结果需要一份 12 MB 以内的有效报告。')
        if not isinstance(file_token, str) or (file_token and not file_token.strip()):
            raise RiskError('工具报告附件回执无效。')

        dest, link, due, finish, attachment = _WORKSPACE_FIELDS[kind]
        path = self.cloud.table_path(BASE, TABLES[dest])
        key = f'workspace:{session_id}:{asset_id}'
        result = ('合格' if qualified else '工具不合格') if kind == 'self' else '已提交'

        with self.workspace_lock:
            schema = {field['field_name']: field for field in self.cloud.all(path + '/fields')}
            fields = {KEY: key, STATE: result, '楼栋': f'南通{scope}楼', link: [asset_id],
                      due: planned, finish: completed}
            if dest == 'maintenance':
                selection = schema.get(_QUALIFICATION) or {}
                options = (selection.get('property') or {}).get('options') or []
                if selection.get('type') != 3 or result not in {
                    option.get('name') for option in options if isinstance(option, dict)
                }:
                    raise RiskError(f'{DESTINATIONS[dest]}的「{_QUALIFICATION}」缺少「{result}」选项。', 503)
                fields[_QUALIFICATION] = result
            missing = (set(fields) | {attachment}) - schema.keys()
            if missing:
                raise RiskError(f'{DESTINATIONS[dest]}缺少字段：' + '、'.join(sorted(missing)) + '。', 503)
            if any(schema[name].get('type') != 1 for name in (KEY, STATE)):
                raise RiskError(f'{DESTINATIONS[dest]}的灯塔流水号和状态必须为文本字段。', 503)
            link_field = schema[link]
            if link_field.get('type') not in (18, 21) or (link_field.get('property') or {}).get('table_id') != INVENTORY:
                raise RiskError(f'{DESTINATIONS[dest]}的「{link}」未关联工具底表。', 503)
            if schema[attachment].get('type') != 17:
                raise RiskError(f'{DESTINATIONS[dest]}的「{attachment}」必须为附件字段。', 503)

            found = self.find(path, key)
            if found and remote_id and found != remote_id:
                raise RiskError('工具台账记录与保存的回执冲突，请核对原任务。', 409)
            identity = found or remote_id
            if identity and not file_token:
                raise RiskError('工具台账缺少原报告附件回执，无法核验已有记录，请核对原任务。', 503)
            if not identity:
                if attempted_at and not 0 <= time.time() - attempted_at < WORKSPACE_CREATE_RETRY_SECONDS:
                    raise RiskError('工具台账新增结果仍待核验，后续只回读原流水号。', 503)
                if attempted_at and not file_token:
                    raise RiskError('工具台账新增待核验，但缺少原报告附件回执。', 503)
                if not attempted_at and not callable(save_attempt):
                    raise RiskError('工具台账新增需要先保存首次写入时间。', 503)
                if not file_token:
                    if not callable(save_file):
                        raise RiskError('工具台账新增需要先保存报告附件回执。', 503)
                    file_token = self.upload(report_name, report_bytes)
                    save_file(file_token)
                if not attempted_at:
                    save_attempt(time.time())
                data = self.cloud.request('POST', path + '/records',
                    params={'client_token': _stable_uuid4_client_token('tool-management:' + key)},
                    json={'fields': fields | {attachment: [{'file_token': file_token}]}})
                created = data.get('record')
                identity = record_id(created.get('record_id')) if isinstance(created, dict) else ''
                if not identity:
                    raise RiskError('飞书未确认工具台账记录，请重试原任务。', 503)
            actual_fields = self._read_record(path, identity)['fields']
            tokens = _attachment_tokens(actual_fields.get(attachment))
            if (len(tokens) != 1 or not isinstance(tokens[0], str) or not tokens[0]
                    or file_token and tokens[0] != file_token
                    or _link_ids(actual_fields.get(link)) != [asset_id]
                    or any(plain(actual_fields.get(field)) != str(value) for field, value in fields.items() if field != link)):
                raise RiskError('工具台账字段或附件回读未一致，请核对后重试原任务。', 503)
            return {'remote_id': identity, 'file_token': tokens[0]}

    def write(self, dest, order, asset, job, person_for, save_file):
        path = self.cloud.table_path(BASE, TABLES[dest])
        schema = self.schema(dest, path)
        if job.get('record_content') and dest in ('maintenance','repair','scrap'):
            name='维护记录单' if dest=='maintenance' else '自检记录单'
            if name not in schema:
                # A timeout may occur after Feishu created the field. Always
                # discard the old schema so a retry reads before creating.
                try:self.cloud.request('POST',path+'/fields',json={'field_name':name,'type':17})
                finally:self.fields.pop(dest,None)
                schema=self.schema(dest,path)
            if (schema.get(name) or {}).get('type')!=17:
                raise RiskError(f'{DESTINATIONS[dest]}的「{name}」必须为附件字段。',503)
            if not job['file_token']:
                job['file_token']=self.upload(job['record_name'],job['record_content'])
                save_file(job['file_token'])
            attachment={name:[{'file_token':job['file_token']}]}
        else:
            attachment={}
        building = asset['scope'] + '楼'
        key = order['id'] + '/' + dest
        outcomes = {'repair': '检修中', 'repaired': '检修完成', 'scrap': '报废中',
                    'scrapped': '报废完成', 'rejected': '报废未通过'}
        fields = {KEY: key, STATE: OUTCOMES.get(order['outcome'], '已提交'),
                  DETAIL: '\n'.join(dict.fromkeys(filter(None, (order['reason'], order['scrap_reason'], order['resolution']))))}
        fields.update(attachment)

        def link(field):
            metadata = schema.get(field) or {}
            if metadata.get('type') not in (18, 21) or (metadata.get('property') or {}).get('table_id') != INVENTORY:
                raise RiskError(f'{DESTINATIONS[dest]}的「{field}」未关联工具底表。', 503)
            return [asset['id']]

        if dest == 'maintenance':
            fields.update({'楼栋': '南通' + building, '维护工具': link('维护工具'),
                           '计划维护日期': epoch(order['due']), '实际维护日期': epoch(order['actual']),
                           '工具检测是否合格': '合格'})
        elif dest == 'inspection':
            if not job['file_token']:
                job['file_token'] = self.upload(order['report_name'], order['report'])
                save_file(job['file_token'])
            fields.update({'楼栋': '南通' + building, '送检工具': link('送检工具'),
                           '计划检测日期': epoch(order['due']), '实际检测日期': epoch(order['actual']),
                           '送检报告': [{'file_token': job['file_token']}]})
        elif dest == 'repair':
            fields.update({'检修工具名称': asset['name'], '楼栋': building, '转检修时间': epoch(order['submitted_at'][:10])})
        elif dest == 'scrap':
            manager = person_for(order['manager'])
            if not manager.get('open_id'):
                raise RiskError('未找到报废审批人的飞书身份。', 503)
            fields.update({'楼栋': '南通' + building, '报废工具': link('报废工具'), '报废数量': order['scrap_qty'],
                           '报废原因': order['scrap_reason'],
                           '机房经理': [{'id': manager['open_id']}]})
            fields[STATE] = {'pending': '待报废审批', 'approved': '已批准报废', 'rejected': '报废未通过'}[order['scrap_status']]
            if order['scrap_status'] == 'approved':
                fields['报废时间'] = epoch(order['resolved_at'][:10])
        elif dest == 'change':
            fields.update({'关联底表（选择）': link('关联底表（选择）'), '日期': epoch(order['submitted_at'][:10]),
                           '处理方式': '检修转报废' if job.get('from_repair') and order['outcome'] in {'scrap','scrapped','rejected'}
                           else '转报废' if order['outcome'] in {'scrap','scrapped','rejected'} else '转检修'})
            fields['进展反馈'] = outcomes.get(order['outcome'])
        missing = set(fields) - schema.keys()
        if missing:
            self.fields.pop(dest, None)
            raise RiskError(f'{DESTINATIONS[dest]}缺少字段：' + '、'.join(sorted(missing)) + '。', 503)
        found = self.find(path, key) if job.get('verify_write') or not job['remote_id'] else ''
        remote_id = job['remote_id'] or job.get('candidate_id') or found
        if found and remote_id != found:
            raise RiskError('飞书工单流水号与本地回执冲突，等待核对。', 409)
        if job.get('verify_write') and remote_id:
            existing = self._read_record(path, remote_id, params={'user_id_type': 'open_id'})['fields']
            if self._matches_order_fields(existing, fields, schema):
                return remote_id
            if not job['remote_id'] or plain(existing.get(KEY)) != key:
                raise RiskError('原工单已存在，但字段或附件未核验一致。', 503)
        params = {'user_id_type': 'open_id'}
        if remote_id:
            result = self.cloud.request('PUT', path + '/records/' + remote_id, params=params, json={'fields': fields})
        else:
            if job.get('verify_write'):
                attempted_at = job['attempted_at']
                if attempted_at and not 0 <= time.time() - attempted_at < WORKSPACE_CREATE_RETRY_SECONDS:
                    raise RiskError('原工单写入结果待核验，后续只回读原流水号。', 503)
                if not attempted_at:
                    job['save_attempt'](time.time())
            params['client_token'] = _stable_uuid4_client_token('tool-management:' + key)
            result = self.cloud.request('POST', path + '/records', params=params, json={'fields': fields})
        recorded = record_id((result.get('record') or {}).get('record_id'))
        if not recorded or remote_id and remote_id != recorded:
            raise RiskError('飞书未确认工具台账写入，保留原任务重试。', 503)
        if job.get('verify_write'):
            if not remote_id:
                job['save_candidate'](recorded)
            readback = self._read_record(path, recorded, params=params if remote_id else {'user_id_type': 'open_id'})
            if not self._matches_order_fields(readback['fields'], fields, schema):
                raise RiskError('飞书台账字段或附件尚未回读一致，等待核对。', 503)
        return recorded

    @staticmethod
    def _matches_order_fields(actual, expected, schema):
        for name, value in expected.items():
            current = actual.get(name)
            kind = schema[name]['type']
            if kind == 17:
                matches = _attachment_tokens(current) == _attachment_tokens(value)
            elif kind in (18, 21):
                matches = sorted(_link_ids(current)) == sorted(_link_ids(value))
            elif kind == 11:
                matches = isinstance(current, list) and sorted(
                    str(person.get('id') or person.get('open_id') or '') if isinstance(person, dict) else ''
                    for person in current) == sorted(
                    person['id'] for person in value)
            else:
                matches = plain(current) == plain(value)
            if not matches:
                return False
        return True


def run_tool_jobs(service, stop):
    next_inventory = next_archive = 0
    while not stop.is_set():
        now = time.monotonic()
        try:
            if now >= next_inventory:
                next_inventory = now + 300
                try:
                    service.refresh()
                except Exception as exc:
                    logging.warning('Tool inventory refresh failed (%s)', type(exc).__name__)
                    with service.connect(True) as db:
                        service.set_state(db, 'inventory_error', str(exc) if isinstance(exc, RiskError)
                                          else '工具底表读取失败，保留上次库存并自动重试。')
            if now >= next_archive:
                next_archive = now + 30
                service.schedule()
                service.archive_ready()
            for _ in range(4):
                if stop.is_set() or not service.sync_one():
                    break
            for _ in range(4):
                if stop.is_set() or not service.workspace_sync_one():
                    break
        except Exception:
            logging.exception('Tool management background work deferred')
        stop.wait(5)

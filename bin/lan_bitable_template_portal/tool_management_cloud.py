"""Read the existing tool inventory and write only the configured ledgers."""

import datetime as dt
import logging
import re
import time

from upload_event_module.services.feishu_service import _stable_uuid4_client_token

from .risk_inspection import RiskError
from .tool_management import DESTINATIONS, OUTCOMES
from .tool_self_check import BUILDINGS, TZ


BASE = 'Ix2Db9hMfaZacms9cxdcBdnJnMc'
INVENTORY = 'tbl8pYoYy88r2OSx'
ORGANIZATION = 'tblxLMZDeBbA6Wn2'
TABLES = {'maintenance': 'tblutFXoTA0mPkt1', 'inspection': 'tblHtl0cVXn6zci6',
          'repair': 'tblTasmHUxgdtZQd', 'scrap': 'tbl71QoG0R0Q3esn', 'change': 'tblEwY3AbwiT021v'}
KEY = '灯塔工具工单号'
STATE = '灯塔处理状态'
DETAIL = '灯塔处理说明'
SOURCE_FIELDS = ['楼栋', '工具名称', '规格型号', '放置区域', '一级分类', '区域对应工具数量',
                 '维护周期', '送检周期', '当前维护日期', '当前送检日期', '是否报废']


def plain(value):
    if isinstance(value, list):
        return ''.join(map(plain, value))
    if isinstance(value, dict):
        return plain(value.get('value', value.get('text', '')))
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


class ToolCloud:
    def __init__(self, cloud):
        self.cloud = cloud
        self.fields = {}

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
        remote_id = job['remote_id'] or self.find(path, key)
        params = {'user_id_type': 'open_id'}
        if remote_id:
            result = self.cloud.request('PUT', path + '/records/' + remote_id, params=params, json={'fields': fields})
        else:
            params['client_token'] = _stable_uuid4_client_token('tool-management:' + key)
            result = self.cloud.request('POST', path + '/records', params=params, json={'fields': fields})
        recorded = (result.get('record') or {}).get('record_id')
        if not recorded or remote_id and remote_id != recorded:
            raise RiskError('飞书未确认工具台账写入，保留原任务重试。', 503)
        return recorded


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
        except Exception:
            logging.exception('Tool management background work deferred')
        stop.wait(5)

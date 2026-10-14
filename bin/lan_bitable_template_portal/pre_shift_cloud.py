"""Bounded roster reads and idempotent checklist uploads using the shared HTTP pool."""
import datetime as dt
import json
import re
import uuid

from .pre_shift_checks import CheckError, SCOPES, TZ

APP_TOKEN = 'MliKbC3fXa8PXrsndKscmxjdn1g'
TABLE_ID = 'tbloxEhpBIIW3Joq'
SCHEMA = {'文本': 1, '检查标识': 1, '楼栋': 1, '检查日期': 5, '班次': 1, '提交人员': 1,
          '提交人员OpenID': 1, '提交时间': 5, '检查项数': 2, '无异常项数': 2,
          '异常项数': 2, '检查明细': 1, '异常备注': 1, '清单版本': 1}


def read_roster(service, day):
    from .learning_reminders import APP_TOKEN as roster_app, TABLE_ID as roster_table, VIEW_ID
    groups, cursor, seen = {}, '', set()
    for _ in range(20):
        response = service._request_json('records', app_token=roster_app, table_id=roster_table,
            params={'page_size': 500, 'view_id': VIEW_ID, 'user_id_type': 'open_id',
                    **({'page_token': cursor} if cursor else {})})
        data = response.get('data') or {}
        if response.get('code') != 0 or not isinstance(data.get('items'), list) or type(data.get('has_more')) is not bool:
            raise CheckError('排班视图内容不完整。')
        for row in data['items']:
            fields = row.get('fields') or {}
            shift = fields.get('班次')
            if shift not in {'白', '夜'} or fields.get('班组') == '长白':
                continue
            try:
                date = dt.datetime.fromtimestamp(float(fields['排班日期']) / 1000, TZ).date().isoformat()
            except (KeyError, TypeError, ValueError, OverflowError, OSError):
                raise CheckError('排班日期无效。') from None
            if date != day:
                continue
            building = fields.get('机楼')
            scope = building.removesuffix('楼').removesuffix('站') if isinstance(building, str) else ''
            if scope not in SCOPES:
                raise CheckError('排班楼栋不明确。')
            people = fields.get('人员') or []
            people = people.get('users', []) if isinstance(people, dict) else people
            if not isinstance(people, list):
                raise CheckError('排班人员格式无效。')
            group = groups.setdefault((scope, shift), {})
            for person in people:
                oid = person.get('id') or person.get('open_id') if isinstance(person, dict) else ''
                if not isinstance(oid, str) or not re.fullmatch(r'ou_[A-Za-z0-9]+', oid):
                    raise CheckError('排班人员缺少有效OpenID。')
                group[oid] = {'scope': scope, 'shift': shift, 'open_id': oid, 'name': str(person.get('name') or '')}
        if not data['has_more']:
            if not groups:
                raise CheckError('视图中未读取到当天白夜班人员。')
            warnings = {}
            for (scope, shift), people in groups.items():
                if len(people) != 2:
                    warnings[scope] = warnings.get(scope, '') + f'{shift}班排班为{len(people)}人，需核对两名值班人员，暂不推送该班。'
            return {'people': [person for people in groups.values() if len(people) == 2 for person in people.values()], 'warnings': warnings}
        cursor = data.get('page_token')
        if not isinstance(cursor, str) or not cursor or cursor in seen:
            break
        seen.add(cursor)
    raise CheckError('排班分页未完成。')


class CheckCloud:
    def __init__(self, service):
        self.service = service
        self.schema_ready = False

    def request(self, method, path, body=None, params=None):
        from .portal_service import TOKEN_ERROR_CODES, external_real_write_guard, refresh_feishu_token
        read = method == 'GET' or path == 'records/search'
        if not read:
            guard = external_real_write_guard()
            if not guard.get('real_write_allowed'):
                raise CheckError(guard.get('reason') or '未允许飞书写入。')
        url = f'https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/{path}'
        for attempt in range(2):
            payload = self.service._http_client.request_json(method, url, headers=self.service._auth_headers(),
                params=params or {}, json_payload=body, retries=1 if read else 0)
            if not isinstance(payload, dict) or type(payload.get('code')) is not int:
                raise CheckError('飞书返回不完整，已保留本地结果。')
            if payload['code'] in TOKEN_ERROR_CODES and not attempt:
                refresh_feishu_token()
                continue
            if payload['code'] != 0 or not isinstance(payload.get('data'), dict):
                raise CheckError(f"班前检查飞书请求未成功：code={payload['code']}")
            return payload['data']
        raise CheckError('飞书授权暂不可用。')

    def all(self, path, body=None):
        items, cursor, seen = [], '', set()
        for _ in range(100):
            data = self.request('GET' if body is None else 'POST', path, body,
                                {'page_size': 100, **({'page_token': cursor} if cursor else {})})
            if not isinstance(data.get('items'), list) or type(data.get('has_more')) is not bool:
                raise CheckError('飞书分页不完整，未再次新增。')
            items.extend(data['items'])
            if not data['has_more']:
                return items
            cursor = data.get('page_token')
            if not isinstance(cursor, str) or not cursor or cursor in seen:
                break
            seen.add(cursor)
        raise CheckError('飞书分页未完成。')

    def ensure_schema(self):
        fields = {row['field_name']: row['type'] for row in self.all('fields')}
        for name, kind in SCHEMA.items():
            if name in fields and fields[name] != kind:
                raise CheckError(f'班前检查表字段类型不匹配：{name}')
            if name not in fields:
                self.request('POST', 'fields', {'field_name': name, 'type': kind})
        self.schema_ready = True

    def upsert(self, key, tasks, record_id=''):
        if not self.schema_ready:
            self.ensure_schema()
        first = tasks[0]
        lines, abnormal, count = [], [], 0
        for task in tasks:
            lines.append(task['shift'] + '班 · ' + task['submitted'])
            for index, answer in enumerate(json.loads(task['answers']), 1):
                count += 1
                label = '异常' if answer['status'] == 'abnormal' else '无异常'
                line = f"{index}. {answer['text']}：{label}" + (f"；{answer['note']}" if answer['note'] else '')
                lines.append(line)
                if answer['status'] == 'abnormal':
                    abnormal.append(task['shift'] + '班 · ' + line)
        fields = {'文本': f"{first['day']} {first['scope']}楼 {first['actor_name']} 班前检查", '检查标识': key, '楼栋': first['scope'], '检查日期': int(dt.datetime.combine(dt.date.fromisoformat(first['day']), dt.time(), TZ).timestamp() * 1000),
            '班次': '、'.join(t['shift'] for t in tasks), '提交人员': first['actor_name'], '提交人员OpenID': first['actor_id'],
            '提交时间': max(int(dt.datetime.fromisoformat(t['submitted']).timestamp() * 1000) for t in tasks),
            '检查项数': count, '无异常项数': count - len(abnormal), '异常项数': len(abnormal),
            '检查明细': '\n'.join(lines), '异常备注': '\n'.join(abnormal),
            '清单版本': '、'.join(f"{t['shift']}班 v{t['definition_version']}" for t in tasks)}
        if not record_id:
            records = self.all('records/search', {'filter': {'conjunction': 'and', 'conditions': [
                {'field_name': '检查标识', 'operator': 'is', 'value': [key]}]}})
            if len(records) > 1:
                raise CheckError('发现重复检查记录，需要管理员核对；未覆盖。')
            if records:
                record_id = records[0].get('record_id', '')
                if not record_id:
                    raise CheckError('飞书返回缺少记录ID。')
        if record_id:
            self.request('PUT', 'records/' + record_id, {'fields': fields})
            return record_id
        data = self.request('POST', 'records', {'fields': fields}, {'client_token': str(uuid.uuid5(uuid.NAMESPACE_URL, key))})
        record_id = data.get('record', {}).get('record_id')
        if not isinstance(record_id, str) or not re.fullmatch(r'rec[A-Za-z0-9]+', record_id):
            raise CheckError('飞书写入结果尚未确认，稍后按检查标识核验。')
        return record_id

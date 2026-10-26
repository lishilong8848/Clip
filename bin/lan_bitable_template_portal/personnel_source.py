"""Authoritative personnel and dated snapshots of the table's TODAY() shift."""
import copy
import datetime as dt
import json
import re
import threading
import time
from functools import lru_cache
from pathlib import Path

APP_TOKEN = 'F9lDbA5XKaLmyasoARRcZFpKnZd'
TABLE_ID = 'tblOu75UtJTDNHIM'
URL = f'https://vnet.feishu.cn/base/{APP_TOKEN}?table={TABLE_ID}'
EXTERNAL_APP_TOKEN = 'HU38bc1vnamMK9sCeOgclUvXnFc'
EXTERNAL_TABLE_ID = 'tblC77nllNrprHBY'
ROSTER_NAMESPACE = 'personnel_daily_roster'
TZ = dt.timezone(dt.timedelta(hours=8))
ROSTER_FIELDS = ['姓名', '员工姓名', '员工工号', '机楼/专业', '体系岗位', '岗位',
                 '账号性质', '离职/异动情况', '班组', '班次']
SIGNATURE_FIELDS = [*ROSTER_FIELDS, '手写签名', '密钥']
_lock = threading.RLock()


@lru_cache(maxsize=1)
def signature_aliases():
    # Existing orders keep their person references; the cloud needs no extra column.
    value = json.loads(Path(__file__).with_name('personnel_signature_ids.json').read_text(encoding='utf-8'))
    aliases = value['aliases']
    if value['table_id'] != TABLE_ID or len(set(aliases.values())) != len(aliases):
        raise ValueError('签名人员对应关系无效。')
    return aliases


def signature_person_id(record_id):
    return next((old for old, current in signature_aliases().items() if current == record_id), record_id)


def values(value):
    if isinstance(value, dict):
        return values(value.get('value', value.get('text', value.get('name', ''))))
    if isinstance(value, list):
        return [part for item in value for part in values(item)]
    return [str(value).strip()] if value not in (None, '') and str(value).strip() else []


def text(value):
    return '、'.join(values(value))


def shift_kind(fields):
    shifts = list(dict.fromkeys(values(fields.get('班次'))))
    if not shifts:
        return ''
    group = text(fields.get('班组'))
    if len(shifts) == 1:
        if group == '长白' and shifts[0] == '白':
            return '长白'
        if group in {'A班', 'B班', 'C班', 'D班'} and shifts[0] in {'白', '夜'}:
            return shifts[0]
    return '待核对'


def read_records(service, *, base=APP_TOKEN, table=TABLE_ID, fields=ROSTER_FIELDS):
    from upload_event_module.config import config
    app_id = config.app_id
    rows, seen, cursor = [], set(), ''
    for _ in range(100):
        params = {'page_size': 500, 'user_id_type': 'open_id', 'field_names': json.dumps(list(fields), ensure_ascii=False),
                  **({'page_token': cursor} if cursor else {})}
        if config.app_id != app_id:
            raise ValueError('Qt飞书应用已变化，请重新读取人员资料。')
        reply = service._request_json('records', app_token=base, table_id=table, params=params)
        data = reply.get('data') or {}
        if reply.get('code') != 0 or not isinstance(data.get('items'), list) or type(data.get('has_more')) is not bool:
            raise ValueError('员工信息池读取未完成（code=' + str(reply.get('code')) + '）。')
        for row in data['items']:
            rid = row.get('record_id')
            if not isinstance(rid, str) or not rid or rid in seen or not isinstance(row.get('fields'), dict):
                raise ValueError('员工信息池记录缺失或分页重复，未采用不完整资料。')
            seen.add(rid)
            rows.append(row)
        if not data['has_more']:
            if config.app_id != app_id:
                raise ValueError('Qt飞书应用已变化，请重新读取人员资料。')
            return rows
        next_cursor = data.get('page_token')
        if not isinstance(next_cursor, str) or not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
    raise ValueError('员工信息池分页未读取完成。')


def daily_roster(service, day, *, force=False):
    from upload_event_module.config import config
    from .portal_service import MaintenancePortalService
    day = dt.date.fromisoformat(day).isoformat()
    with _lock:
        cached = service._state_store.get_document(ROSTER_NAMESPACE, day) or {}
        today = dt.datetime.now(TZ).date().isoformat()
        valid = cached.get('source') == TABLE_ID and cached.get('app_id') == config.app_id and cached.get('complete') is True
        if day != today:
            if valid:
                return copy.deepcopy(cached)
            raise ValueError('员工信息池的班次仅表示今天；所选日期尚无已保存排班快照，不能套用今天的名单。')
        if valid and not force and 0 <= time.time() - cached.get('observed_at', 0) < 60:
            return copy.deepcopy(cached)
        rows = read_records(service)
        people, issues = [], []
        for row in rows:
            fields = row['fields']
            if MaintenancePortalService._signature_person_inactive(fields.get('离职/异动情况')):
                continue
            kind = shift_kind(fields)
            if not kind:
                continue
            name = text(fields.get('姓名')) or text(fields.get('员工姓名'))
            scopes = MaintenancePortalService._building_codes_from_value(fields.get('机楼/专业'))
            if kind == '待核对' or not name or len(scopes) != 1:
                issues.append({'record_id': row['record_id'], 'name': name, 'scope': scopes[0] if len(scopes) == 1 else '',
                               'reason': '班组、班次或归属楼栋待核对'})
                continue
            user = MaintenancePortalService._signature_user_info(fields.get('员工姓名'))
            oid = user.get('open_id', '')
            people.append({'record_id': row['record_id'], 'name': name, 'employee_no': text(fields.get('员工工号')),
                'open_id': oid if re.fullmatch(r'ou_[A-Za-z0-9]+', oid) else '',
                'scope': scopes[0], 'shift': '白' if kind == '长白' else kind,
                'group': text(fields.get('班组')), 'duty_kind': kind})
        # Do not label yesterday's lookup result as today's after a midnight page read.
        if dt.datetime.now(TZ).date().isoformat() != day:
            raise ValueError('人员排班读取跨越零点，请重新读取。')
        result = {'date': day, 'source': TABLE_ID, 'app_id': config.app_id, 'complete': True,
                  'observed_at': time.time(), 'people': people, 'issues': issues}
        service._state_store.put_document(ROSTER_NAMESPACE, day, result)
        return copy.deepcopy(result)

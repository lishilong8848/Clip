"""Frozen D/E inspection sheets and their Feishu archive."""

import asyncio
import datetime as dt
import hashlib
import io
import json
import logging
import re
import threading
import time
import unicodedata
import uuid
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from fastapi import Request
from fastapi.responses import JSONResponse, Response

from upload_event_module.services.feishu_service import _stable_uuid4_client_token
from upload_event_module.services.http_client import FeishuHTTPError
from .daily_inspection_history import (MAX_ARCHIVE, REVIEW_COLUMNS, REVIEW_SHEET, archived_devices,
                                       archived_workbook, edit_archived_devices, valid_cell_text)
from .eop_scenario_editor import plain
from .module_access import allowed_modules
from .risk_inspection import RiskError
from .risk_inspection_cloud import API, RiskCloud

BASE = 'ASLxbfESPahdTKs0A9NccgbrnXc'
SOURCES = {
    'D': (('tblDG0Z3fyKCpZbg', 'vewHD8zqkU'), ('tblU0q6LvNjpMIN1', 'vew6cgYCH7')),
    'E': (('tblbkTs8tfW869ry', 'vewFB8Eusk'), ('tblrUhk1NAmTg1a2', 'vewhLn8sLf')),
}
TEMPLATES = ('tblSWp4UE1ie08YA', 'vewDWZorNW')
ARCHIVE = 'tblppBlXXrGcyLkc'
ARCHIVE_VIEW = 'vewtwaJ8My'
ARCHIVE_FIELDS = {
    '文本': 1, '巡检范围': 1, '排班日期': 1, '班次': 1, '班组': 1, '巡检轮次': 2,
    '开始人员': 1, '巡检人员': 1, '开始时间': 5, '完成时间': 5,
    '源数据批次': 1, '设备数': 2, '源表记录数': 2, '巡检快照': 17,
    '现场已核对设备数': 2, '异常设备数': 2,
}
EDIT_FIELDS = {'最后修改人': 1, '最后修改时间': 5, '上版巡检快照': 17}
NS = 'daily_de_inspection'
CN = dt.timezone(dt.timedelta(hours=8))
DAY_START = dt.time(9)
NIGHT_START = dt.time(18)
ROUNDS_PER_SHIFT = 2
LOCATION_FIELDS = ('目标巡检表', '目标工作表', '目标单元格')
DEVICE_METADATA = {
    '文本', '设备编号', '楼栋', '专业', '位置', '巡检表单', '上传日期', '源文件时间', '源文件',
    '数据完整性', '缺少源数据', '备注', '记录类型', *LOCATION_FIELDS,
}


def formatted_time(value):
    return dt.datetime.fromtimestamp(value / 1000, CN).strftime('%Y-%m-%d %H:%M:%S') if value else '—'


def duty_period(current):
    date, clock = current.date(), current.time()
    if DAY_START <= clock < NIGHT_START:
        return date.isoformat(), '白'
    if clock < DAY_START:
        date -= dt.timedelta(days=1)
    return date.isoformat(), '夜'


def duty_key(duty):
    return {key: duty[key] for key in ('scope', 'date', 'shift')}


def resolve_duty(roster, user, scopes, current=None):
    from .portal_service import BUILDING_OPEN_ID_MAP

    date, shift = duty_period(current or dt.datetime.now(CN))
    try:
        snapshot = roster(date)
    except Exception as error:
        raise RiskError('本班排班快照暂不可用，请稍后刷新。', 503) from error
    if snapshot.get('date') != date or snapshot.get('complete') is not True:
        raise RiskError('本班排班快照不完整，请稍后刷新。', 503)
    people = snapshot['people']
    owner = user['open_id']
    fixed = next((scope for scope, identity in BUILDING_OPEN_ID_MAP.items() if identity == owner), '')
    if fixed:
        assignment = [person for person in people if person['scope'] == fixed
                      and person['shift'] == shift and person.get('duty_kind') != '长白']
        scope = fixed if assignment else ''
    else:
        record_id = user.get('personnel_record_id')
        assignment = [person for person in people if person.get('open_id') == owner or
                      (record_id and owner == 'person:' + record_id and person.get('record_id') == record_id
                       and not person.get('open_id'))]
        person = assignment[0] if len(assignment) == 1 else {}
        scope = person.get('scope') if person.get('shift') == shift and person.get('duty_kind') != '长白' else ''
    if scope not in scopes:
        return None
    duty = {'scope': scope, 'date': date, 'shift': shift}
    groups = sorted({person['group'] for person in people
                     if person.get('scope') == scope and person.get('shift') == shift
                     and person.get('duty_kind') != '长白' and person.get('group')})
    if groups:
        duty['group'] = '、'.join(groups)
    return duty


def key_name(scope, value):
    value = unicodedata.normalize('NFKC', value).upper()
    value = re.sub(r'\s+', '', value)
    return value.removeprefix(scope + '-')


def devices_for(scope, rows, point_configs, templates):
    devices, point_rows, configurations = {}, [], {}
    for row in point_configs:
        fields = row['fields']
        key = (plain(fields.get('专业')).strip(), plain(fields.get('测点ID')).strip())
        if key[1]:
            configurations[key] = fields if key not in configurations else {}
    for row in rows:
        fields = row['fields']
        record_type = plain(fields.get('记录类型'))
        if record_type == '测点明细':
            point_rows.append(row)
        elif record_type == '设备巡检':
            name = plain(fields.get('设备编号')).strip()
            profession = plain(fields.get('专业')).strip()
            if not name:
                raise RiskError('设备巡检记录缺少设备编号。', 503)
            key = (profession, key_name(scope, name))
            device = devices.setdefault(key, {'id': f'{scope}:{key[0]}:{key[1]}', 'name': name, 'scope': scope,
                                               'profession': profession, 'sections': [], 'points': []})
            device['sections'].append({
                'location': plain(fields.get('位置')),
                'form': plain(fields.get('巡检表单')),
                'integrity': plain(fields.get('数据完整性')),
                'missing': plain(fields.get('缺少源数据')),
                'note': plain(fields.get('备注')),
                'values': {name: value if isinstance(value, (int, float)) else plain(value)
                           for name, value in fields.items() if name not in DEVICE_METADATA and value not in (None, '')},
            })
        else:
            raise RiskError('源表出现无法识别的巡检记录类型，请核对后重试。', 503)
    primary = set(devices)
    for row in point_rows:
        fields = row['fields']
        profession = plain(fields.get('专业')).strip()
        name = plain(fields.get('设备编号')).strip()
        location = plain(fields.get('位置'))
        point_id = plain(fields.get('测点ID')).strip()
        if not name or not point_id:
            raise RiskError('测点记录缺少设备编号或测点 ID。', 503)
        exact = (profession, key_name(scope, name))
        aliases = {key_name(scope, value) for value in (name.split('_', 1)[0], location.rsplit('/', 1)[-1])}
        candidates = {(profession, value) for value in aliases} & primary
        key = exact if exact in primary or len(candidates) != 1 else next(iter(candidates))
        device = devices.setdefault(key, {'id': f'{scope}:{key[0]}:{key[1]}', 'name': name, 'scope': scope,
                                           'profession': profession, 'sections': [], 'points': []})
        config = configurations.get((profession, point_id), {})
        template, sheet, cell = [plain(config.get(k)) or plain(fields.get(k)) for k in LOCATION_FIELDS]
        reading = fields.get('测点值')
        device['points'].append({
            'id': row['record_id'], 'point_id': point_id,
            'name': plain(config.get('测点名称')) or plain(fields.get('测点名称')) or point_id,
            'location': location, 'path': plain(config.get('测点路径')) or plain(fields.get('测点路径')),
            'value': reading if isinstance(reading, (int, float)) else plain(reading),
            'sampled': fields.get('采样时间'), 'integrity': plain(fields.get('数据完整性')),
            'enabled': config.get('启用'), 'baseline': config.get('基线测点'),
            'note': plain(config.get('测点备注')) or plain(fields.get('测点备注')),
            'template': template, 'sheet': sheet, 'cell': cell,
            'template_known': template in templates.get((scope, profession), []),
        })
    return list(devices.values())


def parameters(device):
    rows = []
    for index, section in enumerate(device['sections']):
        rows.extend({'id': f's:{index}:{name}', 'name': name, 'reference': value, 'section': index}
                    for name, value in section['values'].items())
    rows.extend({'id': 'p:' + point['id'], 'name': point['name'], 'reference': point['value'],
                 'point': point['id'], 'read_only': point.get('enabled') is False}
                for point in device['points'])
    return rows


def review_content(review):
    return {key: review.get(key, default) for key, default in
            (('values', {}), ('result', ''), ('note', ''))}


def merge_reviews(active, devices, submitted, actor):
    if not isinstance(submitted, dict):
        raise RiskError('巡检填写资料格式无效。', 400)
    known = {device['id']: device for device in devices}
    reviews = dict(active.get('reviews', {}))
    changed = False
    for identity, review in submitted.items():
        device = known.get(identity)
        if not device or not isinstance(review, dict) or type(review.get('revision')) is not int:
            raise RiskError('设备清单或填写版本已变化，请刷新后重试。', 409)
        values, result, note = (review.get(key) for key in ('values', 'result', 'note'))
        fields = {item['id'] for item in parameters(device) if not item.get('read_only')}
        if (not isinstance(values, dict) or values.keys() - fields
                or any(not valid_cell_text(value, 160) for value in values.values())
                or result not in ('', '正常', '异常') or not valid_cell_text(note, 1000)):
            raise RiskError(f'「{device["name"]}」的现场填写无效，请核对后重试。', 400)
        normalized = {'values': {key: value.strip() for key, value in values.items() if value.strip()},
                      'result': result, 'note': note.strip()}
        previous = reviews.get(identity, {})
        if review_content(previous) == normalized:
            continue
        revision = previous.get('revision', 0)
        if revision != review['revision']:
            raise RiskError(f'「{device["name"]}」已被其他页面更新，请刷新并核对。', 409)
        reviews[identity] = {**normalized, 'revision': revision + 1, 'actor': actor}
        changed = True
    return reviews, active.get('review_version', 0) + int(changed)


def require_complete(devices, reviews):
    if not devices:
        raise RiskError('没有可归档的巡检设备。', 409)
    for device in devices:
        review = review_content(reviews.get(device['id'], {}))
        if (any(not valid_cell_text(value, 160) for value in review['values'].values())
                or not valid_cell_text(review['note'], 1000)):
            raise RiskError(f'「{device["name"]}」的读数或备注含无效字符，请重新填写。', 400)
        missing = next((item['name'] for item in parameters(device) if not item.get('read_only')
                        and not review['values'].get(item['id'])), '')
        if missing:
            raise RiskError(f'「{device["name"]}」缺少「{missing}」现场读数。', 409)
        if not review['result'] or (review['result'] == '异常' and not review['note']):
            raise RiskError(f'请填写「{device["name"]}」的核对结论及异常备注。', 409)


class DailyInspection:
    def __init__(self, store, cloud):
        self.store, self.cloud = store, cloud
        self.locks = {scope: threading.RLock() for scope in SOURCES}
        self.schema_lock = threading.Lock()

    def path(self, table):
        return self.cloud.table_path(BASE, table)

    def records(self, table, query, page_size=500):
        rows, ids, cursors = [], set(), set()
        cursor, total = None, None
        for _ in range(50):
            params = {'page_size': page_size}
            if cursor:
                params['page_token'] = cursor
            result = self.cloud.request('POST', self.path(table) + '/records/search', params=params, json=query)
            items, count = result.get('items'), result.get('total')
            if (not isinstance(items, list) or type(count) is not int or type(result.get('has_more')) is not bool
                    or (total is not None and total != count)):
                break
            total = count
            for row in items:
                rid = row.get('record_id')
                if not rid or rid in ids or not isinstance(row.get('fields'), dict):
                    raise RiskError('飞书巡检记录不完整，请重试。', 503)
                rows.append({'record_id': rid, 'fields': row['fields']})
                ids.add(rid)
            if not result['has_more']:
                if len(rows) == total:
                    return rows
                break
            cursor = result.get('page_token')
            if not cursor or cursor in cursors:
                break
            cursors.add(cursor)
        raise RiskError('巡检表正在更新或分页未读完，请稍后重试。', 503)

    def capture(self, scopes):
        template_rows = self.records(TEMPLATES[0], {'view_id': TEMPLATES[1]})
        templates = {}
        for row in template_rows:
            fields = row['fields']
            scope = plain(fields.get('楼栋')).removesuffix('楼')
            if scope in scopes:
                names = [item['name'] for item in (fields.get('附件') or []) if isinstance(item, dict) and item.get('name')]
                templates.setdefault((scope, plain(fields.get('专业'))), []).extend(names)
        batches = {}
        for scope in scopes:
            (table, view), (point_table, point_view) = SOURCES[scope]
            points = self.records(point_table, {'view_id': point_view})
            rows = self.records(table, {'view_id': view})
            dates = {row['fields'].get('上传日期') for row in rows}
            if len(dates) != 1 or not all(type(value) is int and value > 0 for value in dates):
                raise RiskError(f'{scope}楼源表没有完整的单一批次，请等刷新完成后重试。', 409)
            batch = next(iter(dates))
            if any(plain(row['fields'].get('楼栋')).strip() != scope + '楼' for row in rows):
                raise RiskError(f'{scope}楼源表含其它楼栋记录，暂未开始巡检。', 409)
            head = self.cloud.request('POST', self.path(table) + '/records/search', params={'page_size': 1},
                                      json={'view_id': view, 'field_names': ['上传日期']})
            if head.get('total') != len(rows) or not head.get('items') or head['items'][0]['fields'].get('上传日期') != batch:
                raise RiskError(f'{scope}楼读取期间切换了批次，请重试。', 409)
            batches[scope] = {'batch': batch, 'rows': rows, 'devices': devices_for(scope, rows, points, templates)}
        return batches

    def load(self, scope):
        return self.store.get_document(NS, 'building:' + scope) or {'active': None, 'history': []}

    def order(self, owner, scope):
        order = self.store.get_document(NS, f'order:{scope}:{owner}')
        if order is not None:
            return order['ids']
        legacy = self.store.get_document(NS, owner) or {}
        return legacy.get('orders', {}).get(scope, [])

    @staticmethod
    def current(state, inspection_id):
        active = state.get('active')
        if not active or active['id'] != inspection_id:
            raise RiskError('当前巡检已变化，请重新打开页面。', 409)
        return active

    @staticmethod
    def ordered(batch, order):
        positions = {identity: i for i, identity in enumerate(order)}
        return sorted(batch['devices'], key=lambda device: positions.get(device['id'], len(order)))

    @staticmethod
    def completed_rounds(state, duty):
        return sum(duty_key(row['duty']) == duty_key(duty) for row in state['history'])

    def view(self, state, owner, duty):
        active = state.get('active')
        scope = duty['scope']
        result = {'duty': duty, 'history': state['history'], 'completed_rounds': self.completed_rounds(state, duty),
                  'round_limit': ROUNDS_PER_SHIFT, 'active': None}
        if active:
            batch = active['batches'][scope]
            result['active'] = {'id': active['id'], 'duty': active['duty'], 'round': active['round'],
                                'started': active['started'], 'pending': 'completed' in active,
                                'draft_key': hashlib.sha256(f'{owner}:{active["id"]}'.encode()).hexdigest(),
                                'reviews': active.get('reviews', {}), 'review_version': active.get('review_version', 0),
                                'batch': {'batch': batch['batch'], 'count': len(batch['rows']),
                                          'devices': [{**device, 'parameters': parameters(device)}
                                                      for device in self.ordered(batch, self.order(owner, scope))]}}
        return result

    def change(self, owner, actor, duty, action, data):
        scope = duty['scope']
        state_key = 'building:' + scope
        with self.locks[scope]:
            state = self.load(scope)
            if action == 'start':
                submitted_duty = data.get('duty')
                if not isinstance(submitted_duty, dict) or any(submitted_duty.get(key) != value
                                                               for key, value in duty_key(duty).items()):
                    raise RiskError('当班安排已变化，请刷新后开始巡检。', 409)
                if state['active']:
                    raise RiskError('本楼已有一次巡检，请刷新后继续。', 409)
                completed = self.completed_rounds(state, duty)
                if completed >= ROUNDS_PER_SHIFT:
                    raise RiskError('本楼本班两次巡检均已完成。', 409)
                state['active'] = {'id': uuid.uuid4().hex, 'actor': actor, 'started_by': actor,
                                   'started': int(time.time() * 1000), 'duty': duty, 'round': completed + 1,
                                   'batches': self.capture([scope])}
            elif action == 'cancel':
                active = self.current(state, data.get('id'))
                if ('completed' in active or type(data.get('review_version')) is not int
                        or data['review_version'] != active.get('review_version', 0)):
                    raise RiskError('当前巡检资料已变化或正在归档，请重新核对。', 409)
                state['active'] = None
            else:
                if action == 'finish' and any(row['id'] == data.get('id') for row in state['history']):
                    return self.view(state, owner, duty)
                active = self.current(state, data.get('id'))
                devices = active['batches'][scope]['devices']
                if action == 'order':
                    ids = data.get('order')
                    if 'completed' in active:
                        raise RiskError('当前楼栋无法调整顺序。', 409)
                    known = {row['id'] for row in devices}
                    if (not isinstance(ids, list) or any(not isinstance(value, str) for value in ids)
                            or len(ids) != len(known) or set(ids) != known):
                        raise RiskError('设备列表已变化，请重新打开再排序。', 409)
                    self.store.put_document(NS, f'order:{scope}:{owner}', {'ids': ids})
                    return self.view(state, owner, duty)
                else:
                    if 'completed' not in active:
                        if action == 'finish' and (type(data.get('review_version')) is not int or
                                                   data['review_version'] != active.get('review_version', 0)):
                            raise RiskError('本轮核对资料已变化，请刷新并核对后再归档。', 409)
                        reviews, version = merge_reviews(active, devices, data.get('reviews', {}), actor)
                        if action == 'finish':
                            require_complete(devices, reviews)
                        active.update(reviews=reviews, review_version=version)
                    elif action != 'finish':
                        raise RiskError('本轮已进入归档，现场数据已锁定。', 409)
                    if action == 'finish':
                        self.finish(state_key, state, active, actor, self.order(owner, scope))
            self.store.put_document(NS, state_key, state)
            return self.view(state, owner, duty)

    def ensure_archive_fields(self, specs=ARCHIVE_FIELDS):
        with self.schema_lock:
            path = self.path(ARCHIVE) + '/fields'
            existing = {row['field_name']: row['type'] for row in self.cloud.all(path)}
            for name, kind in specs.items():
                if name in existing and existing[name] != kind:
                    raise RiskError(f'巡检记录表的「{name}」字段类型不匹配。', 409)
            for name, kind in specs.items():
                if name not in existing:
                    self.cloud.request('POST', path, json={'field_name': name, 'type': kind})

    def upload_archive(self, content, filename):
        uploaded = self.cloud.request('POST', 'drive/v1/medias/upload_all',
            data={'file_name': filename, 'parent_type': 'bitable_file', 'parent_node': BASE, 'size': str(len(content))},
            files={'file': (filename, content, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')})
        token = uploaded.get('file_token')
        if not isinstance(token, str) or not re.fullmatch(r'[A-Za-z0-9_-]+', token):
            raise RiskError('飞书未确认巡检附件上传，请重试。', 503)
        return token

    @staticmethod
    def archive_summary(row, scope):
        fields = row['fields']
        key = plain(fields.get('文本'))
        if not re.fullmatch(r'DE巡检 [a-f0-9]{32}', key) or plain(fields.get('巡检范围')) != scope + '楼':
            return None
        rid = row['record_id']
        return {'id': rid, 'round_id': key.split(' ', 1)[1], 'scope': scope,
                'date': plain(fields.get('排班日期')), 'shift': plain(fields.get('班次')),
                'group': plain(fields.get('班组')), 'round': fields.get('巡检轮次'),
                'completed': fields.get('完成时间'), 'actor': plain(fields.get('巡检人员')),
                'modified': fields.get('最后修改时间'), 'editor': plain(fields.get('最后修改人')),
                'confirmed': fields.get('现场已核对设备数'), 'devices': fields.get('设备数'),
                'abnormal': fields.get('异常设备数'),
                'url': f'https://vnet.feishu.cn/base/{BASE}?table={ARCHIVE}&view={ARCHIVE_VIEW}&record={rid}'}

    def archive_history(self, scope, day):
        try:
            day = dt.date.fromisoformat(day).isoformat()
        except (ValueError, TypeError):
            raise RiskError('请选择有效巡检日期。', 400) from None
        rows = self.records(ARCHIVE, {'filter': {'conjunction': 'and', 'conditions': [
            {'field_name': '巡检范围', 'operator': 'is', 'value': [scope + '楼']},
            {'field_name': '排班日期', 'operator': 'is', 'value': [day]}]}})
        summaries = [self.archive_summary(row, scope) for row in rows]
        return sorted((row for row in summaries if row and row['date'] == day),
                      key=lambda row: row['completed'] or 0, reverse=True)

    def archive_record(self, scope, rid):
        if not isinstance(rid, str) or not re.fullmatch(r'rec[A-Za-z0-9]{1,80}', rid):
            raise RiskError('巡检记录编号无效。', 400)
        path = self.path(ARCHIVE) + '/records/' + rid
        row = self.cloud.request('GET', path).get('record') or {}
        summary = self.archive_summary(row, scope) if row.get('record_id') == rid and row.get('fields') else None
        if not summary:
            raise RiskError('找不到本楼的巡检归档。', 404)
        attachments = row['fields'].get('巡检快照')
        if not isinstance(attachments, list) or len(attachments) != 1 or not isinstance(attachments[0], dict):
            raise RiskError('归档缺少唯一的巡检附件，请在飞书核对。', 409)
        attachment = attachments[0]
        token = attachment.get('file_token')
        if not isinstance(token, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,180}', token):
            raise RiskError('归档附件标识无效。', 409)
        return path, summary, attachment, row['fields']

    def download_archive(self, attachment):
        extra = parse_qs(urlsplit(attachment.get('url') or '').query).get('extra', [''])[0]
        with self.cloud.lock:
            headers = self.cloud.headers()
        try:
            content, _ = self.cloud.http.request_bytes('GET',
                API + 'drive/v1/medias/' + attachment['file_token'] + '/download',
                headers=headers, params={'extra': extra} if extra else None, max_bytes=MAX_ARCHIVE)
        except FeishuHTTPError:
            raise RiskError('未能下载飞书巡检附件，请核对附件权限或稍后重试。', 503) from None
        return content

    @staticmethod
    def archive_version(attachment):
        return hashlib.sha256(attachment['file_token'].encode()).hexdigest()

    def archive_detail(self, scope, rid):
        _, summary, attachment, _ = self.archive_record(scope, rid)
        return self.read_archive_detail(summary, attachment)

    def read_archive_detail(self, summary, attachment):
        with archived_workbook(self.download_archive(attachment), summary['round_id'], summary['scope']) as book:
            devices = archived_devices(book, summary['scope'])
        return {**summary, 'version': self.archive_version(attachment), 'devices_data': devices}

    @staticmethod
    def verify_archive_fields(fields, expected, error_message='飞书尚未完整确认修改，请重试保存。'):
        for name, value in expected.items():
            actual = fields.get(name)
            if name in ('巡检快照', '上版巡检快照'):
                matches = (isinstance(actual, list) and len(actual) == 1 and isinstance(actual[0], dict)
                           and actual[0].get('file_token') == value[0]['file_token'])
            else:
                matches = (plain(actual) if isinstance(value, str) else actual) == value
            if not matches:
                raise RiskError(error_message, 503)

    def apply_archive_edit(self, scope, rid, path, current_version, expected):
        _, _, latest, _ = self.archive_record(scope, rid)
        if self.archive_version(latest) != current_version:
            raise RiskError('这条巡检已被修改，请重新打开后核对最新数据。', 409)
        self.cloud.request('PUT', path, json={'fields': expected})
        _, summary, attachment, fields = self.archive_record(scope, rid)
        self.verify_archive_fields(fields, expected)
        return summary, attachment

    def edit_archive(self, scope, rid, payload, actor):
        version, changes = payload.get('version'), payload.get('changes')
        fingerprint = hashlib.sha256(json.dumps([version, changes], sort_keys=True).encode()).hexdigest()
        key = 'history-edit:' + rid
        with self.locks[scope]:
            path, summary, attachment, fields = self.archive_record(scope, rid)
            active = self.load(scope).get('active')
            if active and active['id'] == summary['round_id']:
                raise RiskError('本轮归档尚未确认，请先在本班页面完成归档重试。', 409)
            current_version = self.archive_version(attachment)
            pending = self.store.get_document(NS, key) or {}
            pending_is_current = pending.get('token') == attachment['file_token']
            same_edit = pending.get('fingerprint') == fingerprint and (pending_is_current or not pending.get('confirmed'))
            if pending_is_current and (same_edit or not pending.get('confirmed')):
                self.verify_archive_fields(fields, pending['fields'])
                if not pending.get('confirmed'):
                    pending['confirmed'] = True
                    self.store.put_document(NS, key, pending)
                if same_edit:
                    return self.read_archive_detail(summary, attachment)
            if version != current_version:
                raise RiskError('这条巡检已被修改，请重新打开后核对最新数据。', 409)
            if not same_edit:
                if (pending and not pending.get('confirmed')
                        and pending['fields']['上版巡检快照'][0]['file_token'] == attachment['file_token']):
                    self.apply_archive_edit(scope, rid, path, current_version, pending['fields'])
                    self.store.put_document(NS, key, {**pending, 'confirmed': True})
                    raise RiskError('上一份修改已确认，请重新打开记录后再修改。', 409)
                with archived_workbook(self.download_archive(attachment), summary['round_id'], scope) as book:
                    devices = archived_devices(book, scope)
                    confirmed, abnormal = edit_archived_devices(book, devices, changes, actor)
                    output = io.BytesIO()
                    book.save(output)
                self.ensure_archive_fields({**ARCHIVE_FIELDS, **EDIT_FIELDS})
                token = self.upload_archive(output.getvalue(), 'DE巡检修订-' + summary['round_id'][:10] + '.xlsx')
                pending = {'fingerprint': fingerprint, 'token': token, 'fields': {
                    '巡检快照': [{'file_token': token}],
                    '上版巡检快照': [{'file_token': attachment['file_token']}],
                    '最后修改人': actor, '最后修改时间': int(time.time() * 1000),
                    '现场已核对设备数': confirmed, '异常设备数': abnormal}}
                self.store.put_document(NS, key, pending)
            updated, saved = self.apply_archive_edit(scope, rid, path, current_version, pending['fields'])
            self.store.put_document(NS, key, {**pending, 'confirmed': True})
            return self.read_archive_detail(updated, saved)

    def finish(self, state_key, state, active, actor, order):
        content = None
        if 'completed' not in active:
            active['completed'] = int(time.time() * 1000)
            active['actor'] = actor
            active['archive_order'] = order
            content = workbook(active, {active['duty']['scope']: order})
            self.store.put_document(NS, state_key, state)
        self.ensure_archive_fields()
        path = self.path(ARCHIVE) + '/records'
        archive_key = 'DE巡检 ' + active['id']
        existing = self.records(ARCHIVE, {'filter': {'conjunction': 'and', 'conditions': [
            {'field_name': '文本', 'operator': 'is', 'value': [archive_key]}]}}, page_size=20)
        if len(existing) > 1:
            raise RiskError('巡检编号对应多条存档，请管理员核对。', 409)
        scopes = list(active['batches'])
        reviews = active.get('reviews')
        counts = {} if reviews is None else {
            '现场已核对设备数': len(reviews),
            '异常设备数': sum(review['result'] == '异常' for review in reviews.values()),
        }
        expected = {
            '文本': archive_key,
            '巡检范围': '、'.join(scope + '楼' for scope in scopes),
            '排班日期': active['duty']['date'],
            '班次': active['duty']['shift'] + '班',
            '巡检轮次': active['round'],
            **counts,
        }
        if active['duty'].get('group'):
            expected['班组'] = active['duty']['group']
        if existing:
            rid = existing[0]['record_id']
        else:
            if not active.get('file_token'):
                if content is None:
                    content = workbook(active, {active['duty']['scope']: active['archive_order']})
                filename = 'DE巡检表-' + active['id'][:10] + '.xlsx'
                active['file_token'] = self.upload_archive(content, filename)
                self.store.put_document(NS, state_key, state)
            fields = {
                **expected,
                '开始人员': active.get('started_by') or active['actor'],
                '巡检人员': active['actor'],
                '开始时间': active['started'],
                '完成时间': active['completed'],
                '源数据批次': '；'.join(scope + '楼 ' + formatted_time(batch['batch'])
                                    for scope, batch in active['batches'].items()),
                '设备数': sum(len(batch['devices']) for batch in active['batches'].values()),
                '源表记录数': sum(len(batch['rows']) for batch in active['batches'].values()),
                '巡检快照': [{'file_token': active['file_token']}],
            }
            response = self.cloud.request('POST', path, params={'client_token': _stable_uuid4_client_token(archive_key)},
                                          json={'fields': fields})
            rid = (response.get('record') or {}).get('record_id')
        if not isinstance(rid, str) or not re.fullmatch(r'rec[A-Za-z0-9]+', rid):
            raise RiskError('飞书未确认巡检记录，请重试本次归档。', 503)
        error_message = '巡检存档回读未确认，请重试核对原记录。'
        record = self.cloud.request('GET', path + '/' + rid).get('record') or {}
        if record.get('record_id') != rid or not active.get('file_token'):
            raise RiskError(error_message, 503)
        self.verify_archive_fields(record.get('fields') or {},
                                   {**expected, '巡检快照': [{'file_token': active['file_token']}]}, error_message)
        state['history'] = ([{'id': active['id'], 'duty': active['duty'], 'round': active['round'],
                              'actor': active['actor'], 'completed': active['completed'],
                              'url': f'https://vnet.feishu.cn/base/{BASE}?table={ARCHIVE}&view={ARCHIVE_VIEW}&record={rid}'}] + state['history'])[:40]
        state['active'] = None


def workbook(active, orders):
    from openpyxl import Workbook
    from openpyxl.cell import WriteOnlyCell
    from openpyxl.styles import Font, PatternFill

    book = Workbook(write_only=True)

    def sheet(name, columns, rows):
        target = book.create_sheet(name)
        target.freeze_panes = 'A2'
        def cell(value):
            if isinstance(value, (dict, list)):
                value = plain(value)
            result = WriteOnlyCell(target, value=value)
            if isinstance(value, str):
                result.data_type = 's'
            return result
        headers = [cell(label) for label in columns]
        for header in headers:
            header.fill = PatternFill('solid', fgColor='174785')
            header.font = Font(color='FFFFFF', bold=True)
        target.append(headers)
        for row in rows:
            target.append([cell(value) for value in row])

    summary, readings, reviews = [], [], []
    for scope, batch in active['batches'].items():
        for order, device in enumerate(DailyInspection.ordered(batch, orders.get(scope, [])), 1):
            for section in device['sections']:
                prefix = (scope + '楼', order, device['name'], device['profession'], section['location'], section['form'])
                for label, value in section['values'].items():
                    summary.append((*prefix, label, value, section['integrity'], section['missing'], section['note']))
            for point in device['points']:
                readings.append((scope + '楼', order, device['name'], device['profession'], point['point_id'],
                                 point['name'], point['value'], formatted_time(point['sampled']), point['path'],
                                 point['template'], point['sheet'], point['cell'], point['integrity'], point['note']))
            review = active.get('reviews', {}).get(device['id'])
            if review:
                rows = parameters(device)
                points = {point['id']: point for point in device['points']}
                prefix = (scope + '楼', order, device['id'], device['name'], device['profession'])
                outcome = (review['result'], review['note'], review.get('actor'))
                for item in rows:
                    section = device['sections'][item['section']] if 'section' in item else None
                    point = points.get(item.get('point'), {})
                    location = section['location'] if section is not None else point.get('location')
                    source = section['form'] if section is not None else point.get('point_id')
                    status = '配置停用' if item.get('read_only') else ''
                    reviews.append((*prefix, location, source, item['name'], item['reference'],
                                    review['values'].get(item['id']), status, *outcome))
                if not rows:
                    reviews.append((*prefix, '', '', '', '', '', '无可填参数', *outcome))
    duty = active.get('duty', {})
    sheet('巡检信息', ('巡检编号', '开始人员', '完成人员', '排班日期', '班次', '巡检轮次',
                     '开始时间', '完成时间', '楼栋', '源表批次', '源表行数', '班组'),
          [(active['id'], active.get('started_by') or active['actor'], active['actor'] if active.get('completed') else '',
            duty.get('date'), duty.get('shift'), active.get('round'),
            formatted_time(active['started']), formatted_time(active.get('completed')),
            scope + '楼', formatted_time(batch['batch']), len(batch['rows']), duty.get('group', ''))
           for scope, batch in active['batches'].items()])
    sheet('DE巡检表', ('楼栋','顺序','设备','专业','位置','巡检表单','参数','读数','数据完整性','缺少源数据','备注'), summary)
    sheet('测点读数', ('楼栋','顺序','设备','专业','测点ID','测点名称','读数','采样时间','测点路径','模板','页签','单元格','数据完整性','备注'), readings)
    if 'reviews' in active:
        sheet(REVIEW_SHEET, REVIEW_COLUMNS, reviews)
    for scope, batch in active['batches'].items():
        rows = batch['rows']
        columns = list(dict.fromkeys(key for row in rows for key in row['fields']))
        sheet(scope + '楼源表快照', ['源记录ID', *columns],
              ([row['record_id'], *(row['fields'].get(key) for key in columns)] for row in rows))
    stream = io.BytesIO()
    book.save(stream)
    return stream.getvalue()


def install_daily_inspection(app, controller, runtime):
    service = None
    lock = threading.Lock()

    def manager():
        nonlocal service
        with lock:
            if service is None:
                store = runtime.state_store
                service = DailyInspection(store, RiskCloud(None, Path(store.db_path).parent / 'feishu_assistant.json', None, None))
            return service

    async def endpoint(request: Request):
        try:
            session = await asyncio.to_thread(controller._current_session, request)
            if not session:
                return controller._auth_required_response()
            user = session.get('user') or {}
            auth = runtime.auth_manager
            granted, scopes = await asyncio.gather(asyncio.to_thread(allowed_modules, auth, session),
                                                    asyncio.to_thread(auth.session_scopes, session))
            owner = user.get('open_id')
            scopes = [scope for scope in SOURCES if scope in scopes or 'ALL' in scopes]
            if not owner or 'daily' not in granted or not scopes:
                raise RiskError('需要每日任务权限及 D 楼或 E 楼范围。', 403)
            svc = await asyncio.to_thread(manager)
            action = request.path_params.get('action', '')
            if request.method == 'POST':
                source = urlsplit(request.headers.get('origin') or request.headers.get('referer') or '')
                expected = urlsplit(controller._request_base_url(request))
                if (source.scheme, source.netloc) != (expected.scheme, expected.netloc) or request.headers.get('sec-fetch-site') == 'cross-site':
                    raise RiskError('请从当前站点办理巡检。', 403)
                try:
                    payload = await controller._read_json_request(
                        request, max_bytes=4 * 1024 * 1024 if action in {'draft', 'finish', 'history-edit'} else 128 * 1024)
                except ValueError as error:
                    raise RiskError('巡检资料格式无效或超过提交大小限制。', 400) from error
                if not isinstance(payload, dict) or (action != 'start' and not isinstance(payload.get('id'), str)):
                    raise RiskError('巡检请求格式无效。', 400)
            if action in {'history', 'history-record', 'history-edit'}:
                scope = request.query_params.get('scope')
                if scope not in scopes:
                    raise RiskError('没有该楼栋的巡检权限。', 403)
                if request.method == 'GET' and action == 'history':
                    data = await asyncio.to_thread(svc.archive_history, scope, request.query_params.get('date'))
                elif request.method == 'GET' and action == 'history-record':
                    data = await asyncio.to_thread(svc.archive_detail, scope, request.query_params.get('id'))
                    data['can_edit'] = not session.get('impersonation')
                elif request.method == 'POST' and action == 'history-edit':
                    if session.get('impersonation'):
                        raise RiskError('临时代登录不能修改历史巡检。', 403)
                    data = await asyncio.to_thread(svc.edit_archive, scope, payload['id'], payload, user.get('name') or owner)
                else:
                    raise RiskError('巡检操作不存在。', 404)
                return JSONResponse({'ok': True, 'data': {'history': data} if action == 'history' else data},
                                    headers={'Cache-Control': 'private, no-store'})
            from .personnel_source import daily_roster
            try:
                duty = await asyncio.to_thread(resolve_duty, lambda date: daily_roster(runtime.service, date), user, scopes)
            except RiskError as error:
                if request.method != 'GET' or action or error.status != 503:
                    raise
                return JSONResponse({'ok': True, 'data': {'duty': None, 'active': None, 'history': [],
                    'history_scopes': scopes, 'completed_rounds': 0, 'round_limit': ROUNDS_PER_SHIFT,
                    'duty_warning': str(error)}}, headers={'Cache-Control': 'private, no-store'})
            if not duty:
                if request.method == 'GET' and action in ('', 'status'):
                    return JSONResponse({'ok': True, 'data': {'duty': None, 'active': None, 'history': [],
                        'history_scopes': scopes, 'completed_rounds': 0, 'round_limit': ROUNDS_PER_SHIFT}},
                        headers={'Cache-Control': 'private, no-store'})
                raise RiskError('没有可办理的 D 楼或 E 楼当班排班。', 403)
            scope = duty['scope']
            if request.method == 'GET':
                state = await asyncio.to_thread(svc.load, scope)
                if action == 'xlsx':
                    active = svc.current(state, request.query_params.get('id'))
                    order = active.get('archive_order')
                    if order is None:
                        order = await asyncio.to_thread(svc.order, owner, scope)
                    content = await asyncio.to_thread(workbook, active, {scope: order})
                    return Response(content, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                        headers={'Content-Disposition': f'attachment; filename="DE-inspection-{active["id"][:10]}.xlsx"',
                                 'Cache-Control': 'private, no-store'})
                if action == 'status':
                    active = state.get('active')
                    if (active and request.query_params.get('id') == active['id']
                            and request.query_params.get('version') == str(active.get('review_version', 0))
                            and request.query_params.get('pending') == str(int('completed' in active))
                            and request.query_params.get('date') == duty['date']
                            and request.query_params.get('shift') == duty['shift']):
                        return JSONResponse({'ok': True, 'data': {'unchanged': True, 'history_scopes': scopes}},
                                            headers={'Cache-Control': 'private, no-store'})
                elif action:
                    raise RiskError('巡检操作不存在。', 404)
                data = await asyncio.to_thread(svc.view, state, owner, duty)
            else:
                if action not in {'start', 'order', 'draft', 'cancel', 'finish'}:
                    raise RiskError('巡检操作不存在。', 404)
                data = await asyncio.to_thread(svc.change, owner, user.get('name') or owner, duty, action, payload)
            data['history_scopes'] = scopes
            return JSONResponse({'ok': True, 'data': data}, headers={'Cache-Control': 'private, no-store'})
        except RiskError as error:
            return JSONResponse({'ok': False, 'error': str(error)}, status_code=error.status)
        except Exception:
            logging.exception('Daily inspection request failed')
            return JSONResponse({'ok': False, 'error': '巡检操作暂未确认，请保留本次巡检并重试。'}, status_code=503)

    app.add_api_route('/api/daily-inspection', endpoint, methods=['GET'])
    app.add_api_route('/api/daily-inspection/{action}', endpoint, methods=['GET', 'POST'])

"""Frozen D/E inspection sheets and their Feishu archive."""

import asyncio
import base64
import copy
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
from collections import Counter
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from urllib.parse import parse_qs, quote, urlsplit
from functools import lru_cache
from openpyxl.utils.cell import coordinate_from_string, column_index_from_string, range_boundaries

from fastapi import Request
from fastapi.responses import JSONResponse, Response

from upload_event_module.services.feishu_service import _stable_uuid4_client_token
from upload_event_module.services.http_client import FeishuHTTPError
from .daily_inspection_history import (MAX_ARCHIVE, MAX_ROWS, REVIEW_COLUMNS, REVIEW_SHEET, archived_devices,
                                       archived_workbook, edit_archived_devices, valid_cell_text)
from .daily_inspection_original import (ORIGINAL_INDEX_COLUMNS, ORIGINAL_INDEX_SHEET, ORIGINAL_MAPPING_COLUMNS, ORIGINAL_MAPPING_SHEET, catalog_locations,
                                        original_cells, original_templates, original_views, same_reading, template_version, cell_excluded, all_cells_excluded,
                                        refresh_archived_original, write_original_sheet, display_templates)
from .daily_inspection_sheet_edit import apply_cells, form_extras, overlay_views, frozen_readings
from .daily_inspection_template_art import preserve_artwork, read_artwork
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
ROUND_LOCATION_FIELDS = {'白1':'白1目标单元格', '白2':'白2目标单元格',
                         '夜1':'夜1目标单元格', '夜2':'夜2目标单元格'}
INSPECTION_BATCH_PREFIX = 'INSPECTION_BATCH_V1:'
DEVICE_METADATA = {
    '文本', '设备编号', '楼栋', '专业', '位置', '巡检表单', '上传日期', '源文件时间', '源文件',
    '数据完整性', '缺少源数据', '备注', '记录类型', *LOCATION_FIELDS, *ROUND_LOCATION_FIELDS.values(),
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


def inspection_history_scopes(auth, session):
    from .cabinet_guest import is_cabinet_guest

    if not isinstance(session, dict) or is_cabinet_guest(session):
        return []
    owner = (session.get('user') or {}).get('open_id')
    if not owner:
        return []
    profile = auth.account_profile(owner)
    home = profile.get('home_scope')
    allowed = auth.session_scopes(session)
    return [home] if (profile.get('enabled') and home in SOURCES and
                      (home in allowed or 'ALL' in allowed)) else []


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


def building_duty(roster, scope, current=None):
    date, shift = duty_period(current or dt.datetime.now(CN))
    duty = {'scope': scope, 'date': date, 'shift': shift}
    try:
        snapshot = roster(date)
        if snapshot.get('date') == date and snapshot.get('complete') is True:
            groups = sorted({person['group'] for person in snapshot.get('people', [])
                             if person.get('scope') == scope and person.get('shift') == shift
                             and person.get('duty_kind') != '长白' and person.get('group')})
            if groups:
                duty['group'] = '、'.join(groups)
    except Exception:
        pass  # The shift clock is authoritative; do not guess an unavailable roster group.
    return duty


def key_name(scope, value):
    value = unicodedata.normalize('NFKC', value).upper()
    value = re.sub(r'\s+', '', value)
    return value.removeprefix(scope + '-')


def validate_source_batch(scope, rows):
    """A single timestamp is insufficient: verify the uploader's completion receipt."""
    if scope not in SOURCES or not isinstance(rows, list) or not rows:
        return None
    ids, counts, markers, batches = set(), Counter(), [], set()
    try:
        for row in rows:
            identity, fields = row.get('record_id'), row.get('fields')
            if not isinstance(identity, str) or not identity or identity in ids or not isinstance(fields, dict):
                return None
            ids.add(identity)
            kind, building, stamp = plain(fields.get('记录类型')), plain(fields.get('楼栋')), fields.get('上传日期')
            if kind not in ('设备巡检', '测点明细') or building != scope + '楼' or type(stamp) is not int or stamp <= 0:
                return None
            counts[kind] += 1
            batches.add(stamp)
            remark = plain(fields.get('备注'))
            if remark.startswith(INSPECTION_BATCH_PREFIX):
                if kind != '设备巡检':
                    return None
                markers.append(json.loads(remark[len(INSPECTION_BATCH_PREFIX):]))
        if len(batches) != 1 or len(markers) != 1 or not isinstance(markers[0], dict):
            return None
        stamp, marker = next(iter(batches)), markers[0]
        digest = hashlib.sha256(json.dumps(sorted(ids), ensure_ascii=False, separators=(',', ':')).encode('utf-8')).hexdigest()
        if (set(marker) != {'status', 'building', 'upload_time', 'device_count', 'point_count', 'record_ids_sha256'}
                or marker['status'] != 'completed' or marker['building'] != scope + '楼'
                or type(marker['upload_time']) is not int or marker['upload_time'] != stamp
                or type(marker['device_count']) is not int or marker['device_count'] != counts['设备巡检']
                or type(marker['point_count']) is not int or marker['point_count'] != counts['测点明细']
                or counts['设备巡检'] < 1 or marker['record_ids_sha256'] != digest):
            return None
        return marker
    except (TypeError, ValueError, KeyError, AttributeError):
        return None


@lru_cache(maxsize=1)
def confirmed_hvdc_cells():
    path = Path(__file__).resolve().parents[1] / 'resources' / 'daily_inspection_hvdc_mapping.json'
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
        if payload.get('version') != 1 or not isinstance(payload.get('items'), list):
            raise ValueError('映射格式无效')
        result, cells_seen = {}, set()
        for item in payload['items']:
            key = (item['scope'], item['profession'], item['point_id'])
            cells = item['cells']
            source, target = item['source_device'], item['target_device']
            if (key in result or key[:2] != ('D', '电气') or not re.fullmatch(r'\d{3}-HVDC-\d{2}2', source)
                    or target != source[:-1] + '1' or item['source_name'] not in ('直流电压_V', '负载总电流_A')
                    or not isinstance(cells, dict) or set(cells) != {'白1', '白2', '夜1', '夜2'}
                    or any(not isinstance(value, str) or not re.fullmatch(r'[A-Z]{1,3}[1-9]\d{0,5}', value) for value in cells.values())
                    or not item.get('template') or not item.get('sheet')):
                raise ValueError('映射记录无效')
            destinations = {(item['template'], item['sheet'], cell) for cell in cells.values()}
            if len(destinations) != 4 or cells_seen.intersection(destinations):
                raise ValueError('巡检表目标单元格重复')
            cells_seen.update(destinations)
            result[key] = item
        return result
    except (OSError, TypeError, KeyError, ValueError, AttributeError):
        logging.exception('确认的 HVDC 原表映射未就绪；原始测点仍可查看和填写')
        return {}


@lru_cache(maxsize=1)
def confirmed_humidity_cells():
    return load_climate_cells('daily_inspection_humidity_mapping.json', 'humidity')


@lru_cache(maxsize=1)
def confirmed_return_air_cells():
    return load_climate_cells('daily_inspection_return_air_mapping.json', 'return_air')


@lru_cache(maxsize=1)
def confirmed_electrical_environment_cells():
    """Rooms identified by original form labels and source location names."""
    path = Path(__file__).resolve().parents[1] / 'resources' / 'daily_inspection_electrical_environment_mapping.json'
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
        if payload.get('version') != 1 or not isinstance(payload.get('items'), list):
            raise ValueError('映射格式无效')
        e_rooms = {
            '245': (1, 'J'), '246': (1, 'F'), '221': (1, 'H'),
            '218': (2, 'J'), '217': (2, 'F'), '251': (2, 'H'),
            '345': (3, 'J'), '346': (3, 'F'), '320': (3, 'H'),
            '317': (4, 'J'), '316': (4, 'F'), '350': (4, 'H'),
            '445': (5, 'J'), '446': (5, 'F'), '420': (5, 'H'),
            '417': (6, 'J'), '416': (6, 'F'), '450': (6, 'H'),
        }
        d_rooms = {'245':(1,'H'), '246':(1,'F'), '218':(2,'H'), '217':(2,'F'),
                   '345':(3,'L'), '346':(3,'J'), '317':(4,'L'), '316':(4,'J'),
                   '445':(5,'H'), '446':(5,'F'), '417':(6,'H'), '416':(6,'F')}
        result, destinations = {}, set()
        for item in payload['items']:
            room, name, cells = item['source_room'], item['source_name'], item['cells']
            key = (item['scope'], item['profession'], item['point_id'])
            rooms = d_rooms if key[0] == 'D' else e_rooms if key[0] == 'E' else {}
            expected_template = ('21V-JSNTFOC-RM-422 南通数据中心D栋电气巡检表单(2024.11.30）.xlsx' if key[0] == 'D'
                                 else '21V-JSNTFOC-RM-428 南通数据中心E栋电气巡检表单.xlsx')
            if (key in result or key[1] != '电气' or not key[2] or room not in rooms
                    or item['source_device'] != room + '-TH' or name not in ('温度', '湿度')
                    or item['template'] != expected_template
                    or item['sheet'].strip() != f'M{rooms[room][0]}HVDC巡检表'):
                raise ValueError('电气环境映射记录无效')
            column = chr(ord(rooms[room][1]) + (name == '湿度'))
            expected = {slot: f'{column}{row}' for slot, row in
                        zip(('白1', '白2', '夜1', '夜2'), range(20, 24))}
            locations = {(key[0], item['template'], item['sheet'], cell) for cell in expected.values()}
            if cells != expected or destinations.intersection(locations):
                raise ValueError('电气环境映射目标不一致或重复')
            destinations.update(locations)
            result[key] = item
        return result
    except (OSError, TypeError, KeyError, ValueError, AttributeError):
        logging.exception('确认的电气环境原表映射未就绪；原始测点仍可查看和填写')
        return {}


@lru_cache(maxsize=1)
def confirmed_chiller_cells():
    path = Path(__file__).resolve().parents[1] / 'resources' / 'daily_inspection_chiller_mapping.json'
    rows = {'M4031冷凝器饱和温度':12, 'M4033排气温度':7, 'M4061油箱温度':5,
            'M4075油压差':6, 'M4093实际导叶开度':8, 'M4007电机电流百分比':4,
            'M4015冷冻水进水温度':10, 'M4017冷冻水出水温度':11,
            'M4019冷却水进水温度':14, 'M4021冷却水出水温度':15, 'M4023蒸发器饱和温度':9}
    d_rows = {'电流百分比':4, '油箱温度':5, '压缩机供油温度':5, '压缩机排气温度':7,
              '压缩机导叶运行开度':8, '饱和蒸发温度':9, '冷冻水进水温度':10,
              '冷冻水出水温度':11, '饱和冷凝温度':12, '冷却水进水温度':14, '冷却水出水温度':15}
    d_pressures = {'冷机冷却水进口压力':10, '冷机冷却水出口压力':11,
                   '板换冷冻水进口压力':12, '板换冷冻水出口压力':13}
    e_pressures = {'冷凝器进口压力':10, '冷凝器出口压力':11,
                   '冷冻水侧板换进口压力':12, '冷冻水侧板换出口压力':13}
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
        if data.get('version') != 1 or not isinstance(data.get('items'), list):
            raise ValueError('冷机映射格式无效')
        result, destinations = {}, set()
        for item in data['items']:
            key = (item['scope'], item['profession'], item['point_id'])
            match = re.fullmatch(r'(\d{3})-(\d{3})-MOXA_(\d{3})-CH([1-6])', item['source_device_full'])
            e_ddc = re.fullmatch(r'(124|150)-DDC-(10[1-3])', item['source_device_full'])
            d_name = re.fullmatch(r'([1-6])#冷机(.+)', item['source_name'])
            pressure = re.fullmatch(r'([1-6])#(.+)', item['source_name'])
            if key[:2] == ('E','暖通') and match and match.group(1) == match.group(3) and item['source_name'] in rows:
                valid_source = item['source_device'] == f'{match.group(1)}-{match.group(2)}-MOXA'
                valid_template = item['template'] == '21V-JSNTFOC-RM-423 南通数据中心E栋暖通巡检表单(2024年最新版) (2).xlsx'
                cell = chr(ord('A') + int(match.group(4))) + str(rows[item['source_name']])
            elif key[:2] == ('E','暖通') and e_ddc and item['source_name'].startswith('E-' + item['source_device_full'] + '_'):
                metric = item['source_name'][len('E-' + item['source_device_full'] + '_'):]
                if metric not in e_pressures:
                    raise ValueError('板换压力字段无效')
                valid_source = item['source_device'] == item['source_device_full']
                valid_template = item['template'] == '21V-JSNTFOC-RM-423 南通数据中心E栋暖通巡检表单(2024年最新版) (2).xlsx'
                number = int(e_ddc.group(2)) - 100 + (3 if e_ddc.group(1) == '124' else 0)
                cell = chr(ord('J') + number) + str(e_pressures[metric])
            elif key[:2] == ('D','暖通') and d_name and d_name.group(2) in d_rows:
                valid_source = item['source_device'] == item['source_device_full'].split('_', 1)[0]
                valid_template = item['template'] == '21V-JSNTFOC-RM-423 南通数据中心D栋暖通巡检表单(2024.11.30）.xlsx'
                cell = chr(ord('A') + int(d_name.group(1))) + str(d_rows[d_name.group(2)])
            elif key[:2] == ('D','暖通') and pressure and pressure.group(2) in d_pressures:
                valid_source = item['source_device'] == item['source_device_full'].split('_', 1)[0]
                valid_template = item['template'] == '21V-JSNTFOC-RM-423 南通数据中心D栋暖通巡检表单(2024.11.30）.xlsx'
                cell = chr(ord('J') + int(pressure.group(1))) + str(d_pressures[pressure.group(2)])
            else:
                raise ValueError('冷机来源或原表无效')
            if (key in result or not key[2] or not valid_source or not valid_template
                    or item['sheet'].strip() != '暖通冷机系统巡检表'):
                raise ValueError('冷机来源或原表无效')
            if item['cells'] != {slot:cell for slot in ('白1','白2','夜1','夜2')} or (key[0],cell) in destinations:
                raise ValueError('冷机目标位置无效或重复')
            destinations.add((key[0],cell))
            result[key] = item
        return result
    except (OSError, TypeError, KeyError, ValueError, AttributeError):
        logging.exception('确认的冷机原表映射未就绪；原始测点仍可查看和填写')
        return {}


@lru_cache(maxsize=1)
def confirmed_svg_cells():
    path = Path(__file__).resolve().parents[1] / 'resources' / 'daily_inspection_svg_mapping.json'
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
        if data.get('version') != 1 or not isinstance(data.get('items'), list):
            raise ValueError('SVG映射格式无效')
        starts = {'144-SVG-101':'B', '144-SVG-201':'E', '120-SVG-101':'K', '120-SVG-201':'N'}
        result, destinations = {}, set()
        for item in data['items']:
            key = (item['scope'], item['profession'], item['point_id'])
            device = item['source_device']
            if (key in result or key[0] not in ('D','E') or key[1] != '电气' or not key[2]
                    or device not in starts or not item['source_device_full'].startswith(device + '_')
                    or item['sheet'].strip() != 'SVG巡检表'):
                raise ValueError('SVG来源无效')
            if key[0] == 'D' and item['source_name'] in ('装置温度','装置温度_℃'):
                expected_template = '21V-JSNTFOC-RM-422 南通数据中心D栋电气巡检表单(2024.11.30）.xlsx'
                column = chr(ord(starts[device]) + 1)
            elif key[0] == 'E' and item['source_name'] == 'A_功率因数(R)':
                expected_template = '21V-JSNTFOC-RM-428 南通数据中心E栋电气巡检表单.xlsx'
                column = chr(ord(starts[device]) + 2)
            else:
                raise ValueError('SVG参数无效')
            cells = {slot:f'{column}{row}' for slot,row in zip(('白1','白2','夜1','夜2'),range(7,11))}
            targets = {(key[0],item['template'],item['sheet'],cell) for cell in cells.values()}
            if item['template'] != expected_template or item['cells'] != cells or destinations.intersection(targets):
                raise ValueError('SVG目标无效或重复')
            destinations.update(targets)
            result[key] = item
        return result
    except (OSError, TypeError, KeyError, ValueError, AttributeError):
        logging.exception('确认的 SVG 原表映射未就绪；原始测点仍可查看和填写')
        return {}


@lru_cache(maxsize=1)
def confirmed_dc_screen_cells():
    path = Path(__file__).resolve().parents[1] / 'resources' / 'daily_inspection_dc_mapping.json'
    fields = {'主路输入 UV 线电压':'B', '合母电压':'C', '控母电压':'D'}
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
        if data.get('version') != 1 or not isinstance(data.get('items'), list):
            raise ValueError('直流屏映射格式无效')
        result, used = {}, set()
        for item in data['items']:
            key = (item['scope'], item['profession'], item['point_id'])
            column = fields.get(item['source_name'])
            cells = {slot:f'{column}{row}' for slot,row in zip(('白1','白2','夜1','夜2'),range(23,27))}
            if (key in result or key[:2] != ('D','电气') or not key[2] or not column
                    or item['source_device'] != '178-AD001' or item['source_device_full'] != '178-AD001_直流屏'
                    or item['template'] != '21V-JSNTFOC-RM-422 南通数据中心D栋电气巡检表单(2024.11.30）.xlsx'
                    or item['sheet'].strip() != '柴发巡检表' or item['cells'] != cells
                    or used.intersection(cells.values())):
                raise ValueError('直流屏目标映射无效或重复')
            used.update(cells.values())
            result[key] = item
        return result
    except (OSError, TypeError, KeyError, ValueError, AttributeError):
        logging.exception('直流屏原表映射未就绪；原始测点仍可查看和填写')
        return {}


def validate_round_locations(scope, profession, template, sheet_name, cells):
    """Validate explicit four-round targets against the actual original layout."""
    if not template or not sheet_name or not isinstance(cells, dict) or set(cells) - set(ROUND_LOCATION_FIELDS):
        raise ValueError('请填写原表及四轮目标位置')
    matching = [item for item in original_templates() if item['scope']==scope and
                item['profession']==profession and item['name']==template]
    if len(matching)!=1:
        raise ValueError('目标巡检表与楼栋或专业不匹配')
    sheets = matching[0]['sheets']
    exact = [item for item in sheets if item['name']==sheet_name]
    choices = exact or [item for item in sheets if item['name'].strip()==sheet_name.strip()]
    if len(choices)!=1:
        raise ValueError('目标工作表不存在或名称不唯一')
    sheet = choices[0]
    normalized = {slot:str(value).strip().upper() for slot,value in cells.items() if value and str(value).strip()}
    addresses = list(normalized.values())
    is_chiller = sheet['name'].strip()=='暖通冷机系统巡检表'
    if is_chiller and len(set(addresses))>1:
        raise ValueError('冷机表每轮必须使用同一固定位置')
    if not is_chiller and len(addresses)!=len(set(addresses)):
        raise ValueError('普通巡检表各轮必须使用不同位置')
    for address in addresses:
        if not re.fullmatch(r'[A-Z]{1,3}[1-9]\d{0,5}',address):
            raise ValueError('目标单元格格式无效')
        column,row = coordinate_from_string(address)
        col=column_index_from_string(column)
        if row>len(sheet['rows']) or col>len(sheet['rows'][row-1]) or sheet['rows'][row-1][col-1] not in ('',None):
            raise ValueError('目标位置不是原表空白数据格')
        for ref in sheet['merges']:
            c1,r1,c2,r2=range_boundaries(ref)
            if r1<=row<=r2 and c1<=col<=c2 and (row,col)!=(r1,c1):
                raise ValueError('目标位置不是合并单元格的主格')
    return normalized


def deferred_original_location(scope, sheet, cells):
    """Temporarily omitted original-form positions, pending user point setup."""
    sheet = str(sheet or '').strip()
    address = str((cells or {}).get('白1') or '')
    match = re.fullmatch(r'([A-Z]+)(\d+)', address)
    if not match:
        return False
    col, row = column_index_from_string(match.group(1)), int(match.group(2))
    if scope == 'D' and sheet == '辅助区空调巡检表 (2)':
        return (row in (27, 33) and 3 <= col <= 12 or
                row == 39 and 3 <= col <= 11 or row == 45 and 3 <= col <= 6)
    if scope == 'E' and sheet == '辅助区空调巡检表 (3)':
        return row == 26 and 3 <= col <= 16 or row == 32 and 3 <= col <= 13
    if scope in ('D', 'E') and sheet == '辅助区空调巡检表':
        return row in (15, 16, 25, 26) and 3 <= col <= 4
    if scope in ('D', 'E') and sheet == 'SVG巡检表':
        return (row == 7 and 8 <= col <= 10 or row == 7 and 17 <= col <= 19 or
                row == 14 and 2 <= col <= 19 or
                row == 21 and (2 <= col <= 7 or 11 <= col <= 16))
    return False


def deferred_catalog_section(device, section):
    scope, name = device.get('scope'), str(device.get('name') or '').strip()
    form = str(section.get('form') or '').strip()
    location = str(section.get('location') or '').strip()
    if scope not in ('D', 'E'):
        return False
    svg = re.fullmatch(r'(?:[DE]-)?(245|218|345|317|445|417)-SVG-(101|201)', name, re.I)
    if form == 'SVG巡检表' and svg:
        return True
    if form == '辅助区空调巡检表' and re.fullmatch(r'(?:248|215)-CRAH-0[12]', name, re.I):
        return True
    vrv_locations = {
        'D': {'生产线、VMI库房', '货运入口大厅', '电气值班室', 'VMI、运维库房',
              '1F柴发并机控制室', '2F柴发测试控制室', '运维值班室', '1F运维库房',
              '2F运维库房', '3F运维库房', '4F运维库房', '大堂', '运维库房'},
        'E': {'E-178发电机并机室', 'E-142测试区', 'E-119备品备件库房', 'E楼大厅',
              '货运入口大厅', 'E-111值班室', 'E-123', 'E-279并机室', 'E-278库房', 'E-347库房'},
    }
    return (form == ('辅助区空调巡检表 (2)' if scope == 'D' else '辅助区空调巡检表 (3)')
            and bool(re.fullmatch(r'空调\d+号', name)) and location in vrv_locations[scope])


def deferred_point(device, point):
    if all_cells_excluded(point.get('template'),point.get('sheet'),point.get('round_cells')):
        return True
    # User-created non-baseline points are explicitly configured and may be
    # used even where the template's original placeholder is deferred.
    if point.get('enabled') is True and (point.get('baseline') is False or point.get('user_enabled') is True):
        return False
    if deferred_original_location(device.get('scope'), point.get('sheet'), point.get('round_cells')):
        return True
    sections = device.get('sections') or []
    if not sections and device.get('scope') in ('D', 'E'):
        name = str(device.get('name') or '').strip()
        if (re.fullmatch(r'(?:248|215)-CRAH-0[12]', name, re.I)
                or re.fullmatch(r'(?:[DE]-)?(?:245|218|345|317|445|417)-SVG-(?:101|201)', name, re.I)):
            return True
    if sections and all(deferred_catalog_section(device, section) for section in sections):
        return True
    return False


def omit_occupied_form_cells(form_devices, devices):
    occupied = {(point['template'], point['sheet'].strip(), slot, address)
                for device in devices for point in device['points']
                for slot,address in (point.get('round_cells') or {}).items()}
    result = []
    for device in form_devices:
        points = []
        for point in device['points']:
            if deferred_original_location(device['scope'], point.get('sheet'), point.get('round_cells')):
                continue
            remaining = {slot:address for slot,address in point['round_cells'].items()
                         if (point['template'], point['sheet'].strip(), slot, address) not in occupied}
            if remaining:
                points.append({**point,'round_cells':remaining,'cell':next(iter(remaining.values()))})
        if points:
            result.append({**device,'points':points})
    return result


@lru_cache(maxsize=2)
def supplemental_form_devices(scope):
    """Writable original-form cells with no automatic source mapping."""
    path = Path(__file__).resolve().parents[1] / 'resources' / 'daily_inspection_manual_mapping.json'
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
        if data.get('version') != 1 or not isinstance(data.get('items'), list):
            raise ValueError('原表补填目录格式无效')
        layouts = {(item['scope'], item['profession'], item['name'], sheet['name']):sheet
                   for item in original_templates() for sheet in item['sheets']}
        groups, occupied, ids = {}, set(), set()
        for item in data['items']:
            if item['scope'] != scope:
                continue
            key = (scope,item['profession'],item['template'],item['sheet'])
            layout = layouts.get(key)
            cells = item['cells']
            identity = item['point_id']
            if (not layout or identity in ids or not re.fullmatch(r'original-[0-9a-f]{24}',identity)
                    or not isinstance(cells,dict) or set(cells)!={'白1','白2','夜1','夜2'}):
                raise ValueError('原表补填目录存在无效项目')
            ids.add(identity)
            # Display bounds come from the supplied workbook. Old blank padding
            # may have supplemental targets; keep their configuration/history,
            # but do not let one out-of-range padding cell disable the whole form.
            outside = False
            for address in cells.values():
                try:
                    col, row = coordinate_from_string(address)
                    col = column_index_from_string(col)
                    outside = outside or row > len(layout['rows']) or col > len(layout['rows'][row-1])
                except (ValueError,TypeError,IndexError):
                    raise ValueError('原表补填坐标无效') from None
            if outside or all(cell_excluded(layout,address) for address in cells.values()):
                continue
            for slot,address in cells.items():
                match = re.fullmatch(r'([A-Z]{1,3})([1-9]\d{0,5})', address or '')
                if not match:
                    raise ValueError('原表补填单元格无效')
                col = 0
                for char in match.group(1):
                    col = col*26 + ord(char)-64
                row = int(match.group(2))
                if (row>len(layout['rows']) or col>len(layout['rows'][row-1])
                        or layout['rows'][row-1][col-1] not in ('',None)):
                    raise ValueError('原表补填位置不是空白格')
                destination = (key,slot,address)
                if destination in occupied:
                    raise ValueError('原表补填位置重复')
                occupied.add(destination)
            device = groups.setdefault(key, {'id':f'{scope}:{item["profession"]}:original-'+hashlib.sha256(
                (item['template']+'|'+item['sheet']).encode()).hexdigest()[:16],
                'name':'原表补填 · '+item['sheet'].strip(), 'scope':scope,'profession':item['profession'],
                'form_only':True,'sections':[],'points':[]})
            header_is_state = original_cell_is_status(layout, cells['白1'])
            device['points'].append({'id':identity,'point_id':identity,'name':item['name'],'location':'',
                'path':'','value':'','sampled':None,'integrity':'','enabled':True,'baseline':False,'note':'',
                'template':item['template'],'sheet':item['sheet'],'cell':cells['白1'],'round_cells':dict(cells),
                'template_known':True,'value_type':'状态' if header_is_state else ''})
        return list(groups.values())
    except (OSError, TypeError, KeyError, ValueError, AttributeError):
        logging.exception('原表补填目录未就绪')
        return []


def original_cell_is_status(layout, address):
    """Recognize a status header only when it explicitly covers this column."""
    column, row = coordinate_from_string(address)
    column = column_index_from_string(column)
    headers = []
    for previous in range(max(1, row - 5), row):
        cells = layout['rows'][previous - 1]
        if column <= len(cells) and cells[column - 1] not in ('', None):
            headers.append((previous, str(cells[column - 1])))
    for merged in layout.get('merges', []):
        left, top, right, bottom = range_boundaries(merged)
        if left <= column <= right and max(1, row - 5) <= top < row and bottom < row:
            cells = layout['rows'][top - 1]
            if left <= len(cells) and cells[left - 1] not in ('', None):
                headers.append((top, str(cells[left - 1])))
    if not headers:
        return False
    nearest = max(position for position, _ in headers)
    return any(state_parameter(label) for position, label in headers if position == nearest)


def load_climate_cells(filename, kind):
    path = Path(__file__).resolve().parents[1] / 'resources' / filename
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
        if payload.get('version') != 1 or not isinstance(payload.get('items'), list):
            raise ValueError('映射格式无效')
        result, cells_seen = {}, set()
        for item in payload['items']:
            key = (item['scope'], item['profession'], item['point_id'])
            cells, source = item['cells'], item['source_device']
            correct_kind = (kind == 'humidity' and key[0] in ('D', 'E')
                            and re.fullmatch(r'\d{3}-HUM-\d{2}', source)
                            and item['source_name'] == '湿度显示' and item['sheet'] == '恒湿机巡检表') or (
                            kind == 'return_air' and key[0] == 'D' and re.fullmatch(r'\d{3}-CRAC-\d{2}', source)
                            and item['source_name'] in ('1#回风温度', '1#回风湿度') and item['sheet'] == '辅助区空调巡检表 (2)')
            if (key in result or key[1] != '暖通' or not correct_kind or item['target_device'] != source
                    or not item.get('template') or not isinstance(cells, dict)
                    or set(cells) != {'白1', '白2', '夜1', '夜2'}
                    or any(not isinstance(value, str) or not re.fullmatch(r'[A-Z]{1,3}[1-9]\d{0,5}', value) for value in cells.values())):
                raise ValueError('湿度映射记录无效')
            destinations = {(key[0], item['template'], item['sheet'], cell) for cell in cells.values()}
            if len(destinations) != 4 or cells_seen.intersection(destinations):
                raise ValueError('湿度目标单元格重复')
            cells_seen.update(destinations)
            result[key] = item
        return result
    except (OSError, TypeError, KeyError, ValueError, AttributeError):
        logging.exception('确认的暖通原表映射未就绪；原始测点仍可查看和填写')
        return {}


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
            values = {name: value if isinstance(value, (int, float)) else plain(value)
                      for name, value in fields.items() if name not in DEVICE_METADATA}
            # Feishu omits empty cells from a record.  The source explicitly lists
            # missing fields, so retain those parameters as editable blanks.
            for missing in re.split(r'[、,，;；\n]+', plain(fields.get('缺少源数据'))):
                missing = missing.strip()
                if missing and missing not in DEVICE_METADATA:
                    values.setdefault(missing, '')
            note = plain(fields.get('备注'))
            if note.startswith(INSPECTION_BATCH_PREFIX):
                note = ''
            device['sections'].append({
                'location': plain(fields.get('位置')),
                'form': plain(fields.get('巡检表单')),
                'integrity': plain(fields.get('数据完整性')),
                'missing': plain(fields.get('缺少源数据')),
                'note': note,
                'values': values,
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
        # The user confirmed HVDC source suffix 2 corresponds to form suffix 1
        # in D and E. An exact source device always wins; never create a target.
        normalized = key_name(scope, name.split('_', 1)[0])
        confirmed = re.fullmatch(r'(\d{3}-HVDC-\d{2})2', normalized) if scope in ('D', 'E') and profession == '电气' else None
        target = (profession, confirmed.group(1) + '1') if confirmed else None
        if exact not in primary and (profession, normalized) not in primary and target in primary:
            key = target
        device = devices.setdefault(key, {'id': f'{scope}:{key[0]}:{key[1]}', 'name': name, 'scope': scope,
                                           'profession': profession, 'sections': [], 'points': []})
        config_key = (profession, point_id)
        config = configurations.get(config_key, {})
        ambiguous_config = config_key in configurations and not config
        target_source = config if config else fields
        template, sheet, cell = [plain(target_source.get(k)).strip() for k in LOCATION_FIELDS]
        explicit_cells = {slot:plain(target_source.get(field)).strip() for slot,field in ROUND_LOCATION_FIELDS.items()}
        point_name = plain(config.get('测点名称')) or plain(fields.get('测点名称')) or point_id
        lookup = ({**confirmed_hvdc_cells(), **confirmed_electrical_environment_cells(), **confirmed_svg_cells(), **confirmed_dc_screen_cells()} if profession == '电气' else
                  {**confirmed_humidity_cells(), **confirmed_return_air_cells(), **confirmed_chiller_cells()} if profession == '暖通' else {})
        mapped = lookup.get((scope, profession, point_id))
        verified_mapping = (mapped and normalized == mapped['source_device']
                            and (not mapped.get('source_device_full') or key_name(scope, name) == mapped['source_device_full'])
                            and point_name == mapped['source_name'])
        round_cells, mapping_issue = {}, ''
        if any(explicit_cells.values()):
            try:
                round_cells = validate_round_locations(scope, profession, template, sheet, explicit_cells)
                cell = next(iter(round_cells.values()), '')
            except (OSError, ValueError, TypeError, KeyError, AttributeError) as error:
                mapping_issue = str(error) or '四轮目标位置无法核验'
        elif (verified_mapping and template == mapped['template'] and sheet.strip() == mapped['sheet'].strip()
              and cell == mapped['cells'].get('白1')):
            # Use the separately verified four-round mapping, not a guessed offset
            # from the old single location. Mismatched configuration stays blocked.
            round_cells = dict(mapped['cells'])
        elif template or sheet or cell:
            mapping_issue = '尚未配置四轮目标位置'
        elif verified_mapping:
            template, sheet = mapped['template'], mapped['sheet']
            round_cells = dict(mapped['cells'])
            cell = round_cells['白1']
        if ambiguous_config:
            mapping_issue = '测点配置重复，请核对配置后再使用自动值'
        reading = fields.get('测点值')
        raw_value = plain(fields.get('测点原始值')) if '测点原始值' in fields else None
        value_type = plain(config.get('测点类型') or fields.get('测点类型'))
        if value_type == '直接填充':
            reading = raw_value if raw_value is not None else ''
        if ambiguous_config:
            reading = ''
        device['points'].append({
            'id': row['record_id'], 'point_id': point_id,
            'name': point_name,
            'location': location, 'path': plain(config.get('测点路径')) or plain(fields.get('测点路径')),
            'value': reading if isinstance(reading, (int, float)) else plain(reading),
            'raw_value': raw_value,
            'sampled': fields.get('采样时间'), 'integrity': plain(fields.get('数据完整性')),
            # Feishu omits unchecked checkboxes. For an identified config row,
            # omission means False; absence of the whole row remains unknown.
            'enabled': (config.get('启用') is True) if config else None,
            'baseline': (config.get('基线测点') is True) if config else None,
            'user_enabled': config.get('用户主动启用') is True,
            'note': plain(config.get('测点备注')) or plain(fields.get('测点备注')),
            'value_type': value_type,
            'status_normal': '' if ambiguous_config else plain(config.get('状态正常值') or fields.get('状态正常值')),
            'status_abnormal': '' if ambiguous_config else plain(config.get('状态异常值') or fields.get('状态异常值')),
            'status_stopped': '' if ambiguous_config else plain(config.get('状态未运行值') or fields.get('状态未运行值')),
            'template': template, 'sheet': sheet, 'cell': cell,
            'round_cells': round_cells, 'mapping_issue': mapping_issue,
            'template_known': template in templates.get((scope, profession), []),
        })
    # A raw point and a catalog field can represent the same verified form cell.
    # Fill only a missing frozen reference from an unambiguous nonempty point;
    # never rewrite the source table or replace an existing device value.
    try:
        catalog = catalog_locations()
        targets = {}
        for device in devices.values():
            for section in device['sections']:
                for field in section['values']:
                    item = catalog.get((scope + '楼', device['name'], device['profession'], section['location'],
                                        section['form'].strip(), field))
                    if item:
                        target = (item['template'], item['sheet'].strip(), item['cells'].get('白1'))
                        targets.setdefault(target, []).append((device, section, field))
        point_values = {}
        for device in devices.values():
            for point in device['points']:
                cells = point.get('round_cells') or {}
                value = source_text(point['value']).strip()
                try:
                    numeric = bool(value) and Decimal(value).is_finite()
                except InvalidOperation:
                    numeric = False
                if cells and point.get('enabled') is not False and numeric:
                    target = (point['template'], point['sheet'].strip(), cells.get('白1'))
                    point_values.setdefault(target, []).append((device, point))
        for target, sections in targets.items():
            if len(sections) != 1:
                continue
            device, section, field = sections[0]
            readings = []
            for source_device, point in point_values.get(target, []):
                raw = point['value']
                oil = re.fullmatch(r'([1-9]\d*)#油箱体积', point['name'])
                if oil and field == '柴油液位(cm)':
                    if (device['name'] != oil.group(1) + '#柴发'
                            or key_name(scope, source_device['name']) not in ('燃油自控系统','178-燃油自控系统')):
                        continue
                    raw = str((Decimal(source_text(raw)) / 10).quantize(Decimal('0.01'),rounding=ROUND_HALF_UP))
                readings.append(raw)
            if readings and all(same_reading(readings[0], value) for value in readings[1:]):
                if not source_text(section['values'][field]).strip():
                    section['values'][field] = readings[0]
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        logging.exception('巡检原表交叉匹配未就绪，保留来源原值')
    return list(devices.values())


def state_parameter(name, value_type=''):
    return value_type == '状态' or bool(re.search(r'运行状态|启停状态|开关状态|设备状态|工作状态|报警状态|告警状态|有无滴漏', name))


def state_reference(value, point=None):
    raw = source_text(value).strip()
    if point is None:
        if raw in ('√', '×', '△'):
            return raw
        if raw == 'Δ':
            return '△'
    if point is not None:
        # Formatted measurements are not a reliable substitute for the exact
        # raw value used by a configured state rule.
        original = point.get('raw_value')
        if original is None:
            return ''
        raw = original
    matches = []
    for key, symbol in (('status_normal', '√'), ('status_abnormal', '×'), ('status_stopped', '△')):
        try:
            values = json.loads((point or {}).get(key) or '[]')
            if isinstance(values, list) and all(isinstance(item, str) for item in values) and raw and raw in values:
                matches.append(symbol)
        except (TypeError, ValueError):
            pass
    return matches[0] if len(matches) == 1 else ''


def parameters(device):
    rows = []
    for index, section in enumerate(device['sections']):
        if deferred_catalog_section(device, section):
            continue
        values = dict(section['values'])
        # An active round created before this fix may lack omitted fields in its
        # frozen snapshot; add only fields the source explicitly marked missing.
        for name in re.split(r'[、,，;；\n]+', section.get('missing') or ''):
            name = name.strip()
            if name and name not in DEVICE_METADATA:
                values.setdefault(name, '')
        for name, value in values.items():
            mapping=catalog_locations().get((device.get('scope','')+'楼',device['name'],device['profession'],section['location'],section['form'].strip(),name))
            if mapping and all_cells_excluded(mapping['template'],mapping['sheet'],mapping['cells']):
                continue
            is_state = state_parameter(name)
            reference = state_reference(value) if is_state else value
            if is_state and not reference:
                # A device field and a configured point may refer to the same
                # original cells. Only reuse a uniquely verified state mapping.
                try:
                    key = (device['scope'] + '楼', device['name'], device['profession'], section['location'],
                           section['form'].strip(), name)
                    target = catalog_locations().get(key)
                    matches = [point for point in device['points'] if target and point.get('enabled') is not False
                               and point.get('template') == target['template']
                               and str(point.get('sheet') or '').strip() == target['sheet'].strip()
                               and point.get('round_cells') == target['cells']]
                    if len(matches) == 1:
                        reference = state_reference(matches[0]['value'], matches[0])
                except (OSError, TypeError, KeyError, ValueError, AttributeError):
                    pass
            rows.append({'id': f's:{index}:{name}', 'name': name,
                         'reference': reference,
                         'source_reference': value if is_state else None, 'value_type': '状态' if is_state else '数值', 'section': index})
    for point in device['points']:
        if deferred_point(device, point):
            continue
        is_state = state_parameter(point['name'], point.get('value_type', ''))
        rows.append({'id': 'p:' + point['id'], 'name': point['name'],
                     'reference': state_reference(point['value'], point) if is_state else point['value'],
                     'source_reference': (point.get('raw_value') if point.get('raw_value') is not None else point['value']) if is_state else None,
                     'value_type': '状态' if is_state else '数值',
                     'point': point['id'], 'read_only': point.get('enabled') is False})
    return rows


def display_device(device):
    return {**device, 'parameters': parameters(device),
            'deferred_sections': [index for index, section in enumerate(device['sections'])
                                  if deferred_catalog_section(device, section)]}


def manual_state_key(device, parameter):
    """A manual state belongs to one device/parameter and its complete four-round mapping."""
    if parameter.get('value_type')!='状态' or parameter.get('read_only'):
        return None
    if parameter.get('reference') not in ('',None):
        return None
    if 'point' in parameter:
        point=next((p for p in device['points'] if p['id']==parameter['point']),None)
        if not point or point.get('enabled') is False:return None
        for field in ('status_normal','status_abnormal','status_stopped'):
            try:
                rule=json.loads(point.get(field) or '[]')
                if not isinstance(rule,list) or rule:return None
            except (ValueError,TypeError):return None
        identity=['point',point.get('point_id'),point.get('name')]
        target=[point.get('template'),str(point.get('sheet') or '').strip(),point.get('round_cells')]
    else:
        section=device['sections'][parameter['section']]
        item=catalog_locations().get((device['scope']+'楼',device['name'],device['profession'],section['location'],section['form'].strip(),parameter['name']))
        if not item:return None
        # A catalogue field backed by a real state rule keeps its live source.
        for point in device['points']:
            if point.get('template')==item['template'] and str(point.get('sheet') or '').strip()==item['sheet'].strip() and point.get('round_cells')==item['cells']:
                for field in ('status_normal','status_abnormal','status_stopped'):
                    try:
                        rule=json.loads(point.get(field) or '[]')
                        if not isinstance(rule,list) or rule:return None
                    except (ValueError,TypeError):return None
        identity=['section',section['location'],section['form'],parameter['name']]
        target=[item['template'],item['sheet'].strip(),item['cells']]
    if not all(target):return None
    return hashlib.sha256(json.dumps([device['scope'],device['id'],identity,target],ensure_ascii=False,sort_keys=True).encode()).hexdigest()


def inherit_manual_states(state, active, scope):
    saved=state.get('manual_states',{})
    if not saved:return
    reviews={}
    for device in review_devices(active,scope):
        values={};inherited={}
        for param in parameters(device):
            key=manual_state_key(device,param)
            remembered=saved.get(key,{}) if key else {}
            if param.get('reference') not in ('',None) or remembered.get('value') not in ('√','×','△'):continue
            values[param['id']]=remembered['value'];inherited[param['id']]=remembered
        if values:
            latest_note=max((item for item in inherited.values() if item['value']=='×' and item.get('note')),
                            key=lambda item:item.get('at',0),default={}).get('note','')
            reviews[device['id']]={'values':values,'result':'异常' if '×' in values.values() else '',
                                    'note':latest_note,'revision':0,'actor':'','inherited_states':inherited}
    if reviews:active['reviews']=reviews


def remember_manual_states(state, active, devices, before, reviews, submitted, actor):
    saved=dict(state.get('manual_states',{}))
    for device in devices:
        update=submitted.get(device['id'])
        if not isinstance(update,dict):continue
        previous=before.get(device['id'],{}).get('values',{})
        current=reviews.get(device['id'],{})
        for param in parameters(device):
            pid=param['id']
            if pid not in update.get('values',{}):continue
            value=current.get('values',{}).get(pid)
            if value==previous.get(pid) and (value!='×' or current.get('note','')==before.get(device['id'],{}).get('note','')):continue
            key=manual_state_key(device,param)
            if not key:continue
            if value in ('√','×','△'):
                saved[key]={'value':value,'actor':actor,'at':int(time.time()*1000),'round_id':active['id'],
                            'note':current.get('note','') if value=='×' else ''}
            elif value=='':saved.pop(key,None)
    state['manual_states']=saved


def device_profession(device):
    return str(device.get('profession') or '').strip() or '未分类'


def selected_devices(active, scope):
    devices = active['batches'][scope]['devices']
    selected = active.get('professions')
    return [device for device in devices if device_profession(device) in selected] if isinstance(selected, list) else devices


def selected_form_devices(active, scope):
    # Reapply the current omission to persisted snapshots without modifying
    # their original data, including snapshots created before this change.
    devices = omit_occupied_form_cells(active['batches'][scope].get('form_devices', []), [])
    selected = active.get('professions')
    return [device for device in devices if device_profession(device) in selected] if isinstance(selected, list) else devices


def review_devices(active, scope):
    return [*selected_devices(active, scope), *selected_form_devices(active, scope)]


def profession_version(active, scope, profession):
    reviews = active.get('reviews', {})
    devices = [*selected_devices(active, scope),
               *(device for device in selected_form_devices(active, scope) if device['id'] in reviews)]
    revisions = sorted((device['id'], reviews.get(device['id'], {}).get('revision', 0))
                       for device in devices if device_profession(device) == profession)
    return hashlib.sha256(json.dumps(revisions, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()


def all_professions_confirmed(active, scope, confirmations):
    return all(confirmations.get(item, {}).get('version') == profession_version(active, scope, item)
               for item in active.get('professions', []))


def review_content(review):
    return {key: review.get(key, default) for key, default in
            (('values', {}), ('result', ''), ('note', ''))}


def source_text(value):
    if value is None:
        return ''
    if isinstance(value, bool):
        return '是' if value else '否'
    return str(value)


def archive_integer(value):
    if value is None or value == '':
        return None
    try:
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            raise ValueError('不是数字')
        number = Decimal(str(value))
        if not number.is_finite() or number != number.to_integral_value() or number < 0:
            raise ValueError('不是非负整数')
        return int(number)
    except (InvalidOperation, ValueError, OverflowError):
        raise RiskError('巡检归档中的数字字段无法核验，请检查原记录。', 422) from None


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
        fields = {item['id']: item for item in parameters(device) if not item.get('read_only')}
        previous = reviews.get(identity, {})
        old_values = previous.get('values') if isinstance(previous.get('values'), dict) else {}
        # Existing values for newly hidden fields are preserved, but clients
        # cannot create or change those hidden fields through an old page.
        hidden_changed = (isinstance(values, dict) and any(
            key not in fields and (key not in old_values or value != old_values[key])
            for key, value in values.items()))
        if (not isinstance(values, dict) or hidden_changed
                or any(not valid_cell_text(value, 160) for value in values.values())
                or any(fields[key].get('value_type') == '状态' and value.strip() not in ('', '√', '×', '△') for key, value in values.items() if key in fields)
                or result not in ('', '正常', '异常') or not valid_cell_text(note, 1000)):
            raise RiskError(f'「{device["name"]}」的现场填写无效，请核对后重试。', 400)
        preserved = {key: value for key, value in old_values.items() if key not in fields}
        normalized = {'values': {**preserved, **{key: value.strip() for key, value in values.items()}},
                      'result': result, 'note': note.strip()}
        if review_content(previous) == normalized:
            continue
        revision = previous.get('revision', 0)
        if revision != review['revision']:
            raise RiskError(f'「{device["name"]}」已被其他页面更新，请刷新并核对。', 409)
        reviews[identity] = {**normalized, 'revision': revision + 1, 'actor': actor}
        changed = True
    return reviews, active.get('review_version', 0) + int(changed)


def finalize_reviews(devices, reviews, actor, confirmations=None):
    if not devices:
        raise RiskError('没有可归档的巡检设备。', 409)
    finalized = {}
    for device in devices:
        previous = reviews.get(device['id'], {})
        review = review_content(previous)
        if (any(not valid_cell_text(value, 160) for value in review['values'].values())
                or not valid_cell_text(review['note'], 1000)):
            raise RiskError(f'「{device["name"]}」的读数或备注含无效字符，请重新填写。', 400)
        rows = [item for item in parameters(device) if not item.get('read_only')]
        values = dict(review['values'])
        for item in rows:
            if item['id'] not in values:
                value = source_text(item['reference'])
                if not valid_cell_text(value, 32767):
                    raise RiskError(f'「{device["name"]}」的源数据含无效字符，请先核对。', 409)
                values[item['id']] = value
        complete = bool(rows) and all(values.get(item['id'], '').strip() for item in rows)
        states = [values.get(item['id'], '').strip() for item in rows if item.get('value_type') == '状态']
        if any(value not in ('', '√', '×', '△') for value in states):
            raise RiskError(f'「{device["name"]}」的运行状态需选择 √、× 或 △。', 400)
        if '×' in states and not review['note'].strip():
            raise RiskError(f'「{device["name"]}」有异常状态，请填写备注。', 400)
        confirmed_by = (confirmations or {}).get(device_profession(device), {}).get('actor') or actor
        finalized[device['id']] = {**previous, 'values': values,
            'result': '异常' if '×' in states else review['result'] or ('正常' if complete else ''), 'note': review['note'],
            'actor': previous.get('actor') or confirmed_by}
    return finalized


class DailyInspection:
    def __init__(self, store, cloud, source_clouds=None):
        self.store, self.cloud = store, cloud
        self.source_clouds = source_clouds or {}
        self.locks = {scope: threading.RLock() for scope in SOURCES}
        self.rollover_locks = {scope: threading.Lock() for scope in SOURCES}
        self.source_locks = {scope: threading.Lock() for scope in SOURCES}
        self.source_cache_lock = threading.RLock()
        self.source_cache = {}
        self.schema_lock = threading.Lock()

    def path(self, table):
        return self.cloud.table_path(BASE, table)

    def records(self, table, query, page_size=500, cloud=None):
        cloud = cloud or self.cloud
        rows, ids, cursors = [], set(), set()
        cursor, total = None, None
        for _ in range(50):
            params = {'page_size': page_size}
            if cursor:
                params['page_token'] = cursor
            result = cloud.request('POST', self.path(table) + '/records/search', params=params, json=query)
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

    def capture_fresh(self, scopes):
        batches = {}
        for scope in scopes:
            cloud = self.source_clouds.get(scope, self.cloud)
            template_rows = self.records(TEMPLATES[0], {'view_id': TEMPLATES[1]}, cloud=cloud)
            templates = {}
            for row in template_rows:
                fields = row['fields']
                if plain(fields.get('楼栋')).removesuffix('楼') == scope:
                    names = [item['name'] for item in (fields.get('附件') or []) if isinstance(item, dict) and item.get('name')]
                    templates.setdefault((scope, plain(fields.get('专业'))), []).extend(names)
            (table, _view), (point_table, point_view) = SOURCES[scope]
            points = self.records(point_table, {'view_id': point_view}, cloud=cloud)
            # The producer seals the *whole table*, not a potentially filtered view.
            rows = self.records(table, {}, cloud=cloud)
            marker = validate_source_batch(scope, rows)
            if marker is None:
                raise RiskError(f'{scope}楼源表尚无可核验的完整批次，请等待上传完成后重试。', 409)
            check = self.records(table, {}, cloud=cloud)
            if (validate_source_batch(scope, check) != marker
                    or json.dumps(sorted(rows, key=lambda item: item['record_id']), ensure_ascii=False, sort_keys=True)
                    != json.dumps(sorted(check, key=lambda item: item['record_id']), ensure_ascii=False, sort_keys=True)):
                raise RiskError(f'{scope}楼读取期间数据发生变化，请稍后重试。', 409)
            point_check = self.records(point_table, {'view_id': point_view}, cloud=cloud)
            if (json.dumps(sorted(points, key=lambda item: item['record_id']), ensure_ascii=False, sort_keys=True)
                    != json.dumps(sorted(point_check, key=lambda item: item['record_id']), ensure_ascii=False, sort_keys=True)):
                raise RiskError(f'{scope}楼测点配置在读取期间发生变化，请稍后重试。', 409)
            batch = marker['upload_time']
            devices = devices_for(scope, rows, points, templates)
            batches[scope] = {'batch': batch, 'rows': rows, 'devices': devices,
                              'form_devices': omit_occupied_form_cells(copy.deepcopy(supplemental_form_devices(scope)), devices)}
        return batches

    def cached_source(self, scope):
        with self.source_cache_lock:
            value = self.source_cache.get(scope)
            if value is None and self.store:
                value = self.store.get_document(NS, 'source-cache:' + scope)
                if isinstance(value, dict) and value.get('version') == 2:
                    batch = value.get('batch') or {}
                    marker = validate_source_batch(scope, batch.get('rows'))
                    if marker and marker.get('upload_time') == batch.get('batch') and isinstance(batch.get('devices'), list):
                        self.source_cache[scope] = value
                    else:
                        value = None
                else:
                    value = None
            return value

    def refresh_source(self, scope):
        with self.source_locks[scope]:
            cached = self.cached_source(scope)
            if cached and time.time() - cached.get('refreshed_at', 0) < 300:
                return cached
            batch = self.capture_fresh([scope])[scope]
            value = {'version': 2, 'refreshed_at': time.time(), 'batch': batch}
            with self.source_cache_lock:
                self.source_cache[scope] = value
            if self.store:
                try:
                    self.store.put_document(NS, 'source-cache:' + scope, value)
                except Exception:
                    logging.exception('%s楼巡检数据本地缓存未持久化', scope)
            return value

    def capture(self, scopes, allow_stale=False):
        result = {}
        for scope in scopes:
            if scope not in SOURCES:
                raise RiskError('巡检楼栋无效。', 400)
            value = self.cached_source(scope)
            stale = not value or time.time() - value.get('refreshed_at', 0) >= 300
            if stale and not (allow_stale and value):
                value = self.refresh_source(scope)
                stale = False
            result[scope] = copy.deepcopy(value['batch'])
            result[scope]['cache_refreshed_at'] = value['refreshed_at']
            result[scope]['cache_stale'] = stale and bool(allow_stale)
        return result

    def load(self, scope):
        return self.store.get_document(NS, 'building:' + scope) or {'active': None, 'history': []}

    def order(self, owner, scope, view='ALL'):
        if view != 'ALL':
            key = hashlib.sha256(view.encode()).hexdigest()
            order = self.store.get_document(NS, f'order-profession:{scope}:{owner}:{key}')
            if order is not None:
                return order['ids']
            return self.order(owner, scope)
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
        return len({row['id'] for row in [*state['history'], *state.get('rollovers', {}).values()] if duty_key(row['duty']) == duty_key(duty)})

    @staticmethod
    def history_with_keys(history, owner):
        return [{**row, 'draft_key': hashlib.sha256(f'{owner}:{row["id"]}'.encode()).hexdigest()}
                for row in history]

    def recent_history(self, scopes, owner):
        rows = [row for scope in scopes for row in self.load(scope)['history']]
        return self.history_with_keys(sorted(rows, key=lambda row: row.get('completed') or 0, reverse=True)[:80], owner)

    def view(self, state, owner, duty):
        active = state.get('active')
        scope = duty['scope']
        history = self.history_with_keys(state['history'], owner)
        result = {'template_version':template_version(), 'duty': duty, 'history': history, 'completed_rounds': self.completed_rounds(state, duty),
                  'round_limit': ROUNDS_PER_SHIFT, 'active': None}
        if active:
            batch = active['batches'][scope]
            selected = {device['id'] for device in selected_devices(active, scope)}
            professions = list(dict.fromkeys(device_profession(device) for device in selected_devices(active, scope)))
            result['active'] = {'id': active['id'], 'duty': active['duty'], 'round': active['round'],
                                'started': active['started'], 'pending': 'completed' in active,
                                'draft_key': hashlib.sha256(f'{owner}:{active["id"]}'.encode()).hexdigest(),
                                'reviews': active.get('reviews', {}), 'review_version': active.get('review_version', 0),
                                'professions': active.get('professions'),
                                'profession_confirmations': active.get('profession_confirmations', {}),
                                'profession_versions': {profession: profession_version(active, scope, profession)
                                    for profession in active.get('professions', [])},
                                'orders': {view: self.order(owner, scope, view) for view in ['ALL', *professions]},
                                'batch': {'batch': batch['batch'], 'count': len(batch['rows']),
                                          'devices': [display_device(device)
                                                      for device in self.ordered(batch, self.order(owner, scope))
                                                      if device['id'] in selected],
                                          'form_devices': [display_device(device)
                                                           for device in selected_form_devices(active, scope)]}}
        return result

    def rollover_due(self, scope, now=None):
        """Freeze unfinished work locally at the next slot without claiming a cloud receipt."""
        from .daily_inspection_reminders import slot_time
        now = now or dt.datetime.now(CN)
        with self.locks[scope]:
            state = self.load(scope)
            active = state.get('active')
            if not active:
                return
            duty = active['duty']
            day = dt.date.fromisoformat(duty['date'])
            if active['round'] == 1:
                cutoff = slot_time(duty['date'], duty['shift'], 2)
            elif duty['shift'] == '白':
                cutoff = slot_time(duty['date'], '夜', 1)
            else:
                cutoff = slot_time((day + dt.timedelta(days=1)).isoformat(), '白', 1)
            if now < cutoff:
                return
            frozen = copy.deepcopy(active)
            if 'completed' not in frozen:
                frozen.update(auto_closed=True, actor='系统自动收尾（未人工确认）', completed=int(cutoff.timestamp()*1000), archive_order=[])
            pending = {'active': frozen, 'history': []}
            state.setdefault('rollovers', {})[frozen['id']] = {'id':frozen['id'], 'duty':frozen['duty'], 'round':frozen['round']}
            state['active'] = None
            self.store.put_documents(NS, {'building:'+scope:state, 'rollover:'+frozen['id']:pending,
                                         'submitted:'+frozen['id']:frozen})

    def retry_rollovers(self, scope):
        # Remote retries must not hold the current round's editing lock.
        worker_lock = self.rollover_locks[scope]
        if not worker_lock.acquire(blocking=False):
            return
        try:
            with self.locks[scope]:
                identities = list(self.load(scope).get('rollovers', {}))
            for identity in identities:
                key = 'rollover:'+identity
                pending = self.store.get_document(NS,key)
                if not pending:
                    logging.error('%s楼巡检自动收尾快照缺失，保留任务 %s',scope,identity)
                    continue
                try:
                    if pending.get('active'):
                        active = pending['active']
                        self.finish(key,pending,active,active['actor'],active.get('archive_order', []))
                        self.store.put_document(NS,key,pending)
                    history = pending.get('history', [])
                    if not any(row['id']==identity for row in history):
                        continue
                    with self.locks[scope]:
                        # Reload after remote I/O so concurrent edits/new rounds survive.
                        state = self.load(scope)
                        combined = {row['id']:row for row in [*state['history'],*history]}
                        state['history'] = sorted(combined.values(),key=lambda row:row.get('completed',0),reverse=True)[:40]
                        state.get('rollovers',{}).pop(identity,None)
                        self.store.put_document(NS,'building:'+scope,state)
                except Exception:
                    logging.exception('%s楼自动收尾巡检上传待重试，原快照保留',scope)
        finally:
            worker_lock.release()

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
                batches = self.capture([scope])
                available = list(dict.fromkeys(device_profession(device) for device in batches[scope]['devices']))
                requested = data.get('professions')
                if requested is not None and (not isinstance(requested, list) or not requested
                        or any(not isinstance(item, str) for item in requested)
                        or len(requested) != len(set(requested)) or set(requested) - set(available)):
                    raise RiskError('可巡检专业已变化，请刷新后重新选择。', 409)
                state['active'] = {'id': uuid.uuid4().hex, 'actor': actor, 'started_by': actor,
                                   'started': int(time.time() * 1000), 'duty': duty, 'round': completed + 1,
                                   'batches': batches}
                state['active']['sheet_groups'] = (self.store.get_document(NS, 'sheet-groups:' + scope + ':' + duty['date'] + ':' + duty['shift']) or {}).get('groups', {})
                if requested is not None:
                    state['active'].update(professions=[item for item in available if item in requested],
                                           profession_confirmations={})
                inherit_manual_states(state,state['active'],scope)
            elif action == 'cancel':
                raise RiskError('取消本轮功能已移除，请继续完成并归档当前巡检。', 409)
            else:
                if action == 'finish' and any(row['id'] == data.get('id') for row in state['history']):
                    return self.view(state, owner, duty)
                active = self.current(state, data.get('id'))
                devices = review_devices(active, scope)
                if action == 'order':
                    ids = data.get('order')
                    order_view = data.get('view', 'ALL')
                    if 'completed' in active:
                        raise RiskError('当前楼栋无法调整顺序。', 409)
                    if not isinstance(order_view, str) or (order_view != 'ALL' and
                            order_view not in {device_profession(row) for row in devices}):
                        raise RiskError('排序专业已变化，请重新打开排序。', 409)
                    known = {row['id'] for row in devices if not row.get('form_only') and
                             (order_view == 'ALL' or device_profession(row) == order_view)}
                    if (not isinstance(ids, list) or any(not isinstance(value, str) for value in ids)
                            or len(ids) != len(known) or set(ids) != known):
                        raise RiskError('设备列表已变化，请重新打开再排序。', 409)
                    original = self.order(owner, scope, order_view)
                    relevant = (device['id'] for device in active['batches'][scope]['devices']
                                if order_view == 'ALL' or device_profession(device) == order_view)
                    combined = list(dict.fromkeys([*original, *relevant]))
                    selected_ids, reordered = set(ids), iter(ids)
                    key = (f'order:{scope}:{owner}' if order_view == 'ALL' else
                           f'order-profession:{scope}:{owner}:{hashlib.sha256(order_view.encode()).hexdigest()}')
                    self.store.put_document(NS, key,
                                            {'ids': [next(reordered) if item in selected_ids else item for item in combined]})
                    return self.view(state, owner, duty)
                elif action == 'sheet-group':
                    if 'completed' in active or duty_key(active['duty']) != duty_key(duty) or data.get('review_version') != active.get('review_version', 0):
                        raise RiskError('本班次巡检状态已变化，请刷新后调整。', 409)
                    known = {json.dumps([item['name'], sh['name']], ensure_ascii=False, separators=(',', ':'))
                        for item in original_templates() if item['scope'] == scope for sh in item['sheets']}
                    if data.get('sheet') not in known or data.get('profession') not in ('电气', '暖通'):
                        raise RiskError('工作表或专业无效。', 400)
                    groups = dict(active.get('sheet_groups', {}))
                    groups[data['sheet']] = data['profession']
                    active['sheet_groups'] = groups
                    active['review_version'] = active.get('review_version', 0) + 1
                    active['profession_confirmations'] = {}
                    self.store.put_documents(NS, {state_key: state,
                        'sheet-groups:' + scope + ':' + duty['date'] + ':' + duty['shift']: {'groups': groups}})
                    return self.view(state, owner, duty)
                else:
                    if 'completed' not in active:
                        if data.get('sheet_cells'):
                            fingerprint = hashlib.sha256(json.dumps([data.get('review_version'), data['sheet_cells']], sort_keys=True).encode()).hexdigest()
                            if active.get('last_sheet_edit') == fingerprint:
                                return self.view(state, owner, duty)
                            if data.get('review_version') != active.get('review_version', 0):
                                raise RiskError('巡检表已有其他修改，请刷新核对后重试，您的填写仍保留。', 409)
                            with archived_workbook(workbook(active, {scope: []}, strict_original=False, presentation=False), active['id'], scope) as book:
                                edits = apply_cells(book, scope, data['sheet_cells'], actor, enforce_exclusions=True)
                                submitted = {}
                                for device in devices:
                                    change = edits.get(device['id'])
                                    if not change:
                                        continue
                                    previous = copy.deepcopy(active.get('reviews', {}).get(device['id']) or
                                        {'values': {}, 'result': '', 'note': '', 'revision': 0})
                                    for row_id, value in change['values'].items():
                                        row = list(book[REVIEW_SHEET].iter_rows(min_row=int(row_id), max_row=int(row_id), values_only=True))[0]
                                        matches = []
                                        for param in parameters(device):
                                            if param['name'] != row[7]:
                                                continue
                                            if 'section' in param:
                                                section = device['sections'][param['section']]
                                                matched = section['location'] == row[5] and section['form'] == row[6]
                                            else:
                                                matched = any(pt['id'] == param.get('point') and pt['point_id'] == row[6] for pt in device['points'])
                                            if matched:
                                                matches.append(param['id'])
                                        if len(matches) != 1:
                                            raise RiskError('测点对应不唯一，本次修改未保存。', 409)
                                        previous['values'][matches[0]] = value
                                    previous.update(result=change['result'], note=change['note'])
                                    submitted[device['id']] = previous
                                data = {**data, 'reviews': submitted}
                            overrides = {(item['template'], item['sheet'], item['cell']): item for item in active.get('sheet_overrides', [])}
                            mapped_cells = {(row[0], row[1]) for row in book[ORIGINAL_MAPPING_SHEET].iter_rows(min_row=2, values_only=True)}
                            form_names = {(row[1], row[2]): row[0] for row in book[ORIGINAL_INDEX_SHEET].iter_rows(min_row=2, values_only=True)}
                            overrides.update({(item['template'], item['sheet'], item['cell']): item for item in data['sheet_cells']
                                if (form_names.get((item['template'], item['sheet'])), item['cell']) not in mapped_cells})
                            active['sheet_overrides'] = [item for item in overrides.values()
                                if (form_names.get((item['template'], item['sheet'])), item['cell']) not in mapped_cells]
                            active['last_sheet_edit'] = fingerprint
                            active['review_version'] = active.get('review_version', 0) + 1
                            active['profession_confirmations'] = {}
                        if action == 'finish' and data.get('sheet_workflow'):
                            required = {json.dumps([item['name'], sh['name']], ensure_ascii=False, separators=(',', ':'))
                                for item in original_templates() if item['scope'] == scope
                                for sh in item['sheets']}
                            visited = data.get('visited_sheets')
                            if not isinstance(visited, list) or any(not isinstance(item, str) for item in visited) or not required.issubset(visited):
                                raise RiskError('请先逐页查看本次所有工作表后再确认提交。', 409)
                            if data.get('reviews'):
                                raise RiskError('请先保存单元格修改后再确认提交。', 409)
                            active['profession_confirmations'] = {name: {'actor': actor, 'confirmed': int(time.time()*1000),
                                'version': profession_version(active, scope, name)} for name in active.get('professions', [])}
                        profession = data.get('profession') if action == 'confirm-profession' else None
                        chosen = active.get('professions')
                        if action == 'confirm-profession':
                            if (not isinstance(chosen, list) or profession not in chosen
                                    or data.get('profession_version') != profession_version(active, scope, profession)):
                                raise RiskError('该专业资料已变化，请刷新核对后再确认。', 409)
                            allowed = {device['id'] for device in devices if device_profession(device) == profession}
                            if not isinstance(data.get('reviews', {}), dict) or data.get('reviews', {}).keys() - allowed:
                                raise RiskError('只能确认当前专业的设备。', 400)
                        if action == 'finish' and isinstance(chosen, list) and not all_professions_confirmed(
                                active, scope, active.get('profession_confirmations', {})):
                            raise RiskError('还有已选专业尚未确认，请先逐个确认。', 409)
                        if action == 'finish' and (type(data.get('review_version')) is not int or
                                                   data['review_version'] != active.get('review_version', 0)):
                            raise RiskError('本轮核对资料已变化，请刷新并核对后再归档。', 409)
                        before = active.get('reviews', {})
                        reviews, version = merge_reviews(active, devices, data.get('reviews', {}), actor)
                        confirmations = dict(active.get('profession_confirmations', {}))
                        for device in devices:
                            if review_content(before.get(device['id'], {})) != review_content(reviews.get(device['id'], {})):
                                confirmations.pop(device_profession(device), None)
                        if action == 'finish' and isinstance(chosen, list) and not all(item in confirmations for item in chosen):
                            raise RiskError('已确认的专业数据发生变化，请重新确认。', 409)
                        active.update(reviews=reviews, review_version=version)
                        remember_manual_states(state,active,devices,before,reviews,data.get('reviews',{}),actor)
                        if action == 'confirm-profession':
                            confirmed_version = profession_version(active, scope, profession)
                            if confirmations.get(profession, {}).get('version') != confirmed_version:
                                confirmations[profession] = {'actor': actor, 'confirmed': int(time.time() * 1000),
                                    'version': confirmed_version}
                                active['review_version'] += 1
                            active['profession_confirmations'] = confirmations
                        elif isinstance(chosen, list):
                            active['profession_confirmations'] = confirmations
                        if action == 'finish':
                            active['reviews'] = finalize_reviews(devices, reviews, actor, confirmations)
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
                'group': plain(fields.get('班组')), 'round': archive_integer(fields.get('巡检轮次')),
                'completed': archive_integer(fields.get('完成时间')), 'actor': plain(fields.get('巡检人员')),
                'modified': archive_integer(fields.get('最后修改时间')), 'editor': plain(fields.get('最后修改人')),
                'confirmed': archive_integer(fields.get('现场已核对设备数')), 'devices': archive_integer(fields.get('设备数')),
                'abnormal': archive_integer(fields.get('异常设备数')),
                'url': f'https://vnet.feishu.cn/base/{BASE}?table={ARCHIVE}&view={ARCHIVE_VIEW}&record={rid}'}

    def archive_history(self, scope, day):
        if day:
            try:
                day = dt.date.fromisoformat(day).isoformat()
            except (ValueError, TypeError):
                raise RiskError('请选择有效巡检日期。', 400) from None
        conditions = [{'field_name': '巡检范围', 'operator': 'is', 'value': [scope + '楼']}]
        if day:
            conditions.append({'field_name': '排班日期', 'operator': 'is', 'value': [day]})
        rows = self.records(ARCHIVE, {'filter': {'conjunction': 'and', 'conditions': conditions}})
        summaries = [self.archive_summary(row, scope) for row in rows]
        return sorted((row for row in summaries if row and (not day or row['date'] == day)),
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
        cached = self.store.get_document(NS, 'archive-file:' + attachment['file_token']) or {}
        if cached.get('content'):
            try:
                return base64.b64decode(cached['content'], validate=True)
            except (ValueError, TypeError):
                pass  # Re-fetch a damaged cache from the verified attachment.
        extra = parse_qs(urlsplit(attachment.get('url') or '').query).get('extra', [''])[0]
        with self.cloud.lock:
            headers = self.cloud.headers()
        try:
            content, _ = self.cloud.http.request_bytes('GET',
                API + 'drive/v1/medias/' + attachment['file_token'] + '/download',
                headers=headers, params={'extra': extra} if extra else None, max_bytes=MAX_ARCHIVE)
        except FeishuHTTPError:
            raise RiskError('未能下载飞书巡检附件，请核对附件权限或稍后重试。', 503) from None
        self.store.put_document(NS, 'archive-file:' + attachment['file_token'], {'content': base64.b64encode(content).decode('ascii')})
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
            elif isinstance(value, (int, float)) and not isinstance(value, bool):
                try:
                    number = Decimal(str(actual)) if isinstance(actual, (int, float, str)) and not isinstance(actual, bool) else None
                    matches = number is not None and number.is_finite() and number == Decimal(str(value))
                except InvalidOperation:
                    matches = False
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
        fingerprint = hashlib.sha256(json.dumps([version, changes, payload.get('cells')], sort_keys=True).encode()).hexdigest()
        key = 'history-edit:' + rid
        with self.locks[scope]:
            path, summary, attachment, fields = self.archive_record(scope, rid)
            state = self.load(scope)
            active = state.get('active')
            if summary['round_id'] in state.get('rollovers', {}) or (active and active['id'] == summary['round_id']):
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
                previous_content=self.download_archive(attachment)
                with archived_workbook(previous_content, summary['round_id'], scope) as book:
                    devices = archived_devices(book, scope)
                    if payload.get('cells'):
                        changes = apply_cells(book, scope, payload['cells'], actor)
                    else:
                        edit_archived_devices(book, devices, changes, actor)
                    try:
                        refresh_archived_original(book, changes, actor)
                    except (KeyError, ValueError, TypeError, IndexError) as error:
                        raise RiskError('历史原表映射无法核验，本次修改尚未保存。', 409) from error
                    devices = archived_devices(book, scope)
                    confirmed = sum(any(not row['read_only'] for row in device['rows'])
                                    and all(row['read_only'] or row['value'].strip() for row in device['rows'])
                                    for device in devices)
                    abnormal = sum(device['result'] == '异常' for device in devices)
                    output = io.BytesIO()
                    book.save(output)
                self.ensure_archive_fields({**ARCHIVE_FIELDS, **EDIT_FIELDS})
                updated_content=preserve_artwork(output.getvalue(),read_artwork(io.BytesIO(previous_content)))
                token = self.upload_archive(updated_content, 'DE巡检修订-' + summary['round_id'][:10] + '.xlsx')
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
            self.store.put_document(NS, 'submitted:' + active['id'], copy.deepcopy(active))
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
            '现场已核对设备数': sum(
                bool(parameters(device)) and not any(section.get('missing') for section in device['sections'])
                and review.get('result') in ('正常', '异常')
                and all(item.get('read_only') or review.get('values', {}).get(item['id'], '').strip()
                        for item in parameters(device))
                for scope in active['batches'] for device in selected_devices(active, scope)
                for review in [reviews.get(device['id'], {})]),
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
                '设备数': sum(len(selected_devices(active, scope)) for scope in active['batches']),
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


def workbook(active, orders, *, strict_original=True, presentation=True):
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
        selected = {device['id'] for device in selected_devices(active, scope)}
        ordered = [item for item in DailyInspection.ordered(batch, orders.get(scope, [])) if item['id'] in selected]
        for order, device in enumerate([*ordered, *selected_form_devices(active, scope)], 1):
            for section in device['sections']:
                prefix = (scope + '楼', order, device['name'], device['profession'], section['location'], section['form'])
                for label, value in section['values'].items():
                    summary.append((*prefix, label, value, section['integrity'], section['missing'], section['note']))
            for point in device['points']:
                cells = point.get('round_cells') or {}
                readings.append((scope + '楼', order, device['name'], device['profession'], point['point_id'],
                                 point['name'], point['value'], formatted_time(point['sampled']), point['path'],
                                 point['template'], point['sheet'], point['cell'], point['integrity'], point['note'],
                                 *(cells.get(slot, '') for slot in ('白1', '白2', '夜1', '夜2'))))
            review = active.get('reviews', {}).get(device['id']) or {'values': {}, 'result': '', 'note': ''}
            rows = parameters(device)
            points = {point['id']: point for point in device['points']}
            prefix = (scope + '楼', order, device['id'], device['name'], device['profession'])
            outcome = (review['result'], review['note'], review.get('actor'))
            for item in rows:
                section = device['sections'][item['section']] if 'section' in item else None
                point = points.get(item.get('point'), {})
                location = section['location'] if section is not None else point.get('location')
                source = section['form'] if section is not None else point.get('point_id')
                current_value = review['values'].get(item['id'], source_text(item['reference']))
                status = ('配置停用' if item.get('read_only') else
                          '源数据不完整：' + section['missing'] if section and section.get('missing')
                          and not str(current_value or '').strip() else '')
                if item.get('value_type') == '状态' and not item.get('read_only'):
                    status = '状态测点' + ('；' + status if status else '')
                reviews.append((*prefix, location, source, item['name'], item['reference'],
                                review['values'].get(item['id'], source_text(item['reference'])) or None, status, *outcome))
            if not rows:
                reviews.append((*prefix, '', '', '', '', '', '无可填参数', *outcome))
    duty = active.get('duty', {})
    point_columns = ('楼栋','顺序','设备','专业','测点ID','测点名称','读数','采样时间','测点路径','模板','页签','单元格','数据完整性','备注','白班1单元格','白班2单元格','夜班1单元格','夜班2单元格')
    if strict_original:
        try:
            catalogue = original_templates()
            for scope in active['batches']:
                selected_professions = active.get('professions')
                templates = [item for item in catalogue if item.get('scope') == scope and
                             (not isinstance(selected_professions, list) or item.get('profession') in selected_professions)]
                source_sheets = [{'name':'测点读数','rows':[point_columns, *readings]},
                                 {'name':REVIEW_SHEET,'rows':[REVIEW_COLUMNS, *reviews]}]
                conflicts = [entry for entry in original_cells(source_sheets, templates, duty.get('shift'), active.get('round'))
                             if entry.get('conflict')]
                if conflicts:
                    first = conflicts[0]
                    raise RiskError(f'有 {len(conflicts)} 处原表对应的设备数据与测点读数不一致；'
                                    f'请在原表核对「{first["sheet"].strip()} · {first["cell"]}」后再归档。', 409)
        except (OSError, ValueError, TypeError, KeyError, IndexError) as error:
            raise RiskError('巡检原表或测点映射无法核验，本轮未归档。', 409) from error
    sheet('巡检信息', ('巡检编号', '开始人员', '完成人员', '排班日期', '班次', '巡检轮次',
                     '开始时间', '完成时间', '楼栋', '源表批次', '源表行数', '班组', '巡检专业'),
          [(active['id'], active.get('started_by') or active['actor'], active['actor'] if active.get('completed') else '',
            duty.get('date'), duty.get('shift'), active.get('round'),
            formatted_time(active['started']), formatted_time(active.get('completed')),
            scope + '楼', formatted_time(batch['batch']), len(batch['rows']), duty.get('group', ''),
            '、'.join(active.get('professions') or sorted({device_profession(device) for device in batch['devices']})))
           for scope, batch in active['batches'].items()])
    if isinstance(active.get('professions'), list):
        confirmations = active.get('profession_confirmations', {})
        sheet('专业确认', ('专业', '确认人员', '确认时间', '资料版本'),
              [(profession, confirmations.get(profession, {}).get('actor', ''),
                formatted_time(confirmations.get(profession, {}).get('confirmed')),
                confirmations.get(profession, {}).get('version', '')) for profession in active['professions']])
    sheet('确认方式', ('方式','说明'), [('自动收尾' if active.get('auto_closed') else '人工确认', '保留采集值和已保存修改，未进行人工核对' if active.get('auto_closed') else '')])
    sheet('工作表分组', ('原表标识', '本班专业'), [(key, value) for key, value in active.get('sheet_groups', {}).items()])
    sheet('DE巡检表', ('楼栋','顺序','设备','专业','位置','巡检表单','参数','读数','数据完整性','缺少源数据','备注'), summary)
    sheet('测点读数', point_columns, readings)
    sheet(REVIEW_SHEET, REVIEW_COLUMNS, reviews)
    for scope, batch in active['batches'].items():
        rows = batch['rows']
        columns = list(dict.fromkeys(key for row in rows for key in row['fields']))
        sheet(scope + '楼源表快照', ['源记录ID', *columns],
              ([row['record_id'], *(row['fields'].get(key) for key in columns)] for row in rows))
    # Freeze the rendered form and its mapping together with the review data.
    # History edits update only these exact saved coordinates, not today's catalogue.
    mappings, form_index, titles = [], [], set(book.sheetnames)
    artworks={}
    try:
        catalogue = original_templates()
        for scope in active['batches']:
            professions = active.get('professions')
            templates = [item for item in catalogue if item.get('scope') == scope and
                         (not isinstance(professions, list) or item.get('profession') in professions)]
            report_sheets = [{'name':'测点读数','rows':[point_columns, *readings]},
                             {'name':REVIEW_SHEET,'rows':[REVIEW_COLUMNS, *reviews]}]
            entries = original_cells(report_sheets, templates, duty.get('shift'), active.get('round'))
            if not strict_original:
                entries = [entry for entry in entries if not entry.get('conflict')]
            views = original_views(templates, [{'shift':duty.get('shift'), 'round':active.get('round'),
                                                'original_cells':entries}])
            overlay_views(views, active.get('sheet_overrides', []))
            names = {}
            for source in views:
                base = re.sub(r'[\\/*?:\[\]]', '_', source['name'])[:27]
                title, suffix = base, 1
                while title in titles:
                    suffix += 1
                    title = base[:27] + f'-{suffix}'
                titles.add(title)
                names[(source['template'], source['sheet'])] = title
                form_index.append((title, source['template'], source['sheet']))
                target = book.create_sheet(title)
                write_original_sheet(target, source if presentation else {**source,'excel_styles':[],'excel_settings':{}})
                if presentation and source.get('artwork'):artworks[title]=source['artwork']
            for entry in entries:
                name = names.get((entry['template'], entry['sheet']))
                if not name:
                    raise ValueError('原表不存在')
                for member in entry.get('members', [entry]):
                    mappings.append((name, entry['cell'], member['device'], member['kind'], member['point_id'],
                                     member['parameter'], member.get('location'), member.get('form')))
    except (OSError, ValueError, TypeError, KeyError, IndexError) as error:
        raise RiskError('巡检原表或测点映射无法核验，本轮未归档。', 409) from error
    sheet(ORIGINAL_MAPPING_SHEET, ORIGINAL_MAPPING_COLUMNS, mappings)
    book[ORIGINAL_MAPPING_SHEET].sheet_state = 'hidden'
    sheet(ORIGINAL_INDEX_SHEET, ORIGINAL_INDEX_COLUMNS, form_index)
    book[ORIGINAL_INDEX_SHEET].sheet_state = 'hidden'
    stream = io.BytesIO()
    book.save(stream)
    return preserve_artwork(stream.getvalue(),artworks)


def inspection_report(service, scope, day, owner, actor, *, include_active=False, drafts=None, active_id=None):
    """Read one duty date's archived sheets and, when authorized, the current snapshot."""
    try:
        day = dt.date.fromisoformat(day).isoformat()
    except (ValueError, TypeError):
        raise RiskError('请选择有效巡检日期。', 400) from None
    if scope not in SOURCES:
        raise RiskError('请选择有效的巡检楼栋。', 400)
    if not isinstance(drafts, dict):
        raise RiskError('本机填写资料格式无效。', 400)
    with service.locks[scope]:
        state = service.load(scope)
        pending_rounds = [copy.deepcopy(service.store.get_document(NS,'rollover:'+identity) or {}) for identity in state.get('rollovers', {})]
        tracked_active = copy.deepcopy(state.get('active'))
        active = tracked_active if include_active else None
        order = service.order(owner, scope) if active else []
    if drafts:
        if (not active or active.get('id') != active_id or active.get('duty', {}).get('date') != day
                or 'completed' in active):
            raise RiskError('本机填写对应的巡检已变化，请刷新并核对后再查看或导出。', 409)
        before = active.get('reviews', {})
        reviews, version = merge_reviews(active, review_devices(active, scope), drafts,
                                         actor + '（本机未共享）')
        if isinstance(active.get('professions'), list):
            confirmations = dict(active.get('profession_confirmations', {}))
            for device in review_devices(active, scope):
                if review_content(before.get(device['id'], {})) != review_content(reviews.get(device['id'], {})):
                    confirmations.pop(device_profession(device), None)
            active['profession_confirmations'] = confirmations
        active.update(reviews=reviews, review_version=version)

    archive_warning = ''
    try:
        archives = service.archive_history(scope, day)
    except (RiskError, FeishuHTTPError):
        if not pending_rounds:
            raise
        archives = []
        archive_warning = '云端历史暂未读取，当前展示本地已保存的巡检；恢复连接后可刷新。'
    try:
        templates = [item for item in original_templates() if item.get('scope') == scope]
    except (OSError, ValueError, TypeError, KeyError):
        logging.exception('巡检原表布局不可用')
        templates = []
    result = {'scope': scope, 'date': day, 'captured': int(time.time() * 1000), 'rounds': [],
              'original_templates': templates, 'archive_warning':archive_warning}
    cells = 0

    def collect(summary, content):
        nonlocal cells
        with archived_workbook(content, summary['round_id'], scope) as book:
            sheets = []
            original_names = ({row[0] for row in book[ORIGINAL_INDEX_SHEET].iter_rows(min_row=2, values_only=True) if row[0]}
                              if ORIGINAL_INDEX_SHEET in book else set())
            for sheet in book.worksheets:
                if sheet.title in {ORIGINAL_MAPPING_SHEET, ORIGINAL_INDEX_SHEET} or sheet.title in original_names:
                    continue
                if sheet.max_row > MAX_ROWS or sheet.max_column > 200:
                    raise RiskError('巡检附件数据过大，请核对原记录后重试。', 413)
                cells += sheet.max_row * sheet.max_column
                if cells > 1000000:
                    raise RiskError('当日巡检数据过大，请分开查看原记录。', 413)
                rows = []
                for row in sheet.iter_rows(values_only=True):
                    rows.append([value.isoformat(sep=' ') if isinstance(value, dt.datetime)
                                 else value.isoformat() if isinstance(value, (dt.date, dt.time))
                                 else value if isinstance(value, (str, int, float, bool)) or value is None
                                 else str(value) for value in row])
                sheets.append({'name': sheet.title, 'rows': rows})
            entries = (frozen_readings(book, scope) if summary.get('status') == '已归档' and ORIGINAL_MAPPING_SHEET in book and ORIGINAL_INDEX_SHEET in book
                       else original_cells(sheets, templates, summary.get('shift'), summary.get('round')))
            extras = form_extras(book, templates, entries) if ORIGINAL_INDEX_SHEET in book else []
            groups = {row[0]:row[1] for row in book['工作表分组'].iter_rows(min_row=2, values_only=True) if row[0]} if '工作表分组' in book else {}
            automatic = '确认方式' in book and book['确认方式']['A2'].value == '自动收尾'
            result['rounds'].append({**summary, 'auto_closed':automatic, 'sheets': sheets, 'original_cells': entries, 'extra_cells': extras, 'sheet_groups': groups})

    archived_ids = set()
    for summary in archives:
        _, confirmed, attachment, _ = service.archive_record(scope, summary['id'])
        if any(confirmed.get(key) != summary.get(key) for key in ('round_id', 'date', 'scope', 'shift', 'round')):
            raise RiskError('巡检归档在读取时发生变化，请刷新后重试。', 409)
        if confirmed['round_id'] in archived_ids:
            raise RiskError('发现重复巡检归档，请先核对原记录。', 409)
        archived_ids.add(confirmed['round_id'])
        pending_active = next((item.get('active') for item in pending_rounds if (item.get('active') or {}).get('id') == confirmed['round_id']),None)
        if tracked_active and 'completed' in tracked_active and tracked_active['id'] == confirmed['round_id']:
            pending_active = tracked_active
        is_pending = bool(pending_active)
        if is_pending and pending_active.get('file_token') != attachment.get('file_token'):
            raise RiskError('本轮巡检归档附件尚未核验，请稍后重试。', 409)
        collect({**confirmed, 'shift': (confirmed.get('shift') or '').removesuffix('班'),
                 'status': '待归档核验' if is_pending else '已归档', 'version': service.archive_version(attachment), 'local_draft': False},
                service.download_archive(attachment))
    for pending in pending_rounds:
        saved = pending.get('active')
        if saved and saved['duty']['scope']==scope and saved['duty']['date']==day and saved['id'] not in archived_ids:
            collect({'round_id':saved['id'],'scope':scope,'date':day,'shift':saved['duty']['shift'],
                     'round':saved['round'],'version':saved.get('review_version',0),'status':'自动确认·待上传',
                     'auto_closed':bool(saved.get('auto_closed')),'local_draft':False},
                    workbook(saved,{scope:saved.get('archive_order',[])},strict_original=False,presentation=False))
    if active and active.get('duty', {}).get('date') == day:
        if active['id'] in archived_ids:
            if drafts:
                raise RiskError('本轮已归档，本机修改尚未保存，请刷新核对。', 409)
        else:
            summary = {'round_id': active['id'], 'scope': scope, 'date': day,
                       'shift': active['duty']['shift'], 'round': active['round'],
                       'actor': active.get('actor', ''), 'version': active.get('review_version', 0), 'status': '待归档' if 'completed' in active else '进行中',
                       'local_draft': bool(drafts)}
            collect(summary, workbook(active, {scope: active.get('archive_order', order)}, strict_original=False, presentation=False))
    result['rounds'].sort(key=lambda item: (0 if item.get('shift') == '白' else 1,
                                            item.get('round') or 0, item['round_id']))
    group_shift = active['duty']['shift'] if active and active['duty']['date'] == day else (result['rounds'][-1]['shift'] if result['rounds'] else duty_period(dt.datetime.now(CN))[1])
    result['sheet_groups'] = (service.store.get_document(NS, 'sheet-groups:' + scope + ':' + day + ':' + group_shift) or {}).get('groups', {})
    return result


def inspection_report_workbook(report):
    from openpyxl import Workbook
    from openpyxl.cell import WriteOnlyCell
    book = Workbook(write_only=True)
    titles = set()
    artworks={}
    try:
        original = original_views(report.get('original_templates', []), report['rounds'], include_empty_rounds=True)
    except (ValueError, IndexError, TypeError, KeyError) as error:
        raise RiskError('原表测点位置存在冲突，请核对后重试导出。', 409) from error
    if not original:
        raise RiskError('未找到该楼栋的巡检表模板，无法导出。', 409)
    for source in original:
        base = re.sub(r'[\\/*?:\[\]]', '_', source['name'])[:27]
        title, suffix = base, 1
        while title in titles:
            suffix += 1
            title = base[:27] + f'-{suffix}'
        titles.add(title)
        target = book.create_sheet(title)
        write_original_sheet(target, source)
        if source.get('artwork'):artworks[title]=source['artwork']
    stream = io.BytesIO()
    book.save(stream)
    return preserve_artwork(stream.getvalue(),artworks)


def install_daily_inspection(app, controller, runtime):
    service = None
    lock = threading.Lock()
    stopped = threading.Event()

    def manager():
        nonlocal service
        with lock:
            if service is None:
                store = runtime.state_store
                config = Path(store.db_path).parent / 'feishu_assistant.json'
                service = DailyInspection(store, RiskCloud(None, config, None, None),
                                          {scope: RiskCloud(None, config, None, None) for scope in SOURCES})
            return service

    def prefetch(scope):
        last_refresh = 0
        while not stopped.is_set():
            try:
                manager().rollover_due(scope)
                manager().retry_rollovers(scope)
                if time.monotonic()-last_refresh >= 300:
                    manager().refresh_source(scope)
                    last_refresh = time.monotonic()
            except Exception:
                logging.exception('%s楼巡检参考数据预取失败，保留上次完整批次', scope)
            stopped.wait(60)

    def startup():
        for scope in SOURCES:
            threading.Thread(target=prefetch, args=(scope,), name='daily-inspection-prefetch-' + scope, daemon=True).start()

    app.add_event_handler('startup', startup)
    app.add_event_handler('shutdown', stopped.set)

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
            if not owner or not scopes:
                raise RiskError('需要每日任务权限及 D 楼或 E 楼范围。', 403)
            can_manage = 'daily' in granted
            home_scopes = await asyncio.to_thread(inspection_history_scopes, auth, session)
            from .portal_service import LI_SHILONG_OPEN_ID
            viewer = owner == LI_SHILONG_OPEN_ID and not session.get('impersonation') and session.get('source') != 'admin_impersonation'
            history_scopes = scopes if can_manage or viewer else home_scopes
            if not can_manage and not history_scopes:
                raise RiskError('没有可查看的巡检记录。', 403)
            local_write = bool(home_scopes and not session.get('impersonation') and session.get('source') != 'admin_impersonation')
            selected_scope = request.query_params.get('view_scope', '')
            if selected_scope and not ((viewer and selected_scope in scopes) or selected_scope in home_scopes
                                       or (can_manage and selected_scope in scopes)):
                raise RiskError('没有该楼栋的当前巡检查看权限。', 403)
            svc = await asyncio.to_thread(manager)
            action = request.path_params.get('action', '')
            if request.method == 'GET' and action == 'templates':
                template_scope = request.query_params.get('scope')
                if template_scope not in history_scopes:
                    raise RiskError('没有该楼栋的巡检权限。',403)
                data = {'version':template_version(),'scope':template_scope,
                        'templates':display_templates(template_scope)}
                return JSONResponse({'ok':True,'data':data},headers={'Cache-Control':'private, max-age=3600'})
            if request.method == 'GET' and action in ('','status') and (viewer or home_scopes and (not selected_scope or selected_scope in home_scopes)):
                scope = selected_scope or (scopes[0] if viewer else home_scopes[0])
                if not viewer and scope not in home_scopes:
                    raise RiskError('没有该楼栋的巡检权限。',403)
                await asyncio.to_thread(svc.rollover_due,scope)
                state = await asyncio.to_thread(svc.load,scope)
                date,shift = duty_period(dt.datetime.now(CN))
                duty = {'scope':scope,'date':date,'shift':shift}
                if state.get('active') and duty_key(state['active']['duty'])==duty_key(duty):
                    duty=state['active']['duty']
                if action=='status' and state.get('active') and request.query_params.get('id')==state['active']['id'] and request.query_params.get('version')==str(state['active'].get('review_version',0)) and request.query_params.get('pending')==str(int('completed' in state['active'])) and request.query_params.get('date')==date and request.query_params.get('shift')==shift:
                    data={'unchanged':True,'history_scopes':history_scopes}
                else:
                    data=await asyncio.to_thread(svc.view,state,owner,duty)
                    data.update(history_scopes=history_scopes,view_scope=scope,view_scopes=scopes if viewer else home_scopes,can_edit=not session.get('impersonation') and session.get('source')!='admin_impersonation')
                return JSONResponse({'ok':True,'data':data},headers={'Cache-Control':'private, no-store'})
            report_action = action in {'day-sheet', 'day-export'}
            if request.method == 'POST':
                local_actions = {'start', 'order', 'draft', 'cancel', 'finish', 'confirm-profession', 'history-edit', 'sheet-group'}
                if not can_manage and not report_action and not ((local_write or viewer) and action in local_actions):
                    raise RiskError('当前账号仅可查看本楼历史巡检。', 403)
                source = urlsplit(request.headers.get('origin') or request.headers.get('referer') or '')
                expected = urlsplit(controller._request_base_url(request))
                if (source.scheme, source.netloc) != (expected.scheme, expected.netloc) or request.headers.get('sec-fetch-site') == 'cross-site':
                    raise RiskError('请从当前站点办理巡检。', 403)
                try:
                    payload = await controller._read_json_request(
                        request, max_bytes=4 * 1024 * 1024 if action in {'draft', 'finish', 'history-edit', 'day-sheet', 'day-export'} else 128 * 1024)
                except ValueError as error:
                    raise RiskError('巡检资料格式无效或超过提交大小限制。', 400) from error
                if not isinstance(payload, dict) or (action != 'start' and not report_action and not isinstance(payload.get('id'), str)):
                    raise RiskError('巡检请求格式无效。', 400)
            if report_action:
                if request.method != 'POST':
                    raise RiskError('巡检操作不存在。', 404)
                scope = payload.get('scope')
                if scope not in history_scopes:
                    raise RiskError('没有该楼栋的巡检权限。', 403)
                submitted = payload.get('drafts', {})
                duty = None
                if can_manage and not (viewer or local_write and scope in home_scopes):
                    from .personnel_source import daily_roster
                    try:
                        duty = await asyncio.to_thread(resolve_duty, lambda date: daily_roster(runtime.service, date), user, scopes)
                    except RiskError:
                        pass  # Archived records remain readable even when the roster is temporarily unavailable.
                can_write_scope = bool(not session.get('impersonation') and session.get('source') != 'admin_impersonation'
                    and (viewer or (duty and duty['scope'] == scope) or (local_write and scope in home_scopes)))
                if submitted and not can_write_scope:
                    raise RiskError('只有本楼有权限的人员可以带入本机填写。', 403)
                report = await asyncio.to_thread(inspection_report, svc, scope, payload.get('date'), owner,
                    user.get('name') or owner, include_active=viewer or can_write_scope or scope in home_scopes,
                    drafts=submitted, active_id=payload.get('active_id'))
                if action == 'day-sheet' and payload.get('form_only') is True:
                    for item in report['rounds']:
                        item.pop('sheets', None)
                if action=='day-sheet' and payload.get('template_version')==template_version():
                    report.pop('original_templates',None)
                elif action=='day-sheet':
                    report['original_templates']=display_templates(scope)
                report['template_version']=template_version()
                report['can_edit_history'] = bool((viewer or can_manage or (local_write and scope in home_scopes)) and not session.get('impersonation') and session.get('source') != 'admin_impersonation')
                report['draft_key'] = hashlib.sha256(f'{owner}:{scope}:{report["date"]}'.encode()).hexdigest()
                if action == 'day-export':
                    if not report['rounds']:
                        raise RiskError('该日期没有可导出的巡检记录。', 404)
                    content = await asyncio.to_thread(inspection_report_workbook, report)
                    filename = quote(f'{scope}楼-{report["date"]}-白夜班巡检.xlsx')
                    return Response(content, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                        headers={'Content-Disposition': f"attachment; filename*=UTF-8''{filename}",
                                 'Cache-Control': 'private, no-store'})
                return JSONResponse({'ok': True, 'data': report}, headers={'Cache-Control': 'private, no-store'})
            if action in {'history', 'history-record', 'history-edit'}:
                scope = request.query_params.get('scope')
                if scope not in history_scopes:
                    raise RiskError('没有该楼栋的巡检权限。', 403)
                can_edit_history = bool((viewer or can_manage or (local_write and scope in home_scopes))
                                        and not session.get('impersonation')
                                        and session.get('source') != 'admin_impersonation')
                if request.method == 'GET' and action == 'history':
                    data = await asyncio.to_thread(svc.archive_history, scope, request.query_params.get('date'))
                elif request.method == 'GET' and action == 'history-record':
                    data = await asyncio.to_thread(svc.archive_detail, scope, request.query_params.get('id'))
                elif request.method == 'POST' and action == 'history-edit':
                    if not can_edit_history:
                        raise RiskError('当前账号不能修改该楼栋的历史巡检。', 403)
                    data = await asyncio.to_thread(svc.edit_archive, scope, payload['id'], payload, user.get('name') or owner)
                else:
                    raise RiskError('巡检操作不存在。', 404)
                if action in {'history-record', 'history-edit'}:
                    data['draft_key'] = hashlib.sha256(f'{owner}:{scope}:{data["id"]}'.encode()).hexdigest()
                    data['can_edit'] = can_edit_history
                return JSONResponse({'ok': True, 'data': {'history': data} if action == 'history' else data},
                                    headers={'Cache-Control': 'private, no-store'})
            if not can_manage and not local_write and not viewer and (request.method != 'GET' or action not in ('', 'status', 'source', 'xlsx')):
                raise RiskError('当前账号仅可查看本楼巡检。', 403)
            from .personnel_source import daily_roster
            duty = None
            try:
                if can_manage:
                    duty = await asyncio.to_thread(resolve_duty, lambda date: daily_roster(runtime.service, date), user, scopes)
            except RiskError as error:
                if (viewer or local_write) and (request.method == 'GET' or local_write):
                    duty = None
                elif request.method != 'GET' or action or error.status != 503:
                    raise
                else:
                    recent = await asyncio.to_thread(svc.recent_history, history_scopes, owner)
                    return JSONResponse({'ok': True, 'data': {'duty': None, 'active': None, 'history': recent,
                        'history_scopes': history_scopes, 'completed_rounds': 0, 'round_limit': ROUNDS_PER_SHIFT,
                        'duty_warning': str(error)}}, headers={'Cache-Control': 'private, no-store'})
            roster_scope = duty['scope'] if duty else ''
            if selected_scope and not viewer and selected_scope not in home_scopes and selected_scope != roster_scope:
                raise RiskError('没有该楼栋的当前巡检查看权限。', 403)
            if viewer and selected_scope and selected_scope in scopes:
                duty = await asyncio.to_thread(building_duty, lambda date: daily_roster(runtime.service, date), selected_scope)
            elif viewer and not duty:
                duty = await asyncio.to_thread(building_duty, lambda date: daily_roster(runtime.service, date), scopes[0])
            elif local_write and selected_scope in home_scopes and selected_scope != roster_scope:
                duty = await asyncio.to_thread(building_duty, lambda date: daily_roster(runtime.service, date), selected_scope)
            elif not duty and local_write:
                duty = await asyncio.to_thread(building_duty, lambda date: daily_roster(runtime.service, date), home_scopes[0])
            can_edit = True
            if (viewer or home_scopes) and request.method == 'GET':
                scope = selected_scope or (duty['scope'] if duty else (scopes[0] if viewer else home_scopes[0]))
                can_edit = bool(viewer or duty and duty['scope'] == scope)
                if not duty or duty['scope'] != scope:
                    date, shift = duty_period(dt.datetime.now(CN))
                    duty = {'scope': scope, 'date': date, 'shift': shift}
            if not duty:
                if request.method == 'GET' and action in ('', 'status'):
                    recent = await asyncio.to_thread(svc.recent_history, history_scopes, owner)
                    return JSONResponse({'ok': True, 'data': {'duty': None, 'active': None, 'history': recent,
                        'history_scopes': history_scopes, 'completed_rounds': 0, 'round_limit': ROUNDS_PER_SHIFT}},
                        headers={'Cache-Control': 'private, no-store'})
                raise RiskError('没有可办理的 D 楼或 E 楼当班排班。', 403)
            scope = duty['scope']
            await asyncio.to_thread(svc.rollover_due, scope)
            if request.method == 'POST' and selected_scope and selected_scope != scope:
                raise RiskError('只能办理本人当班楼栋的巡检。', 403)
            if request.method == 'GET':
                state = await asyncio.to_thread(svc.load, scope)
                if action == 'source':
                    if not viewer and scope not in home_scopes and not can_manage:
                        raise RiskError('没有当前参考数据的查看权限。', 403)
                    batches = await asyncio.to_thread(svc.capture, [scope], True)
                    batch = batches[scope]
                    order = await asyncio.to_thread(svc.order, owner, scope)
                    professions = list(dict.fromkeys(device_profession(device) for device in batch['devices']))
                    profession_orders = await asyncio.gather(*(asyncio.to_thread(svc.order, owner, scope, name)
                                                               for name in professions))
                    return JSONResponse({'ok': True, 'data': {'scope': scope, 'batch': batch['batch'],
                        'cache_refreshed_at': batch.get('cache_refreshed_at'), 'cache_stale': batch.get('cache_stale', False),
                        'count': len(batch['rows']), 'orders': {'ALL': order, **dict(zip(professions, profession_orders))},
                        'devices': [display_device(device)
                            for device in svc.ordered(batch, order)]}}, headers={'Cache-Control': 'private, no-store'})
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
                    if (not viewer and active and request.query_params.get('id') == active['id']
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
                if action not in {'start', 'order', 'draft', 'cancel', 'finish', 'confirm-profession', 'sheet-group'}:
                    raise RiskError('巡检操作不存在。', 404)
                data = await asyncio.to_thread(svc.change, owner, user.get('name') or owner, duty, action, payload)
            data['history_scopes'] = history_scopes
            if viewer or home_scopes:
                local_views = list(dict.fromkeys(value for value in (roster_scope, *home_scopes) if value))
                data.update(view_scopes=scopes if viewer else local_views if len(local_views)>1 else [], view_scope=scope, can_edit=can_edit)
            return JSONResponse({'ok': True, 'data': data}, headers={'Cache-Control': 'private, no-store'})
        except RiskError as error:
            return JSONResponse({'ok': False, 'error': str(error)}, status_code=error.status)
        except Exception:
            logging.exception('Daily inspection request failed')
            return JSONResponse({'ok': False, 'error': '巡检操作暂未确认，请保留本次巡检并重试。'}, status_code=503)

    app.add_api_route('/api/daily-inspection', endpoint, methods=['GET'])
    app.add_api_route('/api/daily-inspection/{action}', endpoint, methods=['GET', 'POST'])

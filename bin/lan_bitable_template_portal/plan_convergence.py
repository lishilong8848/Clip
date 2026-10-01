"""Native portal service for plan-convergence review; the original matching rules are retained."""
import io
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import closing

import requests

from . import plan_convergence_auth as auth
from . import plan_convergence_rules as rules
from . import plan_convergence_maintenance as maintenance
from .plan_convergence_compare import compare_scenarios_to_details
from . import plan_convergence_points as points
from .plan_convergence_browser_login import BrowserLogin

BASE = auth.ZH_BASE + '/api/alarm/alarmBlock'


class PlanConvergenceService:
    def __init__(self, store, ongoing_provider=None):
        self.store = store
        self._ongoing_provider = ongoing_provider
        self._blocks_lock = threading.Lock()
        self.browser_login = BrowserLogin(store)
        auth.bind_store(store)

    def bootstrap(self, admin):
        credentials = auth.settings_view()
        counts = {}
        if points.CATALOG_PATH.is_file():
            with closing(points._catalog_conn()) as conn:
                counts = {key: conn.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0]
                          for key, table in [('devices', 'zh_device'), ('rules', 'zh_rules')]}
        return {'is_admin': admin, 'catalog_ready': points.CATALOG_PATH.is_file(),
                'points_ready': points.POINTS_PATH.is_file(), 'counts': counts,
                'zh_ready': bool(credentials['has_token'] and not credentials['expired']),
                'blocks': self.store.get_document('plan_convergence', 'blocks') or {'items': [], 'loaded_at': 0}}

    @staticmethod
    def remote(method, path, payload=None, credentials=None):
        headers, cookies = credentials or auth.build_auth()
        response = requests.request(method, BASE + path, headers=headers, cookies=cookies,
                                    json=payload, timeout=(5, 25))
        response.raise_for_status()
        result = response.json()
        if not isinstance(result, dict) or result.get('code') != 200 or not result.get('success'):
            raise ValueError('智航返回异常，请检查平台凭证或网络连接')
        if not isinstance(result.get('data'), dict):
            raise ValueError('智航返回数据不完整，已停止核对')
        return result['data']

    def blocks(self, refresh=False):
        if not refresh:
            return self.store.get_document('plan_convergence', 'blocks') or {'items': [], 'loaded_at': 0}
        with self._blocks_lock:
            items, seen, cursor, seen_pages = [], set(), None, set()
            credentials = auth.build_auth()
            for page in range(1, 101):
                payload = {} if page == 1 else {'page': page, 'size': 100, 'searchAfter': cursor}
                data = self.remote('POST', '/getAlarmBlock', payload, credentials)
                chunk = data.get('content')
                if not isinstance(chunk, list):
                    raise ValueError('智航分页内容不完整')
                signature = tuple(str(row.get('blockId') or row.get('id') or '') for row in chunk if isinstance(row, dict))
                if signature in seen_pages and data.get('hasNext'):
                    raise ValueError('智航重复返回同一页，已停止核对')
                seen_pages.add(signature)
                for row in chunk:
                    if not isinstance(row, dict):
                        raise ValueError('智航屏蔽记录结构无效')
                    key = str(row.get('blockId') or row.get('id') or '')
                    if key and key not in seen:
                        seen.add(key)
                        items.append(row)
                if not data.get('hasNext'):
                    result = {'items': items, 'loaded_at': time.time()}
                    self.store.put_document('plan_convergence', 'blocks', result)
                    return result
                if not chunk:
                    break
                cursor = data.get('searchAfter')
            raise ValueError('智航分页未完整返回，已停止核对')

    def block(self, block_id):
        if not str(block_id).isdigit():
            raise ValueError('屏蔽记录 ID 无效')
        data = self.remote('GET', '/getAlarmBlockDetail/' + str(block_id))
        if not isinstance(data.get('alarmBlockDetailResultList'), list):
            raise ValueError('屏蔽记录明细不完整，不能核对')
        data.setdefault('blockId', str(block_id))
        return data

    def remote_rows(self, kind, payload):
        paths = {'snapshots': '/getBlockInstanceSnapshot', 'rule-view': '/getRuleView'}
        rows, credentials, signatures = [], auth.build_auth(), set()
        for page in range(1, 501):
            data = self.remote('POST', paths[kind], {**payload, 'page': page, 'size': 100}, credentials)
            chunk = data.get('content')
            if not isinstance(chunk, list):
                raise ValueError('智航明细分页结构无效')
            signature = str(chunk)
            if signature in signatures and data.get('hasNext'):
                raise ValueError('智航重复返回同一页，已停止读取')
            signatures.add(signature)
            rows.extend(chunk)
            if not data.get('hasNext'):
                return rows
            if not chunk:
                break
        raise ValueError('智航明细分页未完整返回')

    @staticmethod
    def catalog(query):
        kind, kw = query.get('kind', ''), str(query.get('kw') or '')[:200]
        zone, building, floor, room = (str(query.get(key) or '')[:200] for key in ('zone', 'building', 'floor', 'room'))
        objs = [value for value in str(query.get('objs') or '').split(',') if value and value != 'all'] or None
        rooms = [tuple((part.split('|') + [''] * 4)[:4]) for part in str(query.get('rooms') or '').split(',') if part and part != 'all'] or None
        if len(objs or []) > 500 or len(rooms or []) > 200:
            raise ValueError('筛选条件过多，请缩小范围')
        if kind == 'types':
            rows = points.zh_objtypes_new()
        elif kind == 'zones':
            rows = points.zh_zone_list()
        elif kind == 'buildings':
            rows = points.zh_building_list(zone)
        elif kind == 'floors':
            rows = points.zh_floor_list(zone, building)
        elif kind == 'rooms':
            rows = points.zh_room_list(zone, building, floor)
        elif kind == 'devices':
            rows = points.zh_devices_by_rooms(objs, rooms, kw) if rooms else points.zh_devices_new(objs, zone, building, floor, room, kw)
        elif kind == 'rules':
            if query.get('obj'):
                rows = points.zh_rules_for_obj(str(query['obj'])[:200], kw)
            elif query.get('inst'):
                rows = points.zh_rules_for_device(str(query['inst'])[:200], kw)
            elif objs:
                placeholders = ','.join(['?'] * len(objs))
                rows = points._rules_dedup_sql('WHERE classify_model IN (' + placeholders + ')', tuple(objs), kw, 3000)
            elif rooms:
                rows = points.zh_rules_all_by_rooms(rooms, kw, limit=3000)
            else:
                rows = points.zh_rules_all(kw)
        else:
            raise ValueError('目录类型无效')
        if kind not in {'devices', 'rules'} and kw:
            rows = [row for row in rows if kw.casefold() in str(row).casefold()]
        return {'items': rows, 'has_more': kind in {'devices', 'rules'} and len(rows) >= 3000, 'limit': 3000}

    @staticmethod
    def expand(set_id):
        if not rules.get_set(set_id):
            raise ValueError('规则集不存在')
        ids = list(rules.expand_set_items(set_id))
        devices = []
        for start in range(0, len(ids), 900):
            batch = ids[start:start + 900]
            placeholders = ','.join(['%s'] * len(batch))
            devices.extend(rules._exec('SELECT ins_id,inst_name,obj_name,position FROM zh_device WHERE ins_id IN (' + placeholders + ')', tuple(batch)))
        devices.sort(key=lambda row: str(row.get('inst_name') or ''))
        return {'count': len(ids), 'devices': devices}

    def match(self, set_id, payload):
        if not rules.get_set(set_id):
            raise ValueError('规则集不存在')
        details = payload.get('details')
        if details is None:
            details = self.block(payload.get('block_id', ''))['alarmBlockDetailResultList']
        if not isinstance(details, list) or len(details) > 10000 or any(not isinstance(row, dict) for row in details):
            raise ValueError('屏蔽明细结构无效')
        return rules.match_record_to_set(set_id, details)

    @staticmethod
    def parse_excel(content, filename):
        from openpyxl import load_workbook
        if not filename.lower().endswith(('.xlsx', '.xlsm')) or not content.startswith(b'PK'):
            raise ValueError('请选择 xlsx 或 xlsm 文件')
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            if sum(item.file_size for item in archive.infolist()) > 200 * 1024 * 1024:
                raise ValueError('Excel 解压内容过大，请拆分场景文件')
        workbook = load_workbook(io.BytesIO(content), read_only=True, data_only=True)
        sheets, count = [], 0
        try:
            for sheet in workbook:
                iterator = sheet.iter_rows(values_only=True)
                headers = [str(value).strip() if value is not None else '' for value in next(iterator, ())]
                rows = []
                for number, cells in enumerate(iterator, 2):
                    count += 1
                    if count > 10000:
                        raise ValueError('场景 Excel 最多读取 10000 行，请拆分文件')
                    if not any(value is not None and str(value).strip() for value in cells):
                        continue
                    row = {key: str(cells[index]).strip() if index < len(cells) and cells[index] is not None else ''
                           for index, key in enumerate(headers) if key}
                    rows.append({**row, '_excel_row': number})
                sheets.append({'name': sheet.title, 'headers': headers, 'rows': rows})
        finally:
            workbook.close()
        return {'sheets': sheets}

    def compare(self, payload):
        scenarios = payload.get('scenarios')
        if not isinstance(scenarios, list) or len(scenarios) != 1 or not isinstance(scenarios[0], dict):
            raise ValueError('请选择一个场景进行核对')
        scenario = scenarios[0]
        if not scenario.get('scenario_name') or not isinstance(scenario.get('rows'), list) or not scenario['rows']:
            raise ValueError('场景名称或数据为空')
        data = self.block(payload.get('block_id', ''))
        result = compare_scenarios_to_details(scenarios, data['alarmBlockDetailResultList'], data['blockId'])
        return {**result, 'source_detail': data}

    def maintenance_records(self, record_id=None):
        if self._ongoing_provider is None:
            raise ValueError('本地未结束通告读取入口未接入')
        records = maintenance.ongoing_records(self._ongoing_provider('ALL'))
        if record_id is not None:
            records = [row for row in records if row['record_id'] == str(record_id)]
            if not records:
                raise FileNotFoundError('该检修通告已结束、删除或不在当前未结束列表中，请刷新列表')
        return records

    def maintenance_check(self, record_id=None):
        records = self.maintenance_records(record_id)
        blocks = [row for row in self.blocks(refresh=True)['items'] if str(row.get('status')) == '1']
        details = {}
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = {executor.submit(self.block, str(row.get('blockId') or row.get('id'))):
                       str(row.get('blockId') or row.get('id')) for row in blocks}
            for future in as_completed(futures):
                # Missing upstream detail must stop the audit, never produce a false match result.
                details[futures[future]] = future.result()
        result, orphans = maintenance.match_records(records, blocks, details)
        return {'records': result, 'orphan_block_ids': orphans,
                'stats': {'maintenance': len(records), 'blocks': len(blocks),
                          'matched_records': sum(bool(row['hits']) for row in result), 'orphan_blocks': len(orphans)}}

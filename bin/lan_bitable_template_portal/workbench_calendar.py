"""Build the workbench calendar from Lighthouse's roster, notice and weather data."""
from __future__ import annotations

import asyncio
from collections import defaultdict
import datetime as dt
import logging
import os
import re
import time

from fastapi import Request
from fastapi.responses import JSONResponse
from upload_event_module.building_normalizer import extract_building_codes

from .cabinet_guest import is_cabinet_guest
from .module_access import allowed_modules, engineer_professions, professional_record_visible, record_profession
from .personnel_source import ROSTER_NAMESPACE, TABLE_ID as PERSONNEL_TABLE, daily_roster, observed_on_day
from .pre_shift_checks import windows

TZ = dt.timezone(dt.timedelta(hours=8))
BUILDINGS = frozenset((*'ABCDEH', '110'))
LOG = logging.getLogger(__name__)


def building(value):
    value = str(value or '').strip().upper()
    aliases = {'BUILDING_' + code: code for code in 'ABCDEH'}
    aliases.update({'STATION_110': '110', '110站': '110'})
    return aliases.get(value, value.removesuffix('楼').removesuffix('栋'))


def timestamp(value):
    if isinstance(value, (float, int)) and not isinstance(value, bool):
        try:
            return dt.datetime.fromtimestamp(value / 1000 if abs(value) >= 1e11 else value, TZ)
        except (OverflowError, OSError, ValueError):
            return None
    if not value or not isinstance(value, str):
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
        return parsed.astimezone(TZ) if parsed.tzinfo is not None else parsed.replace(tzinfo=TZ)
    except ValueError:
        return None


def strings(value):
    return list(dict.fromkeys(part.strip()[:90] for part in value if isinstance(part, str) and part.strip())) if isinstance(value, list) else []


def overlap(left, right):
    return left[0] < right[1] and right[0] < left[1]


def month_dates(month):
    day = dt.date.fromisoformat(month + '-01')
    while day.isoformat().startswith(month):
        yield day
        day += dt.timedelta(days=1)


def change_window(item, day, today, *, conflict=False):
    actual_start = timestamp(item.get('actualStartDate')) if day <= today else None
    if conflict and day <= today and not actual_start:
        return None
    planned_start = timestamp(item.get('plannedDelayStartDate') or item.get('startDate'))
    planned_end = timestamp(item.get('plannedDelayEndDate') or item.get('endDate')) or planned_start
    start = actual_start or planned_start
    end = (timestamp(item.get('actualEndDate')) or planned_end or actual_start) if actual_start else planned_end
    if not start or not end:
        return None
    if end <= start:
        end = start + dt.timedelta(minutes=1)
    work_hours = not actual_start or (conflict and end - start > dt.timedelta(hours=12))
    begin = dt.datetime.combine(day, dt.time(9 if work_hours else 0), TZ)
    finish = dt.datetime.combine(day, dt.time(18), TZ) if work_hours else begin + dt.timedelta(days=1)
    clipped = (max(start, begin), min(end, finish))
    return clipped if clipped[0] < clipped[1] else None


def sealing_windows(scopes, records):
    windows = []
    for item in records or ():
        start, end = timestamp(item.get('startDate')), timestamp(item.get('endDate'))
        title = str(item.get('sealingName') or '').strip()
        buildings = item.get('buildings') or []
        visible = sorted(scopes.intersection(buildings))
        if start and end and end > start and title and (not buildings or visible):
            windows.append({'kind':'封网', 'title':title[:240], 'start':start.isoformat(), 'end':end.isoformat(), 'scopes':visible})
    return sorted(windows, key=lambda entry: entry['start'])


def public_schedule(month, scopes, month_data, seals_data, changes_data, today):
    days = defaultdict(lambda: {'items': [], 'staff': []})
    assignments = defaultdict(set)
    previous_day = (dt.date.fromisoformat(month + '-01') - dt.timedelta(days=1)).isoformat()
    for schedule in month_data or ():
        scope = building(schedule.get('building') or schedule.get('buildingText'))
        day = str(schedule.get('scheduleDate') or '')
        if scope not in scopes or not (day.startswith(month + '-') or day == previous_day) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', day):
            continue
        shift = str(schedule.get('shiftText') or schedule.get('shift') or '').strip()
        names = strings(schedule.get('dutyStaffActual'))
        if not names and isinstance(schedule.get('personText'), str):
            names = strings(re.split(r'[，,、\s]+', schedule['personText']))
        if day != previous_day:
            days[day]['staff'].append({'scope': scope, 'shift': shift, 'names': names})
        shift_code = {'白班':'white','白':'white','white':'white','夜班':'night','夜':'night','night':'night'}.get(shift, '')
        for name in names:
            if shift_code:
                assignments[(day, shift_code, name)].add(scope)

    seals = [(item, (timestamp(item['start']), timestamp(item['end']))) for item in sealing_windows(scopes, seals_data)]
    changes = []
    for item in changes_data or ():
        authorized = sorted(scopes.intersection(building(value) for value in strings(item.get('buildings'))))
        title = str(item.get('title') or '').strip()
        if title and authorized:
            risk = ' '.join(str(item.get(field) or '') for field in ('level','type','title','riskSummary'))
            elevated = bool(re.search(r'中风险|中等风险|中级|较高风险|高风险|重大|严重|高危|MEDIUM|HIGH|CRITICAL|(?:I|L|P)[12](?![A-Z0-9])', risk, re.I)
                            or set(re.split(r'[\s,，、/|;；]+', risk)).intersection({'中','中等','中危','高','高等'}))
            changes.append((item, title[:240], authorized, elevated))

    for day in month_dates(month):
        key = day.isoformat()
        midnight = dt.datetime.combine(day, dt.time(), TZ)
        day_window = (midnight, midnight + dt.timedelta(days=1))
        sealing = [(item, window) for item, window in seals if overlap(window, day_window)]
        days[key]['items'].extend(item for item, _ in sealing)
        windows = []
        for item, title, authorized, elevated in changes:
            window = change_window(item, day, today)
            if window:
                windows.append((item, title, authorized, elevated, window))
        for index, (item, title, authorized, elevated, window) in enumerate(windows):
            compare = change_window(item, day, today, conflict=True)
            relevant_seals = [(seal['title'], seal_window) for seal, seal_window in sealing
                              if not seal['scopes'] or set(seal['scopes']).intersection(authorized)]
            conflicts = [name for name, seal_window in relevant_seals if compare and overlap(compare, seal_window)]
            related = [name for other, (_, name, _, other_risk, other_window) in enumerate(windows)
                       if other != index and elevated and other_risk and overlap(window, other_window)]
            notes = [f'与封网 {name} 冲突' for name in conflicts]
            notes += [f'当日有封网 {name}' for name, _ in relevant_seals if name not in conflicts]
            notes += [f'与变更 {name} 时段重叠' for name in related]
            days[key]['items'].append({'kind': '冲突' if conflicts else '提醒' if notes else '变更',
                                      'title': title, 'scopes': authorized, 'notes': notes,
                                      'source_id': item.get('source_id', ''),
                                      'start': window[0].isoformat(), 'end': window[1].isoformat()})
        previous = (day - dt.timedelta(days=1)).isoformat()
        for (duty_day, shift, name), staff_scopes in assignments.items():
            if duty_day != key or shift != 'white':
                continue
            for night, label in ((key, '白班后接夜班'), (previous, '夜班后接白班')):
                together = assignments.get((night, 'night', name))
                if together:
                    days[key]['items'].append({'kind': '冲突', 'title': f'{name} · {label}',
                                               'scopes': sorted(staff_scopes | together)})
    return dict(days)


class CalendarReads:
    def __init__(self):
        self.cache = {}
        self.pending = {}

    def refresh(self, key, loader):
        cached = self.cache.get(key)
        if cached and cached[0] > time.monotonic():
            return
        if key not in self.pending:
            self.pending[key] = asyncio.create_task(self._read(key, loader))
        return self.pending[key]

    async def read(self, key, loader, *, timeout=4):
        task = self.refresh(key, loader)
        cached = self.cache.get(key)
        if not task:
            return cached[1], cached[2]
        if cached and cached[1] is not None:
            return cached[1], False
        try:
            result = await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
            return result, result is not None
        except TimeoutError:
            return None, False

    async def _read(self, key, loader):
        result = None
        try:
            result = await asyncio.to_thread(loader)
        except Exception:
            LOG.warning('工作台%s数据读取失败', key[0], exc_info=False)
        finally:
            self.pending.pop(key, None)
        previous = self.cache.pop(key, None)
        if len(self.cache) >= 36:
            self.cache.pop(next(iter(self.cache)))
        self.cache[key] = (time.monotonic() + (60 if result is not None else 15),
                           previous[1] if result is None and previous else result, result is not None)
        return result

    async def close(self):
        tasks = list(self.pending.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def read_roster(service, month, today, *, cached_only=False, today_only=False):
    from upload_event_module.config import config

    first = dt.date.fromisoformat(month+'-01')
    rows, covered = [], []
    dates = [day for day in (first-dt.timedelta(days=1), *month_dates(month))
             if day <= today and (not today_only or day >= today-dt.timedelta(days=1))]
    for day in reversed(dates):
        date = day.isoformat()
        snapshot = None
        if day == today and not cached_only:
            try:
                snapshot = daily_roster(service, date)
            except Exception:
                pass
        if snapshot is None:
            snapshot = service._state_store.get_document(ROSTER_NAMESPACE, date) or {}
            if snapshot.get('source') != PERSONNEL_TABLE or snapshot.get('app_id') != config.app_id or not observed_on_day(snapshot.get('observed_at'), date):
                continue
        if snapshot.get('date') != date or snapshot.get('complete') is not True:
            continue
        covered.append(date)
        groups = defaultdict(list)
        for person in snapshot.get('people') or []:
            if person.get('name') and person.get('scope') in BUILDINGS:
                shift = person.get('shift') if person.get('duty_kind') != '长白' else '长白'
                groups[(person['scope'], shift)].append({
                    key: str(person.get(key) or '') for key in ('name', 'open_id', 'group', 'position')
                })
        for (scope, shift), people in groups.items():
            rows.append({'building':scope,'scheduleDate':date,'shiftText':shift,
                         'dutyStaffActual':[person['name'] for person in people], 'people':people})
    return rows, covered


def handover_roster(scopes, roster, covered, current):
    today = current.date()
    start, end = windows(today.isoformat(), '白')
    if current < start:
        pairs = ((today-dt.timedelta(days=1), '夜'), (today, '白'))
    elif current < end:
        pairs = ((today, '白'), (today, '夜'))
    else:
        pairs = ((today, '夜'), (today+dt.timedelta(days=1), '白'))

    def shift_rows(day, shift):
        date = day.isoformat()
        _, finish = windows(date, shift)
        staff = []
        labels = {shift, shift+'班'}
        if shift == '白':
            labels.add('长白')
        for row in roster:
            scope = building(row.get('building') or row.get('buildingText'))
            label = str(row.get('shiftText') or row.get('shift') or '')
            if scope not in scopes or row.get('scheduleDate') != date or label not in labels:
                continue
            people = row.get('people') or [{'name': name} for name in strings(row.get('dutyStaffActual'))]
            if label == '长白':
                people = [{**person, 'duty_kind': '长白'} for person in people]
            staff.append({'scope':scope, 'people':people})
        return {'date':date, 'shift':shift, 'end':finish.isoformat(), 'verified':date in covered, 'staff':staff}

    return {name: shift_rows(day, shift) for name, (day, shift) in zip(('current', 'incoming'), pairs)}


def read_seals(service, app_token, table_id):
    if not (re.fullmatch(r'[A-Za-z0-9]+', app_token) and re.fullmatch(r'tbl[A-Za-z0-9]+', table_id)):
        return None
    params, seen, result = {'page_size':500}, set(), []
    for _ in range(40):
        response = service._request_json('records', params=params, app_token=app_token, table_id=table_id)
        data = response.get('data') or {}
        records = data.get('items')
        if response.get('code') != 0 or not isinstance(records, list) or type(data.get('has_more')) is not bool:
            raise ValueError('封网记录读取不完整')
        for record in records:
            fields = record.get('fields') or {}
            title = fields.get('封网名称') or fields.get('封网信息') or fields.get('名称')
            start = fields.get('开始日期') or fields.get('开始时间') or fields.get('开始')
            end = fields.get('结束日期') or fields.get('结束时间') or fields.get('结束')
            area = fields.get('适用楼栋') or fields.get('楼栋') or fields.get('实施区域')
            result.append({'sealingName':service._repair_management_plain_text(title),'startDate':start,'endDate':end,
                           'buildings':extract_building_codes(service._repair_management_plain_text(area))})
        if not data.get('has_more'):
            return result
        token = data.get('page_token')
        if not token or token in seen:
            break
        seen.add(token)
        params = {**params,'page_token':token}
    raise ValueError('封网记录分页未完成')


def read_changes(service):
    store = service._state_store
    snapshot = store.get_source_scope_snapshot('ALL')
    meta = snapshot.get('meta') or {}
    if meta.get('snapshot_mode') == 'per_source' and 'change' not in (snapshot.get('source_versions') or meta.get('source_versions') or {}):
        snapshot = store.get_source_scope_snapshot('ALL', prefer_independent=False)
    if not snapshot.get('exists'):
        return None
    rows = []
    for record in snapshot.get('records') or []:
        if service._record_work_type(record) != 'change':
            continue
        fields = record.get('display_fields') or {}
        if service._change_progress_value(record) in {'取消','已取消','退回','已退回','作废'}:
            continue
        start,end,_ = service._change_time_range(record)
        rows.append({'title':service._change_title(record),'buildings':service._change_record_building_codes(record),
                     'specialty':record_profession(record),
                     'source_id':record.get('record_id'),'startDate':start,'endDate':end,
                     'order_number':service._repair_management_plain_text(fields.get('智航工单编号') or fields.get('工单号')),
                     'plannedDelayStartDate':fields.get('计划延迟开始日期'),'plannedDelayEndDate':fields.get('计划延迟结束日期'),
                     'actualStartDate':fields.get('实际开始时间'),'actualEndDate':fields.get('实际结束时间'),
                     'level':service._repair_management_plain_text(fields.get('变更等级') or fields.get('变更级别'))})
    return rows


ZHIHANG_CALENDAR = {
    'change': ('change', '变更', '标题', ('实际开始时间', '计划延时开始时间', '计划开始时间'), '状态'),
    'risk': ('risk_inspection', '风险排查', '检查项目', ('排查时间', '审核时间'), '排查状态'),
    'drill': ('drills', '演练', '演练场景', ('实际演练时间', '计划演练时间'), '执行状态'),
    'event': ('event', '事件', '事件标题', ('发生时间', '创建时间'), '处理状态'),
    'inspection': ('daily', '巡检', '工单名称', (), '工单状态'),
}


def read_zhihang_schedule(service, month, scopes, modules, native_numbers, *, professions=None):
    from .zhihang_data_sources import _BUILDINGS, _date, _plan_range
    from .zhihang_data_sync import NS

    kinds = {kind for kind, (grant, *_columns) in ZHIHANG_CALENDAR.items() if grant in modules}
    if not kinds:
        return {}
    store = service._state_store
    runs = [doc['payload'] for doc in store.list_documents(NS, key_prefix='run:')
            if doc['payload'].get('status') == 'success' and doc['payload'].get('kind') in kinds]
    days = defaultdict(list)
    for run in sorted(runs, key=lambda run: (run.get('reference_at') or run.get('created_at', ''), run.get('finished_at', '')), reverse=True):
        kind = run['kind']
        if kind not in kinds:
            continue
        _, label, title_field, date_fields, status_field = ZHIHANG_CALENDAR[kind]
        rows = store.get_document(NS, 'rows:' + run['id']) or {}
        found_month = False
        for fields in rows.values():
            if not isinstance(fields, dict):
                continue
            if kind == 'inspection':
                stamp = next(iter(_plan_range(fields.get('计划区间'))), None)
            else:
                stamp = next((value for field in date_fields if (value := _date(fields.get(field))) is not None), None)
            if not stamp or stamp.strftime('%Y-%m') != month:
                continue
            found_month = True
            if not professional_record_visible(professions, fields):
                continue
            if kind == 'change' and fields.get('工单号') in native_numbers:
                continue
            place = fields.get('楼栋') or fields.get('实施区域') or fields.get('数据中心')
            codes = extract_building_codes(place or (fields.get(title_field) if kind == 'change' else ''))
            if kind == 'event' and not codes:
                for raw in (fields.get('数据中心编码'), fields.get('专业域编码')):
                    if match := re.match(r'^([5-9])(?:\.|$)', str(raw or '')):
                        codes = [building(_BUILDINGS[match[1]])]
                        break
            authorized = sorted(scopes.intersection(codes))
            title = str(fields.get(title_field) or '').strip()
            if title and authorized:
                status = str(fields.get(status_field) or '').strip()
                days[stamp.date().isoformat()].append({'kind':label,'title':title[:240],'scopes':authorized,
                    'start':stamp.isoformat(),'source':'智航','notes':[status[:60]] if status else []})
        if found_month or kind == 'change':
            kinds.remove(kind)
        if not kinds:
            break
    return dict(days)


def install_workbench_calendar(app, controller, runtime):
    reads = CalendarReads()
    app.add_event_handler('shutdown', reads.close)

    @app.get('/api/personal-calendar')
    async def calendar(request: Request):
        session = await asyncio.to_thread(controller._current_session, request)
        if not session:
            return controller._auth_required_response()
        auth = runtime.auth_manager
        open_id = str((session.get('user') or {}).get('open_id') or '')
        if not open_id or is_cabinet_guest(session):
            return JSONResponse({'ok': False, 'error': '需要工作台权限。'}, status_code=403)
        month = request.query_params.get('month', '')
        scope = request.query_params.get('scope', 'ALL')
        if not re.fullmatch(r'20\d{2}-(?:0[1-9]|1[0-2])', month) or scope not in BUILDINGS | {'ALL','CAMPUS'}:
            return JSONResponse({'ok': False, 'error': '日历的月份或楼栋不正确。'}, status_code=400)
        granted_scopes, profile, modules = await asyncio.gather(
            asyncio.to_thread(auth.session_scopes, session),
            asyncio.to_thread(auth.account_profile, open_id),
            asyncio.to_thread(allowed_modules, auth, session))
        scopes = set(BUILDINGS if 'ALL' in granted_scopes else BUILDINGS.intersection(granted_scopes))
        level = profile.get('level')
        professions = engineer_professions(profile)
        is_h_admin = profile.get('category') == 'duty' and profile.get('home_scope') == 'H' and await asyncio.to_thread(auth.is_admin, session)
        if not is_h_admin and (level in {'L3','L4'} or profile.get('category') == 'duty'):
            home = profile.get('home_scope')
            scopes &= {home} if home else scopes if len(scopes) == 1 else set()
        if scope in BUILDINGS:
            scopes &= {scope}
        if not scopes:
            return JSONResponse({'ok': False, 'error': '当前楼栋没有日历权限。'}, status_code=403)
        service = runtime.service
        include_shared = level != 'L4' or profile.get('category') == 'duty'
        if level == 'L2' and scopes != {profile.get('home_scope')}:
            include_shared = False
        include_changes = 'change' in modules and include_shared
        sealing_app = os.getenv('CLIPFLOW_CALENDAR_SEALING_APP_TOKEN', '').strip()
        sealing_table = os.getenv('CLIPFLOW_CALENDAR_SEALING_TABLE_ID', '').strip()
        seal_configured = bool(re.fullmatch(r'[A-Za-z0-9]+', sealing_app) and re.fullmatch(r'tbl[A-Za-z0-9]+', sealing_table))
        from upload_event_module.config import config
        current = dt.datetime.now(TZ)
        today = current.date()
        if request.query_params.get('view') == 'roster':
            try:
                roster, covered = await asyncio.to_thread(read_roster, service, month, today, cached_only=True, today_only=True)
            except Exception:
                roster, covered = [], []
            key = ('roster', today, month, config.app_id)
            loader = lambda: read_roster(service, month, today, today_only=True)
            if month == today.strftime('%Y-%m'):
                if today.isoformat() in covered:
                    reads.refresh(key, loader)
                else:
                    stored, _ = await reads.read(key, loader, timeout=8)
                    if stored and today.isoformat() in stored[1]:
                        roster, covered = stored
            return JSONResponse({'ok':True,'data':{'month':month,'days':public_schedule(month, scopes, roster, [], [], today),
                'handover':handover_roster(scopes, roster, covered, current),
                'unavailable':[] if today.isoformat() in covered else ['排班'],'roster_dates':covered,
                'roster_pending':key in reads.pending and today.isoformat() not in covered}},
                headers={'Cache-Control':'private, no-store'})
        staff_data, seal_result, change_result = await asyncio.gather(
            asyncio.to_thread(read_roster, service, month, today, cached_only=True),
            reads.read(('seals', sealing_app, sealing_table), lambda: read_seals(service, sealing_app, sealing_table))
            if seal_configured else asyncio.sleep(0, result=(None, False)),
            reads.read(('changes', config.app_id), lambda: read_changes(service))
            if include_changes else asyncio.sleep(0, result=([], True)),
            return_exceptions=True)
        changes = change_result[0] if isinstance(change_result, tuple) else None
        if changes is not None and professions is not None:
            changes = [row for row in changes if professional_record_visible(professions, row)]
        roster, covered = staff_data if isinstance(staff_data, tuple) else ([], [])
        seals, seal_fresh = seal_result if isinstance(seal_result, tuple) else (None, False)
        seal_status = ('ready' if seal_fresh else 'stale') if seals is not None else 'failed' if seal_configured else 'unconfigured'
        missing = [label for label, value in [('封网', seals), ('计划变更', changes)] if value is None]
        days = public_schedule(month, scopes, roster, seals, changes, today)
        if include_shared:
            try:
                native_ids = {item['source_id'] for day in days.values() for item in day['items'] if item.get('source_id')}
                native_numbers = {row['order_number'] for row in (changes or [])
                                  if row.get('source_id') in native_ids and row.get('order_number')}
                key = ('zhihang', config.app_id, month, frozenset(scopes), frozenset(modules),
                       None if professions is None else frozenset(professions), frozenset(native_numbers))
                zhihang, _ = await reads.read(key, lambda: read_zhihang_schedule(
                    service, month, scopes, modules, native_numbers,
                    **({'professions': professions} if professions is not None else {})))
                if zhihang is None:
                    missing.append('智航日程')
                else:
                    for day, items in zhihang.items():
                        days.setdefault(day, {'items':[], 'staff':[]})['items'].extend(items)
            except Exception:
                missing.append('智航日程')
        return JSONResponse({'ok': True, 'data': {'month': month, 'days': days,
                                                'unavailable': missing, 'seal_status':seal_status,
                                                'seal_windows':sealing_windows(scopes, seals), 'roster_dates':covered}},
                            headers={'Cache-Control': 'private, no-store'})

    @app.get('/api/personal-calendar/weather')
    async def weather(request: Request):
        session = await asyncio.to_thread(controller._current_session, request)
        if not session:
            return controller._auth_required_response()
        if not (session.get('user') or {}).get('open_id') or is_cabinet_guest(session):
            return JSONResponse({'ok': False, 'error': '需要工作台权限。'}, status_code=403)
        try:
            snapshot_state = await asyncio.to_thread(runtime.service._state_store.get_critical_guard_weather_state)
            guard_read_failed = not isinstance(snapshot_state, dict)
        except Exception:
            snapshot_state = None
            guard_read_failed = True
        if guard_read_failed:
            snapshot_state = {}
        snapshot = snapshot_state.get('snapshot') or {}
        snapshot = snapshot if isinstance(snapshot, dict) else {}
        # 重保管理保留上次权威识别结果，即使接下来的天气查询失败或降级。
        current_warnings = snapshot_state.get('current_warnings')
        warning_ready = isinstance(current_warnings, list)
        alerts = [{key: row.get(key) for key in ('id','title','guard_level')}
                  for row in (current_warnings if warning_ready else []) if isinstance(row, dict)]
        guard_level = str(snapshot_state.get('current_guard_level') or '') if warning_ready else ''
        authoritative = snapshot.get('authoritative_for_state') is True
        guard_notes = []
        if guard_read_failed:
            guard_notes.append('暂时无法读取重保管理状态')
        if snapshot_state.get('paused'):
            guard_notes.append('重保自动查询已暂停')
        if warning_ready and (snapshot_state.get('last_error') or (snapshot and not authoritative)):
            guard_notes.append('显示上次识别结果')
        return JSONResponse({'ok': True, 'data': {
            'alerts': alerts, 'warning_ready': warning_ready, 'guard_level': guard_level,
            'guard_note': '，'.join(guard_notes), 'guard_read_failed': guard_read_failed,
            'guard_actions': strings(snapshot.get('actions')) if authoritative and (alerts or guard_level) else []}},
            headers={'Cache-Control': 'private, no-store'})


"""Build the workbench calendar from Lighthouse's roster, notice and weather data."""
from __future__ import annotations

import asyncio
from collections import defaultdict
import datetime as dt
import os
import re
import time

from fastapi import Request
from fastapi.responses import JSONResponse

from .cabinet_guest import is_cabinet_guest
from .module_access import allowed_modules
from .personnel_source import daily_roster

TZ = dt.timezone(dt.timedelta(hours=8))
BUILDINGS = frozenset((*'ABCDEH', '110'))


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

    seals = []
    for item in seals_data or ():
        start, end = timestamp(item.get('startDate')), timestamp(item.get('endDate'))
        title = str(item.get('sealingName') or '').strip()
        if start and end and end > start and title:
            seals.append((title[:240], (start, end)))
    seals.sort(key=lambda entry: entry[1][0])
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
        sealing = [(title, window) for title, window in seals if overlap(window, day_window)]
        for title, window in sealing:
            days[key]['items'].append({'kind': '封网', 'title': title, 'start': window[0].isoformat(), 'end': window[1].isoformat()})
        windows = []
        for item, title, authorized, elevated in changes:
            window = change_window(item, day, today)
            if window:
                windows.append((item, title, authorized, elevated, window))
        for index, (item, title, authorized, elevated, window) in enumerate(windows):
            compare = change_window(item, day, today, conflict=True)
            conflicts = [name for name, seal_window in sealing[:1] if compare and overlap(compare, seal_window)]
            related = [name for other, (_, name, _, other_risk, other_window) in enumerate(windows)
                       if other != index and elevated and other_risk and overlap(window, other_window)]
            notes = [f'与封网 {name} 冲突' for name in conflicts]
            notes += [f'当日有封网 {name}' for name, _ in sealing if name not in conflicts]
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

    async def read(self, key, loader):
        cached = self.cache.get(key)
        if cached and cached[0] > time.monotonic():
            return cached[1]
        if key not in self.pending:
            self.pending[key] = asyncio.create_task(self._read(key, loader))
        return await asyncio.shield(self.pending[key])

    async def _read(self, key, loader):
        result = None
        try:
            result = await asyncio.to_thread(loader)
        except Exception:
            pass
        finally:
            self.pending.pop(key, None)
        if len(self.cache) >= 36:
            self.cache.pop(next(iter(self.cache)))
        self.cache[key] = (time.monotonic() + (60 if result is not None else 15), result)
        return result

    async def close(self):
        tasks = list(self.pending.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def read_roster(service, month, today):
    first = dt.date.fromisoformat(month+'-01')
    rows, covered = [], []
    for day in (first-dt.timedelta(days=1), *month_dates(month)):
        if day > today:
            break
        date = day.isoformat()
        try:
            snapshot = daily_roster(service, date)
        except Exception:
            continue
        if snapshot.get('date') != date or snapshot.get('complete') is not True:
            continue
        covered.append(date)
        groups = defaultdict(list)
        for person in snapshot.get('people') or []:
            if person.get('name') and person.get('scope') in BUILDINGS:
                shift = person.get('shift') if person.get('duty_kind') != '长白' else '长白'
                groups[(person['scope'], shift)].append(person['name'])
        for (scope, shift), names in groups.items():
            rows.append({'building':scope,'scheduleDate':date,'shiftText':shift,'dutyStaffActual':names})
    return rows, covered


def read_seals(service, app_token, table_id):
    if not (re.fullmatch(r'[A-Za-z0-9]+', app_token) and re.fullmatch(r'tbl[A-Za-z0-9]+', table_id)):
        return None
    params, seen, result = {'page_size':500}, set(), []
    for _ in range(40):
        response = service._request_json('records', params=params, app_token=app_token, table_id=table_id)
        data = response.get('data') or {}
        records = data.get('items')
        if not isinstance(records, list):
            raise ValueError('封网记录读取不完整')
        for record in records:
            fields = record.get('fields') or {}
            title = fields.get('封网名称') or fields.get('封网信息') or fields.get('名称')
            start = fields.get('开始日期') or fields.get('开始时间') or fields.get('开始')
            end = fields.get('结束日期') or fields.get('结束时间') or fields.get('结束')
            result.append({'sealingName':service._repair_management_plain_text(title),'startDate':start,'endDate':end})
        if not data.get('has_more'):
            return result
        token = data.get('page_token')
        if not token or token in seen:
            break
        seen.add(token)
        params = {**params,'page_token':token}
    raise ValueError('封网记录分页未完成')


def read_changes(service):
    snapshot = service._state_store.get_source_scope_snapshot('ALL')
    if not snapshot.get('exists'):
        return None
    meta = snapshot.get('meta') or {}
    if meta.get('snapshot_mode') == 'per_source' and 'change' not in (meta.get('source_versions') or {}):
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
                     'source_id':record.get('record_id'),'startDate':start,'endDate':end,
                     'plannedDelayStartDate':fields.get('计划延迟开始日期'),'plannedDelayEndDate':fields.get('计划延迟结束日期'),
                     'actualStartDate':fields.get('实际开始时间'),'actualEndDate':fields.get('实际结束时间'),
                     'level':service._repair_management_plain_text(fields.get('变更等级') or fields.get('变更级别'))})
    return rows


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
        is_h_admin = profile.get('category') == 'duty' and profile.get('home_scope') == 'H' and await asyncio.to_thread(auth.is_admin, session)
        if not is_h_admin and (level in {'L2','L3','L4'} or profile.get('category') == 'duty'):
            home = profile.get('home_scope')
            scopes &= {home} if home else scopes if len(scopes) == 1 else set()
        if scope in BUILDINGS:
            scopes &= {scope}
        if not scopes:
            return JSONResponse({'ok': False, 'error': '当前楼栋没有日历权限。'}, status_code=403)
        service = runtime.service
        include_changes = 'change' in modules and (level != 'L4' or profile.get('category') == 'duty')
        sealing_app = os.getenv('CLIPFLOW_CALENDAR_SEALING_APP_TOKEN', '').strip()
        sealing_table = os.getenv('CLIPFLOW_CALENDAR_SEALING_TABLE_ID', '').strip()
        today = dt.datetime.now(TZ).date()
        staff_data, seals, changes = await asyncio.gather(
            reads.read(('roster', month, today), lambda: read_roster(service, month, today)),
            reads.read(('seals', sealing_app, sealing_table), lambda: read_seals(service, sealing_app, sealing_table)),
            asyncio.to_thread(read_changes, service) if include_changes else asyncio.sleep(0, result=[]),
            return_exceptions=True)
        if isinstance(changes, Exception):
            changes = None
        roster, covered = staff_data or ([], [])
        missing = [label for label, value in [('排班', staff_data), ('封网', seals), ('计划变更', changes)] if value is None]
        return JSONResponse({'ok': True, 'data': {'month': month, 'days': public_schedule(month, scopes, roster, seals, changes, today),
                                                'unavailable': missing, 'roster_dates':covered}},
                            headers={'Cache-Control': 'private, no-store'})

    @app.get('/api/personal-calendar/weather')
    async def weather(request: Request):
        session = await asyncio.to_thread(controller._current_session, request)
        if not session:
            return controller._auth_required_response()
        if is_cabinet_guest(session):
            return JSONResponse({'ok': False, 'error': '需要工作台权限。'}, status_code=403)
        weather_service = getattr(app.state, 'lighthouse_icon_weather', None)
        weather_result, snapshot_state = await asyncio.gather(
            weather_service.get('南通', detail=True) if weather_service else asyncio.sleep(0),
            asyncio.to_thread(runtime.service._state_store.get_critical_guard_weather_state),
            return_exceptions=True)
        hourly_available = isinstance(weather_result, dict) and weather_result.get('available') is True
        hourly = weather_result.get('hourly') if hourly_available else None
        guard_read_failed = isinstance(snapshot_state, Exception)
        snapshot_state = snapshot_state if isinstance(snapshot_state, dict) else {}
        now = dt.datetime.now(TZ)
        hour_start = now.replace(minute=0, second=0, microsecond=0)
        hours = []
        for row in hourly if isinstance(hourly, list) else ():
            if not isinstance(row, dict):
                continue
            at = timestamp(row.get('time'))
            if at and hour_start <= at < hour_start + dt.timedelta(hours=12):
                hours.append({**row, 'time': at.isoformat()})
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
            'hours': sorted(hours, key=lambda row: row['time']), 'hourly_ready': bool(hours),
            'hourly_note': '' if hours else ('暂无逐时气温预报' if hourly_available else '暂时无法读取气温预报'),
            'alerts': alerts, 'warning_ready': warning_ready, 'guard_level': guard_level,
            'guard_note': '，'.join(guard_notes),
            'guard_actions': strings(snapshot.get('actions')) if authoritative and (alerts or guard_level) else []}},
            headers={'Cache-Control': 'private, no-store'})


"""Workbench calendar projection tests using isolated Lighthouse data."""
import asyncio
import datetime as dt
import os
from pathlib import Path
import sys
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.workbench_calendar import (
    CalendarReads, handover_roster, install_workbench_calendar, public_schedule, read_changes, read_roster, read_zhihang_schedule, TZ,
)


class WorkbenchCalendarTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.staff = [
            {'building': 'building_a', 'scheduleDate': '2026-11-18', 'shiftText': '白', 'dutyStaffActual': ['甲']},
            {'building': 'building_b', 'scheduleDate': '2026-11-18', 'shiftText': '白', 'dutyStaffActual': ['乙']},
        ]
        self.seals = [{'sealingName': '封网窗口', 'startDate': '2026-11-18T09:00:00+08:00',
                       'endDate': '2026-11-18T12:00:00+08:00'}]
        self.changes = [{'title': 'A楼窗口', 'buildings': ['A楼'], 'startDate': '2026-11-18T10:00:00+08:00',
                         'endDate': '2026-11-18T14:00:00+08:00'},
                        {'title': 'B楼窗口', 'buildings': ['B楼'], 'startDate': '2026-11-18T10:00:00+08:00',
                         'endDate': '2026-11-18T14:00:00+08:00'}]

    async def test_calendar_keeps_verified_data_while_remote_refresh_is_slow(self):
        reads = CalendarReads()
        key = ('seals', 'app-1', 'table-1')
        reads.cache[key] = (0, self.seals, True)
        ready = threading.Event()

        def refresh():
            ready.wait(3)
            return []

        try:
            self.assertEqual(await asyncio.wait_for(reads.read(key, refresh), timeout=1), (self.seals, False))
            task = reads.pending[key]
            ready.set()
            self.assertEqual(await task, [])
            self.assertEqual(await reads.read(key, refresh), ([], True))
        finally:
            ready.set()
            await reads.close()

    async def test_cached_seal_stays_visible_and_is_marked_stale_when_refresh_fails(self):
        reads = CalendarReads()
        key = ('seals', 'app-1', 'table-1')
        reads.cache[key] = (0, self.seals, True)
        for index in range(35):
            reads.cache[('other', index)] = (0, [], True)

        def unavailable():
            raise RuntimeError('isolated outage')

        self.assertEqual(await reads.read(key, unavailable), (self.seals, False))
        await reads.pending[key]
        self.assertEqual(await reads.read(key, unavailable), (self.seals, False))
        self.assertFalse(reads.pending)

    def test_calendar_distinguishes_future_conflict_and_unstarted_reminder(self):
        def projected(today):
            return public_schedule('2026-11', {'A'}, self.staff, self.seals, self.changes, today)['2026-11-18']
        future = projected(dt.date(2026, 11, 17))
        self.assertEqual(future['staff'], [{'scope': 'A', 'shift': '白', 'names': ['甲']}])
        self.assertEqual([(item['kind'], item['title']) for item in future['items']],
                         [('封网', '封网窗口'), ('冲突', 'A楼窗口')])
        self.assertEqual(projected(dt.date(2026, 11, 18))['items'][-1]['kind'], '提醒')

    def test_roster_fallback_reuses_only_verified_same_day_records(self):
        from lan_bitable_template_portal.personnel_source import TABLE_ID
        from upload_event_module.config import config

        now = dt.datetime.now(TZ)
        day = now.date().isoformat()
        snapshot = {'date': day, 'app_id': config.app_id, 'source': TABLE_ID, 'complete': True,
                    'observed_at': now.timestamp(), 'people': [{'name': '甲', 'scope': 'A', 'shift': '白'}]}
        service = SimpleNamespace(_state_store=SimpleNamespace(get_document=lambda namespace, date: snapshot))
        with patch('lan_bitable_template_portal.workbench_calendar.daily_roster', side_effect=OSError):
            rows, covered = read_roster(service, day[:7], now.date())
            self.assertEqual(covered, [day])
            self.assertEqual(rows[0]['dutyStaffActual'], ['甲'])
            snapshot['observed_at'] -= 86400
            self.assertEqual(read_roster(service, day[:7], now.date()), ([], []))

    def test_change_calendar_uses_saved_combined_snapshot_until_independent_table_is_ready(self):
        new = {'exists': True, 'meta': {'snapshot_mode': 'per_source'}, 'source_versions': {'maintenance': 1}}
        old = {'exists': True, 'records': [{'record_id': 'rec-change', 'display_fields': {}}]}
        lookup = lambda _, prefer_independent=True: new if prefer_independent else old
        service = SimpleNamespace(_state_store=SimpleNamespace(get_source_scope_snapshot=lookup),
                                  _record_work_type=lambda _: 'change', _change_progress_value=lambda _: '',
                                  _change_time_range=lambda _: ('2026-11-18 09:00', '2026-11-18 10:00', None),
                                  _change_title=lambda _: '测试变更', _change_record_building_codes=lambda _: ['D'],
                                  _repair_management_plain_text=lambda value: value or '')
        self.assertEqual([row['source_id'] for row in read_changes(service)], ['rec-change'])
        new.update(source_versions={'change': 1}, records=[])
        self.assertEqual(read_changes(service), [])

    def test_handover_uses_previous_night_and_never_reuses_day_staff_for_tomorrow(self):
        staff = [
            {'building':'A','scheduleDate':'2026-11-17','shiftText':'夜','dutyStaffActual':['昨夜班']},
            {'building':'A','scheduleDate':'2026-11-18','shiftText':'白','dutyStaffActual':['本日白班']},
            {'building':'A','scheduleDate':'2026-11-18','shiftText':'夜','dutyStaffActual':['本日夜班']},
            {'building':'B','scheduleDate':'2026-11-18','shiftText':'白','dutyStaffActual':['其他楼栋']},
        ]
        covered = ['2026-11-17','2026-11-18']
        for hour, names in [(7, ('昨夜班','本日白班')), (17, ('本日白班','本日夜班')), (18, ('本日夜班',None))]:
            result = handover_roster({'A'}, staff, covered, dt.datetime(2026, 11, 18, hour, 55, tzinfo=TZ))
            self.assertEqual(result['current']['staff'][0]['people'][0]['name'], names[0])
            if names[1]:
                self.assertEqual(result['incoming']['staff'], [{'scope':'A','people':[{'name':names[1]}]}])
            else:
                self.assertEqual(result['incoming']['date'], '2026-11-19')
                self.assertFalse(result['incoming']['verified'])
                self.assertFalse(result['incoming']['staff'])

    def test_handover_marks_long_day_staff_only_on_the_day_shift(self):
        person = {'name':'实习验收', 'position':'运维实习生', 'group':'A班'}
        staff = [*self.staff, {'building':'A','scheduleDate':'2026-11-18','shiftText':'长白','people':[person]}]
        result = handover_roster({'A'}, staff, ['2026-11-18'], dt.datetime(2026, 11, 18, 11, tzinfo=TZ))
        self.assertEqual(result['current']['staff'][1]['people'], [{**person, 'duty_kind':'长白'}])
        self.assertEqual(result['incoming']['staff'], [])
        self.assertNotIn('duty_kind', person)

    def test_impersonated_roster_reads_names_without_chat_identifiers(self):
        person = {'name':'隔离值班员', 'position':'运维值班员', 'open_id':'ou_fixture', 'phone':'fixture'}
        staff = [{'building':'A', 'scheduleDate':'2026-11-18', 'shiftText':'白', 'people':[person]}]
        current = dt.datetime(2026, 11, 18, 11, tzinfo=TZ)
        args = ({'A'}, staff, ['2026-11-18'], current)
        self.assertEqual(handover_roster(*args)['current']['staff'][0]['people'], [person])
        self.assertEqual(handover_roster(*args, include_chat_ids=False)['current']['staff'][0]['people'],
                         [{'name':'隔离值班员', 'position':'运维值班员'}])

    async def test_seal_scope_and_missing_source_state(self):
        auth = SimpleNamespace(session_scopes=lambda _: ['A','B'], account_profile=lambda _: {'level':'L1'})
        controller = SimpleNamespace(_current_session=lambda _: {'user':{'open_id':'isolated'}})
        app = FastAPI()
        install_workbench_calendar(app, controller, SimpleNamespace(auth_manager=auth, service=object()))
        keys = {'CLIPFLOW_CALENDAR_SEALING_APP_TOKEN':'', 'CLIPFLOW_CALENDAR_SEALING_TABLE_ID':''}
        scoped = [dict(self.seals[0], buildings=['B'])]
        with patch('lan_bitable_template_portal.workbench_calendar.allowed_modules', return_value=set()), \
             patch('lan_bitable_template_portal.workbench_calendar.read_roster', return_value=([],[])), \
             patch('lan_bitable_template_portal.workbench_calendar.read_seals', side_effect=[OSError(), scoped]) as read_seals, \
             patch.dict(os.environ, keys):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
                endpoint='/api/personal-calendar?month=2026-11'
                unconfigured=(await client.get(endpoint+'&scope=A')).json()['data']
                self.assertEqual(unconfigured['seal_status'],'unconfigured')
                read_seals.assert_not_called()
                keys['CLIPFLOW_CALENDAR_SEALING_APP_TOKEN']='synthetic'
                os.environ.update(keys, CLIPFLOW_CALENDAR_SEALING_TABLE_ID='tblUnavailable')
                failure=(await client.get(endpoint+'&scope=A')).json()['data']
                self.assertEqual(failure['seal_status'],'failed')
                os.environ['CLIPFLOW_CALENDAR_SEALING_TABLE_ID']='tblReady'
                a=(await client.get(endpoint+'&scope=A')).json()['data']
                b=(await client.get(endpoint+'&scope=B')).json()['data']
                self.assertEqual(a['seal_status'],'ready')
                self.assertEqual(a['seal_windows'],[])
                self.assertEqual(b['seal_windows'][0]['scopes'],['B'])
        await app.router.shutdown()

    async def test_calendar_projects_only_current_user_scopes(self):
        auth = SimpleNamespace(session_scopes=lambda session: ['A', 'B'],
                               account_profile=lambda _: {'level':'L3', 'home_scope':'A', 'modules':['change']},
                               is_admin=lambda session: False)
        controller = SimpleNamespace(_current_session=lambda request: {'user': {'open_id': 'isolated'}})
        app = FastAPI()
        install_workbench_calendar(app, controller, SimpleNamespace(auth_manager=auth, service=object()))
        with patch('lan_bitable_template_portal.workbench_calendar.read_roster', return_value=(self.staff,['2026-11-18'])), \
             patch('lan_bitable_template_portal.workbench_calendar.read_seals', return_value=self.seals), \
             patch('lan_bitable_template_portal.workbench_calendar.read_changes', return_value=self.changes), \
             patch('lan_bitable_template_portal.workbench_calendar.read_zhihang_schedule', return_value={}), \
             patch('lan_bitable_template_portal.workbench_calendar.allowed_modules', return_value={'change'}), \
             patch.dict(os.environ, {'CLIPFLOW_CALENDAR_SEALING_APP_TOKEN':'fixture', 'CLIPFLOW_CALENDAR_SEALING_TABLE_ID':'tblSource'}):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
                aggregate = await client.get('/api/personal-calendar?month=2026-11')
                wrong = await client.get('/api/personal-calendar?month=2026-11&scope=B')
        await app.router.shutdown()
        self.assertEqual(wrong.status_code, 403)
        self.assertEqual(aggregate.status_code, 200)
        body = aggregate.json()['data']['days']['2026-11-18']
        self.assertEqual([row['scope'] for row in body['staff']], ['A'])
        self.assertEqual([row['title'] for row in body['items']], ['封网窗口','A楼窗口'])

    async def test_roster_endpoint_uses_verified_today_without_reading_agenda(self):
        today = dt.datetime.now(TZ).date().isoformat()
        auth = SimpleNamespace(session_scopes=lambda _: ['A'], account_profile=lambda _: {'level':'L3','home_scope':'A'})
        app = FastAPI()
        controller = SimpleNamespace(_current_session=lambda _: {'user':{'open_id':'isolated'}})
        install_workbench_calendar(app, controller, SimpleNamespace(auth_manager=auth, service=object()))
        staff = [{**row,'scheduleDate':today} for row in self.staff]
        with patch('lan_bitable_template_portal.workbench_calendar.read_roster', return_value=(staff,[today])), \
             patch('lan_bitable_template_portal.workbench_calendar.read_seals') as seals, \
             patch('lan_bitable_template_portal.workbench_calendar.read_changes') as changes, \
             patch('lan_bitable_template_portal.workbench_calendar.allowed_modules', return_value={'change'}):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
                response = await client.get(f'/api/personal-calendar?month={today[:7]}&scope=A&view=roster')
            self.assertEqual(response.status_code, 200)
            body = response.json()['data']
            self.assertEqual(body['days'][today]['staff'], [{'scope':'A','shift':'白','names':['甲']}])
            self.assertEqual(body['unavailable'], [])
            seals.assert_not_called()
            changes.assert_not_called()
        await app.router.shutdown()

    def test_zhihang_uses_successful_local_rows_with_grants_and_exact_buildings(self):
        documents = {
            'run:success':{'id':'success','kind':'risk','status':'success','created_at':'2026-11-19'},
            'run:failed':{'id':'failed','kind':'risk','status':'failed','created_at':'2026-11-20'},
            'run:change':{'id':'change','kind':'change','status':'success','created_at':'2026-11-20'},
            'run:event':{'id':'event','kind':'event','status':'success','created_at':'2026-11-20'},
            'rows:success':{code:{'楼栋':code+'楼','排查时间':'2026-11-18 14:00:00','检查项目':code+'楼排查'} for code in 'AB'},
            'rows:change':{
                'same':{'工单号':'copy','标题':'A楼本地已有','计划开始时间':'2026-11-18 21:00:00'},
                'new':{'工单号':'unique','标题':'A楼夜间变更','计划开始时间':'2026-11-18 21:00:00'},
                'unknown':{'标题':'未标楼栋','计划开始时间':'2026-11-18 21:00:00'},
            },
            'rows:event':{'a':{'事件标题':'没有该模块','专业域编码':'5.1','发生时间':'2026-11-18 12:00:00'}},
        }
        store = SimpleNamespace(
            list_documents=lambda _namespace, key_prefix: [{'payload':row} for key,row in documents.items() if key.startswith(key_prefix)],
            get_document=lambda _namespace,key: documents[key])
        result = read_zhihang_schedule(SimpleNamespace(_state_store=store),'2026-11',{'A'},{'change','risk_inspection'},{'copy'})
        self.assertEqual({row['title'] for row in result['2026-11-18']},{'A楼夜间变更','A楼排查'})
        self.assertTrue(all(row['source']=='智航' for row in result['2026-11-18']))

    async def test_guard_reads_the_last_verified_state_without_fetching_forecast(self):
        guard_state = {
            'current_warnings': [{'id':'current','title':'当前大风预警','guard_level':'三级'}],
            'current_guard_level': '三级',
            'last_error': '网络读取失败',
            'paused': True,
            'snapshot': {'authoritative_for_state':False,'warnings':[{'id':'unverified','title':'未经识别的预警'}],
                         'guard_level':'一级','actions':['未经确认的动作']},
        }
        controller = SimpleNamespace(_current_session=lambda request: {'user':{'open_id':'isolated'}})
        store = SimpleNamespace(get_critical_guard_weather_state=lambda: guard_state)
        app = FastAPI()
        app.state.lighthouse_icon_weather = SimpleNamespace(get=AsyncMock(side_effect=OSError('forecast unavailable')))
        install_workbench_calendar(app, controller, SimpleNamespace(service=SimpleNamespace(_state_store=store)))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
            received = (await client.get('/api/personal-calendar/weather')).json()['data']
            self.assertTrue(received['warning_ready'])
            self.assertEqual(received['alerts'], guard_state['current_warnings'])
            self.assertEqual(received['guard_level'], '三级')
            self.assertIn('上次识别结果', received['guard_note'])
            self.assertIn('已暂停', received['guard_note'])
            self.assertEqual(received['guard_actions'], [])
            guard_state.update(current_warnings=[], current_guard_level='', last_error='', paused=False,
                               snapshot={'authoritative_for_state':True})
            cleared = (await client.get('/api/personal-calendar/weather')).json()['data']
            self.assertTrue(cleared['warning_ready'])
            self.assertEqual((cleared['alerts'], cleared['guard_level'], cleared['guard_note']), ([], '', ''))

            with patch.object(store, 'get_critical_guard_weather_state', side_effect=OSError('store unavailable')):
                unavailable = (await client.get('/api/personal-calendar/weather')).json()['data']
            self.assertFalse(unavailable['warning_ready'])
            self.assertTrue(unavailable['guard_read_failed'])
            self.assertEqual(unavailable['guard_note'], '暂时无法读取重保管理状态')
        app.state.lighthouse_icon_weather.get.assert_not_awaited()
        await app.router.shutdown()


if __name__ == '__main__':
    unittest.main()


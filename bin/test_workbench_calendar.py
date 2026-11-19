"""Workbench calendar projection tests using isolated Lighthouse data."""
import datetime as dt
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.workbench_calendar import (
    install_workbench_calendar, public_schedule, read_roster, TZ,
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
             patch('lan_bitable_template_portal.workbench_calendar.allowed_modules', return_value={'change'}):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
                aggregate = await client.get('/api/personal-calendar?month=2026-11')
                wrong = await client.get('/api/personal-calendar?month=2026-11&scope=B')
        await app.router.shutdown()
        self.assertEqual(wrong.status_code, 403)
        self.assertEqual(aggregate.status_code, 200)
        body = aggregate.json()['data']['days']['2026-11-18']
        self.assertEqual([row['scope'] for row in body['staff']], ['A'])
        self.assertEqual([row['title'] for row in body['items']], ['封网窗口','A楼窗口'])

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


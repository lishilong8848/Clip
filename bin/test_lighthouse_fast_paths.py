"""Local/native fast paths must not require an LLM, gateway, or cloud writes."""
import asyncio
import datetime as dt
import os
import sys
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
from clipflow_backend.api_models import WorkbenchActionRequest
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_model import LighthouseModel
from openclaw_service.assistant import lighthouse_public as public
from upload_event_module.services import http_client as tls_policy
import test_lighthouse_stream as stream_tests


@asynccontextmanager
async def no_model(*_, **__):
    raise AssertionError('Native fast path entered the model/gateway')
    yield


class FastPathTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await stream_tests.StreamTests.asyncSetUp(self)

    async def asyncTearDown(self):
        await self.runtime.close()

    async def authorize(self):
        return self.actor

    async def test_weather_skill_and_failure_do_not_call_model_or_business(self):
        weather = Mock(weather=AsyncMock(return_value={'ok': False, 'note': 'Source unavailable'}))
        engine = LighthouseModel(self.portal, model_factory=no_model, public_sources=weather)
        turn = {'question': '明天南通天气怎么样?', 'operation_id': 'weather-fast',
                '_profile': self.model.profile(), 'commands': [{'kind': 'skill', 'id': 'workbuddy-tencent-weather'}]}
        result = await engine.answer(self.actor, turn, [], self.request, AsyncMock(), self.authorize, {})
        weather.weather.assert_awaited_once_with('南通', turn['question'])
        self.assertIn('暂未取得', result['answer'])
        self.assertEqual(self.reads, [])

    async def test_pasted_start_prepares_native_form_only_with_scope_protection(self):
        writes = []
        @self.app.post('/api/workbench-actions')
        async def send(body: WorkbenchActionRequest):
            writes.append(body)
            raise AssertionError('No write before confirmation')
        self.portal.catalog = PortalAPICatalog(self.app)
        engine = LighthouseModel(self.portal, model_factory=no_model)
        question = ('发送通告:\n【维保通告】状态:开始\n【名称】EA118-D楼维护\n'
                    '【时间】2026年10月5日9:30-20:50\n【位置】D楼\n'
                    '【内容】维护\n【原因】周期维护\n【影响】无\n【进度】人员就位')
        turn = {'question': question, 'operation_id': 'notice-fast-00001', '_profile': self.model.profile()}
        result = await engine.answer(self.actor, turn, [], self.request, AsyncMock(), self.authorize, {})
        self.assertEqual(result['plan']['status'], 'needs_input')
        fields = result['plan']['fields']
        self.assertTrue(any(field.get('native_notice') for field in fields))
        self.assertTrue(any(field['path'] == 'manual_binding_choice' for field in fields))
        self.assertFalse(any(field['path'] in {'building', 'location', 'notice_type', 'polling_runs'} for field in fields))
        self.assertEqual(writes, [])
        denied = await engine.answer(self.actor, {**turn, 'question': question.replace('D楼', 'E楼')}, [],
                                     self.request, AsyncMock(), self.authorize, {})
        self.assertNotIn('plan', denied)
        self.assertIn('权限', denied['answer'])

    async def test_builtin_module_guide_keeps_direct_pending_query(self):
        engine = LighthouseModel(self.portal, model_factory=no_model)
        with patch('lan_bitable_template_portal.lighthouse_pending.collect_repair_overview',
                   AsyncMock(return_value={'groups': [], 'scopes': ['D'], 'queried_at': 'fixture'})) as read:
            await engine.answer(self.actor, {'question': '当前有多少未完成维修单', '_profile': self.model.profile(),
                'commands': [{'kind': 'skill', 'id': 'lighthouse-repairs'}],
                '_command_context': [{'kind': 'guide', 'source': 'builtin', 'name': 'lighthouse-repairs'}]},
                [], self.request, AsyncMock(), self.authorize, {})
        read.assert_awaited_once()

    async def test_public_pool_is_reused_and_closed_without_disabling_tls(self):
        factory = Mock(return_value=httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200))))
        source = public.PublicSources()
        source._client_factory = factory
        async with source._client() as first:
            async with source._client() as second:
                self.assertIs(first, second)
        factory.assert_called_once()
        self.assertFalse(first.is_closed)
        await source.close()
        self.assertTrue(first.is_closed)
        with patch.dict(os.environ, {}, clear=True), patch.object(tls_policy, '_tls_contexts', {}), patch('ssl.create_default_context', return_value='verified-ca') as context:
            self.assertEqual(public._verified_tls_context(), 'verified-ca')
            self.assertEqual(public._verified_tls_context(), 'verified-ca')
        self.assertEqual(context.call_count, 1)
        self.assertEqual(set(context.call_args.kwargs), {'cadata'})

    def test_weather_request_and_reply_never_replace_missing_dates(self):
        self.assertIsNone(public.weather_request('D楼机房南通天气'))
        self.assertIsNone(public.weather_request('比较南通和上海明天天气'))
        self.assertIsNone(public.weather_request('南通今天和明天的天气'))
        today = dt.datetime.now(public.TZ).date()
        self.assertEqual(public.weather_request('明天南通天气'), ('南通', (today + dt.timedelta(days=1)).isoformat()))
        self.assertIn('覆盖范围', public.weather_reply({'ok': True, 'daily': []}, '南通', '2026-10-01'))

    def test_selected_weather_understands_english_dates_and_rejects_conflicts(self):
        today = dt.datetime.now(public.TZ).date()
        for phrase, offset in (('tomorrow', 1), ('yesterday', -1), ('day after tomorrow', 2),
                               ('day before yesterday', -2), ('tonight', 0)):
            with self.subTest(phrase=phrase):
                self.assertEqual(public.weather_request('Nantong ' + phrase, selected=True),
                                 ('南通', (today + dt.timedelta(days=offset)).isoformat()))
        self.assertIsNone(public.weather_request('Nantong today and tomorrow', selected=True))
        self.assertIsNone(public.weather_request('南通后天和明天天气'))
        self.assertIsNone(public.weather_request('南通明天 ' + (today + dt.timedelta(days=2)).isoformat() + ' 天气'))

    def test_weather_never_defaults_unsupported_calendar_or_hour_to_today(self):
        for question in ('南通下周天气怎么样？', '南通星期五天气', '南通本月底天气', '南通三天后天气',
                '南通10月份天气', '南通7号天气', '南通今晚20点天气', '南通现在气温',
                'Nantong weather next Monday', 'Nantong weather this weekend', 'Nantong weather October 7',
                'Nantong weather next month', 'Nantong weather last night', 'Nantong weather in two days', '南通明年10月7日天气',
                'Nantong temperature right now', 'Nantong weather at 20:00'):
            with self.subTest(question=question):
                self.assertIsNone(public.weather_request(question, selected=True))

    def test_weather_parses_all_explicit_dates_and_month_day_without_losing_year(self):
        today = dt.datetime.now(public.TZ).date()
        self.assertEqual(public.weather_request('南通10月7日天气'), ('南通', f'{today.year}-10-07'))
        self.assertEqual(public.weather_request('南通2027年10月7日天气'), ('南通', '2027-10-07'))
        for question in ('南通2026-10-06和2026-10-07天气', '南通10月6日和10月7日天气',
                         '南通2026-02-30天气', '南通13月7日天气'):
            with self.subTest(question=question):
                self.assertIsNone(public.weather_request(question))

    async def test_english_weather_and_followup_use_real_fast_path_without_model(self):
        today = dt.datetime.now(public.TZ).date()
        tomorrow = (today + dt.timedelta(days=1)).isoformat()
        weather = Mock(weather=AsyncMock(return_value={'ok': True,
            'sourceURL': 'https://example.com/forecast', 'daily': [{'date': tomorrow, 'min': 12, 'max': 23}]}))
        engine = LighthouseModel(self.portal, model_factory=no_model, public_sources=weather)
        for question, history in (('Nantong weather tomorrow?', []),
                ('What about tomorrow?', [{'question': 'Nantong weather today?', 'status': 'completed'}])):
            with self.subTest(question=question):
                before = weather.weather.await_count
                result = await engine.answer(self.actor, {'question': question, 'operation_id': 'english-weather',
                    '_profile': self.model.profile()}, history, self.request, AsyncMock(), self.authorize, {})
                self.assertEqual(weather.weather.await_count - before, 1)
                self.assertIn(tomorrow, result['answer'])
                self.assertIn('12', result['answer'])
                self.assertTrue(result['sources'])
        self.assertEqual(self.reads, [])

    def test_weather_followup_does_not_take_over_translation_or_new_subject(self):
        self.assertIsNone(public.weather_request('明天英文怎么说', '南通今天天气怎么样？'))
        self.assertIsNone(public.weather_request('Translate weather into Chinese', 'Nantong weather today?'))
        self.assertIsNone(public.WEATHER_FOLLOWUP.search('What are my tasks tomorrow?'))
        self.assertIsNone(public.WEATHER_FOLLOWUP.search('明天英语怎么说'))

    async def test_china_weather_fallback_validates_date_temperature_and_city(self):
        today = dt.datetime.now(public.TZ)
        content = (f'<title>南通天气</title><input id="fc_24h_internal_update_time" value="{today:%Y%m%d%H}">'
                   f'<li class="sky"><h1>{today.day}日（今天）</h1><p class="wea">晴</p>'
                   '<p class="tem"><span>22</span>/<i>11℃</i></p><p class="win">东风3级</p></li>').encode()
        parsed = public._china_forecast(content, '南通', today.isoformat())
        self.assertEqual(parsed[0]['min'], 11)
        self.assertEqual(parsed[0]['date'], today.date().isoformat())
        self.assertIsNone(public._china_forecast(content.replace(b'22', b'999'), '南通', today.isoformat()))
        self.assertIsNone(public._china_forecast(content, '上海', today.isoformat()))
        self.assertIsNone(public._china_forecast(content, '南通', (today + dt.timedelta(days=2)).isoformat()))
        calls = []
        def handler(request):
            calls.append(request.url.host)
            return httpx.Response(200, content=content) if request.url.host == 'www.weather.com.cn' else httpx.Response(503)
        source = public.PublicSources(client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)))
        with patch.dict('os.environ', {public.CUSTOM_KEY_ENV: ''}):
            result = await source.weather('南通', '南通今天天气怎么样')
        self.assertTrue(result['ok'])
        self.assertEqual(result['source'], '中国天气网')
        self.assertEqual(calls, ['www.weather.com.cn'])
        self.assertIsNone(result['current'])
        self.assertTrue(result['observation_unavailable'])

    def test_china_weather_keeps_today_night_forecast_without_inventing_maximum(self):
        today = dt.datetime.now(public.TZ).replace(hour=22)
        content = (f'<title>南通天气</title><input id="fc_24h_internal_update_time" value="{today:%Y%m%d%H}">'
                   f'<li class="sky"><h1>{today.day}日（今天）</h1><p class="wea">晴</p>'
                   '<p class="tem"><span></span><i>10℃</i></p><p class="win">西北风&lt;3级</p></li>').encode()
        parsed = public._china_forecast(content, '南通', today.isoformat())
        self.assertEqual(parsed, [{'date': today.date().isoformat(), 'max': None, 'min': 10,
                                  'weather_desc': '晴', 'wind': '西北风<3级', 'night_only': True}])
        reply = public.weather_reply({'ok': True, 'daily': parsed}, '南通', today.date().isoformat())
        self.assertIn('今夜最低 10', reply)
        self.assertIn('白天最高气温未提供', reply)
        self.assertNotIn('覆盖范围', reply)
        self.assertIsNone(public._china_forecast(content.replace(b'<i>10', b'<i>999'), '南通', today.isoformat()))
        self.assertIsNone(public._china_forecast(content.replace(b'<i>10', b'<span>10').replace(b'</i>', b'</span>'),
                                                '南通', today.isoformat()))

    def test_cached_china_forecast_after_midnight_preserves_future_dates_not_yesterday(self):
        published = dt.datetime(2026, 9, 30, 20, tzinfo=public.TZ)
        now = published + dt.timedelta(hours=5)
        content = ('<title>南通天气</title><input id="fc_24h_internal_update_time" value="2026093020">'
                   '<li class="sky"><h1>30日（今天）</h1><p class="wea">晴</p><p class="tem"><i>12℃</i></p></li>'
                   '<li class="sky"><h1>1日（明天）</h1><p class="wea">多云</p><p class="tem"><span>23</span><i>10℃</i></p></li>'
                   '<li class="sky"><h1>2日（后天）</h1><p class="wea">阴</p><p class="tem"><span>22</span><i>11℃</i></p></li>').encode()
        parsed = public._china_forecast(content, '南通', now.isoformat())
        self.assertEqual([row['date'] for row in parsed], ['2026-10-01', '2026-10-02'])
        self.assertEqual((parsed[0]['min'], parsed[0]['max']), (10, 23))
        self.assertIn('覆盖范围', public.weather_reply({'ok': True, 'daily': parsed}, '南通', '2026-09-30'))
        self.assertIsNone(public._china_forecast(content.replace('1日（明天）'.encode(), '3日（明天）'.encode()),
                                                '南通', now.isoformat()))
        self.assertIsNone(public._china_forecast(content, '南通', (now + dt.timedelta(days=2)).isoformat()))


if __name__ == '__main__':
    unittest.main()

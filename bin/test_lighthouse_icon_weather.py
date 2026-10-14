"""Isolated icon-weather checks: no business writes, network, or Agent startup."""
import asyncio
import datetime as dt
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import httpx
from fastapi import FastAPI
from lan_bitable_template_portal.lighthouse_icon_weather import (
    IconWeather, SolarTimes, TZ, city_name, snapshot, install_icon_weather,
)
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_appearance import read_appearance, save_appearance


def forecast(city='南通'):
    return {'ok': True, 'city': city, 'daily': [{'date': dt.datetime.now(TZ).date().isoformat(),
        'weather_desc': '晴转多云', 'min': 13, 'max': 23}]}


class IconWeatherTests(unittest.IsolatedAsyncioTestCase):
    async def test_twenty_accounts_share_one_city_fetch_and_cache(self):
        async def fetch(city, queried_at):
            await asyncio.sleep(.02)
            return forecast(city)
        source = AsyncMock()
        source._weather_china.side_effect = fetch
        weather = IconWeather(source)
        try:
            results = await asyncio.gather(*(weather.get('南通市') for _ in range(20)))
            self.assertTrue(all(row['available'] and row['city'] == '南通' for row in results))
            await weather.get('南通')
            source._weather_china.assert_awaited_once()
            self.assertFalse(weather.tasks)
        finally:
            await weather.close()

    async def test_different_cities_do_not_mix_and_cache_stays_bounded(self):
        source = AsyncMock()
        source._weather_china.side_effect = lambda city, at: forecast(city)
        weather = IconWeather(source)
        try:
            for city in ['南通', '上海'] + ['测试' + chr(0x4e00 + i) for i in range(35)]:
                self.assertEqual((await weather.get(city))['city'], city)
            self.assertEqual(len(weather.cache), 32)
        finally:
            await weather.close()

    async def test_failures_cached_without_clear_weather(self):
        source = AsyncMock()
        source._weather_china.side_effect = OSError('fixture')
        weather = IconWeather(source)
        try:
            for _ in range(3):
                row = await weather.get('石家庄')
                self.assertFalse(row['available'])
                self.assertNotIn('description', row)
            source._weather_china.assert_awaited_once()
        finally:
            await weather.close()

    async def test_previous_date_cache_is_not_reused_and_closed_service_cannot_reopen(self):
        source = AsyncMock()
        source._weather_china.return_value = forecast()
        weather = IconWeather(source)
        yesterday = (dt.datetime.now(TZ).date() - dt.timedelta(days=1)).isoformat()
        weather.cache['南通'] = (time.monotonic() + 600, {'available': True, 'city': '南通', 'date': yesterday})
        self.assertEqual((await weather.get('南通'))['date'], forecast()['daily'][0]['date'])
        source._weather_china.assert_awaited_once()
        await weather.close()
        with self.assertRaises(AssistantError):
            await weather.get('南通')

    async def test_network_concurrency_is_three_and_cancellation_releases_slots(self):
        running = maximum = 0
        release = asyncio.Event()
        async def fetch(city, at):
            nonlocal running, maximum
            running += 1; maximum = max(maximum, running)
            try:
                await release.wait()
                return forecast(city)
            finally:
                running -= 1
        source = AsyncMock()
        source._weather_china.side_effect = fetch
        weather = IconWeather(source)
        callers = [asyncio.create_task(weather.get(city)) for city in ['南通', '上海', '南京', '北京', '天津']]
        await asyncio.sleep(.02)
        self.assertEqual(maximum, 3)
        callers[0].cancel()
        release.set()
        await asyncio.gather(*callers, return_exceptions=True)
        await weather.close()
        self.assertFalse(weather.tasks)
        self.assertEqual(running, 0)

    async def test_route_rejects_unauthenticated_without_preparing_agent(self):
        app = FastAPI()
        install_icon_weather(app, object(), object())
        with patch('lan_bitable_template_portal.lighthouse_icon_weather.actor_for',
                   AsyncMock(side_effect=AssistantError('login', 401))), \
             patch.object(IconWeather, 'get', AsyncMock()) as get:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
                response = await client.get('/api/assistant/icon-weather')
            self.assertEqual(response.status_code, 401)
            get.assert_not_awaited()
        await app.router.shutdown()

    async def test_close_cleans_tasks_cancelled_before_start(self):
        source = AsyncMock()
        weather = IconWeather(source)
        caller = asyncio.create_task(weather.get('南通'))
        await asyncio.sleep(0)
        await weather.close()
        await asyncio.gather(caller, return_exceptions=True)
        self.assertFalse(weather.tasks)
        self.assertFalse(weather.cache)


class WeatherValidationTests(unittest.TestCase):
    def test_current_date_required(self):
        data = forecast()
        self.assertEqual(snapshot(data, '南通', time.time())['source'], '中国天气网')
        data['daily'][0]['date'] = '2000-01-01'
        with self.assertRaises(ValueError):
            snapshot(data, '南通', time.time())

    def test_solar_times_require_today_and_order(self):
        now = dt.datetime.now(TZ).replace(hour=12, minute=0, second=0)
        parser = SolarTimes()
        parser.feed(f'<input id="fc_24h_internal_update_time" value="{now:%Y-%m-%d} 08:00">'
                    '<p class="sun sunUp"><span>日出 05:58</span></p>'
                    '<p class="sun sunDown"><span>日落 17:26</span></p>')
        times = parser.times(now.timestamp())
        self.assertLess(times['sunrise'], times['sunset'])
        self.assertFalse(parser.times(now.timestamp() + 86400))
        parser.values['sunset'] = '01:00'
        self.assertFalse(parser.times(now.timestamp()))

    def test_city_boundary_and_account_isolation(self):
        for value in ('', 'x', '../上海', 'https://x.test', '南通 token', ['南通']):
            with self.assertRaises(AssistantError):
                city_name(value)
        class Store:
            def __init__(self): self.documents = {}
            def get_document(self, namespace, actor): return self.documents.get(actor)
            def put_document(self, namespace, actor, value): self.documents[actor] = value
        store = Store()
        save_appearance(store, 'one', {'shape': 'weather-lighthouse', 'weather_city': '石家庄'})
        self.assertEqual(read_appearance(store, 'one')['weather_city'], '石家庄')
        self.assertEqual(read_appearance(store, 'two')['weather_city'], '南通')
        self.assertEqual(read_appearance(store, 'two')['shape'], 'weather-lighthouse')
        save_appearance(store, 'two', {'shape': 'cercle'})
        self.assertEqual(read_appearance(store, 'two')['shape'], 'cercle', 'explicit personal shapes are retained')


if __name__ == '__main__':
    unittest.main()

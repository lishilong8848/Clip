"""Small, shared public-weather cache; never starts an Agent or touches business data."""
from __future__ import annotations

import asyncio
from collections import OrderedDict
import datetime as dt
from html.parser import HTMLParser
import re
import time
import httpx

from fastapi import Request
from fastapi.responses import JSONResponse

from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_public import PublicSources, _read_bounded
from .lighthouse_bridge import actor_for

TZ = dt.timezone(dt.timedelta(hours=8))


def city_name(value):
    if not isinstance(value, str) or not re.fullmatch(r'[\u4e00-\u9fff]{2,16}', value):
        raise AssistantError('请填写有效的中文城市名称。')
    return value[:-1] if value.endswith('市') and len(value) > 2 else value


def snapshot(data, city, now):
    today = dt.datetime.fromtimestamp(now, TZ).date().isoformat()
    row = next((row for row in data.get('daily', []) if row.get('date') == today), None)
    if not data.get('ok') or not row or not isinstance(row.get('weather_desc'), str) or not row['weather_desc']:
        raise ValueError('No verified forecast for today')
    return {'available': True, 'city': city, 'description': row['weather_desc'], 'date': today,
            'minimum': row.get('min'), 'maximum': row.get('max'),
            'source': '中国天气网', 'fetched_at': now}


class SolarTimes(HTMLParser):
    def __init__(self):
        super().__init__()
        self.published, self.kind, self.values = '', '', {}

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'input' and attrs.get('id') == 'fc_24h_internal_update_time':
            self.published = attrs.get('value', '')
        if tag == 'p':
            classes = attrs.get('class', '').split()
            self.kind = 'sunrise' if 'sunUp' in classes else 'sunset' if 'sunDown' in classes else ''

    def handle_endtag(self, tag):
        if tag == 'p':
            self.kind = ''

    def handle_data(self, text):
        if self.kind and re.fullmatch(r'\s*日[出落] \d{2}:\d{2}\s*', text):
            self.values[self.kind] = text.strip()[-5:]

    def times(self, now):
        today = dt.datetime.fromtimestamp(now, TZ).date()
        published = dt.datetime.strptime(self.published, '%Y-%m-%d %H:%M').replace(tzinfo=TZ)
        if published.date() != today or not 0 <= now - published.timestamp() <= 86400:
            return {}
        result = {key: dt.datetime.combine(today, dt.time.fromisoformat(value), TZ).timestamp()
                  for key, value in self.values.items()}
        return result if result.get('sunrise', 1) < result.get('sunset', 0) else {}


class IconWeather:
    def __init__(self, sources=None):
        self.sources = sources or PublicSources(timeout=8)
        self.cache, self.tasks = OrderedDict(), {}
        self.slots = asyncio.Semaphore(3)
        self.closed = False

    async def get(self, city):
        if self.closed:
            raise AssistantError('天气连接正在关闭。', 503)
        city = city_name(city)
        cached = self.cache.get(city)
        today = dt.datetime.now(TZ).date().isoformat()
        if cached and time.monotonic() < cached[0] and (not cached[1]['available'] or cached[1].get('date') == today):
            self.cache.move_to_end(city)
            return {**cached[1], 'server_time': time.time()}
        if city not in self.tasks:
            # Bound both network concurrency and queued work, including failed cities.
            if len(self.tasks) >= 32:
                return {'available': False, 'city': city, 'note': '天气暂忙', 'server_time': time.time()}
            self.tasks[city] = asyncio.create_task(self._load(city))
        result = await asyncio.shield(self.tasks[city])
        return {**result, 'server_time': time.time()}

    async def _load(self, city):
        result = None
        try:
            async with asyncio.timeout(12), self.slots:
                data = await self.sources._weather_china(city, dt.datetime.now(TZ).isoformat())
                result = snapshot(data, city, time.time())
                url = data.get('sourceURL', '')
                if re.fullmatch(r'https://www\.weather\.com\.cn/weather/101\d{6}\.shtml', url):
                    try:
                        async with self.sources._client() as client:
                            async with client.stream('GET', url.replace('/weather/', '/weather1d/'), timeout=3) as response:
                                if response.status_code == 200:
                                    parser = SolarTimes()
                                    parser.feed((await _read_bounded(response, '日出日落')).decode('utf-8'))
                                    result.update(parser.times(time.time()))
                    except (OSError, ValueError, httpx.HTTPError, asyncio.TimeoutError):
                        pass
        except asyncio.CancelledError:
            raise
        except Exception:
            # Do not retain another city's weather or label a failed request as clear.
            if result is None:
                result = {'available': False, 'city': city, 'note': '天气暂不可用', 'fetched_at': time.time()}
        finally:
            self.tasks.pop(city, None)
        self.cache[city] = (time.monotonic() + (600 if result['available'] else 60), result)
        self.cache.move_to_end(city)
        while len(self.cache) > 32:
            self.cache.popitem(last=False)
        return result

    async def close(self):
        self.closed = True
        tasks = list(self.tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.tasks.clear()
        await self.sources.close()
        self.cache.clear()


def install_icon_weather(app, controller, runtime):
    weather = IconWeather()
    app.state.lighthouse_icon_weather = weather

    async def endpoint(request: Request):
        try:
            await actor_for(controller, runtime, request)
            data = await weather.get(request.query_params.get('city', '南通'))
            return JSONResponse({'ok': True, 'data': data}, headers={'Cache-Control': 'no-store'})
        except AssistantError as exc:
            return JSONResponse({'ok': False, 'error': str(exc)}, status_code=exc.status)

    app.add_api_route('/api/assistant/icon-weather', endpoint, methods=['GET'], name='lighthouse_icon_weather')
    app.add_event_handler('shutdown', weather.close)

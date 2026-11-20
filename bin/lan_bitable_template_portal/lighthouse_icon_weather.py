"""Small, shared public-weather cache; never starts an Agent or touches business data."""
from __future__ import annotations

import asyncio
from collections import OrderedDict
import datetime as dt
from html.parser import HTMLParser
import json
import re
import time
import httpx

from fastapi import Request
from fastapi.responses import JSONResponse

from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_public import (
    CHINA_WEATHER_CITIES, MAX_OBS_AGE_SECONDS, PublicSources, _fnum_str, _read_bounded,
)
from .lighthouse_bridge import actor_for

TZ = dt.timezone(dt.timedelta(hours=8))
CHINA_HOURLY_URL = 'https://www.weather.com.cn/weather1dn/{code}.shtml'
CHINA_SKY = {'00': '晴', '01': '多云', '02': '阴', '03': '阵雨', '04': '雷阵雨',
             '05': '雷阵雨伴冰雹', '06': '雨夹雪', '07': '小雨', '08': '中雨', '09': '大雨',
             '10': '暴雨', '11': '大暴雨', '12': '特大暴雨', '13': '阵雪', '14': '小雪',
             '15': '中雪', '16': '大雪', '17': '暴雪', '18': '雾', '19': '冻雨',
             '20': '沙尘暴', '21': '小到中雨', '22': '中到大雨', '23': '大到暴雨',
             '24': '暴雨到大暴雨', '25': '大暴雨到特大暴雨', '26': '小到中雪',
             '27': '中到大雪', '28': '大到暴雪', '29': '浮尘', '30': '扬沙', '31': '强沙尘暴', '53': '霾'}


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


def _embedded_json(html, name):
    match = re.search(r'\bvar\s+' + name + r'\s*=\s*', html)
    if match:
        try:
            return json.JSONDecoder().raw_decode(html[match.end():])[0]
        except (ValueError, RecursionError):
            pass
    return None


def china_hourly_snapshot(raw, city, now):
    html = raw.decode('utf-8')
    current = dt.datetime.fromtimestamp(now, TZ)
    hour = current.replace(minute=0, second=0, microsecond=0)
    horizon = hour + dt.timedelta(hours=24)
    forecast = _embedded_json(html, 'hour3data')
    hourly = {}
    for day in forecast if isinstance(forecast, list) else []:
        for row in day if isinstance(day, list) else []:
            if not isinstance(row, dict) or not re.fullmatch(r'\d{10}', str(row.get('jf', ''))):
                continue
            try:
                at = dt.datetime.strptime(row['jf'], '%Y%m%d%H').replace(tzinfo=TZ)
            except (TypeError, ValueError):
                continue
            temp = _fnum_str(row.get('jb'))
            if hour <= at <= horizon and temp is not None and -90 <= temp <= 60:
                hourly[at] = {'time': at.isoformat(), 'temp': temp, 'condition': CHINA_SKY.get(str(row.get('ja')), '')}

    observation = _embedded_json(html, 'observe24h_data')
    observation = observation.get('od') if isinstance(observation, dict) else None
    temp = humidity = observed_at = None
    if isinstance(observation, dict) and observation.get('od1') == city:
        rows = observation.get('od2')
        stamp = str(observation.get('od0') or '')
        try:
            at = dt.datetime.strptime(stamp, '%Y%m%d%H%M').replace(tzinfo=TZ) if re.fullmatch(r'\d{12}', stamp) else None
        except ValueError:
            at = None
        if at and 0 <= now - at.timestamp() <= MAX_OBS_AGE_SECONDS and isinstance(rows, list) and rows:
            row = rows[0]
            if isinstance(row, dict) and str(row.get('od21') or '').zfill(2) == at.strftime('%H'):
                temp, humidity = _fnum_str(row.get('od22')), _fnum_str(row.get('od27'))
                temp = temp if temp is not None and -90 <= temp <= 60 else None
                humidity = humidity if humidity is not None and 0 <= humidity <= 100 else None
                observed_at = at.isoformat() if temp is not None else None

    points = [hourly[key] for key in sorted(hourly)]
    if not points and temp is None:
        return None
    return {'available': True, 'city': city, 'date': current.date().isoformat(),
            'description': points[0]['condition'] if points else '', 'temperature': temp, 'humidity': humidity,
            'observed_at': observed_at, 'hourly': points, 'source': '中国天气网', 'fetched_at': now}


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

    async def get(self, city, *, detail=False):
        if self.closed:
            raise AssistantError('天气连接正在关闭。', 503)
        city = city_name(city)
        key = (city, detail)
        cached = self.cache.get(key)
        today = dt.datetime.now(TZ).date().isoformat()
        if cached and time.monotonic() < cached[0] and (not cached[1]['available'] or cached[1].get('date') == today):
            self.cache.move_to_end(key)
            return {**cached[1], 'server_time': time.time()}
        if key not in self.tasks:
            # Bound both network concurrency and queued work, including failed cities.
            if len(self.tasks) >= 32:
                return {'available': False, 'city': city, 'note': '天气暂忙', 'server_time': time.time()}
            self.tasks[key] = asyncio.create_task(self._load(city, detail=detail))
        result = await asyncio.shield(self.tasks[key])
        return {**result, 'server_time': time.time()}

    async def _china_hourly(self, city):
        url = CHINA_HOURLY_URL.format(code=CHINA_WEATHER_CITIES[city])
        async with asyncio.timeout(5), self.sources._client() as client:
            async with client.stream('GET', url, timeout=5) as response:
                if response.status_code == 200:
                    return china_hourly_snapshot(await _read_bounded(response, '逐小时天气'), city, time.time())
        return None

    async def _load(self, city, *, detail=False):
        result = None
        try:
            async with asyncio.timeout(12), self.slots:
                queried_at = dt.datetime.now(TZ).isoformat()
                if detail and city in CHINA_WEATHER_CITIES:
                    try:
                        result = await self._china_hourly(city)
                    except (httpx.HTTPError, OSError, ValueError, asyncio.TimeoutError):
                        pass
                    if result:
                        return self._remember(city, detail, result)
                if detail:
                    try:
                        data = await asyncio.wait_for(self.sources._weather_wttr(city, queried_at), timeout=4)
                    except (httpx.HTTPError, OSError, ValueError, TypeError, asyncio.TimeoutError):
                        data = {}
                    if not data.get('ok'):
                        data = await self.sources._weather_china(city, queried_at)
                else:
                    data = await self.sources._weather_china(city, queried_at)
                result = snapshot(data, city, time.time())
                if detail:
                    current = data.get('current') or {}
                    result.update(source=data.get('source') or '公开天气服务',
                                  temperature=current.get('temperature'),
                                  humidity=current.get('relative_humidity_pct'),
                                  current_description=current.get('weather_desc'),
                                  observed_at=data.get('observation_time'),
                                  hourly=[],
                                  forecast=[{'date': row['date'], 'description': row.get('weather_desc'),
                                             'minimum': row.get('min'), 'maximum': row.get('max')}
                                            for row in data['daily'][:5]])
                url = data.get('sourceURL', '')
                if not detail and re.fullmatch(r'https://www\.weather\.com\.cn/weather/101\d{6}\.shtml', url):
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
            self.tasks.pop((city, detail), None)
        return self._remember(city, detail, result)

    def _remember(self, city, detail, result):
        key = (city, detail)
        complete = result['available'] and (not detail or result.get('hourly'))
        self.cache[key] = (time.monotonic() + (600 if complete else 60), result)
        self.cache.move_to_end(key)
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
            detail = request.query_params.get('detail') == '1'
            data = await weather.get('南通' if detail else request.query_params.get('city', '南通'), detail=detail)
            return JSONResponse({'ok': True, 'data': data}, headers={'Cache-Control': 'no-store'})
        except AssistantError as exc:
            return JSONResponse({'ok': False, 'error': str(exc)}, status_code=exc.status)

    app.add_api_route('/api/assistant/icon-weather', endpoint, methods=['GET'], name='lighthouse_icon_weather')
    app.add_event_handler('shutdown', weather.close)

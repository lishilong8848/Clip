"""Bounded public-query helpers: search (Bing RSS/Mwmbl) and weather.

Only fixed public endpoints are contacted. Responses are streamed with a 512 KiB
hard bound (XML and JSON alike); redirects are never followed (any 3xx is
unavailable); business/private/intranet/secret inputs are rejected before any
network I/O; XML accepts standard entities and numeric refs but rejects
DTD/ENTITY; returned URLs are public http(s) only. Weather uses commercial
Open-Meteo only when OPEN_METEO_CUSTOMER_KEY is set (non-default). Otherwise it
tries the keyless official wttr.in JSON API (https://wttr.in, format=j1&lang=zh)
and the fixed China Weather Nantong page, validates city resolution/freshness/finite ranges, then falls back
to honest Bing search evidence when the structured tool is unavailable. It never
fabricates a structured forecast.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import html
import ipaddress
import json
import math
import os
import re
import unicodedata
import xml.etree.ElementTree as ET
from contextlib import asynccontextmanager
from copy import deepcopy
from html.parser import HTMLParser
from typing import Any, Optional
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

import httpx
from upload_event_module.services.http_client import verified_tls_context as _verified_tls_context
from .lighthouse_ai import API_KEY, CONTACT, is_business_query, private_identifier

TZ = dt.timezone(dt.timedelta(hours=8))
SHANGHAI_TZ = "Asia/Shanghai"
BING_RSS_URL = "https://www.bing.com/search?format=rss&q={q}"
MWMBL_URL = "https://api.mwmbl.org/search/?s={q}"
CUSTOM_METEO_GEO = "https://customer-api.open-meteo.com/v1/search"
CUSTOM_METEO_FORECAST = "https://customer-api.open-meteo.com/v1/forecast"
CUSTOM_KEY_ENV = "OPEN_METEO_CUSTOMER_KEY"
DEFAULT_TIMEOUT = 20
MAX_BYTES = 512 * 1024
MAX_RESULTS = 5
MAX_CITY_CHARS = 40
SEARCH_MIN_QUERY_LEN = 2
SEARCH_QUALIFIERS = frozenset({'official', 'documentation', 'docs', 'tutorial', 'site', 'org', 'com', 'net', 'gov', 'cn'})
FORECAST_DAYS = 3
WEATHER_INTENT = re.compile(r"天气|气温|预报|下雨|下雪|降雨|降雪|温度|\b(?:weather|forecast|temperature|rain|snow|precipitation)\b", re.I)
WEATHER_EXPLANATION = re.compile(r"翻译|译成|怎么说|原理|含义|术语|是什么意思|定义|怎么(?:预测|预报)|如何(?:制作|预测|预报)"
    r"|\b(?:translate|translation|spell|definition|meaning)\b|^\s*what\s+is\s+(?:the\s+)?weather[?.\s]*$", re.I)
WEATHER_FOLLOWUP = re.compile(WEATHER_INTENT.pattern + r"|^(?:那|那么|what about\s+|how about\s+)?"
    r"(?:前天|昨天|今天|今晚|明天|后天|那边|那里|(?:下|本|上)周|周末|(?:周|星期)[一二三四五六日天]"
    r"|day before yesterday|yesterday|today|tonight|tomorrow|day after tomorrow|(?:next|this|last)\s+(?:week|weekend)"
    r"|monday|tuesday|wednesday|thursday|friday|saturday|sunday)(?:呢)?[?？。!！\s]*$", re.I)

EXTRA_INTERNAL = re.compile(r"台账|内部|内网|局域网|本(?:项目|系统|平台)", re.I)
RECORD_ID_RE = re.compile(r"\brec[A-Za-z0-9]{8,}\b|\brecord[-_ ]?id\b|记录\s*id", re.I)
PRIVATE_HOST_RE = re.compile(r"(?:localhost|\.local|\.internal|\.corp\b|\.lan\b|\.home\b|内网|局域网)", re.I)
_XML_DANGER_RE = re.compile(r"<!DOCTYPE|<!ENTITY", re.I)
_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2})?$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_WSP_PUNCT_RE = re.compile(r"[\s，。？！?！、；;：:（）()【】\[\]\"'`~@#%^&*_=+|\\/{}<>.,，-]+")
_IPV4_RE = re.compile(r"(?<![0-9.])(?:\d{1,3}\.){3}\d{1,3}(?![0-9.])")
_DOMAINISH_RE = re.compile(r"(?i)\b(?:[a-z0-9-]+\.)+[a-z]{2,}\b")
_CITY_DANGER_CHARS = re.compile(r"[/\\@:]")

# Keyless public weather community service (wttr.in JSON API). HTTPS fixed host only.
WTTR_URL = "https://wttr.in/{city}?format=j1&lang=zh"
WTTR_LABEL = "publicweathercommunity no SLA"
CHINA_WEATHER_CITIES = {'南通': '101190501'}
CHINA_WEATHER_URL = 'https://www.weather.com.cn/weather/{code}.shtml'
# Known Chinese cities -> canonical English names so the keyless lookup is deterministic.
KNOWN_CHINESE_CITIES = {
    "南通": "Nantong",
    "上海": "Shanghai",
    "北京": "Beijing",
    "南京": "Nanjing",
    "苏州": "Suzhou",
    "无锡": "Wuxi",
    "杭州": "Hangzhou",
    "宁波": "Ningbo",
    "广州": "Guangzhou",
    "深圳": "Shenzhen",
    "成都": "Chengdu",
    "重庆": "Chongqing",
    "武汉": "Wuhan",
    "西安": "Xian",
    "青岛": "Qingdao",
    "厦门": "Xiamen",
    "天津": "Tianjin",
    "长沙": "Changsha",
    "郑州": "Zhengzhou",
    "济南": "Jinan",
    "沈阳": "Shenyang",
    "大连": "Dalian",
}
# Freshness: current-china observation must not be stale nor far in the future.
MAX_OBS_AGE_SECONDS = 12 * 3600
MAX_OBS_FUTURE_SECONDS = 15 * 60
# Network cap (seconds) per the operator requirement.
MAX_NETWORK_SECONDS = 20
# Keyless wttr.in may resolve Nantong to a nearby public station (its place-name
# database lacks Nantong, so it returns a nearby town such as Dexing). We only
# accept such a resolved station when it lies within this radius of the known
# Nantong city centre and is reported in the People's Republic of China, and we
# always label the actual resolved station honestly.
NANTONG_LAT = 32.0167
NANTONG_LON = 120.8667
NANTONG_RADIUS_KM = 30.0

def weather_request(question, previous_question='', *, selected=False):
    """Only unambiguous single-city, single-date queries bypass the model."""
    if len(question) > 100 or _forbidden_inputs(question) or WEATHER_EXPLANATION.search(question) \
            or not (selected or WEATHER_INTENT.search(question) or previous_question):
        return None
    if re.search(r'比较|对比|一周|几天|未来|预警|历史|最近|周末|(?:下|上|本|这)周|(?:周|星期|礼拜)[一二三四五六日天]'
            r'|月底|月初|月末|上旬|中旬|下旬|明年|去年|前年|后年|本月|下月|上月|大前天|大后天|[一二三四五六七八九十\d]+天[前后]|[前后][一二三四五六七八九十\d]+天'
            r'|实时|此刻|当前|现在|\d{1,2}[:：]\d{2}|\d{1,2}(?:点|时)'
            r'|\b(?:in|after)\s+(?:\d+|one|two|three|four|five|six|seven)\s+days?\b'
            r'|\b(?:week|weekend|month|year|season|next|last|previous|ago|monday|tuesday|wednesday|thursday|friday|saturday|sunday|now|currently|hourly'
            r'|january|jan|february|feb|march|mar|april|apr|may|june|jun|july|jul|august|aug|september|sep|october|oct|november|nov|december|dec)\b',
            question, re.I):
        return None
    cities = [city for city in KNOWN_CHINESE_CITIES if _city_in_question(city, question)]
    if not cities and previous_question:
        cities = [city for city in KNOWN_CHINESE_CITIES if _city_in_question(city, previous_question)]
    if len(cities) != 1:
        return None
    full_date = r'(20\d{2})[-年/.](\d{1,2})[-月/.](\d{1,2})日?(?!\d)'
    short_date = r'(\d{1,2})月(\d{1,2})日'
    full_dates = re.findall(full_date, question)
    remaining = re.sub(full_date, '', question)
    short_dates = re.findall(short_date, remaining)
    remaining = re.sub(short_date, '', remaining)
    if re.search(r'20\d{2}年|\d{1,2}月|\d{1,2}[日号]|\d{1,2}[-/]\d{1,2}', remaining):
        return None
    relative_offsets = {'前天': -2, '昨天': -1, '今天': 0, '今晚': 0, '明天': 1, '后天': 2,
                        'day before yesterday': -2, 'yesterday': -1, 'today': 0, 'tonight': 0,
                        'tomorrow': 1, 'day after tomorrow': 2}
    relative = {relative_offsets[match.group().lower()] for match in re.finditer(
        r'前天|昨天|今天|今晚|明天|后天|(?<![A-Za-z])(?:day before yesterday|day after tomorrow|yesterday|today|tonight|tomorrow)(?![A-Za-z])',
        question, re.I)}
    if len(relative) > 1:
        return None
    try:
        today = dt.datetime.now(TZ).date()
        explicit = {dt.date(*map(int, parts)) for parts in full_dates}
        explicit.update(dt.date(today.year, *map(int, parts)) for parts in short_dates)
        if len(explicit) > 1:
            return None
        relative_day = today + dt.timedelta(days=next(iter(relative), 0))
        day = next(iter(explicit), relative_day)
        if explicit and relative and day != relative_day:
            return None
    except ValueError:
        return None
    return cities[0], day.isoformat()


def weather_reply(value, city, day):
    if not value.get('ok'):
        return f'{city} {day} 的实时天气暂未取得。' + str(value.get('note') or '天气来源暂不可用，请稍后重试。')
    row = next((item for item in value.get('daily', []) if item.get('date') == day), None)
    if row is None:
        return f'{city} {day} 的天气不在本次数据源覆盖范围内，不能用其他日期的预报代替。[1]'
    description = row.get('weather_desc') or ''
    chance = row.get('chanceofrain', row.get('precip_probability'))
    rain = f'，降雨概率 {chance:g}%' if isinstance(chance, (int, float)) else ''
    wind = f'，{row["wind"]}' if row.get('wind') else ''
    note = '\n' + value['resolution_note'] if value.get('resolution_note') else ''
    temperature = (f'今夜最低 {row["min"]:g}°C（白天最高气温未提供）' if row.get('night_only')
                   else f'{row["min"]:g}–{row["max"]:g}°C')
    return f'**{city} {day}**：{description + "，" if description else ""}{temperature}{rain}{wind}。[1]{note}'


class _ChinaForecast(HTMLParser):
    """Parse only the site's dated forecast list, not page prose or scripts."""
    def __init__(self):
        super().__init__()
        self.rows, self.row, self.field, self.updated, self.temperature = [], None, '', '', ''

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'input' and attrs.get('id') == 'fc_24h_internal_update_time':
            self.updated = attrs.get('value', '')
        if tag == 'li' and 'sky' in attrs.get('class', '').split():
            self.row = {}
            self.temperature = ''
        if self.row is None:
            return
        if tag == 'h1':
            self.field = 'label'
        elif tag == 'p':
            self.field = attrs.get('class', '')
        if self.field == 'tem' and tag in {'span', 'i'}:
            self.temperature = 'temperature_max' if tag == 'span' else 'temperature_min'
        if tag == 'span' and self.field == 'win' and attrs.get('title'):
            self.row.setdefault('direction', attrs['title'])

    def handle_data(self, data):
        if self.row is not None and self.field:
            self.row[self.field] = self.row.get(self.field, '') + data
            if self.temperature:
                self.row[self.temperature] = self.row.get(self.temperature, '') + data

    def handle_endtag(self, tag):
        if tag in {'h1', 'p'}:
            self.field = ''
        if tag in {'span', 'i', 'p', 'li'}:
            self.temperature = ''
        if tag == 'li' and self.row is not None:
            self.rows.append(self.row)
            self.row = None


def _china_forecast(raw, city, queried_at):
    parser = _ChinaForecast()
    text = raw.decode('utf-8')
    if city + '天气' not in text:
        return None
    parser.feed(text)
    try:
        published = dt.datetime.strptime(parser.updated, '%Y%m%d%H').replace(tzinfo=TZ)
        now = dt.datetime.fromisoformat(queried_at)
        if not dt.timedelta(0) <= now - published <= dt.timedelta(hours=30):
            return None
    except (TypeError, ValueError):
        return None
    if not parser.rows:
        return None
    first = re.fullmatch(r'(\d{1,2})日[（(].+[）)]', _compact(parser.rows[0].get('label')))
    if not first:
        return None
    candidates = [published.date() + dt.timedelta(days=offset) for offset in (-1, 0, 1)
                  if (published.date() + dt.timedelta(days=offset)).day == int(first[1])]
    if len(candidates) != 1:
        return None
    start_date = candidates[0]
    daily = []
    for index, row in enumerate(parser.rows):
        day = start_date + dt.timedelta(days=index)
        label = re.fullmatch(r'(\d{1,2})日[（(].+[）)]', _compact(row.get('label')))
        temps = re.findall(r'-?\d+', row.get('tem', ''))
        if not label or int(label[1]) != day.day or not row.get('wea'):
            return None
        if day < now.date():
            continue  # Old forecasts are not historical observations.
        maximum, minimum = (_compact(row.get(key, '')) for key in ('temperature_max', 'temperature_min'))
        night_only = bool(not maximum and index == 0 and re.fullmatch(r'-?\d+(?:°C)?', minimum))
        if re.fullmatch(r'-?\d+(?:°C)?', maximum) and re.fullmatch(r'-?\d+(?:°C)?', minimum):
            hi, lo = (int(value.removesuffix('°C')) for value in (maximum, minimum))
        elif not maximum and not minimum and len(temps) == 2:
            hi, lo = map(int, temps)
        elif night_only:
            hi, lo = None, int(minimum.removesuffix('°C'))
        else:
            continue
        if not -90 <= lo <= 60 or hi is not None and not lo <= hi <= 60:
            return None
        daily.append({'date': day.isoformat(), 'max': hi, 'min': lo, 'weather_desc': ' '.join(row['wea'].split()),
                      'wind': ' '.join((row.get('direction', '') + ' ' + row.get('win', '')).split()),
                      **({'night_only': True} if night_only else {})})
    return daily or None


# --- input gates --------------------------------------------------------
def _compact(text: Any) -> str:
    return unicodedata.normalize("NFKC", str(text or "")).strip()


def _plain(text: Any) -> str:
    return _WSP_PUNCT_RE.sub("", _compact(text).lower())


def _is_private_ipv4(text: str) -> bool:
    for match in _IPV4_RE.finditer(text):
        try:
            ip = ipaddress.ip_address(match.group())
        except ValueError:
            continue
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved \
                or ip.is_multicast or ip.is_unspecified:
            return True
    return False


def _forbidden_inputs(*texts: Any) -> bool:
    for raw in texts:
        text = _compact(raw)
        if text and (is_business_query(text) or EXTRA_INTERNAL.search(text)
                     or RECORD_ID_RE.search(text) or CONTACT.search(text)
                     or API_KEY.search(text) or PRIVATE_HOST_RE.search(text)
                     or _is_private_ipv4(text) or private_identifier(text)):
            return True
    return False


def _query_derived_from_user(query: str, question: str) -> Optional[list[str]]:
    plain_q, plain_u = _plain(query), _plain(question)
    words = set(re.findall(r'[a-z0-9]{2,}', _compact(question).lower()))
    disallowed = [word for word in re.findall(r'[a-z0-9]{2,}', _compact(query).lower())
                  if word not in words and word not in SEARCH_QUALIFIERS]
    cjk_q = "".join(re.findall(r"[\u4e00-\u9fff]", plain_q))
    cjk_u = "".join(re.findall(r"[\u4e00-\u9fff]", plain_u))
    characters = iter(cjk_u)
    if cjk_q and (not cjk_u or any(ch not in characters for ch in cjk_q)):
        disallowed.append(cjk_q)
    return disallowed or None


class PublicQueryError(ValueError):
    """Raised for unsafe inputs / unusable responses."""


def _ensure_public_query(query: str, question: str) -> None:
    if not isinstance(query, str) or not query.strip():
        raise PublicQueryError("搜索词不能为空。")
    if not isinstance(question, str) or not question.strip():
        raise PublicQueryError("缺少用户问题文本。")
    if _forbidden_inputs(query, question):
        raise PublicQueryError("该请求疑似包含内部业务、私密或个人数据，已拒绝发起公网查询。")
    if len(_plain(query)) < SEARCH_MIN_QUERY_LEN or not _plain(question):
        raise PublicQueryError("搜索词太短，无法可靠执行。")
    disallowed = _query_derived_from_user(query, question)
    if disallowed:
        raise PublicQueryError("不允许把不在用户原话中的业务/补充词用于公网查询：" + "、".join(disallowed))


def public_search_query(query: str, question: str) -> str:
    if _forbidden_inputs(query, question):
        raise PublicQueryError('不会向公网发送内部业务、私密或个人数据。')
    requested = re.sub(r'之前的问题：|(?:本次|用户)补充（采用最新条件）：', '', question)
    if _query_derived_from_user(query, requested):
        query = requested
        comparison = re.search(r'^(?:请)?(?:告诉我|讲一讲|比较一下|对比一下)?\s*([^\n]{1,120}?)'
            r'(?:的(?:优劣点|优缺点)|有什么区别|有何区别)', query.strip())
        if comparison:
            query = re.sub(r'和|与|、', ' ', comparison[1]).strip()
            if '最新' in requested:
                query += ' 最新'
    _ensure_public_query(query, requested)
    return query


def _city_syntax_injection(city: str) -> Optional[str]:
    """Reject city values that could smuggle URLs/domains/IPs/@// into a query path."""
    compact = _compact(city)
    if _CITY_DANGER_CHARS.search(compact):
        return "城市名称不能包含 /、\\\\、@ 或 :。"
    if _DOMAINISH_RE.search(compact):
        return "城市名称不能像域名。"
    if _IPV4_RE.search(compact):
        return "城市名称不能是 IP 地址。"
    if "/" in compact or "\\" in compact:
        return "城市名称不能带路径分隔符。"
    return None


def _canonical_city(city: str) -> str:
    text = _compact(city)
    for name, english in KNOWN_CHINESE_CITIES.items():
        if text in {name, name + "市"} or text.lower() == english.lower():
            return name
    return text


def _city_in_question(city: str, question: str) -> bool:
    canonical = _canonical_city(city)
    english = KNOWN_CHINESE_CITIES.get(canonical)
    if english:
        return canonical in _plain(question) or bool(re.search(r"(?<![a-z])" + re.escape(english) + r"(?![a-z])", question, re.I))
    return _compact(city).lower() in _compact(question).lower() or _plain(city) in _plain(question)


def _ensure_public_city(city: str, question: str, previous_question: str = "") -> None:
    if not isinstance(city, str) or not city.strip():
        raise PublicQueryError("城市不能为空。")
    if len(city.strip()) > MAX_CITY_CHARS:
        raise PublicQueryError("城市名称过长。")
    danger = _city_syntax_injection(city)
    if danger:
        raise PublicQueryError(f"城市名称不合法：{danger}")
    if _forbidden_inputs(city, question):
        raise PublicQueryError("该请求疑似包含内部业务、私密或个人数据，已拒绝发起公网天气查询。")
    if re.search(r"机房|厂区|基地|园区|值班|机楼|机柜|HVDC|CRAC|EA\d+", city, re.I):
        raise PublicQueryError("请使用公开城市名称，不会把内部场所信息提交给天气服务。")
    if not _compact(question):
        raise PublicQueryError("缺少用户问题文本。")
    if not _city_in_question(city, question):
        # Only a server-supplied immediately preceding public weather question
        # can supply the city. Never forward history or business results.
        if previous_question and WEATHER_INTENT.search(previous_question) and WEATHER_FOLLOWUP.search(question) \
                and not WEATHER_EXPLANATION.search(question) \
                and not _forbidden_inputs(previous_question) \
                and not any(_city_in_question(name, question) and name != _canonical_city(city) for name in KNOWN_CHINESE_CITIES):
            _ensure_public_city(city, previous_question)
            return
        raise PublicQueryError("城市名称必须出现在用户的原问题中。")


# --- URL / XML / numeric helpers ---------------------------------------
def _is_literal_ip(host: str) -> bool:
    try:
        ipaddress.ip_address(host.strip("[]"))
        return True
    except ValueError:
        return False


def _verified_public_url(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if len(text) > 2000:
        return None
    try:
        parts = urlsplit(text)
    except ValueError:
        return None
    if parts.scheme not in ("http", "https") or not parts.netloc or not parts.hostname:
        return None
    if _is_literal_ip(parts.hostname) or PRIVATE_HOST_RE.search(parts.netloc) \
            or _is_private_ipv4(text) or parts.username or parts.password:
        return None
    try:
        params = parse_qsl(parts.query, keep_blank_values=True, max_num_fields=40)
        if any(re.search(r'token|password|secret|authorization|api.?key|cookie|signature|ticket|cipher', key, re.I)
               or API_KEY.search(value) or CONTACT.search(value) or private_identifier(value)
               or PRIVATE_HOST_RE.search(value) or _is_private_ipv4(value) for key, value in params):
            return None
        return urlunsplit((parts.scheme, parts.netloc, parts.path or "/", urlencode(params), ""))
    except ValueError:
        return None


def _strip_html_markup(value: str) -> str:
    if not value:
        return ""
    text = html.unescape(re.sub(r"<script\b[^>]*>.*?</script>|<style\b[^>]*>.*?</style>",
                                " ", value, flags=re.I | re.S))
    text = html.unescape(re.sub(r"<[^>]+>", " ", text))
    return " ".join(text.split())[:400]


def _rss_text(item: ET.Element, names) -> Optional[str]:
    for name in names:
        for child in item:
            tag = child.tag.rsplit("}", 1)[-1] if isinstance(child.tag, str) else ""
            if tag == name and isinstance(child.text, str):
                return _strip_html_markup(child.text)
    return None


def _parse_rss_results(raw: bytes) -> list[dict[str, Any]]:
    if not raw:
        raise PublicQueryError("收到空响应。")
    if len(raw) > MAX_BYTES:
        raise PublicQueryError("搜索响应超过大小上限。")
    try:
        text = raw.decode("utf-8", errors="replace")
    except (LookupError, UnicodeError):
        raise PublicQueryError("搜索响应不是有效文本。") from None
    if _XML_DANGER_RE.search(text):
        raise PublicQueryError("搜索响应包含DTD或实体声明，已拒绝解析。")
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        raise PublicQueryError("搜索响应不是有效XML。") from None
    results: list[dict[str, Any]] = []
    for item in root.iter():
        tag = item.tag.rsplit("}", 1)[-1] if isinstance(item.tag, str) else ""
        if tag != "item":
            continue
        link = _rss_text(item, ("link", "guid"))
        url = _verified_public_url(link) if link else None
        if url:
            results.append({"url": url,
                            "title": _rss_text(item, ("title",)) or "",
                            "snippet": _rss_text(item, ("description", "summary")) or ""})
    seen, output = set(), []
    for row in results:
        if row["url"] in seen:
            continue
        seen.add(row["url"])
        output.append(row)
        if len(output) >= 100:
            break
    return output


def _parse_mwmbl_results(raw):
    data = _parse_json_checked(raw, '搜索')
    if not isinstance(data, list):
        raise PublicQueryError('备用搜索返回结构无效。')
    rows = []
    for item in data[:100]:
        if not isinstance(item, dict):
            continue
        url = _verified_public_url(item.get('url'))
        if not url:
            continue
        def text_parts(value):
            return _strip_html_markup(value if isinstance(value, str) else ''.join(
                part['value'] for part in value[:40] if isinstance(part, dict) and isinstance(part.get('value'), str))
                if isinstance(value, list) else '')
        rows.append({'url': url, 'title': text_parts(item.get('title')), 'snippet': text_parts(item.get('extract'))})
    return rows


def _relevant_search_results(rows, query):
    site = re.search(r'\bsite:([a-z0-9.-]+)', query, re.I)
    requested_site = site[1].lower().rstrip('.') if site else ''
    english = set(re.findall(r'[a-z0-9]{2,}', _compact(query).lower())) - {
        'the', 'what', 'how', 'is', 'are', 'of', 'and', 'or', 'search'} - SEARCH_QUALIFIERS
    if requested_site:
        english -= set(requested_site.split('.'))
    terms = set(english)
    for word in re.findall(r'[\u4e00-\u9fff]+', _compact(query)):
        terms.update(word[index:index + 2] for index in range(len(word) - 1))
    terms -= {'今天', '明天', '如何', '怎么', '官方', '文档', '教程', '搜索', '联网', '查询', '最新', '介绍', '请问'}
    if requested_site:
        rows = [row for row in rows if (host := (urlsplit(row['url']).hostname or '').rstrip('.')) == requested_site
                or host.endswith('.' + requested_site)]
    if not terms:
        return rows[:MAX_RESULTS]
    ranked = []
    seen = set()
    for row in rows:
        if row['url'] in seen:
            continue
        seen.add(row['url'])
        haystack = _compact(' '.join(row[key] for key in ('title', 'snippet', 'url'))).lower()
        score = sum(term in haystack for term in terms)
        anchors = english or terms
        minimum = len(anchors) if english and len(anchors) <= 3 else max(1, (len(anchors) + 1) // 2)
        if sum(term in haystack for term in anchors) >= minimum:
            ranked.append((score, row))
    return [row for _, row in sorted(ranked, key=lambda pair: pair[0], reverse=True)[:MAX_RESULTS]]


def _fnum(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _fnum_str(value: Any) -> Optional[float]:
    """Numeric parser that also accepts numeric strings (wttr.in returns them)."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            number = float(text)
        except ValueError:
            return None
    else:
        return None
    return number if math.isfinite(number) else None


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres between two coordinate pairs."""
    radius = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    return 2.0 * radius * math.asin(math.sqrt(a))


def _now_iso() -> str:
    return dt.datetime.now(TZ).isoformat(timespec="seconds")


# --- bounded streaming I/O ---------------------------------------------
async def _read_bounded(response: httpx.Response, label: str) -> bytes:
    chunks: list[bytes] = []
    size = 0
    async for chunk in response.aiter_bytes():
        size += len(chunk)
        if size > MAX_BYTES:
            raise PublicQueryError(f"{label}响应超过大小上限。")
        chunks.append(chunk)
    if not chunks:
        raise PublicQueryError("收到空响应。")
    return b"".join(chunks)


def _parse_json_checked(raw: bytes, label: str) -> Any:
    try:
        text = raw.decode("utf-8")
        return json.loads(text)
    except UnicodeDecodeError:
        raise PublicQueryError(f"{label}响应不是有效UTF-8文本。") from None
    except (json.JSONDecodeError, ValueError):
        raise PublicQueryError(f"{label}返回的不是有效JSON。") from None


def _build_forecast(data: Any) -> Optional[dict[str, Any]]:
    if not isinstance(data, dict):
        return None
    timezone = data.get("timezone")
    if timezone is not None and timezone != SHANGHAI_TZ:
        return None
    for units_key, checks in (("current_units", (("temperature_2m", "°C"),
                                                 ("wind_speed_10m", "km/h"))),
                              ("daily_units", (("temperature_2m_max", "°C"),
                                               ("precipitation_sum", "mm")))):
        units = data.get(units_key)
        if isinstance(units, dict) and any(units.get(k) != v for k, v in checks):
            return None
    current, daily = data.get("current"), data.get("daily")
    if not isinstance(current, dict) or not isinstance(daily, dict):
        return None
    current_time = current.get("time")
    temp, code = _fnum(current.get("temperature_2m")), _fnum(current.get("weather_code"))
    wind, hum = _fnum(current.get("wind_speed_10m")), _fnum(current.get("relative_humidity_2m"))
    if not isinstance(current_time, str) or not _DATETIME_RE.match(current_time) \
            or None in (temp, code, wind, hum):
        return None
    times, hi, lo, rain, prob = (daily.get(k) for k in
                                 ("time", "temperature_2m_max", "temperature_2m_min",
                                  "precipitation_sum", "precipitation_probability_max"))
    if not all(isinstance(x, list) for x in (times, hi, lo, rain, prob)) \
            or len({len(times), len(hi), len(lo), len(rain), len(prob)}) != 1:
        return None
    parsed = []
    for day, h, l, r, p in zip(times, hi, lo, rain, prob):
        if not isinstance(day, str) or not _DATE_RE.match(day):
            return None
        try:
            dt.date.fromisoformat(day)
        except ValueError:
            return None
        hf, lf = _fnum(h), _fnum(l)
        if hf is None or lf is None:
            return None
        parsed.append({"date": day, "max": hf, "min": lf,
                       "rain": _fnum(r), "precip_probability": _fnum(p)})
    if not parsed:
        return None
    return {"current_date": current_time,
            "current": {"temperature": temp, "weather_code": int(code),
                        "wind_speed_kmh": wind, "relative_humidity_pct": hum},
            "daily": parsed}


# --- wttr.in keyless public weather parsing ------------------------------
_LOCAL_OBS_FORMATS = (
    "%Y-%m-%d %I:%M %p",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M",
)


def _parse_local_obs_dt(value: Any) -> Optional[dt.datetime]:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    for fmt in _LOCAL_OBS_FORMATS:
        try:
            parsed = dt.datetime.strptime(text, fmt)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=TZ)
        return parsed
    return None


def _wttr_english_city(city: str) -> tuple[str, bool]:
    """Return (query_city, is_known_chinese). Known cities map to stable English names."""
    compact = _compact(city)
    english = KNOWN_CHINESE_CITIES.get(compact)
    if english:
        return english, True
    return compact, False


def _leaf_value(node: Any) -> str:
    """Extract the ``value`` text of a wttr.in localized leaf (``[{"value": "X"}]``)."""
    if isinstance(node, list) and node and isinstance(node[0], dict):
        return _compact(node[0].get("value", ""))
    return ""


def _wttr_resolve(query_city: str, zh_city: str, is_known: bool,
                  nearest: Any) -> Optional[dict[str, Any]]:
    """Validate the resolved area/country/coordinates and label it honestly.

    Returns a resolution dict (``ok`` + honest label) or None when the resolved
    area must not be claimed. Keyless wttr.in lacks Nantong in its place-name
    database and resolves it to a nearby public station (e.g. Dexing), so Nantong
    is accepted only when that station is inside ``NANTONG_RADIUS_KM`` of the
    known city centre and is reported as China. Other known cities require an
    exact resolved area name; unknown cities accept the query token appearing in
    the resolved area/region.
    """
    if not isinstance(nearest, list) or not nearest:
        return None
    area = nearest[0]
    if not isinstance(area, dict):
        return None

    area_name = _leaf_value(area.get("areaName"))
    country = _leaf_value(area.get("country"))
    region = _leaf_value(area.get("region"))
    lat = _fnum_str(area.get("latitude"))
    lon = _fnum_str(area.get("longitude"))

    # Nantong: accept a nearby China station within a safe radius, never a far or
    # foreign one, and label the actual resolved station (not an exact match we
    # cannot verify).
    if _plain(query_city) == "nantong":
        if country.lower() != "china":
            return None
        if lat is None or lon is None or not (-90.0 <= lat <= 90.0) \
                or not (-180.0 <= lon <= 180.0):
            return None
        if _haversine_km(lat, lon, NANTONG_LAT, NANTONG_LON) > NANTONG_RADIUS_KM:
            return None
        label = area_name or query_city
        return {"ok": True,
                "resolved_city": label,
                "country": country, "region": region,
                "latitude": lat, "longitude": lon,
                "mode": "nearby-coordinates",
                "note": (f"数据来自距南通市中心 {NANTONG_RADIUS_KM:.0f} 公里以内的附近公共气象站，"
                         "仅供附近区域参考，并非南通城区的精确匹配。")}

    combined = _compact(f"{area_name} {region} {country}").lower()
    q_token = _plain(query_city)
    # City query token must appear in the resolved area/region.
    if not q_token or (q_token not in combined and query_city.lower() not in combined):
        return None
    # Known Chinese cities must resolve deterministically to the expected area name.
    if is_known and _compact(area_name).lower() != _compact(query_city).lower():
        return None
    return {"ok": True,
            "resolved_city": area_name or query_city,
            "country": country, "region": region,
            "latitude": lat, "longitude": lon,
            "mode": "exact-name" if is_known else "area-match",
            "note": ""}


def _wttr_desc(condition: dict) -> str:
    text = _leaf_value(condition.get('lang_zh')) or _leaf_value(condition.get('weatherDesc'))
    if not text or re.search(r'[\u4e00-\u9fff]', text):
        return text
    return {'sunny': '晴', 'clear': '晴', 'partly cloudy': '晴间多云', 'cloudy': '多云',
        'overcast': '阴', 'mist': '薄雾', 'fog': '雾', 'light rain': '小雨',
        'moderate rain': '中雨', 'heavy rain': '大雨', 'light snow': '小雪',
        'moderate snow': '中雪', 'heavy snow': '大雪'}.get(text.casefold(), '天气描述待核对')


def _build_wttr_payload(data: Any, zh_city: str, english_city: str,
                        is_known: bool, queried_at: str) -> Optional[dict[str, Any]]:
    """Parse wttr.in ``format=j1`` payload into a bounded structured result.

    The daily forecast (dated Beijing-today, consecutive) is accepted even when
    the current-observation timestamp is unavailable (keyless wttr.in frequently
    omits ``localObsDateTime`` and only offers the undated ``observation_time``
    time-of-day). In that case the current observation is marked undated and no
    freshness timestamp is invented. Returns None (caller marks unavailable)
    instead of fabricating data when city resolution or the forecast is invalid.
    """
    if isinstance(data, list):
        if len(data) != 1:
            return None
        data = data[0]
    if not isinstance(data, dict):
        return None
    resolved = _wttr_resolve(english_city, zh_city, is_known,
                             data.get("nearest_area"))
    if resolved is None:
        return None
    current_condition = data.get("current_condition")
    weather_list = data.get("weather")
    if not isinstance(current_condition, list) or not current_condition:
        return None
    cc = current_condition[0]
    if not isinstance(cc, dict):
        return None
    temp_c = _fnum_str(cc.get("temp_C"))
    humidity = _fnum_str(cc.get("humidity"))
    wind = _fnum_str(cc.get("windspeedKmph"))
    if None in (temp_c, humidity, wind):
        return None
    if not (-90.0 <= temp_c <= 60.0 and 0.0 <= humidity <= 100.0
            and 0.0 <= wind <= 500.0):
        return None

    # Observation-time semantics: ``localObsDateTime`` (when present) is the only
    # dated timestamp we can verify for freshness. ``observation_time`` is just a
    # local time-of-day (no date) and cannot prove the observation is recent, so
    # without ``localObsDateTime`` the current condition is honestly marked
    # undated (no invented freshness). A reliable dated timestamp must be recent.
    now = dt.datetime.now(TZ)
    obs_dt = _parse_local_obs_dt(cc.get("localObsDateTime"))
    obs_iso = None
    obs_reason = "undated"
    if obs_dt is not None:
        age = (now - obs_dt).total_seconds()
        if age < -MAX_OBS_FUTURE_SECONDS or age > MAX_OBS_AGE_SECONDS:
            obs_reason = "stale-or-future"
        else:
            obs_iso = obs_dt.isoformat(timespec="seconds")
    desc = _wttr_desc(cc)

    daily = []
    today = now.date()
    if not isinstance(weather_list, list) or not weather_list:
        return None
    for i, day in enumerate(weather_list[:FORECAST_DAYS]):
        if not isinstance(day, dict):
            return None
        date_s = day.get("date")
        if not isinstance(date_s, str) or not _DATE_RE.match(date_s):
            return None
        try:
            parsed_date = dt.date.fromisoformat(date_s)
        except ValueError:
            return None
        # First forecast day must be Beijing-today; consecutive after that.
        if (i == 0 and parsed_date != today) or (i > 0 and
                                                 parsed_date != today + dt.timedelta(days=i)):
            return None
        mx, mn = _fnum_str(day.get("maxtempC")), _fnum_str(day.get("mintempC"))
        if mx is None or mn is None or not (-90.0 <= mx <= 60.0 and -90.0 <= mn <= 60.0):
            return None
        # Daily rainfall probability = maximum of all valid hourly probabilities.
        chance = None
        hourly = day.get("hourly")
        if isinstance(hourly, list):
            probs = []
            for h in hourly:
                if not isinstance(h, dict):
                    continue
                p = _fnum_str(h.get("chanceofrain"))
                if p is not None and 0.0 <= p <= 100.0:
                    probs.append(p)
            if probs:
                chance = max(probs)
        midday = next((h for h in hourly or [] if isinstance(h, dict) and str(h.get('time')) == '1200'), {})
        daily.append({"date": date_s, "max": mx, "min": mn, "weather_desc": _wttr_desc(midday),
                      "chanceofrain": chance})
    if not daily:
        return None
    observation_unavailable = obs_iso is None
    observation_note = (f"当前观测时间不可靠（{obs_reason}），未提供可验证的新鲜实测时间。"
                        if observation_unavailable else "当前观测时间有效且新鲜。")
    return {"current_date": obs_iso,
            "current": {"temperature": temp_c, "weather_desc": desc,
                        "wind_speed_kmh": wind, "relative_humidity_pct": humidity} if obs_iso else None,
            "daily": daily, "observation_time": obs_iso,
            "observation_unavailable": observation_unavailable,
            "observation_note": observation_note,
            "resolved": resolved}


# --- PublicSources -------------------------------------------------------
class PublicSources:
    """Read-only public query helpers; ``client_factory`` may be injected."""

    def __init__(self, *, client_factory=None, timeout: float = DEFAULT_TIMEOUT) -> None:
        self._timeout = max(1.0, min(float(timeout), MAX_NETWORK_SECONDS))
        self._injected = client_factory is not None
        self._client_factory = client_factory or (lambda: httpx.AsyncClient(
            verify=_verified_tls_context(), timeout=httpx.Timeout(self._timeout), follow_redirects=False))
        self._pool = None
        self._pool_lock = asyncio.Lock()
        self._pages = {}

    def prepare(self):
        if not self._injected:
            _verified_tls_context()

    @asynccontextmanager
    async def _client(self):
        if not self._injected:
            async with self._pool_lock:
                if self._pool is None:
                    await asyncio.to_thread(self.prepare)
                    self._pool = self._client_factory()
            yield self._pool
            return
        client = await asyncio.to_thread(self._client_factory)
        try:
            yield client
        finally:
            close = getattr(client, "aclose", None)
            if callable(close):
                await close()

    async def close(self):
        async with self._pool_lock:
            if self._pool is not None:
                await self._pool.aclose()
                self._pool = None
            self._pages.clear()

    def _unavailable(self, source: str, url: str, queried_at: str,
                     city: Optional[str] = None, note: str = "") -> dict:
        payload = {"ok": False, "source": source, "sourceURL": url,
                   "queried_at": queried_at, "unavailable": True,
                   "empty": False, "count": None, "note": note}
        if city is not None:
            payload.update(city=city, timezone=SHANGHAI_TZ)
        return payload

    async def search(self, query: str, question: str) -> dict[str, Any]:
        _ensure_public_query(query, question)
        queried_at = _now_iso()
        deadline = asyncio.get_running_loop().time() + self._timeout
        failures, empty, navigation = [], None, {}
        attempts = [(BING_RSS_URL, query, _parse_rss_results), (MWMBL_URL, query, _parse_mwmbl_results)]
        site = re.search(r'\bsite:([a-z0-9.-]+)', query, re.I)
        if site:
            subject = [word for word in re.findall(r'[a-z0-9]{2,}', re.sub(r'\bsite:[a-z0-9.-]+', '', query, flags=re.I).lower())
                       if word not in SEARCH_QUALIFIERS]
            publisher_words = set(site[1].lower().split('.')) - SEARCH_QUALIFIERS
            original_subject = next((word for word in re.findall(r'[a-z0-9]{2,}', question.lower())
                                     if word in publisher_words), None)
            if len(subject) > 1 or original_subject and original_subject not in subject:
                shorter = f'site:{site[1]} {original_subject or subject[0]} documentation'
                _ensure_public_query(shorter, question)
                attempts.append((MWMBL_URL, shorter, _parse_mwmbl_results))
        async def fetch(attempt, delay=0):
            if delay:
                await asyncio.sleep(delay)
            template, search_query, parse = attempt
            url = template.format(q=quote(search_query))
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise PublicQueryError('搜索总时限已到。')
            async with asyncio.timeout(min(8, remaining)):
                async with self._client() as client:
                    async with client.stream('GET', url, timeout=self._timeout) as response:
                        if response.status_code != 200:
                            raise PublicQueryError(f'搜索服务返回 HTTP {response.status_code}。')
                        return url, parse(await _read_bounded(response, '搜索'))

        # Hedge slow primary reads; broaden only after both precise sources fail.
        for batch in (attempts[:2], attempts[2:]):
            tasks = [asyncio.create_task(fetch(attempt, .25 if index == 1 else 0))
                     for index, attempt in enumerate(batch)]
            try:
                for completed in asyncio.as_completed(tasks):
                    try:
                        url, rows = await completed
                        # Broader retrieval must still satisfy the original topic/site.
                        results = _relevant_search_results(rows, query)
                        if site and not results:
                            from .lighthouse_public_page import page_url
                            for row in rows:
                                host = (urlsplit(row['url']).hostname or '').lower().rstrip('.')
                                requested = site[1].lower().rstrip('.')
                                link = page_url(row['url'])
                                if not link and row['url'].startswith('http://'):
                                    link = page_url('https://' + row['url'][7:])
                                if link and (host == requested or host.endswith('.' + requested)):
                                    navigation.setdefault(link, {'title': row['title'], 'link': link})
                        value = {'ok': True, 'source': 'public_search', 'sourceURL': url,
                                 'queried_at': queried_at, 'unavailable': False,
                                 'empty': not results, 'count': len(results), 'results': results}
                        if results:
                            return value
                        indexes = [row for row in navigation.values() if re.search(r'/contents(?:\.html)?$', row['link'])]
                        if indexes:
                            return {**self._unavailable('public_search', url, queried_at),
                                'navigation': indexes[:3],
                                'note': '搜索取得官方目录链接但尚未读取专题正文；可读取目录定位专题，不将目录当作专题依据。'}
                        if not rows:
                            empty = value
                        else:
                            failures.append('搜索返回了不相关内容或站点不符，未用作回答依据；可用原问题中的较短关键词重查。')
                    except PublicQueryError as exc:
                        failures.append(str(exc))
                    except (httpx.HTTPError, OSError, ValueError, TypeError, KeyError, IndexError, asyncio.TimeoutError) as exc:
                        failures.append(f'搜索请求失败：{type(exc).__name__}')
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        if empty is not None:
            value = empty
        else:
            value = self._unavailable('public_search', BING_RSS_URL.format(q=quote(query)), queried_at,
                                     note='；'.join(dict.fromkeys(failures)))
        if navigation:
            candidates = sorted(navigation.values(), key=lambda row: not re.search(r'/contents(?:\.html)?$', row['link']))
            value.update(navigation=candidates[:3],
                note='未命中特定主题；这些仅为官方站点导航候选，可用 public_page 读取并定位正文，不是专题内容依据。')
        return value

    async def page(self, url: str, question: str, *, refresh: bool = False) -> dict[str, Any]:
        from .lighthouse_public_page import page_url, read_page
        try:
            target = page_url(url)
            if not target or not question.strip() or _forbidden_inputs(question):
                raise PublicQueryError('只能读取公开问题中的安全 HTTPS 页面，不能发送内部业务或私密信息。')
            now = asyncio.get_running_loop().time()
            cached = self._pages.get(target)
            if not refresh and cached and now - cached[0] < cached[1]['cache_seconds']:
                return {**deepcopy(cached[1]), 'cached': True, 'cache_age_seconds': round(now - cached[0], 1)}
            self._pages.pop(target, None)
            value = await read_page(target, question, dns_client=self._client, timeout=min(self._timeout, 10))
            if value.get('ok') and value.get('cache_seconds', 0) > 0:
                if len(self._pages) >= 64:
                    self._pages.pop(next(iter(self._pages)))
                self._pages[target] = (asyncio.get_running_loop().time(), deepcopy(value))
            return value
        except (PublicQueryError, httpx.HTTPError, OSError, ValueError, LookupError, asyncio.TimeoutError) as exc:
            value = self._unavailable('public_page', '', _now_iso(),
                note=str(exc) if isinstance(exc, PublicQueryError) else '公开页面读取未完成，不能据此回答内容。')
            value['retryable'] = not isinstance(exc, PublicQueryError) and isinstance(exc,
                (httpx.TransportError, OSError, asyncio.TimeoutError))
            return value

    async def weather(self, city: str, question: str, *, previous_question: str = "") -> dict[str, Any]:
        _ensure_public_city(city, question, previous_question)
        city = _canonical_city(city)
        await asyncio.to_thread(self.prepare)
        queried_at = _now_iso()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._timeout
        reasons = []

        async def bounded(awaitable, seconds):
            return await asyncio.wait_for(awaitable, timeout=max(.01, min(seconds, deadline - loop.time())))

        try:
            if os.environ.get(CUSTOM_KEY_ENV):
                try:
                    commercial = await bounded(self._weather_openmeteo(city, queried_at), 8)
                    if commercial.get("ok"):
                        return commercial
                    reasons.append(commercial.get("note") or "商业天气服务暂不可用。")
                except (asyncio.TimeoutError, PublicQueryError, httpx.HTTPError, OSError):
                    reasons.append("商业天气服务请求未完成。")
            # The Chinese site resolves Nantong exactly; wttr may use a nearby station.
            if city in CHINA_WEATHER_CITIES:
                try:
                    china = await bounded(self._weather_china(city, queried_at), 5)
                    if china.get('ok'):
                        return china
                    reasons.append(china.get('note') or '中国天气网暂不可用。')
                except (asyncio.TimeoutError, PublicQueryError, httpx.HTTPError, OSError):
                    reasons.append('中国天气网请求未完成。')
            try:
                wttr = await bounded(self._weather_wttr(city, queried_at), 8)
            except asyncio.TimeoutError:
                wttr = {"ok": False}
            if wttr.get("ok"):
                return wttr
            reasons.append(wttr.get("note") or "wttr.in 天气请求未完成。")
            # Structured keyless tool unavailable -> honest public search evidence.
            try:
                fallback = await bounded(self._weather_fallback(city, question, queried_at), 10)
                if not fallback.get("ok"):
                    fallback['note'] = '；'.join([*reasons, fallback.get('note') or '备用天气搜索暂不可用。'])
                return fallback
            except asyncio.TimeoutError:
                return self._unavailable("public_search_weather", BING_RSS_URL.format(q=quote(f"{city}天气")),
                                         queried_at, city=city, note="天气服务与备用搜索暂不可用，请稍后重试；未虚构天气。")
        except PublicQueryError as exc:
            return self._unavailable("public_weather", CUSTOM_METEO_FORECAST,
                                     queried_at, city=city, note=str(exc))
        except (ValueError, TypeError, KeyError, IndexError, httpx.HTTPError, OSError) as exc:
            return self._unavailable("public_weather", CUSTOM_METEO_FORECAST,
                                     queried_at, city=city,
                                     note=f"天气数据异常：{type(exc).__name__}")

    async def _weather_openmeteo(self, city: str, queried_at: str) -> dict:
        lat, lon, name = await self._geo_city(city)
        if name is None:
            return self._unavailable("public_weather", CUSTOM_METEO_FORECAST,
                                     queried_at, city=city,
                                     note="找不到该城市，或同名城市不唯一，无法确定天气。")
        forecast = await self._forecast(lat, lon)
        if forecast is None:
            return self._unavailable("public_weather", CUSTOM_METEO_FORECAST,
                                     queried_at, city=city,
                                     note="天气预报返回结构无效，未虚构数据。")
        return {"ok": True, "source": "public_weather", "sourceURL": CUSTOM_METEO_FORECAST,
                "queried_at": queried_at, "unavailable": False, "empty": False,
                "count": len(forecast["daily"]), "city": name, "timezone": SHANGHAI_TZ,
                "current_date": forecast["current_date"], "current": forecast["current"],
                "daily": forecast["daily"], "temperature_unit": "°C", "wind_unit": "km/h",
                "capability": {"structured_weather": True, "provider": "open-meteo-commercial"}}

    async def _weather_wttr(self, city: str, queried_at: str) -> dict:
        zh_city = city
        english_city, is_known = _wttr_english_city(city)
        url = WTTR_URL.format(city=quote(english_city))
        try:
            data = await self._client_get(url, None, "天气")
        except (PublicQueryError, httpx.HTTPError, OSError) as exc:
            return self._unavailable("publicweathercommunity", url, queried_at,
                                     city=zh_city,
                                     note=f"wttr.in 结构化天气不可用：{type(exc).__name__}")
        payload = _build_wttr_payload(data, zh_city, english_city, is_known, queried_at)
        if payload is None:
            return self._unavailable("publicweathercommunity", url, queried_at,
                                     city=zh_city,
                                     note="wttr.in 返回的结构/城市/时间无效或过时，未虚构数据。")
        resolved = payload["resolved"]
        return {"ok": True, "source": WTTR_LABEL, "sourceURL": url,
                "queried_at": queried_at, "unavailable": False, "empty": False,
                "count": len(payload["daily"]), "city": zh_city,
                "resolved_city": resolved["resolved_city"],
                "requested_city": english_city,
                "resolved_country": resolved["country"],
                "resolved_latitude": resolved["latitude"],
                "resolved_longitude": resolved["longitude"],
                "resolution_mode": resolved["mode"],
                "resolution_note": resolved.get("note", ""),
                "timezone": SHANGHAI_TZ,
                "current_date": payload["current_date"], "current": payload["current"],
                "daily": payload["daily"], "observation_time": payload["observation_time"],
                "observation_unavailable": payload["observation_unavailable"],
                "observation_note": payload["observation_note"],
                "temperature_unit": "°C", "wind_unit": "km/h",
                "capability": {"structured_weather": True,
                               "provider": "wttr.in-keyless",
                               "label": WTTR_LABEL}}

    async def _weather_china(self, city, queried_at):
        url = CHINA_WEATHER_URL.format(code=CHINA_WEATHER_CITIES[city])
        async with self._client() as client:
            async with client.stream('GET', url, timeout=self._timeout) as response:
                if response.status_code != 200:
                    raise PublicQueryError('中国天气网暂不可用。')
                daily = _china_forecast(await _read_bounded(response, '天气'), city, queried_at)
        if not daily:
            return self._unavailable('china_weather', url, queried_at, city=city, note='中国天气网页日期或预报结构无效。')
        return {'ok': True, 'source': '中国天气网', 'sourceURL': url, 'queried_at': queried_at,
                'city': city, 'daily': daily, 'count': len(daily), 'timezone': SHANGHAI_TZ,
                'current': None, 'observation_time': None, 'observation_unavailable': True,
                'observation_note': '该来源仅提供天气预报，不提供当前实测气温。',
                'capability': {'structured_weather': True, 'provider': 'china-weather'}}

    async def _weather_fallback(self, city: str, question: str, queried_at: str) -> dict:
        # The city was already validated against the user's public question.
        # This fixed weather suffix contains no history or internal data.
        evidence = await self.search(f"{city}天气", f"{city}天气")
        base = {"source": "public_search_weather", "queried_at": queried_at,
                "city": city, "timezone": SHANGHAI_TZ}
        if not evidence.get("ok"):
            return dict(base, ok=False, unavailable=True, empty=False, count=None,
                        sourceURL=evidence.get("sourceURL", BING_RSS_URL.format(q=quote(f"{city}天气"))),
                        note=evidence.get("note", "天气搜索不可用。"))
        if evidence.get('empty'):
            return self._unavailable('public_search_weather', evidence['sourceURL'], queried_at, city=city,
                                     note='天气来源未提供可核验的预报，备用搜索也没有匹配结果，未虚构天气。')
        return dict(base, ok=True, unavailable=False, empty=evidence["empty"],
                    sourceURL=evidence["sourceURL"], count=evidence["count"],
                    results=evidence["results"],
                    note=f"结构化天气来源不可用，以上为公开网页搜索证据，不含结构化预报。",
                    capability={"structured_weather": False, "config": CUSTOM_KEY_ENV})

    async def _client_get(self, url: str, params, label: str) -> Any:
        async with self._client() as client:
            async with client.stream("GET", url, params=params, timeout=self._timeout) as response:
                if response.status_code != 200:
                    raise PublicQueryError(f"{label}服务返回 HTTP {response.status_code}。")
                raw = await _read_bounded(response, label)
        return _parse_json_checked(raw, label)

    async def _geo_city(self, city: str) -> tuple[float, float, Optional[str]]:
        params = {"name": city, "count": 8, "language": "zh", "format": "json",
                  "apikey": os.environ.get(CUSTOM_KEY_ENV)}
        data = await self._client_get(CUSTOM_METEO_GEO, params, "地理编码")
        if not isinstance(data, dict):
            return 0.0, 0.0, None
        results = data.get("results")
        results = results if isinstance(results, list) else []
        exact = [r for r in results if isinstance(r, dict) and _compact(r.get("name")) == city]
        distinct = {}
        for entry in (exact or [r for r in results if isinstance(r, dict)]):
            lat, lon = _fnum(entry.get("latitude")), _fnum(entry.get("longitude"))
            if lat is not None and lon is not None and -90.0 <= lat <= 90.0 \
                    and -180.0 <= lon <= 180.0:
                distinct[(lat, lon)] = entry
        if len(distinct) != 1:
            return 0.0, 0.0, None
        (lat, lon), entry = next(iter(distinct.items()))
        return lat, lon, _compact(entry.get("name")) or city

    async def _forecast(self, lat: float, lon: float) -> Optional[dict[str, Any]]:
        params = {"latitude": lat, "longitude": lon,
                  "current": "temperature_2m,weather_code,wind_speed_10m,relative_humidity_2m",
                  "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,precipitation_probability_max",
                  "timezone": SHANGHAI_TZ, "forecast_days": FORECAST_DAYS,
                  "apikey": os.environ.get(CUSTOM_KEY_ENV)}
        return _build_forecast(await self._client_get(CUSTOM_METEO_FORECAST, params, "天气预报"))

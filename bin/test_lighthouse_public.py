"""Isolated tests for the bounded public-query service (lighthouse_public.PublicSources).

These tests never touch the real internet by default.  A ``httpx.MockTransport``
(or a factory that raises when contacted) is injected so every scenario is checked
without external requests.  Network-dependent live smoke checks are gated behind the
``LIGHTHOUSE_LIVE_TESTS`` environment variable and only use generic public queries
(e.g. ``南通天气`` / official public docs style terms) -- never business data.

Behaviour covered:
  1. No network when the user question/internal keywords are business/private.
  2. No network when the model injects query tokens absent from the user text.
  3. Unknown city / city-not-in-question handling.
  4. Invalid XML / DTD / size-limit rejection, and standard XML entities accepted.
  5. URL sanitisation; redirects are unavailable; unavailable count is None.
  6. Weather: commercial Open-Meteo path (env key), keyless wttr.in JSON API
     (Nantong five-style fixtures, freshness/city-resolution gating), or public
     search evidence fallback; failures are unavailable, never fabricated.
  7. Empty results are distinct from unavailable results.
"""
import datetime as dt
import asyncio
import os
import sys
import ssl
import hashlib
import unittest
from unittest.mock import AsyncMock, patch
from pathlib import Path
from html import escape
from urllib.parse import urlencode

import httpx
import certifi

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lan_bitable_template_portal.lighthouse_public import (
    TZ,
    PublicQueryError,
    PublicSources,
)
from upload_event_module.services import http_client as tls_policy

CUSTOM_KEY_ENV = "OPEN_METEO_CUSTOMER_KEY"


class TLSContextPolicyTests(unittest.TestCase):
    def test_model_search_rephrasing_falls_back_to_user_words_without_exfiltration(self):
        from openclaw_service.assistant.lighthouse_public import public_search_query
        question = '告诉我deepseek和豆包有什么区别，联网给我最新版'
        self.assertEqual(public_search_query('DeepSeek Doubao latest model comparison 2026', question), 'deepseek 豆包 最新')
        self.assertEqual(public_search_query('Python Java language comparison', '告诉我Python和Java有什么区别'), 'Python Java')
        self.assertEqual(public_search_query('DeepSeek Doubao model comparison', '讲一讲deepseek和豆包的优劣点'), 'deepseek 豆包')
        self.assertEqual(public_search_query('DeepSeek Doubao latest model comparison',
            '之前的问题：讲一讲deepseek和豆包的优劣点\n本次补充（采用最新条件）：需要联网'), 'deepseek 豆包')
        for query in ('deepseek 张三身份证号码', 'deepseek recAbc12345678', 'deepseek 内部台账',
                      'deepseek sk-abcdefghijklmn'):
            with self.subTest(query=query), self.assertRaises(PublicQueryError):
                public_search_query(query, question)

    def test_public_and_model_policies_cache_verified_contexts_separately(self):
        from openclaw_service.assistant import lighthouse_public as public
        contexts = {flag: ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT) for flag in (False, True)}
        with patch.dict(os.environ, {'SSL_CERT_FILE': 'public-only-ca.pem'}, clear=True), \
                patch.object(tls_policy, '_tls_contexts', {}), patch.object(httpx, 'create_ssl_context',
                side_effect=lambda **kwargs: contexts[kwargs['trust_env']]) as create, \
                patch.object(ssl, 'create_default_context', return_value=contexts[False]) as default:
            self.assertIs(public._verified_tls_context(), contexts[True])
            self.assertIs(public._verified_tls_context(trust_env=False), contexts[False])
            self.assertIs(public._verified_tls_context(), contexts[True])
            self.assertIs(public._verified_tls_context(trust_env=False), contexts[False])
            self.assertEqual(create.call_count, 1)
            self.assertEqual(default.call_count, 1)
            for context in contexts.values():
                self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
                self.assertTrue(context.check_hostname)

    def test_identical_default_ca_is_loaded_once_for_public_and_model_clients(self):
        from openclaw_service.assistant import lighthouse_public as public
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        with patch.dict(os.environ, {}, clear=True), patch.object(tls_policy, '_tls_contexts', {}), \
                patch.object(httpx, 'create_ssl_context', return_value=context) as create, \
                patch.object(ssl, 'create_default_context', return_value=context) as default:
            self.assertIs(public._verified_tls_context(), context)
            self.assertIs(public._verified_tls_context(trust_env=False), context)
            create.assert_not_called()
            self.assertEqual(default.call_count, 1)
            os.environ['SSL_CERT_DIR'] = 'public-ca-directory'
            public._verified_tls_context()
            create.assert_called_with(trust_env=True)
            self.assertEqual(create.call_count, 1)
            public._verified_tls_context(trust_env=False)
            self.assertEqual(create.call_count, 1)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(context.check_hostname)

    def test_default_ca_uses_exact_bundle_with_standard_tls_security(self):
        from openclaw_service.assistant import lighthouse_public as public
        with patch.dict(os.environ, {}, clear=True), patch.object(tls_policy, '_tls_contexts', {}), \
                patch.object(ssl, 'create_default_context', wraps=ssl.create_default_context) as create:
            context = public._verified_tls_context()
            self.assertEqual(create.call_count, 1)
            self.assertEqual(set(create.call_args.kwargs), {'cadata'})
            self.assertEqual(hashlib.sha256(create.call_args.kwargs['cadata'].encode()).digest(),
                             hashlib.sha256(certifi.contents().encode()).digest())
            defaults = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            defaults.verify_flags |= ssl.VERIFY_X509_PARTIAL_CHAIN | ssl.VERIFY_X509_STRICT
            self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
            self.assertTrue(context.check_hostname)
            self.assertEqual(context.verify_flags, defaults.verify_flags)
            self.assertEqual(context.minimum_version, defaults.minimum_version)
            self.assertGreater(context.cert_store_stats()['x509_ca'], 100)

    def test_invalid_default_bundle_fails_closed_without_caching(self):
        from openclaw_service.assistant import lighthouse_public as public
        with patch.dict(os.environ, {}, clear=True), patch.object(tls_policy, '_tls_contexts', {}), \
                patch.object(certifi, 'contents', return_value='not a CA certificate'):
            with self.assertRaises(ssl.SSLError):
                public._verified_tls_context()
            self.assertEqual(tls_policy._tls_contexts, {})

XML_DTD_PAYLOAD = (
    '<?xml version="1.0"?>\n'
    '<!DOCTYPE rss [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>\n'
    '<rss><channel><item><title>bad</title><link>http://example.com/x</link>'
    '<description>&xxe;</description></item></channel></rss>'
)


def _rss(items):
    parts = [
        "<item>"
        f"<title>{escape(it.get('title', ''))}</title>"
        f"<link>{escape(it.get('link', ''))}</link>"
        f"<description>{escape(it.get('desc', ''))}</description>"
        "</item>"
        for it in items
    ]
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<rss version="2.0"><channel>'
        + "".join(parts)
        + "</channel></rss>"
    )


def _geo_response(name, coords, country="中国", admin1=""):
    results = []
    for lat, lon in coords:
        results.append({
            "id": 1,
            "name": name,
            "latitude": lat,
            "longitude": lon,
            "country": country,
            "admin1": admin1 or name,
            "admin2": name,
        })
    return {"results": results}


def _forecast_payload(days=3):
    return {
        "current": {
            "time": "2026-10-03T10:00",
            "weather_code": 2,
            "temperature_2m": 23.5,
            "wind_speed_10m": 12.0,
            "relative_humidity_2m": 64,
        },
        "daily": {
            "time": [f"2026-10-0{3 + i}" for i in range(days)],
            "temperature_2m_max": [26.0, 24.0, 22.0][:days],
            "temperature_2m_min": [15.0, 14.0, 13.0][:days],
            "precipitation_sum": [0.0, 1.2, 0.0][:days],
            "precipitation_probability_max": [20, 80, 10][:days],
        },
    }


def _wttr_obs_str(offset_seconds=0):
    return (dt.datetime.now(TZ) + dt.timedelta(seconds=offset_seconds)).strftime(
        "%Y-%m-%d %I:%M %p")


def _wttr_nantong_payload(*, local_obs="__dated__", forecast_dates=None, hourly_probs=None,
                          nearest_area="Nantong", country="China", region="Jiangsu",
                          lat="31.977", lon="120.881", temp="23.5", humidity="64",
                          wind="12.0", lang_zh="多云", weather_desc="Overcast",
                          include_observation_time=False):
    """Build a wttr.in j1 fixture for the Nantong family of lookups.

    ``local_obs`` is the dated ``localObsDateTime`` value. The default yields a
    recent dated timestamp; pass ``None`` to omit it (the undated real-world
    case). ``include_observation_time`` adds the undated time-of-day field that
    keyless responses carry instead of a date. ``hourly_probs`` is a list of
    per-day lists of hourly ``chanceofrain`` values; when omitted each day has a
    single value so ``chanceofrain`` is deterministic. Coordinates default to the
    real wttr.in resolved nearby station location.
    """
    if local_obs == "__dated__":
        local_obs = _wttr_obs_str()  # a recent dated timestamp by default
    today = dt.datetime.now(TZ).date()
    if forecast_dates is None:
        forecast_dates = [(today + dt.timedelta(days=i)).isoformat() for i in range(3)]
    if hourly_probs is None:
        hourly_probs = [["30"], ["10"], ["0"]]
    cc = {
        "temp_C": temp, "humidity": humidity, "windspeedKmph": wind,
        "weatherDesc": [{"value": weather_desc}],
        "lang_zh": [{"value": lang_zh}],
    }
    if local_obs is not None:
        cc["localObsDateTime"] = local_obs
    if include_observation_time:
        cc["observation_time"] = "10:18 AM"
    return [{
        "current_condition": [cc],
        "nearest_area": [{
            "areaName": [{"value": nearest_area}],
            "country": [{"value": country}],
            "region": [{"value": region}],
            "latitude": lat,
            "longitude": lon,
        }],
        "weather": [
            {"date": forecast_dates[0], "maxtempC": "26", "mintempC": "15",
             "hourly": [{"time": "0", "chanceofrain": p} for p in hourly_probs[0]]},
            {"date": forecast_dates[1], "maxtempC": "24", "mintempC": "14",
             "hourly": [{"time": "0", "chanceofrain": p} for p in hourly_probs[1]]},
            {"date": forecast_dates[2], "maxtempC": "22", "mintempC": "13",
             "hourly": [{"time": "0", "chanceofrain": p} for p in hourly_probs[2]]},
        ],
    }]


def _wttr_real_nantong_schema():
    """Real-schema keyless response for Nantong (no localObsDateTime, nearby
    station Dexing, undated observation_time, hourly chanceofrain arrays)."""
    return _wttr_nantong_payload(
        nearest_area="Dexing", lat="31.977", lon="120.881",
        hourly_probs=[["15", "72", "30"], ["55", "10", "20"], ["25", "5", "9"]],
        include_observation_time=True, local_obs=None)


def _wttr_route(payload, status=200):
    def handler(request):
        if request.url.host == "www.weather.com.cn":
            return httpx.Response(503)
        if request.url.host != "wttr.in":
            # Any non-wttr host in these isolated tests must not be contacted.
            raise AssertionError(f"Unexpected request: {request.url}")
        return httpx.Response(status, json=payload)
    return handler


def _wttr_bing_route(payload, rss_items):
    """Route wttr.in to ``payload`` and Bing RSS to honest evidence fallback."""
    def handler(request):
        if request.url.host == "wttr.in":
            return httpx.Response(200, json=payload)
        if request.url.host == "www.bing.com":
            return httpx.Response(200, content=_rss(rss_items).encode("utf-8"))
        if request.url.host == "www.weather.com.cn":
            return httpx.Response(503)
        raise AssertionError(f"Unexpected host {request.url.host}")
    return handler


class PublicSourcesIsolatedTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.calls = []
        self.handler = None
        self.default_geo = _geo_response("南通", [(32.0, 120.9)])
        # Structured weather requires a commercial Open-Meteo customer key.
        self.enterContext(patch.dict(os.environ, {CUSTOM_KEY_ENV: 'test-customer-key'}))

    def make_src(self, handler=None):
        handler = handler or self.handler

        def factory():
            if handler is None:
                raise AssertionError("网络请求不应被发起")
            transport = httpx.MockTransport(handler)
            return httpx.AsyncClient(
                transport=transport,
                verify=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT),
                timeout=httpx.Timeout(2),
                base_url="http://public.test",
                follow_redirects=False,
            )

        return PublicSources(client_factory=factory)

    def route(self, handler):
        def wrap(request: httpx.Request) -> httpx.Response:
            self.calls.append(request)
            result = handler(request)
            if isinstance(result, httpx.Response):
                return result
            status, body = result
            return httpx.Response(status, content=body)
        return wrap

    async def test_page_retries_only_transport_failures_not_security_rejections(self):
        src = self.make_src()
        for error, retryable in ((httpx.ConnectError('private detail'), True),
                (asyncio.TimeoutError(), True), (PublicQueryError('链接不安全'), False),
                (ValueError('private detail'), False)):
            with self.subTest(error=type(error).__name__), patch(
                    'openclaw_service.assistant.lighthouse_public_page.read_page', AsyncMock(side_effect=error)):
                value = await src.page('https://docs.python.org/3/contents.html', 'Python documentation')
            self.assertFalse(value['ok'])
            self.assertEqual(value['retryable'], retryable)
            self.assertNotIn('private detail', str(value))

    async def test_recent_public_page_reuses_original_snapshot_without_aliasing(self):
        src, url = self.make_src(), 'https://docs.python.org/3/tutorial/controlflow.html'
        snapshot = {'ok': True, 'sourceURL': url, 'queried_at': 'original-read-time', 'text': 'original', 'links': [], 'cache_seconds': 60}
        with patch('openclaw_service.assistant.lighthouse_public_page.read_page', AsyncMock(return_value=snapshot)) as read:
            first = await src.page(url, 'Python documentation')
            first['text'] = 'caller changed text'
            first['links'].append({'link': 'https://wrong.example/'})
            second = await src.page(url, 'Read Python documentation again')
            self.assertTrue(second['cached'])
            self.assertEqual(second['text'], 'original')
            self.assertEqual(second['links'], [])
            self.assertEqual(second['queried_at'], 'original-read-time')
            self.assertEqual(read.await_count, 1)
            second['text'] = 'another caller change'
            self.assertEqual((await src.page(url, 'Python documentation'))['text'], 'original')
            await src.page(url, 'Please read Python documentation again', refresh=True)
            self.assertEqual(read.await_count, 2)
            saved_at, saved = src._pages[url]
            src._pages[url] = (saved_at - 61, saved)
            await src.page(url, 'Python documentation')
            self.assertEqual(read.await_count, 3)
        await src.close()
        self.assertEqual(src._pages, {})

    async def test_cached_page_cannot_bypass_current_question_or_url_safety(self):
        src, url = self.make_src(), 'https://docs.python.org/3/tutorial/controlflow.html'
        with patch('openclaw_service.assistant.lighthouse_public_page.read_page', AsyncMock(return_value={
                'ok': True, 'sourceURL': url, 'text': 'public content', 'cache_seconds': 60})) as read:
            await src.page(url, 'Python documentation')
            for question in ('查E楼CMDB台账', '员工身份证号', ''):
                with self.subTest(question=question):
                    self.assertFalse((await src.page(url, question))['ok'])
            private = 'https://127.0.0.1/'
            src._pages[private] = src._pages[url]
            self.assertFalse((await src.page(private, 'Python documentation'))['ok'])
            self.assertEqual(read.await_count, 1)

    async def test_page_cache_is_bounded_and_does_not_cache_failures(self):
        src, url = self.make_src(), 'https://docs.python.org/3/tutorial/controlflow.html'
        def result(target, *_args, **_kwargs):
            return {'ok': True, 'sourceURL': target, 'text': 'public content', 'cache_seconds': 60}
        with patch('openclaw_service.assistant.lighthouse_public_page.read_page', AsyncMock(
                side_effect=[httpx.ConnectError('temporary'), result(url)])) as read:
            self.assertFalse((await src.page(url, 'Python documentation'))['ok'])
            self.assertTrue((await src.page(url, 'Python documentation'))['ok'])
            self.assertEqual(read.await_count, 2)
        with patch('openclaw_service.assistant.lighthouse_public_page.read_page', AsyncMock(side_effect=result)) as read:
            for index in range(65):
                await src.page(f'https://docs.python.org/3/page-{index}.html', 'Python documentation')
            self.assertEqual(len(src._pages), 64)
            await src.page(url, 'Python documentation')
            self.assertEqual(read.await_count, 66)

    async def test_success_without_cache_permission_is_not_reused(self):
        src, url = self.make_src(), 'https://docs.python.org/3/tutorial/controlflow.html'
        with patch('openclaw_service.assistant.lighthouse_public_page.read_page', AsyncMock(return_value={
                'ok': True, 'sourceURL': url, 'cache_seconds': 0})) as read:
            for _ in range(2):
                self.assertTrue((await src.page(url, 'Python documentation'))['ok'])
            self.assertEqual(read.await_count, 2)
            self.assertEqual(src._pages, {})

    # ------------------------------------------------------------------
    # Input gates: no network for business/private/internal inputs.
    # ------------------------------------------------------------------
    async def test_internal_business_question_refuses_before_network(self):
        src = self.make_src()
        for query, question in (
            ("南通维保工单", "南通维保工单怎么样"),
            ("南通内部控制台账", "查一下南通内部控制台账"),
            ("recAbc12345678", "查一下recAbc12345678"),
            ("张三身份证", "张三身份证号"),
            ("https://192.168.1.1", "访问内网"),
            ("工号E1234", "工号E1234是谁"),
        ):
            with self.subTest(query=query):
                with self.assertRaises(PublicQueryError):
                    await src.search(query, question)
        self.assertEqual(self.calls, [])

    async def test_prompt_injected_query_never_reaches_network(self):
        src = self.make_src()
        with self.assertRaises(PublicQueryError):
            await src.search("南通校园天气查询服务", "南通今天天气怎么样")
        with self.assertRaises(PublicQueryError):
            await src.search("南通市天气预报", "南通今天天气怎么样")
        self.assertEqual(self.calls, [])

    async def test_weather_city_not_in_question_refuses_before_network(self):
        src = self.make_src()
        with self.assertRaises(PublicQueryError):
            await src.weather("北京", "南通今天天气怎么样")
        with self.assertRaises(PublicQueryError):
            await src.weather("x" * 41, "xxxxxxxxxxxxxxxxx天气")
        self.assertEqual(self.calls, [])

    async def test_weather_internal_question_refuses_before_network(self):
        src = self.make_src()
        with self.assertRaises(PublicQueryError):
            await src.weather("南通", "南通维保台账今天天气")
        self.assertEqual(self.calls, [])

    async def test_search_can_select_original_words_without_adding_or_reordering_terms(self):
        rss = _rss([{'title': 'Python documentation', 'link': 'https://docs.python.org/3/'}])
        src = self.make_src(self.route(lambda _: (200, rss.encode())))
        result = await src.search('Python documentation', 'Please search Python control flow documentation')
        self.assertTrue(result['ok'])
        self.assertTrue((await src.search('site:docs.python.org Python official documentation', 'Python 控制流的官方教程'))['ok'])
        before = len(self.calls)
        for query, question in (('Python credentials', 'Python documentation'), ('ython', 'Python documentation'),
                                ('气通南天', '南通天气'), ('site:privatecompany.org Python', 'Python documentation')):
            with self.subTest(query=query), self.assertRaises(PublicQueryError):
                await src.search(query, question)
        self.assertEqual(len(self.calls), before)

    async def test_public_news_event_is_not_internal_event_record(self):
        rss = _rss([{'title': '国际新闻事件', 'link': 'https://example.com/news', 'desc': '公开资料'}])
        src = self.make_src(self.route(lambda _: (200, rss.encode('utf-8'))))
        self.assertTrue((await src.search('国际新闻事件', '今天国际新闻事件有哪些'))['ok'])
        before = len(self.calls)
        for question in ['新闻中的E楼事件', '全球新闻中的EA118机柜', '国际新闻内部台账', '翻译人员工号',
                         '新闻事件 recAbc12345678', '国际新闻 sk-abcdefghijklmn']:
            with self.subTest(question=question), self.assertRaises(PublicQueryError):
                await src.search(question, question)
        self.assertEqual(len(self.calls), before)

    async def test_public_control_flow_tutorial_is_not_internal_business(self):
        rss = _rss([{'title': 'Python control flow', 'link': 'https://docs.python.org/3/tutorial/controlflow.html'}])
        src = self.make_src(self.route(lambda _: (200, rss.encode('utf-8'))))
        result = await src.search('Python 控制流 教程', '请搜索 Python 控制流的官方教程')
        self.assertTrue(result['ok'])
        before = len(self.calls)
        for question in ('查询E楼设备控制状态', '本系统控制API的记录', 'Python 内部台账控制',
                         '控制记录 recAbc12345678', '控制服务 sk-abcdefghijklmn'):
            with self.subTest(question=question), self.assertRaises(PublicQueryError):
                await src.search(question, question)
        self.assertEqual(len(self.calls), before)

    async def test_weather_followup_reuses_only_public_city(self):
        os.environ.pop(CUSTOM_KEY_ENV, None)
        rss = _rss([{"title": "南通天气", "link": "https://example.com/w", "desc": "公开天气资料"}])
        src = self.make_src(self.route(lambda _: (200, rss.encode("utf-8"))))
        result = await src.weather("南通", "明天呢", previous_question="南通今天天气怎么样")
        self.assertTrue(result["ok"])
        self.assertFalse(result["capability"]["structured_weather"])
        self.assertTrue(all("明天" not in str(request.url) for request in self.calls))
        self.calls.clear()
        for current, previous in (("明天呢", "南通机柜台账天气"), ("上海明天呢", "南通天气"),
                                  ("明天的维修单呢", "南通天气"), ("你好", "南通天气")):
            with self.subTest(current=current, previous=previous), self.assertRaises(PublicQueryError):
                await src.weather("南通", current, previous_question=previous)
        self.assertEqual(self.calls, [])

    # ------------------------------------------------------------------
    # Search RSS handling.
    # ------------------------------------------------------------------
    async def test_search_happy_path_bounded_five_and_sanitized_urls(self):
        items = [
            {"title": "r1", "link": "https://example.com/a", "desc": "first"},
            {"title": "r2", "link": "http://example.org/b", "desc": "second"},
            {"title": "javascript", "link": "javascript:alert(1)", "desc": "bad"},
            {"title": "ftp", "link": "ftp://example.com/f", "desc": "bad"},
            {"title": "private ip", "link": "http://192.168.1.9/x", "desc": "bad"},
            {"title": "localhost", "link": "http://localhost/y", "desc": "bad"},
            {"title": "public3", "link": "https://example.net/c", "desc": "third"},
            {"title": "public4", "link": "https://example.edu/d", "desc": "fourth"},
            {"title": "public5", "link": "https://example.cn/e", "desc": "fifth"},
            {"title": "public6", "link": "https://example.io/f", "desc": "sixth"},
        ]
        for item in items:
            item['title'] = '南通天气 ' + item['title']
        handler = self.route(lambda _: (200, _rss(items).encode("utf-8")))
        src = self.make_src(handler)
        result = await src.search("南通天气", "南通今天天气怎么样")
        self.assertTrue(result["ok"])
        self.assertFalse(result["unavailable"])
        self.assertFalse(result["empty"])
        self.assertEqual(len(result["results"]), 5)  # bounded to MAX_RESULTS
        urls = [r["url"] for r in result["results"]]
        self.assertNotIn("javascript:alert(1)", urls)
        self.assertNotIn("ftp://example.com/f", urls)
        self.assertNotIn("http://192.168.1.9/x", urls)
        self.assertNotIn("http://localhost/y", urls)
        self.assertEqual(len([u for u in urls if u.startswith("http")]), 5)
        self.assertTrue(
            result["sourceURL"].startswith("https://www.bing.com/search?format=rss&q="))
        self.assertTrue(result["queried_at"])

    async def test_search_empty_results_is_not_unavailable(self):
        handler = self.route(lambda _: (200, _rss([]).encode("utf-8")))
        src = self.make_src(handler)
        result = await src.search("上海天气", "上海今天天气怎么样")
        self.assertTrue(result["ok"])
        self.assertTrue(result["empty"])
        self.assertEqual(result["count"], 0)
        self.assertFalse(result["unavailable"])

    async def test_fast_relevant_backup_does_not_wait_for_slow_primary(self):
        entered, stopped, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        async def handler(request):
            if request.url.host == 'www.bing.com':
                entered.set()
                try:
                    await release.wait()
                finally:
                    stopped.set()
            return httpx.Response(200, json=[{'url': 'https://docs.python.org/3/tutorial/controlflow.html',
                'title': 'Python Control Flow Tools', 'extract': 'Python if and for statements'}])
        task = asyncio.create_task(self.make_src(handler).search('Python control flow documentation',
                                                               'Python control flow documentation'))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            result = await asyncio.wait_for(task, 1)
            self.assertTrue(result['ok'])
            self.assertIn('api.mwmbl.org', result['sourceURL'])
            self.assertEqual(result['results'][0]['url'], 'https://docs.python.org/3/tutorial/controlflow.html')
            self.assertTrue(stopped.is_set(), 'The unused slow request must be cancelled before returning')
        finally:
            release.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_fast_unrelated_backup_cannot_replace_relevant_primary(self):
        backup = asyncio.Event()
        async def handler(request):
            if request.url.host == 'www.bing.com':
                await backup.wait()
                return httpx.Response(200, content=_rss([{'title': 'Python Control Flow Tools',
                    'link': 'https://docs.python.org/3/tutorial/controlflow.html'}]).encode())
            backup.set()
            return httpx.Response(200, json=[{'url': 'https://example.com/bank', 'title': '银行账户登录'}])
        result = await asyncio.wait_for(self.make_src(handler).search('site:docs.python.org Python control flow',
                                                                     'Python control flow 官方文档'), 1)
        self.assertTrue(result['ok'])
        self.assertEqual([row['url'] for row in result['results']], ['https://docs.python.org/3/tutorial/controlflow.html'])

    async def test_cancelled_search_closes_all_started_sources(self):
        started, stopped, clients = set(), set(), []
        both, release = asyncio.Event(), asyncio.Event()
        async def handler(request):
            host = request.url.host
            started.add(host)
            if len(started) == 2:
                both.set()
            try:
                await release.wait()
            finally:
                stopped.add(host)
            return httpx.Response(200, content=b'')
        def factory():
            client = httpx.AsyncClient(verify=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT), transport=httpx.MockTransport(handler))
            clients.append(client)
            return client
        task = asyncio.create_task(PublicSources(client_factory=factory).search('Python control flow', 'Python control flow'))
        try:
            await asyncio.wait_for(both.wait(), 1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual(stopped, started)
            self.assertTrue(all(client.is_closed for client in clients))
        finally:
            release.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_search_dtd_rejected_as_unavailable(self):
        handler = self.route(lambda _: (200, XML_DTD_PAYLOAD.encode("utf-8")))
        src = self.make_src(handler)
        result = await src.search("南通天气", "南通天气怎么样")
        self.assertFalse(result["ok"])
        self.assertTrue(result["unavailable"])
        self.assertIsNone(result["count"])
        self.assertIn("DTD", result["note"])

    async def test_search_standard_xml_entities_accepted(self):
        xml = (
            '<?xml version="1.0"?><rss><channel><item>'
            "<title>Tom &amp; Jerry</title>"
            "<link>https://example.com/tj</link>"
            "<description>a &lt;b&gt; &#233;&#x4E2D;</description>"
            "</item></channel></rss>"
        )
        handler = self.route(lambda _: (200, xml.encode("utf-8")))
        src = self.make_src(handler)
        result = await src.search("Tom Jerry", "Tom Jerry")
        self.assertTrue(result["ok"])
        self.assertEqual(result["count"], 1)
        self.assertEqual(result["results"][0]["title"], "Tom & Jerry")

    async def test_unrelated_search_results_use_bounded_public_backup(self):
        def handler(request):
            if request.url.host == 'www.bing.com':
                return httpx.Response(200, content=_rss([{'title': '银行账户登录', 'link': 'https://example.com/bank'}]).encode())
            self.assertEqual(request.url.host, 'api.mwmbl.org')
            self.assertEqual(request.url.params['s'], 'Python control flow documentation')
            return httpx.Response(200, json=[
                {'url': 'https://example.com/unrelated', 'title': [{'value': '银行账户登录'}], 'extract': []},
                {'url': 'https://docs.python.org/3/tutorial/controlflow.html',
                 'title': [{'value': 'Python '}, {'value': 'Control Flow Tools'}],
                 'extract': [{'value': 'Python if and for statements'}]},
            ])
        src = self.make_src(self.route(handler))
        result = await src.search('Python control flow documentation', 'Python control flow documentation')
        self.assertTrue(result['ok'])
        self.assertEqual(result['count'], 1)
        self.assertIn('api.mwmbl.org', result['sourceURL'])
        self.assertEqual(result['results'][0]['url'], 'https://docs.python.org/3/tutorial/controlflow.html')
        self.assertEqual(len(self.calls), 2)

    async def test_unrelated_or_invalid_backup_is_unknown_not_evidence(self):
        for backup in ([{'url': 'https://example.com/bank', 'title': '银行账户登录', 'extract': ''}], {'results': []}):
            def handler(request):
                if request.url.host == 'www.bing.com':
                    return httpx.Response(200, content=_rss([{'title': '银行登录', 'link': 'https://example.com/bank'}]).encode())
                return httpx.Response(200, json=backup)
            with self.subTest(backup=backup):
                result = await self.make_src(self.route(handler)).search('Python control flow', 'Python control flow')
                self.assertFalse(result['ok'])
                self.assertTrue(result['unavailable'])
                self.assertFalse(result['empty'])
                self.assertIsNone(result['count'])
                self.assertIn('不相关', result['note'])

    async def test_empty_weather_search_is_unavailable_not_a_dated_forecast(self):
        def handler(request):
            if request.url.host == 'www.bing.com':
                return httpx.Response(200, content=_rss([]).encode())
            if request.url.host == 'api.mwmbl.org':
                return httpx.Response(200, json=[])
            return httpx.Response(503)
        src = self.make_src(self.route(handler))
        with patch.dict(os.environ, {CUSTOM_KEY_ENV: ''}):
            result = await src.weather('南通', '南通今天天气怎么样')
        self.assertFalse(result['ok'])
        self.assertTrue(result['unavailable'])
        self.assertIsNone(result['count'])
        self.assertIn('没有匹配结果', result['note'])

    async def test_search_sources_preserve_normal_query_parameters_and_reject_credentials(self):
        rows = [{'title': 'Python search documentation', 'link': link} for link in (
            'https://example.com/docs?id=1&lang=zh', 'https://example.com/docs?id=2&lang=zh',
            'https://example.com/docs?token=secret', 'https://example.com/docs?key=sk-abcdefghijklmn',
            'https://example.com/docs?redirect=http%3A%2F%2F192.168.1.1',
        )]
        result = await self.make_src(self.route(lambda _: (200, _rss(rows).encode()))).search('Python documentation', 'Python documentation')
        self.assertTrue(result['ok'])
        self.assertEqual([row['url'] for row in result['results']],
                         ['https://example.com/docs?id=1&lang=zh', 'https://example.com/docs?id=2&lang=zh'])

    async def test_search_enforces_requested_publisher_when_engine_ignores_site(self):
        rows = [{'title': 'Python documentation', 'link': link} for link in (
            'https://thirdparty.example/python', 'https://docs.python.org.evil.example/python',
            'https://docs.python.org/3/',
        )]
        src = self.make_src(self.route(lambda _: (200, _rss(rows).encode())))
        result = await src.search('site:docs.python.org Python documentation', 'Python 官方文档')
        self.assertEqual([row['url'] for row in result['results']], ['https://docs.python.org/3/'])
        rows = [{'title': 'No topic', 'link': link} for link in ('https://thirdparty.example/', 'https://docs.org/')]
        result = await self.make_src(self.route(lambda _: (200, _rss(rows).encode()))).search('site:docs.org', 'site:docs.org')
        self.assertEqual([row['url'] for row in result['results']], ['https://docs.org/'])

    async def test_official_search_requires_topic_not_just_publisher(self):
        rows = [
            {'title': 'Python documentation', 'link': 'https://docs.python.org/3/contents.html'},
            {'title': 'Python Control Systems', 'link': 'https://docs.python.org/control.html'},
            {'title': 'Python Control Flow Tools', 'link': 'https://docs.python.org/3/tutorial/controlflow.html'},
        ]
        src = self.make_src(self.route(lambda _: (200, _rss(rows).encode())))
        result = await src.search('site:docs.python.org control flow documentation', 'Python control flow documentation')
        self.assertEqual([row['url'] for row in result['results']], ['https://docs.python.org/3/tutorial/controlflow.html'])
        result = await src.search('Python control flow documentation', 'Python control flow documentation')
        self.assertEqual([row['url'] for row in result['results']], ['https://docs.python.org/3/tutorial/controlflow.html'])

    async def test_rss_ranks_topic_before_truncating_results(self):
        rows = [{'title': 'Python documentation', 'link': f'https://docs.python.org/3/intro{index}.html'}
                for index in range(8)]
        rows.append({'title': 'Python Control Flow Tools', 'link': 'https://docs.python.org/3/tutorial/controlflow.html'})
        result = await self.make_src(self.route(lambda _: (200, _rss(rows).encode()))).search(
            'Python control flow documentation', 'Python control flow documentation')
        self.assertEqual([row['url'] for row in result['results']], ['https://docs.python.org/3/tutorial/controlflow.html'])

    async def test_broad_official_fallback_does_not_turn_index_into_topic_evidence(self):
        def handler(request):
            if request.url.host == 'www.bing.com':
                return httpx.Response(200, content=_rss([{'title': 'Python documentation',
                    'link': 'https://docs.python.org/3/contents.html'}]).encode())
            return httpx.Response(200, json=[{'url': 'https://docs.python.org/2/', 'title': 'Python documentation'}])
        result = await self.make_src(self.route(handler)).search('site:docs.python.org Python control flow documentation',
            'Python control flow documentation')
        self.assertFalse(result['ok'])
        self.assertTrue(result['unavailable'])
        self.assertNotIn('results', result)
        self.assertEqual(len(self.calls), 1, 'An actual official contents link can be read without three search attempts')
        self.assertEqual(result['navigation'][0]['link'], 'https://docs.python.org/3/contents.html')

    async def test_official_search_broadens_once_without_losing_publisher(self):
        def handler(request):
            if request.url.host == 'www.bing.com':
                return httpx.Response(200, content=_rss([]).encode())
            query = request.url.params['s']
            if query.lower() == 'site:docs.python.org python documentation':
                return httpx.Response(200, json=[
                    {'url': 'https://thirdparty.example/python', 'title': 'Python documentation'},
                    {'url': 'https://docs.python.org/3/tutorial/controlflow.html', 'title': 'Python Control Flow Tools'},
                ])
            return httpx.Response(200, json=[])
        result = await self.make_src(self.route(handler)).search('site:docs.python.org Python control flow documentation',
                                                               'Python control flow documentation 官方文档')
        self.assertTrue(result['ok'])
        self.assertEqual([row['url'] for row in result['results']], ['https://docs.python.org/3/tutorial/controlflow.html'])
        self.assertEqual(len(self.calls), 3)

    async def test_official_search_restores_user_subject_omitted_from_query(self):
        def handler(request):
            if request.url.host == 'www.bing.com':
                return httpx.Response(200, content=_rss([]).encode())
            if request.url.params['s'].lower() == 'site:docs.python.org python documentation':
                return httpx.Response(200, json=[
                    {'url': 'https://docs.python.org/3/tutorial/controlflow.html', 'title': 'Python Control Flow Tools'},
                    {'url': 'https://thirdparty.example/python', 'title': 'Python documentation'},
                ])
            return httpx.Response(200, json=[])
        for query in ('site:docs.python.org control flow documentation', 'site:docs.python.org control'):
            with self.subTest(query=query):
                before = len(self.calls)
                result = await self.make_src(self.route(handler)).search(query, 'Python control flow 官方文档')
                self.assertTrue(result['ok'])
                self.assertEqual([row['url'] for row in result['results']], ['https://docs.python.org/3/tutorial/controlflow.html'])
                self.assertEqual(len(self.calls) - before, 3)

    async def test_search_invalid_xml_rejected(self):
        handler = self.route(lambda _: (200, b"this is not xml at all"))
        src = self.make_src(handler)
        result = await src.search("南通天气", "南通天气怎么样")
        self.assertFalse(result["ok"])
        self.assertTrue(result["unavailable"])
        self.assertIsNone(result["count"])
        self.assertIn("XML", result["note"])

    async def test_search_oversized_response_rejected(self):
        big = b"<rss><channel>" + b"x" * (512 * 1024) + b"</channel></rss>"
        handler = self.route(lambda _: (200, big))
        src = self.make_src(handler)
        result = await src.search("南通天气", "南通天气怎么样")
        self.assertFalse(result["ok"])
        self.assertTrue(result["unavailable"])
        self.assertIsNone(result["count"])
        self.assertIn("上限", result["note"])

    async def test_search_http_error_is_unavailable_not_empty(self):
        handler = self.route(lambda _: (500, b"boom"))
        src = self.make_src(handler)
        result = await src.search("南通天气", "南通天气怎么样")
        self.assertFalse(result["ok"])
        self.assertTrue(result["unavailable"])
        self.assertFalse(result["empty"])
        self.assertIsNone(result["count"])
        self.assertIn("500", result["note"])

    async def test_search_redirect_is_unavailable(self):
        handler = self.route(
            lambda _: httpx.Response(302, headers={"Location": "https://evil.example.com"}))
        src = self.make_src(handler)
        result = await src.search("南通天气", "南通天气怎么样")
        self.assertFalse(result["ok"])
        self.assertTrue(result["unavailable"])
        self.assertFalse(result["empty"])
        self.assertIsNone(result["count"])
        self.assertIn("302", result["note"])

    # ------------------------------------------------------------------
    # Weather handling (commercial Open-Meteo path with customer key).
    # ------------------------------------------------------------------
    async def test_weather_happy_path_structure(self):
        def handler(request):
            if request.url.host == "customer-api.open-meteo.com" and "/search" in request.url.path:
                return httpx.Response(200, json=_geo_response("南通", [(32.0, 120.9)]))
            if request.url.host == "customer-api.open-meteo.com" and "/forecast" in request.url.path:
                return httpx.Response(200, json=_forecast_payload())
            return httpx.Response(404, json={})

        src = self.make_src(handler)
        result = await src.weather("南通", "南通今天天气怎么样")
        self.assertTrue(result["ok"])
        self.assertFalse(result["unavailable"])
        self.assertEqual(result["city"], "南通")
        self.assertEqual(result["timezone"], "Asia/Shanghai")
        self.assertIn("current", result)
        self.assertIn("daily", result)
        self.assertEqual(len(result["daily"]), 3)
        self.assertIn("temperature", result["current"])
        self.assertIn("weather_code", result["current"])
        self.assertIn("wind_speed_kmh", result["current"])
        self.assertIn("relative_humidity_pct", result["current"])
        self.assertTrue(
            result["sourceURL"].startswith("https://customer-api.open-meteo.com"))
        self.assertTrue(result["queried_at"])
        self.assertEqual(result["capability"]["structured_weather"], True)

    async def test_weather_ambiguous_geocode_is_unavailable_not_faked(self):
        def handler(request):
            if request.url.host == "customer-api.open-meteo.com" and "/search" in request.url.path:
                return httpx.Response(
                    200, json=_geo_response("南通", [(32.0, 120.9), (31.5, 121.1)]))
            return httpx.Response(500, json={})

        src = self.make_src(handler)
        result = await src.weather("南通", "南通今天天气怎么样")
        self.assertFalse(result["ok"])
        self.assertTrue(result["unavailable"])
        self.assertNotIn("daily", result)

    async def test_weather_unknown_city_is_unavailable(self):
        def handler(request):
            return httpx.Response(200, json={"results": []})

        src = self.make_src(handler)
        result = await src.weather("Zzzzz", "Zzzzz今天天气怎么样")
        self.assertFalse(result["ok"])
        self.assertTrue(result["unavailable"])
        self.assertIn("找不到", result["note"])

    async def test_known_city_aliases_ground_to_users_city_before_network(self):
        calls = []
        def handler(request):
            calls.append(request)
            return httpx.Response(200, json=_wttr_nantong_payload())
        with patch.dict(os.environ, {CUSTOM_KEY_ENV: ''}):
            source = self.make_src(handler)
            for city, question in [('南通市', '南通今天天气怎么样?'), ('Nantong', '南通今天天气怎么样?'), ('南通', 'Nantong天气怎么样?')]:
                result = await source.weather(city, question)
                self.assertTrue(result['ok'], result)
                self.assertEqual(result['city'], '南通')
        self.assertTrue(all((request.url.host, request.url.path) in {('wttr.in', '/Nantong'), ('www.weather.com.cn', '/weather/101190501.shtml')} for request in calls))
        before = len(calls)
        with self.assertRaises(PublicQueryError):
            await source.weather('Nantong', '上海今天天气怎么样?')
        self.assertEqual(len(calls), before)

    async def test_private_facility_is_not_a_public_weather_city(self):
        source = PublicSources(client_factory=lambda: self.fail('must not contact public services'))
        for city in ['EA118机房', '南通园区', 'D楼机房', 'D-346-HVDC基地']:
            with self.subTest(city=city), self.assertRaises(PublicQueryError):
                await source.weather(city, city + '今天天气怎么样?')

    async def test_slow_structured_weather_preserves_budget_for_fallback(self):
        source = PublicSources()
        source._timeout = .05
        async def slow(*_):
            await asyncio.sleep(10)
        fallback = AsyncMock(return_value={'ok': True, 'source': 'public_search_weather', 'results': []})
        with patch.dict(os.environ, {CUSTOM_KEY_ENV: ''}), patch.object(source, '_weather_wttr', slow), patch.object(source, '_weather_fallback', fallback):
            result = await source.weather('南通', '南通天气')
        self.assertTrue(result['ok'])
        fallback.assert_awaited_once()

    async def test_weather_invalid_forecast_structure_is_unavailable_not_faked(self):
        def handler(request):
            if request.url.host == "customer-api.open-meteo.com" and "/search" in request.url.path:
                return httpx.Response(200, json=_geo_response("南通", [(32.0, 120.9)]))
            return httpx.Response(200, json={"current": {}, "daily": {}})

        src = self.make_src(handler)
        result = await src.weather("南通", "南通今天天气怎么样")
        self.assertFalse(result["ok"])
        self.assertTrue(result["unavailable"])
        self.assertIn("结构无效", result["note"])

    async def test_weather_forecast_error_is_unavailable(self):
        def handler(request):
            if request.url.host == "customer-api.open-meteo.com" and "/search" in request.url.path:
                return httpx.Response(200, json=_geo_response("南通", [(32.0, 120.9)]))
            return httpx.Response(503, json={})

        src = self.make_src(handler)
        result = await src.weather("南通", "南通今天天气怎么样")
        self.assertFalse(result["ok"])
        self.assertTrue(result["unavailable"])
        self.assertIn("503", result["note"])

    async def test_weather_geocode_error_is_unavailable(self):
        def handler(request):
            return httpx.Response(500, json={})

        src = self.make_src(handler)
        result = await src.weather("南通", "南通今天天气怎么样")
        self.assertFalse(result["ok"])
        self.assertTrue(result["unavailable"])
        self.assertIn("500", result["note"])

    # ------------------------------------------------------------------
    # Weather fallback: without a customer key, honest public search evidence.
    # ------------------------------------------------------------------
    async def test_weather_no_key_falls_back_to_public_search_evidence(self):
        os.environ.pop(CUSTOM_KEY_ENV, None)
        rss = _rss([{"title": "南通天气", "link": "https://example.com/w",
                     "desc": "今日多云"}])
        handler = self.route(lambda _: (200, rss.encode("utf-8")))
        src = self.make_src(handler)
        result = await src.weather("南通", "南通今天天气怎么样")
        self.assertTrue(result["ok"])
        self.assertFalse(result["unavailable"])
        self.assertNotIn("current", result)
        self.assertNotIn("daily", result)
        self.assertIn("results", result)
        self.assertEqual(result["capability"]["structured_weather"], False)
        self.assertTrue(
            result["sourceURL"].startswith("https://www.bing.com/search?format=rss&q="))
        self.assertTrue(result["queried_at"])

    # ------------------------------------------------------------------
    # Keyless wttr.in public weather (public weather community, no SLA).
    # Nantong: real-schema fixture (Dexing nearby station, undated
    # observation_time), valid dated structure, lang_zh desc, undated/stale
    # observations, malformed dates, daily rain max, and wrong city/country.
    # ------------------------------------------------------------------
    async def test_weather_wttr_real_nantong_schema_undated_but_daily_accepted(self):
        """The current live response (no localObsDateTime, station Dexing within
        30 km, China) must yield structured daily data with the current
        observation honestly marked undated — no invented freshness."""
        os.environ.pop(CUSTOM_KEY_ENV, None)
        handler = self.route(_wttr_route(_wttr_real_nantong_schema()))
        src = self.make_src(handler)
        result = await src.weather("南通", "南通今天天气怎么样")
        self.assertTrue(result["ok"])
        self.assertFalse(result["unavailable"])
        self.assertEqual(result["capability"]["structured_weather"], True)
        self.assertEqual(result["resolved_city"], "Dexing")
        self.assertEqual(result["requested_city"], "Nantong")
        self.assertEqual(result["resolved_country"], "China")
        self.assertEqual(result["resolution_mode"], "nearby-coordinates")
        self.assertIsNone(result["observation_time"])
        self.assertTrue(result["observation_unavailable"])
        self.assertIn("undated", result["observation_note"])
        self.assertEqual(len(result["daily"]), 3)
        # Daily dates are dated today and consecutive; current observation is not.
        self.assertEqual(result["daily"][0]["date"],
                         dt.datetime.now(TZ).date().isoformat())

    async def test_wttr_weather_and_nearby_station_reply_are_chinese(self):
        from lan_bitable_template_portal.lighthouse_public import weather_reply
        os.environ.pop(CUSTOM_KEY_ENV, None)
        for english, chinese in (('Sunny', '晴'), ('Cloudy', '多云'), ('Unrecognized condition', '天气描述待核对')):
            with self.subTest(description=english):
                payload = _wttr_real_nantong_schema()
                for day in payload[0]['weather']:
                    day['hourly'].append({'time': '1200', 'weatherDesc': [{'value': english}]})
                src = self.make_src(self.route(_wttr_route(payload)))
                result = await src.weather('南通', '南通今天天气怎么样')
                reply = weather_reply(result, '南通', result['daily'][0]['date'])
                self.assertIn(chinese, reply)
                self.assertIn('附近公共气象站', reply)
                self.assertNotIn(english, reply)
                self.assertNotIn('Dexing', reply)
                self.assertNotIn('resolved to', reply)

    async def test_wttr_prefers_supplied_chinese_description(self):
        os.environ.pop(CUSTOM_KEY_ENV, None)
        payload = _wttr_nantong_payload()
        payload[0]['current_condition'][0].update(weatherDesc=[{'value': 'Sunny'}], lang_zh=[{'value': '晴间多云'}])
        src = self.make_src(self.route(_wttr_route(payload)))
        result = await src.weather('南通', '南通今天天气怎么样')
        self.assertEqual(result['current']['weather_desc'], '晴间多云')

    async def test_weather_wttr_nantong_style1_valid_dated_structure(self):
        os.environ.pop(CUSTOM_KEY_ENV, None)
        handler = self.route(_wttr_route(_wttr_nantong_payload()))
        src = self.make_src(handler)
        result = await src.weather("南通", "南通今天天气怎么样")
        self.assertTrue(result["ok"])
        self.assertFalse(result["unavailable"])
        self.assertEqual(result["city"], "南通")
        self.assertEqual(result["resolved_city"], "Nantong")
        self.assertEqual(result["resolution_mode"], "nearby-coordinates")
        self.assertIn("current", result)
        self.assertEqual(len(result["daily"]), 3)
        self.assertIn("temperature", result["current"])
        self.assertIn("relative_humidity_pct", result["current"])
        self.assertIn("wind_speed_kmh", result["current"])
        self.assertIn("weather_desc", result["current"])
        self.assertEqual(result["capability"]["structured_weather"], True)
        self.assertEqual(result["capability"]["provider"], "wttr.in-keyless")
        self.assertEqual(result["source"], "publicweathercommunity no SLA")
        self.assertTrue(result["sourceURL"].startswith("https://wttr.in/Nantong?"))
        self.assertIsNotNone(result["observation_time"])
        self.assertFalse(result["observation_unavailable"])
        self.assertTrue(result["queried_at"])

    async def test_weather_wttr_nantong_style2_lang_zh_desc(self):
        os.environ.pop(CUSTOM_KEY_ENV, None)
        payload = _wttr_nantong_payload(weather_desc="", lang_zh="多云转阴")
        handler = self.route(_wttr_route(payload))
        src = self.make_src(handler)
        result = await src.weather("南通", "南通今天天气怎么样")
        self.assertTrue(result["ok"])
        self.assertEqual(result["current"]["weather_desc"], "多云转阴")

    async def test_weather_wttr_nantong_daily_rain_uses_max_hourly_probability(self):
        os.environ.pop(CUSTOM_KEY_ENV, None)
        # Day 0 hourly probs [27, 72, 73, 10]: max 73, first hour 27.
        payload = _wttr_nantong_payload(
            hourly_probs=[["27", "72", "73", "10"], ["10", "5"], ["0", "1"]])
        handler = self.route(_wttr_route(payload))
        src = self.make_src(handler)
        result = await src.weather("南通", "南通今天天气怎么样")
        self.assertTrue(result["ok"])
        self.assertEqual(result["daily"][0]["chanceofrain"], 73.0)
        self.assertEqual(result["daily"][1]["chanceofrain"], 10.0)
        self.assertEqual(result["daily"][2]["chanceofrain"], 1.0)

    async def test_weather_wttr_nantong_undated_observation_accepted_with_daily(self):
        os.environ.pop(CUSTOM_KEY_ENV, None)
        # Neither localObsDateTime nor its date is present (only undated time).
        payload = _wttr_nantong_payload(local_obs=None, include_observation_time=True)
        handler = self.route(_wttr_route(payload))
        src = self.make_src(handler)
        result = await src.weather("南通", "南通今天天气怎么样")
        self.assertTrue(result["ok"])
        self.assertEqual(result["capability"]["structured_weather"], True)
        self.assertIsNone(result["observation_time"])
        self.assertTrue(result["observation_unavailable"])
        self.assertEqual(len(result["daily"]), 3)

    async def test_weather_wttr_nantong_stale_observation_marks_current_undated_keeps_daily(self):
        os.environ.pop(CUSTOM_KEY_ENV, None)
        stale = _wttr_obs_str(offset_seconds=-2 * 24 * 3600)
        payload = _wttr_nantong_payload(local_obs=stale)
        handler = self.route(_wttr_route(payload))
        src = self.make_src(handler)
        result = await src.weather("南通", "南通今天天气怎么样")
        # A stale dated observation is trusted no more than an undated one:
        # structured daily forecast is still returned, current observation marked
        # unavailable rather than pretending a fresh timestamp.
        self.assertTrue(result["ok"])
        self.assertEqual(result["capability"]["structured_weather"], True)
        self.assertIsNone(result["observation_time"])
        self.assertTrue(result["observation_unavailable"])
        self.assertIn("stale", result["observation_note"])
        self.assertEqual(len(result["daily"]), 3)

    async def test_weather_wttr_nantong_wrong_city_not_claimed(self):
        os.environ.pop(CUSTOM_KEY_ENV, None)
        # Shanghai is ~80 km from the Nantong centre -> far station must be
        # rejected even though country is China.
        payload = _wttr_nantong_payload(nearest_area="Shanghai",
                                        lat="31.2304", lon="121.4737")
        rss_items = [{"title": "南通天气", "link": "https://example.com/w",
                      "desc": "今日多云"}]
        handler = self.route(_wttr_bing_route(payload, rss_items))
        src = self.make_src(handler)
        result = await src.weather("南通", "南通今天天气怎么样")
        self.assertTrue(any(r.url.host == "wttr.in" for r in self.calls))
        self.assertEqual(result["capability"]["structured_weather"], False)
        self.assertNotIn("current", result)
        self.assertNotIn("daily", result)

    async def test_weather_wttr_nantong_wrong_country_not_claimed(self):
        os.environ.pop(CUSTOM_KEY_ENV, None)
        # Coordinates are inside the 30 km radius but the station country is wrong.
        payload = _wttr_nantong_payload(nearest_area="Dexing", country="Japan")
        rss_items = [{"title": "南通天气", "link": "https://example.com/w",
                      "desc": "今日多云"}]
        handler = self.route(_wttr_bing_route(payload, rss_items))
        src = self.make_src(handler)
        result = await src.weather("南通", "南通今天天气怎么样")
        self.assertTrue(any(r.url.host == "wttr.in" for r in self.calls))
        self.assertEqual(result["capability"]["structured_weather"], False)
        self.assertNotIn("current", result)
        self.assertNotIn("daily", result)

    async def test_weather_wttr_nantong_far_station_not_claimed(self):
        os.environ.pop(CUSTOM_KEY_ENV, None)
        # Within China but beyond 30 km (e.g. a station far in Jiangsu).
        payload = _wttr_nantong_payload(nearest_area="Wuxi",
                                        lat="31.4912", lon="120.3119")
        rss_items = [{"title": "南通天气", "link": "https://example.com/w",
                      "desc": "今日多云"}]
        handler = self.route(_wttr_bing_route(payload, rss_items))
        src = self.make_src(handler)
        result = await src.weather("南通", "南通今天天气怎么样")
        self.assertTrue(any(r.url.host == "wttr.in" for r in self.calls))
        self.assertEqual(result["capability"]["structured_weather"], False)
        self.assertNotIn("current", result)
        self.assertNotIn("daily", result)

    async def test_weather_wttr_nantong_malformed_daily_date_rejected(self):
        os.environ.pop(CUSTOM_KEY_ENV, None)
        today = dt.datetime.now(TZ).date().isoformat()
        bad_dates = [today, "not-a-date", "2026-13-40"]
        payload = _wttr_nantong_payload(forecast_dates=bad_dates)
        rss_items = [{"title": "南通天气", "link": "https://example.com/w",
                      "desc": "今日多云"}]
        handler = self.route(_wttr_bing_route(payload, rss_items))
        src = self.make_src(handler)
        result = await src.weather("南通", "南通今天天气怎么样")
        # Malformed daily dates -> structured reject -> honest search evidence.
        self.assertEqual(result["capability"]["structured_weather"], False)
        self.assertNotIn("daily", result)
        self.assertNotIn("current", result)

    async def test_weather_wttr_malformed_json_marks_notstructured_fallback(self):
        os.environ.pop(CUSTOM_KEY_ENV, None)

        def handler(request):
            if request.url.host == "wttr.in":
                return httpx.Response(200, content=b"{not json")
            if request.url.host == "www.weather.com.cn":
                return httpx.Response(503)
            if request.url.host == "www.bing.com":
                rss = _rss([{"title": "南通天气",
                             "link": "https://example.com/w", "desc": "多云"}])
                return httpx.Response(200, content=rss.encode("utf-8"))
            raise AssertionError(f"Unexpected host {request.url.host}")

        src = self.make_src(self.route(handler))
        result = await src.weather("南通", "南通今天天气怎么样")
        self.assertTrue(result["ok"])
        self.assertIn("results", result)
        self.assertEqual(result["capability"]["structured_weather"], False)

    async def test_weather_wttr_unauthorized_city_no_request(self):
        os.environ.pop(CUSTOM_KEY_ENV, None)
        src = self.make_src()
        for bad_city, question in (
            ("南通/123", "南通/123今天天气怎么样"),
            ("http://x.cn", "http://x.cn今天天气怎么样"),
            ("192.168.1.1", "192.168.1.1今天天气怎么样"),
            ("a@b.cn", "a@b.cn今天天气怎么样"),
            ("example.com", "example.com今天天气怎么样"),
        ):
            with self.subTest(city=bad_city):
                with self.assertRaises(PublicQueryError):
                    await src.weather(bad_city, question)
        self.assertEqual(self.calls, [])

    # ------------------------------------------------------------------
    # Token derivation edge cases (no permissive semantic guessing).
    # ------------------------------------------------------------------
    async def test_query_must_be_derived_from_user_text(self):
        src = self.make_src()
        with self.assertRaises(PublicQueryError):
            await src.search("南通股市行情", "南通今天天气怎么样")
        with self.assertRaises(PublicQueryError):
            await src.search("济南天气", "南通天气怎么样")


# ---------------------------------------------------------------------------
# Optional live smoke checks (only generic public queries, no business data).
# ---------------------------------------------------------------------------
@unittest.skipUnless(
    os.environ.get("LIGHTHOUSE_LIVE_TESTS") == "1",
    "set LIGHTHOUSE_LIVE_TESTS=1 to run live public smoke checks",
)
class PublicSourcesLiveTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_weather_nantong(self):
        src = PublicSources(timeout=15)
        self.addAsyncCleanup(src.close)
        result = await src.weather("南通", "南通今天天气怎么样")
        self.assertIn("ok", result)
        self.assertIn("queried_at", result)
        if result.get("ok"):
            if result.get("capability", {}).get("structured_weather"):
                # Structured forecast (Open-Meteo or keyless wttr.in); when the
                # current observation is undated, no invented freshness is served.
                self.assertIn("daily", result)
                self.assertIn("current", result)
                if result.get("observation_unavailable"):
                    self.assertIsNone(result.get("observation_time"))
            else:
                # honest public search evidence fallback (no fabricated forecast)
                self.assertIn("results", result)
                self.assertIn("capability", result)
        else:
            self.assertTrue(result.get("unavailable"))

    async def test_live_search_publicdocs(self):
        src = PublicSources(timeout=15)
        self.addAsyncCleanup(src.close)
        result = await src.search("open meteo documentation", "open meteo documentation")
        self.assertIn("ok", result)
        self.assertIn("queried_at", result)
        if result.get("ok"):
            for row in result["results"]:
                self.assertTrue(row["url"].startswith("http"))
        else:
            self.assertTrue(result.get("unavailable"))


if __name__ == "__main__":
    unittest.main(verbosity=2)

"""Public news tests use isolated feeds, never live business data."""
import sys
import asyncio
import json
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service.assistant.lighthouse_public import (
    NEWS_RSS_URLS, PublicSources, PublicQueryError, _current_news_results, _parse_rss_results, public_search_query,
)

NOW = '2026-10-15T18:00:00+08:00'


def feed(title='空间站完成新一轮科学实验', date='Thu, 15 Oct 2026 16:00:00 +0800', link='https://www.chinanews.com.cn/test/1.shtml'):
    return f'<rss><channel><item><title>{title}</title><link>{link}</link><description>公开新闻摘要</description><pubDate>{date}</pubDate></item></channel></rss>'.encode()


class NewsTests(unittest.IsolatedAsyncioTestCase):
    def source(self, payload=None, fail=False, fallback=None):
        self.calls = []
        def handler(request):
            self.calls.append(str(request.url))
            if str(request.url) in NEWS_RSS_URLS:
                return httpx.Response(503 if fail else 200, content=payload if payload is not None else feed())
            if request.url.host == 'api.mwmbl.org':
                return httpx.Response(200, json=[])
            return httpx.Response(200, content=fallback if fallback is not None else b'<rss><channel/></rss>')
        source = PublicSources(client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)))
        self.addAsyncCleanup(source.close)
        return source

    async def test_today_headlines_do_not_require_literal_hot_news_in_each_title(self):
        source = self.source()
        question = '查询一下今天热点新闻'
        query = public_search_query(question, question)
        self.assertEqual(query, '今天热点新闻')
        with patch('openclaw_service.assistant.lighthouse_public._now_iso', return_value=NOW):
            result = await source.search(query, question, search_type='today_news')
        self.assertTrue(result['ok'])
        self.assertEqual(result['source'], 'public_news')
        self.assertEqual(result['count'], 1)
        self.assertEqual(result['results'][0]['published_at'], '2026-10-15T16:00:00+08:00')
        self.assertIn('不代表全网热度排名', result['note'])
        self.assertTrue(all(url in NEWS_RSS_URLS for url in self.calls))

    async def test_old_missing_and_future_dates_are_not_today_news(self):
        for date in ('Wed, 14 Oct 2026 16:00:00 +0800', '', 'broken', 'Thu, 15 Oct 2026 20:00:00 +0800'):
            with self.subTest(date=date):
                source = self.source(feed(date=date))
                with patch('openclaw_service.assistant.lighthouse_public._now_iso', return_value=NOW):
                    result = await source.search('今天热点新闻', '查询一下今天热点新闻', search_type='today_news')
                self.assertFalse(result.get('results'))

    async def test_topic_and_requested_publisher_are_not_discarded(self):
        for query, question in (('今天南通新闻', '今天南通新闻'), ('site:example.com 今天新闻', '今天新闻 site:example.com')):
            source = self.source()
            with patch('openclaw_service.assistant.lighthouse_public._now_iso', return_value=NOW):
                result = await source.search(query, question, search_type='today_news', topic='南通' if '南通' in question else '')
            self.assertFalse(result.get('results'))

    async def test_feed_failure_falls_back_without_fabricated_rows(self):
        source = self.source(fail=True)
        with patch('openclaw_service.assistant.lighthouse_public._now_iso', return_value=NOW):
            result = await source.search('今天新闻', '今天新闻', search_type='today_news')
        self.assertFalse(result.get('results'))
        self.assertTrue(any('bing.com' in url for url in self.calls))

    async def test_other_questions_and_private_data_do_not_enter_news_feeds(self):
        source = self.source()
        await source.search('Python documentation', 'Python documentation')
        self.assertFalse(any(url in NEWS_RSS_URLS for url in self.calls))
        self.calls.clear()
        with self.assertRaises(PublicQueryError):
            await source.search('今天内部新闻台账', '今天内部新闻台账')
        self.assertEqual(self.calls, [])

    async def test_query_mode_not_question_wording_selects_news_feed(self):
        for question in ('今天热点新闻是什么', '查询一下今天热点新闻', '今天有什么要闻？'):
            with self.subTest(question=question):
                source = self.source()
                with patch('openclaw_service.assistant.lighthouse_public._now_iso', return_value=NOW):
                    result = await source.search(public_search_query(question, question), question, search_type='today_news')
                self.assertEqual(result['count'], 1)
                self.assertTrue(all(url in NEWS_RSS_URLS for url in self.calls))
        source = self.source()
        await source.search('今天热点新闻是什么', '今天热点新闻是什么')
        self.assertFalse(any(url in NEWS_RSS_URLS for url in self.calls))

    async def test_model_topic_filters_feed_not_full_question(self):
        source = self.source(feed(title='南通公布科技成果'))
        with patch('openclaw_service.assistant.lighthouse_public._now_iso', return_value=NOW):
            result = await source.search('南通新闻', '今天南通的新闻是什么', search_type='today_news', topic='南通')
        self.assertEqual(result['results'][0]['title'], '南通公布科技成果')

    async def test_fallback_news_also_requires_verified_today_publication(self):
        for date, accepted in (('Wed, 14 Oct 2026 16:00:00 +0800', False), ('', False),
                               ('Thu, 15 Oct 2026 16:00:00', False), ('Thu, 15 Oct 2026 16:00:00 +0800', True)):
            with self.subTest(date=date):
                source = self.source(fail=True, fallback=feed(date=date))
                with patch('openclaw_service.assistant.lighthouse_public._now_iso', return_value=NOW):
                    result = await source.search('今天新闻', '今天新闻', search_type='today_news')
                self.assertEqual(bool(result.get('results')), accepted)
                self.assertEqual(result['source'], 'public_news')

    async def test_news_keeps_requested_publisher_during_fallback(self):
        for host in ('example.com', 'other.example.com'):
            source = self.source(fail=True, fallback=feed(link='https://' + host + '/news'))
            with patch('openclaw_service.assistant.lighthouse_public._now_iso', return_value=NOW):
                result = await source.search('site:example.com 今天新闻', '今天新闻 site:example.com', search_type='today_news')
            self.assertEqual(bool(result.get('results')), True)
        source = self.source(fail=True, fallback=feed(link='https://unrelated.com/news'))
        with patch('openclaw_service.assistant.lighthouse_public._now_iso', return_value=NOW):
            result = await source.search('site:example.com 今天新闻', '今天新闻 site:example.com', search_type='today_news')
        self.assertFalse(result.get('results'))
        self.assertNotIn('navigation', result)

    async def test_topic_and_mode_are_checked_before_network(self):
        source = self.source()
        for arguments in ({'search_type': 'unknown'}, {'topic': '内部台账'}, {'topic': 'sk-abcdefghijklmn'},
                          {'topic': 'recAbc12345678'}, {'topic': '未提及的科技'}, {'topic': 'x' * 201}):
            with self.subTest(arguments=arguments), self.assertRaises(PublicQueryError):
                await source.search('今天新闻', '今天新闻', **arguments)
        self.assertEqual(self.calls, [])

    async def test_cache_isolated_by_mode_topic_and_beijing_date(self):
        source = self.source()
        read = AsyncMock(side_effect=lambda *args: {'ok': True, 'queried_at': args_now[0], 'results': [{'title': '新闻'}]})
        source._search_uncached = read
        args_now = [NOW]
        with patch('openclaw_service.assistant.lighthouse_public._now_iso', side_effect=lambda: args_now[0]):
            await source.search('南通新闻', '今天南通新闻')
            await source.search('南通新闻', '今天南通新闻', search_type='today_news')
            await source.search('南通新闻', '今天南通新闻', search_type='today_news', topic='南通')
            cached = await source.search('南通新闻', '今天南通新闻', search_type='today_news', topic='南通')
            self.assertTrue(cached['cached'])
            self.assertEqual(read.await_count, 3)
            args_now[0] = '2026-10-16T00:01:00+08:00'
            await source.search('南通新闻', '今天南通新闻', search_type='today_news', topic='南通')
            self.assertEqual(read.await_count, 4)

    async def test_same_news_calls_share_one_request_without_crossing_topics(self):
        source = self.source()
        ready = asyncio.Event()
        async def read(*args):
            await ready.wait()
            return {'ok': True, 'queried_at': NOW, 'results': [{'title': args[3] or '新闻'}]}
        source._search_uncached = AsyncMock(side_effect=read)
        with patch('openclaw_service.assistant.lighthouse_public._now_iso', return_value=NOW):
            tasks = [asyncio.create_task(source.search('南通新闻', '今天南通新闻', search_type='today_news')) for _ in range(10)]
            tasks.append(asyncio.create_task(source.search('南通新闻', '今天南通新闻', search_type='today_news', topic='南通')))
            await asyncio.sleep(.02)
            ready.set()
            results = await asyncio.gather(*tasks)
        self.assertEqual(source._search_uncached.await_count, 2)
        self.assertEqual(results[-1]['results'][0]['title'], '南通')

    def test_rss_publication_time_converts_to_beijing_time(self):
        self.assertEqual(_parse_rss_results(feed(date='Wed, 14 Oct 2026 23:30:00 GMT'))[0]['published_at'],
                         '2026-10-15T07:30:00+08:00')
        self.assertNotIn('published_at', _parse_rss_results(feed(date='Thu, 15 Oct 2026 16:00:00'))[0])

    def test_news_date_is_compared_in_beijing_time_and_invalid_entries_are_skipped(self):
        rows = [{'title': '测试新闻', 'url': 'https://example.com/' + str(index), 'snippet': '', 'published_at': stamp}
                for index, stamp in enumerate(('2026-10-14T16:15:00+00:00', '2026-10-15T00:00:00+09:00',
                                                '2026-10-15T00:15:00', 'invalid', '9999-12-31T23:59:59-23:59'))]
        result = _current_news_results(rows, '今天新闻', '', '2026-10-15T00:30:00+08:00')
        self.assertEqual(result, rows[:1])


class NewsModelTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from bin import test_lighthouse_stream as fixtures
        await fixtures.StreamTests.asyncSetUp(self)
        self.addAsyncCleanup(self.runtime.close)

    source = NewsTests.source

    async def answer(self, question, steps, channel='', public=None):
        from pydantic_ai.models.function import DeltaToolCall, FunctionModel
        from openclaw_service.assistant.lighthouse_model import LighthouseModel
        rounds = []
        async def stream(messages, info):
            index = len(rounds)
            rounds.append(info)
            if index < len(steps) and not isinstance(steps[index], str):
                yield {0: DeltaToolCall(name='public_search', json_args=json.dumps(steps[index], ensure_ascii=False))}
            else:
                yield steps[index] if index < len(steps) else '空间站完成新一轮科学实验。[1]'
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=stream)
        engine = LighthouseModel(self.portal, model_factory=factory, public_sources=public or self.source())
        self.addAsyncCleanup(engine.close)
        actor = {**self.actor, 'channel': channel}
        with patch('openclaw_service.assistant.lighthouse_public._now_iso', return_value=NOW):
            result = await engine.answer(actor, {'question': question, '_profile': self.model.profile()}, [],
                                         self.request, AsyncMock(), AsyncMock(return_value=actor), {})
        self.assertEqual(self.reads, [])
        return result, rounds

    async def test_both_channels_and_similar_wording_use_the_models_news_mode(self):
        for channel in ('', 'feishu:oc_fixture'):
            for question in ('今天热点新闻是什么', '查询一下今天热点新闻'):
                with self.subTest(channel=channel, question=question):
                    result, rounds = await self.answer(question, [{'query': '今天热点新闻', 'search_type': 'today_news'}], channel)
                    self.assertIn('空间站', result['answer'])
                    self.assertEqual(len(result['sources']), 1)
                    self.assertEqual(len(rounds), 2)
                    self.assertTrue(all(url in NEWS_RSS_URLS for url in self.calls))
                    tool = next(tool for tool in rounds[0].function_tools if tool.name == 'public_search')
                    self.assertEqual(tool.parameters_json_schema['properties']['search_type']['enum'], ['web', 'today_news'])

    async def test_concept_explanation_does_not_force_network_in_either_channel(self):
        for channel in ('', 'feishu:oc_fixture'):
            result, rounds = await self.answer('热点新闻是什么意思', ['热点新闻指近期受到较多关注的新闻报道。'], channel)
            self.assertIn('受到较多关注', result['answer'])
            self.assertEqual(result['sources'], [])
            self.assertEqual(len(rounds), 1)
            self.assertEqual(self.calls, [])

    async def test_no_news_cannot_become_a_heading_or_use_general_search_as_today_evidence(self):
        source = self.source(fail=True)
        result, rounds = await self.answer('今天热点新闻是什么', [
            {'query': '今天热点新闻', 'search_type': 'today_news'}, '**今日要闻**'], public=source)
        self.assertIn('未取得', result['answer'])
        self.assertNotIn('今日要闻', result['answer'])
        self.assertEqual(result['sources'], [])
        self.assertEqual(len(rounds), 2)

    async def test_general_search_cannot_replace_a_failed_today_news_lookup(self):
        old = feed(title='今天热点新闻', date='Wed, 14 Oct 2026 16:00:00 +0800')
        source = self.source(payload=old, fallback=old)
        result, rounds = await self.answer('今天热点新闻是什么', [
            {'query': '今天热点新闻', 'search_type': 'today_news'},
            {'query': '今天热点新闻'}, '**今日要闻**'], public=source)
        self.assertIn('未取得', result['answer'])
        self.assertNotIn('今日要闻', result['answer'])
        self.assertEqual(len(result['sources']), 1)
        self.assertEqual(len(rounds), 3)

    async def test_live_news_without_tools_is_not_accepted_as_verified(self):
        from pydantic_ai.exceptions import UnexpectedModelBehavior
        with self.assertRaises(UnexpectedModelBehavior):
            await self.answer('今天热点新闻是什么', ['**今日要闻**', '**今日要闻**'])
        self.assertEqual(self.calls, [])

    async def test_tool_disabled_has_clear_message_and_no_model_request(self):
        from openclaw_service.assistant.lighthouse_ai import AssistantError
        self.model.profile.return_value['tool_calls'] = False
        with self.assertRaisesRegex(AssistantError, '工具调用'):
            await self.answer('今天热点新闻是什么', [])
        self.assertEqual(self.calls, [])

    async def test_news_timeout_reports_the_reason_without_a_heading(self):
        from types import SimpleNamespace
        stopped = asyncio.Event()
        async def slow_search(*args, **kwargs):
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()
        public = SimpleNamespace(search=AsyncMock(side_effect=slow_search), close=AsyncMock())
        with patch('openclaw_service.assistant.lighthouse_public.MAX_NETWORK_SECONDS', .03):
            result, rounds = await self.answer('今天热点新闻是什么', [
                {'query': '今天热点新闻', 'search_type': 'today_news'}, '**今日要闻**'], public=public)
        self.assertTrue(stopped.is_set())
        self.assertIn('时限', result['answer'])
        self.assertNotIn('今日要闻', result['answer'])
        self.assertEqual(len(rounds), 2)
        public.search.assert_awaited_once()

    async def test_mcp_uses_the_same_model_selected_mode_and_topic(self):
        from openclaw_service.assistant import lighthouse_mcp_public
        source = self.source(feed(title='南通公布科技成果'))
        with patch.object(lighthouse_mcp_public, 'PublicSources', return_value=source), \
             patch('openclaw_service.assistant.lighthouse_public._now_iso', return_value=NOW):
            tool = lighthouse_mcp_public.functions()['public_search']
            result = await tool('南通新闻', '今天南通新闻是什么', search_type='today_news', topic='南通')
        self.assertEqual(result['results'][0]['title'], '南通公布科技成果')
        self.assertIn('link', result['results'][0])


if __name__ == '__main__':
    unittest.main()

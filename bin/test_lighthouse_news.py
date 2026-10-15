"""Public news tests use isolated feeds, never live business data."""
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service.assistant.lighthouse_public import (
    NEWS_RSS_URLS, PublicSources, PublicQueryError, _parse_rss_results, public_search_query,
)

NOW = '2026-10-15T18:00:00+08:00'


def feed(title='空间站完成新一轮科学实验', date='Thu, 15 Oct 2026 16:00:00 +0800'):
    return f'<rss><channel><item><title>{title}</title><link>https://www.chinanews.com.cn/test/1.shtml</link><description>公开新闻摘要</description><pubDate>{date}</pubDate></item></channel></rss>'.encode()


class NewsTests(unittest.IsolatedAsyncioTestCase):
    def source(self, payload=None, fail=False):
        self.calls = []
        def handler(request):
            self.calls.append(str(request.url))
            if str(request.url) in NEWS_RSS_URLS:
                return httpx.Response(503 if fail else 200, content=payload if payload is not None else feed())
            if request.url.host == 'api.mwmbl.org':
                return httpx.Response(200, json=[])
            return httpx.Response(200, content=b'<rss><channel/></rss>')
        source = PublicSources(client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)))
        self.addAsyncCleanup(source.close)
        return source

    async def test_today_headlines_do_not_require_literal_hot_news_in_each_title(self):
        source = self.source()
        question = '查询一下今天热点新闻'
        query = public_search_query(question, question)
        self.assertEqual(query, '今天热点新闻')
        with patch('openclaw_service.assistant.lighthouse_public._now_iso', return_value=NOW):
            result = await source.search(query, question)
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
                    result = await source.search('今天热点新闻', '查询一下今天热点新闻')
                self.assertFalse(result.get('results'))

    async def test_topic_and_requested_publisher_are_not_discarded(self):
        for query, question in (('今天南通新闻', '今天南通新闻'), ('site:example.com 今天新闻', '今天新闻 site:example.com')):
            source = self.source()
            with patch('openclaw_service.assistant.lighthouse_public._now_iso', return_value=NOW):
                result = await source.search(query, question)
            self.assertFalse(result.get('results'))

    async def test_feed_failure_falls_back_without_fabricated_rows(self):
        source = self.source(fail=True)
        with patch('openclaw_service.assistant.lighthouse_public._now_iso', return_value=NOW):
            result = await source.search('今天新闻', '今天新闻')
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

    def test_rss_publication_time_converts_to_beijing_time(self):
        self.assertEqual(_parse_rss_results(feed(date='Wed, 14 Oct 2026 23:30:00 GMT'))[0]['published_at'],
                         '2026-10-15T07:30:00+08:00')
        self.assertNotIn('published_at', _parse_rss_results(feed(date='Thu, 15 Oct 2026 16:00:00'))[0])


if __name__ == '__main__':
    unittest.main()

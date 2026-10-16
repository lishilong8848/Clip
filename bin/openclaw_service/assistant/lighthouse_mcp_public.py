"""Public search/page MCP backed by the portal's existing protected sources."""
from .lighthouse_public import PublicSources, SearchType, public_search_query

TOOLS = {
    'public_search': '公开查询MCP：按语义选择web普通网页或today_news今日新闻；泛问今日要闻时topic留空，专题仅取本轮明确主题；保留真实来源和时间',
    'read_public_page': '公开网页读取MCP：提取网页正文及同站点链接，不执行网页脚本或读取内部业务',
}


def public_links(value):
    if isinstance(value, dict):
        return {('link' if key == 'url' else 'source_link' if key == 'sourceURL' else key): public_links(item) for key, item in value.items()}
    if isinstance(value, list):
        return [public_links(item) for item in value]
    return value


def functions():
    sources = PublicSources()
    async def public_search(query: str, question: str, search_type: SearchType = 'web', topic: str = '') -> dict:
        """The host injects the real current question; never accept another account's context."""
        query = public_search_query(query, question)
        return public_links(await sources.search(query, question, search_type=search_type, topic=topic))

    async def read_public_page(url: str, question: str) -> dict:
        return public_links(await sources.page(url, question))
    return {'public_search': public_search, 'read_public_page': read_public_page}

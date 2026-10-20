"""Read-only research checkpoints in the existing assistant store, not business data."""
import asyncio
import copy
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import time
from urllib.parse import urlsplit

from .lighthouse_ai import AssistantError, API_KEY, CONTACT, private_identifier, safe_text


class Research:
    def __init__(self, store):
        self.store = store

    @staticmethod
    def key(actor, operation):
        topic = (actor.get('_feishu_topic') or {}).get('id', '')
        return 'research:' + hashlib.sha256(json.dumps([actor['id'], topic, operation]).encode()).hexdigest()

    def _previous_checkpoint(self, actor, operation):
        database = getattr(self.store, 'db_path', None)
        if database is None:
            return False
        try:
            database = Path(database)
            if not database.is_file():
                return False
            from lan_bitable_template_portal.feishu_identity_migration import MARKER_TABLE
            with closing(sqlite3.connect(database.absolute().as_uri() + '?mode=ro', uri=True, timeout=1)) as db:
                if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (MARKER_TABLE,)).fetchone():
                    return False
                previous = set()
                for row in db.execute(f'SELECT mappings_json FROM {MARKER_TABLE}'):
                    mapping = json.loads(row[0])
                    previous.update(owner for owner, current in mapping.items() if current == actor['id'] and owner != current)
            # Applied mappings locate only an old operation; they never authorize its data.
            return any(self.store.get_document('lighthouse_ai', self.key({**actor, 'id': owner}, operation)) is not None
                       for owner in previous)
        except (OSError, sqlite3.Error, ValueError, TypeError, AttributeError):
            raise AssistantError('原研究的迁移状态尚未核实，未重新研究；请先核验历史记录。', 409,
                                 category='research_checkpoint_blocked') from None

    def read(self, actor, operation):
        saved = self.store.get_document('lighthouse_ai', self.key(actor, operation))
        migration = (saved or {}).get('identity_migration') or {}
        if (not isinstance(migration, dict) or migration.get('blocked') or migration.get('archived')
                or migration.get('resume_allowed') is False or self._previous_checkpoint(actor, operation)):
            raise AssistantError('原研究检查点已归档或暂停，历史记录已保留；不能恢复或重放同一操作，请先核验。', 409,
                                 category='research_checkpoint_blocked')
        if saved and (set(saved['scopes']) - set(actor.get('allowed_scopes', actor['scopes']))
                      or set(saved['modules']) - set(actor.get('modules', []))):
            raise AssistantError('研究资料的访问范围已变化，请重新建立研究任务。', 403)
        return saved

    async def collect(self, actor, operation, queries, sources, authorize, emit, *, question):
        if not isinstance(queries, list) or not 1 <= len(queries) <= 4 or any(
                not isinstance(q, str) or not 2 <= len(q.strip()) <= 160 or private_identifier(q)
                or API_KEY.search(q) or CONTACT.search(q) for q in queries):
            raise AssistantError('请提供1至4条不含内部业务或敏感资料的公开研究问题。')
        if sources is None:
            raise AssistantError('尚未接入公开查询来源，不能执行联网研究。', 503)
        from .lighthouse_public import public_search_query
        try:
            queries = [public_search_query(q, question.removeprefix('/深入研究 ')) for q in queries]
        except ValueError:
            raise AssistantError('公开研究问题含内部资料或不安全内容，未发送到公网。', 403) from None
        # The original plan survives a retry; it cannot silently become another task.
        saved = self.read(actor, operation) or {
            'owner': actor['id'], 'topic_id': (actor.get('_feishu_topic') or {}).get('id', ''),
            'operation_id': operation,
            'queries': list(dict.fromkeys(q.strip() for q in queries)), 'searches': {}, 'pages': {},
            'scopes': list(actor.get('allowed_scopes', actor['scopes'])),
            'modules': list(actor.get('modules', [])), 'created_at': time.time()}
        key = self.key(actor, operation)

        async def check():
            current = await authorize()
            if current['id'] != actor['id'] or set(saved['scopes']) - set(current['scopes']) \
                    or set(saved['modules']) - set(current.get('modules', [])):
                raise AssistantError('研究期间权限已变化，未返回原资料。', 403)

        async def checkpoint():
            await check()
            saved['updated_at'] = time.time()
            await asyncio.to_thread(self.store.put_document, 'lighthouse_ai', key, copy.deepcopy(saved))

        await check()
        await checkpoint()
        async with asyncio.timeout(125):
            for number, query in enumerate(saved['queries'], 1):
                if saved['searches'].get(query, {}).get('ok'):
                    continue
                await check()
                await emit('status', {'label': f'研究资料检索 {number}/{len(saved["queries"])}'})
                try:
                    value = await sources.search(query, question.removeprefix('/深入研究 '))
                except (ValueError, TimeoutError):
                    value = {'ok': False, 'note': '此项检索暂未完成，可继续原任务。'}
                saved['searches'][query] = {
                    'ok': bool(value.get('ok')), 'queried_at': value.get('queried_at'),
                    'note': safe_text(value.get('note', '')),
                    'results': [{k: row.get(k) for k in ('title', 'url', 'snippet', 'published_at')}
                                for row in value.get('results', [])[:5]]}
                await checkpoint()
            from .lighthouse_public_page import page_url
            links = list(dict.fromkeys(url for search in saved['searches'].values()
                for row in search['results'] if (url := page_url(row.get('url')))))[:5]
            for number, url in enumerate(links, 1):
                if saved['pages'].get(url, {}).get('ok'):
                    continue
                await check()
                await emit('status', {'label': f'研究原文核对 {number}/{len(links)}'})
                try:
                    value = await sources.page(url, question.removeprefix('/深入研究 '))
                except (ValueError, TimeoutError):
                    value = {'ok': False, 'note': '原文暂不可读，仅有搜索摘要。'}
                saved['pages'][url] = {
                    'ok': bool(value.get('ok')), 'text': safe_text(value.get('text', ''), limit=5000),
                    'truncated': bool(value.get('truncated')) or len(str(value.get('text') or '')) > 5000,
                    'queried_at': value.get('queried_at'),
                    'note': safe_text(value.get('note', ''))}
                await checkpoint()
        await check()
        evidence = []
        for search in saved['searches'].values():
            for row in search['results']:
                if any(item['url'] == row['url'] for item in evidence):
                    continue
                evidence.append({**row, 'queried_at': search.get('queried_at'),
                    'page': saved['pages'].get(row['url'], {}), 'external_untrusted': True})
        return {'queries': saved['queries'], 'items': evidence,
            'distinct_domains': len({urlsplit(row['url']).hostname for row in evidence}),
            'complete': all(v['ok'] for v in saved['searches'].values())
                        and all(v['ok'] for v in saved['pages'].values()),
            'note': '比较来源的独立性、时间和冲突；搜索摘要不等于读过原文。多个网站转引同一报道不算独立验证。',
            'checkpoint_at': saved.get('updated_at')}

"""Explicit live-model acceptance; only public synthetic questions, no business writes.

Reads a configured model from the assistant DB in SQLite read-only mode. The
gateway, session and tool bridge use an isolated temporary directory and server.
No production model settings, conversations or capability records are changed.
"""
import asyncio
import ast
import copy
import datetime as dt
import json
import re
import sqlite3
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import urlsplit
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import uvicorn
from fastapi import FastAPI, Request
from test_lighthouse_stream import Store
from openclaw_service.assistant.lighthouse_ai import CustomModel, LighthouseAssistant, safe_text
from openclaw_service.assistant.lighthouse_agent import PortalAgent
from openclaw_service.assistant.lighthouse_api import PortalAPICatalog
from openclaw_service.assistant.lighthouse_files import LighthouseFiles
from openclaw_service.assistant.lighthouse_openclaw import LighthouseOpenClaw
from openclaw_service.assistant.lighthouse_runtime import free_port, install_model_route


def has_security_threshold(text):
    return bool(re.search(r'(?:\d+|[一二三四五六七八九十]+)\s*(?:[~～—–\-至到]\s*\d+)?\s*'
        r'(?:位|个字符|字符|个月|月|天|characters?\b|days?\b|months?\b)', text, re.I))


def valid_python_function(text):
    for block in re.findall(r'```(?:python|py)?\s*\n(.*?)```', text, re.S | re.I):
        try:
            tree = ast.parse(block)
        except (SyntaxError, ValueError):
            continue
        if any(isinstance(node, ast.FunctionDef) for node in tree.body):
            return True
    return False


def claims_no_public_tools(text):
    return bool(re.search(r'(?:未接入|没有|不(?:能|支持|具备)).{0,12}(?:联网|搜索|实时查询)', text))


def configured_profile(root):
    database = root / 'bin/data/lighthouse_openclaw/assistant.sqlite3'
    connection = sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True)
    try:
        rows = connection.execute("SELECT payload_json FROM documents WHERE namespace='lighthouse_ai' AND key LIKE 'model%'").fetchall()
    finally:
        connection.close()
    for row in rows:
        settings = json.loads(row[0])
        for profile in settings.get('models', []):
            if profile.get('id') == settings.get('active_model_id') and profile.get('key_cipher'):
                return copy.deepcopy(profile)
    raise RuntimeError('No configured model is available for the explicit live check')


async def run(*, include_public=False):
    project = Path(__file__).resolve().parents[2]
    profile = configured_profile(project)
    parent = project / 'build_output/agent_probes'
    parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='general-', dir=parent) as directory:
        store = Store(Path(directory) / 'fixture.sqlite3')
        custom = CustomModel(store, client=Mock())
        model = Mock()
        model.unprotect.side_effect = custom.unprotect
        assistant = LighthouseAssistant(store, Mock(return_value=([], [])), model=model)
        app, port, tools, search_queries, page_reads = FastAPI(), free_port(), [], [], []
        portal = PortalAgent(assistant, PortalAPICatalog(app), LighthouseFiles(store))
        engine = LighthouseOpenClaw(portal, bridge_url=lambda: f'http://127.0.0.1:{port}/api/assistant/openclaw-tools')
        phase = {'started': None}
        original_forward = engine.manager.forward_model
        async def timed_forward(request):
            arrived = time.monotonic()
            response = await original_forward(request)
            print('[ModelTransportLive] arrival_seconds=', round(arrived - phase['started'], 2)
                  if phase['started'] is not None else None,
                  'upstream_headers_seconds=', round(time.monotonic() - arrived, 2),
                  'status=', response.status_code, flush=True)
            return response
        engine.manager.forward_model = timed_forward
        original_page = engine.public_sources.page
        async def traced_page(url, question, **kwargs):
            started = time.monotonic()
            value = await original_page(url, question, **kwargs)
            print('[PublicPageLive]', safe_text(url, limit=250), 'seconds=', round(time.monotonic() - started, 2),
                'ok=', bool(value.get('ok')), 'cached=', bool(value.get('cached')), 'cache_seconds=', value.get('cache_seconds', 0),
                'retryable=', bool(value.get('retryable')),
                'note=', safe_text(value.get('note', ''), limit=200), flush=True)
            if value.get('ok') and not value.get('cached'):
                page_reads.append(value['sourceURL'])
            return value
        engine.public_sources.page = traced_page
        install_model_route(app, engine.manager)

        @app.post('/api/assistant/openclaw-tools')
        async def bridge(request: Request):
            payload = await request.json()
            tools.append(payload['tool'])
            if payload['tool'] == 'lighthouse_public_search':
                search_queries.append(safe_text(payload.get('params', {}).get('query', ''), limit=200))
            return await engine.bridge.call_shared(request.headers.get('authorization', '').removeprefix('Bearer '), payload, engine.manager)

        server = uvicorn.Server(uvicorn.Config(app, host='127.0.0.1', port=port, log_level='error'))
        server_task = asyncio.create_task(server.serve())
        actor = {'id': 'isolated-general-acceptance', 'scopes': ['D'], 'is_admin': False}
        request = Request({'type': 'http', 'scheme': 'http', 'server': ('127.0.0.1', port),
            'client': ('127.0.0.1', 1), 'path': '/', 'root_path': '', 'query_string': b'', 'headers': []})
        async def authorize(): return copy.deepcopy(actor)
        async def emit(*_): pass
        questions = [
            ('Translate "Good morning" into Chinese. Answer briefly.', lambda text: any(word in text for word in ('早上好', '早安'))),
            ('Explain Python list comprehensions in two sentences, with one short example.', lambda text: 'for' in text and 'in' in text),
            ('Calculate fifteen percent of 350. Use the calculate tool and answer briefly.', lambda text: '52.5' in text),
            ('给我一份汽车维修工单的简单文本模板。', lambda text: '维修' in text and len(text) > 30),
            ('电脑设备怎么设置登录密码？给三个简单建议。', lambda text: '密码' in text and '/?admin=' not in text),
            ('what is UPS? Explain its electrical purpose briefly in Chinese.', lambda text: any(word in text for word in ('不间断', '供电', '电源'))),
            ('你是什么模型？', lambda text: profile['model'] in text),
            ('请写一封通知同事下周开会的邮件模板。', lambda text: '会议' in text and len(text) > 40),
            ('写一个 Python 函数，返回列表中的偶数。放在代码块里，不运行代码。', valid_python_function),
            ('你能联网查询吗？简单说明。', lambda text: any(word in text for word in ('查询', '搜索')) and not claims_no_public_tools(text)),
            ('请记住本次聊天代号为 ALPHA-PUBLIC-47，只回复已记住。', lambda text: any(word in text for word in ('记住', '记下'))),
            ('上一条让你记住的聊天代号是什么？只回复代号。', lambda text: 'ALPHA-PUBLIC-47' in text),
            ('将聊天代号改为 BETA-PUBLIC-82，后续以这个为准。', lambda text: 'BETA-PUBLIC-82' in text),
            ('现在我让你记住的聊天代号是什么？只回复代号。', lambda text: 'BETA-PUBLIC-82' in text and 'ALPHA-PUBLIC-47' not in text),
        ]
        public_start = len(questions)
        if include_public:
            tomorrow = (dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).date() + dt.timedelta(days=1)).isoformat()
            questions.extend([
                ('南通今天天气怎么样?', lambda text: '南通' in text and '°C' in text),
                ('南通明天天气怎么样?', lambda text: '南通' in text and '°C' in text),
                ('请联网搜索 Python control flow documentation 的官方文档，简要说明并给来源。', lambda text: 'Python' in text),
                ('请阅读 https://docs.python.org/3/tutorial/controlflow.html，简要解释 Python 的 if 和 for，给来源。', lambda text: 'if' in text and 'for' in text),
                ('Nantong weather tomorrow?', lambda text: '南通' in text and tomorrow in text),
            ])
        try:
            for _ in range(100):
                if server.started: break
                await asyncio.sleep(.05)
            assert server.started, 'Isolated bridge did not start'
            start = time.monotonic()
            await asyncio.wait_for(asyncio.gather(engine.prepare(), asyncio.to_thread(engine.public_sources.prepare),
                engine.answer(actor, {'question': '', '_profile': profile, 'operation_id': 'general-warmup'},
                    [], request, emit, authorize, {}, warm_only=True)), 240)
            assert engine.manager.http is not None and not tools, 'Preparation called model tools'
            print('[GeneralLive] background preparation', round(time.monotonic() - start, 2), 'seconds; no inference.', flush=True)
            previous_item = None
            for index, (question, validate) in enumerate(questions):
                start, before, search_before, page_before = time.monotonic(), len(tools), len(search_queries), len(page_reads)
                phase['started'] = start
                result = await asyncio.wait_for(engine.answer(actor,
                    {'question': question, '_profile': profile, 'operation_id': 'general-acceptance-%04d' % index,
                     'run_id': 'general-run-%04d' % index}, [], request, emit, authorize, {}), 240)
                assert validate(result['answer']), 'Live response did not satisfy this case: ' + safe_text(result['answer'], limit=400)
                if index == 4 and not result.get('sources'):
                    assert not has_security_threshold(result['answer']), 'Unsourced security threshold in live response: ' + safe_text(result['answer'], limit=400)
                    assert not claims_no_public_tools(result['answer']), 'Unqueried evidence misreported as missing capability: ' + safe_text(result['answer'], limit=400)
                assert not result.get('plan'), 'General question must not become a business operation'
                if index < public_start:
                    assert not result.get('sources'), 'Ordinary knowledge must not query portal business records'
                else:
                    assert result.get('sources'), 'Real-time answers require actual source evidence'
                    print('[PublicLive] source evidence', [source.get('url') for source in result['sources']],
                          'tools=', tools[before:], 'queries=', search_queries[search_before:], flush=True)
                    assert all(source.get('url', '').startswith(('https://', 'http://')) and source.get('queried_at')
                               for source in result['sources']), 'Public sources need safe URLs and query times'
                    if index in {public_start + 2, public_start + 3}:
                        if index == public_start + 2:
                            assert 'lighthouse_public_search' in tools[before:], 'Live documentation query skipped public search'
                        assert any(urlsplit(url).hostname == 'docs.python.org' and urlsplit(url).path.endswith('/tutorial/controlflow.html')
                                   for url in (page_reads[page_before:] if index == public_start + 2 else page_reads)), 'Documentation query did not retrieve the actual chapter'
                        assert any(urlsplit(source['url']).hostname == 'docs.python.org'
                            and urlsplit(source['url']).path.endswith('/tutorial/controlflow.html')
                            and source['url'] in page_reads and 'if' in source.get('data', {}).get('text', '')
                            and 'for' in source.get('data', {}).get('text', '') for source in result['sources']), \
                            'The actual official control-flow chapter text was not retrieved'
                if index == 2:
                    assert 'lighthouse_calculate' in tools[before:], 'Model skipped the real calculator'
                item = next(iter(engine.manager.accounts.values()))
                if previous_item is not None:
                    assert item is previous_item, 'Stable tool schema must reuse the account gateway'
                previous_item = item
                print('[GeneralLive]', index + 1, 'OK', round(time.monotonic() - start, 2), 'seconds',
                    'tools=', tools[before:], 'answer=', result['answer'][:500], flush=True)
            secret = custom.unprotect(profile['key_cipher']).encode()
            for file in Path(directory).rglob('*'):
                if file.is_file() and file.stat().st_size <= 30 * 1024 ** 2:
                    assert secret not in file.read_bytes(), 'Credential persisted in isolated gateway files'
            print('[GeneralLive] PASS; no business data submitted or cloud records written.', flush=True)
        finally:
            await engine.close()
            server.should_exit = True
            await asyncio.wait_for(server_task, 5)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--public', action='store_true', help='Also verify real weather and official documentation search')
    asyncio.run(run(include_public=parser.parse_args().public))

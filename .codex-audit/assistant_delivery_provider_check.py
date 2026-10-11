"""Real provider with synthetic local APIs and a temporary file store; no business writes."""
import ast
import asyncio
import json
import os
import sqlite3
import sys
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'bin'), str(ROOT)]
os.environ.setdefault('PYDANTIC_AI_NO_BANNER', '1')


async def main():
    from fastapi import FastAPI, Request
    from test_lighthouse_stream import Store
    from lan_bitable_template_portal.lighthouse_ai import CustomModel, LighthouseAssistant
    from lan_bitable_template_portal.lighthouse_agent import PortalAgent
    from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
    from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
    from lan_bitable_template_portal.lighthouse_model import LighthouseModel
    from lan_bitable_template_portal.lighthouse_stream import LighthouseStream
    from lan_bitable_template_portal.lighthouse_ai import safe_text
    from pydantic_ai import Agent
    path = ROOT / 'bin/data/lan_portal_state.sqlite3'
    with sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True) as conn:
        row = conn.execute('SELECT payload_json FROM json_documents WHERE namespace=? AND key=?',
                           ('lighthouse_ai', 'model')).fetchone()
    profile = CustomModel._default(json.loads(row[0])) if row else None
    if not profile or not profile.get('key_cipher'):
        print('Delivery provider check: no configured profile')
        return 2
    with tempfile.TemporaryDirectory(prefix='clipflow-provider-workflow-') as directory:
        store = Store(Path(directory) / 'state.sqlite3')
        assistant = LighthouseAssistant(store, lambda *_: ([], []), model=CustomModel(store))
        app = FastAPI()
        reads, writes = [], []
        @app.get('/api/repair-management/records')
        def records(scope: str = 'D'):
            reads.append(scope)
            return {'ok': True, 'data': {'scope': scope, 'total': 2, 'records': [
                {'record_id': 'rec_fixture_one', 'scope': 'D', 'title': 'D楼空调测试维修'},
                {'record_id': 'rec_fixture_two', 'scope': 'D', 'title': 'D楼电源测试维修'}]}}
        @app.delete('/api/repair-management/records/{record_id}')
        def delete(record_id: str):
            writes.append(record_id)
            raise AssertionError('No operation is authorized by this test')
        @app.get('/api/repair-management/records/{record_id}')
        def record(record_id: str, scope: str = 'D'):
            reads.append(scope)
            return {'ok': True, 'data': {'record': {'record_id': record_id, 'scope': 'D',
                'title': 'D楼空调测试维修', 'fields': {'楼栋': 'D楼', '故障现象': '空调测试维修'}}}}
        files = LighthouseFiles(store)
        portal = PortalAgent(assistant, PortalAPICatalog(app), files)
        def trace_factory(model, **kwargs):
            for key in ('actor', 'turn', 'emit'):
                kwargs.pop(key, None)
            agent = Agent(model, **kwargs)
            original = agent.run_stream_events
            @asynccontextmanager
            async def traced(*args, **options):
                async with original(*args, **options) as events:
                    async def observe():
                        async for event in events:
                            if event.event_kind in {'function_tool_call', 'function_tool_result'}:
                                part = event.part
                                if part.part_kind == 'tool-call' or part.part_kind == 'retry-prompt':
                                    print('Synthetic tool trace: ' + safe_text(str(part), limit=1600), flush=True)
                            yield event
                    yield observe()
            agent.run_stream_events = traced
            return agent
        engine = LighthouseModel(portal, agent_factory=trace_factory)
        actor = {'id': 'synthetic-provider', 'name': '测试账号', 'scopes': ['D'], 'is_admin': False}
        request = Request({'type': 'http', 'scheme': 'http', 'server': ('testserver', 80),
                           'path': '/api/assistant/messages', 'query_string': b'', 'headers': []})
        async def authorize(): return dict(actor)
        async def emit(*_): pass
        cases = [('file', '帮我生成一个冒泡排序Python示例，文件名 bubble_sort.py，仅生成文件，不需要飞书发送。'),
                 ('office', '生成 sample.xlsx，工作表名示例，列名为字母和值，两行分别是 a,23 与 b,19，仅使用这些示例数据。'),
                 ('clarification', '帮我删除维修单')]
        if '--followup-only' in sys.argv:
            cases = []
        for kind, question in cases:
            turn = {'question': question, 'file_ids': [], 'operation_id': 'provider_case_' + kind, '_profile': profile}
            try:
                async with asyncio.timeout(120):
                    result = await engine.answer(actor, turn, [], request, emit, authorize, {})
                assert not writes, 'Model attempted a business write'
                if kind == 'file':
                    outputs = result.get('output_files') or []
                    assert len(outputs) == 1 and outputs[0]['name'] == 'bubble_sort.py', 'Actual requested file was not generated'
                    code = Path(files.get(actor, outputs[0]['id'])['path']).read_text(encoding='utf-8')
                    ast.parse(code)
                    assert 'def ' in code and not result.get('plan'), 'Generated code or delivery contract is invalid'
                elif kind == 'office':
                    from openpyxl import load_workbook
                    outputs = result.get('output_files') or []
                    assert len(outputs) == 1 and outputs[0]['name'] == 'sample.xlsx', 'Actual requested workbook was not generated'
                    book = load_workbook(files.get(actor, outputs[0]['id'])['path'], read_only=True)
                    try:
                        assert book.active['A2'].value == 'a' and book.active['B2'].value == 23 and book.active['B3'].value == 19, 'Workbook lost the provided sample values'
                    finally:
                        book.close()
                    assert not result.get('plan'), 'File generation unexpectedly prepared a business operation'
                else:
                    assert result.get('clarification') is not None and result.get('answer'), 'Unspecified delete target was not clarified'
                    assert not result.get('plan'), 'An unspecified target was prepared for deletion'
                print('Delivery provider check: ' + kind + ' OK', flush=True)
            except Exception as exc:
                print('Delivery provider check: ' + kind + ' FAIL ' + type(exc).__name__ + '; status=' +
                      str(getattr(exc, 'status_code', 'unavailable')), flush=True)
                print('Synthetic diagnostic: ' + safe_text(str(exc), limit=1200), flush=True)
                if isinstance(exc, AssertionError):
                    print('Contract failure: ' + str(exc), flush=True)
                    print('Result kinds: clarification=' + str(bool(result.get('clarification'))) +
                          ' plan=' + str((result.get('plan') or {}).get('status')) +
                          ' sources=' + str(len(result.get('sources') or [])), flush=True)
                return 1
        configured = Mock()
        configured.settings.return_value = {'enabled': True, 'configured': True,
            'active_model_id': 'probe', 'models': [{'id': 'probe', 'name': profile['name'], 'model': profile['model'], 'configured': True}]}
        configured.profile.return_value = profile
        assistant.model_for = lambda _: configured
        runtime = LighthouseStream(portal, engine=engine)
        try:
            for channel in ('', 'feishu:oc_fixture'):
                actor['channel'] = channel
                for index, question in enumerate(('帮我删除D楼的维修单', 'D楼空调测试维修')):
                    payload = {'conversation_id': assistant._state(actor)['id'],
                        'operation_id': 'followup_probe_' + ('feishu' if channel else 'web') + '_' + str(index),
                        'question': question}
                    await runtime.submit(actor, payload, request, authorize)
                    await asyncio.wait_for(asyncio.gather(*runtime.workers.values()), 120)
                    saved = (await runtime.conversation(actor))['turns'][-1]
                    assert saved['status'] == 'completed', 'Follow-up did not complete: ' + str(saved.get('error'))
                    if index == 0:
                        assert saved.get('clarification') and not saved.get('plan'), 'Ambiguous target was not clarified'
                    else:
                        plan = saved.get('plan') or {}
                        assert plan.get('status') in {'needs_input', 'awaiting_confirmation'}, 'Named reply lost its business intent: ' + saved['answer']
                        operations = plan['operations']
                        assert len(operations) == 1 and operations[0]['api_id'] == 'DELETE /api/repair-management/records/{record_id}'
                        assert operations[0]['path_params']['record_id'] == 'rec_fixture_one'
                    assert not writes, 'Clarification executed a business write'
                print('Delivery provider check: named followup ' + (channel or 'web') + ' OK', flush=True)
        finally:
            await runtime.close()
            await engine.close()
    return 0


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))

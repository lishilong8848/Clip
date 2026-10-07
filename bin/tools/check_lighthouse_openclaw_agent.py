"""Live OpenClaw + real plugin + synthetic model/business API acceptance."""
import asyncio
import json
import os
import sys
import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
from test_lighthouse_stream import Store
from lan_bitable_template_portal.lighthouse_agent import PortalAgent
from lan_bitable_template_portal.lighthouse_ai import LighthouseAssistant
from lan_bitable_template_portal.lighthouse_ai import AssistantError
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from lan_bitable_template_portal.lighthouse_openclaw import LighthouseOpenClaw
from lan_bitable_template_portal.lighthouse_gateway import GatewayClient, GatewayError
from lan_bitable_template_portal.lighthouse_runtime import free_port
from lan_bitable_template_portal.lighthouse_stream import LighthouseStream


class RuleCreate(BaseModel):
    name: str
    description: str = ''


async def check():
    root = Path(__file__).resolve().parents[2]
    with tempfile.TemporaryDirectory(prefix='agent-', dir=root / 'build_output/agent_probes') as directory:
        store = Store(Path(directory) / 'fixture.sqlite3')
        model = Mock()
        model.unprotect.return_value = 'synthetic-test-key'
        assistant = LighthouseAssistant(store, Mock(return_value=([], [])), model=model)
        app = FastAPI()
        calls, model_calls, writes = [], [], []
        mode = {'proposal': False}

        @app.get('/api/repair-management/records')
        async def records(scope: str):
            assert scope == 'D'
            calls.append(scope)
            return {'ok': True, 'data': {'scope': scope, 'total': 2, 'records': [{'record_id': 'fixture-repair-1', 'scope': scope, 'title': '隔离维修1'}, {'record_id': 'fixture-repair-2', 'scope': scope, 'title': '隔离维修2'}]}}

        @app.post('/api/plan-convergence/rulesets')
        async def create_rules(payload: RuleCreate):
            writes.append(payload.model_dump())
            return {'ok': True, 'data': {'id': 42, **payload.model_dump()}}

        @app.post('/v1/chat/completions')
        async def completions(request: Request):
            assert request.headers['authorization'] == 'Bearer synthetic-test-key'
            payload = await request.json()
            model_calls.append(payload)
            if mode.get('auth_failure'):
                return JSONResponse({'error': {'message': 'invalid key: synthetic-test-key', 'type': 'invalid_api_key'}}, status_code=401)
            print('[FixtureModel] request', len(model_calls), 'tools', len(payload.get('tools', [])), flush=True)
            last_user = max(index for index, item in enumerate(payload['messages']) if item.get('role') == 'user')
            found = any(item.get('role') == 'tool' for item in payload['messages'][last_user + 1:])
            tool = {'id': 'fixture-call-1', 'type': 'function', 'function': {'name': 'lighthouse_query', 'arguments': json.dumps({'api_id': 'GET /api/repair-management/records', 'params': {'scope': 'D'}})}}
            message = {'role': 'assistant', 'content': 'D楼维修项目共2条。[1]'} if found else {'role': 'assistant', 'content': None, 'tool_calls': [tool]}
            if mode['proposal']:
                tool = {'id': 'fixture-prepare-1', 'type': 'function', 'function': {'name': 'lighthouse_prepare_business', 'arguments': json.dumps({
                    'title': '创建隔离规则集', 'operations': [{'api_id': 'POST /api/plan-convergence/rulesets',
                        'body': {'name': '隔离验收规则', 'description': '仅合成测试'}}]})}}
                message = {'role': 'assistant', 'content': '请核对后确认。'} if found else {'role': 'assistant', 'content': None, 'tool_calls': [tool]}
            if not payload.get('stream'):
                return {'id': 'fixture-completion', 'object': 'chat.completion', 'created': int(time.time()), 'model': 'fixture', 'choices': [{'index': 0, 'message': message, 'finish_reason': 'stop' if found else 'tool_calls'}], 'usage': {'prompt_tokens': 100, 'completion_tokens': 20, 'total_tokens': 120}}
            async def events():
                delta = message if found else {'role': 'assistant', 'tool_calls': [{'index': 0, **tool}]}
                chunk = {'id': 'fixture-completion', 'object': 'chat.completion.chunk', 'created': int(time.time()), 'model': 'fixture', 'choices': [{'index': 0, 'delta': delta, 'finish_reason': None}]}
                yield 'data: ' + json.dumps(chunk) + '\n\n'
                chunk['choices'] = [{'index': 0, 'delta': {}, 'finish_reason': 'stop' if found else 'tool_calls'}]
                yield 'data: ' + json.dumps(chunk) + '\n\n'
                yield 'data: [DONE]\n\n'
            return StreamingResponse(events(), media_type='text/event-stream')

        port = free_port()
        portal = PortalAgent(assistant, PortalAPICatalog(app), LighthouseFiles(store))
        engine = LighthouseOpenClaw(portal, bridge_url=lambda: f'http://127.0.0.1:{port}/api/assistant/openclaw-tools',
            state_root=Path(directory) / 'runtime', runtime_root=Path(os.environ.get('LIGHTHOUSE_PROBE_RUNTIME', root / 'build_output/lighthouse_openclaw_verified')))
        from lan_bitable_template_portal.lighthouse_runtime import install_model_route
        install_model_route(app, engine.manager)

        @app.post('/api/assistant/openclaw-tools')
        async def bridge(request: Request):
            try:
                payload = await request.json()
                print('[FixtureBridge]', payload.get('tool'), flush=True)
                result = await engine.bridge.call_shared(request.headers.get('authorization', '').removeprefix('Bearer '), payload, engine.manager)
                print('[FixtureBridge] ok', result.get('ok'), 'state', result.get('query_state'), flush=True)
                return result
            except Exception as exc:
                print('Bridge failure:', type(exc).__name__)
                return JSONResponse({'ok': False}, status_code=500)

        server = uvicorn.Server(uvicorn.Config(app, host='127.0.0.1', port=port, log_level='error'))
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        try:
            for _ in range(100):
                if server.started:
                    break
                await asyncio.sleep(.05)
            actor = {'id': 'isolated-agent', 'scopes': ['D'], 'is_admin': False}
            profile = {'id': 'fixture', 'name': 'Fixture', 'model': 'fixture', 'endpoint': f'http://127.0.0.1:{port}/v1/chat/completions', 'key_cipher': 'synthetic'}
            model.settings.return_value = {'configured': True, 'enabled': True, 'active_model_id': 'fixture',
                'models': [{'id': 'fixture', 'name': 'Fixture', 'model': 'fixture', 'configured': True}]}
            model.profile.return_value = profile
            turn = {'question': '查询D楼维修项目列表', 'operation_id': 'fixture-message-0001', 'run_id': 'fixture-run-0001', '_profile': profile, 'file_ids': []}
            request = Request({'type': 'http', 'scheme': 'http', 'server': ('127.0.0.1', port), 'client': ('127.0.0.1', 1), 'path': '/', 'root_path': '', 'query_string': b'', 'headers': [(b'origin', f'http://127.0.0.1:{port}'.encode())]})
            async def authorize():
                return actor.copy()
            async def emit(kind, value):
                if kind == 'status':
                    print(value['label'], flush=True)
            result = await engine.answer(actor, turn, [], request, emit, authorize, {})
            assert '2' in result['answer'] and result['sources'] and calls == ['D'], (result['answer'], calls)
            assert len(model_calls) >= 2
            actor['is_admin'] = True
            mode['proposal'] = True
            streams = LighthouseStream(portal, engine=engine)
            ack = await streams.submit(actor, {'question': '创建名为隔离验收规则的计划收敛规则集',
                'operation_id': 'fixture-message-0002', 'conversation_id': assistant._state(actor)['id']}, request, authorize)
            await asyncio.wait_for(streams.workers[actor['id']], 180)
            proposed = (await streams.conversation(actor))['turns'][-1]
            assert proposed['status'] == 'completed', proposed.get('error')
            assert proposed.get('plan'), proposed
            plan = portal.get_plan(actor, proposed['plan']['id'])
            assert plan['status'] == 'awaiting_confirmation', (plan['status'], plan.get('error'))
            assert not writes, 'native prepare must not write before human confirmation'
            reviewed = await portal.confirm(actor, plan['id'], {'version': plan['version'], 'stage': 'review'}, request)
            if reviewed['status'] == 'awaiting_second_confirmation':
                await portal.confirm(actor, plan['id'], {'version': reviewed['version'], 'stage': 'execute'}, request)
            if portal.tasks:
                await asyncio.gather(*tuple(portal.tasks))
            completed = portal.get_plan(actor, plan['id'])
            assert completed['status'] == 'completed', completed.get('error')
            assert len(writes) == 1
            restored = PortalAgent(assistant, PortalAPICatalog(app), LighthouseFiles(store))
            await restored.confirm(actor, plan['id'], {'version': plan['version'], 'stage': 'review'}, request)
            assert len(writes) == 1, 'retry after worker recreation must return the original completed result'
            print('[OpenClawAgent] Native proposal stayed unwritten until original confirmation; restart/repeated confirmation wrote once.')
            mode['proposal'] = False
            disconnected, submitted = [], []
            class DisconnectOnce(GatewayClient):
                async def request(self, method, params=None, **kwargs):
                    if method == 'agent':
                        submitted.append(params['idempotencyKey'])
                    return await super().request(method, params, **kwargs)
                async def next_event(self, timeout=None):
                    frame = await super().next_event(timeout)
                    data = frame.get('payload') or {}
                    if not disconnected and data.get('stream') == 'lifecycle' and (data.get('data') or {}).get('phase') == 'start':
                        disconnected.append(True)
                        await self.close()
                        raise GatewayError('DISCONNECTED')
                    return frame
            reads_before = len(calls)
            with patch('lan_bitable_template_portal.lighthouse_openclaw.GatewayClient', DisconnectOnce):
                recovered = await engine.answer(actor, {**turn, 'operation_id': 'fixture-message-0003',
                    'run_id': 'fixture-run-0003'}, [], request, emit, authorize, {})
            assert disconnected and submitted == ['fixture-run-0003'], submitted
            assert len(calls) == reads_before + 1 and recovered['sources'] and '2' in recovered['answer']
            print('[OpenClawAgent] Real socket disconnect recovered the exact owned run without resubmitting agent RPC.')
            mode['auth_failure'] = True
            try:
                await engine.answer(actor, {**turn, 'question': '你好', 'operation_id': 'fixture-message-0004',
                    'run_id': 'fixture-run-0004'}, [], request, emit, authorize, {})
            except AssistantError as error:
                assert 'synthetic-test-key' not in str(error)
            else:
                raise AssertionError('provider auth failure must not complete as a successful answer')
            print('[OpenClawAgent] Provider auth error stayed private; scanning owned files next.')
            for call in model_calls:
                serialized = json.dumps(call)
                assert 'synthetic-test-key' not in serialized
                assert {tool['function']['name'] for tool in call.get('tools', [])} <= set('lighthouse_' + name for name in ('calculate', 'date_time', 'discover', 'query', 'notice_sends', 'read_query', 'repair_overview', 'event_notices', 'repair_followup_status', 'guard_task_status', 'pending_work', 'search_local', 'read_file', 'parse_notice', 'search_history', 'prepare_business', 'public_search', 'weather', 'read_skill'))
            print('[OpenClawAgent] Actual typed tool loop, authorized source, and streaming verified.')
            for file in Path(directory).rglob('*'):
                if file.is_file() and file.stat().st_size <= 30 * 1024 * 1024:
                    assert b'synthetic-test-key' not in file.read_bytes(), file.name
            print('[OpenClawAgent] No plaintext key persisted in account files.')
        except Exception:
            for file in Path(directory).rglob('gateway.log'):
                print(file.read_text(encoding='utf-8', errors='replace')[-4000:])
            raise
        finally:
            await engine.close()
            server.should_exit = True
            await asyncio.to_thread(thread.join, 5)


if __name__ == '__main__':
    asyncio.run(check())

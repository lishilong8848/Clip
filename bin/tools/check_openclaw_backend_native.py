"""Actual resident Python host + Node gateway + isolated portal/model acceptance."""
from __future__ import annotations

import argparse
import asyncio
from contextlib import closing
import json
import os
import re
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from lan_bitable_template_portal.lighthouse_routes import install_lighthouse_routes
from lan_bitable_template_portal.state_store import LanPortalStateStore
from openclaw_service.assistant.lighthouse_ai import protect_key
from openclaw_service.assistant.lighthouse_runtime import free_port
from openclaw_service.protocol import PROJECT, control_key, descriptor, process_stamp
from openclaw_service.client import ResidentRuntime


class RuleCreate(BaseModel):
    name: str
    description: str = ''


class Listener:
    def __init__(self, app, port):
        self.server = uvicorn.Server(uvicorn.Config(app, host='127.0.0.1', port=port,
            log_level='error', access_log=False, use_colors=False, timeout_graceful_shutdown=3))
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    async def start(self):
        self.thread.start()
        deadline = time.monotonic() + 15
        while not self.server.started and self.thread.is_alive() and time.monotonic() < deadline:
            await asyncio.sleep(.05)
        assert self.server.started, 'Isolated listener did not start'

    async def close(self):
        self.server.should_exit = True
        await asyncio.to_thread(self.thread.join, 5)
        assert not self.thread.is_alive(), 'Isolated listener did not stop'


def process_resources(pid):
    import win32api
    import win32con
    import win32process
    handle = win32api.OpenProcess(win32con.PROCESS_QUERY_INFORMATION | win32con.PROCESS_VM_READ, False, pid)
    try:
        times = win32process.GetProcessTimes(handle)
        memory = win32process.GetProcessMemoryInfo(handle)
        return {'cpu_seconds': times['KernelTime'] / 10_000_000 + times['UserTime'] / 10_000_000,
            'working_set_mib': memory['WorkingSetSize'] / 1024**2,
            'private_mib': memory['PagefileUsage'] / 1024**2,
            'priority': win32process.GetPriorityClass(handle)}
    finally:
        handle.Close()


async def check(runtime, *, browser_check=False):
    control_loop = asyncio.get_running_loop()
    with tempfile.TemporaryDirectory(prefix='resident-native-') as directory:
        root = Path(directory)
        state = root / 'data/lighthouse_openclaw'
        legacy = LanPortalStateStore(root / 'data/lan_portal_state.sqlite3')
        actor_id = 'isolated-resident-account'
        provider_port = free_port()
        reads, writes, model_calls = [], [], []
        delayed_calls = set()
        mode = {'proposal': False, 'delay': 0, 'generation': 0}
        provider = FastAPI()

        @provider.post('/v1/chat/completions')
        async def completion(request: Request):
            assert request.headers.get('authorization') == 'Bearer synthetic-native-key'
            payload = await request.json()
            model_calls.append(payload)
            if mode['delay']:
                task = asyncio.current_task()
                delayed_calls.add(task)
                try:
                    await asyncio.sleep(mode['delay'])
                except asyncio.CancelledError:
                    return JSONResponse({'error': 'Isolated provider stopped'}, status_code=499)
                finally:
                    delayed_calls.discard(task)
            last_user = max(index for index, item in enumerate(payload['messages']) if item.get('role') == 'user')
            found = any(item.get('role') == 'tool' for item in payload['messages'][last_user + 1:])
            if mode['proposal']:
                tool = {'id': 'prepare-' + str(len(model_calls)), 'type': 'function', 'function': {
                    'name': 'lighthouse_prepare_business', 'arguments': json.dumps({
                        'title': '创建隔离规则集', 'operations': [{'api_id': 'POST /api/plan-convergence/rulesets',
                            'body': {'name': '隔离规则', 'description': '合成验收，不写业务云端'}}]}, ensure_ascii=False)}}
                answer = '请核对后确认。'
            else:
                tool = {'id': 'query-' + str(len(model_calls)), 'type': 'function', 'function': {
                    'name': 'lighthouse_query', 'arguments': json.dumps({
                        'api_id': 'GET /api/repair-management/records', 'params': {'scope': 'D'}})}}
                answer = 'D楼维修项目共2条。[1]'
            message = {'role': 'assistant', 'content': answer} if found else {'role': 'assistant', 'content': None, 'tool_calls': [tool]}
            if not payload.get('stream'):
                return {'id': 'native-fixture', 'object': 'chat.completion', 'model': 'fixture', 'created': int(time.time()),
                    'choices': [{'index': 0, 'message': message, 'finish_reason': 'stop' if found else 'tool_calls'}],
                    'usage': {'prompt_tokens': 100, 'completion_tokens': 20, 'total_tokens': 120}}
            async def events():
                delta = message if found else {'role': 'assistant', 'tool_calls': [{'index': 0, **tool}]}
                chunk = {'id': 'native-fixture', 'object': 'chat.completion.chunk', 'created': int(time.time()),
                    'model': 'fixture', 'choices': [{'index': 0, 'delta': delta, 'finish_reason': None}]}
                yield 'data: ' + json.dumps(chunk, ensure_ascii=False) + '\n\n'
                chunk['choices'] = [{'index': 0, 'delta': {}, 'finish_reason': 'stop' if found else 'tool_calls'}]
                yield 'data: ' + json.dumps(chunk) + '\n\ndata: [DONE]\n\n'
            return StreamingResponse(events(), media_type='text/event-stream')

        # Deliberately seeded test-only loopback model; production settings still
        # reject loopback URLs. No production provider or credentials are used.
        profile = {'id': 'fixture', 'name': 'Fixture', 'model': 'fixture',
            'endpoint': f'http://127.0.0.1:{provider_port}/v1/chat/completions', 'key_cipher': protect_key('synthetic-native-key')}
        profiles = [profile, {**profile, 'id': 'alternate', 'name': 'Alternate', 'model': 'fixture-alternate'}]
        legacy.put_document('lighthouse_ai', 'model:' + actor_id, {'enabled': True, 'active_model_id': 'fixture', 'models': profiles})
        course = root / 'native-course.txt'
        course.write_text('isolated original learning material', encoding='utf-8')
        def attachment_allowed(meta, actor):
            assert actor['id'] == actor_id and actor['scopes'] == ['D']
        learning = SimpleNamespace(_get=lambda kind, identity: {'question_id': 'native-course', 'kind': 'material', 'name': 'course.txt'}
            if kind == 'attachment' and identity == 'native-course' else None,
            _attachment_allowed=attachment_allowed, attachment=lambda *_: (course, 'course.txt', 'text/plain'))
        provider_listener = Listener(provider, provider_port)
        portal_listener = None
        child = None
        console_path = root / 'console.log'

        async def start_portal(port=None):
            app = FastAPI()
            port = port or free_port()
            session = {'open_id': actor_id, 'scopes': ['D'], 'is_admin': True}
            controller = SimpleNamespace(bound_port=port, preferred_port=port,
                _current_session=lambda request: session if request.cookies.get('sid') == 'isolated' else None,
                _request_base_url=lambda request: f'http://127.0.0.1:{port}/')
            portal_runtime = SimpleNamespace(state_store=legacy, auth_manager=SimpleNamespace(
                session_scopes=lambda value: value['scopes'], is_admin=lambda value: value['is_admin']), learning_service=learning)
            install_lighthouse_routes(app, controller, portal_runtime)
            health_client = await asyncio.to_thread(httpx.AsyncClient, trust_env=False, timeout=3)
            app.add_event_handler('shutdown', health_client.aclose)
            @app.get('/api/repair-management/records')
            async def records(scope: str):
                assert scope == 'D'
                reads.append(scope)
                return {'ok': True, 'data': {'scope': scope, 'total': 2, 'records': [
                    {'record_id': 'fixture-1', 'scope': scope, 'title': '隔离维修1'},
                    {'record_id': 'fixture-2', 'scope': scope, 'title': '隔离维修2'}]}}
            @app.post('/api/plan-convergence/rulesets')
            async def create(payload: RuleCreate):
                writes.append(payload.model_dump())
                return {'ok': True, 'data': {'id': 42, **payload.model_dump()}}
            @app.get('/api/health')
            async def health():
                current = descriptor(state, PROJECT)
                value = None
                if current:
                    reply = await health_client.post(f"http://127.0.0.1:{current['port']}/health",
                        json={'protocol': 1, 'instance': current['instance']},
                        headers={'Authorization': 'Bearer ' + control_key(state)})
                    value = reply.json().get('data', {})
                return {'ok': True, 'instance_id': 'isolated-lighthouse-resident', 'generation': mode['generation'],
                    'service_pid': current['pid'] if current else None, 'gateways': (value or {}).get('gateways', []),
                    'model_calls': len(model_calls), 'writes': len(writes)}
            @app.get('/api/ping')
            async def ping():
                return {'ok': True, 'instance_id': 'isolated-lighthouse-resident'}
            @app.post('/__fixture/mode')
            async def fixture_mode(request: Request):
                assert request.cookies.get('sid') == 'isolated'
                payload = await request.json()
                assert set(payload) <= {'delay'} and type(payload.get('delay', 0)) is int and 0 <= payload.get('delay', 0) <= 20
                mode['delay'] = payload.get('delay', 0)
                return {'ok': True, 'model_calls': len(model_calls)}
            @app.post('/__fixture/restart')
            async def fixture_restart(request: Request):
                assert request.cookies.get('sid') == 'isolated'
                async def restart():
                    nonlocal portal_listener
                    await asyncio.sleep(.2)
                    await portal_listener.close()
                    portal_listener = None
                    mode['generation'] += 1
                    portal_listener, _ = await start_portal(port)
                # The old portal's loop is about to close; the fixture controller
                # must own its restart, just as the real main launcher does.
                asyncio.run_coroutine_threadsafe(restart(), control_loop)
                return {'ok': True}
            if browser_check:
                dist = PROJECT / 'bin/lan_bitable_template_portal/frontend/dist'
                app.mount('/assets', StaticFiles(directory=dist / 'assets'), name='native_fixture_assets')
                @app.get('/')
                async def widget(request: Request):
                    assert request.cookies.get('sid') == 'isolated'
                    html = (dist / 'assistant.html').read_text(encoding='utf-8').replace(
                        '<div id="clipflow-lighthouse-widget">',
                        '<div id="clipflow-lighthouse-widget" data-user-id="' + actor_id + '" data-user-name="隔离验收">')
                    return HTMLResponse(html)
            listener = Listener(app, port)
            await listener.start()
            return listener, f'http://127.0.0.1:{port}'

        def data(response):
            assert response.status_code in (200, 202), (response.status_code, response.text[:500])
            value = response.json()
            assert value.get('ok') is True, value
            return value['data']

        async def timed_query(browser, base, payload):
            before = process_resources(child.pid)
            began = time.monotonic()
            task = asyncio.create_task(browser.post(base + '/api/assistant/messages', headers={'Origin': base}, json=payload))
            latencies = []
            while not task.done():
                tick = time.monotonic()
                response = await browser.get(base + '/api/ping', timeout=3)
                assert response.status_code == 200
                latencies.append(time.monotonic() - tick)
                await asyncio.sleep(.1)
            ack = data(await task)
            observer = asyncio.create_task(settled(browser, base, ack['run_id']))
            while not observer.done():
                tick = time.monotonic()
                response = await browser.get(base + '/api/ping', timeout=3)
                assert response.status_code == 200
                latencies.append(time.monotonic() - tick)
                await asyncio.sleep(.1)
            turn = await observer
            duration = time.monotonic() - began
            after = process_resources(child.pid)
            p95 = sorted(latencies)[int((len(latencies) - 1) * .95)]
            print(f'[ResidentPerformance] samples={len(latencies)} portal_p95_ms={p95 * 1000:.1f} '
                f'portal_max_ms={max(latencies) * 1000:.1f} host_cpu_core_pct=' 
                f'{(after["cpu_seconds"] - before["cpu_seconds"]) / duration * 100:.1f} '
                f'host_private_mib={after["private_mib"]:.1f}', flush=True)
            assert max(latencies) < 1, 'Assistant blocked isolated portal responses for a second'
            return ack, turn, duration

        async def settled(browser, base, run):
            async with browser.stream('GET', base + '/api/assistant/runs/' + run + '/stream') as response:
                assert response.status_code == 200, await response.aread()
                saw_text = False
                async for line in response.aiter_lines():
                    if line.startswith('data: ') and 'text-delta' in line:
                        saw_text = True
                assert saw_text, 'Real SSE did not return an assistant text delta'
            conversation = data(await browser.get(base + '/api/assistant/conversation'))
            turn = next(item for item in conversation['turns'] if item.get('run_id') == run)
            assert turn['status'] == 'completed', turn.get('error')
            return turn

        try:
            await provider_listener.start()
            with console_path.open('wb') as console:
                child = subprocess.Popen([sys.executable, '-B', '-u', '-m', 'openclaw_service',
                    '--state-root', str(state), '--runtime-root', str(runtime), '--parent-pid', str(os.getpid())], cwd=PROJECT,
                    env={**os.environ, 'PYTHONPATH': str(PROJECT / 'bin'), 'LIGHTHOUSE_AGENT_ENGINE': 'openclaw'},
                    stdout=console, stderr=subprocess.STDOUT, creationflags=subprocess.CREATE_NO_WINDOW)
                deadline = time.monotonic() + 30
                saved = None
                while saved is None and child.poll() is None and time.monotonic() < deadline:
                    saved = descriptor(state, PROJECT)
                    await asyncio.sleep(.1)
                assert saved is not None, 'Real service did not start'
                service_url = f"http://127.0.0.1:{saved['port']}"
                control = {'Authorization': 'Bearer ' + control_key(state)}
                envelope = {'protocol': 1, 'instance': saved['instance']}
                async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=230,
                    cookies={'sid': 'isolated'}) as browser:
                    portal_listener, base = await start_portal()
                    conversation = data(await browser.get(base + '/api/assistant/conversation'))
                    conversation_id = conversation['conversation_id']
                    material = data(await browser.get(base + '/api/assistant/question-material', params={'material_id': 'native-course'}))
                    assert material['text'] == 'isolated original learning material' and course.read_text() == material['text']
                    ack, first, cold = await timed_query(browser, base, {
                        'question': '查询D楼维修项目列表', 'conversation_id': conversation_id, 'operation_id': 'native-query-0001'})
                    assert '2' in first['answer'] and first['sources'] and reads == ['D']
                    health = data(await browser.post(service_url + '/health', json=envelope, headers=control))
                    assert health['accounts'] == 1
                    gateway = health['gateways'][0]['pid']
                    if os.environ.get('OPENCLAW_GATEWAY_STARTUP_TRACE') == '1':
                        for log in state.glob('accounts/*/gateway.log'):
                            for stage, duration, total in re.findall(r'startup trace: ([a-zA-Z0-9_.-]+) ([0-9.]+)ms total=([0-9.]+)ms', log.read_text(encoding='utf-8', errors='replace')):
                                print(f'[GatewayStage] {stage} ms={duration} total_ms={total}', flush=True)
                    assert process_stamp(gateway) is not None
                    await portal_listener.close()
                    portal_listener = None
                    assert child.poll() is None and process_stamp(gateway) is not None, 'Portal shutdown killed resident gateway'
                    portal_listener, base = await start_portal()
                    conversation = data(await browser.get(base + '/api/assistant/conversation'))
                    assert conversation['conversation_id'] == conversation_id and conversation['turns'][0]['run_id'] == ack['run_id']
                    ack2, second, hot = await timed_query(browser, base, {
                        'question': '再查询D楼维修项目列表', 'conversation_id': conversation_id, 'operation_id': 'native-query-0002'})
                    assert '2' in second['answer'] and len(reads) == 2
                    health = data(await browser.post(service_url + '/health', json=envelope, headers=control))
                    assert health['gateways'][0]['pid'] == gateway, 'Portal restart cold-started the existing account gateway'
                    idle_start = {pid: process_resources(pid) for pid in (child.pid, gateway)}
                    began = time.monotonic()
                    await asyncio.sleep(3)
                    idle_end = {pid: process_resources(pid) for pid in idle_start}
                    duration = time.monotonic() - began
                    for pid, sample in idle_end.items():
                        assert sample['priority'] == 0x4000, 'Owned assistant process was not below-normal priority'
                        cpu = (sample['cpu_seconds'] - idle_start[pid]['cpu_seconds']) / duration * 100
                        print(f'[ResidentIdle] role={"host" if pid == child.pid else "gateway"} '
                            f'cpu_core_pct={cpu:.1f} private_mib={sample["private_mib"]:.1f} priority=below_normal', flush=True)
                    if browser_check:
                        frontend = PROJECT / 'bin/lan_bitable_template_portal/frontend'
                        check_process = await asyncio.create_subprocess_exec('node',
                            str(frontend / 'scripts/check-assistant-resident-browser.mjs'), cwd=frontend,
                            env={**os.environ, 'LIGHTHOUSE_RESIDENT_URL': base})
                        assert await check_process.wait() == 0, 'Independent backend PC acceptance failed'
                        mode['delay'] = 0
                        health = data(await browser.post(service_url + '/health', json=envelope, headers=control))
                        gateway = health['gateways'][0]['pid']
                    mode['proposal'] = True
                    ack3 = data(await browser.post(base + '/api/assistant/messages', headers={'Origin': base}, json={
                        'question': '创建名为隔离规则的计划收敛规则集', 'conversation_id': conversation_id, 'operation_id': 'native-proposal-0003'}))
                    proposed = await settled(browser, base, ack3['run_id'])
                    assert proposed.get('plan') and not writes, 'Model must not execute native business before confirmation'
                    plan_id = proposed['plan']['id']
                    plan = data(await browser.get(base + '/api/assistant/plans/' + plan_id))
                    reviewed = data(await browser.post(base + '/api/assistant/plans/' + plan_id + '/confirm', headers={'Origin': base},
                        json={'version': plan['version'], 'stage': 'review'}))
                    if reviewed['status'] == 'awaiting_second_confirmation':
                        data(await browser.post(base + '/api/assistant/plans/' + plan_id + '/confirm', headers={'Origin': base},
                            json={'version': reviewed['version'], 'stage': 'execute'}))
                    deadline = time.monotonic() + 25
                    while time.monotonic() < deadline:
                        completed = data(await browser.get(base + '/api/assistant/plans/' + plan_id))
                        if completed['status'] not in {'running', 'submitted'}:
                            break
                        await asyncio.sleep(.1)
                    assert completed['status'] == 'completed' and len(writes) == 1, completed.get('error')
                    data(await browser.post(base + '/api/assistant/plans/' + plan_id + '/confirm', headers={'Origin': base},
                        json={'version': plan['version'], 'stage': 'review'}))
                    assert len(writes) == 1, 'Repeated confirmation duplicated business write'
                    stop = time.monotonic()
                    data(await browser.post(service_url + '/shutdown', json={**envelope, 'reason': 'manual'}, headers=control))
                    await asyncio.to_thread(child.wait, 5)
                    elapsed = time.monotonic() - stop
                    assert process_stamp(gateway) is None and child.poll() == 0
                    health_response = await browser.get(base + '/api/health')
                    assert health_response.status_code == 200 and health_response.json()['ok'] is True
                    failure = await browser.get(base + '/api/assistant/conversation')
                    # The borrowed fixture intentionally forbids worker spawning.
                    # Production owners recover automatically instead of asking
                    # for the now-removed manual BAT.
                    assert failure.status_code == 503 and '启动' in failure.text
                    assert len(writes) == 1, 'Worker loss replayed a business write'
            with closing(sqlite3.connect(legacy.db_path)) as connection:
                main_count = connection.execute("SELECT COUNT(*) FROM json_documents WHERE namespace LIKE 'lighthouse_%'").fetchone()[0]
            assert main_count == 1, 'Main portal wrote assistant state after the one seeded migration input'
            assert (state / 'assistant.sqlite3').is_file()
            assert len(model_calls) >= 2 and any(item.get('tools') for item in model_calls)
            assert any(message.get('role') == 'tool' for item in model_calls for message in item['messages'])
            for file in state.rglob('*'):
                if file.is_file() and file.stat().st_size < 30 * 1024 * 1024:
                    assert b'synthetic-native-key' not in file.read_bytes(), 'Plaintext model key persisted'
            print(f'[ResidentNative] cold_ms={cold * 1000:.1f} hot_ms={hot * 1000:.1f} stop_ms={elapsed * 1000:.1f}; '
                'real proxy/SSE/native query/portal restart/gateway reuse/human-confirmed write/private storage passed', flush=True)
        except BaseException:
            if console_path.exists():
                print(console_path.read_text(encoding='utf-8', errors='replace')[-2500:], flush=True)
            raise
        finally:
            if portal_listener is not None:
                await portal_listener.close()
            for task in tuple(delayed_calls):
                task.get_loop().call_soon_threadsafe(task.cancel)
            await provider_listener.close()
            if child is not None and child.poll() is None:
                child.terminate()
                await asyncio.to_thread(child.wait, 5)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--runtime', type=Path, default=PROJECT / 'build_output/lighthouse_openclaw_verified')
    parser.add_argument('--browser', action='store_true')
    args = parser.parse_args()
    # This proxy probe borrows its explicit fixture worker; production portals
    # own their workers. The integrated lifecycle has its own native probe.
    def fixture_client(*values, **options):
        def forbidden_launch(*_):
            raise AssertionError('The proxy fixture must not start a second worker')
        return ResidentRuntime(*values, **options, launch=forbidden_launch)
    with patch('openclaw_service.client.ResidentRuntime', side_effect=fixture_client):
        asyncio.run(check(args.runtime, browser_check=args.browser))

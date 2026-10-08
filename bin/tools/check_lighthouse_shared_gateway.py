"""Real shared Node gateway acceptance with twenty synthetic local providers.

No configured credentials, production databases, Feishu or public APIs are used.
Run explicitly from the repository with the project Python interpreter.
"""
import asyncio
import json
import os
import sys
import tempfile
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_openclaw import BusinessBridge, LighthouseOpenClaw, OpenClawToolAgent
from openclaw_service.assistant.lighthouse_runtime import OpenClawRuntime, free_port, install_model_route


async def run(account_count=20, *, compaction=False, replay=False, faults=False, legacy_state=False,
              read_profile=False, cpu_profile=False, no_cache_respawn=False, diagnose_runs=False,
              lose_terminal_response=False, startup_profile=False, cache_profile=False, flush_cache=False, spawn_profile=False, images=False):
    if not 2 <= account_count <= 20:
        raise ValueError('Fixture account count must be between 2 and 20')
    os.environ['OPENCLAW_GATEWAY_STARTUP_TRACE'] = '1'
    project = Path(__file__).resolve().parents[2]
    parent = project / 'build_output/agent_probes'
    parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='shared-', dir=parent) as directory:
        app, port = FastAPI(), free_port()
        manager = OpenClawRuntime(Path(directory) / 'accounts')
        from openclaw_service.assistant import lighthouse_runtime as runtime_module
        original_popen, native_launches = runtime_module.subprocess.Popen, []
        timelines = []
        def launch(*args, **kwargs):
            env = kwargs.get('env', {})
            if env.get('OPENCLAW_CONFIG_PATH') == str(manager.root / 'shared-gateway/openclaw.json'):
                argv = list(args[0])
                flags = []
                if read_profile or cpu_profile or cache_profile or spawn_profile:
                    flags.extend(['--import', (Path(__file__).with_name('openclaw_startup_profile.mjs')).as_uri()])
                    kwargs['env'] = {**env, 'LIGHTHOUSE_PROFILE_CODE_ROOT': str(manager.prepared[1].parent / 'dist'),
                        'LIGHTHOUSE_PROFILE_READS': '1' if read_profile else '0', 'LIGHTHOUSE_PROFILE_CPU': '1' if cpu_profile else '0',
                        'LIGHTHOUSE_PROFILE_COMPILE_CACHE': '1' if cache_profile else '0',
                        'LIGHTHOUSE_FLUSH_COMPILE_CACHE': '1' if flush_cache else '0',
                        'LIGHTHOUSE_PROFILE_SPAWNS': '1' if spawn_profile else '0'}
                if no_cache_respawn:
                    kwargs['env'] = {**kwargs.get('env', env), 'OPENCLAW_PACKAGED_COMPILE_CACHE_RESPAWNED': '1'}
                if startup_profile:
                    timeline = Path(directory) / f'startup-{len(native_launches)}.jsonl'
                    timelines.append(timeline)
                    kwargs['env'] = {**kwargs.get('env', env), 'OPENCLAW_DIAGNOSTICS': '1',
                        'OPENCLAW_DIAGNOSTICS_TIMELINE_PATH': str(timeline)}
                args = ([argv[0], *flags, *argv[1:]], *args[1:])
                if legacy_state and not native_launches:
                    kwargs['env'] = {**kwargs.get('env', env), 'OPENCLAW_STATE_DIR': str(manager.root / 'shared-gateway')}
                    print('[SharedGateway] simulating previous state boundary on first launch', flush=True)
                native_launches.append(kwargs['env']['OPENCLAW_STATE_DIR'])
            return original_popen(*args, **kwargs)
        process_patch = patch.object(runtime_module.subprocess, 'Popen', launch)
        process_patch.start()
        diagnostic_patches = []
        lost_terminal = []
        if diagnose_runs or lose_terminal_response:
            from openclaw_service.assistant.lighthouse_gateway import GatewayClient
            original_request, original_event = GatewayClient.request, GatewayClient.next_event
            async def traced_request(client, method, *args, **kwargs):
                if method not in {'agent', 'agent.wait'}:
                    return await original_request(client, method, *args, **kwargs)
                params = args[0] if args else kwargs.get('params') or {}
                run_id = params.get('runId') or params.get('idempotencyKey')
                started = time.monotonic()
                print('[RunDiagnostic] request', method, run_id, flush=True)
                try:
                    result = await original_request(client, method, *args, **kwargs)
                    if lose_terminal_response and not lost_terminal and method == 'agent.wait' \
                            and run_id == agents[0].run_id and run_id.endswith(':same-operation-across-accounts'):
                        lost_terminal.append(run_id)
                        print('[RunDiagnostic] discarded completed observer response once', flush=True)
                        raise asyncio.TimeoutError()
                except BaseException as exc:
                    print('[RunDiagnostic] request_failed', method, run_id, type(exc).__name__, flush=True)
                    raise
                print('[RunDiagnostic] response', method, run_id, result.get('status'),
                    (result.get('terminalReply') or {}).get('disposition'), 'seconds=', round(time.monotonic() - started, 3), flush=True)
                return result
            async def traced_event(client, *args, **kwargs):
                result = await original_event(client, *args, **kwargs)
                payload = result.get('payload') or {}
                data = payload.get('data') or {}
                if payload.get('stream') == 'lifecycle':
                    print('[RunDiagnostic] lifecycle', payload.get('runId'), data.get('phase'), payload.get('seq'), flush=True)
                return result
            for method, implementation in [('request', traced_request), ('next_event', traced_event)]:
                patched = patch.object(GatewayClient, method, implementation)
                patched.start()
                diagnostic_patches.append(patched)
        engine = SimpleNamespace(manager=manager, tokens={}, bridge=BusinessBridge(), gateways={}, closing=False,
            assistant=SimpleNamespace(_state=lambda actor: {'id': 'conv-' + actor['id']}, model_for=lambda _: Mock(unprotect=lambda cipher: cipher)),
            wait_warmup=AsyncMock(), bridge_url=lambda: f'http://127.0.0.1:{port}/api/assistant/openclaw-tools')
        for name in ('gateway_ready', 'gateway_client', 'keep_gateway'):
            setattr(engine, name, getattr(LighthouseOpenClaw, name).__get__(engine))
        install_model_route(app, manager)
        arrivals, requests, tool_calls = set(), [], []
        image_owners = set()
        memory = 'SYNTHETIC_MEMORY_00=7362'
        compact = {'fill': False, 'summaries': 0, 'events': 0, 'recalls': 0,
                   'summaries_with_memory': 0, 'overflow_rejections': 0, 'event_owners': set()}
        release = threading.Event()
        held, release_held = threading.Event(), threading.Event()
        concurrent_phase = {'enabled': False}
        fault_phase = {'enabled': False}

        @app.post('/api/assistant/openclaw-tools')
        async def bridge(request: Request):
            payload = await request.json()
            tool_calls.append((payload['agent_id'], payload['session_key'], payload['run_id']))
            return await engine.bridge.call_shared(request.headers.get('authorization', '').removeprefix('Bearer '), payload, manager)

        @app.post('/provider/{index}/v1/chat/completions')
        async def provider(index: int, request: Request):
            payload = await request.json()
            assert request.headers.get('authorization') == f'Bearer synthetic-key-{index}'
            assert payload['model'] == f'fixture-model-{index}'
            owner = index % 20
            serialized = json.dumps(payload.get('messages', []))
            if 'data:image/png;base64,' in serialized:
                assert owner == 0, 'Image crossed account boundary'
                image_owners.add(owner)
            assert all(f'fixture-owner-{other:02d}' not in serialized for other in range(20) if other != owner), 'Cross-account history'
            assert owner == 0 or memory not in serialized, 'Cross-account compacted memory'
            if index == 20:
                assert 'fixture-owner-00 complete' in serialized, 'History lost on model switch'
            requests.append(index)
            print('[SharedGateway] model arrival', index, 'total=', len(requests), flush=True)
            if fault_phase['enabled'] and owner == 0:
                return JSONResponse({'error': {'message': f'synthetic-key-{index} private-provider-detail'}}, status_code=401)
            last = payload['messages'][-1]
            tool_result = last.get('role') == 'tool'
            if owner == 0 and last.get('role') == 'user' and 'STOP-PROBE' in json.dumps(last):
                held.set()
                await asyncio.to_thread(release_held.wait, 45)
            if concurrent_phase['enabled'] and not tool_result:
                arrivals.add(index)
                if len(arrivals) == account_count:
                    release.set()
                assert await asyncio.to_thread(release.wait, 45), 'Model requests were serialized'
            tool_requested = any(t['function']['name'] == 'lighthouse_query_owner' for t in payload.get('tools', []))
            text = f'fixture-owner-{owner:02d} complete'
            summarizing = any(message.get('role') == 'system' and 'context summarization assistant' in
                              json.dumps(message.get('content', '')).lower() for message in payload['messages'])
            if compaction and owner == 0 and not summarizing and not compact['overflow_rejections'] and \
                    last.get('role') == 'user' and 'PROVIDER-OVERFLOW-PROBE' in json.dumps(last):
                compact['overflow_rejections'] += 1
                return JSONResponse({'error': {'code': 'context_length_exceeded',
                    'message': f'synthetic-key-{index} maximum context length exceeded'}}, status_code=400)
            if summarizing:
                assert compaction and owner == 0, 'Unexpected compaction owner'
                compact['summaries'] += 1
                tool_requested = False
                text = (f'## Decisions\nfixture-owner-{owner:02d} keeps private context.\n\n'
                        '## Open TODOs\nCall query_owner and answer briefly. Continue the context fixture.\n\n'
                        '## Constraints/Rules\nUse only the authorized query_owner tool.\n\n'
                        '## Pending user asks\nNone.\n\n'
                        f'## Exact identifiers\nfixture-owner-{owner:02d}\n')
                if memory in serialized:
                    compact['summaries_with_memory'] += 1
                    text += memory
            elif owner == 0 and 'MEMORY-PROBE' in json.dumps(last):
                assert memory in serialized, 'Memory lost after native compaction'
                compact['recalls'] += 1
                text += '\n' + memory
            elif compact['fill'] and owner == 0 and tool_result:
                text += '\n' + 'context detail ' * 1200
            if tool_result:
                assert f'fixture-owner-{owner:02d}' in last['content'], 'Wrong callback owner'

            async def chunks():
                if tool_requested and not tool_result:
                    delta = {'tool_calls': [{'index': 0, 'id': f'fixture-call-{index}-{len(requests)}', 'type': 'function',
                        'function': {'name': 'lighthouse_query_owner', 'arguments': '{}'}}]}
                    finish = 'tool_calls'
                else:
                    delta, finish = {'content': text}, 'stop'
                for value, reason in (({'role': 'assistant', **delta}, None), ({}, finish)):
                    frame = {'id': f'fixture-{index}', 'object': 'chat.completion.chunk',
                        'created': int(time.time()), 'model': payload['model'],
                        'choices': [{'index': 0, 'delta': value, 'finish_reason': reason}]}
                    if reason:
                        prompt_tokens = max(1, len(serialized) // 4)
                        output_tokens = 40 if finish == 'tool_calls' else max(1, len(text) // 4)
                        frame['usage'] = {'prompt_tokens': prompt_tokens, 'completion_tokens': output_tokens,
                                          'total_tokens': prompt_tokens + output_tokens}
                    yield 'data: ' + json.dumps(frame) + '\n\n'
                yield 'data: [DONE]\n\n'
            return StreamingResponse(chunks(), media_type='text/event-stream')

        server = uvicorn.Server(uvicorn.Config(app, host='127.0.0.1', port=port, log_level='error'))
        server_task = asyncio.create_task(server.serve())
        def emitter(owner):
            async def emit(kind, value):
                if kind == 'status' and value.get('label') == '正在自动整理上下文':
                    compact['events'] += 1
                    compact['event_owners'].add(owner)
            return emit
        actors, agents, tasks = [], [], []
        for index in range(account_count):
            actor = {'id': f'fixture-owner-{index:02d}', 'scopes': ['ABCDE'[index % 5]]}
            actors.append(actor)
            profile = {'id': f'profile-{index}', 'model': f'fixture-model-{index}', 'name': 'Fixture',
                'endpoint': f'http://127.0.0.1:{port}/provider/{index}/v1/chat/completions',
                'key_cipher': f'synthetic-key-{index}', 'context_window': 32000}
            agent = OpenClawToolAgent(engine, actor, {'operation_id': 'same-operation-across-accounts'}, emitter(index), profile,
                instructions='Call query_owner once, then return its account name. Do not use other tools.')
            # No caller-controlled identity parameter in the tool schema.
            def bind(identity):
                async def query_owner(): return {'account': identity}
                return query_owner
            agent.tool_plain(bind(actor['id']))
            agents.append(agent)
        async def answer(agent, question='Call query_owner and answer briefly.', attachments=()):
            result = None
            async with agent.run_stream_events([question, *attachments], message_history=[]) as events:
                async for event in events:
                    if event.event_kind == 'agent_run_result':
                        result = event.result.output
            assert agent.actor['id'] in (result or ''), 'Missing isolated answer'
            return result
        try:
            for _ in range(100):
                if server.started: break
                await asyncio.sleep(.05)
            assert server.started
            start = time.monotonic()
            tasks = [asyncio.create_task(LighthouseOpenClaw.prepare(engine)),
                     *(asyncio.create_task(agent.warmup()) for agent in agents)]
            await asyncio.gather(*tasks)
            assert manager.http is not None and not requests and not tool_calls, 'Preparation inferred or queried business data'
            print('[SharedGateway] account startup', account_count, 'seconds=', round(time.monotonic() - start, 2), flush=True)
            items = list(manager.accounts.values())
            assert len(items) == account_count
            assert len({item['process'].pid for item in items}) == len({item['port'] for item in items}) == 1
            assert len({item['agent_id'] for item in items}) == len({item['root'] for item in items}) == account_count
            if no_cache_respawn:
                from subprocess import check_output, CREATE_NO_WINDOW
                output = await asyncio.to_thread(check_output, ['powershell.exe', '-NoProfile', '-NonInteractive', '-Command',
                    f"Get-NetTCPConnection -LocalPort {items[0]['port']} -State Listen -ErrorAction Stop | Select-Object -ExpandProperty OwningProcess"],
                    creationflags=CREATE_NO_WINDOW, text=True, timeout=20)
                assert {int(value) for value in output.split()} == {items[0]['process'].pid}, 'Tracked Node process does not own the gateway listener'
                print('[SharedGateway] tracked process owns the gateway listener directly PASS', flush=True)
            if compaction:
                compact['fill'] = True
                for number in range(9):
                    agents[0].turn = {'operation_id': 'compact-fill-' + str(number)}
                    question = 'Call query_owner. Keep this memory: ' + memory if number == 0 else 'Call query_owner. Continue the context fixture.'
                    await asyncio.wait_for(answer(agents[0], question), 150)
                    print('[SharedGateway] long-history turn', number + 1, 'summaries=', compact['summaries'], flush=True)
                    if compact['summaries']:
                        break
                compact['fill'] = False
                assert compact['summaries'] > 0 and compact['summaries_with_memory'] > 0, 'Native automatic compaction lost memory'
                assert compact['events'] > 0, 'Native automatic compaction progress was not reported'
                agents[0].turn = {'operation_id': 'recall-after-compaction'}
                await answer(agents[0], 'MEMORY-PROBE Call query_owner and recall the original memory.')
                agents[1].turn = {'operation_id': 'other-owner-after-compaction'}
                await answer(agents[1])
                assert compact['event_owners'] == {0}, 'Compaction progress leaked to another account'
                print('[SharedGateway] native automatic compaction, memory and other-owner isolation PASS', flush=True)
                summaries = compact['summaries']
                agents[0].turn = {'operation_id': 'provider-overflow'}
                await answer(agents[0], 'PROVIDER-OVERFLOW-PROBE Call query_owner and answer briefly.')
                assert compact['overflow_rejections'] == 1 and compact['summaries'] > summaries, 'Provider overflow did not trigger native recovery'
                print('[SharedGateway] provider context rejection recovered through native compaction PASS', flush=True)
            agents[0].turn = {'operation_id': 'preflight'}
            await asyncio.wait_for(answer(agents[0]), 65)
            print('[SharedGateway] single-agent inference/callback PASS', flush=True)
            if replay:
                request_count, callback_count = len(requests), len(tool_calls)
                replay_started = time.monotonic()
                await asyncio.wait_for(answer(agents[0]), 10)
                assert len(requests) == request_count and len(tool_calls) == callback_count, 'Completed run replay repeated tools or inference'
                print('[SharedGateway] completed run replay returned original result without inference/callback PASS seconds=',
                      round(time.monotonic() - replay_started, 3), flush=True)
            for agent in agents:
                agent.turn = {'operation_id': 'same-operation-across-accounts'}
            tool_calls.clear()
            concurrent_phase['enabled'] = True
            provider_count = len(requests)
            start = time.monotonic()
            tasks = [asyncio.create_task(answer(agent)) for agent in agents]
            await asyncio.wait_for(asyncio.gather(*tasks), 90)
            assert len(arrivals) == account_count and len(tool_calls) == account_count
            if lose_terminal_response:
                assert len(lost_terminal) == 1 and len(requests) - provider_count == account_count * 2, 'Lost completion response repeated inference or tools'
                print('[SharedGateway] completion response loss recovered without new inference/callback PASS', flush=True)
            print('[SharedGateway] concurrent models/callbacks', account_count, 'PASS seconds=', round(time.monotonic() - start, 2), flush=True)
            concurrent_phase['enabled'] = False
            if images:
                import io
                from PIL import Image
                agents[0].profile = {**agents[0].profile, 'vision_verified': True}
                agents[0].turn = {'operation_id': 'image-input'}
                png = io.BytesIO()
                Image.new('RGB', (2, 2), (0, 0, 0)).save(png, format='PNG')
                await answer(agents[0], attachments=[SimpleNamespace(media_type='image/png', data=png.getvalue())])
                assert image_owners == {0}, 'Verified image did not reach its private provider'
                print('[SharedGateway] image input through core custom-model transport PASS', flush=True)
            # A second turn must use private native history without changing PID.
            pid = items[0]['process'].pid
            agents[0].turn = {'operation_id': 'second-turn'}
            await answer(agents[0])
            assert manager.gateway['process'].pid == pid
            agents[0].profile = {**agents[0].profile, 'model': 'fixture-model-20',
                'key_cipher': 'synthetic-key-20', 'endpoint': f'http://127.0.0.1:{port}/provider/20/v1/chat/completions'}
            agents[0].turn = {'operation_id': 'switch-model'}
            await answer(agents[0])
            assert 20 in requests and manager.gateway['process'].pid == pid
            print('[SharedGateway] private model/key switch, retained history, no restart PASS', flush=True)
            agents[0].turn = {'operation_id': 'stop-active'}
            agents[1].turn = {'operation_id': 'after-other-account-stop'}
            tasks = [asyncio.create_task(answer(agents[0], 'STOP-PROBE Call query_owner.')),
                     asyncio.create_task(answer(agents[1]))]
            assert await asyncio.to_thread(held.wait, 20), 'Held model did not start'
            tasks[0].cancel()
            await asyncio.gather(tasks[0], return_exceptions=True)
            release_held.set()
            await tasks[1]
            assert manager.gateway['process'].pid == pid
            print('[SharedGateway] stopping one account leaves another usable PASS', flush=True)
            if faults:
                callback_count = len(tool_calls)
                fault_phase['enabled'] = True
                agents[0].turn = {'operation_id': 'model-auth-failure'}
                try:
                    await asyncio.wait_for(answer(agents[0]), 65)
                    raise AssertionError('Authentication failure was not surfaced')
                except AssistantError as exc:
                    assert exc.category == 'model_auth', 'Authentication failure remained an undiagnosed error'
                    assert 'synthetic-key-' not in str(exc) and 'private-provider-detail' not in str(exc), 'Provider details leaked'
                finally:
                    fault_phase['enabled'] = False
                assert len(tool_calls) == callback_count, 'Failed model unexpectedly called a business tool'
                agents[1].turn = {'operation_id': 'after-other-account-auth-failure'}
                await answer(agents[1])
                assert manager.gateway['process'].pid == pid
                print('[SharedGateway] model auth error isolated, actionable, sanitized; other account usable PASS', flush=True)
            for _, client in tuple(engine.gateways.values()):
                await client.close()
            engine.gateways.clear()
            await asyncio.to_thread(manager._terminate_startup_process, manager.gateway['process'])
            agents[0].turn = {'operation_id': 'after-gateway-restart'}
            await agents[0].warmup()
            await answer(agents[0], 'MEMORY-PROBE Call query_owner and recall the original memory.'
                         if compaction else 'Call query_owner and answer briefly.')
            assert manager.gateway['process'].pid != pid
            if legacy_state:
                assert native_launches == [str(manager.root / 'shared-gateway'), str(manager.root)]
                print('[SharedGateway] previous-state upgrade retained context and owned auth database PASS', flush=True)
            if compaction:
                assert compact['recalls'] >= 2, 'Compacted memory was not checked after restart'
            print('[SharedGateway] gateway restart retained private model context PASS', flush=True)
            for file in Path(directory).rglob('*'):
                if file.is_file() and file.stat().st_size < 20 * 1024 * 1024:
                    data = file.read_bytes()
                    assert all(f'synthetic-key-{i}'.encode() not in data for i in range(21)), 'Model key persisted'
            print('[SharedGateway] retained history, one PID/port, private keys PASS', flush=True)
            if startup_profile:
                import re
                for index, timeline in enumerate(timelines):
                    spans = []
                    for line in timeline.read_text(encoding='utf-8').splitlines():
                        event = json.loads(line)
                        name = event.get('name', '')
                        stage = (event.get('attributes') or {}).get('stage', '')
                        if event.get('type') != 'span.end' or not re.fullmatch(r'[\w.:-]+', name) or stage and not re.fullmatch(r'[\w.:-]+', stage):
                            continue
                        spans.append({'name': name, 'stage': stage, 'ms': event.get('durationMs', 0)})
                    print('[StartupProfile]', index, json.dumps(sorted(spans, key=lambda row: row['ms'], reverse=True)[:35]), flush=True)
            if read_profile or cpu_profile or cache_profile or spawn_profile:
                log = manager.root / 'shared-gateway/gateway.log'
                for line in log.read_text(encoding='utf-8', errors='replace').splitlines():
                    for tag in ('[SDKReadProfile]', '[SDKCPUProfile]', '[CompileCacheProfile]', '[SDKSpawnProfile]'):
                        if tag in line:
                            print(line[line.index(tag):], flush=True)
        except BaseException:
            log = Path(directory) / 'accounts/shared-gateway/gateway.log'
            if log.is_file():
                print('[SharedGateway] diagnostic tail:', log.read_text(encoding='utf-8', errors='replace')[-6500:], flush=True)
            raise
        finally:
            release.set()
            release_held.set()
            for task in tasks:
                if not task.done(): task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for _, client in tuple(engine.gateways.values()):
                await client.close()
            engine.gateways.clear()
            await manager.close()
            for patched in reversed(diagnostic_patches):
                patched.stop()
            process_patch.stop()
            server.should_exit = True
            await asyncio.wait_for(server_task, 5)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--accounts', type=int, default=20)
    parser.add_argument('--compaction', action='store_true')
    parser.add_argument('--replay', action='store_true')
    parser.add_argument('--faults', action='store_true')
    parser.add_argument('--legacy-state', action='store_true')
    parser.add_argument('--profile-reads', action='store_true')
    parser.add_argument('--profile-cpu', action='store_true')
    parser.add_argument('--no-cache-respawn', action='store_true')
    parser.add_argument('--diagnose-runs', action='store_true')
    parser.add_argument('--lose-terminal-response', action='store_true')
    parser.add_argument('--profile-startup', action='store_true')
    parser.add_argument('--profile-cache', action='store_true')
    parser.add_argument('--profile-spawns', action='store_true')
    parser.add_argument('--flush-cache', action='store_true')
    parser.add_argument('--images', action='store_true')
    args = parser.parse_args()
    asyncio.run(run(args.accounts, compaction=args.compaction, replay=args.replay, faults=args.faults,
                   legacy_state=args.legacy_state, read_profile=args.profile_reads, cpu_profile=args.profile_cpu,
                   no_cache_respawn=args.no_cache_respawn, diagnose_runs=args.diagnose_runs,
                   lose_terminal_response=args.lose_terminal_response, startup_profile=args.profile_startup,
                   cache_profile=args.profile_cache or args.flush_cache, flush_cache=args.flush_cache,
                   spawn_profile=args.profile_spawns, images=args.images))

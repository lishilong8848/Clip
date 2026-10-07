"""Read-only provider acceptance using synthetic questions, tools and images."""
import asyncio
import base64
import io
import json
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import uvicorn
from fastapi import FastAPI, Request
from PIL import Image, ImageDraw
from lan_bitable_template_portal.lighthouse_ai import CustomModel, NAMESPACE
from lan_bitable_template_portal.lighthouse_gateway import GatewayClient
from lan_bitable_template_portal.lighthouse_runtime import OpenClawRuntime, free_port
from lan_bitable_template_portal.state_store import LanPortalStateStore


async def run():
    store = LanPortalStateStore()
    model = CustomModel(store)
    profiles = {}
    for document in store.list_documents(NAMESPACE):
        if str(document['key']).startswith('model'):
            for profile in (document.get('payload') or {}).get('models', []):
                if profile.get('key_cipher'):
                    if '--model' in sys.argv and profile.get('model') != sys.argv[sys.argv.index('--model') + 1]:
                        continue
                    profiles.setdefault(model.capability_key(profile), profile)
    app, port = FastAPI(), free_port()
    calls = []
    forwarding = {}
    manager = None

    @app.post('/api/assistant/openclaw-models/chat/completions')
    async def model_proxy(request: Request):
        return await manager.forward_model(request)

    @app.post('/v1/chat/completions')
    async def inspect_provider(request: Request):
        from fastapi.responses import StreamingResponse
        import httpx
        profile = forwarding['profile']
        body = await request.json()
        print('[ProviderWire] request keys:', sorted(body), 'tool choice:', body.get('tool_choice'), flush=True)
        async def response():
            async with httpx.AsyncClient(timeout=75, follow_redirects=False) as client:
                async with client.stream('POST', profile['endpoint'], json=body,
                    headers={'Authorization': 'Bearer ' + model.unprotect(profile['key_cipher'])}) as remote:
                    print('[ProviderWire] HTTP', remote.status_code, flush=True)
                    characters, reasoning = 0, 0
                    async for line in remote.aiter_lines():
                        if line.startswith('data: ') and line != 'data: [DONE]':
                            try:
                                value = json.loads(line[6:])
                                for choice in value.get('choices', []):
                                    delta = choice.get('delta') or {}
                                    characters += len(str(delta.get('content') or ''))
                                    reasoning += len(str(delta.get('reasoning_content') or delta.get('reasoning') or ''))
                                    if choice.get('finish_reason'):
                                        print('[ProviderWire] terminal:', choice['finish_reason'], 'content chars:', characters, 'reasoning chars:', reasoning, flush=True)
                            except ValueError:
                                pass
                        yield line + '\n'
        return StreamingResponse(response(), media_type='text/event-stream')

    @app.post('/api/assistant/openclaw-tools')
    async def bridge(request: Request):
        assert request.headers.get('authorization', '') == 'Bearer ' + manager.bridge_token
        payload = await request.json()
        assert payload['agent_id'] == item['agent_id']
        assert payload['session_key'].startswith('agent:' + item['agent_id'] + ':')
        assert payload['tool'] == 'lighthouse_probe' and payload['params'] == {'scope': 'D'}
        calls.append(payload['call_id'])
        return {'ok': True, 'count': 2, 'scope': 'D', 'fixture': True}

    server = uvicorn.Server(uvicorn.Config(app, host='127.0.0.1', port=port, log_level='error'))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        for _ in range(100):
            if server.started:
                break
            await asyncio.sleep(.05)
        with tempfile.TemporaryDirectory(prefix='provider-', dir=Path(__file__).resolve().parents[2] / 'build_output/agent_probes') as folder:
            manager = OpenClawRuntime(folder, max_accounts=1)
            try:
                for index, (identity, original) in enumerate(profiles.items()):
                    profile = {**original, 'vision_verified': True}
                    if '--inspect-provider' in sys.argv:
                        forwarding['profile'] = original
                        profile['endpoint'] = f'http://127.0.0.1:{port}/v1/chat/completions'
                    root = Path(folder) / ('plugin-' + str(index))
                    root.mkdir()
                    import shutil
                    for file in (Path(__file__).resolve().parents[1] / 'openclaw_service/assistant/openclaw/plugin').iterdir():
                        if file.is_file():
                            shutil.copyfile(file, root / file.name)
                    definition = {'name': 'lighthouse_probe', 'label': '隔离查询', 'description': 'Return synthetic count for test building D.',
                        'parameters': {'type': 'object', 'properties': {'scope': {'type': 'string', 'enum': ['D']}}, 'required': ['scope'], 'additionalProperties': False}}
                    (root / 'tools.json').write_text(json.dumps([definition]), encoding='utf-8')
                    manifest = json.loads((root / 'openclaw.plugin.json').read_text(encoding='utf-8'))
                    manifest.update(activation={'onStartup': True}, contracts={'tools': ['lighthouse_probe']}, toolMetadata={'lighthouse_probe': {'optional': False}})
                    (root / 'openclaw.plugin.json').write_text(json.dumps(manifest), encoding='utf-8')
                    import secrets
                    token = secrets.token_urlsafe(32)
                    result = {'model': profile['model'], 'text': False, 'stream': False, 'tools': False, 'vision': False, 'checked_at': time.time()}
                    print('[Provider] Testing ' + profile['model'], flush=True)
                    item = None
                    try:
                        item = await manager.acquire({'id': 'isolated-provider-' + str(index), 'scopes': ['D']}, model, profile,
                            plugin=root, tool_names=['lighthouse_probe'], bridge_token=token,
                            bridge_url=f'http://127.0.0.1:{port}/api/assistant/openclaw-tools')
                        async with GatewayClient(f"ws://127.0.0.1:{item['port']}", item['token']) as client:
                            async def turn(suffix, text, attachments=None):
                                session = 'agent:' + item['agent_id'] + ':provider-' + suffix
                                accepted = await client.request('agent', {'sessionKey': session, 'agentId': item['agent_id'],
                                    'idempotencyKey': identity + suffix, 'message': text, 'timeout': 65, 'deliver': False,
                                    'thinking': 'off', 'extraSystemPrompt': 'Only perform this synthetic acceptance task. This is a direct interactive user question: always give a visible answer, never NO_REPLY or HEARTBEAT_OK. No business or real personal data.',
                                    **({'attachments': attachments} if attachments else {})}, timeout=20)
                                output, streamed, deadline = '', False, time.monotonic() + 75
                                while time.monotonic() < deadline:
                                    frame = await client.next_event(timeout=75)
                                    data = frame.get('payload') or {}
                                    if data.get('runId') != accepted['runId']:
                                        continue
                                    update = data.get('data') or {}
                                    if data.get('stream') == 'assistant':
                                        output = update.get('text', output + update.get('delta', ''))
                                        streamed |= bool(update.get('delta'))
                                    if data.get('stream') == 'lifecycle' and update.get('phase') in {'end', 'error'}:
                                        waited = await client.request('agent.wait', {'runId': accepted['runId'], 'timeoutMs': 5000}, timeout=7)
                                        print('[ProviderTurn]', suffix, waited.get('status'), 'stop=', waited.get('stopReason'), 'chars=', len(output), flush=True)
                                        terminal = waited.get('terminalReply') or {}
                                        if not output and terminal.get('disposition') == 'visible':
                                            output = terminal.get('text') or ''
                                        return output, streamed, waited.get('status') == 'ok'
                                await client.request('chat.abort', {'sessionKey': session, 'runId': accepted['runId']})
                                return '', False, False
                            text, streamed, ok = await turn('chat', '请用一个词回答：你好。不要调用工具。')
                            result.update(text=ok and bool(text), stream=streamed)
                            before = len(calls)
                            text, _, ok = await turn('tool', '这是隔离测试，请调用lighthouse_probe，scope必须为D，返回工具给出的数量。不能猜测。')
                            result['tools'] = ok and len(calls) > before and '2' in text
                            image = Image.new('RGB', (180, 60), 'white')
                            ImageDraw.Draw(image).text((15, 10), 'TEST42', fill='black', font_size=32)
                            stream = io.BytesIO()
                            image.save(stream, format='PNG')
                            text, _, ok = await turn('image', '只读出图片中的英文和数字，不调用工具，不解释。',
                                [{'type': 'image', 'mimeType': 'image/png', 'content': base64.b64encode(stream.getvalue()).decode()}])
                            result['vision'] = ok and 'TEST42' in text.replace(' ', '').upper()
                    except Exception as exc:
                        result['failure_type'] = type(exc).__name__
                        if hasattr(exc, 'status'):
                            result['failure_status'] = exc.status
                    finally:
                        if item:
                            item['busy'] = False
                    store.put_document('lighthouse_model_capabilities', identity, result)
                    print('[Provider] ' + json.dumps(result, ensure_ascii=False), flush=True)
                for file in Path(folder).rglob('*'):
                    if file.is_file() and file.stat().st_size <= 30 * 1024 ** 2:
                        content = file.read_bytes()
                        for profile in profiles.values():
                            assert model.unprotect(profile['key_cipher']).encode() not in content, file.name
            finally:
                await manager.close()
    finally:
        server.should_exit = True
        await asyncio.to_thread(thread.join, 5)
        model.close()


if __name__ == '__main__':
    asyncio.run(run())

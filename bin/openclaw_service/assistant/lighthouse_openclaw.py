"""OpenClaw owns inference; the portal owns tool authorization and confirmation."""
import asyncio
import copy
import hashlib
import hmac
import json
import logging
import secrets
import time
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

from .lighthouse_ai import AssistantError, safe_text
from .lighthouse_gateway import GatewayClient, GatewayError
from .lighthouse_model import LighthouseModel
from .lighthouse_runtime import OpenClawRuntime, account_key, agent_id
from .lighthouse_startup_log import emit as startup_log
from ..protocol import MAX_CONCURRENT_ACCOUNTS


@asynccontextmanager
async def profile_model(_, profile):
    yield profile


def native_model_error(error, *, timeout=False):
    """Classify the broker's fixed messages; never display native error bodies."""
    text = error.get('message') if isinstance(error, dict) else error
    text = text.lower() if isinstance(text, str) else ''
    for marker, category, status, message in (
        ('model provider authentication failed', 'model_auth', 502, '模型认证未通过，请核对本人模型设置。'),
        ('model provider rate limit exceeded', 'model_busy', 429, '模型服务繁忙或额度不足，请稍后重试原消息。'),
        ('model provider model not found', 'model_request', 502, '模型名称或接口地址无效，请核对本人模型设置。'),
        ('model provider rejected request', 'model_request', 502, '模型接口不接受当前请求，请核对模型地址及工具调用支持。'),
        ('model provider timed out', 'timeout', 504, '模型服务响应超时，原消息已保留，可重试。'),
        ('model provider unavailable', 'model_connection', 502, '未能连接模型服务，请稍后重试；这不代表网页与灯塔的连接中断。'),
        ('context length exceeded', 'model_context', 502, '模型上下文整理未完成，原消息已保留；可重试或切换上下文容量更大的模型。'),
        ('context overflow', 'model_context', 502, '模型上下文整理未完成，原消息已保留；可重试或切换上下文容量更大的模型。'),
    ):
        if marker in text:
            return AssistantError(message, status, category=category)
    return AssistantError('模型运行未完成，原消息已保留，可重试。', 504 if timeout else 502,
                          category='timeout' if timeout else 'model_protocol')


class BusinessBridge:
    """Tokens bind live server-owned callbacks, not model-selected accounts or URLs."""
    def __init__(self):
        self.active = {}

    async def call_shared(self, token, payload, manager):
        secret = getattr(manager, 'bridge_token', '')
        if not secret or not isinstance(token, str) or not token.isascii() or not hmac.compare_digest(token, secret):
            raise AssistantError('内部工具认证未通过。', 403)
        active = next((run for run in self.active.values() if run.session_key == payload.get('session_key')
                       and run.run_id == payload.get('run_id') and agent_id(run.actor['id']) == payload.get('agent_id')), None)
        if not active or getattr(active, 'item', {}).get('blocked'):
            raise AssistantError('工具调用已失效。', 403)
        return await self.call(active.token, payload)

    async def call(self, token, payload):
        active = self.active.get(token)
        if not active or not active.running or payload.get('session_key') != active.session_key:
            raise AssistantError('工具调用已失效。', 403)
        if getattr(active, 'run_id', None) and payload.get('run_id') != active.run_id:
            raise AssistantError('工具运行编号已失效，未执行业务。', 403)
        name, identity = payload.get('tool'), payload.get('call_id')
        if name not in active.tools or not isinstance(identity, str) or not 1 <= len(identity) <= 200:
            raise AssistantError('不允许此工具调用。', 403)
        args = payload.get('params')
        if not isinstance(args, dict):
            raise AssistantError('工具参数无效。')
        signature = hashlib.sha256(json.dumps([name, args], sort_keys=True).encode()).hexdigest()
        async with active.call_lock:
            cached = active.results.get(identity)
            if cached:
                if cached[0] != signature:
                    raise AssistantError('工具调用编号冲突。', 409)
                return copy.deepcopy(cached[1])
            if not active.running or active.calls >= 24:
                raise AssistantError('本轮工具调用已停止。', 409)
            active.calls += 1
            tool = active.tools[name]
            try:
                validated = tool.function_schema.validator.validate_python(args)
                result = await asyncio.wait_for(tool.function_schema.call(validated, None), tool.timeout or 45)
            except AssistantError as exc:
                result = {'ok': False, 'error': safe_text(str(exc)), 'status': exc.status}
            except asyncio.TimeoutError:
                result = {'ok': False, 'error': '本次查询超时，结果未知。'}
            except Exception as exc:
                from pydantic_ai import ModelRetry
                result = {'ok': False, 'error': safe_text(str(exc)) if isinstance(exc, ModelRetry) else '工具参数或响应无效，请核对后继续。'}
            if not active.running:
                raise AssistantError('本轮已停止。', 409)
            result = json.loads(json.dumps(result, ensure_ascii=False))
            active.results[identity] = (signature, copy.deepcopy(result))
            await active.events.put(SimpleNamespace(event_kind='function_tool_result', part=SimpleNamespace(tool_name=name.removeprefix('lighthouse_'), content=result)))
            return result


class OpenClawToolAgent:
    def __init__(self, engine, actor, turn, emit, profile, *, instructions, **_):
        self.engine, self.actor, self.turn, self.emit, self.profile = engine, actor, turn, emit, profile
        self.resident = getattr(engine.manager, 'resident', False) is True
        self.instructions = instructions
        self.tools, self.validators, self.results = {}, [], {}
        self.call_lock, self.events = asyncio.Lock(), asyncio.Queue(64)
        self.running, self.calls, self.run_id, self.client = False, 0, '', None
        scope_key = hashlib.sha256(json.dumps(sorted(actor['scopes'])).encode()).hexdigest()[:16]
        state = engine.assistant._state(actor)
        self.session_key = 'agent:' + agent_id(actor['id']) + ':lighthouse:' + state['id'] + ':' + scope_key
        self.token = engine.tokens.setdefault(account_key(actor['id']), secrets.token_urlsafe(40))

    def tool_plain(self, function=None, **kwargs):
        def register(fn):
            from pydantic_ai import Tool
            self.tools['lighthouse_' + fn.__name__] = Tool(fn, **kwargs)
            return fn
        return register(function) if function else register

    def output_validator(self, function):
        self.validators.append(function)
        return function

    async def _abort(self):
        if self.client and self.run_id:
            try:
                await self.client.request('chat.abort', {'sessionKey': self.session_key, 'runId': self.run_id}, timeout=5)
            except (GatewayError, OSError, asyncio.TimeoutError):
                try:
                    async with GatewayClient(self.item_url, self.item['token']) as client:
                        await client.request('chat.abort', {'sessionKey': self.session_key, 'runId': self.run_id}, timeout=5)
                except (GatewayError, OSError, asyncio.TimeoutError):
                    # Revoke only this account, never terminate the shared gateway.
                    await self.engine.manager._stop(self.item)

    async def _recover(self, answer):
        await self.emit('status', {'label': '正在恢复原回答，不重复提交业务'})
        await self.client.close()
        self.client = GatewayClient(self.item_url, self.item['token'])
        self.client.select_run(self.run_id)
        await self.client.__aenter__()
        await self.engine.keep_gateway(self.actor, self.item, self.client)
        # This is an observer RPC for the exact accepted run, not another agent call.
        waited = await self.client.request('agent.wait', {'runId': self.run_id, 'timeoutMs': 0}, timeout=5)
        if waited.get('runId', self.run_id) != self.run_id:
            raise AssistantError('原回答运行编号不一致，未使用其他会话内容。', 502)
        if waited.get('status') == 'error':
            raise native_model_error(waited.get('error'))
        reply = waited.get('terminalReply') or {}
        if waited.get('status') == 'ok':
            if reply.get('disposition') == 'visible' and isinstance(reply.get('text'), str):
                return reply['text'], True
            raise AssistantError('原回答已结束但未取得完整内容，消息已保留。', 502)
        history = await self.client.request('chat.history', {'sessionKey': self.session_key, 'limit': 1}, timeout=5)
        flight = history.get('inFlightRun') or {}
        return flight.get('text', answer) if flight.get('runId') == self.run_id else answer, False

    async def _acquire_runtime(self, *, warm_only=False):
        definitions = [{'name': name, 'label': tool.name, 'description': tool.description or '',
                        'parameters': tool.function_schema.json_schema} for name, tool in self.tools.items()]
        if self.resident:
            from openclaw_service.protocol import ServiceError
            if not self.engine.gateway_ready(self.actor):
                await self.emit('status', {'label': '正在连接灯塔助手'})
            try:
                return await self.engine.manager.acquire(self.actor, self.engine.assistant.model_for(self.actor), self.profile,
                    definitions=definitions, warm_only=warm_only)
            except ServiceError as exc:
                raise AssistantError(str(exc), exc.status) from None
        plugin = self.engine.manager.root / account_key(self.actor['id']) / 'plugin'
        from openclaw_service.protocol import prepare_tool_plugin
        await asyncio.to_thread(prepare_tool_plugin, Path(__file__).parent / 'openclaw/plugin', plugin, definitions)
        if not self.engine.gateway_ready(self.actor):
            await self.emit('status', {'label': '正在连接助手'})
        loop = asyncio.get_running_loop()
        progress = asyncio.Queue()
        async def announce_install():
            while True:
                await self.emit('status', {'label': await progress.get()})
        announcer = asyncio.create_task(announce_install())
        try:
            item = await self.engine.manager.acquire(self.actor, self.engine.assistant.model_for(self.actor), self.profile,
                plugin=plugin, tool_names=list(self.tools), bridge_token=self.token, bridge_url=self.engine.bridge_url(),
                progress=lambda label: loop.call_soon_threadsafe(progress.put_nowait, label))
        finally:
            announcer.cancel()
            await asyncio.gather(announcer, return_exceptions=True)
        return item

    async def warmup(self):
        item = await self._acquire_runtime(warm_only=True)
        try:
            lock = getattr(self.engine, 'session_prepare_lock', None)
            if lock is None:
                lock = self.engine.session_prepare_lock = asyncio.Lock()
            # SQLite's first schema migration is synchronous in the native
            # gateway. Serialize only this cold preparation, never questions.
            async with lock:
                client = await self.engine.gateway_client(self.actor, item)
                if self.session_key not in item.get('prepared_sessions', set()):
                    # Key-only patching preserves history, without inference.
                    await client.request('sessions.patch', {'key': self.session_key}, timeout=60)
                    item.setdefault('prepared_sessions', set()).add(self.session_key)
        finally:
            item['busy'] = False
            item['used_at'] = time.monotonic()
            if self.resident:
                await self.engine.manager.release(item)

    @asynccontextmanager
    async def run_stream_events(self, prompt, *, message_history, **_):
        await self.engine.wait_warmup(self.actor, self.emit)
        item = await self._acquire_runtime()
        self.running = True
        self.item, self.item_url = item, 'ws://127.0.0.1:' + str(item['port'])
        item['busy'] = True
        self.engine.bridge.active[self.token] = self
        events = None
        try:
            self.client = await self.engine.gateway_client(self.actor, item)
            events = self._events(self.client, prompt, message_history)
            yield events
        finally:
            self.running = False
            self.engine.bridge.active.pop(self.token, None)
            try:
                await self._abort()
                if events is not None:
                    await events.aclose()
                if self.client is not None:
                    self.client.select_run()
                    await self.engine.keep_gateway(self.actor, item, self.client)
            finally:
                self.client = None
                item['busy'] = False
                item['used_at'] = time.monotonic()
                if self.resident:
                    try:
                        await self.engine.manager.release(item)
                    except Exception:
                        logging.getLogger(__name__).warning('Assistant run release not confirmed; no business replay.')

    async def _events(self, client, prompt, history):
        from pydantic_ai import ModelRetry
        message = '\n'.join(part for part in prompt if isinstance(part, str))
        # Native history is authoritative; import the previous UI history once.
        saved = await client.request('chat.history', {'sessionKey': self.session_key, 'limit': 1})
        if not saved.get('messages'):
            legacy = []
            for msg in history:
                for part in msg.parts:
                    content = getattr(part, 'content', '')
                    if isinstance(content, str):
                        legacy.append({'role': 'assistant' if getattr(part, 'part_kind', '') == 'text' else 'user', 'content': content})
            if legacy:
                message = '此前已授权会话（历史而非当前事实，不能重执行业务）：\n' + json.dumps(legacy, ensure_ascii=False) + '\n本轮问题：\n' + message
        images = [{'type': 'image', 'mimeType': part.media_type, 'content': __import__('base64').b64encode(part.data).decode('ascii')}
                  for part in prompt if hasattr(part, 'media_type')]
        if images and not self.profile.get('vision_verified'):
            images = []
            message += '\n当前模型尚未验证图片能力，仅使用附件中已提取的OCR文字；无可靠文字时请用户补充，不推断图像内容。'
        for attempt in range(2):
            native_run = 'lh-' + account_key(self.actor['id']) + ':' + (self.turn.get('run_id') or self.turn['operation_id'])
            params = {'message': message, 'sessionKey': self.session_key, 'idempotencyKey': native_run,
                      'deliver': False, 'thinking': 'off', 'extraSystemPrompt': self.instructions, 'timeout': 180}
            if attempt:
                params['idempotencyKey'] += ':validation'
            if images:
                params['attachments'] = images
            self.run_id = params['idempotencyKey']
            self.item['run_id'] = self.run_id
            self.item['session_key'] = self.session_key
            params['agentId'] = agent_id(self.actor['id'])
            self.client.select_run(self.run_id)
            if self.resident:
                from openclaw_service.protocol import ServiceError
                try:
                    await self.engine.manager.begin_run(self.item, self.session_key, self.run_id, self.token)
                except ServiceError as exc:
                    raise AssistantError(str(exc), exc.status) from None
            answer, reconnected, terminal_ack, current_item = '', False, False, None
            try:
                accepted = await self.client.request('agent', params, timeout=20)
            except (GatewayError, OSError, asyncio.TimeoutError) as exc:
                if isinstance(exc, GatewayError) and exc.code not in {'DISCONNECTED', 'CLOSED', 'UNAVAILABLE', 'TIMEOUT'}:
                    raise
                answer, terminal_ack = await self._recover('')
                reconnected = True
                accepted = {'runId': self.run_id}
                if answer:
                    yield SimpleNamespace(event_kind='part_delta', delta=SimpleNamespace(part_delta_kind='text', content_delta=answer))
            if accepted.get('runId') != self.run_id:
                raise AssistantError('助手运行编号校验失败，未继续执行。', 502)
            if accepted.get('status') in {'error', 'timeout'}:
                self.run_id = ''
                raise native_model_error(accepted.get('summary'), timeout=accepted['status'] == 'timeout')
            if accepted.get('status') == 'ok' and not terminal_ack:
                # A completed idempotent RPC returns its outcome, not new WS events.
                finished = await self.client.request('agent.wait', {'runId': self.run_id, 'timeoutMs': 0}, timeout=5)
                terminal = finished.get('terminalReply') or {}
                text = terminal.get('text') if terminal.get('disposition') == 'visible' else None
                if finished.get('status') != 'ok' or finished.get('runId', self.run_id) != self.run_id or \
                        not isinstance(text, str) or not text.strip():
                    self.run_id = ''
                    raise AssistantError('原回答已结束但未取得完整内容，消息已保留；未重新提交业务。', 502)
                answer, terminal_ack = text, True
                yield SimpleNamespace(event_kind='part_start', part=SimpleNamespace(part_kind='text', content=answer))
            if terminal_ack:
                self.run_id = ''
            deadline, sequence, compaction_notified = time.monotonic() + 190, 0, False
            while self.run_id and time.monotonic() < deadline:
                if not compaction_notified and self.item.get('compaction_run_id') == self.run_id:
                    await self.emit('status', {'label': '正在自动整理上下文'})
                    compaction_notified = True
                while not self.events.empty():
                    yield self.events.get_nowait()
                try:
                    frame = await self.client.next_event(timeout=.5)
                except asyncio.TimeoutError:
                    continue
                except (GatewayError, OSError) as exc:
                    if reconnected or (isinstance(exc, GatewayError) and exc.code not in {'DISCONNECTED', 'CLOSED', 'UNAVAILABLE'}):
                        raise AssistantError('助手连接中断，原消息已保留。', 502) from None
                    reconnected = True
                    restored, terminal = await self._recover(answer)
                    if not isinstance(restored, str):
                        raise AssistantError('原回答内容无效，未拼接不完整回答。', 502)
                    if restored != answer:
                        if restored.startswith(answer):
                            yield SimpleNamespace(event_kind='part_delta', delta=SimpleNamespace(part_delta_kind='text', content_delta=restored[len(answer):]))
                        else:
                            # Recovery is an authenticated snapshot of this exact run.
                            yield SimpleNamespace(event_kind='part_start', part=SimpleNamespace(part_kind='text', content=restored))
                        answer = restored
                    if terminal:
                        self.run_id = ''
                        break
                    continue
                data = frame.get('payload') or {}
                if data.get('runId') != self.run_id:
                    continue
                seq = data.get('seq')
                if type(seq) is int:
                    if seq <= sequence:
                        continue
                    sequence = seq
                if frame['event'] == 'agent':
                    stream, update = data.get('stream'), data.get('data') or {}
                    if stream == 'assistant':
                        if update.get('phase') == 'commentary':
                            answer = ''
                            continue
                        text = update.get('text')
                        delta = update.get('delta')
                        item = update.get('itemId')
                        new_part = bool(item and item != current_item) or update.get('replace') is True
                        if new_part:
                            answer = ''
                        if item:
                            current_item = item
                        previous = answer
                        if isinstance(text, str):
                            if not text.startswith(previous):
                                if item and not new_part:
                                    raise AssistantError('流式回答内容不连续，原问题已保留。', 502)
                                # Some native adapters relay full snapshots without item IDs.
                                new_part, previous = True, ''
                            answer = text
                        elif isinstance(delta, str):
                            answer += delta
                        if answer != previous:
                            if new_part:
                                yield SimpleNamespace(event_kind='part_start', part=SimpleNamespace(part_kind='text', content=answer))
                            else:
                                yield SimpleNamespace(event_kind='part_delta', delta=SimpleNamespace(part_delta_kind='text', content_delta=answer[len(previous):]))
                    if stream == 'compaction' and not compaction_notified:
                        await self.emit('status', {'label': '正在自动整理上下文'})
                        compaction_notified = True
                    if stream == 'lifecycle' and update.get('phase') in {'end', 'error'}:
                        if update['phase'] == 'error':
                            raise native_model_error(update.get('error'))
                        try:
                            finished = await self.client.request('agent.wait', {'runId': self.run_id, 'timeoutMs': 5000}, timeout=7)
                        except (GatewayError, OSError, asyncio.TimeoutError) as exc:
                            if reconnected or isinstance(exc, GatewayError) and exc.code not in {'DISCONNECTED', 'CLOSED', 'UNAVAILABLE', 'TIMEOUT'}:
                                raise AssistantError('原回答完成核验未成功，消息已保留；未重新提交业务。', 502) from None
                            reconnected = True
                            text, terminal = await self._recover(answer)
                            if not terminal:
                                raise AssistantError('模型运行结果尚未确认，原消息已保留。', 502)
                        else:
                            if finished.get('status') != 'ok' or finished.get('runId', self.run_id) != self.run_id:
                                raise AssistantError('模型运行结果尚未确认，原消息已保留。', 502)
                            terminal = finished.get('terminalReply') or {}
                            text = terminal.get('text') if terminal.get('disposition') == 'visible' else None
                        if isinstance(text, str) and text != answer:
                            if text.startswith(answer):
                                yield SimpleNamespace(event_kind='part_delta', delta=SimpleNamespace(part_delta_kind='text', content_delta=text[len(answer):]))
                            else:
                                yield SimpleNamespace(event_kind='part_start', part=SimpleNamespace(part_kind='text', content=text))
                            answer = text
                        self.run_id = ''
                        break
            if self.run_id:
                raise AssistantError('助手响应超时，原消息已保留。', 504)
            while not self.events.empty():
                yield self.events.get_nowait()
            if not answer.strip():
                if attempt:
                    raise AssistantError('模型未返回可见回答，原问题已保留，可重试或切换模型。', 502)
                message = '用户主动提问，需要可见回答，不能使用静默标记。请回答原问题：\n' + message
                continue
            try:
                for validate in self.validators:
                    answer = await validate(answer)
            except ModelRetry as exc:
                if attempt:
                    raise AssistantError('未取得可靠业务依据，未执行业务，请重试或明确查询内容。', 502) from None
                message = safe_text(str(exc))
                continue
            yield SimpleNamespace(event_kind='agent_run_result', result=SimpleNamespace(output=answer))
            return


class LighthouseOpenClaw(LighthouseModel):
    manages_context = True
    manages_runtime = True
    max_parallel = MAX_CONCURRENT_ACCOUNTS

    def __init__(self, portal, *, bridge_url, state_root=None, cached_reader=None, runtime_root=None, manager=None):
        from .lighthouse_public import PublicSources
        super().__init__(portal, cached_reader=cached_reader, model_factory=profile_model, agent_factory=self.make_agent, public_sources=PublicSources())
        self.bridge_url = bridge_url
        root = state_root or Path(portal.store.db_path).parent / 'lighthouse_openclaw'
        self.manager = manager if manager is not None else OpenClawRuntime(root, runtime_root=runtime_root)
        self.bridge, self.tokens = BusinessBridge(), {}
        self.warming, self.warm_attempts, self.warm_errors = {}, {}, {}
        self.warm_slots = asyncio.Semaphore(MAX_CONCURRENT_ACCOUNTS)
        self.session_prepare_lock = asyncio.Lock()
        self.closing = False
        self.gateways = {}

    def gateway_ready(self, actor):
        cached = self.gateways.get(account_key(actor['id']))
        return bool(cached and cached[1].connected and not cached[0].get('stopped'))

    async def keep_gateway(self, actor, item, client):
        key = account_key(actor['id'])
        previous = self.gateways.pop(key, None)
        if previous and previous[1] is not client:
            await previous[1].close()
        if self.closing or item.get('stopped') or not client.connected:
            await client.close()
        else:
            self.gateways[key] = (item, client)

    async def gateway_client(self, actor, item):
        key = account_key(actor['id'])
        for old_key, cached_entry in tuple(self.gateways.items()):
            old_item, client = cached_entry
            if old_item.get('stopped') or not client.connected:
                if self.gateways.get(old_key) is cached_entry:
                    self.gateways.pop(old_key, None)
                    await client.close()
        cached = self.gateways.get(key)
        if cached and all(cached[0].get(field) == item.get(field) for field in ('port', 'token', 'fingerprint')):
            try:
                cached[1].discard_events()
            except RuntimeError:
                raise AssistantError('当前会话仍在收尾，请稍后继续；未重复提交业务。', 409) from None
            self.gateways[key] = (item, cached[1])
            return cached[1]
        if cached:
            self.gateways.pop(key, None)
            await cached[1].close()
        if self.closing:
            raise AssistantError('助手服务正在关闭。', 503)
        client = GatewayClient('ws://127.0.0.1:' + str(item['port']), item['token'])
        client.select_run()
        await client.__aenter__()
        await self.keep_gateway(actor, item, client)
        if self.closing:
            raise AssistantError('助手服务正在关闭。', 503)
        return client

    async def prepare(self):
        if getattr(self.manager, 'resident', False) is True:
            from openclaw_service.protocol import ServiceError
            try:
                await self.manager.prepare()
            except ServiceError as exc:
                raise AssistantError(str(exc), exc.status) from None
        else:
            tasks = (asyncio.create_task(self.manager.prepare()), asyncio.create_task(self.manager.model_client()))
            try:
                await asyncio.gather(*tasks)
            finally:
                for task in tasks:
                    if not task.done(): task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    def queue_warmup(self, actor):
        key = account_key(actor['id'])
        existing = self.manager.accounts.get(key)
        if self.closing or key in self.warming or (existing and existing.get('process') and existing['process'].poll() is None):
            return
        if len(self.warming) >= self.manager.maximum or time.monotonic() - self.warm_attempts.get(key, -60) < 60:
            return
        self.warm_attempts[key] = time.monotonic()
        self.warm_errors.pop(key, None)
        task = asyncio.create_task(self._warmup(copy.deepcopy(actor)))
        self.warming[key] = task
        task.add_done_callback(lambda done: self.warming.pop(key, None) if self.warming.get(key) is done else None)

    async def _warmup(self, actor):
        key = account_key(actor['id'])
        try:
            async with self.warm_slots:
                def selected_profile():
                    with self.assistant._lock:
                        model = self.assistant.model_for(actor)
                        settings = model.settings()
                        selected = self.assistant._selected(self.assistant._state(actor), settings)
                        return model.profile(selected['id']) if settings['enabled'] and selected else None
                profile = await asyncio.to_thread(selected_profile)
                if profile is None or self.closing:
                    if not self.closing:
                        startup_log('warmup_skipped', account=key[:8])
                    return
                async def emit(*_):
                    pass
                async def authorize():
                    return actor
                # No stored message, file read, business query or inference during startup.
                await self.answer(actor, {'question': '', '_profile': profile}, [], None, emit, authorize, {}, warm_only=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            startup_log('startup_failed', account=key[:8], error=type(exc).__name__, last_stage='warmup')
            self.warm_errors[key] = exc if isinstance(exc, AssistantError) else AssistantError('助手后台准备未完成，请重试原消息。', 503)
            logging.getLogger(__name__).warning('Assistant warmup failed: account=%s type=%s', key, type(exc).__name__)

    async def wait_warmup(self, actor, emit):
        key = account_key(actor['id'])
        task = self.warming.get(key)
        if task and task is not asyncio.current_task():
            await emit('status', {'label': '正在准备助手，原消息已保留'})
            await asyncio.shield(task)
            if key in self.warm_errors:
                raise self.warm_errors[key]

    def make_agent(self, profile, *, actor, turn, emit, **kwargs):
        return OpenClawToolAgent(self, actor, turn, emit, profile, **kwargs)

    async def close(self):
        self.closing = True
        pending = tuple(self.warming.values())
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self.warming.clear()
        for active in tuple(self.bridge.active.values()):
            active.running = False
            await active._abort()
        self.bridge.active.clear()
        gateways = tuple(self.gateways.values())
        self.gateways.clear()
        await asyncio.gather(*(client.close() for _, client in gateways), return_exceptions=True)
        try:
            await super().close()
        finally:
            await self.manager.close()

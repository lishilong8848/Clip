"""Loopback gateway host. Only the current portal executes business callbacks."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import time
import uuid
from fastapi import Request

from .protocol import MAX_CONCURRENT_ACCOUNTS, PROTOCOL, PROJECT, STATE, ServiceError, code_digest, process_stamp, prepare_tool_plugin


def text(value, name, limit=200):
    if not isinstance(value, str) or not value or len(value) > limit or '\x00' in value:
        raise ServiceError('助手服务请求参数无效。', 400, 'invalid_request')
    return value


class Host:
    def __init__(self, *, project=PROJECT, state=STATE, runtime_root=None, key=None, manager=None):
        self.project, self.state = Path(project).resolve(), Path(state).resolve()
        self.key = key
        self.instance = uuid.uuid4().hex
        self.digest = code_digest(self.project)
        self.port = None
        self.lease = None
        self.runs, self.reservations, self.account_locks = {}, {}, {}
        self.registration_lock = asyncio.Lock()
        self.closing = False
        self.stop_reason = 'crash'
        self.runtime_state, self.runtime_error = 'preparing', ''
        self.tasks = set()
        self.backend_hooks = None
        self.store = self.catalog = self.portal_bridge = None
        self.backend_lock = asyncio.Lock()
        self.backend_lease = None
        if manager is None:
            from lan_bitable_template_portal.lighthouse_runtime import OpenClawRuntime
            manager = OpenClawRuntime(self.state / 'accounts', runtime_root=runtime_root, max_accounts=MAX_CONCURRENT_ACCOUNTS)
        self.manager = manager
        if hasattr(manager, 'model_url'):
            manager.model_url = lambda: f'http://127.0.0.1:{self.port}/api/assistant/openclaw-models'

    def spawn(self, coro):
        task = asyncio.create_task(coro)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return task

    async def prepare(self):
        tasks = (asyncio.create_task(self.manager.prepare()), asyncio.create_task(self.manager.model_client()))
        try:
            await asyncio.gather(*tasks)
            self.runtime_state, self.runtime_error = 'ready', ''
        except asyncio.CancelledError:
            raise
        except Exception:
            self.runtime_state, self.runtime_error = 'failed', 'runtime_unavailable'
        finally:
            for task in tasks:
                if not task.done(): task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def current_lease(self, payload):
        lease = self.lease
        if self.closing:
            raise ServiceError('助手服务正在停止。', code='service_stopping')
        if not lease or payload.get('lease') != lease['id']:
            raise ServiceError('助手业务连接已失效，请重新连接。', 409, 'lease_expired')
        if process_stamp(lease['pid']) != lease['process_stamp']:
            raise ServiceError('灯塔门户已停止，未调用业务。', 409, 'portal_offline')
        return lease

    async def register(self, payload):
        async with self.registration_lock:
            return await self._register(payload)

    async def _register(self, payload):
        portal_id = text(payload.get('portal_id'), 'portal_id', 64)
        callback = payload.get('callback_url')
        if not isinstance(callback, str) or not re.fullmatch(r'http://127\.0\.0\.1:[0-9]{1,5}/api/assistant/(?:openclaw-tools|service-bridge)', callback):
            raise ServiceError('助手工具桥地址无效。', 400, 'invalid_callback')
        port = int(callback.split(':')[2].split('/')[0])
        if not 1 <= port <= 65535:
            raise ServiceError('助手工具桥地址无效。', 400, 'invalid_callback')
        pid, stamp = payload.get('pid'), payload.get('process_stamp')
        if stamp is None or process_stamp(pid) != stamp:
            raise ServiceError('助手门户进程已失效。', 409, 'portal_offline')
        if payload.get('digest') != self.digest:
            raise ServiceError('助手服务需要重新启动以加载更新。', 409, 'service_restart_required')
        if self.lease and self.lease['portal_id'] == portal_id:
            return {'lease': self.lease['id'], 'instance': self.instance}
        await self.invalidate()
        lease = {'id': secrets.token_urlsafe(32), 'portal_id': portal_id, 'pid': pid,
            'process_stamp': stamp, 'callback_url': callback,
            'assistant_backend': payload.get('assistant_backend') is True, 'legacy_db': payload.get('legacy_db')}
        self.lease = lease
        self.spawn(self.watch_portal(lease))
        return {'lease': lease['id'], 'instance': self.instance}

    async def watch_portal(self, lease):
        # A cheap exact-process check only; no portal HTTP polling or business reads.
        while not self.closing and self.lease is lease:
            await asyncio.sleep(.5)
            if process_stamp(lease['pid']) != lease['process_stamp']:
                await self.invalidate(lease)
                return

    async def abort_run(self, run):
        from lan_bitable_template_portal.lighthouse_gateway import GatewayClient
        item = self.manager.accounts.get(run['account'])
        if not item or item['process'].poll() is not None:
            return
        try:
            async with GatewayClient('ws://127.0.0.1:' + str(item['port']), item['token']) as client:
                await client.request('chat.abort', {'sessionKey': run['session_key'], 'runId': run['run_id']}, timeout=2)
        except Exception:
            # Do not park an unconfirmed run as reusable: its model could still
            # be processing after the portal that owned it has gone away.
            await self.manager._stop(item)
        finally:
            item['busy'] = False
            item['used_at'] = time.monotonic()

    async def invalidate(self, lease=None):
        if lease is not None and self.lease is not lease:
            return
        self.lease = None
        self.backend_lease = None
        if self.backend_hooks is not None:
            await self.backend_hooks['disconnect']()
        runs, self.runs = tuple(self.runs.values()), {}
        await asyncio.gather(*(self.abort_run(run) for run in runs))
        for value in self.reservations.values():
            item = self.manager.accounts.get(value['account'])
            if item:
                item['busy'] = False
        self.reservations.clear()

    async def setup_backend(self, payload):
        from .bridge import PortalBridge, RemoteCatalog
        from .store import AssistantStore
        lease = self.current_lease(payload)
        if not lease.get('assistant_backend'):
            raise ServiceError('此连接不支持完整助手后端。', 409, 'service_restart_required')
        async with self.backend_lock:
            self.current_lease(payload)
            if self.backend_lease == lease['id']:
                return {'ready': True}
            if self.store is None:
                legacy = Path(lease['legacy_db']).resolve() if lease.get('legacy_db') else None
                if legacy is not None and (not legacy.is_relative_to(self.state.parent) or legacy == self.state / 'assistant.sqlite3'):
                    raise ServiceError('原助手存储位置无效，未执行迁移。', 400, 'invalid_migration')
                self.store = await asyncio.to_thread(AssistantStore, self.state, legacy_db=legacy,
                    legacy_files=legacy.parent / 'lighthouse_assistant/files' if legacy else None)
                self.portal_bridge = PortalBridge(self)
                self.catalog = RemoteCatalog(self.portal_bridge)
            descriptors = await self.portal_bridge.acall('catalog')
            await asyncio.to_thread(self.catalog.replace, descriptors)
            self.current_lease(payload)
            self.backend_lease = lease['id']
            if self.backend_hooks is not None:
                await self.backend_hooks['startup']()
            return {'ready': True}

    async def authorize(self, request):
        from .assistant.lighthouse_ai import AssistantError
        from .bridge import CONTEXT
        if not request.client or request.client.host != '127.0.0.1' or request.headers.get('origin'):
            raise AssistantError('不允许直接访问助手服务。', 403)
        authorization = request.headers.get('authorization', '')
        token = authorization[7:] if authorization.startswith('Bearer ') else ''
        if not token.isascii() or not self.key or not hmac.compare_digest(token, self.key):
            raise AssistantError('助手服务认证未通过。', 403)
        if request.headers.get('x-clipflow-instance') != self.instance:
            raise AssistantError('助手实例已变化，请重新连接。', 409)
        lease = self.current_lease({'lease': request.headers.get('x-clipflow-lease')})
        context = {'id': request.headers.get('x-clipflow-context'), 'lease': lease['id']}
        if not isinstance(context['id'], str) or not re.fullmatch(r'[A-Za-z0-9_-]{30,100}', context['id']):
            raise AssistantError('当前登录授权无效。', 403)
        if self.backend_lease != lease['id']:
            raise AssistantError('助手存储尚未就绪，原消息已保留。', 503)
        request.state.portal_context = context
        CONTEXT.set(context)
        return await self.portal_bridge.acall('authorize', context=context)

    def bridge_key(self, account):
        return hmac.new(self.key.encode(), ('account-bridge:' + account).encode(), hashlib.sha256).hexdigest()

    async def acquire(self, payload):
        from lan_bitable_template_portal.lighthouse_runtime import account_key
        from lan_bitable_template_portal.lighthouse_ai import unprotect_key
        lease = self.current_lease(payload)
        actor = payload.get('actor')
        if not isinstance(actor, dict) or not isinstance(actor.get('scopes'), list) or not actor['scopes'] \
                or any(scope not in {'A', 'B', 'C', 'D', 'E', 'H', '110'} for scope in actor['scopes']):
            raise ServiceError('助手账号范围无效。', 400, 'invalid_actor')
        key = account_key(text(actor.get('id'), 'actor', 200))
        request_id = text(payload.get('request_id'), 'request_id', 64)
        profile, definitions = payload.get('profile'), payload.get('definitions')
        if not isinstance(profile, dict) or not isinstance(definitions, list) or not 1 <= len(definitions) <= 40:
            raise ServiceError('助手网关配置无效。', 400, 'invalid_profile')
        for field in ('id', 'model', 'name', 'endpoint', 'key_cipher'):
            text(profile.get(field), field, 8192 if field == 'key_cipher' else 2000)
        names = [entry.get('name') if isinstance(entry, dict) else None for entry in definitions]
        if len(set(names)) != len(names) or any(not isinstance(name, str) or not re.fullmatch(r'lighthouse_[a-z_]{1,60}', name) for name in names):
            raise ServiceError('助手工具契约无效。', 400, 'invalid_tools')
        async with self.account_locks.setdefault(key, asyncio.Lock()):
            self.current_lease(payload)
            existing = self.manager.accounts.get(key)
            if existing and existing.get('busy'):
                raise ServiceError('当前账号已有正在处理的会话，请稍后继续。', 409, 'account_busy')
            plugin = self.manager.root / key / 'plugin' if hasattr(self.manager, 'root') else self.state / 'accounts' / key / 'plugin'
            await asyncio.to_thread(prepare_tool_plugin,
                self.project / 'bin/openclaw_service/assistant/openclaw/plugin', plugin, definitions)
            model = type('UserModel', (), {'unprotect': staticmethod(unprotect_key)})()
            item = await self.manager.acquire(actor, model, profile, plugin=plugin, tool_names=names,
                bridge_token=self.bridge_key(key), bridge_url=f'http://127.0.0.1:{self.port}/bridge',
                model_parameters=payload.get('model_parameters'))
            if self.lease is not lease:
                item['busy'] = False
                raise ServiceError('助手业务连接已失效，网关已保留待命。', 409, 'lease_expired')
            if payload.get('warm_only') is True:
                item['busy'] = False
            else:
                self.reservations[request_id] = {'account': key, 'lease': lease['id'], 'expires': time.monotonic() + 45}
            return {name: item[name] for name in ('key', 'port', 'token', 'fingerprint', 'protocol')} | {
                'pid': item['process'].pid, 'instance': self.instance, 'reservation': request_id}

    async def begin_run(self, payload):
        lease = self.current_lease(payload)
        key, reservation = payload.get('account'), payload.get('reservation')
        item = self.manager.accounts.get(key)
        owned = self.reservations.get(reservation)
        if not item or not owned or owned['lease'] != lease['id'] or owned['account'] != key or key in self.runs:
            raise ServiceError('助手本轮运行已失效，请重新连接。', 409, 'run_expired')
        session_key = text(payload.get('session_key'), 'session_key', 300)
        if item.get('agent_id') and not session_key.startswith('agent:' + item['agent_id'] + ':'):
            raise ServiceError('助手会话不属于当前登录账号。', 403, 'invalid_session')
        run = {'account': key, 'lease': lease['id'], 'run_id': text(payload.get('run_id'), 'run_id'),
            'session_key': session_key,
            'callback_token': text(payload.get('callback_token'), 'callback_token', 100)}
        self.runs[key] = run
        owned['expires'] = time.monotonic() + 240
        item['busy'] = True
        return {'ok': True}

    async def end_run(self, payload):
        self.current_lease(payload)
        key = payload.get('account')
        run = self.runs.get(key)
        if run and run['lease'] == payload['lease'] and run['run_id'] == payload.get('run_id'):
            self.runs.pop(key, None)
            item = self.manager.accounts.get(key)
            if item:
                item['busy'], item['used_at'] = payload.get('final') is not True, time.monotonic()
        reservation = self.reservations.get(payload.get('reservation'))
        if reservation and reservation['lease'] == payload['lease'] and payload.get('final') is True:
            self.reservations.pop(payload['reservation'], None)
            item = self.manager.accounts.get(reservation['account'])
            if item and reservation['account'] not in self.runs:
                item['busy'] = False
        return {'ok': True}

    async def bridge(self, payload, token):
        lease = self.lease
        secret = getattr(self.manager, 'bridge_token', '')
        if secret:
            if not token.isascii() or not hmac.compare_digest(token, secret):
                raise ServiceError('助手工具认证未通过。', 403, 'unauthorized')
            run = next((entry for key, entry in self.runs.items()
                        if payload.get('agent_id') == self.manager.accounts.get(key, {}).get('agent_id')
                        and payload.get('session_key') == entry['session_key']), None)
        else:
            run = next((entry for key, entry in self.runs.items() if hmac.compare_digest(token, self.bridge_key(key))), None)
        if not run or not lease or run['lease'] != lease['id'] \
                or payload.get('run_id') != run['run_id'] or payload.get('session_key') != run['session_key']:
            raise ServiceError('助手工具调用已失效，未调用业务。', 403, 'run_expired')
        self.current_lease({'lease': lease['id']})
        import httpx
        client = await asyncio.to_thread(httpx.AsyncClient, verify=False, trust_env=False, follow_redirects=False, timeout=310)
        async with client:
            response = await client.post(lease['callback_url'], json=payload,
                headers={'Authorization': 'Bearer ' + run['callback_token']})
            if self.lease is not lease or self.runs.get(run['account']) is not run:
                raise ServiceError('助手工具调用已取消。', 409, 'run_expired')
            if response.status_code != 200 or len(response.content) > 8 * 1024 * 1024:
                raise ServiceError('助手业务查询未完成，原操作未重新提交。', 502, 'callback_unavailable')
            return response.json()

    async def expire_reservations(self):
        while not self.closing:
            await asyncio.sleep(1)
            for identity, record in tuple(self.reservations.items()):
                if record['expires'] <= time.monotonic() and record['account'] not in self.runs:
                    self.reservations.pop(identity, None)
                    item = self.manager.accounts.get(record['account'])
                    if item and record['account'] not in self.runs:
                        item['busy'] = False

    async def close(self):
        self.closing = True
        for task in tuple(self.tasks):
            task.cancel()
        await asyncio.gather(*tuple(self.tasks), return_exceptions=True)
        self.tasks.clear()
        await self.invalidate()
        if self.backend_hooks is not None:
            await self.backend_hooks['shutdown']()
        await self.manager.close()
        if self.portal_bridge is not None:
            await self.portal_bridge.close()


def build_app(host, *, request_stop=lambda _: None):
    from fastapi import FastAPI, Request
    from fastapi.responses import JSONResponse
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    from .assistant.routes import install_assistant_routes
    host.backend_hooks = install_assistant_routes(app, host)
    if hasattr(host.manager, 'forward_model'):
        from .assistant.lighthouse_runtime import install_model_route
        install_model_route(app, host.manager)

    @app.post('/{action:path}')
    async def control(action: str, request: Request):
        try:
            if not request.client or request.client.host != '127.0.0.1' or request.headers.get('origin'):
                raise ServiceError('不允许访问助手服务。', 403, 'forbidden')
            header = request.headers.get('authorization', '')
            token = header[7:] if header.startswith('Bearer ') else ''
            if not token or not token.isascii() or (action != 'bridge' and (not host.key or not hmac.compare_digest(token, host.key))):
                raise ServiceError('助手服务认证未通过。', 403, 'unauthorized')
            body = bytearray()
            async for chunk in request.stream():
                body.extend(chunk)
                if len(body) > (256 * 1024 if action == 'bridge' else 1024 * 1024):
                    raise ServiceError('助手请求过大。', 413, 'request_too_large')
            payload = json.loads(body or b'{}')
            if not isinstance(payload, dict):
                raise ServiceError('助手请求格式无效。', 400, 'invalid_request')
            if action == 'bridge':
                return JSONResponse(await host.bridge(payload, token))
            protocol = payload.get('protocol', PROTOCOL)
            if payload.get('instance') not in (None, host.instance) or type(protocol) is not int or protocol != PROTOCOL:
                raise ServiceError('助手服务版本已变化，请重新连接。', 409, 'service_restart_required')
            if action == 'health':
                data = {'instance': host.instance, 'pid': os.getpid(), 'protocol': PROTOCOL, 'digest': host.digest,
                    'runtime_state': host.runtime_state, 'runtime_error': host.runtime_error,
                    'accounts': len(host.manager.accounts),
                    'gateway_count': len({item['process'].pid for item in host.manager.accounts.values() if item.get('process')}),
                    'gateways': [{'account': key, 'pid': item['process'].pid, 'busy': item.get('busy', False)}
                        for key, item in host.manager.accounts.items() if item.get('process')]}
            elif action == 'register':
                data = await host.register(payload)
            elif action == 'setup':
                data = await host.setup_backend(payload)
            elif action == 'recommend-notice-tags':
                host.current_lease(payload)
                data = await host.backend_hooks['recommend_notice_tags'](payload)
            elif action == 'acquire':
                data = await host.acquire(payload)
            elif action == 'begin-run':
                data = await host.begin_run(payload)
            elif action == 'end-run':
                data = await host.end_run(payload)
            elif action == 'unregister':
                host.current_lease(payload)
                await host.invalidate()
                data = {'ok': True}
            elif action == 'stop-account':
                host.current_lease(payload)
                account = payload.get('account')
                item = host.manager.accounts.get(account)
                if item:
                    await host.manager._stop(item)
                host.runs.pop(account, None)
                data = {'ok': True}
            elif action == 'shutdown':
                reason = payload.get('reason')
                if reason not in {'manual', 'update', 'system'}:
                    raise ServiceError('助手停止原因无效。', 400, 'invalid_request')
                request_stop(reason)
                data = {'ok': True}
            else:
                raise ServiceError('助手服务入口不存在。', 404, 'not_found')
            return JSONResponse({'ok': True, 'data': data}, headers={'Cache-Control': 'no-store'})
        except ServiceError as exc:
            return JSONResponse({'ok': False, 'error': str(exc), 'code': exc.code}, status_code=exc.status)
        except Exception:
            return JSONResponse({'ok': False, 'error': '助手服务请求未完成，原消息已保留。', 'code': 'service_error'}, status_code=503)
    return app

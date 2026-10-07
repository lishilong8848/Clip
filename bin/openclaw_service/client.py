"""Portal connection and lifecycle owner for its internal assistant worker."""
from __future__ import annotations

import asyncio
import math
import os
from pathlib import Path
import time
import uuid

from .protocol import MAX_CONCURRENT_ACCOUNTS, PROTOCOL, PROJECT, ServiceError, atomic_json, code_digest, control_key, descriptor, process_stamp, read_json


class RemoteProcess:
    def __init__(self, pid, owner):
        self.pid, self.stamp, self.owner = pid, process_stamp(pid), owner

    def poll(self):
        return None if not self.owner.disconnected and self.stamp is not None and process_stamp(self.pid) == self.stamp else -1


class ResidentRuntime:
    resident = True

    def __init__(self, state_root, *, callback_url, project=PROJECT, launch=None, legacy_db=None, assistant_backend=False):
        self.root, self.project = Path(state_root).resolve(), Path(project).resolve()
        self.callback_url, self.launch = callback_url, launch
        from .launcher import ManagedService
        self.owner = ManagedService(self.project, self.root) if launch is None else None
        self.legacy_db, self.assistant_backend = legacy_db, assistant_backend
        self.maximum = MAX_CONCURRENT_ACCOUNTS
        self.accounts = {}
        self.portal_id = uuid.uuid4().hex
        self.digest = code_digest(self.project)
        self.descriptor = None
        self.instance, self.lease, self.key = None, None, None
        self.closing, self.disconnected = False, True
        self.lock = asyncio.Lock()
        self.http_lock = asyncio.Lock()
        self.http = None
        self.monitor = None
        self.retry_after = 0

    async def http_client(self):
        async with self.http_lock:
            if self.http is None:
                if self.closing:
                    raise ServiceError('助手业务连接正在关闭。')
                import httpx
                # Creating the TLS context can take a second on Windows, even
                # for loopback HTTP. Reuse the pool and never do that on the loop.
                self.http = await asyncio.to_thread(httpx.AsyncClient, trust_env=False,
                    verify=False,  # Authenticated literal loopback HTTP; never HTTPS.
                    follow_redirects=False, timeout=5,
                    limits=httpx.Limits(max_connections=64, max_keepalive_connections=16, keepalive_expiry=30))
            return self.http

    async def _request(self, action, payload=None, *, timeout=5, registered=True):
        if not self.descriptor or not self.key:
            raise ServiceError('助手服务尚未连接，原消息已保留。')
        import httpx
        data = {'protocol': PROTOCOL, 'instance': self.instance, **(payload or {})}
        if registered:
            data['lease'] = self.lease
        try:
            client = await self.http_client()
            response = await client.post(f"http://127.0.0.1:{self.descriptor['port']}/{action}", json=data,
                headers={'Authorization': 'Bearer ' + self.key}, timeout=timeout)
            value = response.json()
        except (httpx.HTTPError, ValueError):
            self.disconnected = True
            raise ServiceError('助手服务连接中断，原消息已保留；没有重新提交业务。') from None
        if not isinstance(value, dict) or not value.get('ok'):
            codes = {'service_restart_required': '助手需要加载更新，请重启灯塔主程序。',
                'lease_expired': '助手业务连接已失效，原消息已保留，请重新连接。',
                'portal_offline': '灯塔业务连接已停止，未重新提交业务。',
                'account_busy': '当前账号已有处理中的会话，请稍后继续。'}
            code = value.get('code') if isinstance(value, dict) else 'service_error'
            raise ServiceError(codes.get(code, '助手服务请求未完成，原消息已保留。'), response.status_code, code or 'service_error')
        return value['data']

    async def prepare(self, *, progress=lambda _: None):
        async with self.lock:
            if self.closing:
                raise ServiceError('助手业务连接正在关闭。')
            if self.monitor is None:
                self.monitor = asyncio.create_task(self._monitor())
            if (self.root / 'update-hold.json').is_file():
                raise ServiceError('助手正在更新，其他业务可继续使用。', code='service_updating')
            saved = await asyncio.to_thread(descriptor, self.root, self.project)
            if saved is None or self.owner is not None:
                recovery = await asyncio.to_thread(read_json, self.root / 'recovery.json', {})
                saved_attempts = recovery.get('attempts', [])
                if not isinstance(saved_attempts, list):
                    saved_attempts = []
                now = time.time()
                attempts = [stamp for stamp in saved_attempts if type(stamp) in (int, float) and math.isfinite(stamp) and 0 <= now - stamp < 600]
                running = self.owner is not None and self.owner.process is not None and self.owner.process.poll() is None
                if saved is None and not running and len(attempts) >= 3:
                    raise ServiceError('助手连续启动未完成，请查看启动日志或重启灯塔；其他功能不受影响。', code='service_recovery_limited')
                launch = self.owner.start if self.owner is not None else self.launch
                try:
                    spawned = await asyncio.to_thread(launch, self.project)
                except ServiceError:
                    raise
                except Exception:
                    raise ServiceError('助手后台启动未完成，请查看灯塔启动日志；其他功能不受影响。', code='service_launch_denied') from None
                if spawned is not False:
                    await asyncio.to_thread(atomic_json, self.root / 'recovery.json', {'attempts': [*attempts, time.time()]})
                saved = await asyncio.to_thread(descriptor, self.root, self.project)
                deadline = time.monotonic() + 25
                while saved is None and time.monotonic() < deadline and not self.closing:
                    if self.owner is not None and self.owner.process is not None and self.owner.process.poll() is not None:
                        raise ServiceError('助手后台启动失败，请查看同一启动窗口中的 OpenClaw 日志；其他功能不受影响。', code='service_launch_denied')
                    await asyncio.sleep(.25)
                    saved = await asyncio.to_thread(descriptor, self.root, self.project)
                if saved is None:
                    raise ServiceError('助手仍在后台启动，原消息已保留；其他功能可正常使用。', code='service_startup_timeout')
            if self.closing:
                raise ServiceError('助手业务连接正在关闭。')
            self.key = await asyncio.to_thread(control_key, self.root)
            if self.descriptor is None or saved['instance'] != self.descriptor['instance']:
                self.instance, self.lease = None, None
                self.accounts.clear()
            self.descriptor = saved
            health = await self._request('health', registered=False)
            if health.get('protocol') != PROTOCOL or health.get('digest') != self.digest:
                raise ServiceError('助手需要加载更新，请重启灯塔主程序。', 409, 'service_restart_required')
            self.instance = health['instance']
            if self.lease is None or self.disconnected:
                registered = await self._request('register', {'portal_id': self.portal_id, 'pid': os.getpid(),
                    'process_stamp': process_stamp(os.getpid()), 'callback_url': self.callback_url(), 'digest': self.digest,
                    'assistant_backend': self.assistant_backend, 'legacy_db': str(self.legacy_db) if self.legacy_db else None}, registered=False)
                self.lease = registered['lease']
                if self.assistant_backend:
                    await self._request('setup', timeout=45)
                # Only consecutive failed starts are rate-limited. Healthy main
                # restarts/updates must not exhaust the recovery allowance.
                await asyncio.to_thread(atomic_json, self.root / 'recovery.json', {'attempts': []})
            self.disconnected = False
            return health

    async def _monitor(self):
        while not self.closing:
            await asyncio.sleep(2)
            await self._check_connection()

    async def _check_connection(self):
        saved = await asyncio.to_thread(descriptor, self.root, self.project)
        if not saved or not self.descriptor or saved['instance'] != self.descriptor['instance']:
            self.disconnected = True
            self.accounts.clear()
            self.lease = None
        if self.closing or not self.disconnected or time.monotonic() < self.retry_after:
            return
        try:
            # Reconnect the control/authorization channel only. Never replay a
            # message or business operation while recovering the resident host.
            await self.prepare()
            self.retry_after = 0
        except ServiceError as exc:
            self.retry_after = time.monotonic() + (60 if exc.code in {
                'service_updating', 'service_recovery_limited', 'service_restart_required', 'service_owner_active'} else 15)

    async def acquire(self, actor, model, profile, *, definitions=(), warm_only=False, **_):
        await self.prepare()
        identity = uuid.uuid4().hex
        value = await self._request('acquire', {'actor': {'id': actor['id'], 'scopes': actor.get('allowed_scopes', actor['scopes'])},
            'profile': profile, 'definitions': definitions, 'request_id': identity, 'warm_only': warm_only}, timeout=210)
        item = {**value, 'busy': not warm_only, 'used_at': time.monotonic(), 'root': self.root / value['key']}
        item['process'] = RemoteProcess(value['pid'], self)
        self.accounts[item['key']] = item
        return item

    async def begin_run(self, item, session_key, run_id, callback_token):
        if item.get('run_id'):
            await self._request('end-run', {'account': item['key'], 'run_id': item['run_id'],
                'reservation': item['reservation'], 'final': False})
        await self._request('begin-run', {'account': item['key'], 'reservation': item['reservation'],
            'session_key': session_key, 'run_id': run_id, 'callback_token': callback_token})
        item['run_id'] = run_id

    async def release(self, item):
        try:
            await self._request('end-run', {'account': item['key'], 'run_id': item.get('run_id'),
                'reservation': item['reservation'], 'final': True})
        finally:
            item['busy'] = False

    async def _stop(self, item):
        await self._request('stop-account', {'account': item['key']})
        self.accounts.pop(item['key'], None)

    async def close(self):
        self.closing = True
        if self.monitor:
            self.monitor.cancel()
            await asyncio.gather(self.monitor, return_exceptions=True)
            self.monitor = None
        try:
            if self.lease and not self.disconnected:
                try:
                    await self._request('unregister')
                except ServiceError:
                    pass
        finally:
            try:
                if self.owner is not None:
                    await asyncio.to_thread(self.owner.close)
            finally:
                self.accounts.clear()
                self.lease = None
                self.disconnected = True
                async with self.http_lock:
                    if self.http is not None:
                        await self.http.aclose()
                        self.http = None

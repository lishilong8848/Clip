"""Assistant-side business bridge. The portal retains sessions and final authority."""
from __future__ import annotations

import asyncio
import base64
from contextvars import ContextVar
import copy
import json
import threading

import httpx

from .assistant.lighthouse_ai import AssistantError
from .assistant.lighthouse_api import PortalAPICatalog

CONTEXT = ContextVar('lighthouse_portal_context', default=None)
OPERATION = ContextVar('lighthouse_business_operation', default=None)
MAX_RESPONSE = 100 * 1024 * 1024


class PortalBridge:
    def __init__(self, host):
        self.host = host
        self.sync_lock, self.async_lock = threading.Lock(), asyncio.Lock()
        self.sync_http = self.async_http = None

    def sync_client(self):
        with self.sync_lock:
            if self.sync_http is None:
                self.sync_http = httpx.Client(verify=False, trust_env=False, follow_redirects=False, timeout=8)
            return self.sync_http

    async def async_client(self):
        async with self.async_lock:
            if self.async_http is None:
                self.async_http = await asyncio.to_thread(httpx.AsyncClient,
                    verify=False, trust_env=False, follow_redirects=False, timeout=12)
            return self.async_http

    async def close(self):
        if self.async_http is not None:
            await self.async_http.aclose()
        if self.sync_http is not None:
            await asyncio.to_thread(self.sync_http.close)

    def _request(self, action, payload=None, context=None):
        lease = self.host.current_lease({'lease': self.host.lease['id'] if self.host.lease else None})
        context = context or CONTEXT.get()
        if action != 'catalog' and (not isinstance(context, dict) or context.get('lease') != lease['id']):
            raise AssistantError('灯塔业务连接已失效，未重新提交业务。', 409)
        body = {'instance': self.host.instance, 'lease': lease['id'], 'action': action,
            'context_id': context.get('id') if context else None, 'payload': payload or {}}
        return lease, lease['callback_url'], body

    @staticmethod
    def _decode(status, body):
        try:
            value = json.loads(body)
        except (ValueError, UnicodeError):
            raise AssistantError('灯塔业务响应无效，原输入已保留。', 502) from None
        if not isinstance(value, dict) or not value.get('ok'):
            error = value.get('error') if isinstance(value, dict) else None
            raise AssistantError(error or '灯塔业务连接未完成，未自动重发。', status if status >= 400 else 502)
        return value.get('data')

    def call(self, action, payload=None, context=None):
        lease, url, body = self._request(action, payload, context)
        try:
            with self.sync_client().stream('POST', url, json=body, headers={'Authorization': 'Bearer ' + self.host.key}) as response:
                parts, size = [], 0
                for chunk in response.iter_bytes():
                    size += len(chunk)
                    if size > MAX_RESPONSE:
                        raise AssistantError('灯塔业务响应过大，未使用不完整数据。', 502)
                    parts.append(chunk)
                value = self._decode(response.status_code, b''.join(parts))
        except httpx.HTTPError:
            raise AssistantError('灯塔业务连接未完成，原输入已保留；未自动重发。', 503) from None
        self.host.current_lease({'lease': lease['id']})
        return value

    async def acall(self, action, payload=None, context=None):
        lease, url, body = self._request(action, payload, context)
        timeout = 135 if action == 'invoke' else 12
        try:
            client = await self.async_client()
            async with client.stream('POST', url, json=body, headers={'Authorization': 'Bearer ' + self.host.key}, timeout=timeout) as response:
                parts, size = [], 0
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > MAX_RESPONSE:
                        raise AssistantError('灯塔业务响应过大，未使用不完整数据。', 502)
                    parts.append(chunk)
                value = self._decode(response.status_code, b''.join(parts))
        except httpx.HTTPError:
            raise AssistantError('灯塔业务连接未完成，原输入已保留；未自动重发。', 503) from None
        self.host.current_lease({'lease': lease['id']})
        return value

    def search(self, question, actor):
        result = self.call('search', {'question': question, 'scopes': actor['scopes']})
        if not isinstance(result, dict) or not isinstance(result.get('hits'), list) or not isinstance(result.get('warnings'), list):
            raise AssistantError('本地资料查询未完成，不能判断为空。', 502)
        return result['hits'], result['warnings']

    def cached(self, kind, scopes, allowed):
        return self.call('cached', {'kind': kind, 'scopes': scopes, 'allowed': allowed})


class RemoteCatalog(PortalAPICatalog):
    """Reuse descriptors/UI helpers, validate and execute with the real portal."""
    def __init__(self, bridge):
        self.bridge = bridge
        self.app = None
        self._descriptors, self._models, self._route_for_id, self._order = {}, {}, {}, []

    def replace(self, descriptors):
        if not isinstance(descriptors, list) or any(not isinstance(row, dict) or not isinstance(row.get('id'), str) for row in descriptors):
            raise AssistantError('业务接口目录无效。', 502)
        self._descriptors = {row['id']: copy.deepcopy(row) for row in descriptors}
        self._order = list(self._descriptors)

    def validate_operation(self, operation):
        result = self.bridge.call('validate', {'operation': operation})
        if not isinstance(result, dict) or not isinstance(result.get('operation'), dict) or not isinstance(result.get('missing'), list):
            raise AssistantError('业务参数核对未完成。', 502)
        return result['operation'], result['missing']

    def parse_notice(self, text, *, fallback_work_type=''):
        return self.bridge.call('parse_notice', {'text': text, 'fallback_work_type': fallback_work_type})

    async def invoke(self, operation, request, file_provider=None):
        context = getattr(request.state, 'portal_context', None)
        ids = [identity for values in (operation.get('files') or {}).values() for identity in (values if isinstance(values, list) else [values])]
        uploads = []
        if ids:
            if file_provider is None:
                raise AssistantError('附件不存在或没有访问权限。', 404)
            for identity in dict.fromkeys(ids):
                item = await asyncio.to_thread(file_provider, identity)
                uploads.append({key: item[key] for key in ('id', 'owner', 'name', 'mime', 'path', 'sha256', 'size', 'source_scopes') if key in item})
        result = await self.bridge.acall('invoke', {'operation': operation, 'uploads': uploads,
            'operation_id': OPERATION.get()}, context)
        binary = result.get('_binary') if isinstance(result, dict) else None
        if binary:
            try:
                binary['content'] = base64.b64decode(binary.pop('content_base64'), validate=True)
            except (KeyError, ValueError, TypeError):
                raise AssistantError('业务文件返回不完整，未保存成功。', 502) from None
        return result

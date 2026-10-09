"""Portal authority for the independent assistant. No models or assistant writes."""
from __future__ import annotations

import asyncio
import base64
import copy
import hashlib
import json
from pathlib import Path
import re
import secrets
import threading
import time

from openclaw_service.assistant.lighthouse_ai import AssistantError


async def actor_for(controller, runtime, request):
    from openclaw_service.assistant.lighthouse_sources import SCOPES
    from .portal_service import BUILDING_OPEN_ID_MAP
    session = await asyncio.to_thread(controller._current_session, request)
    if not session:
        raise AssistantError('请重新登录后继续。', 401)
    user = session.get('user') or {}
    identity = str(session.get('open_id') or user.get('open_id') or '').strip()
    if not identity or session.get('is_guest') or str(session.get('role') or user.get('role')).lower() == 'guest':
        raise AssistantError('请使用正式账号登录后使用灯塔助手。', 403)
    allowed = set(runtime.auth_manager.session_scopes(session))
    if 'ALL' in allowed:
        allowed = set(SCOPES)
    if 'CAMPUS' in allowed:
        allowed.update('ABCDE')
    actor = {'id': identity, 'is_admin': runtime.auth_manager.is_admin(session),
        'scopes': sorted(allowed & SCOPES), 'can_manage_settings': True}
    actor['name'] = str(user.get('name') or user.get('en_name') or session.get('name') or '').strip()
    actor['employee_no'] = str(user.get('employee_no') or user.get('employee_number') or '').strip()
    actor['role_label'] = '管理员' if actor['is_admin'] else '楼栋值班账号' if identity in BUILDING_OPEN_ID_MAP.values() else '普通账号'
    actor['home_scope'] = next((code for code, open_id in BUILDING_OPEN_ID_MAP.items()
                                if open_id == identity and code in actor['scopes']), '')
    actor['learning_scopes'] = sorted(set('ABCDEH') & set(actor['scopes'])) if actor['is_admin'] else [
        code for code in 'ABCDEH' if BUILDING_OPEN_ID_MAP.get(code) == identity and code in actor['scopes']]
    channel = getattr(request.state, 'lighthouse_channel', '')
    if isinstance(channel, str) and re.fullmatch(r'feishu:oc_[A-Za-z0-9]+', channel):
        actor['channel'] = channel
    return actor


class PortalAuthority:
    def __init__(self, app, controller, runtime, client):
        self.app, self.controller, self.runtime, self.client = app, controller, runtime, client
        self.contexts, self.writes = {}, {}
        self.catalog = None
        self.catalog_lock = threading.Lock()
        from .assistant_read_consent import ReadConsent
        self.read_consent = ReadConsent(runtime.state_store)
        self.tables = None

    def get_catalog(self):
        with self.catalog_lock:
            if self.catalog is None:
                from openclaw_service.assistant.lighthouse_api import PortalAPICatalog
                self.catalog = PortalAPICatalog(self.app)
        return self.catalog

    def context(self, request, actor, *, plan_id=None):
        now = time.monotonic()
        expired = [key for key, value in self.contexts.items() if value['expires'] <= now]
        for key in expired:
            self.contexts.pop(key, None)
        if len(self.contexts) >= 1024:
            raise AssistantError('助手请求较多，请稍后继续。', 429)
        identity = secrets.token_urlsafe(32)
        self.contexts[identity] = {'request': request, 'actor': copy.deepcopy(actor),
            'expires': now + 1800, 'plan_id': plan_id, 'lease': self.client.lease}
        return identity

    async def authorize(self, identity):
        context = self.contexts.get(identity)
        if not context or context['expires'] <= time.monotonic() or context['lease'] != self.client.lease:
            raise AssistantError('原登录授权已失效，未重新提交业务。', 403)
        current = await actor_for(self.controller, self.runtime, context['request'])
        prior = context['actor']
        if current['id'] != prior['id'] or set(prior['scopes']) - set(current['scopes']) or prior['is_admin'] and not current['is_admin']:
            raise AssistantError('登录身份或权限已变化，请重新操作。', 403)
        # A long-running turn cannot gain new scopes without a fresh user request.
        current['scopes'] = list(prior['scopes'])
        current['learning_scopes'] = sorted(set(current['learning_scopes']) & set(prior['learning_scopes']))
        return context, current

    @staticmethod
    def narrowed(actor, scopes):
        if not isinstance(scopes, list) or any(not isinstance(code, str) for code in scopes) or not scopes or set(scopes) - set(actor['scopes']):
            raise AssistantError('请求楼栋超出当前登录权限。', 403)
        return {**actor, 'scopes': list(scopes)}

    def files(self, actor, operation, uploads):
        if not isinstance(uploads, list) or len(uploads) > 100:
            raise AssistantError('业务附件参数无效。')
        wanted = {identity for values in (operation.get('files') or {}).values() for identity in (values if isinstance(values, list) else [values])}
        root = (self.client.root / 'files').resolve()
        files, total = {}, 0
        for item in uploads:
            if not isinstance(item, dict) or item.get('owner') != actor['id'] or item.get('id') not in wanted:
                raise AssistantError('附件不存在或没有访问权限。', 403)
            identity = item['id']
            if not isinstance(identity, str) or not re.fullmatch(r'[a-f0-9]{32}', identity) or identity in files:
                raise AssistantError('附件编号无效。', 403)
            path = Path(str(item.get('path') or '')).resolve()
            size = item.get('size')
            if not path.is_relative_to(root) or path.stem != identity or not path.is_file() or type(size) is not int or not 0 < size <= 64 * 1024 * 1024 or path.stat().st_size != size:
                raise AssistantError('附件原文件不可用。', 404)
            if set(item.get('source_scopes') or []) - set(actor['scopes']):
                raise AssistantError('当前账号无权上传该业务附件。', 403)
            total += size
            if total > 100 * 1024 * 1024:
                raise AssistantError('业务附件合计不得超过100MiB。', 413)
            files[identity] = dict(item)
        if wanted != set(files):
            raise AssistantError('业务附件不完整，未执行上传。', 404)
        return files

    async def invoke(self, context, actor, payload):
        catalog = await asyncio.to_thread(self.get_catalog)
        operation = payload.get('operation')
        if not isinstance(operation, dict):
            raise AssistantError('业务操作参数无效。')
        descriptor = catalog.get(operation.get('api_id', ''))
        if descriptor['read_only'] and re.search(r'people|signature|personnel|recipients|customer|contract|/plan-convergence/|/files|download|export', operation.get('api_id', ''), re.I):
            consent = await asyncio.to_thread(self.read_consent.require, actor, 'invoke', operation,
                descriptor.get('group', '') + '：' + descriptor.get('name', '读取受保护业务资料'),
                getattr(context['request'].state, 'feishu_question', ''))
            if consent:
                raise AssistantError(consent['message'], 409)
        provider = await asyncio.to_thread(self.files, actor, operation, payload.get('uploads') or [])
        if descriptor['read_only']:
            return await catalog.invoke(operation, context['request'], file_provider=provider.get)
        operation_id = payload.get('operation_id')
        plan_id = context.get('plan_id')
        if not plan_id or not isinstance(operation_id, str) or not re.fullmatch('plan:' + re.escape(plan_id) + r':[0-9]{1,3}', operation_id):
            raise AssistantError('业务写入须由本人在助手中确认，未执行操作。', 403)
        fingerprint = hashlib.sha256(json.dumps(operation, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        key = (actor['id'], operation_id, fingerprint)
        task = self.writes.get(key)
        if task is None:
            if len(self.writes) >= 4096:
                raise AssistantError('业务请求较多，请重新连接后继续。', 429)
            task = asyncio.create_task(catalog.invoke(operation, context['request'], file_provider=provider.get))
            self.writes[key] = task
        # Dropping the chat connection must not cancel an already submitted write.
        return copy.deepcopy(await asyncio.shield(task))

    async def dispatch(self, message):
        if not isinstance(message, dict) or message.get('instance') != self.client.instance or message.get('lease') != self.client.lease:
            raise AssistantError('原助手业务连接已失效。', 403)
        action, payload = message.get('action'), message.get('payload') or {}
        if not isinstance(payload, dict):
            raise AssistantError('业务请求参数无效。')
        if action == 'catalog':
            catalog = await asyncio.to_thread(self.get_catalog)
            return list(catalog._descriptors.values())
        context, actor = await self.authorize(message.get('context_id'))
        if action == 'authorize':
            return actor
        if action in {'table_catalog', 'table_records', 'staff_count'}:
            if self.tables is None:
                from .assistant_tables import AssistantTables
                self.tables = AssistantTables(self.runtime.service, self.runtime.state_store)
            scoped = self.narrowed(actor, payload.get('scopes', actor['scopes']))
            if action == 'table_catalog':
                return await asyncio.to_thread(self.tables.catalog, payload.get('keyword', ''), payload.get('page', 1))
            query = payload.get('query') or {}
            if not isinstance(query, dict):
                raise AssistantError('查询参数无效。')
            label = '在岗人员数量统计（仅返回汇总，不提供个人明细）' if action == 'staff_count' else (
                await asyncio.to_thread(self.tables.table, query.get('table_id')))['name'] + '的多维表记录'
            label += '；楼栋范围：' + '、'.join(scoped['scopes'])
            if query.get('filters'):
                from openclaw_service.assistant.lighthouse_ai import safe_data
                label += '；筛选：' + json.dumps(safe_data(query['filters']), ensure_ascii=False)
            if action == 'staff_count' or not query.get('metadata_only'):
                consent = await asyncio.to_thread(self.read_consent.require, scoped, action, payload, label,
                    getattr(context['request'].state, 'feishu_question', ''))
                if consent:
                    return consent
            result = await asyncio.to_thread(self.tables.staff_count, scoped) if action == 'staff_count' else await asyncio.to_thread(self.tables.read, scoped, query)
            # Trace source and scope without logging values, questions or personal data.
            import logging
            logging.info('Assistant read action=%s actor=%s channel=%s table=%s scopes=%s', action,
                hashlib.sha256(actor['id'].encode()).hexdigest()[:12], 'feishu' if actor.get('channel') else 'web',
                query.get('table_id', 'personnel'), ','.join(scoped['scopes']))
            return result
        catalog = await asyncio.to_thread(self.get_catalog)
        if action == 'validate':
            operation, missing = await asyncio.to_thread(catalog.validate_operation, payload.get('operation'))
            return {'operation': operation, 'missing': missing}
        if action == 'parse_notice':
            return await asyncio.to_thread(catalog.parse_notice, payload.get('text', ''), fallback_work_type=payload.get('fallback_work_type', ''))
        if action == 'invoke':
            result = await self.invoke(context, actor, payload)
            if result.get('_binary'):
                binary = result['_binary']
                binary['content_base64'] = await asyncio.to_thread(lambda: base64.b64encode(binary.pop('content')).decode('ascii'))
            return result
        if action == 'search':
            from openclaw_service.assistant.lighthouse_sources import LocalAssistantSources
            scoped = self.narrowed(actor, payload.get('scopes'))
            consent = await asyncio.to_thread(self.read_consent.require, scoped, action, payload,
                '检索本地业务资料（可能包含人员信息）；范围：' + '、'.join(scoped['scopes']),
                getattr(context['request'].state, 'feishu_question', ''))
            if consent:
                raise AssistantError(consent['message'], 409)
            hits, warnings = await asyncio.to_thread(LocalAssistantSources(self.runtime.state_store), payload.get('question', ''), scoped)
            return {'hits': hits, 'warnings': warnings}
        if action == 'cached':
            from openclaw_service.assistant.lighthouse_pending import cached_items
            scoped = self.narrowed(actor, payload.get('scopes'))
            return await asyncio.to_thread(cached_items, payload.get('kind', ''), scoped['scopes'], self.runtime, allowed=actor['scopes'])
        from openclaw_service.assistant.lighthouse_sources import question_bank, question_material_file, work_order_records
        query = payload.get('query') or {}
        if not isinstance(query, dict):
            raise AssistantError('查询参数无效。')
        if action == 'question_bank':
            return await asyncio.to_thread(question_bank, getattr(self.runtime, 'learning_service', None), actor, query)
        if action == 'question_material':
            consent = await asyncio.to_thread(self.read_consent.require, actor, action, payload,
                '读取原权限内的题库附件资料', getattr(context['request'].state, 'feishu_question', ''))
            if consent:
                raise AssistantError(consent['message'], 409)
            return await asyncio.to_thread(question_material_file, getattr(self.runtime, 'learning_service', None), actor, query)
        if action == 'work_orders':
            return await asyncio.to_thread(work_order_records, self.runtime.state_store, actor, query)
        raise AssistantError('业务桥入口不存在。', 404)

    async def close(self):
        self.contexts.clear()
        pending = [task for task in self.writes.values() if not task.done()]
        if pending:
            try:
                await asyncio.wait_for(asyncio.shield(asyncio.gather(*pending, return_exceptions=True)), 2)
            except asyncio.TimeoutError:
                pass

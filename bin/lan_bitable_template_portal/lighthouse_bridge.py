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
import sqlite3
import threading
import time

from openclaw_service.assistant.lighthouse_ai import AssistantError, safe_text
from .assistant_shared_read import original_actor, reader_actor, shared_query_actor, shared_operation, SharedRead


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
    from .module_access import allowed_modules
    actor['modules'] = sorted(allowed_modules(runtime.auth_manager, session))
    actor['name'] = str(user.get('name') or user.get('en_name') or session.get('name') or '').strip()
    actor['employee_no'] = str(user.get('employee_no') or user.get('employee_number') or '').strip()
    actor['role_label'] = '管理员' if actor['is_admin'] else '楼栋值班账号' if identity in BUILDING_OPEN_ID_MAP.values() else '普通账号'
    actor['home_scope'] = next((code for code, open_id in BUILDING_OPEN_ID_MAP.items()
                                if open_id == identity and code in actor['scopes']), '')
    actor['learning_scopes'] = sorted(set('ABCDEH') & set(actor['scopes'])) if actor['is_admin'] else [
        code for code in 'ABCDEH' if BUILDING_OPEN_ID_MAP.get(code) == identity and code in actor['scopes']]
    if not actor['is_admin'] and identity not in BUILDING_OPEN_ID_MAP.values():
        resolve = getattr(getattr(runtime, 'learning_service', None), 'resolve_self', None)
        if callable(resolve):
            from .learning import LearningError
            try:
                person = await asyncio.to_thread(resolve, identity)
            except (LearningError, OSError, sqlite3.Error):
                person = {}
            if person.get('person_id'):
                actor['learning_person_id'] = person['person_id']
                actor['learning_scopes'] = sorted(set((person.get('person') or {}).get('scopes') or []) & set(actor['scopes']) & set('ABCDEH'))
    channel = getattr(request.state, 'lighthouse_channel', '')
    if isinstance(channel, str) and re.fullmatch(r'feishu:oc_[A-Za-z0-9]+', channel):
        actor['channel'] = channel
    client = request.headers.get('x-lighthouse-client', '')
    if client and not re.fullmatch(r'[A-Za-z0-9_-]{16,80}', client):
        raise AssistantError('助手客户端标识无效。')
    if client and not actor.get('channel'):
        actor['_execution_lane'] = 'web:' + client
    return shared_query_actor(actor)


class PortalAuthority:
    def __init__(self, app, controller, runtime, client):
        self.app, self.controller, self.runtime, self.client = app, controller, runtime, client
        self.contexts, self.writes = {}, {}
        self.catalog = None
        self.catalog_lock = threading.Lock()
        from .assistant_read_consent import ReadConsent
        self.read_consent = ReadConsent(runtime.state_store)
        self.tables = None
        from .portal_auth import TopicDelegations
        auth = getattr(runtime, 'auth_manager', None)
        self.delegations = getattr(auth, '_topic_delegations', None)
        if auth is not None and self.delegations is None:
            self.delegations = auth._topic_delegations = TopicDelegations(auth, runtime.state_store)

    def get_catalog(self):
        with self.catalog_lock:
            if self.catalog is None:
                from openclaw_service.assistant.lighthouse_api import PortalAPICatalog
                self.catalog = PortalAPICatalog(self.app)
        return self.catalog

    def context(self, request, actor, *, plan_id=None, shared_plan=None):
        now = time.monotonic()
        expired = [key for key, value in self.contexts.items() if value['expires'] <= now]
        for key in expired:
            self.contexts.pop(key, None)
        if len(self.contexts) >= 1024:
            raise AssistantError('助手请求较多，请稍后继续。', 429)
        identity = secrets.token_urlsafe(32)
        personal = self.personal_actor(actor)
        self.contexts[identity] = {'request': request, 'actor': copy.deepcopy(personal),
            'expires': now + 1800, 'plan_id': plan_id, 'lease': self.client.lease,
            'shared_plan': copy.deepcopy(shared_plan),
            'schedule': copy.deepcopy(getattr(getattr(request, 'state', None), 'assistant_schedule', None)),
            'topic_evidence_pending': bool(getattr(getattr(request, 'state', None), 'feishu_topic_evidence_pending', False)),
            'feishu_topic': copy.deepcopy(getattr(getattr(request, 'state', None), 'feishu_topic', None))}
        return identity

    async def authorize(self, identity):
        context = self.contexts.get(identity)
        if not context or context['expires'] <= time.monotonic() or context['lease'] != self.client.lease:
            raise AssistantError('原登录授权已失效，未重新提交业务。', 403)
        schedule = context.get('schedule')
        if schedule:
            await asyncio.to_thread(self.runtime.assistant_scheduler.validate, schedule)
        current = original_actor(await actor_for(self.controller, self.runtime, context['request']))
        prior = context['actor']
        if set(prior.get('modules', [])) - set(current.get('modules', [])):
            raise AssistantError('功能权限已变化，请重新操作。', 403)
        if current['id'] != prior['id'] or set(prior['scopes']) - set(current['scopes']) or prior['is_admin'] and not current['is_admin']:
            raise AssistantError('登录身份或权限已变化，请重新操作。', 403)
        if (prior.get('learning_person_id') and current.get('learning_person_id')
                and prior['learning_person_id'] != current['learning_person_id']):
            raise AssistantError('学练人员身份已变化，请重新操作。', 403)
        # A long-running turn cannot gain new scopes without a fresh user request.
        current['scopes'] = list(prior['scopes'])
        current['modules'] = list(prior.get('modules', []))
        current['learning_scopes'] = sorted(set(current['learning_scopes']) & set(prior['learning_scopes']))
        # Only the verified Feishu callback creates this server-side capability.
        shared = context.get('shared_plan')
        if shared and current.get('channel') == 'feishu:' + shared.get('chat_id', ''):
            current['_shared_plan'] = copy.deepcopy(shared)
        topic = context.get('feishu_topic')
        channel = current.get('channel', '')
        if (isinstance(topic, dict) and channel.startswith('feishu:')
                and str(topic.get('id', '')).startswith(channel.removeprefix('feishu:') + ':om_')):
            current['_feishu_topic'] = {'id': topic['id'], 'context': str(topic.get('context', ''))[:12000]}
            if self.delegations is None:
                raise AssistantError('话题授权服务尚未就绪，未执行委派操作。', 503)
            from .portal_service import PortalError
            try:
                if not context.get('topic_evidence_pending'):
                    current = await asyncio.to_thread(self.delegations.enrich, current, topic['id'])
            except PortalError:
                raise AssistantError('话题参与人或权限已失效，请重新操作。', 403) from None
            current['_topic_access'] = copy.deepcopy(context.get('topic_access') or topic.get('access') or [])
            from .portal_auth import topic_access_allowed
            if not topic_access_allowed({'capabilities': current['_topic_access']}, current):
                raise AssistantError('话题资料权限已变化，未继续读取或回复旧资料。', 403)
            if isinstance(topic.get('questions'), list):
                current['_feishu_topic']['questions'] = [safe_text(question, limit=2000)
                    for question in topic['questions'][-8:] if isinstance(question, str)]
            if isinstance(topic.get('documents'), list):
                current['_feishu_topic']['documents'] = copy.deepcopy(topic['documents'][-8:])
        current = shared_query_actor(current)
        if schedule:
            current.update(_scheduled_task=schedule['id'], _scheduled_mode=schedule['mode'],
                           _execution_lane='schedule:' + schedule['id'])
        return context, current

    @staticmethod
    def record_access(context, capability):
        values = context.setdefault('topic_access', copy.deepcopy((context.get('feishu_topic') or {}).get('access') or []))
        if capability not in values:
            if len(values) >= 128:
                raise AssistantError('本话题资料范围较多，请分话题查询。', 409)
            values.append(copy.deepcopy(capability))

    @staticmethod
    def personal_actor(actor):
        actual = original_actor(actor)
        return {**actual, 'scopes': list(actual.get('_personal_scopes', actual['scopes'])),
                'modules': list(actual.get('_personal_modules', actual.get('modules', [])))}

    def learning_summary(self, execution, operation):
        import datetime as dt
        from contextlib import closing
        from .learning_engagement import weekly_stats
        execution.validate()
        service = getattr(self.runtime, 'learning_service', None)
        if service is None:
            raise AssistantError('画像学练资料暂未就绪。', 503)
        params = dict(operation.get('params') or {})
        if params.get('view') == 'participation':
            from .learning_personal import participation
            scope = params.get('scope')
            if (not execution.matches_operation(operation) or not execution.matches('GET', '/api/learning/profile')
                    or 'learning' not in execution.modules or set(execution.scopes) != {scope}
                    or set(params) - {'scope', 'from', 'to', 'period', 'view'}
                    or operation.get('body') or operation.get('files')):
                raise AssistantError('学练参与统计与原话题授权范围不一致，未查询。', 403)
            # Scope-only projection through the native read-only building guard, never personal history.
            data = participation(service, {'id': execution.operator, 'shared_account': True, 'scope': scope}, params)
            data.update(read_only=True, aggregate_only=True)
            execution.audit('read')
            return {'ok': True, 'status': 200, 'data': data, '_raw': data,
                    'api_id': operation['api_id'], 'truncated': False}
        today = dt.date.today()
        dates = service._date_filters(params)
        first, last = dates.get('from') or (today - dt.timedelta(days=6)).isoformat(), dates.get('to') or today.isoformat()
        if not 0 <= (dt.date.fromisoformat(last) - dt.date.fromisoformat(first)).days <= 366:
            raise AssistantError('学练汇总日期范围无效，单次最多366天。')
        people = {p['id']: p for p in service._all('person')}
        rows = []
        with closing(service._connect()) as db:
            for scope in execution.scopes:
                for identity, row in weekly_stats(service, first, last, db, scope).items():
                    rows.append({'name': people.get(identity, {}).get('name', '人员信息待核对'), 'scope': scope,
                                 **{k: row[k] for k in ('answered', 'correct', 'wrong', 'choices', 'accuracy')}})
        execution.audit('read')
        summary = {key: sum(row[key] for row in rows) for key in ('answered', 'correct', 'wrong', 'choices')}
        summary['accuracy'] = round(summary['correct'] * 100 / summary['choices'], 1) if summary['choices'] else None
        data = {'scope': params.get('scope'), 'from': first, 'to': last, 'summary': summary, 'people': rows,
                'read_only': True, 'aggregate_only': True}
        return {'ok': True, 'status': 200, 'data': data, '_raw': data, 'api_id': operation['api_id'], 'truncated': False}

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
        grant = actor.get('_shared_plan') or {}
        shared_ids = set(grant.get('file_ids', [])) if actor.get('channel') == 'feishu:' + str(grant.get('chat_id')) else set()
        for item in uploads:
            if not isinstance(item, dict) or item.get('owner') != actor['id'] and item.get('id') not in shared_ids or item.get('id') not in wanted:
                raise AssistantError('附件不存在或没有访问权限。', 403)
            topic_id = (actor.get('_feishu_topic') or {}).get('id')
            if item.get('topic_id') != topic_id and not (item.get('id') in shared_ids and topic_id and grant.get('topic_id') == topic_id):
                raise AssistantError('附件不属于本话题，未用于业务上传。', 403)
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
        normalized, missing = await asyncio.to_thread(catalog.validate_operation, operation)
        if context.get('schedule') and descriptor.get('method') not in {'GET', 'HEAD'} and not shared_operation(normalized):
            raise AssistantError('定时任务只能准备业务操作，必须由用户确认后才能提交。', 403)
        if actor.get('_shared_business_queries') and not missing and shared_operation(normalized):
            from starlette.requests import Request
            def validate_login():
                session = self.controller._current_session(context['request'])
                identity = str((session or {}).get('open_id') or ((session or {}).get('user') or {}).get('open_id') or '')
                if (identity != actor['id'] or (session or {}).get('is_guest')
                        or str((session or {}).get('role') or ((session or {}).get('user') or {}).get('role') or '').lower() == 'guest'):
                    raise AssistantError('登录已失效，未继续共享业务查询。', 401)
            grant = SharedRead(self.runtime.auth_manager, actor['id'], normalized, validate_login)
            gateway_request = Request({**context['request'].scope,
                'state': {**context['request'].scope.get('state', {}), 'assistant_shared_business_read': grant}})
            if context.get('feishu_topic'):
                self.record_access(context, {'shared_business_read': True})
            # The gateway validates again and still needs the original path parameters.
            result = await catalog.invoke(operation, gateway_request)
            if result.get('_binary'):
                # Original files can contain private data that text redaction cannot remove.
                raise AssistantError('共享查询不直接转发未脱敏原文件，请读取文档解析内容或在原业务页面按原权限下载。', 403)
            from openclaw_service.assistant.lighthouse_ai import shared_data
            return shared_data(result)
        actor = original_actor(actor)
        execution = None
        gateway_request = context['request']
        topic = actor.get('_feishu_topic') or {}
        if topic.get('id') and actor.get('_topic_capabilities'):
            from .portal_auth import topic_capability
            from .portal_service import PortalError
            normalized, missing = await asyncio.to_thread(catalog.validate_operation, operation)
            if missing:
                raise AssistantError('缺少必填字段，请先补全后再执行。', 422)
            try:
                capability = topic_capability(normalized, actor.get('_personal_scopes', []))
            except PortalError:
                # Nondelegable personal/admin APIs still use the real account's original guards.
                actor = self.personal_actor(actor)
            else:
                try:
                    execution = await asyncio.to_thread(self.delegations.issue, topic['id'], actor['id'], capability)
                except PortalError:
                    raise AssistantError('没有一位有效话题参与人具备本项业务的完整权限，未执行。', 403) from None
                from starlette.requests import Request
                gateway_request = Request({**context['request'].scope,
                    'state': {**context['request'].scope.get('state', {}), 'lighthouse_topic_execution': execution}})
                await asyncio.to_thread(execution.audit, 'authorized', operation_id=str(payload.get('operation_id') or ''))
                self.record_access(context, capability)
                if capability['module'] == 'learning':
                    return await asyncio.to_thread(self.learning_summary, execution, normalized)
        if topic.get('id') and execution is None:
            personal = self.personal_actor(actor)
            self.record_access(context, {'legacy': {k: personal.get(k) for k in ('scopes', 'modules', 'is_admin')},
                                         'source_owner': actor['id']})
        from .module_access import required_modules
        needed = required_modules(descriptor['path'], (operation.get('body') or {}).get('work_type') or (operation.get('params') or {}).get('work_type', ''))
        if needed and not needed & set(actor.get('modules', [])):
            raise AssistantError('当前账号尚未获授该功能权限。', 403)
        if descriptor['read_only'] and re.search(r'people|signature|personnel|recipients|customer|contract|/plan-convergence/|/files|download|export', operation.get('api_id', ''), re.I):
            consent = await asyncio.to_thread(self.read_consent.require, actor, 'invoke', operation,
                descriptor.get('group', '') + '：' + descriptor.get('name', '读取受保护业务资料'),
                getattr(context['request'].state, 'feishu_question', ''))
            if consent:
                raise AssistantError(consent['message'], 409)
        provider = await asyncio.to_thread(self.files, actor, operation, payload.get('uploads') or [])
        if descriptor['read_only']:
            return await catalog.invoke(operation, gateway_request, file_provider=provider.get)
        operation_id = payload.get('operation_id')
        plan_id = context.get('plan_id')
        if not plan_id or not isinstance(operation_id, str) or not re.fullmatch('plan:' + re.escape(plan_id) + r':[0-9]{1,3}', operation_id):
            raise AssistantError('业务写入须由本人在助手中确认，未执行操作。', 403)
        fingerprint = hashlib.sha256(json.dumps(operation, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        key = ('topic:' + topic['id'] if topic.get('id') else actor['id'], operation_id, fingerprint)
        task = self.writes.get(key)
        if task is None:
            if len(self.writes) >= 4096:
                raise AssistantError('业务请求较多，请重新连接后继续。', 429)
            if descriptor['path'].startswith('/api/feishu-workpartner/'):
                from .feishu_workpartner import CONFIRMED_EXECUTION
                token = CONFIRMED_EXECUTION.set((actor['id'], operation_id))
                try:
                    task = asyncio.create_task(catalog.invoke(operation, gateway_request, file_provider=provider.get))
                finally:
                    CONFIRMED_EXECUTION.reset(token)
            else:
                task = asyncio.create_task(catalog.invoke(operation, gateway_request, file_provider=provider.get))
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
        if action == 'assistant_schedule':
            if context.get('schedule'):
                raise AssistantError('定时任务不能再次创建或修改定时任务。', 403)
            return await self.runtime.assistant_scheduler.manage(context, actor, payload)
        if action in {'feishu_document', 'table_catalog', 'table_records', 'staff_count', 'roster_count', 'question_bank', 'question_material', 'search', 'cached', 'work_orders'}:
            shared = actor.get('_shared_business_queries')
            actor = reader_actor(actor) if shared else self.personal_actor(actor)
            actor.pop('_topic_capabilities', None)
            if context.get('feishu_topic'):
                self.record_access(context, {'shared_business_read': True} if shared else
                    {'legacy': {k: actor.get(k) for k in ('scopes', 'modules', 'is_admin')}, 'source_owner': actor['id']})
        if action == 'feishu_document':
            from openclaw_service.assistant.lighthouse_documents import read_document
            if set(payload) - {'url', 'offset', 'scopes'}:
                raise AssistantError('文档读取参数无效。')
            scoped = self.narrowed(actor, payload.get('scopes', actor['scopes']))
            result = await asyncio.to_thread(read_document, self.runtime.service, scoped,
                payload.get('url'), payload.get('offset', 0))
            await self.authorize(message.get('context_id'))
            import logging
            logging.info('Assistant document read actor=%s channel=%s scopes=%s characters=%s',
                hashlib.sha256(actor['id'].encode()).hexdigest()[:12], 'feishu' if actor.get('channel') else 'web',
                ','.join(scoped['scopes']), result['total_characters'])
            return result
        if action in {'table_catalog', 'table_records', 'staff_count', 'roster_count'}:
            if 'links' not in actor.get('modules', []):
                raise AssistantError('当前账号尚未获授多维表导航与查询权限。', 403)
            if self.tables is None:
                from .assistant_tables import AssistantTables
                self.tables = AssistantTables(self.runtime.service, self.runtime.state_store)
            scoped = self.narrowed(actor, payload.get('scopes', actor['scopes']))
            if action == 'table_catalog':
                return await asyncio.to_thread(self.tables.catalog, payload.get('keyword', ''), payload.get('page', 1))
            query = payload.get('query') or {}
            if not isinstance(query, dict):
                raise AssistantError('查询参数无效。')
            if action == 'roster_count':
                label = '值班排班人数及姓名（不提供个人身份标识）；日期：' + str(payload.get('date', '')) + '；班次：' + str(payload.get('shift') or '白夜班')
            else:
                label = '在岗人员数量统计（仅返回汇总，不提供个人明细）' if action == 'staff_count' else (
                    await asyncio.to_thread(self.tables.table, query.get('table_id')))['name'] + '的多维表记录'
            label += '；楼栋范围：' + '、'.join(scoped['scopes'])
            if query.get('filters'):
                from openclaw_service.assistant.lighthouse_ai import safe_data
                label += '；筛选：' + json.dumps(safe_data(query['filters']), ensure_ascii=False)
            if not actor.get('_shared_business_queries') and (action in {'staff_count', 'roster_count'} or not query.get('metadata_only')):
                consent = await asyncio.to_thread(self.read_consent.require, scoped, action, payload, label,
                    getattr(context['request'].state, 'feishu_question', ''))
                if consent:
                    return consent
            if action == 'roster_count':
                result = await asyncio.to_thread(self.tables.roster_count, scoped, payload.get('date'), payload.get('shift', ''))
            else:
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
            consent = None if actor.get('_shared_business_queries') else await asyncio.to_thread(self.read_consent.require, scoped, action, payload,
                '检索本地业务资料（可能包含人员信息）；范围：' + '、'.join(scoped['scopes']),
                getattr(context['request'].state, 'feishu_question', ''))
            if consent:
                raise AssistantError(consent['message'], 409)
            from .module_access import visible_url
            profiles = actor.get('_topic_capabilities') or [actor]
            hits, warnings, eligible = [], [], False
            for profile in profiles:
                if set(scoped['scopes']) - set(profile.get('scopes', [])):
                    continue
                eligible = True
                view = {**scoped, 'modules': profile.get('modules', [])}
                rows, notes = await asyncio.to_thread(LocalAssistantSources(self.runtime.state_store), payload.get('question', ''), view)
                warnings.extend(notes)
                for hit in rows:
                    if visible_url(view['modules'], hit.get('url', '')) and hit not in hits:
                        hits.append(hit)
            if not eligible:
                raise AssistantError('没有一位有效话题参与人具备该完整查询范围。', 403)
            return {'hits': hits, 'warnings': warnings}
        if action == 'cached':
            modules = {'events': {'event'}, 'orders': {'maintenance', 'polling', 'adjust'}, 'mops': {'engineer_mop'}}.get(payload.get('kind', ''), set())
            if not modules & set(actor.get('modules', [])):
                raise AssistantError('当前账号尚未获授该业务模块权限。', 403)
            from openclaw_service.assistant.lighthouse_pending import cached_items
            scoped = self.narrowed(actor, payload.get('scopes'))
            result = await asyncio.to_thread(cached_items, payload.get('kind', ''), scoped['scopes'], self.runtime, allowed=actor['scopes'])
            from urllib.parse import parse_qs, urlsplit
            profiles = actor.get('_topic_capabilities') or [actor]
            def visible(row):
                values = parse_qs(urlsplit(row.get('url', '')).query)
                module = values.get('work_type', ['maintenance'])[0] if payload.get('kind') == 'orders' else next(iter(modules))
                scopes = set(row.get('scopes') or [])
                permit = bool(scopes) and module in modules and any(module in profile.get('modules', []) and scopes <= set(profile.get('scopes', [])) for profile in profiles)
                if permit and context.get('feishu_topic') and not actor.get('_shared_business_queries'):
                    self.record_access(context, {'module': module, 'scopes': sorted(scopes), 'action': 'read', 'method': 'READ',
                        'path': 'cached:' + payload.get('kind', ''), 'resource': hashlib.sha256(str(row.get('url', '')).encode()).hexdigest(), 'admin': False})
                return permit
            return ([row for row in result[0] if visible(row)], *result[1:])
        from openclaw_service.assistant.lighthouse_sources import question_bank, question_material_file, work_order_records
        query = payload.get('query') or {}
        if not isinstance(query, dict):
            raise AssistantError('查询参数无效。')
        if action == 'question_bank':
            if 'learning' not in actor.get('modules', []):
                raise AssistantError('当前账号尚未获授画像学练权限。', 403)
            return await asyncio.to_thread(question_bank, getattr(self.runtime, 'learning_service', None), actor, query)
        if action == 'question_material':
            if 'learning' not in actor.get('modules', []):
                raise AssistantError('当前账号尚未获授画像学练权限。', 403)
            consent = None if actor.get('_shared_business_queries') else await asyncio.to_thread(self.read_consent.require, actor, action, payload,
                '读取原权限内的题库附件资料', getattr(context['request'].state, 'feishu_question', ''))
            if consent:
                raise AssistantError(consent['message'], 409)
            return await asyncio.to_thread(question_material_file, getattr(self.runtime, 'learning_service', None), actor, query)
        if action == 'work_orders':
            if not set(actor.get('modules', [])) & {'maintenance', 'polling', 'adjust'}:
                raise AssistantError('当前账号尚未获授工单所属功能权限。', 403)
            result = await asyncio.to_thread(work_order_records, self.runtime.state_store, actor, query)
            if context.get('feishu_topic') and not actor.get('_shared_business_queries'):
                from openclaw_service.assistant.lighthouse_sources import codes
                for row in result.get('items', []):
                    self.record_access(context, {'module': row.get('work_type') or 'polling', 'scopes': sorted(codes(row.get('scope'))),
                        'action': 'read', 'method': 'READ', 'path': 'work_orders', 'resource': hashlib.sha256(str(row.get('group_id', '')).encode()).hexdigest(), 'admin': False})
            return result
        raise AssistantError('业务桥入口不存在。', 404)

    async def close(self):
        self.contexts.clear()
        pending = [task for task in self.writes.values() if not task.done()]
        if pending:
            try:
                await asyncio.wait_for(asyncio.shield(asyncio.gather(*pending, return_exceptions=True)), 2)
            except asyncio.TimeoutError:
                pass

"""Assistant-only shared business reads; never an account or write grant."""
from __future__ import annotations

import copy
from contextvars import ContextVar

from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_sources import SCOPES
from .module_access import MODULE_IDS, required_modules

SHARED_READ = ContextVar('assistant_shared_business_read', default=None)
SHARED_MODULES = MODULE_IDS - {'accounts'}
_ACCESS = ('scopes', 'allowed_scopes', 'modules', 'learning_scopes', 'is_admin')


def original_actor(actor):
    result = dict(actor)
    if actor.get('_shared_business_queries'):
        for key in _ACCESS:
            result.pop(key, None)
    result.update(copy.deepcopy(actor.get('_business_write_access') or {}))
    result.pop('_shared_business_queries', None)
    result.pop('_business_write_access', None)
    return result


def shared_query_actor(actor):
    if actor.get('_shared_business_queries'):
        return dict(actor)
    return {**actor, '_shared_business_queries': True,
            '_business_write_access': {key: copy.deepcopy(actor[key]) for key in _ACCESS if key in actor},
            'scopes': sorted(SCOPES), 'learning_scopes': list('ABCDEH'),
            'modules': sorted(SHARED_MODULES | set(actor.get('modules') or []))}


def reader_actor(actor):
    return {**actor, 'is_admin': True} if actor.get('_shared_business_queries') else actor


def shared_operation(operation):
    path = operation.get('path', '')
    from openclaw_service.assistant.lighthouse_api import _is_readonly_post
    readonly_post = operation.get('method') == 'POST' and _is_readonly_post(operation.get('api_id', '').removeprefix('POST '))
    if (not operation.get('read_only') or operation.get('method') not in {'GET', 'HEAD'} and not readonly_post
            or not path.startswith('/api/')):
        return False
    if path.startswith(('/api/assistant/', '/api/signatures/')):
        return path in {'/api/assistant/pending', '/api/assistant/work-orders', '/api/assistant/question-bank',
                        '/api/assistant/question-material', '/api/signatures/people',
                        '/api/signatures/management/people', '/api/signatures/management/recipients'}
    if path.startswith(('/api/auth/', '/api/admin/', '/api/backend/', '/api/feishu-workpartner/',
                        '/api/handover-links', '/api/handover-password-reset', '/api/notice-memory', '/api/workspace-layouts')):
        return False
    if path == '/api/scope-overview' or path.startswith('/api/pre-shift-checks'):
        return not operation.get('files')
    needed = required_modules(path, (operation.get('params') or {}).get('work_type', ''))
    return bool(needed) and needed <= SHARED_MODULES and not operation.get('files')


class SharedRead:
    def __init__(self, auth, operator, operation, validate):
        if not shared_operation(operation):
            raise AssistantError('该入口不是共享业务只读查询。', 403)
        self.auth, self.operator = auth, operator
        self.operation, self._validate = copy.deepcopy(operation), validate
        self.active = True

    def matches_operation(self, operation):
        return self.active and self.operation == operation and shared_operation(operation)

    def matches(self, method, path):
        from urllib.parse import unquote
        return method == self.operation['method'] and unquote(path) == self.operation['path']

    def validate(self):
        if not self.active:
            raise AssistantError('本次共享读取已结束，请重新查询。', 403)
        token = SHARED_READ.set(None)
        try:
            self._validate()
        finally:
            SHARED_READ.reset(token)

    def profile(self, profile):
        return {**profile, 'role': 'admin', 'level': 'L0', 'is_admin': True,
                'scopes': ['ALL', *sorted(SCOPES)], 'scope_rules': ['ALL'], 'modules': sorted(SHARED_MODULES)}

    def session(self, session):
        if (not isinstance(session, dict) or session.get('is_guest')
                or str(session.get('role') or (session.get('user') or {}).get('role') or '').lower() == 'guest'
                or str(session.get('open_id') or (session.get('user') or {}).get('open_id') or '') != self.operator):
            raise AssistantError('共享查询身份已失效。', 401)
        return {**session, 'role': 'admin', 'allowed_scopes': ['ALL', *sorted(SCOPES)],
                'user': {**(session.get('user') or {}), 'role': 'admin'}}

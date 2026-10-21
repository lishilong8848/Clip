"""Use existing portal sessions and grants for the document-library child page."""
import asyncio
from collections import Counter
from pathlib import Path
import re
import threading
import time
from urllib.parse import urlsplit

from openclaw_service.assistant.lighthouse_ai import AssistantError
from .document_library import DocumentLibraryService, SCOPES
from .document_library_routes import install_document_library_routes
from .module_access import allowed_modules


def install_document_library_portal(app, controller, runtime):
    service = None
    lock = threading.Lock()

    def actor_from_session(session):
        if not session:
            raise AssistantError('请先登录。', 401)
        user = session.get('user') or {}
        identity = str(user.get('open_id') or session.get('open_id') or '').strip()
        if not identity or session.get('is_guest') or session.get('role') in {'guest', 'temporary'}:
            raise AssistantError('请使用正式账号查看资料。', 403)
        modules = allowed_modules(runtime.auth_manager, session)
        if 'knowledge' not in modules:
            raise AssistantError('当前账号没有共享知识库权限。', 403)
        scopes = set(runtime.auth_manager.session_scopes(session))
        if 'ALL' in scopes:
            scopes = set(SCOPES)
        if 'CAMPUS' in scopes:
            scopes.update('ABCDE')
        return {'id': identity, 'name': str(user.get('name') or ''), 'modules': sorted(modules),
                'scopes': sorted(scopes & SCOPES), 'is_admin': runtime.auth_manager.is_admin(session),
                'read_only': session.get('source') == 'admin_impersonation'}

    def refresh_actor(actor):
        current = actor_from_session({'user': {'open_id': actor['id'], 'name': actor.get('name', '')}})
        current['read_only'] = bool(actor.get('read_only'))
        if current['read_only']:
            current['is_admin'] = False
        return current

    async def authorize(request):
        actor = await asyncio.to_thread(actor_from_session, await asyncio.to_thread(controller._current_session, request))
        if request.method not in {'GET', 'HEAD', 'OPTIONS'}:
            if actor['read_only']:
                raise AssistantError('临时代登录仅可查看资料，不能修改或上传。', 403)
            source = urlsplit(request.headers.get('origin') or request.headers.get('referer') or '')
            expected = urlsplit(controller._request_base_url(request))
            if ((source.scheme, source.netloc.lower()) != (expected.scheme, expected.netloc.lower())
                    or request.headers.get('sec-fetch-site') == 'cross-site'):
                raise AssistantError('不允许跨来源提交资料。', 403)
        return actor

    def people_provider(actor, source):
        auth = runtime.auth_manager
        provider = getattr(auth, '_account_directory_provider', None)
        snapshot = provider() if provider else None
        app_id = auth.current_feishu_app_id()
        if (not app_id or not isinstance(snapshot, dict) or snapshot.get('schema') != 4
                or snapshot.get('app_id') != app_id or not isinstance(snapshot.get('people'), dict)
                or not 0 <= time.time() - float(snapshot.get('loaded_at') or 0) < 300
                or getattr(auth, '_account_directory_refresh_failed', False)):
            return None
        people = list(snapshot['people'].values())
        if any(not isinstance(person, dict) for person in people):
            return None
        counts = Counter(identity for person in people if not person.get('inactive')
            and isinstance(person.get('login_ids'), list) for identity in set(
                value for value in person['login_ids'] if isinstance(value, str)))
        scopes = (set(actor['scopes']) & set(source['read_scopes']) & set(source['write_scopes'])
            & set(source['fixed_scopes'] or SCOPES))
        options = []
        for person in people:
            ids, declared = person.get('login_ids'), person.get('scopes')
            name = person.get('name')
            if (person.get('selectable') is not True or person.get('inactive') or person.get('disabled_reason')
                    or not isinstance(ids, list) or len(ids) != 1 or not isinstance(ids[0], str)
                    or not re.fullmatch(r'ou_[A-Za-z0-9]+', ids[0]) or counts[ids[0]] != 1
                    or not isinstance(name, str) or not name.strip() or not isinstance(declared, list)
                    or not any(isinstance(scope, str) and scope in scopes for scope in declared)
                    or auth._open_id_explicitly_disabled(ids[0])):
                continue
            options.append({'id': ids[0], 'name': name.strip()})
        if auth.current_feishu_app_id() != app_id or provider() is not snapshot:
            return None
        return sorted(options, key=lambda person: (person['name'], person['id']))

    def get_service():
        nonlocal service
        with lock:
            if service is None:
                service = DocumentLibraryService(Path(runtime.state_store.db_path).parent,
                    refresh_actor=refresh_actor, people_provider=people_provider,
                    authorize_source=lambda actor, _source, _scopes, action: action != 'write' or not actor.get('read_only'))
        return service

    install_document_library_routes(app, authorize, get_service)

"""Test-only assistant API fixture. Never starts a resident host, Node or tasks."""
from __future__ import annotations

import asyncio
from contextvars import ContextVar
from pathlib import Path
import tempfile
from types import SimpleNamespace
from urllib.parse import urlsplit

from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_api import PortalAPICatalog
from openclaw_service.assistant.lighthouse_sources import LocalAssistantSources
from openclaw_service.assistant.routes import install_assistant_routes
from lan_bitable_template_portal.lighthouse_bridge import actor_for


def install_test_backend(app, controller, runtime):
    """Unit-test the extracted API with synthetic authority; proxy tests are separate."""
    if not hasattr(runtime, 'state_store'):
        from openclaw_service.store import AssistantStore
        directory = tempfile.TemporaryDirectory(prefix='assistant_route_fixture_')
        app.state.assistant_test_directory = directory
        runtime.state_store = AssistantStore(directory.name)
        app.add_event_handler('shutdown', directory.cleanup)
    root = Path(runtime.state_store.db_path).resolve().parent
    if not root.is_relative_to(Path(tempfile.gettempdir()).resolve()):
        raise AssertionError('Assistant route tests require a temporary data root')
    current_actor = ContextVar('assistant_route_fixture_actor', default=None)
    async def authorize(request):
        actor = await actor_for(controller, runtime, request)
        if request.method != 'GET':
            source = request.headers.get('origin') or request.headers.get('referer')
            expected = urlsplit(controller._request_base_url(request))
            actual = urlsplit(source) if source else None
            if actual is None or (actual.scheme, actual.netloc) != (expected.scheme, expected.netloc):
                raise AssistantError('不允许跨来源提交。', 403)
        current_actor.set(actor)
        return actor

    async def prepare(**_):
        return None
    async def close():
        return None
    async def no_process(*_, **__):
        raise AssertionError('A route unit test must not start an OpenClaw process')
    def cached(kind, scopes, allowed):
        from openclaw_service.assistant.lighthouse_pending import cached_items
        return cached_items(kind, scopes, runtime, allowed=allowed)
    async def business(action, payload):
        from openclaw_service.assistant.lighthouse_sources import question_bank, question_material_file, work_order_records
        actor, query = current_actor.get(), payload['query']
        if actor is None:
            raise AssertionError('A route fixture requires an authenticated actor')
        if action == 'question_bank':
            return await asyncio.to_thread(question_bank, getattr(runtime, 'learning_service', None), actor, query)
        if action == 'question_material':
            return await asyncio.to_thread(question_material_file, getattr(runtime, 'learning_service', None), actor, query)
        if action == 'work_orders':
            return await asyncio.to_thread(work_order_records, runtime.state_store, actor, query)
        raise AssertionError('Unsupported test business read')

    manager = SimpleNamespace(root=root / 'gateway-fixture', maximum=2, accounts={}, resident=False,
        prepare=prepare, model_client=prepare, close=close, acquire=no_process)
    host = SimpleNamespace(store=runtime.state_store, state=root, port=getattr(controller, 'bound_port', 19004),
        catalog=PortalAPICatalog(app), authorize=authorize, manager=manager,
        portal_bridge=SimpleNamespace(search=LocalAssistantSources(runtime.state_store), cached=cached, acall=business))
    hooks = install_assistant_routes(app, host)
    # Some route fixtures add native routes after installation. Scan at the
    # first authorized request, matching service registration's final catalogue.
    original_authorize = host.authorize
    async def registered_authorize(request):
        if not getattr(host, 'scanned', False):
            host.catalog = PortalAPICatalog(app)
            host.scanned = True
        return await original_authorize(request)
    host.authorize = registered_authorize
    app.add_event_handler('startup', hooks['startup'])
    app.add_event_handler('shutdown', hooks['shutdown'])
    app.state.assistant_test_host = host
    return host

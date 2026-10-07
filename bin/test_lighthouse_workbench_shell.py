"""Exercise the actual workbench route body without starting business services."""
import ast
import asyncio
import copy
import sys
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlencode, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fastapi import Request, Response


class WorkbenchShellTests(unittest.IsolatedAsyncioTestCase):
    async def call_route(self, path, query='', authenticated=True, ongoing=None, detail_only=False):
        session = {'user': {'open_id': 'fixture', 'name': 'Fixture'}, 'role': 'admin'} if authenticated else None
        renderer = Mock(return_value='<html><body><main class="workspace">native</main></body></html>')
        service = SimpleNamespace(query_records=Mock(return_value={'records': [], 'ongoing': []}))
        controller = SimpleNamespace(_current_session=lambda _: session,
            _static_file_response=Mock(return_value=Response('index-shell', media_type='text/html')),
            _ensure_source_snapshot_background=Mock(), _authorized_scope_or_error=lambda _session, scope: scope,
            _get_ongoing=Mock(return_value=ongoing or []), _reconcile_orphan_started_items=Mock(),
            _cached_service_payload=lambda _key, loader: loader(),
            _portal_error_response=lambda error, **_: Response(str(error), status_code=500))
        runtime = SimpleNamespace(service=service, _ongoing_items_marker=lambda _: 'fixture', auth_manager=SimpleNamespace(
            default_scope=lambda _: 'E', filter_scope_options=lambda options, _: options, is_admin=lambda _: True))
        tree = ast.parse((Path(__file__).parent / 'clipflow_backend/main.py').read_text(encoding='utf-8-sig'))
        node = copy.deepcopy(next(node for node in ast.walk(tree) if isinstance(node, ast.AsyncFunctionDef) and node.name == 'workbench_lite_page'))
        node.decorator_list = []
        namespace = {'Request': Request, 'Response': Response, 'Any': object, 'self': controller, 'asyncio': asyncio, 'time': time,
            'PortalRuntime': runtime, 'urlencode': urlencode, 'portal_index_file': lambda: Path('fixture-index.html'),
            'NOTICE_TYPE_BY_WORK_TYPE': {'maintenance': '维保通告'}, 'SCOPE_OPTIONS': [{'value': 'E', 'label': 'E楼'}],
            'PENDING_PAGE_SIZE': 24, 'ONGOING_PAGE_SIZE': 18, 'BUILDING_OPEN_ID_MAP': {}, '_current_month_label': lambda: '10月',
            'render_workbench_lite': renderer}
        exec(compile(ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])), '<native-workbench-route>', 'exec'), namespace)
        request = Request({'type': 'http', 'method': 'GET', 'path': path, 'scheme': 'http', 'server': ('fixture', 80),
            'query_string': query.encode(), 'headers': []})
        request.state.workbench_detail_only = detail_only
        with patch('lan_bitable_template_portal.lighthouse_widget.inject_lighthouse_widget', side_effect=lambda html, *_: html + '<div>widget</div>') as widget:
            response = await namespace['workbench_lite_page'](request)
        return response, controller, renderer, service, widget

    async def test_top_level_serves_shell_without_initial_business_reads(self):
        response, controller, renderer, service, _ = await self.call_route('/workbench-lite', 'scope=E&work_type=maintenance')
        self.assertEqual(response.body, b'index-shell')
        controller._static_file_response.assert_called_once()
        controller._get_ongoing.assert_not_called()
        renderer.assert_not_called(); service.query_records.assert_not_called()

    async def test_embedded_page_keeps_native_rendering_without_duplicate_widget(self):
        response, controller, renderer, service, widget = await self.call_route('/workbench-lite', 'scope=E&work_type=maintenance&_assistant_frame=1')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'class="workspace"', response.body)
        self.assertNotIn(b'widget', response.body)
        renderer.assert_called_once(); service.query_records.assert_called_once()
        controller._static_file_response.assert_not_called(); widget.assert_not_called()

    async def test_fragment_path_still_renders_native_document_not_shell(self):
        response, controller, renderer, _, _ = await self.call_route('/api/workbench/lite-fragment', 'scope=E&work_type=maintenance')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'class="workspace"', response.body)
        renderer.assert_called_once(); controller._static_file_response.assert_not_called()

    async def test_ongoing_detail_does_not_requery_full_workbench(self):
        for kind in ['maintenance', 'change', 'repair', 'power', 'polling', 'adjust']:
            row = {'active_item_id': 'active-fixture', 'target_record_id': 'rec-fixture', 'work_type': kind,
                   'building_codes': ['E'], 'title': 'Fixture', 'progress': 'Original progress'}
            response, _, renderer, service, _ = await self.call_route('/api/workbench/lite-detail',
                f'scope=E&work_type={kind}&active_item_id=active-fixture', ongoing=[row], detail_only=True)
            self.assertEqual(response.status_code, 200)
            service.query_records.assert_not_called()
            self.assertEqual(renderer.call_args.kwargs['payload']['ongoing'], [row])

    async def test_missing_ongoing_detail_uses_original_query_path(self):
        response, _, _, service, _ = await self.call_route('/api/workbench/lite-detail',
            'scope=E&active_item_id=missing', detail_only=True)
        self.assertEqual(response.status_code, 200)
        service.query_records.assert_called_once()

    async def test_unauthenticated_top_and_embedded_requests_require_login(self):
        for query in ['', '_assistant_frame=1']:
            with self.subTest(query=query):
                response, controller, renderer, service, _ = await self.call_route('/workbench-lite', query, authenticated=False)
                self.assertEqual(response.status_code, 302)
                self.assertTrue(response.headers['location'].startswith('/api/auth/login?'))
                next_url = parse_qs(urlsplit(response.headers['location']).query)['next'][0]
                self.assertNotIn('_assistant_frame', next_url)
                controller._static_file_response.assert_not_called()
                renderer.assert_not_called(); service.query_records.assert_not_called()

    async def test_order_pages_preserve_anonymous_html_and_only_add_authenticated_widget(self):
        tree = ast.parse((Path(__file__).parent / 'clipflow_backend/main.py').read_text(encoding='utf-8-sig'))
        for name in ['polling_work_order_page', 'polling_work_order_steps_page']:
            node = copy.deepcopy(next(node for node in ast.walk(tree) if isinstance(node, ast.AsyncFunctionDef) and node.name == name))
            node.decorator_list = []
            for session in [None, {'user': {'open_id': 'fixture', 'name': 'Fixture'}, 'role': 'admin'}]:
                with self.subTest(page=name, logged_in=bool(session)):
                    controller = SimpleNamespace(_current_session=lambda _: session)
                    namespace = {'self': controller, 'Request': Request, 'Response': Response, 'asyncio': asyncio,
                        'portal_index_file': lambda: Path('fixture-index.html'), 'render_polling_work_order_page': lambda: 'original-order-html',
                        'render_polling_work_order_steps_page': lambda: 'original-order-html'}
                    exec(compile(ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])), '<native-order-page>', 'exec'), namespace)
                    request = Request({'type': 'http', 'method': 'GET', 'path': '/polling-work-order', 'query_string': b'token=fixture', 'headers': []})
                    with patch('lan_bitable_template_portal.lighthouse_widget.inject_lighthouse_widget',
                               side_effect=lambda html, auth, *_: html + ':widget' if auth else html):
                        response = await namespace[name](request)
                    self.assertEqual(response.body, b'original-order-html:widget' if session else b'original-order-html')
                    self.assertEqual(response.headers['cache-control'], 'no-store')


if __name__ == '__main__':
    unittest.main()

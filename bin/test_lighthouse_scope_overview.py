"""Run the native scope-overview endpoint with isolated projections, not Feishu."""
import ast
import asyncio
import copy
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fastapi import Request
from lan_bitable_template_portal.lighthouse_model import scoped_operation
from lan_bitable_template_portal.portal_service import PortalError
from test_lighthouse_frontend_contracts import build_native_catalog

OPTIONS = [{'value': code, 'label': code + '楼'} for code in ('A', 'B', 'C', 'D', 'E', 'H', '110', 'CAMPUS', 'ALL')]


class ScopeOverviewTests(unittest.IsolatedAsyncioTestCase):
    async def run_endpoint(self, query, allowed=('ALL',)):
        session = {'allowed_scopes': list(allowed), 'user': {'open_id': 'fixture'}}
        service = Mock()
        service.get_scope_overview.side_effect = lambda **kw: {'source_snapshot_ready': True,
            'scope_options': OPTIONS, 'scopes': {scope: {'scope': scope, 'ongoing': 1} for scope in kw['scopes']}}
        auth = SimpleNamespace(filter_scope_options=lambda options, _session: options if 'ALL' in allowed else [o for o in options if o['value'] in allowed],
                               filter_scope_overview=lambda data, _session: data)
        def authorized(_session, scope):
            if scope not in {o['value'] for o in OPTIONS} or ('ALL' not in allowed and scope not in allowed):
                raise PortalError('无权访问此楼栋')
            return scope
        controller = SimpleNamespace(_current_session=lambda _: session, _auth_required_response=lambda: {},
            _ensure_source_snapshot_background=Mock(), _get_ongoing=Mock(return_value=[]),
            _reconcile_orphan_started_items=Mock(), _authorized_scope_or_error=authorized,
            _cached_service_payload=lambda _key, loader: loader(), _json_ok=lambda _r, _s, data: data,
            _portal_error_response=lambda exc, default_status: {'error': str(exc), 'status': getattr(exc, 'status_code', default_status)})
        tree = ast.parse((Path(__file__).parent / 'clipflow_backend/main.py').read_text(encoding='utf-8-sig'))
        node = copy.deepcopy(next(node for node in ast.walk(tree) if isinstance(node, ast.AsyncFunctionDef) and node.name == 'scope_overview'))
        node.decorator_list = []
        namespace = {'self': controller, 'Request': Request, 'asyncio': asyncio, 'SCOPE_OPTIONS': OPTIONS, 'PortalError': PortalError,
            'PortalRuntime': SimpleNamespace(auth_manager=auth, service=service, _ongoing_items_marker=lambda _: 'fixture')}
        exec(compile(ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])), '<native-scope-overview>', 'exec'), namespace)
        request = Request({'type': 'http', 'method': 'GET', 'path': '/api/scope-overview', 'query_string': query.encode(), 'headers': []})
        return await namespace['scope_overview'](request), service

    async def test_admin_selected_e_returns_only_e_data_and_options(self):
        data, service = await self.run_endpoint('scope=E')
        self.assertEqual(set(data['scopes']), {'E'})
        self.assertEqual([o['value'] for o in data['scope_options']], ['E'])
        self.assertEqual(service.get_scope_overview.call_args.kwargs['scopes'], ['E'])

    async def test_no_parameter_preserves_homepage_all_scope_contract(self):
        data, _ = await self.run_endpoint('')
        self.assertEqual(set(data['scopes']), {o['value'] for o in OPTIONS})

    async def test_unauthorized_scope_does_not_read_business_data(self):
        data, service = await self.run_endpoint('scope=E', allowed=('D',))
        self.assertEqual(data['status'], 403)
        service.get_scope_overview.assert_not_called()

    async def test_campus_contains_only_authorized_campus_scopes(self):
        data, _ = await self.run_endpoint('scope=CAMPUS')
        self.assertEqual(set(data['scopes']), {*'ABCDE', 'CAMPUS'})

    def test_assistant_catalog_and_scope_gate_supply_selected_e_by_default(self):
        descriptor = build_native_catalog().get('GET /api/scope-overview')
        self.assertIn('scope', descriptor['schema']['query'])
        result = scoped_operation({'api_id': descriptor['id']}, descriptor,
            {'id': 'fixture', 'is_admin': True, 'scopes': ['E'], 'allowed_scopes': list('ABCDEH')})
        self.assertEqual(result['params']['scope'], 'E')


if __name__ == '__main__':
    unittest.main()

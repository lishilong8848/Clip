"""Business guides are discoverable references, never executable authority."""
from copy import deepcopy
import ast
from pathlib import Path
import re
import sys
import unittest
from unittest.mock import patch
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service.assistant import lighthouse_api as api
from openclaw_service.assistant import lighthouse_skills as skills


class ModuleGuideTests(unittest.TestCase):
    def test_every_registered_business_group_has_a_packaged_guide(self):
        groups = {label for _, label in api._GROUP_BY_PREFIX}
        self.assertLessEqual(groups, set(skills.MODULE_GUIDES))
        names = {item['name'] for item in skills.catalog()}
        self.assertLessEqual(set(skills.MODULE_GUIDES.values()), names)
        for name in set(skills.MODULE_GUIDES.values()):
            with self.subTest(skill=name):
                output = skills.read(name)
                self.assertEqual(output['kind'], 'workflow_guide')
                self.assertEqual(output['execution_status'], 'guide_only')
                self.assertIn(f'name: {name}', output['content'])
                self.assertIn('description:', output['content'])
                self.assertIn('不是业务数据', output['note'])
                self.assertEqual(output['next_offset'], 0)

    def test_discovery_recommends_only_relevant_guides_and_preserves_schema(self):
        found = {'items': [{'id': 'GET /api/repair-management/projects',
            'group': '维修单与跟进', 'read_only': True, 'schema': {'properties': {'scope': {'type': 'string'}}}}],
            'groups': [{'name': '维修单与跟进'}, {'name': '机柜上下电'}]}
        original = deepcopy(found)
        result = skills.annotate_discovery(found)
        self.assertEqual(result['items'][0]['skill'], 'lighthouse-repairs')
        self.assertEqual(result['groups'][1]['skill'], 'lighthouse-cabinet-power')
        self.assertEqual(result['recommended_skills'], ['lighthouse-business', 'lighthouse-repairs'])
        self.assertNotIn('lighthouse-cabinet-power', result['recommended_skills'])
        descriptor = {key: value for key, value in result['items'][0].items() if key != 'skill'}
        self.assertEqual(descriptor, original['items'][0])
        self.assertTrue(all(item['execution_status'] == 'guide_only' for item in result['skills']))

    def test_unknown_or_missing_guide_never_creates_authority_or_arbitrary_path(self):
        found = {'items': [{'id': 'GET /api/new-feature', 'group': '新增功能', 'read_only': True}],
            'groups': [{'name': '新增功能'}]}
        result = skills.annotate_discovery(found)
        self.assertEqual(result['items'][0]['skill'], 'lighthouse-business')
        self.assertEqual(result['recommended_skills'], ['lighthouse-business'])
        with patch.object(skills, 'catalog', return_value=[]):
            absent = skills.annotate_discovery({'items': [{'group': '../../outside'}]})
        self.assertNotIn('skill', absent['items'][0])
        self.assertEqual(absent['recommended_skills'], [])

    def test_guides_never_contain_automatic_executable_resources(self):
        for name in set(skills.MODULE_GUIDES.values()):
            root = skills.ROOT / name
            self.assertFalse((root / 'scripts').exists())
            self.assertEqual({path.name for path in root.iterdir() if path.is_file()}, {'SKILL.md'})

    def test_documented_api_examples_exist_with_the_declared_method(self):
        root = Path(__file__).resolve().parent
        sources = [root / 'clipflow_backend/main.py', Path(api.__file__)]
        sources.extend((root / 'lan_bitable_template_portal').glob('*routes.py'))
        known = set()
        verbs = {'GET', 'POST', 'PUT', 'PATCH', 'DELETE'}
        for source in sources:
            for node in ast.walk(ast.parse(source.read_text(encoding='utf-8-sig'))):
                if isinstance(node, ast.Constant) and isinstance(node.value, str):
                    method, _, path = node.value.partition(' ')
                    if method in verbs and path.startswith('/api/'):
                        known.add((method, path))
                if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute) or not node.args:
                    continue
                if not isinstance(node.args[0], ast.Constant) or not isinstance(node.args[0].value, str):
                    continue
                path = node.args[0].value
                if not path.startswith('/api/'):
                    continue
                if node.func.attr.upper() in verbs:
                    known.add((node.func.attr.upper(), path))
                elif node.func.attr in {'api_route', 'add_api_route'}:
                    methods = next((value.value for value in node.keywords if value.arg == 'methods'), None)
                    if isinstance(methods, (ast.List, ast.Tuple)):
                        for value in methods.elts:
                            if isinstance(value, ast.Constant):
                                known.add((str(value.value).upper(), path))
        # Several modules register routes from tables rather than decorators.
        # Install only their route definitions, without startup or business I/O.
        from fastapi import FastAPI
        from lan_bitable_template_portal import cabinet_power_routes, lighthouse_routes, plan_convergence_routes
        app = FastAPI()
        controller = SimpleNamespace()
        runtime = SimpleNamespace(service=SimpleNamespace(), state_store=object())
        with patch.object(cabinet_power_routes, 'CabinetPowerService', return_value=SimpleNamespace()):
            cabinet_power_routes.install_cabinet_power_routes(app, controller, runtime)
        lighthouse_routes.install_lighthouse_routes(app, controller, runtime)
        plan_convergence_routes.install_plan_convergence_routes(app, controller, runtime)
        self.assertTrue(callable(runtime.service._notice_plan_guard))
        for route in app.routes:
            for method in getattr(route, 'methods', ()):
                known.add((method, route.path))
        pattern = r'\b((?:GET|POST|PUT|PATCH|DELETE)(?:/(?:GET|POST|PUT|PATCH|DELETE))*) (/api/[a-zA-Z0-9_/{}/.-]+)'
        for name in set(skills.MODULE_GUIDES.values()):
            text = skills.read(name)['content']
            for methods, path in re.findall(pattern, text):
                for method in methods.split('/'):
                    with self.subTest(skill=name, api=f'{method} {path}'):
                        self.assertTrue((method, path) in known, f'Undeclared API in {name}: {method} {path}')


if __name__ == '__main__':
    unittest.main()

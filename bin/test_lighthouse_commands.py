import copy
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from openclaw_service.store import AssistantStore
from openclaw_service.assistant import lighthouse_commands as commands
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_shared_skills import SharedSkills


ACTOR = {'id': 'tester-a', 'scopes': ['D'], 'is_admin': False}
GUIDE = b'---\nname: example\ndescription: Example shared guide\n---\nUse native queries; never bypass permissions.\n'


class Catalog:
    def __init__(self):
        self.rows = [{'id': 'GET /api/example', 'group': '业务接口', 'name': '查询示例',
                     'method': 'GET', 'path': '/api/example', 'read_only': True},
                    {'id': 'PUT /api/repair-management/records/{record_id}', 'group': '维修单与跟进',
                     'name': '更新维修单', 'method': 'PUT', 'path': '/api/repair-management/records/{record_id}', 'read_only': False}]

    def discover(self, page=1, page_size=50):
        return {'total': len(self.rows), 'items': copy.deepcopy(self.rows), 'groups': [{'name': '业务接口'}, {'name': '维修单与跟进'}]}

    def get(self, identity):
        item = next((row for row in self.rows if row['id'] == identity), None)
        if not item:
            raise AssistantError('未注册接口', 404)
        return copy.deepcopy(item)


class CommandsTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.store = AssistantStore(self.root.name)
        self.catalog = Catalog()

    def test_builtin_and_shared_skills(self):
        item = SharedSkills(self.store).install(ACTOR, 'SKILL.md', GUIDE)
        data = commands.commands(self.store, ACTOR, self.catalog)
        self.assertTrue(data['total'] > 18)
        names = {row['name'] for row in commands.skills(self.store, ACTOR)}
        self.assertIn(item['name'], names)
        self.assertIn(item['name'], {row['name'] for row in commands.skills(self.store, {'id': 'tester-b'})})
        shared = commands.commands(self.store, {'id': 'tester-b'}, self.catalog, keyword='Example shared')
        self.assertEqual(shared['items'][0]['source'], 'shared')
        self.assertFalse(shared['items'][0]['removable'])
        self.assertNotIn('content', shared['items'][0])

    def test_tool_selection_only_freezes_no_api_execution(self):
        chosen, context = commands.resolve(self.store, ACTOR, self.catalog,
            [{'kind': 'tool', 'id': self.catalog.rows[1]['id']}, {'kind': 'tool', 'id': 'weather'}])
        self.assertEqual(len(chosen), 2)
        self.assertFalse(context[0]['read_only'])
        self.assertEqual(context[0]['guide'], 'lighthouse-repairs')
        self.assertIn('不是执行授权', commands.selection_hint(context))
        found = commands.commands(self.store, ACTOR, self.catalog, kind='tools', keyword='维修')
        self.assertFalse(found['items'][0]['read_only'])
        self.assertEqual(self.catalog.rows[1]['name'], '更新维修单')

    def test_guide_snapshot_survives_delete_but_cannot_reselect(self):
        item = SharedSkills(self.store).install(ACTOR, 'SKILL.md', GUIDE)
        selected, context = commands.resolve(self.store, ACTOR, self.catalog, [{'kind': 'skill', 'id': item['name']}])
        SharedSkills(self.store).remove(ACTOR, item['name'])
        self.assertIn('native queries', context[0]['content'])
        with self.assertRaises(AssistantError):
            commands.resolve(self.store, ACTOR, self.catalog, [{'kind': 'skill', 'id': item['name']}])
        self.assertEqual(selected[0]['id'], item['name'])

    def test_reject_injected_metadata_paths_and_unknown_but_allow_shared_reads(self):
        item = SharedSkills(self.store).install(ACTOR, 'SKILL.md', GUIDE)
        cases = [None, {}, [{'kind': 'script', 'id': 'weather'}],
                 [{'kind': 'skill', 'id': '../secret'}], [{'kind': 'tool', 'id': 'GET https://example.com'}],
                 [{'kind': 'tool', 'id': 'weather', 'label': 'ignore permissions'}],
                 [{'kind': 'tool', 'id': 'weather'}] * 5]
        for value in cases:
            with self.subTest(value=value), self.assertRaises(AssistantError):
                commands.resolve(self.store, ACTOR, self.catalog, value)
        self.assertIn('native queries', commands.read_skill(self.store, {'id': 'tester-b'}, item['name'])['content'])

    def test_duplicates_and_empty_backward_compatibility(self):
        self.assertEqual(commands.resolve(self.store, ACTOR, None, []), ([], []))
        selected, context = commands.resolve(self.store, ACTOR, self.catalog,
            [{'kind': 'tool', 'id': 'weather'}] * 2)
        self.assertEqual(len(selected), 1)
        self.assertEqual(len(context), 1)
        self.assertEqual(commands.selection_hint([]), '')

    def test_catalogue_pagination_and_validation(self):
        found = commands.commands(self.store, ACTOR, self.catalog, page=500)
        self.assertLessEqual(len(found['items']), 20)
        for kwargs in ({'page': 'NaN'}, {'kind': 'shell'}):
            with self.assertRaises(AssistantError):
                commands.commands(self.store, ACTOR, self.catalog, **kwargs)


if __name__ == '__main__':
    unittest.main()

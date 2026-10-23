"""Read-only packaging contract: helpers ship, runtime data does not."""
import ast
import unittest
from pathlib import Path

from bin.test_mcp_packaging import package, ROOT


class QualityPackaging(unittest.TestCase):
    def test_quality_regressions_are_part_of_the_default_preflight(self):
        tree = ast.parse((ROOT / 'package_portable.py').read_text(encoding='utf-8-sig'))
        preflight = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                         and node.name == '_run_packaging_preflight_tests')
        strings = {node.value for node in ast.walk(preflight) if isinstance(node, ast.Constant)
                   and isinstance(node.value, str)}
        for name in ('knowledge_repair', 'knowledge_evidence', 'claim_review', 'answer_quality_replay', 'quality_packaging',
                     'reviewer_reread', 'mixed_query', 'query_intent', 'task_model_quality', 'mixed_native_guard',
                     'live_evaluation', 'quality_integration'):
            self.assertIn('bin.test_lighthouse_' + name, strings)
            self.assertTrue((ROOT / 'bin' / ('test_lighthouse_' + name + '.py')).is_file())

    def test_runtime_helpers_not_excluded_and_user_state_remains_excluded(self):
        self.assertIn('openclaw_service.assistant.lighthouse_knowledge_evidence', package.SMOKE_IMPORT_MODULES)
        for name in ('lighthouse_knowledge_answer.py', 'lighthouse_knowledge_evidence.py', 'lighthouse_review.py',
                     'lighthouse_query_intent.py', 'lighthouse_model_health.py'):
            path = ROOT / 'bin/openclaw_service/assistant' / name
            self.assertTrue(path.is_file())
            self.assertFalse(package._is_excluded(path, root=ROOT, exclude_venv=True,
                                                  relative_path=path.relative_to(ROOT)))
        package._assert_project_iterator_excludes_runtime_data()


if __name__ == '__main__':
    unittest.main()

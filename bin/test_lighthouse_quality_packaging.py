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
                     'live_evaluation', 'quality_integration', 'engine_evaluation', 'learning_participation',
                     'source_contracts', 'partial_claims'):
            self.assertIn('bin.test_lighthouse_' + name, strings)
            self.assertTrue((ROOT / 'bin' / ('test_lighthouse_' + name + '.py')).is_file())
        for name in ('assistant_diagnostics', 'assistant_diagnostic_store', 'assistant_diagnostics_cloud', 'lighthouse_tool_readiness'):
            self.assertIn('bin.test_' + name, strings)
            self.assertTrue((ROOT / 'bin' / ('test_' + name + '.py')).is_file())

    def test_runtime_helpers_not_excluded_and_user_state_remains_excluded(self):
        self.assertIn('openclaw_service.assistant.lighthouse_knowledge_evidence', package.SMOKE_IMPORT_MODULES)
        for name in ('lighthouse_knowledge_answer.py', 'lighthouse_knowledge_evidence.py', 'lighthouse_review.py',
                     'lighthouse_query_intent.py', 'lighthouse_model_health.py', 'lighthouse_sources.py',
                     'lighthouse_artifacts.py', 'lighthouse_queries.py', 'lighthouse_model.py'):
            path = ROOT / 'bin/openclaw_service/assistant' / name
            self.assertTrue(path.is_file())
            self.assertFalse(package._is_excluded(path, root=ROOT, exclude_venv=True,
                                                  relative_path=path.relative_to(ROOT)))
        package._assert_project_iterator_excludes_runtime_data()
        for module in ('openclaw_service.diagnostics', 'openclaw_service.diagnostic_store',
                       'lan_bitable_template_portal.assistant_diagnostics_cloud'):
            self.assertIn(module, package.SMOKE_IMPORT_MODULES)
            path = Path('bin') / (module.replace('.', '/') + '.py')
            self.assertIn(path, package.FORCE_PATCH_INCLUDE_FILES)
        for name in ('diagnostics.sqlite3', 'diagnostics.sqlite3-wal', 'diagnostics.sqlite3-shm'):
            path = Path('bin/data/lighthouse_openclaw') / name
            self.assertTrue(package._is_excluded(ROOT / path, root=ROOT, exclude_venv=True, relative_path=path))

    def test_citation_parser_is_checked_and_available_after_update(self):
        from upload_event_module.services.dependency_bootstrap import DEFAULT_MODULE_TO_PACKAGE
        self.assertEqual(DEFAULT_MODULE_TO_PACKAGE['markdown_it'], 'markdown-it-py')
        self.assertEqual(package.RUNTIME_MODULE_TO_PACKAGE['markdown_it'], 'markdown-it-py')
        for modules in (package.SMOKE_IMPORT_MODULES, package.RUNTIME_PACKAGE_INSTALL_ORDER,
                        package.PACKAGING_PREFLIGHT_MODULES):
            self.assertIn('markdown_it', modules)


if __name__ == '__main__':
    unittest.main()

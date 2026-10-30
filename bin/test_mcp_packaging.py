"""Package coverage and UI freshness; never builds or publishes a real patch."""
import importlib.util
import sys
import tempfile
import subprocess
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'bin'))
spec = importlib.util.spec_from_file_location('mcp_packaging_fixture', ROOT / 'package_portable.py')
package = importlib.util.module_from_spec(spec)
spec.loader.exec_module(package)


class PackagingTests(unittest.TestCase):
    def test_notice_navigation_browser_checks_are_mandatory_and_failure_stops(self):
        expected = ['check-test-server-ports.mjs', 'check-workspace-palettes.mjs', 'check-workspace-navigation.mjs', 'check-login-version-switch.mjs', 'check-legacy-module-entry.mjs',
                    'check-notice-photo-paste.mjs', 'check-legacy-notice-cycle.mjs',
                    'check-notice-cycle-warnings.mjs', 'check-account-fixed-duty.mjs',
                    'check-assistant-shared-query.mjs', 'check-repair-overview.mjs',
                    'check-document-library-all-scopes.mjs', 'check-document-library-resilience.mjs',
                    'check-learning-sharing-types.mjs', 'check-learning-sharing-resilience.mjs',
                    'check-learning-question-import.mjs', 'check-learning-daily-entry.mjs', 'check-pre-shift-edits.mjs',
                    'check-repair-plan-binding.mjs', 'check-notice-convergence-confirmation.mjs']
        frontend = ROOT / 'bin/lan_bitable_template_portal/frontend'
        with patch.object(package, 'PREFLIGHT_WORKERS', 2), \
                patch.object(package.shutil, 'which', return_value='fixture-node'), \
                patch.object(package, '_run_preflight_check') as run:
            package._run_frontend_notice_navigation_checks()
        self.assertCountEqual([Path(call.args[0][1]).name for call in run.call_args_list], expected)
        for call in run.call_args_list:
            self.assertEqual(call.args[0][0], 'fixture-node')
            self.assertEqual(call.kwargs, {'cwd': frontend, 'check': True, 'timeout': 300})
        with patch.object(package.shutil, 'which', return_value=None), \
                patch.object(package, '_run_preflight_check') as run:
            with self.assertRaisesRegex(RuntimeError, 'Node.js'):
                package._run_frontend_notice_navigation_checks()
            run.assert_not_called()
        for name in expected:
            with self.subTest(missing=name), \
                    patch.object(Path, 'is_file', autospec=True, side_effect=lambda path, missing=name: path.name != missing), \
                    patch.object(package.shutil, 'which', return_value='fixture-node'), \
                    patch.object(package, '_run_preflight_check') as run:
                with self.assertRaisesRegex(RuntimeError, name):
                    package._run_frontend_notice_navigation_checks()
                run.assert_not_called()
            def fail_named(args, **_kwargs):
                if Path(args[1]).name == name:
                    raise subprocess.CalledProcessError(1, name)
            with self.subTest(failed=name), \
                    patch.object(package, 'PREFLIGHT_WORKERS', 2), \
                    patch.object(package.shutil, 'which', return_value='fixture-node'), \
                    patch.object(package, '_run_preflight_check', side_effect=fail_named) as run:
                with self.assertRaises(subprocess.CalledProcessError):
                    package._run_frontend_notice_navigation_checks()
                parallel = {'check-legacy-module-entry.mjs', 'check-pre-shift-edits.mjs'}
                serial = [item for item in expected if item not in parallel]
                reached = parallel if name in parallel else parallel | set(serial[:serial.index(name) + 1])
                self.assertCountEqual([Path(call.args[0][1]).name for call in run.call_args_list], reached)

    def test_document_extras_and_knowledge_compatible_versions_are_checked(self):
        from bin.upload_event_module.services import dependency_bootstrap as dependencies
        with patch.object(dependencies.importlib.util, 'find_spec', side_effect=lambda name: None if name == 'dateutil' else object()), \
                patch.object(dependencies.importlib.metadata, 'version', return_value='0.1.8'):
            self.assertFalse(dependencies._has_module('markitdown'))
        with patch.object(dependencies.importlib.util, 'find_spec', return_value=object()), \
                patch.object(dependencies.importlib.metadata, 'version', return_value='1.20.1'):
            self.assertFalse(dependencies._has_module('onnxruntime'))

    def test_sdk_and_all_local_server_modules_are_in_update_contract(self):
        policy = Path('bin/lan_bitable_template_portal/assistant_shared_read.py')
        self.assertIn(policy, package.FORCE_PATCH_INCLUDE_FILES)
        self.assertFalse(package._is_development_only_path(ROOT / policy, ROOT))
        self.assertEqual(package.RUNTIME_MODULE_TO_PACKAGE['mcp'], 'mcp==1.30.0')
        self.assertIn('mcp', package.RUNTIME_PACKAGE_INSTALL_ORDER)
        for name in ('lighthouse_extensions.py', 'lighthouse_mcp_builtin.py', 'lighthouse_mcp_office.py', 'lighthouse_mcp_public.py'):
            self.assertIn(Path('bin/openclaw_service/assistant') / name, package.FORCE_PATCH_INCLUDE_FILES)
        from bin.upload_event_module.services.dependency_bootstrap import DEFAULT_MODULE_TO_PACKAGE
        self.assertEqual(DEFAULT_MODULE_TO_PACKAGE['mcp'], package.RUNTIME_MODULE_TO_PACKAGE['mcp'])
        self.assertIn(Path('bin/openclaw_service/assistant/lighthouse_document_tools.py'), package.FORCE_PATCH_INCLUDE_FILES)
        for name in ('markitdown', 'magika', 'rapidocr', 'onnxruntime', 'pypdfium2', 'playwright'):
            self.assertEqual(DEFAULT_MODULE_TO_PACKAGE[name], package.RUNTIME_MODULE_TO_PACKAGE[name])
            self.assertIn(name, package.RUNTIME_PACKAGE_INSTALL_ORDER)
        self.assertEqual(package.RUNTIME_MODULE_TO_PACKAGE['magika'], 'magika==0.6.2')
        self.assertEqual(package.RUNTIME_MODULE_TO_PACKAGE['onnxruntime'], 'onnxruntime==1.23.2')

    def test_flowchart_vendor_and_office_modules_ship_with_updates(self):
        import hashlib
        from bin.openclaw_service.assistant.lighthouse_diagrams import MERMAID_ASSET, MERMAID_SHA256
        self.assertEqual(hashlib.sha256(MERMAID_ASSET.read_bytes()).hexdigest(), MERMAID_SHA256)
        for path in ('bin/resources/assistant_tools/mermaid.min.js', 'bin/resources/assistant_tools/MERMAID-LICENSE.txt',
                     'bin/openclaw_service/assistant/lighthouse_office_tools.py', 'bin/openclaw_service/assistant/lighthouse_diagrams.py'):
            self.assertIn(Path(path), package.FORCE_PATCH_INCLUDE_FILES)

    def test_missing_tool_assets_build_once_but_current_assets_skip(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            frontend = root / 'bin/lan_bitable_template_portal/frontend'
            (frontend / 'src/components').mkdir(parents=True)
            (frontend / 'src/components/LighthouseTools.vue').touch()
            (frontend / 'src/components/LighthouseSkills.vue').touch()
            build = Mock()
            with patch.object(package, 'PROJECT_ROOT', root), patch.object(package.shutil, 'which', return_value='fixture-npm'), patch.object(package.subprocess, 'run', build):
                package._ensure_mcp_frontend_assets()
            build.assert_called_once()
            (frontend / 'dist/assets').mkdir(parents=True)
            (frontend / 'dist/assets/tools.js').write_text('/api/assistant/extensions', encoding='utf-8')
            (frontend / 'dist/assistant.html').touch()
            (frontend / 'dist/index.html').touch()
            asset = Path('bin/lan_bitable_template_portal/frontend/dist/assets/tools.js')
            with patch.object(package, 'PROJECT_ROOT', root), patch.object(package, 'referenced_assets', return_value={asset}), patch.object(package.subprocess, 'run', side_effect=AssertionError('fresh assets must not rebuild')):
                package._ensure_mcp_frontend_assets()

    def test_checklist_changes_or_stale_workspace_entry_rebuild_both_frontends(self):
        import os
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);frontend=root/'bin/lan_bitable_template_portal/frontend'
            inputs=['src/components/LighthouseTools.vue','src/components/LighthouseSkills.vue',
                    'src/workspace-v2/dashboard/PreShiftCheck.vue','src/preShiftUtils.ts',
                    'scripts/workspace-build-adapter.mjs','scripts/workspace-night-theme.mjs',
                    'scripts/prune-dist-assets.mjs']
            entries=['dist/assistant.html','dist/index.html','dist/assets/workspace-v2/index.html']
            for relative in inputs+entries+['dist/assets/tools.js']:
                path=frontend/relative;path.parent.mkdir(parents=True,exist_ok=True)
                path.write_text('/api/assistant/extensions',encoding='utf-8')
                os.utime(path,(100,100) if relative in entries else (50,50))
            asset=Path('bin/lan_bitable_template_portal/frontend/dist/assets/tools.js')
            with patch.object(package,'PROJECT_ROOT',root),patch.object(package,'referenced_assets',return_value={asset}),patch.object(
                package.shutil,'which',return_value='fixture-npm'),patch.object(package.subprocess,'run') as build:
                package._ensure_mcp_frontend_assets();build.assert_not_called()
                for relative in entries:
                    os.utime(frontend/relative,(40,40));package._ensure_mcp_frontend_assets()
                    build.assert_called_once();build.reset_mock();os.utime(frontend/relative,(100,100))
                for relative in inputs[2:]:
                    os.utime(frontend/relative,(110,110));package._ensure_mcp_frontend_assets()
                    build.assert_called_once()
                    self.assertEqual(build.call_args.args[0], ['fixture-npm', 'run', 'build'])
                    build.reset_mock();os.utime(frontend/relative,(50,50))
                (frontend/'scripts/check-navigation-back.mjs').touch()
                package._ensure_mcp_frontend_assets();build.assert_not_called()


if __name__ == '__main__':
    unittest.main()

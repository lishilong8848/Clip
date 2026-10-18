"""Package coverage and UI freshness; never builds or publishes a real patch."""
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'bin'))
spec = importlib.util.spec_from_file_location('mcp_packaging_fixture', ROOT / 'package_portable.py')
package = importlib.util.module_from_spec(spec)
spec.loader.exec_module(package)


class PackagingTests(unittest.TestCase):
    def test_document_extras_and_knowledge_compatible_versions_are_checked(self):
        from bin.upload_event_module.services import dependency_bootstrap as dependencies
        with patch.object(dependencies.importlib.util, 'find_spec', side_effect=lambda name: None if name == 'dateutil' else object()), \
                patch.object(dependencies.importlib.metadata, 'version', return_value='0.1.8'):
            self.assertFalse(dependencies._has_module('markitdown'))
        with patch.object(dependencies.importlib.util, 'find_spec', return_value=object()), \
                patch.object(dependencies.importlib.metadata, 'version', return_value='1.20.1'):
            self.assertFalse(dependencies._has_module('onnxruntime'))

    def test_sdk_and_all_local_server_modules_are_in_update_contract(self):
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
                    'src/workspace-v2/dashboard/PreShiftCheck.vue','src/preShiftUtils.ts']
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
                os.utime(frontend/inputs[2],(110,110));package._ensure_mcp_frontend_assets();build.assert_called_once()
                build.reset_mock();os.utime(frontend/inputs[2],(50,50))
                os.utime(frontend/inputs[3],(120,120));package._ensure_mcp_frontend_assets();build.assert_called_once()


if __name__ == '__main__':
    unittest.main()

# -*- coding: utf-8 -*-
from __future__ import annotations

import unittest
import io
import os
import sys
from contextlib import redirect_stdout
import tempfile
import subprocess
import threading
from pathlib import Path
from unittest.mock import patch

from bin.tools import release_readiness_check as readiness
import package_portable


class ReleaseReadinessTests(unittest.TestCase):
    def test_patch_includes_model_manifest_but_not_model_cache_or_bundle(self):
        from bin.openclaw_service.assistant import lighthouse_distribution as distribution
        from bin.openclaw_service.assistant.lighthouse_knowledge_model import SPEC
        import json
        distribution.validate_spec(json.loads(SPEC.read_text(encoding='utf-8')), component='knowledge_model')
        files = {path.relative_to(package_portable.PROJECT_ROOT).as_posix()
                 for path in package_portable._iter_project_files(package_portable.PROJECT_ROOT, exclude_venv=True)}
        self.assertIn('bin/openclaw_service/assistant/openclaw/knowledge-model.json', files)
        self.assertFalse(any(path.endswith('.onnx') for path in files))
        self.assertTrue(package_portable._is_runtime_data_path(
            package_portable.PROJECT_ROOT / 'bin/resources/knowledge_model/bge-small-zh-v1.5/model_optimized.onnx'))

    def test_planned_match_suite_runs_without_prior_test_imports(self):
        env = {key: value for key, value in os.environ.items() if key != 'PYTHONPATH'}
        env['PYTHONNOUSERSITE'] = '1'
        with tempfile.TemporaryDirectory() as directory:
            env['CLIPFLOW_DATA_DIR'] = directory
            result = subprocess.run(
                [sys.executable, '-s', '-m', 'unittest', 'bin.test_lighthouse_planned_match'],
                cwd=package_portable.PROJECT_ROOT, env=env, capture_output=True,
                text=True, encoding='utf-8', errors='replace', timeout=30,
            )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_production_frontend_uses_shared_api_client(self):
        ok, offenders = readiness.check_frontend_api_client()
        self.assertTrue(ok, '\n'.join(offenders))

    def test_admin_settings_guard_checks_full_page_not_old_style_comment(self):
        ok, errors = readiness.check_frontend_vnet_skin()
        self.assertTrue(ok, '\n'.join(errors))
        original = Path.read_text
        def reverted_modal(path, *args, **kwargs):
            value = original(path, *args, **kwargs)
            return value.replace('.admin-shell {', '.admin-shell { position: fixed;') if path.name == 'AdminTools.vue' else value
        with patch.object(Path, 'read_text', reverted_modal):
            ok, errors = readiness.check_frontend_vnet_skin()
        self.assertFalse(ok)
        self.assertTrue(any('不能恢复为浮层' in error for error in errors))

    def test_admin_capability_gate_is_required(self):
        ok, errors = readiness.check_frontend_vnet_skin()
        self.assertTrue(ok, '\n'.join(errors))
        original = Path.read_text

        def replaced_gate(path, *args, **kwargs):
            value = original(path, *args, **kwargs)
            if path.name != 'App.vue':
                return value
            # Replace the capability gate with the obsolete literal old guard.
            value = value.replace(
                'v-else-if="isAdminSettingsPage && canManageAccounts"',
                'v-else-if="isAdminSettingsPage && isAdmin"',
            )
            return value

        with patch.object(Path, 'read_text', replaced_gate):
            ok, errors = readiness.check_frontend_vnet_skin()
        self.assertFalse(ok)
        self.assertTrue(
            any('canManageAccounts 门禁' in error for error in errors),
            '\n'.join(errors),
        )

    def test_admin_system_admin_binding_is_required(self):
        ok, errors = readiness.check_frontend_vnet_skin()
        self.assertTrue(ok, '\n'.join(errors))
        original = Path.read_text

        def removed_binding(path, *args, **kwargs):
            value = original(path, *args, **kwargs)
            if path.name != 'App.vue':
                return value
            # Drop the system-admin binding so L2/L3 accounts could be treated as admins.
            return value.replace(':system-admin="isAdmin"', ':system-admin="false"')

        with patch.object(Path, 'read_text', removed_binding):
            ok, errors = readiness.check_frontend_vnet_skin()
        self.assertFalse(ok)
        self.assertTrue(
            any('system-admin 绑定' in error for error in errors),
            '\n'.join(errors),
        )

    def test_admin_non_admin_early_return_is_required(self):
        ok, errors = readiness.check_frontend_vnet_skin()
        self.assertTrue(ok, '\n'.join(errors))
        original = Path.read_text

        def removed_early_return(path, *args, **kwargs):
            value = original(path, *args, **kwargs)
            if path.name != 'AdminTools.vue':
                return value
            # Remove the gate that stops non-system admins from issuing global requests.
            return value.replace(
                "if (props.systemAdmin === false) { tab.value = 'permissions'; return; }",
                "if (props.systemAdmin === false) { tab.value = 'permissions'; }",
            )

        with patch.object(Path, 'read_text', removed_early_return):
            ok, errors = readiness.check_frontend_vnet_skin()
        self.assertFalse(ok)
        self.assertTrue(
            any('非系统管理员提前返回' in error for error in errors),
            '\n'.join(errors),
        )

    def test_admin_accounts_functional_branch_is_required(self):
        ok, errors = readiness.check_frontend_vnet_skin()
        self.assertTrue(ok, '\n'.join(errors))
        original = Path.read_text

        def removed_accounts_branch(path, *args, **kwargs):
            value = original(path, *args, **kwargs)
            if path.name != 'AdminTools.vue':
                return value
            # Drop the functional v-if=!systemAdmin gate so non-admins lose the accounts UI.
            return value.replace(
                '<AdminAccounts v-if="!systemAdmin"',
                '<AdminAccounts v-if="true"',
            )

        with patch.object(Path, 'read_text', removed_accounts_branch):
            ok, errors = readiness.check_frontend_vnet_skin()
        self.assertFalse(ok)
        self.assertTrue(
            any('AdminAccounts 分支' in error for error in errors),
            '\n'.join(errors),
        )

    def test_admin_workspace_chrome_gate_is_required(self):
        ok, errors = readiness.check_frontend_vnet_skin()
        self.assertTrue(ok, '\n'.join(errors))
        original = Path.read_text

        def removed_chrome_gate(path, *args, **kwargs):
            value = original(path, *args, **kwargs)
            if path.name != 'AdminTools.vue':
                return value
            # Remove the v-if=systemAdmin gate so non-admins could reach the admin nav.
            return value.replace(
                '      <AdminWorkspaceChrome\n        v-if="systemAdmin"\n',
                '      <AdminWorkspaceChrome\n',
            )

        with patch.object(Path, 'read_text', removed_chrome_gate):
            ok, errors = readiness.check_frontend_vnet_skin()
        self.assertFalse(ok)
        self.assertTrue(
            any('AdminWorkspaceChrome 的 systemAdmin 门禁' in error for error in errors),
            '\n'.join(errors),
        )

    def test_preflight_shards_run_concurrently_with_isolated_data_and_all_modules(self):
        barrier = threading.Barrier(3, timeout=5)
        calls = []
        modules = [f'bin.test_fixture_{index}' for index in range(17)]

        def run(args, **kwargs):
            directory = Path(kwargs['env']['CLIPFLOW_DATA_DIR'])
            self.assertTrue(directory.is_dir())
            self.assertEqual(kwargs['env']['PYTHONIOENCODING'], 'utf-8')
            self.assertEqual(kwargs['env']['PYTHONNOUSERSITE'], '1')
            self.assertTrue(kwargs['check'])
            self.assertEqual(kwargs['timeout'], 900)
            calls.append((args[3:], directory))
            barrier.wait()
            kwargs['stdout'].write('isolated test output\n')

        with patch.object(package_portable, 'PREFLIGHT_WORKERS', 3), \
                patch.object(package_portable.subprocess, 'run', side_effect=run), \
                patch.object(package_portable, 'log'):
            package_portable._run_preflight_check(['python', '-m', 'unittest', *modules], check=True)
        self.assertEqual(sorted(name for names, _ in calls for name in names), sorted(modules))
        self.assertEqual(len({directory for _, directory in calls}), 3)
        self.assertTrue(all(not directory.exists() for _, directory in calls))

    def test_preflight_shard_failure_stops_packaging(self):
        with patch.object(package_portable.subprocess, 'run', side_effect=subprocess.CalledProcessError(1, 'fixture')), \
                patch.object(package_portable, 'log'):
            with self.assertRaises(subprocess.CalledProcessError):
                package_portable._run_preflight_check(['python', '-m', 'unittest', 'bin.test_fixture'], check=True)

    def test_failed_test_details_are_repeated_after_other_shards_finish(self):
        failed = threading.Event()
        output = io.StringIO()
        detail = '=' * 70 + '\nFAIL: test_formal_account (fixture.Tests)\nAssertionError: 200 != 403\n'

        def run(args, **kwargs):
            if args[-1] == 'bin.test_bad':
                kwargs['stdout'].write(detail)
                failed.set()
                raise subprocess.CalledProcessError(1, args)
            self.assertTrue(failed.wait(5))
            kwargs['stdout'].write('Other test group: OK\n')

        with patch.object(package_portable, 'PREFLIGHT_WORKERS', 2), \
                patch.object(package_portable.subprocess, 'run', side_effect=run), redirect_stdout(output):
            with self.assertRaises(subprocess.CalledProcessError):
                package_portable._run_preflight_check(['python', '-m', 'unittest', 'bin.test_good', 'bin.test_bad'], check=True)
        text = output.getvalue()
        self.assertGreater(text.rfind('FAIL: test_formal_account'), text.index('Other test group: OK'))
        self.assertIn('已停止打包', text)

    def test_preflight_timeout_reports_details_and_still_stops(self):
        output = io.StringIO()
        with patch.object(package_portable.subprocess, 'run', side_effect=subprocess.TimeoutExpired('fixture', 900)), redirect_stdout(output):
            with self.assertRaises(subprocess.TimeoutExpired):
                package_portable._run_preflight_check(['python', '-m', 'unittest', 'bin.test_slow'], check=True)
        self.assertIn('bin.test_slow', output.getvalue())
        self.assertIn('已停止打包', output.getvalue())

    def test_packaging_lock_blocks_overlap_and_releases_after_failure(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(package_portable, 'BUILD_DIR', Path(directory)):
            with self.assertRaisesRegex(ValueError, 'fixture'):
                with package_portable._packaging_lock():
                    with self.assertRaisesRegex(RuntimeError, '已有打包'):
                        with package_portable._packaging_lock():
                            self.fail('second packager acquired the lock')
                    raise ValueError('fixture')
            with package_portable._packaging_lock():
                pass

    def test_requests_imports_are_blocked_but_local_lists_are_not(self) -> None:
        cases = [
            ('requests = []\nrequests.append(1)\n', True),
            ('def query(requests):\n    requests.append(1)\n', True),
            ('# requests.get(url)\ntext = "import requests"\n', True),
            ('from requests.exceptions import Timeout, ReadTimeout, ConnectionError\n', True),
            ('import requests\nrequests.get("https://example.invalid")\n', False),
            ('import requests as network\n', False),
            ('from requests import Session as SessionFactory\n', False),
            ('from requests.sessions import Session\n', False),
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / 'business.py'
            with patch.object(readiness, 'PROJECT_ROOT', root), patch.object(readiness, '_git_tracked_files', return_value=['business.py']):
                for source, allowed in cases:
                    with self.subTest(source=source):
                        target.write_text(source, encoding='utf-8')
                        ok, offenders = readiness.check_requests_usage()
                        self.assertEqual(ok, allowed)
                        self.assertEqual(offenders, [] if allowed else ['business.py'])

    def test_preflight_uses_one_interpreter_for_dependency_checks_and_tests(self) -> None:
        python = readiness.BIN_DIR / '.venv/Scripts/python.exe'
        with patch.object(package_portable, '_find_build_python', return_value=python), \
                patch.object(package_portable, 'log'), \
                patch.object(package_portable, '_assert_project_iterator_excludes_runtime_data'), \
                patch.object(package_portable, '_cleanup_vue_dist_assets'), \
                patch.object(package_portable, '_ensure_packaging_preflight_dependencies') as ensure, \
                patch.object(package_portable, '_verify_runtime_imports', return_value=True) as imports, \
                patch.object(package_portable.subprocess, 'run') as run:
            package_portable._run_packaging_preflight_tests()
        ensure.assert_called_once_with(python)
        imports.assert_called_once_with(python, package_portable.PROJECT_ROOT)
        self.assertGreater(len(run.call_args_list), 5)
        self.assertTrue(all(call.args[0][0] == str(python) for call in run.call_args_list))
        self.assertTrue(all(call.kwargs['env']['PYTHONNOUSERSITE'] == '1' for call in run.call_args_list))
        compile_call = run.call_args_list[0]
        self.assertEqual(compile_call.args[0][1], '-c')
        self.assertIn('cfile=', compile_call.args[0][2])
        self.assertNotIn('PYTHONPYCACHEPREFIX', compile_call.kwargs.get('env', {}))

    def test_build_uses_project_python_even_when_launched_from_system_python(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project = root / 'bin/.venv/Scripts/python.exe'
            system = root / 'system/python.exe'
            for executable in (project, system):
                executable.parent.mkdir(parents=True, exist_ok=True)
                executable.touch()
            with patch.dict(package_portable.os.environ, {'PACKAGE_BUILD_PYTHON': ''}), \
                    patch.object(package_portable, 'PROJECT_ROOT', root), \
                    patch.object(package_portable, 'PREFERRED_BUILD_PYTHON', system), \
                    patch.object(package_portable.sys, 'executable', str(system)):
                self.assertEqual(package_portable._find_build_python(), project)
                with patch.dict(package_portable.os.environ, {'PACKAGE_BUILD_PYTHON': str(system)}):
                    self.assertEqual(package_portable._find_build_python(), system)
                project.unlink()
                self.assertEqual(package_portable._find_build_python(), system)

    def test_preflight_stops_before_tests_when_runtime_imports_fail(self) -> None:
        python = readiness.BIN_DIR / '.venv/Scripts/python.exe'
        with patch.object(package_portable, '_find_build_python', return_value=python), \
                patch.object(package_portable, 'log'), \
                patch.object(package_portable, '_assert_project_iterator_excludes_runtime_data'), \
                patch.object(package_portable, '_cleanup_vue_dist_assets'), \
                patch.object(package_portable, '_ensure_packaging_preflight_dependencies'), \
                patch.object(package_portable, '_verify_runtime_imports', return_value=False), \
                patch.object(package_portable, '_run_preflight_check') as run:
            with self.assertRaisesRegex(RuntimeError, '打包运行时导入失败'):
                package_portable._run_packaging_preflight_tests()
        self.assertFalse(any(call.args[0][1:3] == ['-m', 'unittest'] for call in run.call_args_list))

    def test_packaging_filter_reuses_validated_relative_paths(self) -> None:
        root = package_portable.PROJECT_ROOT
        for relative, excluded in [('bin/example.py', False), ('bin/test_example.py', True),
                                   ('bin/data/private.db', True), ('bin/runtime/node.exe', True)]:
            with self.subTest(relative=relative):
                source = root / relative
                original = Path.resolve
                with patch.object(Path, 'resolve', autospec=True, side_effect=original) as resolve:
                    self.assertEqual(package_portable._is_excluded(source, root=root), excluded)
                    self.assertEqual(resolve.call_count, 2)
                with patch.object(Path, 'resolve', side_effect=AssertionError('already validated')):
                    self.assertEqual(package_portable._is_excluded(source, root=root, relative_path=Path(relative)), excluded)

    def test_full_package_skips_runtime_and_databases_before_copy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'source'
            output = Path(directory) / 'full'
            runtime_files = ['bin/runtime/node.exe', 'bin/runtime/cache/model.bin',
                             'bin/data/private.db', 'bin/cache.sqlite3', 'private.db',
                             'bin/subfolder/cache.sqlite3-wal']
            for relative in ['bin/refactored_main.py', *runtime_files]:
                item = root / relative
                item.parent.mkdir(parents=True, exist_ok=True)
                item.write_text('fixture', encoding='utf-8')
            original_copy = package_portable.shutil.copy2

            def copy_file(source, destination, **kwargs):
                self.assertNotIn(Path(source).relative_to(root).as_posix(), runtime_files)
                return original_copy(source, destination, **kwargs)

            with patch.object(package_portable, 'PROJECT_ROOT', root), \
                    patch.object(package_portable.shutil, 'copy2', side_effect=copy_file):
                package_portable.copy_project(output)
            self.assertTrue((output / 'bin/refactored_main.py').is_file())
            self.assertFalse((output / 'bin/runtime').exists())
            self.assertEqual(package_portable._scan_runtime_data_files(output), [])

    def test_dependency_probe_does_not_mix_site_packages(self) -> None:
        python = Path('selected-python')
        with patch.object(package_portable, '_run_cmd_capture', return_value=(True, '')) as run:
            self.assertEqual(package_portable._missing_selected_modules(python, ['pydantic']), [])
        self.assertEqual(run.call_args.args[0][:2], [str(python), '-c'])
        self.assertNotIn('sys.path', run.call_args.args[0][2])

    def test_packaging_dependency_commands_disable_user_site_packages(self) -> None:
        for command in (package_portable._run_cmd, package_portable._run_cmd_capture):
            with self.subTest(command=command.__name__), \
                    patch.object(package_portable.subprocess, 'run',
                                 return_value=subprocess.CompletedProcess([], 0, '', '')) as run:
                command(['python', '-c', 'pass'])
                self.assertEqual(run.call_args.kwargs['env']['PYTHONNOUSERSITE'], '1')

    def test_preflight_preserves_explicit_environment_without_mutating_it(self) -> None:
        env = {'FIXTURE': 'kept', 'PYTHONNOUSERSITE': '0'}
        with patch.object(package_portable.subprocess, 'run') as run, patch.object(package_portable, 'log'):
            package_portable._run_preflight_check(['python', 'check.py'], env=env)
        self.assertEqual(run.call_args.kwargs['env'], {'FIXTURE': 'kept', 'PYTHONNOUSERSITE': '1'})
        self.assertEqual(env['PYTHONNOUSERSITE'], '0')

    def test_dependency_probe_checks_missing_and_pinned_versions_without_loading_sdk(self) -> None:
        def capture(args):
            output = io.StringIO()
            with patch('importlib.util.find_spec', side_effect=lambda name: None if name == 'missing_fixture' else object()), \
                    patch('importlib.metadata.version', return_value='2.52.0'), \
                    patch('importlib.import_module', side_effect=AssertionError('must not load the SDK')), \
                    redirect_stdout(output):
                exec(args[2], {})
            return True, output.getvalue()

        with patch.object(package_portable, '_run_cmd_capture', side_effect=capture):
            self.assertEqual(package_portable._missing_selected_modules(
                Path('python'), ['lark_oapi', 'missing_fixture', 'pydantic_ai', 'openai']),
                ['missing_fixture', 'openai'])

    def test_multipart_pin_matches_startup_and_rejects_incompatible_parser(self) -> None:
        from bin.upload_event_module.services import dependency_bootstrap as deps
        self.assertEqual(package_portable.RUNTIME_MODULE_TO_PACKAGE['multipart'],
                         deps.DEFAULT_MODULE_TO_PACKAGE['multipart'])
        def capture(args):
            output = io.StringIO()
            with patch('importlib.util.find_spec', return_value=object()), \
                    patch('importlib.metadata.version', return_value='0.0.32'), redirect_stdout(output):
                exec(args[2], {})
            return True, output.getvalue()
        with patch.object(package_portable, '_run_cmd_capture', side_effect=capture):
            self.assertEqual(package_portable._missing_selected_modules(Path('python'), ['multipart']), ['multipart'])

    def test_frontend_dist_rejects_native_prompt(self) -> None:
        dist_index = (
            readiness.BIN_DIR
            / "lan_bitable_template_portal"
            / "frontend"
            / "dist"
            / "index.html"
        )
        self.assertTrue(dist_index.is_file(), "Vue dist/index.html must exist")
        asset_names = readiness.re.findall(
            r"/assets/([^\"'>]+)", dist_index.read_text(encoding="utf-8", errors="ignore")
        )
        self.assertTrue(asset_names, "Vue dist/index.html must reference assets")
        for name in asset_names:
            path = dist_index.parent / "assets" / name
            if Path(name).suffix.lower() != ".js":
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            self.assertNotIn("window.prompt", text)
            self.assertNotIn("chooseCandidateByPrompt", text)

    def test_legacy_server_not_instantiable(self) -> None:
        server_text = (
            readiness.BIN_DIR / "lan_bitable_template_portal" / "server.py"
        ).read_text(encoding="utf-8", errors="ignore")
        self.assertNotIn("PortalHandler", server_text)
        self.assertNotIn("from http.server import", server_text)
        self.assertNotIn("ThreadingHTTPServer(", server_text)


if __name__ == "__main__":
    unittest.main()

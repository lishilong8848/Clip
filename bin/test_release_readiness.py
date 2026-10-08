# -*- coding: utf-8 -*-
from __future__ import annotations

import unittest
import io
from contextlib import redirect_stdout
import tempfile
import subprocess
import threading
from pathlib import Path
from unittest.mock import patch

from bin.tools import release_readiness_check as readiness
import package_portable


class ReleaseReadinessTests(unittest.TestCase):
    def test_preflight_shards_run_concurrently_with_isolated_data_and_all_modules(self):
        barrier = threading.Barrier(3, timeout=5)
        calls = []
        modules = [f'bin.test_fixture_{index}' for index in range(17)]

        def run(args, **kwargs):
            directory = Path(kwargs['env']['CLIPFLOW_DATA_DIR'])
            self.assertTrue(directory.is_dir())
            self.assertEqual(kwargs['env']['PYTHONIOENCODING'], 'utf-8')
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
        with patch.object(package_portable, '_find_dist_venv_python', return_value=python), \
                patch.object(package_portable, 'log'), \
                patch.object(package_portable, '_assert_project_iterator_excludes_runtime_data'), \
                patch.object(package_portable, '_cleanup_vue_dist_assets'), \
                patch.object(package_portable, '_ensure_packaging_preflight_dependencies') as ensure, \
                patch.object(package_portable.subprocess, 'run') as run:
            package_portable._run_packaging_preflight_tests()
        ensure.assert_called_once_with(python)
        self.assertGreater(len(run.call_args_list), 5)
        self.assertTrue(all(call.args[0][0] == str(python) for call in run.call_args_list))
        compile_call = run.call_args_list[0]
        self.assertEqual(compile_call.args[0][1], '-c')
        self.assertIn('cfile=', compile_call.args[0][2])
        self.assertNotIn('PYTHONPYCACHEPREFIX', compile_call.kwargs.get('env', {}))

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

    def test_dependency_probe_does_not_mix_site_packages(self) -> None:
        python = Path('selected-python')
        with patch.object(package_portable, '_run_cmd_capture', return_value=(True, '')) as run:
            self.assertEqual(package_portable._missing_selected_modules(python, ['pydantic']), [])
        self.assertEqual(run.call_args.args[0][:2], [str(python), '-c'])
        self.assertNotIn('sys.path', run.call_args.args[0][2])

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

"""Dependency updates use fake installers or local Python, never pip/network."""
import io
import logging
from pathlib import Path
import sys
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from upload_event_module.services import dependency_bootstrap as deps


class DependencyTests(unittest.TestCase):
    def test_multipart_keeps_the_supported_streaming_parser_version(self):
        self.assertEqual(deps.DEFAULT_MODULE_TO_PACKAGE['multipart'], 'python-multipart==0.0.22')
        for version, supported in [('0.0.22', True), ('0.0.32', False)]:
            with self.subTest(version=version), \
                    patch.object(deps.importlib.util, 'find_spec', return_value=object()), \
                    patch.object(deps.importlib.metadata, 'version', return_value=version):
                self.assertEqual(deps._has_module('multipart'), supported)

    def setUp(self):
        for target, value in (('handlers', [logging.NullHandler()]), ('propagate', False)):
            guard = patch.object(deps._LOG, target, value)
            guard.start()
            self.addCleanup(guard.stop)

    def test_installer_logs_output_before_process_exits_and_redacts_urls(self):
        output, saw_line = io.StringIO(), threading.Event()
        class Handler(logging.StreamHandler):
            def emit(self, record):
                super().emit(record)
                if 'package-ready' in record.getMessage():
                    saw_line.set()
        handler = Handler(output)
        deps._LOG.addHandler(handler)
        self.addCleanup(deps._LOG.removeHandler, handler)
        results = []
        worker = threading.Thread(target=lambda: results.append(deps._run_cmd([sys.executable, '-u', '-c',
            "import time;print('package-ready https://user:secret@example.invalid/simple?token=private');time.sleep(1.5)"], 10)))
        worker.start()
        self.addCleanup(worker.join, 12)
        self.assertTrue(saw_line.wait(5))
        self.assertTrue(worker.is_alive(), 'output should appear while installation is still running')
        worker.join(10)
        self.assertTrue(results[0][0])
        for text in (output.getvalue(), results[0][1]):
            self.assertIn('package-ready', text)
            self.assertNotIn('secret', text)
            self.assertNotIn('private', text)

    def test_timeout_is_bounded_and_has_output(self):
        started = time.monotonic()
        ok, detail = deps._run_cmd([sys.executable, '-u', '-c', "import time;print('started');time.sleep(30)"], 5)
        self.assertFalse(ok)
        self.assertIn('超时', detail)
        self.assertIn('started', detail)
        self.assertLess(time.monotonic() - started, 12)

    def test_failed_command_with_verified_dependencies_is_success(self):
        with patch.object(deps, '_resolve_missing_packages', return_value=(['fixture'], ['fixture'])), \
                patch.object(deps, '_bootstrap_pip', return_value=(True, 'ready')), \
                patch.object(deps, '_run_cmd', return_value=(False, 'timeout')) as install, \
                patch.object(deps, '_verify_modules', return_value=[]):
            ok, detail = deps.ensure_runtime_dependencies({}, Path(sys.executable))
        self.assertTrue(ok)
        self.assertIn('核验通过', detail)
        install.assert_called_once()

    def test_missing_modules_remain_failure_with_useful_detail(self):
        with patch.object(deps, '_resolve_missing_packages', return_value=(['fixture'], ['fixture'])), \
                patch.object(deps, '_bootstrap_pip', return_value=(True, 'ready')), \
                patch.object(deps, '_run_cmd', return_value=(False, 'download timed out')), \
                patch.object(deps, '_verify_modules', return_value=['fixture']):
            ok, detail = deps.ensure_runtime_dependencies({}, Path(sys.executable), mirrors=['https://example.invalid'])
        self.assertFalse(ok)
        self.assertIn('fixture', detail)
        self.assertIn('download timed out', detail)

    def test_final_verification_is_authoritative_and_import_cache_is_invalidated(self):
        with patch.object(deps, '_resolve_missing_packages', return_value=(['fixture'], ['fixture'])), \
                patch.object(deps, '_bootstrap_pip', return_value=(True, 'ready')), \
                patch.object(deps, '_run_cmd', return_value=(True, 'installed')), \
                patch.object(deps, '_verify_modules', side_effect=[['fixture'], []]):
            self.assertTrue(deps.ensure_runtime_dependencies({}, Path(sys.executable), mirrors=['https://example.invalid'])[0])
        with patch.object(deps.importlib, 'invalidate_caches') as invalidate, patch.object(deps, '_has_module', return_value=True):
            self.assertEqual(deps._verify_modules({'fixture': 'fixture'}), [])
            invalidate.assert_called_once()

    def test_satisfied_environment_does_not_launch_installer(self):
        with patch.object(deps, '_resolve_missing_packages', return_value=([], [])), patch.object(deps, '_run_cmd') as install:
            self.assertTrue(deps.ensure_runtime_dependencies({}, Path(sys.executable))[0])
            install.assert_not_called()


if __name__ == '__main__':
    unittest.main()

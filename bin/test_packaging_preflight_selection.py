"""Offline selector/receipt tests. Never invoke a real release or full preflight."""
import ast
from contextlib import ExitStack, redirect_stderr
import copy
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

import packaging_preflight as preflight
import package_portable as packaging


LEARNING = "bin/lan_bitable_template_portal/learning_reminders.py"
CABINET = "bin/lan_bitable_template_portal/cabinet_power_text.py"
DIAGRAMS = "bin/openclaw_service/assistant/lighthouse_diagrams.py"
CHECKS = (
    "safety:syntax", "safety:imports", "safety:runtime-data", "safety:readiness",
    "bin.test_notice_identity_boundaries", "bin.test_module_access", "bin.test_patch_mirror",
    "bin.test_openclaw_service_update", "bin.test_state_store_migrations",
    "bin.test_learning", "bin.test_pre_shift_schedule", "bin.test_cabinet_power",
    "bin.test_lighthouse_queries", "bin.test_document_library",
    "frontend:check-workspace-navigation.mjs", "frontend:check-learning-sharing-types.mjs",
)


class SelectionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="clipflow_selector_test_")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.cache = self.root / "build_output/.preflight-coverage.json"
        self.state = {"root": str(self.root), "environment": "fixture-environment",
                      "files": {LEARNING: "learn-1", CABINET: "cabinet-1", DIAGRAMS: "diagram-1",
                                "package_portable.py": "runner-1",
                                "bin/test_learning.py": "test-1",
                                preflight.FRONTEND + "dist/assets/app.js": "asset-1"},
                      "stamps": {LEARNING: [1, 2, 3]}}

    def select(self, state=None, *, full=False, checks=CHECKS):
        return preflight.Selection(self.state if state is None else state, checks, self.cache, full=full)

    def baseline(self):
        selection = self.select()
        self.assertTrue(selection.full)
        selection.passed(*selection.selected)
        selection.finish(self.state)

    def test_first_run_full_and_log_is_not_a_receipt(self):
        self.cache.parent.mkdir()
        self.cache.write_text("8086 tests passed in 15m30", encoding="utf-8")
        selection = self.select()
        self.assertTrue(selection.full)
        self.assertEqual(selection.selected, frozenset(CHECKS))

    def test_unchanged_exact_reuse_keeps_safety_and_updater_checks(self):
        self.baseline()
        selection = self.select()
        self.assertFalse(selection.full)
        self.assertEqual(selection.selected, frozenset(check for check in CHECKS
            if check.startswith("safety:") or check in preflight.EXACT_RECHECK_TESTS))
        self.assertNotIn("bin.test_learning", selection.selected)
        self.assertNotIn("bin.test_notice_identity_boundaries", selection.selected)
        self.assertIn("bin.test_patch_mirror", selection.selected)

    def test_exact_reuse_expires_without_extending_full_baseline(self):
        self.baseline()
        original_time = json.loads(self.cache.read_text())["payload"]["full_passed_at"]
        with patch.object(preflight.time, "time", return_value=original_time + preflight.FULL_BASELINE_MAX_AGE - 1):
            selection = self.select()
            self.assertFalse(selection.full)
            selection.passed(*selection.selected)
            selection.finish(self.state)
        self.assertEqual(json.loads(self.cache.read_text())["payload"]["full_passed_at"], original_time)
        for now in (original_time - 1, original_time + preflight.FULL_BASELINE_MAX_AGE):
            with self.subTest(now=now), patch.object(preflight.time, "time", return_value=now):
                self.assertTrue(self.select().full)

    def test_unknown_or_invalid_full_baseline_time_is_untrusted(self):
        self.baseline()
        envelope = json.loads(self.cache.read_text())
        for value in (None, True, -1, "invalid", float("nan"), float("inf"), 10 ** 100):
            envelope["payload"]["full_passed_at"] = value
            envelope["signature"] = preflight.hmac.new(self.cache.with_suffix(".key").read_bytes(),
                preflight._digest(envelope["payload"]).encode(), "sha256").hexdigest()
            self.cache.write_text(json.dumps(envelope))
            with self.subTest(value=value):
                self.assertTrue(self.select().full)

    def test_learning_change_selects_consumers_and_frontend(self):
        self.baseline()
        self.state["files"][LEARNING] = "uncommitted-edit"
        selection = self.select()
        self.assertFalse(selection.full)
        self.assertTrue({"bin.test_learning", "bin.test_pre_shift_schedule", "bin.test_lighthouse_queries",
                         "bin.test_document_library", "frontend:check-learning-sharing-types.mjs"} <= selection.selected)
        self.assertNotIn("bin.test_cabinet_power", selection.selected)

    def test_cabinet_change_selects_assistant_regressions(self):
        self.baseline()
        self.state["files"][CABINET] = "changed"
        selected = self.select().selected
        self.assertIn("bin.test_cabinet_power", selected)
        self.assertIn("bin.test_lighthouse_queries", selected)
        self.assertNotIn("bin.test_learning", selected)

    def test_frontend_assets_invalidate_frontend_and_backend_contracts(self):
        self.baseline()
        self.state["files"][preflight.FRONTEND + "dist/assets/app.js"] = "changed"
        self.assertEqual(self.select().selected, frozenset(CHECKS))

    def test_new_and_deleted_files_force_full_even_in_known_area(self):
        self.baseline()
        for path, delete in ((LEARNING, True), ("bin/new_scheduler.py", False),
                             (preflight.FRONTEND + "dist/assets/new.js", False)):
            state = copy.deepcopy(self.state)
            if delete:
                del state["files"][path]
            else:
                state["files"][path] = "new"
            with self.subTest(path=path):
                self.assertTrue(self.select(state).full)

    def test_shared_unknown_test_configuration_dependency_changes_force_full(self):
        paths = ("bin/lan_bitable_template_portal/portal_auth.py",
                 "bin/lan_bitable_template_portal/account_policy.py",
                 "bin/lan_bitable_template_portal/state_store.py",
                 "bin/upload_event_module/services/remote_patch.py",
                 "bin/clipflow_backend/main.py", "bin/unknown.py", "requirements.txt",
                 preflight.FRONTEND + "package-lock.json", preflight.FRONTEND + "vite.config.ts",
                 "bin/test_learning.py", "package_portable.py")
        self.state["files"].update({path: "original" for path in paths})
        self.baseline()
        for path in paths:
            state = copy.deepcopy(self.state)
            state["files"][path] = "changed"
            with self.subTest(path=path):
                self.assertTrue(self.select(state).full)

    def test_environment_change_forces_full(self):
        self.baseline()
        self.state["environment"] = "same-version-package-with-local-edit"
        self.assertTrue(self.select().full)

    def test_explicit_full_bypasses_exact_pass(self):
        self.baseline()
        selection = self.select(full=True)
        self.assertTrue(selection.full)
        self.assertEqual(selection.selected, frozenset(CHECKS))

    def test_changed_check_manifest_forces_full(self):
        self.baseline()
        for checks in (CHECKS + ("bin.test_new_regression",), CHECKS[:-1]):
            self.assertTrue(self.select(checks=checks).full)

    def test_tampered_or_missing_signature_and_key_are_untrusted(self):
        self.baseline()
        original = self.cache.read_bytes()
        for contents in (b"{}", b"[]", b"broken", original.replace(b"learn-1", b"learn-2")):
            self.cache.write_bytes(contents)
            self.assertTrue(self.select().full)
        self.cache.write_bytes(original)
        self.cache.with_suffix(".key").unlink()
        self.assertTrue(self.select().full)

    def test_incomplete_coverage_cannot_advance_baseline(self):
        self.baseline()
        original = self.cache.read_bytes()
        selection = self.select()
        selected = sorted(selection.selected)
        selection.passed(*selected[:-1])
        with self.assertRaisesRegex(RuntimeError, "未完成"):
            selection.finish(self.state)
        self.assertEqual(self.cache.read_bytes(), original)

    def test_source_content_or_timestamp_mutation_during_run_rejected(self):
        self.baseline()
        original = self.cache.read_bytes()
        for stamps_only in (False, True):
            selection = self.select()
            selection.passed(*selection.selected)
            after = copy.deepcopy(self.state)
            if stamps_only:
                after["stamps"][LEARNING][2] += 1
            else:
                after["files"][LEARNING] = "edited-during-check"
            with self.subTest(stamps_only=stamps_only), self.assertRaisesRegex(RuntimeError, "发生变化"):
                selection.finish(after)
            self.assertEqual(self.cache.read_bytes(), original)

    def test_drift_report_names_changed_source_without_advancing_receipt(self):
        self.baseline()
        original = self.cache.read_bytes()
        selection = self.select(); selection.passed(*selection.selected)
        after = copy.deepcopy(self.state)
        after['files'][LEARNING] = 'new-content-hash'
        after['files']['new-module.py'] = 'new-file-hash'
        del after['files'][CABINET]
        with self.assertRaisesRegex(RuntimeError, LEARNING):
            selection.finish(after)
        report = json.loads(self.cache.with_name('.preflight-changes.json').read_text(encoding='utf-8'))
        self.assertEqual(report['changes']['项目文件内容变化']['paths'], [LEARNING])
        self.assertEqual(report['changes']['项目文件新增']['paths'], ['new-module.py'])
        self.assertEqual(report['changes']['项目文件删除']['paths'], [CABINET])
        self.assertEqual(self.cache.read_bytes(), original)

    def test_environment_details_distinguish_dependency_content_from_metadata_and_hide_values(self):
        before = copy.deepcopy(self.state)
        before['environment_details'] = {'probe': 'probe-hash',
            'variables': {'PRIVATE_KEY': preflight._digest('do-not-store-secret')},
            'dependencies': {'python-library': {'module.py': ('same-hash', (1, 2, 3))}}}
        after = copy.deepcopy(before); after['environment'] = 'changed-hash'
        after['environment_details']['variables']['PRIVATE_KEY'] = preflight._digest('new-private-value')
        after['environment_details']['dependencies']['python-library']['module.py'] = ('same-hash', (1, 4, 3))
        result = preflight.changes(before, after)
        self.assertEqual(result['环境变量变化（仅名称）'], ['PRIVATE_KEY'])
        self.assertEqual(result['依赖文件时间或状态变化'], [str(Path('python-library/module.py'))])
        self.assertNotIn('do-not-store-secret', json.dumps(result))
        self.assertNotIn('new-private-value', json.dumps(result))
        after['environment_details']['dependencies']['python-library']['module.py'] = ('different-content', (1, 4, 3))
        self.assertIn('依赖文件新增、删除或内容变化', preflight.changes(before, after))

    def test_unwritable_drift_report_does_not_hide_failure_or_replace_cache(self):
        self.baseline()
        original = self.cache.read_bytes()
        selection = self.select(); selection.passed(*selection.selected)
        after = copy.deepcopy(self.state); after['stamps'][LEARNING][1] += 1
        with patch.object(Path, 'write_text', side_effect=OSError('report unavailable')):
            with self.assertRaisesRegex(RuntimeError, '项目文件时间或状态变化'):
                selection.finish(after)
        self.assertEqual(self.cache.read_bytes(), original)

    def test_successful_layered_run_carries_only_matching_passes_forward(self):
        self.baseline()
        self.state["files"][LEARNING] = "edited"
        selection = self.select()
        selection.passed(*selection.selected)
        selection.finish(self.state)
        self.assertNotIn("bin.test_learning", self.select().selected)
        self.state["files"][CABINET] = "next-edit"
        self.assertIn("bin.test_cabinet_power", self.select().selected)

    def test_unavailable_snapshot_runs_full_without_creating_cache(self):
        selection = preflight.Selection(None, CHECKS, self.cache)
        self.assertEqual(selection.selected, frozenset(CHECKS))
        selection.passed(*selection.selected)
        selection.finish(None)
        self.assertFalse(self.cache.exists())

    def test_atomic_write_failure_preserves_old_receipt(self):
        self.baseline()
        original = self.cache.read_bytes()
        selection = self.select()
        selection.passed(*selection.selected)
        with patch.object(preflight.os, "replace", side_effect=OSError("fixture write failure")):
            with self.assertRaises(OSError):
                selection.finish(self.state)
        self.assertEqual(self.cache.read_bytes(), original)
        self.assertEqual(list(self.cache.parent.glob(".preflight-*")), [self.cache, self.cache.with_suffix(".key")])

    def test_disk_fingerprint_includes_uncommitted_untracked_and_deleted_files(self):
        tracked = self.root / LEARNING
        tracked.parent.mkdir(parents=True)
        tracked.write_text("original", encoding="utf-8")
        before = preflight._tree(self.root, project=True)
        stamp = tracked.stat()
        tracked.write_text("modified", encoding="utf-8")
        os.utime(tracked, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
        after = preflight._tree(self.root, project=True)
        self.assertNotEqual(before[LEARNING][0], after[LEARNING][0])
        new = tracked.with_name("new_untracked.py")
        new.write_text("x = 1", encoding="utf-8")
        self.assertIn(new.relative_to(self.root).as_posix(), preflight._tree(self.root, project=True))
        tracked.unlink()
        self.assertNotIn(LEARNING, preflight._tree(self.root, project=True))

    def test_disk_fingerprint_covers_frontend_config_fixtures_and_excludes_runtime(self):
        included = (preflight.FRONTEND + "src/components/Test.vue", preflight.FRONTEND + "dist/assets/test.js",
                    preflight.FRONTEND + "public/icon.svg", preflight.FRONTEND + "package-lock.json",
                    "bin/tests/fixtures/example.json", "bin/tests/fixtures/example.sqlite",
                    "bin/tests/fixtures/parser.log", "bin/wheels/dependency.whl", ".env")
        excluded = ("build_output/.preflight-coverage.json", "bin/data/config.json", "output/run.log",
                    "bin/__pycache__/x.pyc", "bin/.venv/package.py", preflight.FRONTEND + "node_modules/x.js")
        for name in included + excluded:
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("fixture", encoding="utf-8")
        self.assertEqual(set(preflight._tree(self.root, project=True)), set(included))

    def test_safety_families_are_always_on(self):
        self.assertIsNone(preflight.check_areas("frontend:unknown-new-check.mjs"))
        for name in ("notice_undo_isolation", "document_library_maintenance_permissions", "learning_scope_boundaries",
                     "lighthouse_shared_privacy", "lighthouse_sandbox", "learning_shift_storage",
                     "openclaw_service_update", "release_readiness", "transport_safety", "packaging_preflight_selection"):
            with self.subTest(name=name):
                self.assertIsNone(preflight.check_areas("bin.test_" + name))

    def test_real_environment_fingerprint_detects_same_version_local_dependency_edits(self):
        root = self.root / "project"
        library = self.root / "python-library"
        modules = root / preflight.FRONTEND / "node_modules"
        for directory in (root, library, modules):
            directory.mkdir(parents=True, exist_ok=True)
        executable = self.root / "python.exe"
        node = self.root / "node.exe"
        dependency = library / "dependency.py"
        metadata = library / "METADATA"
        node_dependency = modules / "dependency.js"
        compiler = modules / "@vue/language-core/lib/codegen/globalTypes.js"
        browsers = self.root / "local/ms-playwright"
        browser = browsers / "chromium_headless_shell-123/chrome-headless-shell-win64/chrome-headless-shell.exe"
        generated = (modules / ".vue-global-types/vue_99.d.ts",
                     browsers / "b/browser@fixture", browser.with_name("debug.log"))
        inputs = (executable, node, dependency, metadata, node_dependency, compiler, browser)
        for path in inputs + generated:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("unchanged-version", encoding="utf-8")
        info = {"version": "fixture", "platform": "fixture", "executable": str(executable),
                "paths": [str(root), str(library)], "stdlib": str(library)}
        with patch.dict(os.environ, {"LOCALAPPDATA": str(self.root / "local")}, clear=True), \
                patch.object(preflight.shutil, "which", return_value=str(node)), \
                patch.object(preflight.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, json.dumps(info))) as run:
            before = preflight.snapshot(root, str(executable))
            self.assertEqual(before, preflight.snapshot(root, str(executable)))
            with patch.dict(os.environ, {"CODEX_TOOL_CALL_ID": "another-command"}):
                self.assertEqual(before, preflight.snapshot(root, str(executable)))
            for key in ("PATH", "PYTHONPATH", "NODE_OPTIONS", "HTTP_PROXY", "CLIPFLOW_MOCK_EXTERNAL"):
                with patch.dict(os.environ, {key: "changed-configuration"}):
                    self.assertNotEqual(before["environment"], preflight.snapshot(root, str(executable))["environment"])
            for path in generated:
                path.write_text("regenerated during checks", encoding="utf-8")
                self.assertEqual(before, preflight.snapshot(root, str(executable)))
                path.unlink()
                self.assertEqual(before, preflight.snapshot(root, str(executable)))
            for path in inputs:
                stamp = path.stat()
                path.write_text("same-version-edit", encoding="utf-8")
                os.utime(path, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
                after = preflight.snapshot(root, str(executable))
                self.assertNotEqual(before["environment"], after["environment"])
                before = after
            self.assertTrue(all(call.kwargs["env"]["PYTHONNOUSERSITE"] == "1" for call in run.call_args_list))

    def test_signed_receipt_with_missing_coverage_is_untrusted(self):
        self.baseline()
        envelope = json.loads(self.cache.read_text())
        del envelope["payload"]["coverage"]["bin.test_learning"]
        envelope["signature"] = preflight.hmac.new(self.cache.with_suffix(".key").read_bytes(),
            preflight._digest(envelope["payload"]).encode(), "sha256").hexdigest()
        self.cache.write_text(json.dumps(envelope))
        self.assertTrue(self.select().full)


class PackagingIntegrationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="clipflow_selector_integration_")
        self.addCleanup(temporary.cleanup)
        self.build = Path(temporary.name)
        self.state = {"root": str(packaging.PROJECT_ROOT), "environment": "fake-isolated-environment",
                      "files": {LEARNING: "one", CABINET: "one"}, "stamps": {}}
        self.executed = []

    def run_preflight(self, *, flags=(), failure=None, snapshot=None):
        def run(args, **kwargs):
            if args[1:3] == ["-m", "unittest"]:
                self.executed.extend(args[3:])
                if failure:
                    raise failure
        with ExitStack() as stack:
            for name in ("_ensure_packaging_preflight_dependencies", "_cleanup_vue_dist_assets",
                         "_assert_project_iterator_excludes_runtime_data", "log"):
                stack.enter_context(patch.object(packaging, name))
            stack.enter_context(patch.object(packaging, "_verify_runtime_imports", return_value=True))
            stack.enter_context(patch.object(packaging, "_iter_project_files", return_value=[]))
            stack.enter_context(patch.object(packaging, "_find_build_python", return_value=Path(sys.executable)))
            stack.enter_context(patch.object(packaging, "BUILD_DIR", self.build))
            stack.enter_context(patch.object(packaging, "_run_preflight_check", side_effect=run))
            stack.enter_context(patch.object(sys, "argv", ["package_portable.py", *flags]))
            snapshot_mock = stack.enter_context(patch.object(preflight, "snapshot", side_effect=snapshot,
                                                            return_value=self.state))
            frontend = stack.enter_context(patch.object(packaging, "_run_frontend_notice_navigation_checks"))
            self.frontend = frontend
            packaging._run_packaging_preflight_tests()
        return frontend, snapshot_mock

    def test_normal_unchanged_run_reuses_pass_and_retains_safety_tests(self):
        self.run_preflight()
        full_count = len(self.executed)
        self.executed.clear()
        frontend, _ = self.run_preflight()
        self.assertLess(len(self.executed), full_count)
        self.assertNotIn("bin.test_learning", self.executed)
        self.assertIn("bin.test_packaging_preflight_selection", self.executed)
        self.assertIn("bin.test_patch_mirror", self.executed)
        self.assertNotIn("bin.test_notice_identity_boundaries", self.executed)
        self.assertNotIn("bin.test_module_access", self.executed)
        self.assertIn("bin.test_release_readiness", self.executed)
        self.assertIn("bin.test_transport_safety", self.executed)
        self.assertIn("bin.test_update_check_recovery", self.executed)
        self.assertFalse(frontend.call_args.args[0].full)

    def test_changed_known_module_still_reruns_all_mandatory_checks(self):
        self.run_preflight()
        self.executed.clear()
        self.state["files"][LEARNING] = "changed"
        frontend, _ = self.run_preflight()
        self.assertIn("bin.test_notice_identity_boundaries", self.executed)
        self.assertIn("bin.test_module_access", self.executed)
        self.assertIn("bin.test_learning", self.executed)
        self.assertFalse(frontend.call_args.args[0].full)

    def test_cli_full_flags_bypass_normal_reuse(self):
        self.run_preflight()
        expected = list(self.executed)
        for flag in ("--preflight-test", "--full-preflight"):
            self.executed.clear()
            frontend, _ = self.run_preflight(flags=(flag,))
            frontend.assert_called_once_with()
            self.assertIn("bin.test_learning", self.executed)
            self.assertEqual(self.executed, expected)

    def test_failed_and_interrupted_runs_do_not_advance_receipt(self):
        self.run_preflight()
        cache = self.build / ".preflight-coverage.json"
        original = cache.read_bytes()
        self.state["files"][LEARNING] = "edited"
        for error in (subprocess.CalledProcessError(1, "fixture"), KeyboardInterrupt()):
            with self.subTest(error=type(error).__name__), self.assertRaises(type(error)):
                self.run_preflight(failure=error)
            self.assertEqual(cache.read_bytes(), original)

    def test_failed_first_run_creates_no_baseline(self):
        with self.assertRaises(subprocess.CalledProcessError):
            self.run_preflight(failure=subprocess.CalledProcessError(1, "fixture"))
        self.assertFalse((self.build / ".preflight-coverage.json").exists())
        self.frontend.assert_not_called()

    def test_midrun_change_stops_without_advancing_baseline(self):
        self.run_preflight()
        cache = self.build / ".preflight-coverage.json"
        original = cache.read_bytes()
        after = copy.deepcopy(self.state)
        after["files"][LEARNING] = "edited-during-run"
        with self.assertRaisesRegex(RuntimeError, "发生变化"):
            self.run_preflight(snapshot=[self.state, after])
        self.assertEqual(cache.read_bytes(), original)

    def test_missing_cached_test_is_rejected_before_execution(self):
        self.run_preflight()
        original = Path.is_file
        def exists(path):
            return False if path.name == "test_learning.py" else original(path)
        with patch.object(Path, "is_file", exists), self.assertRaisesRegex(RuntimeError, "缺少预检文件"):
            self.run_preflight()

    def test_missing_cached_frontend_check_is_rejected(self):
        self.run_preflight()
        original = Path.is_file
        def exists(path):
            return False if path.name == "check-learning-sharing-types.mjs" else original(path)
        with patch.object(Path, "is_file", exists), self.assertRaisesRegex(RuntimeError, "缺少预检文件"):
            self.run_preflight()

    def test_frontend_runner_uses_selected_checks_and_always_checks_files(self):
        selection = preflight.Selection(None, ["frontend:" + name for name in packaging.FRONTEND_PREFLIGHT_SCRIPTS], self.build / "unused")
        selection.selected = frozenset("frontend:" + name for name in preflight.CORE_FRONTEND)
        with patch.object(packaging.shutil, "which", return_value="fixture-node"), \
                patch.object(packaging, "_run_preflight_check") as run:
            packaging._run_frontend_notice_navigation_checks(selection)
        names = {Path(call.args[0][1]).name for call in run.call_args_list}
        self.assertEqual(names, preflight.CORE_FRONTEND)

    def test_frontend_independent_long_checks_run_concurrently(self):
        names = ("check-legacy-module-entry.mjs", "check-pre-shift-edits.mjs", "check-workspace-navigation.mjs")
        barrier, calls = threading.Barrier(2), []
        def run(args, **kwargs):
            name = Path(args[1]).name
            calls.append(name)
            if name in names[:2]:
                barrier.wait(timeout=5)
        with patch.object(packaging, "FRONTEND_PREFLIGHT_SCRIPTS", names), \
                patch.object(packaging, "PREFLIGHT_WORKERS", 2), \
                patch.object(packaging.shutil, "which", return_value="fixture-node"), \
                patch.object(packaging, "_run_preflight_check", side_effect=run):
            packaging._run_frontend_notice_navigation_checks()
        self.assertEqual(set(calls[:2]), set(names[:2]))
        self.assertEqual(calls[2:], [names[2]])

    def test_frontend_parallel_failure_stops_remaining_checks(self):
        names = ("check-legacy-module-entry.mjs", "check-pre-shift-edits.mjs", "check-workspace-navigation.mjs")
        barrier, calls = threading.Barrier(2), []
        def run(args, **kwargs):
            name = Path(args[1]).name
            calls.append(name)
            if name in names[:2]:
                barrier.wait(timeout=5)
            if name == names[0]:
                raise subprocess.CalledProcessError(1, name)
        with patch.object(packaging, "FRONTEND_PREFLIGHT_SCRIPTS", names), \
                patch.object(packaging, "PREFLIGHT_WORKERS", 2), \
                patch.object(packaging.shutil, "which", return_value="fixture-node"), \
                patch.object(packaging, "_run_preflight_check", side_effect=run), \
                self.assertRaises(subprocess.CalledProcessError):
            packaging._run_frontend_notice_navigation_checks()
        self.assertEqual(set(calls), set(names[:2]))

    def test_non_unittest_checks_receive_isolated_data(self):
        directories = []
        def run(args, **kwargs):
            directory = Path(kwargs["env"]["CLIPFLOW_DATA_DIR"])
            self.assertTrue(directory.is_dir())
            directories.append(directory)
        with patch.object(packaging.subprocess, "run", side_effect=run), patch.object(packaging, "log"):
            packaging._run_preflight_check(["python", "fixture.py"], check=True)
            packaging._run_preflight_check(["node", "fixture.mjs"], check=True)
        self.assertEqual(len(set(directories)), 2)
        self.assertTrue(all(not path.exists() for path in directories))

    def test_cli_cannot_silently_skip_explicit_full(self):
        for flags in (("--full-preflight", "--skip-preflight"), ("--preflight-test", "--skip-preflight"),
                      ("--full-preflight", "--retry-upload"), ("--preflight-test", "--retry-upload"),
                      ("--full-pre",)):
            with self.subTest(flags=flags), patch.object(sys, "argv", ["package_portable.py", *flags]), \
                    patch.object(packaging, "_ensure_mcp_frontend_assets") as assets, \
                    redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
                packaging.main()
            self.assertEqual(raised.exception.code, 2)
            assets.assert_not_called()

    def test_packaging_helper_is_not_distributed(self):
        self.assertTrue(packaging._is_excluded(packaging.PROJECT_ROOT / "packaging_preflight.py",
                                               root=packaging.PROJECT_ROOT))

    def test_existing_test_registration_and_timing_groups_are_preserved(self):
        tree = ast.parse(Path(packaging.__file__).read_text(encoding="utf-8-sig"))
        function = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                        and node.name == "_run_packaging_preflight_tests")
        groups = next(node.value for node in function.body if isinstance(node, ast.Assign)
                      and any(isinstance(target, ast.Name) and target.id == "test_groups" for target in node.targets))
        groups = ast.literal_eval(groups)
        self.assertEqual(groups[1], ["bin.test_public_access"])
        self.assertEqual(groups[2], ["bin.test_lighthouse_knowledge_stream_storage"])
        self.assertLessEqual(packaging.PREFLIGHT_WORKERS, 4)


if __name__ == "__main__":
    unittest.main()

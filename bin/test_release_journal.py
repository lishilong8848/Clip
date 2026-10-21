"""Release drafts and publication state tested only with local Git and fake transports."""
import ast
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import zipfile

BIN = Path(__file__).resolve().parent
if str(BIN) not in sys.path:
    sys.path.insert(0, str(BIN))

import package_portable as packaging
from frontend_assets import patch_deletions
from upload_event_module.services import release_journal as journal_module
from lan_bitable_template_portal.release_journal import ReleaseJournalStore
from upload_event_module.services.remote_patch_updater import RemotePatchUpdater


class ReleaseJournalTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="release-journal-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name) / "project"
        self.root.mkdir()
        self.build = self.root / "build_output"
        self.journal = journal_module.BuildReleaseJournal(self.root, self.build)
        self.git("init", "-q")
        (self.root / "app.py").write_text("first = True\n", encoding="utf-8")
        self.commit("initial fixture")

    def git(self, *args):
        result = subprocess.run(["git", "-c", "core.hooksPath=", "-c", "commit.gpgsign=false", "-C", str(self.root), *args], check=True,
                                capture_output=True, text=True, encoding="utf-8")
        return result.stdout.strip()

    def commit(self, subject):
        self.git("add", "app.py")
        self.git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", subject)
        return self.git("rev-parse", "HEAD")

    def confirmed_draft(self):
        draft = self.journal.prepare_draft()
        draft["items"] = [{"problem": "recursive console output", "solution": "guard each thread", "module": "transport"}]
        return self.journal.save_draft(draft, confirmed=True)

    def package(self, *, number=1, publish=True, baseline=None):
        version = "ClipFlow_V2_fixture_" + str(number)
        draft = self.confirmed_draft()
        bundle = self.journal.bundle(draft, version, number, publish=publish)
        with patch.object(packaging, "PROJECT_ROOT", self.root), patch.object(packaging, "BUILD_DIR", self.build):
            packaging.build_patch(self.root / "unused", baseline, version, target_patch_version=number,
                                  release_journal_bundle=bundle)
            directory = self.build / (version + "_patch_only")
            archive = packaging._zip_patch_dir(directory)
            manifest_path, manifest = packaging._write_latest_patch_manifest(directory, archive)
        return directory, archive, manifest_path, manifest

    def publish(self, archive, manifest_path, manifest, *, failure=None):
        with patch.object(packaging, "PROJECT_ROOT", self.root), patch.object(packaging, "BUILD_DIR", self.build), \
                patch("upload_event_module.services.patch_mirror.publish_patch", return_value={"fixture": "verified"}) as upload, \
                patch("upload_event_module.services.patch_mirror.publish_manifest", side_effect=failure) as publish:
            packaging._publish_patch(archive, manifest_path, manifest)
        return upload, publish

    def test_first_snapshot_uses_current_commit_only_and_dirty_file_summary(self):
        first = self.git("rev-parse", "HEAD")
        (self.root / "app.py").write_text("second = True\n", encoding="utf-8")
        current = self.commit("second fixture")
        (self.root / "app.py").write_text("dirty = True\n", encoding="utf-8")
        snapshot = self.journal.snapshot()
        self.assertEqual(snapshot["baseline"], "")
        self.assertEqual([c["id"] for c in snapshot["commits"]], [current])
        self.assertNotIn(first, [c["id"] for c in snapshot["commits"]])
        self.assertEqual(snapshot["files"][0]["path"], "app.py")
        self.assertNotIn("dirty = True", json.dumps(snapshot))
        self.assertNotIn("fixture@example.invalid", json.dumps(snapshot))

    def test_draft_recovers_and_source_changes_require_reconfirmation(self):
        draft = self.confirmed_draft()
        other = journal_module.BuildReleaseJournal(self.root, self.build)
        self.assertEqual(other.prepare_draft(), draft)
        (self.root / "app.py").write_text("changed_content = True\n", encoding="utf-8")
        changed = other.prepare_draft()
        self.assertNotEqual(changed["id"], draft["id"])
        self.assertFalse(changed["confirmed"])
        self.assertEqual(changed["items"], draft["items"])

    def test_incremental_snapshot_keeps_every_commit_since_successful_baseline(self):
        _, archive, path, manifest = self.package()
        self.publish(archive, path, manifest)
        baseline = self.journal.state()["baseline"]
        heads = []
        for number in (2, 3):
            (self.root / "app.py").write_text("value = " + str(number), encoding="utf-8")
            heads.append(self.commit("fixture change " + str(number)))
        snapshot = self.journal.snapshot()
        self.assertEqual(snapshot["baseline"], baseline)
        self.assertEqual([commit["id"] for commit in snapshot["commits"]], list(reversed(heads)))

    def test_success_advances_baseline_and_next_snapshot_is_incremental(self):
        _, archive, manifest_path, manifest = self.package()
        packaged_head = self.git("rev-parse", "HEAD")
        self.assertFalse(self.journal.state_path.exists())
        self.publish(archive, manifest_path, manifest)
        self.assertEqual(self.journal.state()["baseline"], packaged_head)
        (self.root / "app.py").write_text("next = True\n", encoding="utf-8")
        next_head = self.commit("next fixture")
        snapshot = self.journal.snapshot()
        self.assertEqual(snapshot["baseline"], packaged_head)
        self.assertEqual([c["id"] for c in snapshot["commits"]], [next_head])

    def test_failed_publication_retry_reuses_receipt_and_packaged_commit(self):
        directory, archive, manifest_path, manifest = self.package()
        packaged_head = self.git("rev-parse", "HEAD")
        before = archive.read_bytes()
        with self.assertRaises(OSError):
            self.publish(archive, manifest_path, manifest, failure=OSError("fixture publication failure"))
        self.assertFalse(self.journal.state_path.exists())
        receipt = journal_module.read_json(self.journal.receipt_path)["proof"]
        self.assertNotIn("release_journal", json.loads(manifest_path.read_text(encoding="utf-8")))
        (self.root / "app.py").write_text("later = True\n", encoding="utf-8")
        self.commit("later unshipped fixture")
        upload, publish = self.publish(archive, manifest_path, manifest)
        self.assertEqual(publish.call_args.args[0]["release_journal"], receipt)
        upload.assert_called_once()
        self.assertEqual(self.journal.state()["baseline"], packaged_head)
        self.assertEqual(archive.read_bytes(), before)
        self.assertEqual(hashlib.sha256(before).hexdigest(), manifest["zip_sha256"])
        self.assertEqual(len(self.journal.state()["entries"]), 1)
        self.publish(archive, manifest_path, manifest)
        self.assertEqual(len(self.journal.state()["entries"]), 1)

    def test_failed_upload_does_not_create_publication_state(self):
        _, archive, manifest_path, manifest = self.package()
        with patch.object(packaging, "BUILD_DIR", self.build), \
                patch("upload_event_module.services.patch_mirror.publish_patch", side_effect=TimeoutError("fixture upload")), \
                patch("upload_event_module.services.patch_mirror.publish_manifest") as publish:
            with self.assertRaises(TimeoutError):
                packaging._publish_patch(archive, manifest_path, manifest)
            publish.assert_not_called()
        self.assertFalse(self.journal.state_path.exists())
        self.assertFalse(self.journal.receipt_path.exists())

    def test_history_is_complete_hashed_and_not_deleted_or_public_assets(self):
        first_dir, archive, path, manifest = self.package()
        self.publish(archive, path, manifest)
        second_dir, _, _, second = self.package(number=2, baseline=first_dir)
        history = second_dir / journal_module.BUNDLE_PATH
        bundle = json.loads(history.read_text(encoding="utf-8"))
        meta = json.loads((second_dir / "bin/patch_meta.json").read_text(encoding="utf-8"))
        self.assertEqual(len(bundle["entries"]), 2)
        self.assertEqual(meta["file_sha256"][journal_module.BUNDLE_PATH.as_posix()], hashlib.sha256(history.read_bytes()).hexdigest())
        self.assertNotIn("zip_sha256", history.read_text(encoding="utf-8"))
        self.assertNotIn(second["zip_sha256"], history.read_text(encoding="utf-8"))
        self.assertNotIn(journal_module.BUNDLE_PATH.as_posix(), (second_dir / "patch_manifest.txt").read_text(encoding="utf-8"))
        self.assertNotIn("assets", journal_module.BUNDLE_PATH.parts)
        packaging._assert_no_runtime_data_in_output(second_dir, "fixture")
        packaging._assert_no_development_files_in_output(second_dir, "fixture")

    def test_reimporting_old_success_does_not_regress_new_baseline(self):
        first_dir, archive, path, first = self.package()
        self.publish(archive, path, first)
        content = (first_dir / journal_module.BUNDLE_PATH).read_bytes()
        (self.root / "app.py").write_text("next = True", encoding="utf-8")
        head = self.commit("next fixture")
        _, archive, path, second = self.package(number=2)
        self.publish(archive, path, second)
        self.journal.mark_published(content, first)
        self.assertEqual(self.journal.state()["baseline"], head)
        self.assertEqual(len(self.journal.state()["entries"]), 2)

    def test_skip_upload_has_no_dialog_and_cannot_be_published_as_confirmed(self):
        args = SimpleNamespace(skip_upload=True, release_notes_file="")
        with patch.object(packaging, "PROJECT_ROOT", self.root), patch.object(packaging, "BUILD_DIR", self.build), \
                patch("upload_event_module.ui.release_notes_dialog.review_release_notes") as review:
            _, draft = packaging._prepare_release_notes(args)
            review.assert_not_called()
        self.assertFalse(draft["confirmed"])
        _, archive, _, manifest = self.package(publish=False)
        with patch("upload_event_module.services.patch_mirror.publish_patch") as upload:
            with self.assertRaises(ValueError):
                packaging._publish_patch(archive, self.build / "latest_patch.json", manifest)
            upload.assert_not_called()
        self.assertFalse(self.journal.state_path.exists())

    def test_noninteractive_publish_requires_explicit_notes_file_without_dialog(self):
        args = SimpleNamespace(skip_upload=False, release_notes_file="")
        with patch.object(packaging, "PROJECT_ROOT", self.root), patch.object(packaging, "BUILD_DIR", self.build), \
                patch("upload_event_module.ui.release_notes_dialog.review_release_notes") as review:
            with self.assertRaises(ValueError):
                packaging._prepare_release_notes(args)
            review.assert_not_called()
            supplied = self.root / "reviewed.json"
            supplied.write_text(json.dumps({"items": self.confirmed_draft()["items"]}), encoding="utf-8")
            args.release_notes_file = str(supplied)
            _, draft = packaging._prepare_release_notes(args)
            self.assertTrue(draft["confirmed"])
            review.assert_not_called()

    def test_preflight_exits_before_draft_or_dialog(self):
        with patch.object(sys, "argv", ["package_portable.py", "--preflight-test"]), \
                patch.object(packaging, "_ensure_mcp_frontend_assets"), \
                patch.object(packaging, "_ensure_feishu_app_profile"), \
                patch.object(packaging, "_find_build_python", return_value=None), \
                patch.object(packaging, "_run_packaging_preflight_tests") as preflight, \
                patch.object(packaging, "_prepare_release_notes") as prepare:
            packaging.main()
            preflight.assert_called_once_with()
            prepare.assert_not_called()

    def test_preflight_registers_new_backend_suites(self):
        tree = ast.parse(Path(packaging.__file__).read_text(encoding="utf-8-sig"))
        preflight = next(node for node in tree.body
                         if isinstance(node, ast.FunctionDef) and node.name == "_run_packaging_preflight_tests")
        registered = {node.value for node in ast.walk(preflight)
                      if isinstance(node, ast.Constant) and isinstance(node.value, str)}
        required = {"bin.test_" + name for name in (
            "document_library", "document_library_remote", "document_library_portal",
            "learning_completion", "learning_sharing_materials",
            "logger_reentry", "qt_settings_save_pipeline", "release_journal", "release_journal_routes",
            "update_check_recovery", "workspace_extension_access",
        )}
        self.assertFalse(required - registered, "Missing preflight suites: " + repr(required - registered))

    def test_normal_packaging_dialog_to_verified_install_and_restart(self):
        baseline = self.build / "ClipFlow_V2"
        baseline.mkdir(parents=True)
        with patch.object(sys, "argv", ["package_portable.py"]), \
                patch.object(packaging, "PROJECT_ROOT", self.root), \
                patch.object(packaging, "BUILD_DIR", self.build), \
                patch.object(packaging, "PATCH_SEQUENCE_STATE_FILE", self.build / ".patch_sequence_state.json"), \
                patch.object(packaging, "find_base_dist", return_value=baseline), \
                patch.object(packaging, "_find_build_python", return_value=None), \
                patch.object(packaging, "_ensure_mcp_frontend_assets"), \
                patch.object(packaging, "_ensure_feishu_app_profile"), \
                patch.object(packaging, "_ensure_lighthouse_distribution"), \
                patch.object(packaging, "_ensure_photo_location_distribution"), \
                patch.object(packaging, "_run_packaging_preflight_tests") as preflight, \
                patch("upload_event_module.services.patch_mirror.publish_patch", return_value={"fixture": "verified"}) as upload, \
                patch("upload_event_module.services.patch_mirror.publish_manifest") as publish, \
                patch.dict(os.environ, {"QT_QPA_PLATFORM": "offscreen"}), patch.dict(sys.modules):
            # Exercise the direct CLI branch while keeping all publication I/O fake.
            sys.modules.pop("unittest", None)
            os.environ.pop("PYTEST_CURRENT_TEST", None)
            self.review_dialog(confirm=True, action=packaging.main)
            preflight.assert_called_once_with()
            upload.assert_called_once()
            publish.assert_called_once()
        manifest = journal_module.read_json(self.build / "latest_patch.json")
        self.assertEqual(self.journal.state()["baseline"], self.git("rev-parse", "HEAD"))
        self.assertEqual(self.journal.state()["entries"][0]["items"][0]["problem"], "fixture reviewed problem")
        app = self.root / "normal-client"
        data = app / "bin/data"
        updater = RemotePatchUpdater(app, data / "remote_patch", "")
        with patch.object(updater, "_download_zip", return_value=self.build / manifest["zip_name"]):
            staged = updater.prepare_remote_patch(manifest)
        self.run_installer(app, staged)
        self.assertEqual(ReleaseJournalStore(data).list()["total"], 0)
        RemotePatchUpdater(app, data / "remote_patch", "")
        self.assertEqual(ReleaseJournalStore(data).entries(), self.journal.state()["entries"])

    def test_sensitive_text_is_redacted_and_only_public_fields_leave_build_state(self):
        notes = [{"problem": "secret=fixturePrivateValue person@example.invalid 13800138000 身份证00000020000101000X", "solution": "Bearer fixtureBearerValue https://example.invalid/?token=value", "module": "cli_fixturePrivate"}]
        draft = self.confirmed_draft()
        draft["items"] = notes
        draft = self.journal.save_draft(draft, confirmed=True)
        bundle = self.journal.bundle(draft, "ClipFlow_fixture", 1, publish=True)
        text = journal_module.encode(bundle).decode("utf-8")
        for value in ("fixturePrivateValue", "person@example.invalid", "13800138000", "00000020000101000X", "fixtureBearerValue", "cli_fixturePrivate", "https://example.invalid"):
            self.assertNotIn(value, text)
        entry = bundle["entries"][-1]
        self.assertEqual(set(entry), {"id", "version", "patch", "published_at", "commits", "items"})
        self.assertNotIn("files", entry)
        self.assertNotIn("author", text)

    def test_only_verified_installed_publication_imports_and_preserves_user_data(self):
        directory, archive, path, manifest = self.package()
        data = self.root / "user-data"
        data.mkdir()
        preserved = data / "business.txt"
        preserved.write_text("fixture business history", encoding="utf-8")
        store = ReleaseJournalStore(data)
        self.assertFalse(store.import_published(directory, manifest))
        self.assertFalse(store.path.exists())
        self.publish(archive, path, manifest)
        journal_module.write_json(directory / "bin/build_meta.json", {
            "build_id": manifest["target_version"], "patch_version": manifest["target_patch_version"],
        })
        self.assertTrue(store.import_published(directory, manifest))
        self.assertTrue(store.import_published(directory, manifest))
        self.assertEqual(store.list()["total"], 1)
        self.assertEqual(preserved.read_text(encoding="utf-8"), "fixture business history")
        content = (directory / journal_module.BUNDLE_PATH).read_bytes()
        (directory / journal_module.BUNDLE_PATH).write_bytes(content + b" ")
        with self.assertRaises(ValueError):
            store.import_published(directory, manifest)
        self.assertEqual(store.list()["total"], 1)

    def test_archive_record_is_bound_to_version_patch_and_installed_hash(self):
        directory, archive, path, manifest = self.package()
        self.publish(archive, path, manifest)
        store = ReleaseJournalStore(self.root / "runtime")
        for field, value in (("target_version", "wrong"), ("target_patch_version", 88), ("release_journal", {})):
            candidate = dict(manifest, **{field: value})
            if field == "release_journal":
                self.assertFalse(store.import_published(directory, candidate))
            else:
                with self.assertRaises(ValueError):
                    store.import_published(directory, candidate)
        journal_module.write_json(directory / "bin/build_meta.json", {
            "build_id": manifest["target_version"], "patch_version": -1,
        })
        with self.assertRaises(ValueError):
            store.import_published(directory, manifest)
        self.assertFalse(store.path.exists())

    def run_installer(self, app_root, patch_dir, *, fail=False):
        """Run the actual installer methods without Qt, dependency installs or alerts."""
        source = BIN / "upload_event_module/ui/main_window_patch.py"
        tree = ast.parse(source.read_text(encoding="utf-8-sig"))
        original = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "PatchUpdateMixin")
        methods = {"_apply_patch_worker", "_collect_patch_files", "_parse_deleted_files", "_is_runtime_data_patch_path",
                   "_copy_with_retry", "_delete_with_retry", "_backup_patch_targets", "_rollback_patch", "_sha256_file",
                   "_update_build_meta", "_read_local_build_meta", "_read_json_file", "_delete_patch_dir"}
        selected = [node for node in original.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in methods]
        module = ast.Module(body=[ast.ClassDef(name="Installer", bases=[], keywords=[], body=selected, decorator_list=[])], type_ignores=[])
        noop = lambda *a, **kw: None
        namespace = dict(Path=Path, json=json, os=os, shutil=shutil, tempfile=tempfile, time=time, hashlib=hashlib,
                         config=SimpleNamespace(auto_install_dependencies=False), FRONTEND_INDEX=packaging.FRONTEND_INDEX,
                         patch_deletions=patch_deletions, RUNTIME_PATCH_DATA_SUFFIXES=(".db", ".sqlite", ".sqlite3"),
                         log_error=noop, log_warning=noop, log_info=noop)
        exec(compile(ast.fix_missing_locations(module), str(source), "exec"), namespace)
        installer = namespace["Installer"]()
        installer._get_app_root_dir = lambda: app_root
        installer._last_patch_meta = journal_module.read_json(patch_dir / "bin/patch_meta.json")
        installer._last_patch_source = "remote_ui"
        finished = []
        installer.patch_update_finished = SimpleNamespace(emit=lambda *args: finished.append(args))
        installer._discard_invalid_patch = lambda directory, reason: finished.append((False, reason))
        guard = SimpleNamespace(pause=noop, finish=noop)
        if fail:
            installer._copy_with_retry = lambda *a, **kw: False
        with patch.dict(sys.modules, {"openclaw_service.update": SimpleNamespace(AssistantUpdateGuard=lambda *a: guard)}), \
                patch("upload_event_module.services.process_lifetime.lower_current_thread_priority"):
            installer._apply_patch_worker(patch_dir)
        self.assertTrue(finished)
        self.assertEqual(finished[-1][0], not fail)
        return installer

    def test_new_updater_persists_receipt_but_imports_only_after_verified_install_and_restart(self):
        _, archive, path, manifest = self.package()
        self.publish(archive, path, manifest)
        app = self.root / "client"
        data = app / "bin/data"
        updater = RemotePatchUpdater(app, data / "remote_patch", "")
        store = ReleaseJournalStore(data)
        with patch.object(updater, "_download_zip", return_value=archive):
            staged = updater.prepare_remote_patch(manifest)
        pending = updater.data_dir / "release_journal_pending.json"
        self.assertTrue(pending.is_file())
        self.assertFalse(updater.complete_release_journal())
        self.assertEqual(store.list()["total"], 0)
        self.run_installer(app, staged)
        self.assertFalse(staged.exists(), "the installer removes the patch folder before restart")
        self.assertFalse((app / "bin/patch_meta.json").exists(), "patch_meta is intentionally not installed")
        self.assertEqual(store.list()["total"], 0)
        RemotePatchUpdater(app, data / "remote_patch", "")
        self.assertEqual(store.list()["total"], 1)
        self.assertFalse(pending.exists())
        before = store.path.stat().st_mtime_ns
        RemotePatchUpdater(app, data / "remote_patch", "")
        self.assertEqual(store.path.stat().st_mtime_ns, before)

    def test_failed_install_keeps_history_empty_and_pending_receipt_for_retry(self):
        _, archive, path, manifest = self.package()
        self.publish(archive, path, manifest)
        app = self.root / "client"
        data = app / "bin/data"
        updater = RemotePatchUpdater(app, data / "remote_patch", "")
        with patch.object(updater, "_download_zip", return_value=archive):
            staged = updater.prepare_remote_patch(manifest)
        self.run_installer(app, staged, fail=True)
        RemotePatchUpdater(app, data / "remote_patch", "")
        self.assertEqual(ReleaseJournalStore(data).list()["total"], 0)
        self.assertTrue((updater.data_dir / "release_journal_pending.json").exists())
        self.run_installer(app, staged)
        RemotePatchUpdater(app, data / "remote_patch", "")
        self.assertEqual(ReleaseJournalStore(data).list()["total"], 1)

    def test_old_updater_bootstraps_after_restart_in_fresh_process_via_normal_verified_check(self):
        for relative in (
            "upload_event_module/__init__.py", "upload_event_module/services/__init__.py",
            "upload_event_module/services/remote_patch_updater.py", "upload_event_module/services/release_journal.py",
            "lan_bitable_template_portal/__init__.py", "lan_bitable_template_portal/release_journal.py",
        ):
            target = self.root / "bin" / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(BIN / relative, target)
        _, archive, path, manifest = self.package()
        self.publish(archive, path, manifest)
        app = self.root / "legacy-client"
        data = app / "bin/data"
        updater = RemotePatchUpdater(app, data / "remote_patch", "")
        # Old code extracted and installed the ZIP, without the new pending-receipt hook.
        staged = updater._extract_patch_dir(archive)
        self.run_installer(app, staged)
        self.assertFalse((updater.data_dir / "release_journal_pending.json").exists())
        proof = data / "verified-fixture-manifest.json"
        journal_module.write_json(proof, manifest)
        script = """
import json, pathlib, socket, sys, types
sys.path.insert(0, sys.argv[1])
from upload_event_module.services.remote_patch_updater import RemotePatchUpdater
from lan_bitable_template_portal.release_journal import ReleaseJournalStore
app, data = pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[2]) / 'bin/data'
assert pathlib.Path(sys.modules[RemotePatchUpdater.__module__].__file__).is_relative_to(app)
assert pathlib.Path(sys.modules[ReleaseJournalStore.__module__].__file__).is_relative_to(app)
manifest = json.loads(pathlib.Path(sys.argv[3]).read_text(encoding='utf-8'))
def denied(*args): raise AssertionError('fixture forbids real network')
socket.socket.connect = denied
calls = []
def verified_fetch(**kwargs): calls.append(kwargs); return manifest
sys.modules['upload_event_module.services.patch_mirror'] = types.SimpleNamespace(fetch_manifest=verified_fetch)
updater = RemotePatchUpdater(app, data / 'remote_patch', '')
assert ReleaseJournalStore(data).list()['total'] == 0
updater.fetch_manifest()
assert ReleaseJournalStore(data).list()['total'] == 1
assert len(calls) == 1
updater.fetch_manifest()
assert ReleaseJournalStore(data).list()['total'] == 1
print('legacy restart history imported without replay or extra cloud requests')
"""
        result = subprocess.run([sys.executable, "-B", "-c", script, str(app / "bin"), str(app), str(proof)],
                                capture_output=True, text=True, encoding="utf-8", timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr[-2000:])
        self.assertIn("legacy restart history imported", result.stdout)
        self.assertEqual(ReleaseJournalStore(data).list()["total"], 1)

    def test_same_version_check_refuses_tampered_bundle_and_retries_without_losing_history(self):
        directory, archive, path, manifest = self.package()
        self.publish(archive, path, manifest)
        app = self.root / "client"
        data = app / "bin/data"
        updater = RemotePatchUpdater(app, data / "remote_patch", "")
        staged = updater._extract_patch_dir(archive)
        self.run_installer(app, staged)
        bundle = app / journal_module.BUNDLE_PATH
        original = bundle.read_bytes()
        bundle.write_bytes(original + b" ")
        with patch("upload_event_module.services.patch_mirror.fetch_manifest", return_value=manifest) as fetch:
            updater.fetch_manifest()
            self.assertEqual(ReleaseJournalStore(data).list()["total"], 0)
            bundle.write_bytes(original)
            updater.fetch_manifest()
            self.assertEqual(ReleaseJournalStore(data).list()["total"], 1)
            self.assertEqual(fetch.call_count, 2)

    def review_dialog(self, *, confirm, invalid_first=False, action=None):
        from PyQt6.QtCore import QTimer
        from PyQt6.QtWidgets import QApplication, QDialogButtonBox, QTableWidget
        from upload_event_module.ui.release_notes_dialog import review_release_notes

        app = QApplication.instance() or QApplication([])
        errors = []

        def interact():
            dialog = app.activeModalWidget()
            try:
                self.assertIsNotNone(dialog)
                table = dialog.findChild(QTableWidget)
                self.assertEqual(table.columnCount(), 3)
                self.assertEqual([table.horizontalHeaderItem(i).text() for i in range(3)], ["问题", "解决方案", "模块"])
                if invalid_first:
                    with patch("PyQt6.QtWidgets.QMessageBox.warning") as warning:
                        dialog.findChild(QDialogButtonBox).button(QDialogButtonBox.StandardButton.Ok).click()
                        warning.assert_called_once()
                        self.assertTrue(dialog.isVisible())
                for column, value in enumerate(("fixture reviewed problem", "fixture reviewed solution" if confirm else "", "transport")):
                    table.item(0, column).setText(value)
                if confirm:
                    dialog.findChild(QDialogButtonBox).button(QDialogButtonBox.StandardButton.Ok).click()
                else:
                    dialog.reject()
            except BaseException as error:
                errors.append(error)
                if dialog is not None:
                    dialog.reject()

        QTimer.singleShot(0, interact)
        result = action() if action is not None else review_release_notes(self.journal, self.journal.prepare_draft())
        self.assertEqual(errors, [])
        return result

    def test_offscreen_qt_dialog_confirms_reviewed_items(self):
        with patch.dict(os.environ, {"QT_QPA_PLATFORM": "offscreen"}):
            result = self.review_dialog(confirm=True)
        self.assertTrue(result["confirmed"])
        self.assertEqual(result["items"][0], {"problem": "fixture reviewed problem", "solution": "fixture reviewed solution", "module": "transport"})

    def test_offscreen_qt_cancel_preserves_incomplete_draft(self):
        with patch.dict(os.environ, {"QT_QPA_PLATFORM": "offscreen"}):
            self.assertIsNone(self.review_dialog(confirm=False))
        draft = journal_module.read_json(self.journal.draft_path)
        self.assertFalse(draft["confirmed"])
        self.assertEqual(draft["items"][0]["problem"], "fixture reviewed problem")
        self.assertEqual(draft["items"][0]["solution"], "")

    def test_packaging_dialog_handles_validation_errors_without_leaking_exceptions(self):
        self.journal = packaging.release_journal.BuildReleaseJournal(self.root, self.build)
        with patch.dict(os.environ, {"QT_QPA_PLATFORM": "offscreen"}):
            result = self.review_dialog(confirm=True, invalid_first=True)
        self.assertTrue(result["confirmed"])


if __name__ == "__main__":
    unittest.main()

import ast
import asyncio
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
from unittest.mock import MagicMock, patch
import zipfile

from starlette.requests import Request

BIN = Path(__file__).resolve().parent
sys.path.insert(0, str(BIN))
from frontend_assets import FRONTEND_DIST, FRONTEND_INDEX, patch_deletions, referenced_assets
import package_portable as portable_packaging
from upload_event_module.services.remote_patch_updater import RemotePatchUpdater


def isolated_class(path, class_name, methods, namespace):
    tree = ast.parse(path.read_text(encoding="utf-8-sig"))
    original = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == class_name)
    selected = [node for node in original.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in methods]
    module = ast.Module(body=[ast.ClassDef(name=class_name, bases=[], keywords=[], body=selected, decorator_list=[])], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(path), "exec"), namespace)
    return namespace[class_name]


class TransportSafetyTests(unittest.TestCase):
    def test_sparse_patch_upload_and_existing_archive_validation(self):
        def git(*args, cwd=None):
            return subprocess.run(
                ["git", *map(str, args)], cwd=cwd, check=True,
                capture_output=True, text=True,
            ).stdout

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            remote, source, build = (root / name for name in ("remote.git", "source", "build"))
            git("init", "--bare", "--initial-branch=master", remote)
            git("init", "-b", "master", source)
            git("-C", source, "config", "user.name", "Package Test")
            git("-C", source, "config", "user.email", "package@example.invalid")
            patches = source / "updates" / "patches"
            patches.mkdir(parents=True)
            for stamp in ("100000", "110000", "120000"):
                (patches / f"ClipFlow_V2_20260917_{stamp}_patch_only.zip").write_bytes(b"old")
            (patches / "ClipFlow_patch_only.zip").write_bytes(b"legacy")
            (source / "updates" / "latest_patch.json").write_text("old", encoding="utf-8")
            git("-C", source, "add", ".")
            git("-C", source, "commit", "-m", "initial")
            git("-C", source, "remote", "add", "origin", remote)
            git("-C", source, "push", "origin", "master")

            build.mkdir()
            name = "ClipFlow_V2_20260918_153651_patch_only.zip"
            archive = build / name
            archive.write_bytes(b"new-patch")
            manifest = {
                "target_version": "ClipFlow_V2_20260918_153651",
                "target_patch_version": 381,
                "zip_name": name,
                "zip_size": archive.stat().st_size,
                "zip_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
            }
            manifest_path = build / "latest_patch.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with patch.object(portable_packaging, "BUILD_DIR", build), patch.dict(os.environ, {
                "GIT_AUTHOR_NAME": "Package Test", "GIT_AUTHOR_EMAIL": "package@example.invalid",
                "GIT_COMMITTER_NAME": "Package Test", "GIT_COMMITTER_EMAIL": "package@example.invalid",
            }):
                self.assertEqual(portable_packaging._load_existing_patch()[0], archive)
                self.assertTrue(portable_packaging._upload_patch_to_gitee(
                    archive, manifest_path, repo_url=remote.as_uri(), branch="master",
                    subdir="updates/patches", manifest_repo_path="updates/latest_patch.json",
                ))
                self.assertFalse(list(build.glob(".gitee_upload_*")))
                archive.write_bytes(b"tampered")
                with self.assertRaisesRegex(RuntimeError, "ZIP 与清单不一致"):
                    portable_packaging._load_existing_patch()

            tracked = git("--git-dir", remote, "ls-tree", "-r", "--name-only", "master")
            self.assertIn(name, tracked)
            self.assertNotIn("ClipFlow_V2_20260917_100000_patch_only.zip", tracked)
            self.assertIn("ClipFlow_patch_only.zip", tracked)
            self.assertEqual(git("--git-dir", remote, "show", "master:updates/latest_patch.json"), json.dumps(manifest))

    def test_upload_pushes_when_commit_reports_failure_after_creating_commit(self):
        def git(*args, cwd=None):
            return subprocess.run(
                ["git", *map(str, args)], cwd=cwd, check=True,
                capture_output=True, text=True,
            ).stdout

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            remote, source, build = (root / name for name in ("remote.git", "source", "build"))
            git("init", "--bare", "--initial-branch=master", remote)
            git("init", "-b", "master", source)
            git("-C", source, "config", "user.name", "Package Test")
            git("-C", source, "config", "user.email", "package@example.invalid")
            (source / "updates" / "patches").mkdir(parents=True)
            (source / "updates" / "latest_patch.json").write_text("{}", encoding="utf-8")
            git("-C", source, "add", ".")
            git("-C", source, "commit", "-m", "initial")
            git("-C", source, "remote", "add", "origin", remote)
            git("-C", source, "push", "origin", "master")

            build.mkdir()
            name = "ClipFlow_V2_20260923_171740_patch_only.zip"
            archive = build / name
            archive.write_bytes(b"patch")
            manifest = {
                "target_version": "ClipFlow_V2_20260923_171740",
                "target_patch_version": 435,
                "zip_name": name,
                "zip_size": archive.stat().st_size,
                "zip_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
            }
            manifest_path = build / "latest_patch.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            original_run = portable_packaging._run_cmd

            def commit_succeeds_but_reports_failure(args, **kwargs):
                result = original_run(args, **kwargs)
                return False if args[:2] == ["git", "commit"] and result else result

            with patch.object(portable_packaging, "BUILD_DIR", build), patch.object(
                portable_packaging, "_run_cmd", side_effect=commit_succeeds_but_reports_failure
            ), patch.dict(os.environ, {
                "GIT_AUTHOR_NAME": "Package Test", "GIT_AUTHOR_EMAIL": "package@example.invalid",
                "GIT_COMMITTER_NAME": "Package Test", "GIT_COMMITTER_EMAIL": "package@example.invalid",
            }):
                self.assertTrue(portable_packaging._upload_patch_to_gitee(
                    archive, manifest_path, repo_url=remote.as_uri(), branch="master",
                    subdir="updates/patches", manifest_repo_path="updates/latest_patch.json",
                ))

            self.assertEqual(
                json.loads(git("--git-dir", remote, "show", "master:updates/latest_patch.json")),
                manifest,
            )

    def test_packaging_checks_published_manifest_and_zip_hash(self):
        content = b"patch-content"
        manifest = {
            "target_patch_version": 378,
            "zip_name": "ClipFlow_V2_test_patch_only.zip",
            "zip_url": "https://example.invalid/patch.zip",
            "zip_sha256": hashlib.sha256(content).hexdigest(),
            "zip_size": len(content),
        }
        remote_manifest = MagicMock()
        remote_manifest.__enter__.return_value = remote_manifest
        remote_manifest.read.return_value = json.dumps(manifest).encode()
        archive = MagicMock()
        archive.__enter__.return_value = archive
        archive.read.side_effect = [content, b""]
        with patch("urllib.request.urlopen", side_effect=[remote_manifest, archive]) as get:
            portable_packaging._verify_published_patch(
                manifest, repo_url="https://example.invalid/repo.git",
                branch="master", manifest_path="updates/latest_patch.json"
            )
        self.assertEqual(get.call_count, 2)
        wrong_archive = MagicMock()
        wrong_archive.__enter__.return_value = wrong_archive
        def wrong_get(request, **_kwargs):
            if request.full_url == manifest["zip_url"]:
                wrong_archive.read.side_effect = [b"wrong-content", b""]
                return wrong_archive
            return remote_manifest
        with patch("urllib.request.urlopen", side_effect=wrong_get), patch("time.sleep"):
            with self.assertRaisesRegex(RuntimeError, "下载核验失败"):
                portable_packaging._verify_published_patch(
                    manifest, repo_url="https://example.invalid/repo.git",
                    branch="master", manifest_path="updates/latest_patch.json"
                )

    def test_cabinet_templates_are_always_included_in_patch(self):
        root = Path("bin/lan_bitable_template_portal/templates/cabinet_power")
        for scope in "ABCDE":
            self.assertTrue(portable_packaging._should_force_include_in_patch(root / f"{scope}.xlsm"))
            self.assertTrue(portable_packaging._should_force_include_in_patch(root / f"{scope}.layouts.json.gz"))
        self.assertTrue(portable_packaging._should_force_include_in_patch(root / "layouts.json.gz"))

    def test_manifest_fetch_bypasses_mutable_url_cache(self):
        updater = RemotePatchUpdater(
            Path.cwd(), Path(tempfile.gettempdir()), "https://example.invalid/latest.json"
        )
        response = MagicMock()
        response.json.return_value = {"version": "next"}
        with patch("requests.get", return_value=response) as get:
            self.assertEqual(updater.fetch_manifest(), {"version": "next"})
        kwargs = get.call_args.kwargs
        self.assertTrue(kwargs["params"]["_clipflow"])
        self.assertEqual(kwargs["headers"]["Cache-Control"], "no-cache")

    def test_streamed_body_limit_and_json_validation(self):
        controller = isolated_class(BIN / "clipflow_backend/main.py", "FastAPIPortalController", {"_read_bounded_body", "_read_json_request"}, {"Request": Request, "json": json, "MAX_JSON_BODY_BYTES": 8})

        async def run():
            for header in [[], [(b"content-length", b"1")]]:
                chunks = [b"1234", b"56789", b"never read"]
                calls = []

                async def receive():
                    calls.append(1)
                    return {"type": "http.request", "body": chunks.pop(0), "more_body": bool(chunks)}

                request = Request({"type": "http", "headers": header}, receive)
                with self.assertRaisesRegex(ValueError, "请求体过大"):
                    await controller._read_bounded_body(request, 8)
                self.assertEqual(len(calls), 2)
            for content, valid in [(b'{"a":1}', True), (b'[]', False), (b'{', False)]:
                async def receive():
                    return {"type": "http.request", "body": content, "more_body": False}
                request = Request({"type": "http", "headers": []}, receive)
                if valid:
                    self.assertEqual(await controller._read_json_request(request, max_bytes=len(content)), {"a": 1})
                else:
                    with self.assertRaises(ValueError):
                        await controller._read_json_request(request)
            for length in [b"-1", b"bad", b"9"]:
                request = Request({"type": "http", "headers": [(b"content-length", length)]})
                with self.assertRaises(ValueError):
                    await controller._read_bounded_body(request, 8)
        asyncio.run(run())

    def test_download_rejects_unsafe_names_before_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            updater = RemotePatchUpdater(Path(tmp), Path(tmp) / "data", "")
            with patch("requests.get") as get:
                for name in ["../a.zip", "a/b.zip", "a\\b.zip", "C:\\a.zip", "//server/a", "a:stream", "CON.zip", "LPT1.zip", "a.", "a ", ".."]:
                    with self.subTest(name=name), self.assertRaisesRegex(RuntimeError, "unsafe"):
                        updater._download_zip({"zip_url": "https://example.invalid/a", "zip_name": name})
                get.assert_not_called()

    def test_download_atomic_failure_preserves_previous_archive(self):
        with tempfile.TemporaryDirectory() as tmp:
            updater = RemotePatchUpdater(Path(tmp), Path(tmp) / "data", "")
            target = updater.data_dir / "patch.zip"
            target.write_bytes(b"previous")
            response = MagicMock()
            response.__enter__.return_value = response
            manifest = {"zip_url": "https://example.invalid/patch.zip?version=2", "zip_sha256": hashlib.sha256(b"new").hexdigest(), "zip_size": 3}
            for chunks in [[b"bad"], [b"too long"], [b"n"]]:
                response.iter_content.return_value = iter(chunks)
                with patch("requests.get", return_value=response), self.assertRaises(RuntimeError):
                    updater._download_zip(manifest)
                self.assertEqual(target.read_bytes(), b"previous")
                self.assertEqual(list(updater.data_dir.glob("*.part")), [])
            def interrupted():
                yield b"n"
                raise OSError("disconnected")
            response.iter_content.return_value = interrupted()
            with patch("requests.get", return_value=response), self.assertRaises(OSError):
                updater._download_zip(manifest)
            self.assertEqual(target.read_bytes(), b"previous")
            response.iter_content.return_value = iter([b"n", b"ew"])
            with patch("requests.get", return_value=response):
                self.assertEqual(updater._download_zip(manifest), target.resolve())
            self.assertEqual(target.read_bytes(), b"new")
            self.assertEqual(list(updater.data_dir.glob("*.part")), [])

    def test_frontend_generations_and_patch_rollback(self):
        methods = {"_apply_patch_worker", "_collect_patch_files", "_parse_deleted_files", "_is_runtime_data_patch_path", "_copy_with_retry", "_delete_with_retry", "_backup_patch_targets", "_rollback_patch", "_sha256_file"}
        namespace = dict(Path=Path, json=json, os=os, shutil=shutil, tempfile=tempfile, time=time, hashlib=hashlib, config=SimpleNamespace(auto_install_dependencies=False), FRONTEND_INDEX=FRONTEND_INDEX, patch_deletions=patch_deletions, RUNTIME_PATCH_DATA_SUFFIXES=(), log_error=lambda *_: None, log_warning=lambda *_: None)
        installer = isolated_class(BIN / "upload_event_module/ui/main_window_patch.py", "PatchUpdateMixin", methods, namespace)
        for failure in [None, "new.js", "index.html", "oldest.js"]:
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp:
                root, overlay = Path(tmp) / "app", Path(tmp) / "patch"
                assets = root / FRONTEND_DIST / "assets"
                assets.mkdir(parents=True)
                (assets / "old.js").write_text('import "./shared.js"', encoding="utf-8")
                (assets / "shared.js").write_text("// shared", encoding="utf-8")
                (assets / "oldest.js").write_text("// oldest", encoding="utf-8")
                (root / FRONTEND_INDEX).write_text('<script src="/assets/old.js"></script>', encoding="utf-8")
                new_assets = overlay / FRONTEND_DIST / "assets"
                new_assets.mkdir(parents=True)
                (new_assets / "new.js").write_text('import "./shared.js"', encoding="utf-8")
                (overlay / FRONTEND_INDEX).write_text('<script src="/assets/new.js"></script>', encoding="utf-8")
                plan = patch_deletions(root, overlay, [FRONTEND_DIST / "assets/old.js"])
                self.assertEqual(plan, [FRONTEND_DIST / "assets/oldest.js"])
                instance = installer()
                instance._last_patch_meta = {}
                instance._last_patch_source = "local"
                instance._get_app_root_dir = lambda: root
                instance._update_build_meta = lambda *_: None
                instance._delete_patch_dir = lambda *_: ""
                results = []
                instance.patch_update_finished = SimpleNamespace(emit=lambda *args: results.append(args))
                original_copy = instance._copy_with_retry
                original_delete = instance._delete_with_retry
                def copy(src, dest):
                    if src.is_relative_to(overlay) and src.name == failure:
                        return False
                    if src == overlay / FRONTEND_INDEX:
                        self.assertTrue((assets / "new.js").is_file())
                        self.assertIn("old.js", (root / FRONTEND_INDEX).read_text())
                    return original_copy(src, dest)
                instance._copy_with_retry = copy
                instance._delete_with_retry = lambda target: False if target.name == failure else original_delete(target)
                instance._apply_patch_worker(overlay)
                self.assertTrue(results, results)
                self.assertEqual(results[-1][0], failure is None, results)
                self.assertTrue((assets / "old.js").is_file())
                self.assertTrue((assets / "shared.js").is_file())
                if failure:
                    self.assertIn("old.js", (root / FRONTEND_INDEX).read_text())
                    self.assertFalse((assets / "new.js").exists())
                    self.assertTrue((assets / "oldest.js").exists())
                else:
                    self.assertIn("new.js", (root / FRONTEND_INDEX).read_text())
                    self.assertFalse((assets / "oldest.js").exists())
                    self.assertEqual(len(referenced_assets(root)), 2)
                    newer = Path(tmp) / "newer"
                    (newer / FRONTEND_DIST / "assets").mkdir(parents=True)
                    (newer / FRONTEND_INDEX).write_text('<script src="/assets/newer.js"></script>', encoding="utf-8")
                    (newer / FRONTEND_DIST / "assets/newer.js").write_text("// next", encoding="utf-8")
                    instance._apply_patch_worker(newer)
                    self.assertTrue(results[-1][0], results)
                    self.assertFalse((assets / "old.js").exists())
                    self.assertTrue((assets / "new.js").exists())
                    self.assertTrue((assets / "shared.js").exists())

    def test_patch_extraction_uses_short_staging_and_cleans_failed_archives(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            app = root / "app"
            updater = RemotePatchUpdater(app, app / "bin/data/remote_patch", "")
            name = "ClipFlow_V2_20260923_082137_patch_only"
            rel = "bin/lan_bitable_template_portal/frontend/dist/assets/VnetBackButton.vue_vue_type_script_setup_true_lang-HbkUmn7D.js"
            archive = updater.data_dir / f"{name}.zip"
            with zipfile.ZipFile(archive, "w") as zf:
                zf.writestr(f"{name}/{rel}", "// complete")
            extracted_paths = []
            extract = zipfile.ZipFile.extract

            def bounded_extract(zf, info, path, *args, **kwargs):
                target = Path(path) / info.filename
                extracted_paths.append(target)
                if len(str(target).encode("utf-16-le")) // 2 >= 260:
                    raise FileNotFoundError(2, "No such file or directory", str(target))
                return extract(zf, info, path, *args, **kwargs)

            with patch.object(zipfile.ZipFile, "extract", bounded_extract):
                result = updater._extract_patch_dir(archive)
            self.assertEqual((result / rel).read_text(), "// complete")
            self.assertFalse(any(path.is_relative_to(updater.data_dir) for path in extracted_paths))
            self.assertEqual(list(app.glob(".patch-*")), [])
            with zipfile.ZipFile(archive, "w") as zf:
                zf.writestr(f"{name}/{rel}", "// partial")
                zf.writestr("../outside.txt", "unsafe")
            with self.assertRaisesRegex(RuntimeError, "unsafe path"):
                updater._extract_patch_dir(archive)
            self.assertEqual((result / rel).read_text(), "// complete")
            self.assertEqual(list(app.glob(".patch-*")), [])

    def test_patch_extraction_rejects_ambiguous_roots(self):
        with tempfile.TemporaryDirectory() as tmp:
            app = Path(tmp) / "app"
            updater = RemotePatchUpdater(app, Path(tmp) / "data", "")
            archive = Path(tmp) / "ambiguous.zip"
            with zipfile.ZipFile(archive, "w") as zf:
                zf.writestr("a_patch_only/bin/app.py", "a")
                zf.writestr("b_patch_only/bin/app.py", "b")
            with self.assertRaisesRegex(RuntimeError, "multiple"):
                updater._extract_patch_dir(archive)
            self.assertEqual(list(app.iterdir()), [])

    def test_packaging_rejects_legacy_long_paths_without_deleting_previous_zip(self):
        with tempfile.TemporaryDirectory() as tmp:
            build = Path(tmp)
            previous = build / "previous_patch_only.zip"
            previous.write_bytes(b"previous archive")
            patch_dir = build / "ClipFlow_V2_20260923_082137_patch_only"
            bad = patch_dir / FRONTEND_DIST / "assets/VnetBackButton.vue_vue_type_script_setup_true_lang-HbkUmn7D.js"
            bad.parent.mkdir(parents=True)
            bad.write_text("// too long", encoding="utf-8")
            with patch.object(portable_packaging, "BUILD_DIR", build):
                with self.assertRaisesRegex(RuntimeError, "补丁路径过长"):
                    portable_packaging._zip_patch_dir(patch_dir)
                self.assertEqual(previous.read_bytes(), b"previous archive")
                bad.rename(bad.with_name("c-HbkUmn7D.js"))
                archive = portable_packaging._zip_patch_dir(patch_dir)
            with zipfile.ZipFile(archive) as zf:
                prefix = ("C:/Users/HP/Desktop/ClipFlow_portable_20260204_125611/"
                          f"bin/data/remote_patch/.extract_{archive.stem}/")
                self.assertTrue(all(len(prefix + path) < 260 for path in zf.namelist()))

    def test_frontend_missing_new_asset_rejected_without_removing_old_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, overlay = Path(tmp) / "app", Path(tmp) / "patch"
            (root / FRONTEND_DIST / "assets").mkdir(parents=True)
            (root / FRONTEND_INDEX).write_text('<script src="/assets/old.js"></script>', encoding="utf-8")
            old = root / FRONTEND_DIST / "assets/old.js"
            old.write_text("// old", encoding="utf-8")
            (overlay / FRONTEND_DIST).mkdir(parents=True)
            (overlay / FRONTEND_INDEX).write_text('<script src="/assets/missing.js"></script>', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Missing frontend asset"):
                patch_deletions(root, overlay, [old.relative_to(root)])
            self.assertEqual(old.read_text(), "// old")

    def test_invalid_patch_folder_is_removed_instead_of_retried_forever(self):
        installer = isolated_class(
            BIN / "upload_event_module/ui/main_window_patch.py",
            "PatchUpdateMixin",
            {"_delete_patch_dir", "_discard_invalid_patch"},
            {"Path": Path, "shutil": shutil, "time": time, "log_error": lambda *_: None},
        )
        with tempfile.TemporaryDirectory() as tmp:
            patch_dir = Path(tmp) / "broken_patch_only"
            patch_dir.mkdir()
            instance = installer()
            instance._patch_dir = patch_dir
            results = []
            instance.patch_update_finished = SimpleNamespace(emit=lambda *args: results.append(args))
            instance._discard_invalid_patch(patch_dir, "Missing frontend asset: bad.css")
            self.assertFalse(patch_dir.exists())
            self.assertIsNone(instance._patch_dir)
            self.assertEqual(results[-1][0], False)

    def test_patch_bundles_complete_frontend_generation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, patch_dir = Path(tmp) / "source", Path(tmp) / "patch"
            assets = root / FRONTEND_DIST / "assets"
            assets.mkdir(parents=True)
            (root / FRONTEND_INDEX).write_text(
                '<script src="/assets/new.js"></script><link href="/assets/new.css">'
                '<link rel="stylesheet" href="/assets/page-navigation.css">'
                '<link rel="icon" href="/assets/favicon.svg">',
                encoding="utf-8",
            )
            (assets / "new.js").write_text(
                'import "./shared.js"; const logo = "/assets/logo.png"',
                encoding="utf-8",
            )
            (assets / "shared.js").write_text("// shared", encoding="utf-8")
            (assets / "new.css").write_text("/* current */", encoding="utf-8")
            (assets / "page-navigation.css").write_text("#page-navigation{position:sticky}", encoding="utf-8")
            (assets / "favicon.svg").write_text("<svg/>", encoding="utf-8")
            (assets / "logo.png").write_bytes(b"png")
            node = shutil.which("node")
            if node:
                script = root / FRONTEND_DIST.parent / "scripts/prune-dist-assets.mjs"
                script.parent.mkdir(parents=True)
                shutil.copy2(BIN / "lan_bitable_template_portal/frontend/scripts/prune-dist-assets.mjs", script)
                (assets / "stale.js").write_text("// stale", encoding="utf-8")
                subprocess.run([node, str(script)], check=True, capture_output=True, timeout=15)
                self.assertFalse((assets / "stale.js").exists())
                self.assertTrue((assets / "shared.js").exists())
            (patch_dir / FRONTEND_INDEX).parent.mkdir(parents=True)
            shutil.copy2(root / FRONTEND_INDEX, patch_dir / FRONTEND_INDEX)

            with patch.object(portable_packaging, "PROJECT_ROOT", root):
                portable_packaging._cleanup_vue_dist_assets()
            self.assertTrue((assets / "page-navigation.css").is_file())
            self.assertEqual(portable_packaging._include_frontend_generation(root, patch_dir), 6)
            self.assertEqual(referenced_assets(patch_dir), referenced_assets(root))
            self.assertIn(FRONTEND_DIST / "assets/page-navigation.css", referenced_assets(patch_dir))

            (assets / "shared.js").unlink()
            (assets / "stale.js").write_text("// keep until validated", encoding="utf-8")
            with patch.object(portable_packaging, "PROJECT_ROOT", root), self.assertRaisesRegex(ValueError, "Missing frontend asset"):
                portable_packaging._cleanup_vue_dist_assets()
            if node:
                result = subprocess.run([node, str(script)], capture_output=True, timeout=15)
                self.assertNotEqual(result.returncode, 0)
            self.assertTrue((assets / "stale.js").exists())

    def test_distribution_filter_keeps_only_runtime_material(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            excluded = [
                ".patch-staging/ClipFlow_patch_only/bin/runtime.py",
                "bin/test_feature.py",
                "bin/tests/test_feature.py",
                "bin/tools/audit_cabinet.py",
                "bin/lan_bitable_template_portal/frontend/src/App.vue",
                "bin/lan_bitable_template_portal/frontend/package.json",
                "docs/notes.md",
                "bin/app.log",
                "bin/config.json.legacy_conflict_1",
            ]
            kept = [
                "bin/tools/mock_lan_portal_pressure.py",
                "bin/lan_bitable_template_portal/frontend/data/runtime.json",
                "bin/lan_bitable_template_portal/frontend/dist/index.html",
                "bin/lan_bitable_template_portal/templates/cabinet_power/A.xlsm",
                "bin/ca_bundle.pem",
            ]
            for rel in excluded:
                self.assertTrue(portable_packaging._is_development_only_path(root / rel, root), rel)
            for rel in kept:
                self.assertFalse(portable_packaging._is_development_only_path(root / rel, root), rel)

    def test_patch_zip_name_is_immutable_and_archive_is_valid(self):
        with tempfile.TemporaryDirectory() as tmp:
            build_dir = Path(tmp)
            patch_dir = build_dir / "ClipFlow_V2_20260914_230354_patch_only"
            payload = patch_dir / "bin/runtime.py"
            payload.parent.mkdir(parents=True)
            payload.write_text("value = 1", encoding="utf-8")
            with patch.object(portable_packaging, "BUILD_DIR", build_dir):
                archive = portable_packaging._zip_patch_dir(patch_dir)
            self.assertEqual(archive.name, f"{patch_dir.name}.zip")
            with zipfile.ZipFile(archive, "r") as zf:
                self.assertIsNone(zf.testzip())
                self.assertIn(f"{patch_dir.name}/bin/runtime.py", zf.namelist())


if __name__ == "__main__":
    unittest.main()

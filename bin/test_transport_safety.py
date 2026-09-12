import ast
import asyncio
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from starlette.requests import Request

BIN = Path(__file__).resolve().parent
sys.path.insert(0, str(BIN))
from frontend_assets import FRONTEND_DIST, FRONTEND_INDEX, patch_deletions, referenced_assets
from upload_event_module.services.remote_patch_updater import RemotePatchUpdater


def isolated_class(path, class_name, methods, namespace):
    tree = ast.parse(path.read_text(encoding="utf-8-sig"))
    original = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == class_name)
    selected = [node for node in original.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in methods]
    module = ast.Module(body=[ast.ClassDef(name=class_name, bases=[], keywords=[], body=selected, decorator_list=[])], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(path), "exec"), namespace)
    return namespace[class_name]


class TransportSafetyTests(unittest.TestCase):
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
            with patch("upload_event_module.services.remote_patch_updater.requests.get") as get:
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
                with patch("upload_event_module.services.remote_patch_updater.requests.get", return_value=response), self.assertRaises(RuntimeError):
                    updater._download_zip(manifest)
                self.assertEqual(target.read_bytes(), b"previous")
                self.assertEqual(list(updater.data_dir.glob("*.part")), [])
            def interrupted():
                yield b"n"
                raise OSError("disconnected")
            response.iter_content.return_value = interrupted()
            with patch("upload_event_module.services.remote_patch_updater.requests.get", return_value=response), self.assertRaises(OSError):
                updater._download_zip(manifest)
            self.assertEqual(target.read_bytes(), b"previous")
            response.iter_content.return_value = iter([b"n", b"ew"])
            with patch("upload_event_module.services.remote_patch_updater.requests.get", return_value=response):
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


if __name__ == "__main__":
    unittest.main()

"""Credential-free packages, generated entirely from isolated fixtures."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

BIN = Path(__file__).resolve().parent
if str(BIN) not in sys.path:
    sys.path.insert(0, str(BIN))

import package_portable as packaging


class ManualFeishuPackagingTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="manual-feishu-package-")
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.source = self.root / "source"
        self.source.mkdir()
        self.build = self.root / "build"
        (self.source / "app.py").write_text("fixture = True\n", encoding="utf-8")
        self.enterContext(patch.object(packaging, "PROJECT_ROOT", self.source))
        self.enterContext(patch.object(packaging, "BUILD_DIR", self.build))
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("Network forbidden")))

    def test_full_copy_and_patch_exclude_plaintext_and_encrypted_credentials(self):
        folder = self.source / "bin/upload_event_module/services"
        folder.mkdir(parents=True)
        for name in ("feishu_app_profile.json", "feishu_bootstrap.json"):
            (folder / name).write_text('{"secret":"fixture-must-not-ship"}', encoding="utf-8")
        data = self.source / "bin/data"
        data.mkdir()
        for name in ("config.json", "feishu_assistant.json", "lan_portal_state.sqlite3"):
            (data / name).write_text("fixture-private-runtime-data", encoding="utf-8")
        for relative in ("document_library/files/cached-attachment", "document_library/files/unfinished.part",
                         "lighthouse_openclaw/knowledge_base/originals/cached.txt"):
            cached = data / relative
            cached.parent.mkdir(parents=True, exist_ok=True)
            cached.write_text("fixture-private-runtime-data", encoding="utf-8")
        copied = self.root / "copied"
        packaging.copy_project(copied)
        self.assertTrue((copied / "app.py").exists())
        self.assertFalse(list(copied.rglob("feishu_app_profile.json")))
        self.assertFalse(list(copied.rglob("feishu_bootstrap.json")))
        for number in (1, 2):
            version = f"ClipFlow_V2_fixture_{number}"
            packaging.build_patch(self.root / "unused", None, version, target_patch_version=number)
            directory = self.build / (version + "_patch_only")
            archive = packaging._zip_patch_dir(directory)
            path, manifest = packaging._write_latest_patch_manifest(directory, archive)
            packaging._assert_no_feishu_credentials_in_archive(archive)
            with zipfile.ZipFile(archive) as source:
                for name in source.namelist():
                    self.assertNotIn(b"fixture-must-not-ship", source.read(name))
                    self.assertNotIn(b"fixture-private-runtime-data", source.read(name))
            self.assertNotIn("app_secret", path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["update_source"], "feishu")

    def test_old_credential_bundle_retry_is_rejected_before_any_cloud_operation(self):
        for name in ("feishu_app_profile.json", "feishu_bootstrap.json", "FEISHU_APP_PROFILE.JSON"):
            with self.subTest(name=name):
                archive = self.root / "old.zip"
                with zipfile.ZipFile(archive, "w") as target:
                    target.writestr("old/bin/upload_event_module/services/" + name, "fixture-private")
                with patch("upload_event_module.services.patch_mirror.publish_patch") as upload, \
                        patch("upload_event_module.services.patch_mirror.publish_manifest") as publish:
                    with self.assertRaisesRegex(RuntimeError, "重新运行"):
                        packaging._publish_patch(archive, self.root / "latest_patch.json", {})
                upload.assert_not_called()
                publish.assert_not_called()

    def test_identity_preflight_does_not_export_or_wrap_credentials(self):
        with patch.object(packaging.subprocess, "run") as run:
            packaging._ensure_feishu_identity_manifest("fixture-python")
        script = run.call_args.args[0][-1]
        self.assertIn("load_identity_manifest", script)
        for forbidden in ("prepare_deployment_profile", "payload_from_qt", "app_secret", "bootstrap"):
            self.assertNotIn(forbidden, script)

    def test_valid_credential_free_patch_preserves_standard_publication(self):
        version = "ClipFlow_V2_fixture_1"
        packaging.build_patch(self.root / "unused", None, version, target_patch_version=1)
        directory = self.build / (version + "_patch_only")
        archive = packaging._zip_patch_dir(directory)
        path, manifest = packaging._write_latest_patch_manifest(directory, archive)
        with patch("upload_event_module.services.patch_mirror.publish_patch", return_value={"fixture": "verified"}), \
                patch("upload_event_module.services.patch_mirror.publish_manifest") as publish:
            packaging._publish_patch(archive, path, manifest)
        publish.assert_called_once_with(manifest)
        self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["feishu_patch"], {"fixture": "verified"})


if __name__ == "__main__":
    unittest.main()

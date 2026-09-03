from __future__ import annotations

import io
import os
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

os.environ.setdefault("CLIPFLOW_MOCK_EXTERNAL", "1")

from clipflow_backend.main import FastAPIPortalController, PortalRuntime  # noqa: E402
from lan_bitable_template_portal.portal_auth import AUTH_COOKIE_NAME  # noqa: E402
from lan_bitable_template_portal.portal_service import (  # noqa: E402
    DRILL_ARCHIVE_SIMPLE_UPLOAD_MAX_BYTES,
    MaintenancePortalService,
    PortalConflictError,
    PortalError,
)


class DrillBackendIntegrationTests(unittest.TestCase):
    def test_http_routes_filter_drafts_and_create_empty_execution(self):
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        definitions = [
            {
                "drill_id": "draft-drill",
                "status": "draft",
                "source": {"path": r"D:\private\draft.xlsx", "name": "draft.xlsx"},
            },
            {
                "drill_id": "published-drill",
                "status": "published",
                "source": {
                    "path": r"D:\private\published.xlsx",
                    "name": "published.xlsx",
                },
            },
        ]
        drills = Mock()
        drills.list_definitions.return_value = definitions
        drills.pending_counts.return_value = {"A": 1, "E": 1}
        drills.get_definition.side_effect = lambda drill_id: next(
            item for item in definitions if item["drill_id"] == drill_id
        )
        drills.get_execution.return_value = {
            "drill_id": "published-drill",
            "scope": "E",
            "version": 0,
            "status": "draft",
            "generated": {},
        }
        controller._drills = drills
        previous_sessions = dict(PortalRuntime.auth_manager._sessions)
        with PortalRuntime.auth_manager._lock:
            PortalRuntime.auth_manager._sessions.update(
                {
                    "drill-user": {
                        "session_id": "drill-user",
                        "user": {"name": "E楼用户", "open_id": ""},
                        "role": "user",
                        "allowed_scopes": ["E"],
                        "expires_at": 9_999_999_999,
                    },
                    "drill-admin": {
                        "session_id": "drill-admin",
                        "user": {"name": "管理员", "open_id": ""},
                        "role": "admin",
                        "allowed_scopes": ["A"],
                        "expires_at": 9_999_999_999,
                    },
                }
            )
        client = TestClient(controller._build_app())
        try:
            with patch.object(
                PortalRuntime.service,
                "_load_signature_people",
                return_value=[
                    {"record_id": "person-a", "name": "A楼人员", "building": "A楼"},
                    {"record_id": "person-e", "name": "E楼人员", "building": "E楼"},
                ],
            ) as people_loader:
                user = client.get(
                    "/api/drills/bootstrap?scope=E&month=2026-08&refresh_people=1",
                    headers={"Cookie": f"{AUTH_COOKIE_NAME}=drill-user"},
                )
                people_loader.assert_called_once_with(force=True)
                other_building = client.get(
                    "/api/drills/bootstrap?scope=A&month=2026-08",
                    headers={"Cookie": f"{AUTH_COOKIE_NAME}=drill-user"},
                )
                admin = client.get(
                    "/api/drills/bootstrap?month=2026-08",
                    headers={"Cookie": f"{AUTH_COOKIE_NAME}=drill-admin"},
                )
                denied = client.get(
                    "/api/drills/draft-drill/execution?scope=E",
                    headers={"Cookie": f"{AUTH_COOKIE_NAME}=drill-user"},
                )
                allowed = client.get(
                    "/api/drills/published-drill/execution?scope=E",
                    headers={"Cookie": f"{AUTH_COOKIE_NAME}=drill-user"},
                )
            self.assertEqual(user.status_code, 200, user.text)
            self.assertEqual(
                [item["drill_id"] for item in user.json()["data"]["drills"]],
                ["published-drill"],
            )
            self.assertEqual(user.json()["data"]["scopes"][0]["pending"], 1)
            self.assertEqual(
                [person["record_id"] for person in user.json()["data"]["people"]],
                ["person-a", "person-e"],
            )
            self.assertEqual(other_building.status_code, 403, other_building.text)
            self.assertNotIn("path", user.json()["data"]["drills"][0]["source"])
            self.assertEqual(
                {item["drill_id"] for item in admin.json()["data"]["drills"]},
                {"draft-drill", "published-drill"},
            )
            self.assertEqual(denied.status_code, 403, denied.text)
            self.assertIn("尚未发布", denied.json()["error"])
            self.assertEqual(allowed.status_code, 200, allowed.text)
            drills.get_execution.assert_called_with(
                "published-drill", "E", create=True
            )
        finally:
            with PortalRuntime.auth_manager._lock:
                PortalRuntime.auth_manager._sessions = previous_sessions

    def test_public_payload_hides_local_paths_and_file_tokens(self):
        payload = FastAPIPortalController._public_drill_payload(
            {
                "drill_id": "d1",
                "source": {
                    "path": r"D:\private\source.xlsx",
                    "name": "source.xlsx",
                    "sha256": "abc",
                },
                "generated": {"file_path": r"D:\private\current.xlsx"},
                "sync": {"file_token": "secret", "status": "synced"},
            }
        )
        self.assertEqual(payload["source"], {"name": "source.xlsx", "sha256": "abc"})
        self.assertEqual(payload["generated"], {})
        self.assertEqual(payload["sync"], {"status": "synced"})

    def test_archive_retry_reuses_verified_remote_version_without_reupload(self):
        service = object.__new__(MaintenancePortalService)
        service._drill_archive_lock = threading.RLock()
        existing = {
            "record_id": "rec-1",
            "fields": {
                "本地演练ID": "drill-1:A",
                "生成版本": 4,
                "演练文件名": "演练.xlsx",
                "演练记录表": [{"file_token": "file-1"}],
            },
        }
        with (
            patch.object(service, "_ensure_drill_archive_schema", return_value={}),
            patch.object(service, "_find_drill_archive_records", return_value=[existing]),
            patch.object(service, "_upload_drill_archive_file") as upload,
        ):
            result = service.sync_drill_archive(
                drill_id="drill-1",
                scope="A",
                year=2026,
                month=8,
                generated_version=4,
                file_path="unused.xlsx",
                file_name="演练.xlsx",
            )
        self.assertTrue(result["reused"])
        self.assertEqual(result["record_id"], "rec-1")
        self.assertEqual(result["file_token"], "file-1")
        upload.assert_not_called()

    def test_upload_dispatches_large_file_to_multipart(self):
        service = object.__new__(MaintenancePortalService)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "large.xlsx"
            with path.open("wb") as handle:
                handle.truncate(DRILL_ARCHIVE_SIMPLE_UPLOAD_MAX_BYTES + 1)
            with (
                patch.object(service, "_upload_large_bitable_file", return_value="large") as large,
                patch.object(service, "_upload_drive_media_file", return_value="small") as small,
            ):
                token = service._upload_drill_archive_file(
                    file_path=path,
                    file_name=path.name,
                    parent_node="canonical-app-token",
                )
        self.assertEqual(token, "large")
        large.assert_called_once()
        self.assertEqual(large.call_args.kwargs["app_token"], "canonical-app-token")
        small.assert_not_called()

    def test_archive_resolves_wiki_alias_to_canonical_bitable_token(self):
        service = object.__new__(MaintenancePortalService)
        service._http_client = Mock()
        with (
            patch.object(service, "_auth_headers", return_value={}),
            patch.object(
                service,
                "_request_payload",
                return_value={"code": 0, "data": {"app": {"app_token": "canonical-app-token"}}},
            ) as request,
        ):
            self.assertEqual(
                service._canonical_bitable_app_token("wiki-alias-token"),
                "canonical-app-token",
            )
            self.assertEqual(
                service._canonical_bitable_app_token("wiki-alias-token"),
                "canonical-app-token",
            )
        request.assert_called_once()

    def test_archive_refuses_to_replace_a_newer_remote_version(self):
        service = object.__new__(MaintenancePortalService)
        service._drill_archive_lock = threading.RLock()
        existing = {
            "record_id": "rec-newer",
            "fields": {
                "本地演练ID": "drill-1:A",
                "生成版本": 5,
                "演练文件名": "演练.xlsx",
                "演练记录表": [{"file_token": "file-newer"}],
            },
        }
        with (
            patch.object(service, "_ensure_drill_archive_schema", return_value={}),
            patch.object(service, "_find_drill_archive_records", return_value=[existing]),
            patch.object(service, "_upload_drill_archive_file") as upload,
        ):
            with self.assertRaisesRegex(PortalConflictError, "高于本机"):
                service.sync_drill_archive(
                    drill_id="drill-1",
                    scope="A",
                    year=2026,
                    month=8,
                    generated_version=4,
                    file_path="unused.xlsx",
                    file_name="演练.xlsx",
                )
        upload.assert_not_called()

    def test_print_signature_cells_receive_one_composite_image(self):
        from PIL import Image

        image = Image.new("RGBA", (24, 12), (0, 0, 0, 0))
        output = io.BytesIO()
        image.save(output, format="PNG")
        previous_service = PortalRuntime.service
        PortalRuntime.service = Mock()
        PortalRuntime.service.signature_image_bytes.return_value = (
            output.getvalue(),
            "image/png",
        )
        try:
            model = FastAPIPortalController._attach_drill_signature_composites(
                {
                    "signature_cells": [
                        {
                            "range": "H14",
                            "layout": "vertical",
                            "width_px": 100,
                            "height_px": 120,
                            "signers": [
                                {"record_id": "rec-1", "name": "甲"},
                                {"record_id": "rec-2", "name": "乙"},
                            ],
                        }
                    ]
                },
                required=True,
            )
        finally:
            PortalRuntime.service = previous_service
        data_url = model["signature_cells"][0]["image_data_url"]
        self.assertTrue(data_url.startswith("data:image/png;base64,"))

    def test_draft_can_save_incomplete_but_generate_cannot(self):
        controller = object.__new__(FastAPIPortalController)
        controller._drill_people = lambda scope, refresh=False: []
        definition = {
            "configuration": {
                "steps": [{"row": 13, "location": "ECC", "signature_slots": 2}]
            }
        }
        execution = {"commander": {}, "participants": [], "step_signers": {}}
        controller._validate_drill_people_payload(
            "A",
            definition,
            execution,
            require_complete=False,
            require_signatures=False,
        )
        with self.assertRaisesRegex(PortalError, "指挥人"):
            controller._validate_drill_people_payload(
                "A",
                definition,
                execution,
                require_complete=True,
                require_signatures=True,
            )

    def test_non_admin_cannot_read_unpublished_drill_by_id(self):
        controller = object.__new__(FastAPIPortalController)
        controller._drills = Mock()
        controller._drills.get_definition.return_value = {
            "drill_id": "d1",
            "status": "draft",
        }
        with patch.object(PortalRuntime.auth_manager, "is_admin", return_value=False):
            with self.assertRaisesRegex(PortalError, "尚未发布"):
                controller._require_drill_visible({}, "d1")

    def test_drill_people_include_all_buildings_beyond_first_500_without_raw_signatures(self):
        controller = object.__new__(FastAPIPortalController)
        people = [
            {
                "record_id": f"person-{index}",
                "building": ("A楼", "E楼", "H楼", "110站", "")[index % 5],
                "has_signature": index % 2 == 0,
                "raw_fields": {"签名": "must-stay-private"},
            }
            for index in range(502)
        ]
        with patch.object(PortalRuntime.service, "_load_signature_people", return_value=people) as load:
            for scope in ("A", "E", ""):
                result = controller._drill_people(scope, refresh=True)
                self.assertEqual(len(result), 502)
                self.assertEqual(result[-1]["record_id"], "person-501")
                self.assertTrue(all("raw_fields" not in person for person in result))
            load.assert_called_with(force=True)
        self.assertIn("raw_fields", people[0])

    def test_cross_building_people_can_save_and_generate_but_still_require_valid_signatures(self):
        controller = object.__new__(FastAPIPortalController)
        people = [
            {"record_id": "a", "name": "A楼指挥人", "building": "A楼", "has_signature": True},
            {"record_id": "b", "name": "B楼参演人", "building": "B楼", "has_signature": True},
        ]
        definition = {"configuration": {"steps": [{"row": 13, "location": "ECC", "signature_slots": 2}]}}
        execution = {
            "commander": {"record_id": "a", "name": "不可采信的姓名"},
            "participants": [{"record_id": "b"}],
            "step_signers": {"13": ["a", "b"]},
        }
        with patch.object(PortalRuntime.service, "_load_signature_people", return_value=people):
            controller._validate_drill_people_payload(
                "E", definition, execution, require_complete=True, require_signatures=True,
            )
            self.assertEqual(execution["commander"]["name"], "A楼指挥人")
            self.assertEqual([person["record_id"] for person in execution["participants"]], ["a", "b"])
            people[1]["has_signature"] = False
            with self.assertRaisesRegex(PortalError, "尚未保存可用签名"):
                controller._validate_drill_people_payload(
                    "E", definition, execution, require_complete=True, require_signatures=True,
                )
            execution["participants"].append({"record_id": "deleted-person"})
            with self.assertRaisesRegex(PortalError, "不存在或已离职"):
                controller._validate_drill_people_payload(
                    "E", definition, execution, require_complete=False, require_signatures=False,
                )

    def test_drill_jobs_use_the_dedicated_single_worker_executor(self):
        controller = object.__new__(FastAPIPortalController)
        controller._shutdown_event = threading.Event()
        controller._drill_jobs_lock = threading.RLock()
        controller._drill_jobs = set()
        controller._drill_executor = Mock()
        controller._background_executor = Mock()
        controller._run_drill_job = Mock()
        self.assertTrue(
            controller._queue_drill_job(
                "drill-1",
                "A",
                generate=True,
                expected_execution_version=3,
            )
        )
        controller._drill_executor.submit.assert_called_once_with(
            controller._run_drill_job,
            "drill-1",
            "A",
            True,
            3,
        )
        controller._background_executor.submit.assert_not_called()


if __name__ == "__main__":
    unittest.main()

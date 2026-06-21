import sys
import tempfile
import unittest
from pathlib import Path


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from clipflow_backend.api_models import (  # noqa: E402
    OngoingDeleteRequest,
    JobMarkStuckFailedRequest,
    PermissionRequestBulkReviewRequest,
    PermissionRequestCreate,
    PermissionRequestReviewRequest,
    QtClipboardAckRequest,
    QtActiveItemsDeltaRequest,
    QtCommandRequest,
    QtDialogSessionRequest,
    QtEventAckRequest,
    QtJobProgressRequest,
    WorkbenchActionRequest,
    parse_api_model,
)
from clipflow_backend.main import FastAPIPortalController  # noqa: E402
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402


class BackendApiModelTests(unittest.TestCase):
    def test_workbench_action_keeps_extra_fields_for_compatibility(self):
        model = parse_api_model(
            WorkbenchActionRequest,
            {
                "scope": "A",
                "action": "start",
                "record_id": "rec-1",
                "custom_field": "kept",
            },
        )
        payload = model.to_payload()
        self.assertEqual(payload["scope"], "A")
        self.assertEqual(payload["record_id"], "rec-1")
        self.assertEqual(payload["custom_field"], "kept")

    def test_permission_request_requires_scope_list_shape(self):
        with self.assertRaises(ValueError):
            parse_api_model(PermissionRequestCreate, {"scopes": "A"})

    def test_permission_review_models_keep_safe_defaults(self):
        review = parse_api_model(PermissionRequestReviewRequest, {}).to_payload()
        bulk = parse_api_model(PermissionRequestBulkReviewRequest, {}).to_payload()
        self.assertEqual(review["scopes"], [])
        self.assertEqual(review["reason"], "")
        self.assertEqual(bulk["request_ids"], [])
        self.assertEqual(bulk["scopes_by_request_id"], {})

    def test_ongoing_delete_defaults_are_stable(self):
        payload = parse_api_model(OngoingDeleteRequest, {}).to_payload()
        self.assertEqual(payload["scope"], "ALL")
        self.assertEqual(payload["work_type"], "maintenance")

    def test_job_mark_stuck_failed_default_reason_is_stable(self):
        payload = parse_api_model(JobMarkStuckFailedRequest, {}).to_payload()
        self.assertIn("卡住任务", payload["reason"])

    def test_qt_command_payload_must_be_object(self):
        with self.assertRaises(ValueError):
            parse_api_model(QtCommandRequest, {"command": "x", "payload": "bad"})

    def test_qt_local_models_keep_safe_defaults(self):
        dialog = parse_api_model(QtDialogSessionRequest, {}).to_payload()
        clipboard_ack = parse_api_model(QtClipboardAckRequest, {}).to_payload()
        event_ack = parse_api_model(QtEventAckRequest, {}).to_payload()
        self.assertEqual(dialog["payload"], {})
        self.assertTrue(clipboard_ack["ok"])
        self.assertTrue(event_ack["ok"])

    def test_qt_delta_requires_list_shapes(self):
        with self.assertRaises(ValueError):
            parse_api_model(QtActiveItemsDeltaRequest, {"upserts": {"bad": True}})

    def test_qt_progress_keeps_extra_fields_for_bridge_compatibility(self):
        payload = parse_api_model(
            QtJobProgressRequest,
            {"phase": "uploading", "custom": "kept"},
        ).to_payload()
        self.assertEqual(payload["phase"], "uploading")
        self.assertEqual(payload["custom"], "kept")

    def test_qt_notice_upload_command_is_pass_through(self):
        payload = parse_api_model(
            QtCommandRequest,
            {
                "command": "notice_upload",
                "payload": {"data_dict": {"record_id": "rid-1"}, "action_type": "upload"},
            },
        ).to_payload()
        self.assertEqual(payload["command"], "notice_upload")
        self.assertEqual(payload["payload"]["data_dict"]["record_id"], "rid-1")
        self.assertEqual(payload["payload"]["action_type"], "upload")

    def test_inline_image_payload_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "upload_id"):
            FastAPIPortalController._reject_large_inline_images(
                {"extra_images": [{"bytes_b64": "a" * (260 * 1024)}]}
            )
        with self.assertRaisesRegex(ValueError, "base64"):
            FastAPIPortalController._reject_large_inline_images(
                {"extra_images": [{"upload_id": "up-1", "bytes_b64": "small"}]}
            )
        FastAPIPortalController._reject_large_inline_images({"extra_images": [{"upload_id": "up-1"}]})

    def test_notice_upload_attachment_stats(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            store.put_notice_upload_attachment(
                open_id="ou-test",
                file_name="site.png",
                mime_type="image/png",
                content=b"12345",
            )
            stats = store.notice_upload_attachment_stats()
            self.assertEqual(stats["total"], 1)
            self.assertEqual(stats["pending"], 1)
            self.assertEqual(stats["total_bytes"], 5)

    def test_notice_upload_attachment_capacity_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            store.put_notice_upload_attachment(content=b"12345", max_pending_bytes=10)
            with self.assertRaisesRegex(ValueError, "暂存空间已满"):
                store.put_notice_upload_attachment(content=b"678901", max_pending_bytes=10)

    def test_batch_job_visibility_uses_auth_open_id(self):
        session = {"user": {"open_id": "ou-allowed"}, "role": "user", "is_admin": False}
        visible_job = {"request": {"_auth_open_id": "ou-allowed"}}
        denied_job = {"request": {"_auth_open_id": "ou-other"}}
        self.assertTrue(FastAPIPortalController._job_visible_to_session(visible_job, session))
        self.assertFalse(FastAPIPortalController._job_visible_to_session(denied_job, session))


if __name__ == "__main__":
    unittest.main()

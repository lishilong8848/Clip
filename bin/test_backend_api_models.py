import sys
import unittest
from pathlib import Path


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from clipflow_backend.api_models import (  # noqa: E402
    OngoingDeleteRequest,
    PermissionRequestCreate,
    QtClipboardAckRequest,
    QtActiveItemsDeltaRequest,
    QtCommandRequest,
    QtDialogSessionRequest,
    QtEventAckRequest,
    QtJobProgressRequest,
    QtNoticeUploadRequest,
    WorkbenchActionRequest,
    parse_api_model,
)


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

    def test_ongoing_delete_defaults_are_stable(self):
        payload = parse_api_model(OngoingDeleteRequest, {}).to_payload()
        self.assertEqual(payload["scope"], "ALL")
        self.assertEqual(payload["work_type"], "maintenance")

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

    def test_qt_notice_upload_is_pass_through(self):
        payload = parse_api_model(
            QtNoticeUploadRequest,
            {"data_dict": {"record_id": "rid-1"}, "action_type": "upload"},
        ).to_payload()
        self.assertEqual(payload["data_dict"]["record_id"], "rid-1")
        self.assertEqual(payload["action_type"], "upload")


if __name__ == "__main__":
    unittest.main()

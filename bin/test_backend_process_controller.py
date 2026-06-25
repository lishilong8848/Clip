import os
import sys
import unittest
from pathlib import Path


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from clipflow_backend.process_controller import BackendProcessPortalController  # noqa: E402


class BackendProcessControllerTests(unittest.TestCase):
    def setUp(self):
        self._old_legacy = os.environ.pop(
            "CLIPFLOW_ENABLE_QT_LEGACY_PORTAL_CALLBACKS",
            None,
        )
        self._old_snapshot = os.environ.pop(
            "CLIPFLOW_QT_SNAPSHOT_INTERVAL_SECONDS",
            None,
        )

    def tearDown(self):
        if self._old_legacy is not None:
            os.environ["CLIPFLOW_ENABLE_QT_LEGACY_PORTAL_CALLBACKS"] = self._old_legacy
        else:
            os.environ.pop("CLIPFLOW_ENABLE_QT_LEGACY_PORTAL_CALLBACKS", None)
        if self._old_snapshot is not None:
            os.environ["CLIPFLOW_QT_SNAPSHOT_INTERVAL_SECONDS"] = self._old_snapshot
        else:
            os.environ.pop("CLIPFLOW_QT_SNAPSHOT_INTERVAL_SECONDS", None)

    def test_legacy_qt_business_callbacks_are_disabled_by_default(self):
        controller = BackendProcessPortalController()
        controller._ensure_bridge_threads = lambda: None
        called = {"maintenance": 0, "delete": 0}

        controller.set_maintenance_action_callback(
            lambda _payload: called.__setitem__("maintenance", called["maintenance"] + 1)
        )
        controller.set_ongoing_delete_callback(
            lambda _payload: called.__setitem__("delete", called["delete"] + 1)
        )

        self.assertIsNone(controller.maintenance_action_callback)
        self.assertIsNone(controller.ongoing_delete_callback)

        acked = []
        controller._ack_event = lambda event_id, **patch: acked.append((event_id, patch))
        controller._dispatch_event(
            {
                "id": 101,
                "payload": {
                    "kind": "maintenance_action",
                    "payload": {"job_id": "job-1"},
                },
            }
        )
        controller._dispatch_event(
            {
                "id": 102,
                "payload": {
                    "kind": "ongoing_delete",
                    "payload": {"active_item_id": "active-1"},
                },
            }
        )

        self.assertEqual(called, {"maintenance": 0, "delete": 0})
        self.assertEqual(len(acked), 2)
        self.assertTrue(all(patch.get("ok") for _event_id, patch in acked))

    def test_snapshot_callback_is_low_frequency_fallback_by_default(self):
        controller = BackendProcessPortalController()
        self.assertGreaterEqual(controller._snapshot_interval_s, 300)

    def test_qt_command_strips_legacy_inline_image_fields(self):
        controller = BackendProcessPortalController()
        payload = {
            "data_dict": {
                "record_id": "rec1",
                "bytes_b64": "legacy-inline",
                "nested": {"screenshot_bytes_b64": "legacy-screenshot"},
            },
            "extra_images": [
                {
                    "upload_id": "upload-1",
                    "file_name": "site.png",
                    "bytes_b64": "legacy-site",
                }
            ],
        }

        cleaned = controller._strip_inline_image_command_fields(payload)

        self.assertNotIn("bytes_b64", cleaned["data_dict"])
        self.assertNotIn("screenshot_bytes_b64", cleaned["data_dict"]["nested"])
        self.assertNotIn("bytes_b64", cleaned["extra_images"][0])
        self.assertEqual(cleaned["extra_images"][0]["upload_id"], "upload-1")


if __name__ == "__main__":
    unittest.main()

import sys
import unittest
from pathlib import Path


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from upload_event_module.ui.main_window_runtime import MainWindowRuntimeMixin  # noqa: E402


class _Controller:
    def __init__(self):
        self.acks = []

    def acknowledge_clipboard_candidate(self, candidate_id, *, ok=True, status=""):
        self.acks.append({"candidate_id": candidate_id, "ok": ok, "status": status})
        return {"candidate_id": candidate_id}


class _Harness(MainWindowRuntimeMixin):
    def __init__(self):
        self.lan_template_portal_controller = _Controller()
        self._qt_shell_dialog_sessions = []
        self.reproject_called = False
        self.applied_projection_payloads = []

    def _submit_notice_text_to_backend_projection(self, *args, **kwargs):
        self.reproject_called = True
        raise AssertionError("Qt shell must not re-project backend clipboard candidates")

    def _apply_backend_active_upsert(self, payload):
        self.applied_projection_payloads.append(payload)
        return {"ok": True, "created": True}

    def _enqueue_ui_mutation(self, _name, callback):
        callback()


class QtShellBackendEventTests(unittest.TestCase):
    def test_bootstrap_clipboard_candidates_are_acknowledged_not_reprojected(self):
        harness = _Harness()

        harness._consume_qt_shell_bootstrap_state(
            {
                "clipboard_candidates": [
                    {
                        "candidate_id": "cand-1",
                        "content": "【维保通告】状态：开始\n\n【标题】测试",
                    }
                ],
                "dialog_sessions": [{"session_id": "dlg-1"}],
            }
        )

        self.assertFalse(harness.reproject_called)
        self.assertEqual(
            harness.lan_template_portal_controller.acks,
            [{"candidate_id": "cand-1", "ok": True, "status": "backend_projected"}],
        )
        self.assertEqual(harness._qt_shell_dialog_sessions[0]["session_id"], "dlg-1")

    def test_clipboard_projection_response_is_applied_directly(self):
        harness = _Harness()

        result = harness._apply_clipboard_projection_result(
            {
                "projection": {
                    "ok": True,
                    "item": {
                        "active_item_id": "active-1",
                        "record_id": "local_active-1",
                        "payload": {
                            "active_item_id": "active-1",
                            "record_id": "local_active-1",
                            "notice_type": "维保通告",
                            "text": "【维保通告】状态：开始\n\n【标题】测试",
                        },
                    },
                }
            }
        )

        self.assertTrue(result["ok"])
        self.assertEqual(len(harness.applied_projection_payloads), 1)
        self.assertEqual(
            harness.applied_projection_payloads[0]["item"]["active_item_id"],
            "active-1",
        )


if __name__ == "__main__":
    unittest.main()

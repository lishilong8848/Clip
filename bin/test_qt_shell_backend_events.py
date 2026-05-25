import sys
import tempfile
import unittest
from pathlib import Path


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from upload_event_module.ui.main_window_runtime import MainWindowRuntimeMixin  # noqa: E402
from upload_event_module.ui.main_window_clipboard import MainWindowClipboardMixin  # noqa: E402
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402


class _Controller:
    def __init__(self):
        self.acks = []
        self.clipboard_events = []
        self.fail_contents: set[str] = set()

    def acknowledge_clipboard_candidate(self, candidate_id, *, ok=True, status=""):
        self.acks.append({"candidate_id": candidate_id, "ok": ok, "status": status})
        return {"candidate_id": candidate_id}

    def post_local_clipboard_event(
        self, content, *, ts=None, source="clipboard", target_record_id=""
    ):
        if content in self.fail_contents:
            raise RuntimeError("mock projection failed")
        self.clipboard_events.append(
            {
                "content": content,
                "ts": ts,
                "source": source,
                "target_record_id": target_record_id,
            }
        )
        return {
            "ok": True,
            "projection": {
                "ok": True,
                "item": {
                    "active_item_id": "active-from-sqlite",
                    "payload": {
                        "active_item_id": "active-from-sqlite",
                        "record_id": "local_active-from-sqlite",
                        "notice_type": "维保通告",
                        "text": content,
                    },
                },
            },
        }


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


class _ClipboardHarness(MainWindowClipboardMixin):
    def __init__(self, store: LanPortalStateStore, clipboard_file: Path):
        self._closing = False
        self._clipboard_state_store = store
        self._clipboard_sqlite_last_event_id = 0
        self.clipboard_paused = False
        self._ui_update_in_progress = False
        self.clipboard_event_file = clipboard_file
        self._clipboard_pending_lines = []
        self._clipboard_file_index = 0
        self._clipboard_partial_line = ""
        self._clipboard_file_max_bytes = 1024 * 1024
        self.lan_template_portal_controller = _Controller()
        self.snapshots = []
        self.projections = []
        self.failures = []

    def _is_clipboard_listener_disabled(self):
        return False

    def _is_in_clipboard_cooldown(self):
        return False

    def _update_last_clipboard_snapshot(self, content, timestamp_ms=None):
        self.snapshots.append({"content": content, "ts": timestamp_ms})

    def _apply_clipboard_projection_result(self, result):
        self.projections.append(result)
        return {"ok": True}

    def _remember_clipboard_failure(self, reason: str):
        self.failures.append(reason)


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

    def test_sqlite_clipboard_fallback_events_are_projected_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            store.append_event(
                "clipboard",
                {
                    "content": "【维保通告】状态：开始\n\n【标题】A楼测试测试测试",
                    "ts": 1779660000000,
                },
            )
            harness = _ClipboardHarness(store, Path(tmp) / "clipboard.jsonl")

            harness._poll_clipboard_event_file()
            harness._poll_clipboard_event_file()

            self.assertEqual(len(harness.lan_template_portal_controller.clipboard_events), 1)
            self.assertEqual(
                harness.lan_template_portal_controller.clipboard_events[0]["source"],
                "clipboard_sqlite_fallback",
            )
            self.assertEqual(len(harness.projections), 1)

    def test_sqlite_clipboard_bad_event_does_not_block_later_events(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            bad_text = "【维保通告】状态：开始\n\n【标题】坏事件"
            good_text = "【维保通告】状态：开始\n\n【标题】后续好事件"
            store.append_event("clipboard", {"content": bad_text, "ts": 1})
            store.append_event("clipboard", {"content": good_text, "ts": 2})
            harness = _ClipboardHarness(store, Path(tmp) / "clipboard.jsonl")
            harness._clipboard_sqlite_event_max_failures = 2
            harness.lan_template_portal_controller.fail_contents.add(bad_text)

            harness._poll_clipboard_event_file()
            harness._poll_clipboard_event_file()
            harness._poll_clipboard_event_file()

            self.assertEqual(
                [event["content"] for event in harness.lan_template_portal_controller.clipboard_events],
                [good_text],
            )
            self.assertEqual(harness._clipboard_sqlite_last_event_id, 2)
            self.assertTrue(harness.failures)


if __name__ == "__main__":
    unittest.main()

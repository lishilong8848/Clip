import datetime as dt
import queue
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from upload_event_module.ui.main_window_runtime import MainWindowRuntimeMixin  # noqa: E402
from upload_event_module.ui.main_window_clipboard import MainWindowClipboardMixin  # noqa: E402
from upload_event_module.ui.main_window_records import MainWindowRecordsMixin  # noqa: E402
from upload_event_module.ui.main_window_workflow import MainWindowWorkflowMixin  # noqa: E402
from upload_event_module.core.parser import extract_notice_info  # noqa: E402
from upload_event_module.config import EVENT_NOTICE_FIELDS  # noqa: E402
from clipflow_backend.main import FastAPIPortalController  # noqa: E402
from clipflow_backend.process_controller import BackendProcessPortalController  # noqa: E402
from lan_bitable_template_portal.server import PortalRuntime  # noqa: E402
from lan_bitable_template_portal.portal_service import MaintenancePortalService  # noqa: E402
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
        self,
        content,
        *,
        ts=None,
        source="clipboard",
        target_record_id="",
        qt_active_item_id="",
        qt_upload_in_progress=False,
    ):
        if content in self.fail_contents:
            raise RuntimeError("mock projection failed")
        self.clipboard_events.append(
            {
                "content": content,
                "ts": ts,
                "source": source,
                "target_record_id": target_record_id,
                "qt_active_item_id": qt_active_item_id,
                "qt_upload_in_progress": qt_upload_in_progress,
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


class _ImmediateDeleteHarness(MainWindowWorkflowMixin):
    def __init__(self, *, remote_deleted: bool):
        self.remote_deleted = remote_deleted
        self.backend_started = threading.Event()
        self.backend_release = threading.Event()
        self.backend_finished = threading.Event()
        self.cache_delete_count = 0
        self.messages = []
        self._today_in_progress_pending_record_ids = set()
        self._today_in_progress_synced_record_ids = set()
        self.pending_new_by_record_id = {}
        self.pending_replace_by_record_id = {}
        self.pending_update_after_upload = {}
        self.pending_action_record_ids = set()
        self.pending_action_types = {}

    def _is_screenshot_dialog_active(self):
        return False

    def _find_active_item_by_record_id(self, _record_id):
        return None, None

    def _find_active_item_by_active_item_id(self, _active_item_id):
        return None, None

    def _safe_item_widget(self, _list_widget, _item):
        return None

    def _clear_upload_queue(self, _record_id):
        return None

    def _delete_active_cache_record(self, _data_dict):
        self.cache_delete_count += 1
        return True

    def request_active_cache_save(self, *args, **kwargs):
        return None

    def _submit_delete_active_item_to_backend(self, _data_dict):
        self.backend_started.set()
        self.backend_release.wait(2.0)
        return True, "", {"remote_deleted": self.remote_deleted}

    def _enqueue_ui_mutation(self, _name, callback):
        callback()
        self.backend_finished.set()

    def _remember_delete_undo(self, _data_dict, _result):
        return None

    def show_message(self, message):
        self.messages.append(str(message))


class _DeleteCommandIdHarness(MainWindowWorkflowMixin):
    def __init__(self):
        self.lan_template_portal_controller = type(
            "Controller", (), {"submit_qt_command": lambda *args, **kwargs: None}
        )()
        self.submissions = []

    def _submit_qt_command(self, command, payload, *, timeout):
        self.submissions.append((command, payload, timeout))
        return {"ok": True, "remote_deleted": True}


class _RecordsHarness(MainWindowRecordsMixin):
    pass


class _OperationItem:
    def __init__(self, data):
        self.payload = dict(data)

    def data(self, _role):
        return dict(self.payload)


class _DuplicateEventDeleteHarness(_ImmediateDeleteHarness):
    def __init__(self):
        super().__init__(remote_deleted=True)
        self.backend_calls = 0
        self.pending_action_record_ids = {"rec-shared-event"}
        self.pending_action_types = {"rec-shared-event": "update"}
        self.current = {
            "active_item_id": "active-uploading",
            "record_id": "rec-shared-event",
            "target_record_id": "rec-shared-event",
            "notice_type": "事件通告",
            "_upload_in_progress": True,
        }

    def _active_notice_store(self):
        current = dict(self.current)

        class Store:
            @staticmethod
            def entries():
                return [("event-list", "uploading-item", current)]

        return Store()

    def _submit_delete_active_item_to_backend(self, _data_dict):
        self.backend_calls += 1
        return False, "不应删除共享远端目标", {}


class _PriorityMutationHarness(MainWindowRuntimeMixin):
    def __init__(self, *, priority_size: int = 2):
        self._closing = False
        self._ui_update_in_progress = False
        self._ui_priority_mutation_queue = queue.Queue(maxsize=priority_size)
        self._ui_mutation_queue = queue.Queue(maxsize=2)
        self._ui_mutation_max_per_tick = 1
        self._ui_mutation_budget_ms = 40.0
        self._ui_slow_threshold_ms = 120.0
        self.executed = []

    def _set_last_ui_op(self, *_args, **_kwargs):
        return None

    def _record_slow_ui_operation(self, *_args, **_kwargs):
        return None

    def _apply_backend_active_upsert(self, _payload):
        self.executed.append("active_upsert")
        return {"ok": True}


class _ActiveUpsertVisibilityHarness(MainWindowRuntimeMixin):
    def __init__(self):
        self.added = []

    @staticmethod
    def _ensure_active_item_identity(data):
        return dict(data)

    @staticmethod
    def _find_active_item_by_active_item_id(_active_item_id):
        return None, None

    @staticmethod
    def _find_active_item_by_record_id(_record_id):
        return None, None

    @staticmethod
    def _is_valid_list_item(_item):
        return False

    def add_active_item(self, data, **_kwargs):
        self.added.append(dict(data))
        return object(), None


class _CanonicalActiveDeleteHarness(MainWindowRuntimeMixin):
    def __init__(self, store: LanPortalStateStore, current: dict):
        self.cache_store = type(
            "CacheStore",
            (),
            {"_state_store": store},
        )()
        self.current = dict(current)
        self.removed = []

    def _find_active_item_by_active_item_id(self, active_item_id):
        if str(self.current.get("active_item_id") or "") == str(active_item_id or ""):
            return "other-list", "current-item"
        return None, None

    def _find_active_item_by_record_id(self, record_id):
        current_record_id = str(
            self.current.get("target_record_id")
            or self.current.get("record_id")
            or ""
        )
        if current_record_id == str(record_id or ""):
            return "other-list", "current-item"
        return None, None

    @staticmethod
    def _is_valid_list_item(item):
        return item == "current-item"

    def _remove_active_item_from_source(self, list_widget, item):
        self.removed.append((list_widget, item))
        self.current = {}

    def _apply_backend_active_upsert(self, payload):
        item = payload.get("item") if isinstance(payload.get("item"), dict) else payload
        data = item.get("payload") if isinstance(item.get("payload"), dict) else item
        self.current = dict(data)
        return {"ok": True, "updated": True}


class QtShellBackendEventTests(unittest.TestCase):
    def test_stopping_controller_invalidates_active_sse_connections(self):
        controller = object.__new__(FastAPIPortalController)
        controller._stopping_event = threading.Event()
        controller._sse_lock = threading.RLock()
        controller._sse_connections = {("qt-active", "127.0.0.1", "session", "A"): 1}
        key = next(iter(controller._sse_connections))

        self.assertTrue(controller._sse_active(key, 1))
        controller._stopping_event.set()
        self.assertFalse(controller._sse_active(key, 1))
        controller._shutdown_event = threading.Event()
        self.assertFalse(controller._submit_background("after-stop", lambda: None))

    def test_startup_event_reconcile_removes_finished_and_missing_targets_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            original_store = PortalRuntime.state_store
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            PortalRuntime.state_store = store
            try:
                targets = [
                    ("active-finished", "rec-finished"),
                    ("active-finished-copy", "rec-finished"),
                    ("active-status-finished", "rec-status-finished"),
                    ("active-live", "rec-live"),
                    ("active-transient", "rec-transient"),
                    ("active-missing", "rec-missing"),
                ]
                for active_item_id, record_id in targets:
                    store.upsert_qt_active_item(
                        {
                            "active_item_id": active_item_id,
                            "record_id": record_id,
                            "target_record_id": record_id,
                            "notice_type": "事件通告",
                            "work_type": "event",
                            "_is_placeholder_record": False,
                            "text": f"【事件通告】状态：更新\n【标题】{active_item_id}",
                        },
                        section="event",
                        origin="clipboard",
                    )

                def query(record_id, _notice_type):
                    if record_id == "rec-finished":
                        return True, {
                            "fields": {EVENT_NOTICE_FIELDS["end_time"]: 1788148800000}
                        }
                    if record_id == "rec-status-finished":
                        return True, {"fields": {"事件状态": "已结束"}}
                    if record_id == "rec-missing":
                        return False, "code=1254043, msg=RecordIdNotFound"
                    if record_id == "rec-transient":
                        raise ConnectionError("connection reset")
                    return True, {"fields": {}}

                rows = store.list_qt_active_items()
                with patch(
                    "clipflow_backend.main.query_record_by_id",
                    side_effect=query,
                ) as query_mock:
                    stats = FastAPIPortalController._reconcile_finished_qt_event_items(rows)

                remaining = {
                    row["record_id"] for row in store.list_qt_active_items()
                }
                self.assertEqual(remaining, {"rec-live", "rec-transient"})
                self.assertEqual(stats["checked"], 5)
                self.assertEqual(stats["removed"], 3)
                self.assertEqual(stats["missing"], 1)
                self.assertEqual(stats["failed"], 1)
                self.assertEqual(query_mock.call_count, 5)
                finished_identity = store.resolve_notice_identity(
                    work_type="event",
                    active_item_id="active-finished",
                    target_record_id="rec-finished",
                )
                self.assertEqual(finished_identity["status"], "已结束")
                missing_identities = store.list_notice_identities(
                    include_deleted=True,
                    limit=100,
                )
                missing_identity = next(
                    item
                    for item in missing_identities
                    if item.get("target_record_id") == "rec-missing"
                )
                self.assertIsNotNone(missing_identity.get("deleted_at"))
            finally:
                PortalRuntime.state_store = original_store

    def test_startup_event_reconcile_is_single_flight(self):
        entered = threading.Event()
        release = threading.Event()
        row = {
            "active_item_id": "active-single-flight",
            "record_id": "rec-single-flight",
            "notice_type": "事件通告",
            "payload": {
                "active_item_id": "active-single-flight",
                "record_id": "rec-single-flight",
                "target_record_id": "rec-single-flight",
                "notice_type": "事件通告",
                "work_type": "event",
            },
        }

        def query(_record_id, _notice_type):
            entered.set()
            release.wait(timeout=2)
            return True, {"fields": {}}

        first_result = {}

        def run_first():
            first_result.update(
                FastAPIPortalController._reconcile_finished_qt_event_items([row])
            )

        with patch(
            "clipflow_backend.main.query_record_by_id",
            side_effect=query,
        ) as query_mock:
            worker = threading.Thread(target=run_first)
            worker.start()
            self.assertTrue(entered.wait(timeout=1))
            second = FastAPIPortalController._reconcile_finished_qt_event_items([row])
            release.set()
            worker.join(timeout=2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(second.get("skipped_inflight"), 1)
        self.assertEqual(first_result.get("checked"), 1)
        self.assertEqual(query_mock.call_count, 1)

    def test_upload_operation_lookup_skips_wrong_duplicate_record_row(self):
        harness = _RecordsHarness()
        wrong = _OperationItem(
            {"record_id": "rec-shared", "_upload_operation_id": "other-op"}
        )
        right = _OperationItem(
            {
                "record_id": "rec-shared",
                "_upload_operation_id": "wanted-op",
                "_upload_in_progress": True,
            }
        )
        harness._find_active_item_by_upload_completion_id = (
            lambda _record_id: ("event-list", wrong, "rec-shared")
        )
        harness._is_valid_list_item = lambda _item: True

        class Store:
            @staticmethod
            def entries():
                return [
                    ("event-list", wrong, wrong.payload),
                    ("event-list", right, right.payload),
                ]

        harness._active_notice_store = lambda: Store()
        harness.pending_action_record_ids = set()

        self.assertTrue(
            harness._is_current_upload_operation("rec-shared", "wanted-op")
        )
        list_widget, item = harness._find_active_item_by_upload_operation(
            "wanted-op"
        )
        self.assertEqual(list_widget, "event-list")
        self.assertIs(item, right)

    def test_deleting_duplicate_event_keeps_sibling_upload_slot_and_remote_target(self):
        harness = _DuplicateEventDeleteHarness()
        duplicate = {
            "active_item_id": "active-duplicate",
            "record_id": "rec-shared-event",
            "target_record_id": "rec-shared-event",
            "notice_type": "事件通告",
            "_upload_in_progress": False,
        }

        harness._delete_active_item(duplicate)
        self.assertTrue(harness.backend_finished.wait(1.0))

        self.assertEqual(harness.backend_calls, 0)
        self.assertIn("rec-shared-event", harness.pending_action_record_ids)
        self.assertEqual(
            harness.pending_action_types["rec-shared-event"], "update"
        )
        self.assertEqual(harness.cache_delete_count, 1)

    def test_clipboard_event_upload_context_is_scoped_to_matching_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            harness = _ClipboardHarness(
                LanPortalStateStore(Path(tmp) / "state.sqlite3"),
                Path(tmp) / "clipboard.jsonl",
            )
            data = {
                "active_item_id": "active-recreated-busy",
                "record_id": "local_recreated_busy",
                "notice_type": "事件通告",
                "_upload_in_progress": False,
            }

            class Item:
                @staticmethod
                def data(_role):
                    return data

            harness._find_active_item_by_content_or_title = (
                lambda *_args, **_kwargs: ("event-list", Item())
            )
            harness._is_valid_list_item = lambda _item: True
            harness._upload_completion_record_id_candidates = (
                lambda record_id: [record_id]
            )
            harness.pending_action_record_ids = {"local_recreated_busy"}
            text = (
                "【事件通告】状态：更新\n"
                "【标题】E楼事件\n【来源】BMS\n"
                "【时间】2026-08-26 17:00\n【概述】处理中"
            )

            context = harness._clipboard_event_upload_context(text)

            self.assertEqual(
                context,
                {
                    "qt_active_item_id": "active-recreated-busy",
                    "qt_upload_in_progress": True,
                },
            )
            harness._clipboard_state_store.shutdown_write_worker(timeout=1.0)

    def test_qt_local_event_match_normalizes_iso_time_and_building_code(self):
        harness = _RecordsHarness()
        update_text = (
            "【事件通告】状态：更新\n"
            "【标题】EA118机房B楼I3级事件通报；\n"
            "【来源】巡检发现；\n"
            "【时间】2026年8月11日09:37分；\n"
            "【概述】巡检发现B-127冷冻站A区变频补水环网管道有渗水现象\n"
            "【进展】人员已经到达现场，正在排查"
        )
        incoming = {"notice_type": "事件通告", "text": update_text}
        candidate = {
            "notice_type": "事件通告",
            "title": "巡检发现B-127冷冻站A区变频补水环网管道有渗水现象",
            "start_time": "2026-08-11T09:37",
            "building_codes": ["B"],
            "event_source": "巡检发现",
            "level": "I3",
        }

        incoming_fields = harness._event_sparse_match_fields(incoming)
        candidate_fields = harness._event_sparse_match_fields(candidate)

        self.assertEqual(incoming_fields["time"], "202608110937")
        self.assertEqual(candidate_fields["time"], "202608110937")
        self.assertEqual(incoming_fields["building"], "B")
        self.assertEqual(candidate_fields["building"], "B")
        self.assertTrue(harness._event_sparse_identity_matches(incoming, candidate))

        class _Store:
            @staticmethod
            def candidates_by_exact_text(_text):
                return []

            @staticmethod
            def candidates_by_match_key(_key):
                return []

            @staticmethod
            def candidates_by_match_title(_title):
                return []

            @staticmethod
            def entries():
                return [("event-list", "event-item", candidate)]

        harness._active_notice_store = lambda: _Store()
        list_widget, item = harness._find_active_item_by_content_or_title(
            update_text,
            "EA118机房B楼I3级事件通报",
            "事件通告",
        )
        self.assertEqual((list_widget, item), ("event-list", "event-item"))

    def test_live_active_upsert_runs_before_bulk_snapshot_mutation(self):
        harness = _PriorityMutationHarness()
        self.assertTrue(
            harness._enqueue_ui_mutation(
                "backend_active_sync",
                lambda: harness.executed.append("snapshot"),
            )
        )
        self.assertTrue(
            harness._enqueue_ui_mutation(
                "active_upsert",
                lambda: harness.executed.append("live"),
            )
        )

        harness._drain_ui_mutations()

        self.assertEqual(harness.executed, ["live"])

    def test_backend_active_sync_enqueues_without_waiting_for_ui_apply(self):
        harness = _PriorityMutationHarness()

        result = harness.handle_qt_shell_event(
            "active_upsert",
            {
                "source": "backend_active_sync",
                "item": {"payload": {"text": "snapshot"}},
            },
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["queued"])
        self.assertEqual(harness.executed, [])
        harness._drain_ui_mutations()
        self.assertEqual(harness.executed, ["active_upsert"])

    def test_full_live_mutation_queue_rejects_event_for_backend_retry(self):
        harness = _PriorityMutationHarness(priority_size=1)
        self.assertTrue(harness._enqueue_ui_mutation("active_upsert", lambda: None))

        result = harness.handle_qt_shell_event(
            "active_upsert",
            {"source": "qt_event", "item": {"payload": {"text": "test"}}},
        )

        self.assertFalse(result["ok"])
        self.assertIn("队列已满", result["error"])

    def test_queued_active_apply_failure_is_nacked_after_ui_mutation_runs(self):
        for kind in ("active_upsert", "active_delete"):
            with self.subTest(kind=kind):
                harness = _PriorityMutationHarness()
                queued = threading.Event()
                apply_calls = []
                payload = {
                    "active_item_id": f"active-{kind}-apply-failure",
                    "record_id": f"rec-{kind}-apply-failure",
                    "text": "【变更通告】状态：更新\n【名称】应用失败重试",
                }

                def fail_apply(actual_payload, *, _kind=kind):
                    apply_calls.append((_kind, dict(actual_payload or {})))
                    return {"ok": False, "error": "sqlite busy"}

                if kind == "active_upsert":
                    harness._apply_backend_active_upsert = fail_apply
                else:
                    harness._apply_backend_active_delete = fail_apply

                enqueue = harness._enqueue_ui_mutation

                def signal_enqueue(name, callback):
                    accepted = enqueue(name, callback)
                    if accepted:
                        queued.set()
                    return accepted

                harness._enqueue_ui_mutation = signal_enqueue
                controller = BackendProcessPortalController()
                controller.shell_event_callback = harness.handle_qt_shell_event
                acknowledgements = []
                controller._ack_event = lambda event_id, *, ok, error="": (
                    acknowledgements.append(
                        {"event_id": event_id, "ok": ok, "error": error}
                    )
                )
                event_id = 701 if kind == "active_upsert" else 702
                dispatch = threading.Thread(
                    target=lambda: controller._dispatch_event(
                        {
                            "id": event_id,
                            "payload": {"kind": kind, "payload": payload},
                        }
                    ),
                    daemon=True,
                )
                dispatch.start()
                self.assertTrue(queued.wait(1.0))
                harness._drain_ui_mutations()
                dispatch.join(1.0)

                self.assertFalse(dispatch.is_alive())
                self.assertEqual(apply_calls, [(kind, payload)])
                self.assertEqual(len(acknowledgements), 1)
                self.assertEqual(acknowledgements[0]["event_id"], event_id)
                self.assertFalse(
                    acknowledgements[0]["ok"],
                    "UI 实际 apply 失败时不得把 outbox 确认为 done",
                )
                self.assertIn("sqlite busy", acknowledgements[0]["error"])

    def test_scoped_qt_active_identities_drop_deleted_local_event(self):
        previous_store = PortalRuntime.state_store
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            PortalRuntime.state_store = store
            try:
                month = dt.datetime.now().strftime("%Y-%m")
                payload = {
                    "active_item_id": "active-local-event-e",
                    "record_id": "localid-event-e",
                    "target_record_id": "localid-event-e",
                    "notice_type": "事件通告",
                    "work_type": "event",
                    "scope": "E",
                    "building_codes": ["E"],
                    "status": "更新",
                    "title": "E楼未上传事件",
                    "text": (
                        "【事件通告】状态：更新\n"
                        "【标题】E楼未上传事件\n"
                        f"【时间】{month}-15 09:35"
                    ),
                }
                self.assertTrue(
                    store.upsert_qt_active_item(
                        payload,
                        section="event",
                        origin="qt",
                    )
                )

                identities = FastAPIPortalController._scoped_qt_active_identities(
                    "E",
                    month_key=month,
                )

                self.assertEqual(len(identities), 1)
                self.assertEqual(
                    identities[0]["active_item_id"],
                    "active-local-event-e",
                )
                self.assertTrue(identities[0]["local_only"])
                self.assertTrue(
                    store.delete_qt_active_item(
                        active_item_id="active-local-event-e",
                    )
                )
                self.assertEqual(
                    FastAPIPortalController._scoped_qt_active_identities(
                        "E",
                        month_key=month,
                    ),
                    [],
                )
            finally:
                PortalRuntime.state_store = previous_store

    def test_lite_workbench_subscribes_to_immediate_qt_delete_updates(self):
        workbench_text = (
            BIN_DIR / "lan_bitable_template_portal" / "workbench_lite.py"
        ).read_text(encoding="utf-8")
        backend_text = (
            BIN_DIR / "clipflow_backend" / "main.py"
        ).read_text(encoding="utf-8")

        self.assertIn(
            "new EventSource(streamUrl.pathname + streamUrl.search)",
            workbench_text,
        )
        self.assertIn("applyQtActiveIdentitySnapshot", workbench_text)
        self.assertIn(
            '.ongoing-row[data-local-only="1"]:not(.optimistic)',
            workbench_text,
        )
        self.assertIn('"active_identities": active_identities', backend_text)
        self.assertIn("self._notify_qt_active_streams()", backend_text)

    def test_local_only_delete_waits_for_backend_before_removing_qt_cache(self):
        harness = _ImmediateDeleteHarness(remote_deleted=False)
        payload = {
            "active_item_id": "active-local-delete",
            "record_id": "localid-event-update",
            "target_record_id": "localid-event-update",
            "notice_type": "事件通告",
            "work_type": "event",
        }

        harness._delete_active_item(payload)

        self.assertTrue(harness.backend_started.wait(1.0))
        self.assertEqual(harness.cache_delete_count, 0)
        self.assertEqual(harness.messages, [])
        harness.backend_release.set()
        self.assertTrue(harness.backend_finished.wait(1.0))
        self.assertEqual(harness.cache_delete_count, 1)
        self.assertEqual(harness.messages, [])

    def test_remote_delete_waits_for_backend_before_removing_qt_cache(self):
        harness = _ImmediateDeleteHarness(remote_deleted=True)
        payload = {
            "active_item_id": "active-remote-delete",
            "record_id": "rec-event-update",
            "target_record_id": "rec-event-update",
            "notice_type": "事件通告",
            "work_type": "event",
        }

        harness._delete_active_item(payload)

        self.assertTrue(harness.backend_started.wait(1.0))
        self.assertEqual(harness.cache_delete_count, 0)
        harness.backend_release.set()
        self.assertTrue(harness.backend_finished.wait(1.0))
        self.assertEqual(harness.cache_delete_count, 1)

    def test_qt_swipe_delete_keeps_upload_operation_id_and_reuses_delete_id(self):
        harness = _DeleteCommandIdHarness()
        payload = {
            "active_item_id": "active-delete-id",
            "record_id": "rec-delete-id",
            "target_record_id": "rec-delete-id",
            "operation_id": "upload-operation-id",
        }

        first = harness._submit_delete_active_item_to_backend(payload)
        second = harness._submit_delete_active_item_to_backend(payload)

        self.assertTrue(first[0])
        self.assertTrue(second[0])
        submitted_ids = [
            item[1]["data_dict"]["operation_id"] for item in harness.submissions
        ]
        self.assertEqual(len(set(submitted_ids)), 1)
        self.assertTrue(submitted_ids[0].startswith("qt-delete:"))
        self.assertEqual(payload["operation_id"], "upload-operation-id")
        self.assertEqual(payload["_delete_operation_id"], submitted_ids[0])

    def test_runtime_active_upsert_keeps_cross_month_ongoing_item(self):
        harness = _ActiveUpsertVisibilityHarness()
        previous_month = (
            dt.datetime.now().replace(day=1) - dt.timedelta(days=1)
        ).strftime("%Y-%m")
        result = harness._apply_backend_active_upsert(
            {
                "item": {
                    "active_item_id": "active-old-runtime",
                    "record_id": "rec-old-runtime",
                    "payload": {
                        "active_item_id": "active-old-runtime",
                        "record_id": "rec-old-runtime",
                        "notice_type": "变更通告",
                        "text": (
                            "【变更通告】状态：更新\n"
                            "【名称】旧月份变更\n"
                            f"【时间】{previous_month}-08 09:00"
                            f"~{previous_month}-08 18:00"
                        ),
                    },
                }
            }
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["created"])
        self.assertEqual(len(harness.added), 1)
        self.assertEqual(
            harness.added[0]["active_item_id"],
            "active-old-runtime",
        )

    def test_event_active_upsert_uses_indexed_state_lookup(self):
        target_record_id = "rec-event-indexed-upsert"
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            canonical = {
                "active_item_id": "active-event-indexed-upsert",
                "record_id": target_record_id,
                "target_record_id": target_record_id,
                "notice_type": "事件通告",
                "work_type": "event",
                "text": "【事件通告】状态：更新\n【标题】索引点查事件",
            }
            self.assertTrue(
                store.upsert_qt_active_item(
                    canonical,
                    section="event",
                    origin="portal",
                )
            )
            harness = _ActiveUpsertVisibilityHarness()
            harness.cache_store = type(
                "CacheStore",
                (),
                {"_state_store": store},
            )()

            with patch.object(
                store,
                "list_visible_qt_active_items",
                side_effect=AssertionError("不得扫描全部活动通告"),
            ):
                result = harness._apply_backend_active_upsert(
                    {
                        "item": {
                            "active_item_id": canonical["active_item_id"],
                            "record_id": target_record_id,
                            "payload": canonical,
                        }
                    }
                )

            self.assertTrue(result["ok"])
            self.assertEqual(len(harness.added), 1)
            self.assertEqual(
                harness.added[0]["target_record_id"],
                target_record_id,
            )
            self.assertEqual(harness.added[0]["text"], canonical["text"])

    def test_stale_active_delete_keeps_live_canonical_target(self):
        target_record_id = "rec-canonical-after-stale-delete"
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            canonical = {
                "active_item_id": "active-current-canonical",
                "record_id": target_record_id,
                "target_record_id": target_record_id,
                "notice_type": "变更通告",
                "work_type": "change",
                "text": "【变更通告】状态：更新\n【名称】当前权威通告",
            }
            store.upsert_qt_active_item(
                canonical,
                section="other",
                origin="target_snapshot_refresh",
            )
            harness = _CanonicalActiveDeleteHarness(
                store,
                {
                    **canonical,
                    "active_item_id": "active-visible-before-migration",
                },
            )

            result = harness._apply_backend_active_delete(
                {
                    "active_item_id": "active-visible-before-migration",
                    "record_id": target_record_id,
                    "target_record_id": target_record_id,
                }
            )

            self.assertFalse(result.get("deleted", False))
            self.assertEqual(harness.removed, [])
            self.assertEqual(
                harness.current.get("target_record_id"),
                target_record_id,
            )

    def test_active_delete_fails_closed_when_canonical_state_read_fails(self):
        target_record_id = "rec-delete-canonical-read-failure"
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            canonical = {
                "active_item_id": "active-delete-canonical-read-failure",
                "record_id": target_record_id,
                "target_record_id": target_record_id,
                "notice_type": "变更通告",
                "work_type": "change",
                "text": "【变更通告】状态：更新\n【名称】权威仍进行",
            }
            self.assertTrue(
                store.upsert_qt_active_item(
                    canonical,
                    section="other",
                    origin="portal",
                )
            )
            harness = _CanonicalActiveDeleteHarness(store, canonical)

            with patch.object(
                store,
                "list_visible_qt_active_items",
                side_effect=RuntimeError("sqlite busy"),
            ):
                result = harness._apply_backend_active_delete(
                    {
                        "active_item_id": canonical["active_item_id"],
                        "record_id": target_record_id,
                        "target_record_id": target_record_id,
                    }
                )

            self.assertFalse(result["ok"])
            self.assertEqual(harness.removed, [])
            self.assertEqual(harness.current, canonical)
            self.assertEqual(len(store.list_visible_qt_active_items()), 1)

    def test_stale_active_upsert_does_not_revive_soft_deleted_canonical_item(self):
        target_record_id = "rec-canonical-before-stale-upsert"
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            stale = {
                "active_item_id": "active-stale-upsert",
                "record_id": target_record_id,
                "target_record_id": target_record_id,
                "notice_type": "变更通告",
                "work_type": "change",
                "text": "【变更通告】状态：更新\n【名称】陈旧投影",
            }
            self.assertTrue(
                store.upsert_qt_active_item(
                    stale,
                    section="other",
                    origin="portal",
                )
            )
            deleted, _event_id = store.delete_qt_active_item_and_enqueue(
                active_item_id=stale["active_item_id"],
                record_id=target_record_id,
                channel="qt_action",
                payload={"kind": "active_delete", "payload": stale},
            )
            self.assertTrue(deleted)
            self.assertEqual(store.list_visible_qt_active_items(), [])

            harness = _ActiveUpsertVisibilityHarness()
            harness.cache_store = type(
                "CacheStore",
                (),
                {"_state_store": store},
            )()
            result = harness._apply_backend_active_upsert(
                {
                    "item": {
                        "active_item_id": stale["active_item_id"],
                        "record_id": target_record_id,
                        "origin": "portal",
                        "payload": stale,
                    }
                }
            )

            self.assertFalse(result.get("created", False))
            self.assertEqual(
                harness.added,
                [],
                "权威 qta 已软删除时，Qt 不得重放旧 active_upsert 复活",
            )

    def test_stale_active_upsert_does_not_overwrite_newer_canonical_fields(self):
        target_record_id = "rec-canonical-newer-fields"
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            canonical = {
                "active_item_id": "active-canonical-newer-fields",
                "record_id": target_record_id,
                "target_record_id": target_record_id,
                "source_record_id": "source-canonical-newer-fields",
                "notice_type": "变更通告",
                "work_type": "change",
                "progress": "NEW",
                "text": "【变更通告】状态：更新\n【名称】同身份\n【进度】NEW",
            }
            self.assertTrue(
                store.upsert_qt_active_item(
                    canonical,
                    section="other",
                    origin="portal",
                    allow_revive=True,
                )
            )
            stale = {
                **canonical,
                "progress": "OLD",
                "text": "【变更通告】状态：更新\n【名称】同身份\n【进度】OLD",
            }
            harness = _ActiveUpsertVisibilityHarness()
            harness.cache_store = type(
                "CacheStore",
                (),
                {"_state_store": store},
            )()

            result = harness._apply_backend_active_upsert(
                {
                    "item": {
                        "active_item_id": canonical["active_item_id"],
                        "record_id": target_record_id,
                        "origin": "portal",
                        "payload": stale,
                    }
                }
            )

            self.assertTrue(result["ok"])
            self.assertEqual(len(harness.added), 1)
            self.assertEqual(harness.added[0]["progress"], "NEW")
            self.assertIn("【进度】NEW", harness.added[0]["text"])

    def test_target_upsert_converges_source_and_target_qt_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            source_record_id = "rec-source-maintenance-e"
            target_record_id = "rec-target-maintenance-e"
            common = {
                "work_type": "maintenance",
                "notice_type": "维保通告",
                "title": "EA118机房E楼定压补水系统月度维护",
                "building": "E楼",
                "maintenance_cycle": "每月",
                "start_time": "2026-08-15 00:00",
                "end_time": "2026-08-21 00:00",
            }
            source = {
                **common,
                "active_item_id": f"source-maintenance-{source_record_id}",
                "source_record_id": source_record_id,
                "source_snapshot_authoritative": True,
                "status": "更新",
                "maintenance_cycle": "",
                "display_fields": {"维护周期": "每月"},
                "location": "E楼",
                "content": common["title"],
                "text": "【维保通告】状态：更新\n【名称】EA118机房E楼定压补水系统月度维护",
            }
            target = {
                **common,
                "active_item_id": target_record_id,
                "record_id": target_record_id,
                "target_record_id": target_record_id,
                "status": "开始",
                "start_time": "2026-08-17 14:00",
                "location": "E楼冷站",
                "content": "补水装置检查、泵及管道检查",
                "text": "【维保通告】状态：开始\n【名称】EA118机房E楼定压补水系统月度维护",
            }
            store.upsert_qt_active_item(
                source,
                section="other",
                origin="source_snapshot_refresh",
            )
            store.upsert_qt_active_item(target, section="other", origin="portal")

            class Harness(MainWindowRuntimeMixin):
                def __init__(self, include_target: bool):
                    self.cache_store = type("CacheStore", (), {"_state_store": store})()
                    self.rows = {"source-item": dict(source)}
                    if include_target:
                        self.rows["target-item"] = dict(target)
                    self.added = []

                def _find(self, value, *keys):
                    for item, data in self.rows.items():
                        if str(value or "") in {
                            str(data.get(key) or "") for key in keys
                        }:
                            return "other-list", item
                    return None, None

                def _find_active_item_by_active_item_id(self, value):
                    return self._find(value, "active_item_id")

                def _find_active_item_by_record_id(self, value):
                    return self._find(value, "target_record_id", "record_id")

                @staticmethod
                def _ensure_active_item_identity(data):
                    return dict(data)

                def _is_valid_list_item(self, item):
                    return item in self.rows

                def _active_notice_store(self):
                    harness = self

                    class ActiveStore:
                        @staticmethod
                        def candidates_by_source_record_id(value):
                            return [
                                ("other-list", item, data)
                                for item, data in harness.rows.items()
                                if data.get("source_record_id") == value
                            ]

                        @staticmethod
                        def entries():
                            return [
                                ("other-list", item, data)
                                for item, data in harness.rows.items()
                            ]

                    return ActiveStore()

                def _active_item_data(self, item):
                    return dict(self.rows[item])

                @staticmethod
                def _inherit_active_runtime_fields(data, _existing):
                    return dict(data)

                def _set_active_item_data(self, _list_widget, item, data):
                    self.rows[item] = dict(data)

                def _upsert_active_notice_model_item(self, *_args):
                    return None

                def _maybe_update_detail_dialog(self, *_args):
                    return None

                def _remove_active_item_from_source(self, _list_widget, item):
                    self.rows.pop(item, None)

                def add_active_item(self, data, **_kwargs):
                    self.added.append(dict(data))
                    self.rows["added-item"] = dict(data)
                    return object(), None

            for include_target in (False, True):
                with self.subTest(preexisting_target=include_target):
                    harness = Harness(include_target)
                    result = harness._apply_backend_active_upsert(
                        {"item": {"active_item_id": target_record_id, "payload": target}}
                    )

                    self.assertTrue(result.get("updated"))
                    self.assertEqual(harness.added, [])
                    self.assertEqual(len(harness.rows), 1)
                    current = next(iter(harness.rows.values()))
                    self.assertEqual(current.get("active_item_id"), target_record_id)
                    self.assertEqual(current.get("source_record_id"), source_record_id)
                    self.assertEqual(current.get("target_record_id"), target_record_id)

    def test_canonical_active_guard_prioritizes_work_type_and_strong_ids(self):
        shared_target_id = "rec-cross-work-type"
        change = {
            "active_item_id": "active-change-canonical",
            "record_id": shared_target_id,
            "target_record_id": shared_target_id,
            "source_record_id": "source-change-canonical",
            "notice_type": "变更通告",
            "work_type": "change",
            "progress": "CHANGE",
            "text": "【变更通告】状态：更新\n【进度】CHANGE",
        }
        maintenance = {
            "active_item_id": "active-maintenance-collision",
            "record_id": shared_target_id,
            "target_record_id": shared_target_id,
            "source_record_id": "source-maintenance-collision",
            "notice_type": "维保通告",
            "work_type": "maintenance",
            "progress": "MAINTENANCE",
            "text": "【维保通告】状态：更新\n【进度】MAINTENANCE",
        }
        strong_a = {
            "active_item_id": "active-strong-a",
            "record_id": "rec-strong-a",
            "target_record_id": "rec-strong-a",
            "source_record_id": "source-strong-a",
            "zhihang_record_id": "zhihang-shared-collision",
            "notice_type": "变更通告",
            "work_type": "change",
            "progress": "A",
            "text": "【变更通告】状态：更新\n【进度】A",
        }
        strong_b = {
            "active_item_id": "active-strong-b",
            "record_id": "rec-strong-b",
            "target_record_id": "rec-strong-b",
            "source_record_id": "source-strong-b",
            "zhihang_record_id": "zhihang-shared-collision",
            "notice_type": "变更通告",
            "work_type": "change",
            "progress": "B",
            "text": "【变更通告】状态：更新\n【进度】B",
        }
        cases = (
            ("cross_work_type", change, [maintenance, change]),
            ("conflicting_strong_ids", strong_a, [strong_b, strong_a]),
        )

        for label, incoming, canonical_payloads in cases:
            with self.subTest(case=label):
                rows = [
                    {
                        "active_item_id": item["active_item_id"],
                        "record_id": item["target_record_id"],
                        "origin": "portal",
                        "payload": dict(item),
                    }
                    for item in canonical_payloads
                ]
                state_store = type(
                    "StateStore",
                    (),
                    {"list_visible_qt_active_items": lambda _self: rows},
                )()
                harness = _ActiveUpsertVisibilityHarness()
                harness.cache_store = type(
                    "CacheStore",
                    (),
                    {"_state_store": state_store},
                )()
                harness._canonical_backend_active_payload = (
                    lambda data, **_kwargs: dict(data)
                )

                result = harness._apply_backend_active_upsert(
                    {
                        "item": {
                            "active_item_id": incoming["active_item_id"],
                            "record_id": incoming["target_record_id"],
                            "origin": "qt_event",
                            "payload": dict(incoming),
                        }
                    }
                )

                self.assertTrue(result["ok"])
                self.assertEqual(len(harness.added), 1)
                applied = harness.added[0]
                self.assertEqual(applied["work_type"], incoming["work_type"])
                self.assertEqual(
                    applied["active_item_id"], incoming["active_item_id"]
                )
                self.assertEqual(
                    applied["target_record_id"], incoming["target_record_id"]
                )
                self.assertEqual(
                    applied["source_record_id"], incoming["source_record_id"]
                )
                self.assertEqual(applied["progress"], incoming["progress"])

    def test_target_delete_migrates_qt_to_live_source_fallback_without_removal(self):
        source_record_id = "source-live-after-target-end"
        target_record_id = "target-ended-with-live-source"
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            source_only = {
                "active_item_id": f"source-change-{source_record_id}",
                "source_record_id": source_record_id,
                "notice_type": "变更通告",
                "work_type": "change",
                "source_snapshot_authoritative": True,
                "text": "【变更通告】状态：更新\n【名称】源表仍进行中",
            }
            store.upsert_qt_active_item(
                source_only,
                section="other",
                origin="source_snapshot_refresh",
            )
            harness = _CanonicalActiveDeleteHarness(
                store,
                {
                    **source_only,
                    "active_item_id": target_record_id,
                    "record_id": target_record_id,
                    "target_record_id": target_record_id,
                },
            )

            result = harness._apply_backend_active_delete(
                {
                    "active_item_id": target_record_id,
                    "record_id": target_record_id,
                    "target_record_id": target_record_id,
                    "source_record_id": source_record_id,
                }
            )

            self.assertFalse(result.get("deleted", False))
            self.assertEqual(harness.removed, [])
            self.assertEqual(harness.current["source_record_id"], source_record_id)
            self.assertFalse(harness.current.get("target_record_id"))


    def test_event_parser_accepts_long_source_and_level_labels(self):
        text = (
            "【事件通告】状态：开始\n"
            "【标题】A楼冷机告警\n"
            "【事件发生时间】2026-06-24 10:00\n"
            "【机楼】A楼\n"
            "【事件等级】I2\n"
            "【事件发现来源】BMS"
        )

        info = extract_notice_info(text)

        self.assertIsNotNone(info)
        self.assertEqual(info["source"], "BMS")
        self.assertEqual(info["level"], "I2")
        self.assertIn("BMS", info["unique_key"])
        self.assertIn("I2", info["unique_key"])

    def test_event_parser_accepts_alarm_description_as_title(self):
        text = (
            "【事件通告】状态：开始\n"
            "【告警描述】BMS报A楼冷机高压告警\n"
            "【事件发生时间】2026-06-24 10:00\n"
            "【机楼】A楼\n"
            "【事件等级】I2\n"
            "【事件发现来源】BMS"
        )

        info = extract_notice_info(text)

        self.assertIsNotNone(info)
        self.assertEqual(info["title"], "BMS报A楼冷机高压告警")
        self.assertEqual(info["source"], "BMS")
        self.assertEqual(info["level"], "I2")

    def test_backend_event_clipboard_entry_does_not_use_clipboard_as_event_source(self):
        text = (
            "【事件通告】状态：开始\n"
            "【标题】A楼冷机告警\n"
            "【事件发生时间】2026-06-24 10:00\n"
            "【机楼】A楼\n"
            "【事件等级】I2"
        )

        entry = FastAPIPortalController._clipboard_entry_from_content(text)

        self.assertIsNotNone(entry)
        self.assertEqual(entry["source"], "")
        self.assertEqual(entry["origin"], "clipboard")

    def test_event_active_update_inherits_existing_target_record_id(self):
        existing = {
            "active_item_id": "event-active-1",
            "record_id": "rec_event_target_1",
            "target_record_id": "rec_event_target_1",
            "_is_placeholder_record": False,
            "event_source": "BMS",
            "source": "BMS",
            "event_identity_key": "事件|2026-06-24 10:00|A楼|BMS|I2",
            "event_match_fields": {
                "title": "A楼冷机告警",
                "event_time": "2026-06-24 10:00",
                "building": "A楼",
                "source": "BMS",
                "level": "I2",
            },
            "site_photo_count": 1,
            "extra_image_count": 2,
        }
        incoming = {
            "active_item_id": "localid_event_active_1",
            "record_id": "localid_event_update_1",
            "target_record_id": "localid_event_update_1",
            "_is_placeholder_record": True,
            "notice_type": "事件通告",
            "text": "【事件通告】状态：更新\n【标题】A楼冷机告警",
        }

        updated = _RecordsHarness()._inherit_active_runtime_fields(incoming, existing)

        self.assertEqual(updated["active_item_id"], "event-active-1")
        self.assertEqual(updated["record_id"], "rec_event_target_1")
        self.assertEqual(updated["target_record_id"], "rec_event_target_1")
        self.assertFalse(updated["_is_placeholder_record"])
        self.assertEqual(updated["event_source"], "BMS")
        self.assertEqual(updated["source"], "BMS")
        self.assertEqual(updated["event_identity_key"], existing["event_identity_key"])
        self.assertEqual(updated["event_match_fields"], existing["event_match_fields"])
        self.assertEqual(updated["site_photo_count"], 1)
        self.assertEqual(updated["extra_image_count"], 2)

    def test_notice_text_projection_covers_all_non_event_work_types(self):
        cases = [
            (
                "maintenance",
                (
                    "【维保通告】状态：开始\n"
                    "【名称】EA118机房B楼过滤网维护\n"
                    "【时间】2026-06-18 09:00~2026-06-18 18:00\n"
                    "【位置】B楼空调间\n"
                    "【内容】更换过滤网\n"
                    "【原因】周期维保\n"
                    "【影响】无影响\n"
                    "【进度】准备完成"
                ),
                {"location": "B楼空调间", "content": "更换过滤网", "reason": "周期维保", "impact": "无影响", "progress": "准备完成"},
            ),
            (
                "repair",
                (
                    "【设备检修】状态：更新\n"
                    "【标题】EA118_C01机房D楼直流屏系统总故障告警检修\n"
                    "【地点】D-178配电室\n"
                    "【紧急程度】低\n"
                    "【专业】电气\n"
                    "【发现故障时间】2026-06-18 10:44\n"
                    "【期望完成时间】2026-06-18 23:50\n"
                    "【维修设备】D-178-AD001\n"
                    "【维修故障】直流屏系统总故障\n"
                    "【故障类型】设备故障\n"
                    "【维修方式】自维\n"
                    "【影响范围】无影响\n"
                    "【故障发现方式】告警发现\n"
                    "【故障现象】系统总故障\n"
                    "【故障原因】BMS告警\n"
                    "【解决方案】检查直流屏\n"
                    "【备件更换情况】无\n"
                    "【完成情况】处理中"
                ),
                {"location": "D-178配电室", "repair_device": "D-178-AD001", "fault_type": "设备故障", "repair_mode": "自维", "discovery": "告警发现", "symptom": "系统总故障", "progress": "处理中"},
            ),
            (
                "power",
                (
                    "【上电通告】状态：开始\n"
                    "【名称】EA118机房E楼设备上电通告\n"
                    "【时间】2026-06-18 09:00~2026-06-18 18:00\n"
                    "【柜号】E-201 B01\n"
                    "【数量】2个\n"
                    "【进度】准备上电"
                ),
                {"cabinet": "E-201 B01", "quantity": "2个", "progress": "准备上电"},
            ),
            (
                "polling",
                (
                    "【设备轮巡】状态：开始\n"
                    "【标题】EA118机房C楼制冷单元轮巡通告\n"
                    "【时间】2026-06-18 09:00~2026-06-18 18:00\n"
                    "【设备】C-127制冷单元\n"
                    "【内容】3号轮巡至2号运行\n"
                    "【影响】无影响\n"
                    "【进度】准备完成"
                ),
                {"device": "C-127制冷单元", "content": "3号轮巡至2号运行", "impact": "无影响", "progress": "准备完成"},
            ),
            (
                "adjust",
                (
                    "【设备调整】状态：开始\n"
                    "【名称】EA118机房H楼空调调整通告\n"
                    "【时间】2026-06-18 09:00~2026-06-18 18:00\n"
                    "【位置】H-440空调间\n"
                    "【内容】调整空调参数\n"
                    "【原因】环境优化\n"
                    "【影响】无影响\n"
                    "【进度】准备完成"
                ),
                {"location": "H-440空调间", "content": "调整空调参数", "reason": "环境优化", "impact": "无影响", "progress": "准备完成"},
            ),
        ]
        for work_type, text, expected in cases:
            with self.subTest(work_type=work_type):
                fields = FastAPIPortalController._projected_notice_fields_from_text(text)
                self.assertEqual(fields["work_type"], work_type)
                for key, value in expected.items():
                    self.assertEqual(fields.get(key), value)

    def test_clipboard_projection_keeps_full_change_fields_and_normalizes_heading(self):
        current_month = dt.datetime.now().strftime("%Y-%m")
        text = (
            "【变更通告】状态：开始\n"
            "【名称】EA118机房A楼蓄电池测试变更\n"
            "【等级】I3\n"
            f"【时间】{current_month}-18 09:00~{current_month}-18 18:00\n"
            "【位置】A-245配电室\n"
            "【内容】工程师对蓄电池进行测试\n"
            "【原因】容量测试\n"
            "【影响】对IT业务无影响\n"
            "【进度】准备工作已完成"
        )
        with tempfile.TemporaryDirectory() as tmp:
            original_store = PortalRuntime.state_store
            PortalRuntime.state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            try:
                entry = FastAPIPortalController._clipboard_entry_from_content(text)
                self.assertIsNotNone(entry)
                result = FastAPIPortalController._project_clipboard_entry_to_active(entry or {})
                payload = result["item"]["payload"]
                self.assertEqual(payload["notice_type"], "变更通告")
                self.assertEqual(payload["work_type"], "change")
                self.assertEqual(payload["location"], "A-245配电室")
                self.assertEqual(payload["content"], "工程师对蓄电池进行测试")
                self.assertEqual(payload["reason"], "容量测试")
                self.assertEqual(payload["impact"], "对IT业务无影响")
                self.assertEqual(payload["progress"], "准备工作已完成")
                self.assertEqual(payload["start_time"], f"{current_month}-18 09:00")
                self.assertEqual(payload["end_time"], f"{current_month}-18 18:00")

                ongoing = FastAPIPortalController._get_ongoing("A")
                self.assertEqual(len(ongoing), 1)
                self.assertEqual(ongoing[0]["location"], "A-245配电室")
                self.assertEqual(ongoing[0]["content"], "工程师对蓄电池进行测试")
                self.assertEqual(ongoing[0]["impact"], "对IT业务无影响")
                self.assertEqual(ongoing[0]["progress"], "准备工作已完成")
            finally:
                PortalRuntime.state_store = original_store

    def test_deleted_clipboard_event_recreates_on_new_then_accepts_busy_update(self):
        first_text = (
            "【事件通告】状态：新增\n"
            "【标题】EA118机房C楼I3级事件通报\n"
            "【来源】BMS发现\n"
            "【时间】2026-08-15 10:00\n"
            "【概述】C楼空调间漏水告警\n"
            "【进展】首次内容"
        )
        changed_text = first_text.replace("状态：新增", "状态：更新").replace(
            "首次内容", "重新复制后的新内容"
        )
        with tempfile.TemporaryDirectory() as tmp:
            original_store = PortalRuntime.state_store
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            PortalRuntime.state_store = store
            try:
                first_entry = FastAPIPortalController._clipboard_entry_from_content(
                    first_text
                )
                changed_entry = FastAPIPortalController._clipboard_entry_from_content(
                    changed_text
                )

                first = FastAPIPortalController._project_clipboard_entry_to_active(
                    first_entry
                )
                removed = PortalRuntime.execute_local_remove_active_item(
                    {
                        "active_item_id": first["active_item_id"],
                        "record_id": first["record_id"],
                        "notice_type": "事件通告",
                        "work_type": "event",
                    }
                )
                self.assertTrue(removed["ok"])
                early_update = FastAPIPortalController._project_clipboard_entry_to_active(
                    changed_entry
                )
                self.assertTrue(early_update.get("ignored"))
                self.assertEqual(store.list_visible_qt_active_items(), [])

                recreated = FastAPIPortalController._project_clipboard_entry_to_active(
                    first_entry
                )
                self.assertTrue(recreated["ok"])
                self.assertEqual(recreated["active_item_id"], first["active_item_id"])

                changed_entry["qt_upload_in_progress"] = True
                changed_entry["qt_active_item_id"] = recreated["active_item_id"]
                queued = FastAPIPortalController._project_clipboard_entry_to_active(
                    changed_entry
                )

                self.assertTrue(queued["ok"])
                visible = store.list_visible_qt_active_items()
                self.assertEqual(len(visible), 1)
                self.assertEqual(
                    visible[0]["active_item_id"],
                    recreated["active_item_id"],
                )
                self.assertIn("重新复制后的新内容", visible[0]["payload"]["text"])
            finally:
                PortalRuntime.state_store = original_store

    def test_deleted_clipboard_event_identical_new_recreates_local_item(self):
        text = (
            "【事件通告】状态：新增\n"
            "【标题】EA118机房D楼I3级事件通报\n"
            "【来源】BMS发现\n"
            "【时间】2026-08-15 10:30\n"
            "【概述】D楼空调告警\n"
            "【进展】值班工程师已前往现场"
        )
        with tempfile.TemporaryDirectory() as tmp:
            original_store = PortalRuntime.state_store
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            PortalRuntime.state_store = store
            try:
                entry = FastAPIPortalController._clipboard_entry_from_content(text)
                first = FastAPIPortalController._project_clipboard_entry_to_active(entry)
                removed = PortalRuntime.execute_local_remove_active_item(
                    {
                        "active_item_id": first["active_item_id"],
                        "record_id": first["record_id"],
                        "notice_type": "事件通告",
                        "work_type": "event",
                    }
                )

                replayed = FastAPIPortalController._project_clipboard_entry_to_active(
                    entry
                )

                self.assertTrue(removed["ok"])
                self.assertTrue(replayed["ok"])
                self.assertFalse(replayed.get("ignored", False))
                self.assertEqual(replayed["active_item_id"], first["active_item_id"])
                visible = store.list_visible_qt_active_items()
                self.assertEqual(len(visible), 1)
                payload = visible[0]["payload"]
                self.assertTrue(payload["_is_placeholder_record"])
                self.assertFalse(payload.get("target_record_id"))
                self.assertTrue(str(payload["record_id"]).startswith("local_"))
            finally:
                PortalRuntime.state_store = original_store

    def test_deleted_uploaded_event_new_does_not_reuse_old_target(self):
        text = (
            "【事件通告】状态：新增\n"
            "【标题】EA118机房E楼I2级事件通报\n"
            "【来源】BMS发现\n"
            "【时间】2026-08-15 11:00\n"
            "【概述】E楼冷机故障"
        )
        with tempfile.TemporaryDirectory() as tmp:
            original_store = PortalRuntime.state_store
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            PortalRuntime.state_store = store
            try:
                entry = FastAPIPortalController._clipboard_entry_from_content(text)
                first = FastAPIPortalController._project_clipboard_entry_to_active(entry)
                old_target = "rec-deleted-uploaded-event"
                bound = {
                    **first["item"]["payload"],
                    "record_id": old_target,
                    "target_record_id": old_target,
                    "record_version": "old-version",
                    "expected_record_version": "old-version",
                    "binding_status": "bound",
                    "_is_placeholder_record": False,
                }
                self.assertTrue(
                    store.upsert_qt_active_item(
                        bound,
                        section="event",
                        origin="qt_upload",
                    )
                )
                removed = PortalRuntime.execute_local_remove_active_item(bound)
                self.assertTrue(removed["ok"])

                recreated = FastAPIPortalController._project_clipboard_entry_to_active(
                    entry
                )

                self.assertTrue(recreated["ok"])
                payload = recreated["item"]["payload"]
                self.assertEqual(recreated["active_item_id"], first["active_item_id"])
                self.assertNotEqual(recreated["record_id"], old_target)
                self.assertTrue(str(recreated["record_id"]).startswith("local_"))
                self.assertFalse(payload.get("target_record_id"))
                self.assertTrue(payload["_is_placeholder_record"])
                self.assertNotIn("record_version", payload)
                self.assertNotIn("expected_record_version", payload)
                self.assertNotIn("binding_status", payload)
            finally:
                PortalRuntime.state_store = original_store

    def test_deleted_event_recreated_before_delete_ack_drops_old_target(self):
        text = (
            "【事件通告】状态：新增\n"
            "【标题】EA118机房E楼I2级事件通报\n"
            "【来源】BMS发现\n"
            "【时间】2026-08-15 11:20\n"
            "【概述】E楼冷机故障"
        )
        with tempfile.TemporaryDirectory() as tmp:
            original_store = PortalRuntime.state_store
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            PortalRuntime.state_store = store
            try:
                entry = FastAPIPortalController._clipboard_entry_from_content(text)
                first = FastAPIPortalController._project_clipboard_entry_to_active(entry)
                old_target = "rec-delete-ack-pending"
                bound = {
                    **first["item"]["payload"],
                    "record_id": old_target,
                    "target_record_id": old_target,
                    "_is_placeholder_record": False,
                }
                self.assertTrue(store.upsert_qt_active_item(bound, section="event"))
                self.assertTrue(
                    store.delete_qt_active_item(
                        active_item_id=first["active_item_id"],
                        record_id=old_target,
                    )
                )

                recreated = FastAPIPortalController._project_clipboard_entry_to_active(entry)

                payload = recreated["item"]["payload"]
                self.assertNotEqual(recreated["record_id"], old_target)
                self.assertTrue(str(recreated["record_id"]).startswith("local_"))
                self.assertFalse(payload.get("target_record_id"))
                identity = store.resolve_notice_identity(
                    work_type="event",
                    active_item_id=first["active_item_id"],
                )
                self.assertFalse((identity or {}).get("target_record_id"))
            finally:
                PortalRuntime.state_store = original_store

    def test_event_clipboard_projection_reuses_existing_target_record_by_event_identity(self):
        current_month = dt.datetime.now().strftime("%Y-%m")
        first_text = (
            "【事件通告】状态：开始\n"
            "【标题】D楼直流屏系统总故障\n"
            f"【时间】{current_month}-24 10:00\n"
            "【机楼】D楼\n"
            "【来源】BMS\n"
            "【等级】I2"
        )
        update_text = (
            "【事件通告】状态：更新\n"
            "【标题】D楼直流屏系统总故障\n"
            f"【时间】{current_month}-24 10:00\n"
            "【机楼】D楼\n"
            "【来源】BMS\n"
            "【等级】I2\n"
            "【进展】处理中"
        )
        with tempfile.TemporaryDirectory() as tmp:
            original_store = PortalRuntime.state_store
            PortalRuntime.state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            try:
                PortalRuntime.state_store.upsert_qt_active_item(
                    {
                        "active_item_id": "event-active-1",
                        "record_id": "rec-event-target",
                        "target_record_id": "rec-event-target",
                        "notice_type": "事件通告",
                        "work_type": "event",
                        "title": "D楼直流屏系统总故障",
                        "text": first_text,
                        "_is_placeholder_record": False,
                    },
                    section="event",
                    origin="clipboard",
                )

                entry = FastAPIPortalController._clipboard_entry_from_content(update_text)
                self.assertIsNotNone(entry)
                result = FastAPIPortalController._project_clipboard_entry_to_active(entry or {})

                self.assertEqual(result["active_item_id"], "event-active-1")
                self.assertEqual(result["record_id"], "rec-event-target")
                items = PortalRuntime.state_store.list_qt_active_items()
                self.assertEqual(len(items), 1)
                self.assertEqual(items[0]["active_item_id"], "event-active-1")
                self.assertEqual(items[0]["record_id"], "rec-event-target")
                payload = items[0]["payload"]
                self.assertEqual(payload["target_record_id"], "rec-event-target")
                self.assertIn("状态：更新", payload["text"])
            finally:
                PortalRuntime.state_store = original_store

    def test_event_target_snapshot_keeps_stable_clipboard_active_id(self):
        stable_active_id = "e3a873cda97073fa9897e90c3ee5ca62"
        target_record_id = "rec-event-stable-target"
        start_text = (
            "【事件通告】状态：新增\n"
            "【标题】EA118机房B楼I3级事件通报\n"
            "【来源】巡检发现\n"
            "【时间】2026-08-11 09:37\n"
            "【概述】B-127冷冻站补水管道渗水"
        )
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            store.upsert_qt_active_item(
                {
                    "active_item_id": stable_active_id,
                    "record_id": f"local_{stable_active_id}",
                    "notice_type": "事件通告",
                    "work_type": "event",
                    "text": start_text,
                    "_is_placeholder_record": True,
                },
                section="event",
                origin="clipboard",
            )
            store.upsert_notice_identity(
                {
                    "active_item_id": stable_active_id,
                    "record_id": target_record_id,
                    "target_record_id": target_record_id,
                    "notice_type": "事件通告",
                    "work_type": "event",
                    "text": start_text,
                    "_is_placeholder_record": False,
                },
                origin="qt_upload",
            )

            store.upsert_qt_active_item(
                {
                    "active_item_id": target_record_id,
                    "record_id": target_record_id,
                    "target_record_id": target_record_id,
                    "notice_type": "事件通告",
                    "work_type": "event",
                    "text": start_text,
                    "_is_placeholder_record": False,
                },
                section="event",
                origin="target_snapshot_refresh",
            )

            items = store.list_visible_qt_active_items()
            identity = store.resolve_notice_identity(
                work_type="event",
                target_record_id=target_record_id,
            )
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["active_item_id"], stable_active_id)
            self.assertEqual(items[0]["payload"]["active_item_id"], stable_active_id)
            self.assertEqual(items[0]["record_id"], target_record_id)
            self.assertIsNotNone(identity)
            self.assertEqual(identity["active_item_id"], stable_active_id)

    def test_runtime_live_event_delta_wins_over_stale_target_snapshot(self):
        target_record_id = "rec-event-canonical-race"
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            store.upsert_qt_active_item(
                {
                    "active_item_id": target_record_id,
                    "record_id": target_record_id,
                    "target_record_id": target_record_id,
                    "notice_type": "事件通告",
                    "work_type": "event",
                    "text": "【事件通告】状态：新增\n【概述】旧进展",
                },
                section="event",
                origin="target_snapshot_refresh",
            )
            harness = _ActiveUpsertVisibilityHarness()
            harness.cache_store = type(
                "CacheStore",
                (),
                {"_state_store": store},
            )()

            merged = harness._canonical_backend_active_payload(
                {
                    "active_item_id": "stable-event-ui-id",
                    "record_id": target_record_id,
                    "target_record_id": target_record_id,
                    "notice_type": "事件通告",
                    "work_type": "event",
                    "text": "【事件通告】状态：更新\n【概述】最新进展",
                }
            )

            self.assertEqual(merged["active_item_id"], "stable-event-ui-id")
            self.assertIn("状态：更新", merged["text"])
            self.assertIn("最新进展", merged["text"])

    def test_qt_identity_keeps_backend_event_identity_key(self):
        strict_identity = "event:strict:summary-time-building-source-level"
        ensured = _RecordsHarness()._ensure_active_item_identity(
            {
                "active_item_id": "event-stable-id",
                "notice_type": "事件通告",
                "event_identity_key": strict_identity,
                "text": (
                    "【事件通告】状态：更新\n"
                    "【标题】EA118机房B楼I3级事件通报\n"
                    "【来源】巡检发现\n"
                    "【时间】2026-08-11 09:37\n"
                    "【概述】B-127冷冻站补水管道渗水"
                ),
            }
        )

        self.assertEqual(ensured["event_identity_key"], strict_identity)
        self.assertTrue(ensured.get("match_key"))

    def test_event_clipboard_update_and_end_inherit_dialog_fields_and_target(self):
        current_month = dt.datetime.now().strftime("%Y-%m")
        first_text = (
            "【事件通告】状态：开始\n"
            "【标题】EA118机房B楼I3级事件通报\n"
            "【来源】BMS系统\n"
            f"【时间】{current_month}-24 10:00\n"
            "【概述】BMS报B-301支路功率过高报警\n"
            "【进展】值班工程师正在前往查看"
        )
        with tempfile.TemporaryDirectory() as tmp:
            original_store = PortalRuntime.state_store
            PortalRuntime.state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            try:
                PortalRuntime.state_store.upsert_qt_active_item(
                    {
                        "active_item_id": "event-active-sparse",
                        "record_id": "rec-event-sparse",
                        "target_record_id": "rec-event-sparse",
                        "notice_type": "事件通告",
                        "work_type": "event",
                        "title": "EA118机房B楼I3级事件通报",
                        "text": first_text,
                        "building_codes": ["B"],
                        "level": "I3",
                        "source": "BMS系统",
                        "event_source": "BMS系统",
                        "_is_placeholder_record": False,
                    },
                    section="event",
                    origin="clipboard",
                )

                for status in ("更新", "结束"):
                    text = (
                        f"【事件通告】状态：{status}\n"
                        "【标题】EA118机房B楼I3级事件通报\n"
                        "【来源】BMS系统\n"
                        f"【时间】{current_month}-24 10:00\n"
                        "【概述】BMS报B-301支路功率过高报警\n"
                        f"【进展】事件{status}内容"
                    )
                    entry = FastAPIPortalController._clipboard_entry_from_content(text)
                    self.assertIsNotNone(entry)
                    self.assertEqual(entry.get("level"), "I3")

                    result = FastAPIPortalController._project_clipboard_entry_to_active(
                        entry or {}
                    )

                    self.assertFalse(result.get("ignored"))
                    self.assertEqual(result["active_item_id"], "event-active-sparse")
                    self.assertEqual(result["record_id"], "rec-event-sparse")
                    payload = result["item"]["payload"]
                    self.assertEqual(payload["target_record_id"], "rec-event-sparse")
                    self.assertEqual(payload["level"], "I3")
                    self.assertEqual(payload["event_source"], "BMS系统")
            finally:
                PortalRuntime.state_store = original_store

    def test_event_clipboard_update_reuses_target_snapshot_for_exact_user_sample(self):
        update_text = (
            "【事件通告】状态：更新\n"
            "【标题】EA118机房B楼I3级事件通报；\n"
            "【来源】巡检发现；\n"
            "【时间】2026年8月11日09:37分；\n"
            "【概述】巡检发现B-127冷冻站A区变频补水环网管道有渗水现象\n"
            "【影响】对IT业务暂无影响；\n"
            "【进展】人员已经到达现场，正在排查，请知晓"
        )
        with tempfile.TemporaryDirectory() as tmp:
            original_store = PortalRuntime.state_store
            PortalRuntime.state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            try:
                PortalRuntime.state_store.upsert_qt_active_item(
                    {
                        "active_item_id": "rec-event-target-snapshot",
                        "record_id": "rec-event-target-snapshot",
                        "target_record_id": "rec-event-target-snapshot",
                        "notice_type": "事件通告",
                        "work_type": "event",
                        "title": "巡检发现B-127冷冻站A区变频补水环网管道有渗水现象",
                        "start_time": "2026-08-11T09:37",
                        "time_str": "2026-08-11T09:37",
                        "building": "B楼",
                        "building_codes": ["B"],
                        "source": "巡检发现",
                        "event_source": "巡检发现",
                        "level": "I3",
                        "_is_placeholder_record": False,
                    },
                    section="event",
                    origin="target_snapshot_refresh",
                )

                entry = FastAPIPortalController._clipboard_entry_from_content(update_text)
                self.assertIsNotNone(entry)
                result = FastAPIPortalController._project_clipboard_entry_to_active(
                    entry or {}
                )

                self.assertFalse(result.get("ignored"))
                self.assertEqual(result["active_item_id"], "rec-event-target-snapshot")
                self.assertEqual(result["record_id"], "rec-event-target-snapshot")
                payload = result["item"]["payload"]
                self.assertEqual(payload["target_record_id"], "rec-event-target-snapshot")
                self.assertEqual(payload["building_codes"], ["B"])
                self.assertEqual(payload["event_source"], "巡检发现")
                self.assertEqual(payload["level"], "I3")
            finally:
                PortalRuntime.state_store = original_store

    def test_event_clipboard_exact_c_building_sample_updates_same_uploaded_item(self):
        texts = (
            (
                "【事件通告】状态：新增\n"
                "【标题】EA118机房C楼I3级事件通报\n"
                "【来源】BMS发现\n"
                "【时间】2026-08-11 13:24分\n"
                "【概述】BMS发现C楼311空调间漏水告警\n"
                "【影响】IT业务暂无影响\n"
                "【进展】1、值班工程师已前往现场查看,请知晓!@I3通报组"
            ),
            (
                "【事件通告】状态：更新\n"
                "【标题】EA118机房C楼I3级事件通报\n"
                "【来源】BMS发现\n"
                "【时间】2026-08-11 13:24分\n"
                "【概述】BMS发现C楼311空调间漏水告警\n"
                "【影响】IT业务暂无影响\n"
                "【进展】1、值班工程师已前往现场查看,请知晓!\n"
                "2、现场正在处理积水中@I3通报组"
            ),
            (
                "【事件通告】状态：结束\n"
                "【标题】EA118机房C楼I3级事件通报\n"
                "【来源】BMS发现\n"
                "【时间】2026-08-11 13:24分\n"
                "【概述】BMS发现C楼311空调间漏水告警\n"
                "【影响】IT业务暂无影响\n"
                "【进展】1、值班工程师已前往现场查看,请知晓!\n"
                "2、现场正在处理积水中\n"
                "3、现场为天花板积水，清理完成，告警已恢复，"
                "后续加强巡检@I3通报组"
            ),
        )
        target_record_id = "rec-event-c-building-sample"
        with tempfile.TemporaryDirectory() as tmp:
            original_store = PortalRuntime.state_store
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            PortalRuntime.state_store = store
            try:
                first_entry = FastAPIPortalController._clipboard_entry_from_content(
                    texts[0]
                )
                first = FastAPIPortalController._project_clipboard_entry_to_active(
                    first_entry or {}
                )
                stable_active_id = first["active_item_id"]
                first_payload = dict(first["item"]["payload"])
                bound_payload = {
                    **first_payload,
                    "record_id": target_record_id,
                    "target_record_id": target_record_id,
                    "_is_placeholder_record": False,
                }
                store.upsert_qt_active_item(
                    bound_payload,
                    section="event",
                    origin="qt_upload",
                )
                store.upsert_notice_identity(bound_payload, origin="qt_upload")

                for expected_status, text_value in zip(
                    ("更新", "结束"), texts[1:]
                ):
                    entry = FastAPIPortalController._clipboard_entry_from_content(
                        text_value
                    )
                    result = FastAPIPortalController._project_clipboard_entry_to_active(
                        entry or {}
                    )
                    payload = result["item"]["payload"]
                    self.assertEqual(result["active_item_id"], stable_active_id)
                    self.assertEqual(result["record_id"], target_record_id)
                    self.assertEqual(payload["target_record_id"], target_record_id)
                    self.assertEqual(payload["status"], expected_status)
                    self.assertEqual(payload["building_codes"], ["C"])
                    self.assertEqual(payload["event_source"], "BMS发现")
                    self.assertEqual(payload["level"], "I3")
                    self.assertEqual(len(store.list_visible_qt_active_items()), 1)
            finally:
                PortalRuntime.state_store = original_store

    def test_event_clipboard_summary_correction_updates_same_uploaded_item(self):
        start_text = (
            "【事件通告】状态：新增\n"
            "【标题】EA118机房A楼I3级事件通报\n"
            "【来源】BMS系统\n"
            "【时间】2026-08-21 10:00\n"
            "【概述】A楼空调压差告警"
        )
        update_text = (
            "【事件通告】状态：更新\n"
            "【标题】EA118机房A楼I3级事件通报\n"
            "【来源】BMS系统\n"
            "【时间】2026-08-21 10:00\n"
            "【概述】经现场确认改为A楼过滤器堵塞告警\n"
            "【进展】正在更换过滤器"
        )
        with tempfile.TemporaryDirectory() as tmp:
            original_store = PortalRuntime.state_store
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            PortalRuntime.state_store = store
            try:
                first = FastAPIPortalController._project_clipboard_entry_to_active(
                    FastAPIPortalController._clipboard_entry_from_content(start_text) or {}
                )
                bound = {
                    **first["item"]["payload"],
                    "record_id": "rec-event-summary-corrected",
                    "target_record_id": "rec-event-summary-corrected",
                    "_is_placeholder_record": False,
                }
                store.upsert_qt_active_item(bound, section="event", origin="qt_upload")

                result = FastAPIPortalController._project_clipboard_entry_to_active(
                    FastAPIPortalController._clipboard_entry_from_content(update_text) or {}
                )

                self.assertFalse(result.get("ignored"))
                self.assertEqual(result["active_item_id"], first["active_item_id"])
                self.assertEqual(result["record_id"], "rec-event-summary-corrected")
                self.assertIn("过滤器堵塞告警", result["item"]["payload"]["text"])
                self.assertEqual(len(store.list_visible_qt_active_items()), 1)
            finally:
                PortalRuntime.state_store = original_store

    def test_sparse_event_clipboard_update_refuses_ambiguous_targets(self):
        current_month = dt.datetime.now().strftime("%Y-%m")
        first_text = (
            "【事件通告】状态：开始\n"
            "【标题】EA118机房事件通报\n"
            "【来源】BMS系统\n"
            f"【时间】{current_month}-24 10:00\n"
            "【概述】公共告警描述"
        )
        update_text = first_text.replace("状态：开始", "状态：更新")
        with tempfile.TemporaryDirectory() as tmp:
            original_store = PortalRuntime.state_store
            PortalRuntime.state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            try:
                for code in ("A", "B"):
                    PortalRuntime.state_store.upsert_qt_active_item(
                        {
                            "active_item_id": f"event-active-{code}",
                            "record_id": f"rec-event-{code}",
                            "target_record_id": f"rec-event-{code}",
                            "notice_type": "事件通告",
                            "work_type": "event",
                            "text": f"{first_text}\n【进展】候选{code}",
                            "building_codes": [code],
                            "level": "I3",
                            "source": "BMS系统",
                            "event_source": "BMS系统",
                            "_is_placeholder_record": False,
                        },
                        section="event",
                        origin="clipboard",
                    )

                entry = FastAPIPortalController._clipboard_entry_from_content(update_text)
                result = FastAPIPortalController._project_clipboard_entry_to_active(
                    entry or {}
                )

                self.assertTrue(result.get("ignored"))
                self.assertEqual(len(PortalRuntime.state_store.list_qt_active_items()), 2)
                self.assertEqual(
                    len(PortalRuntime.state_store.list_visible_qt_active_items()),
                    2,
                )
            finally:
                PortalRuntime.state_store = original_store

    def test_qt_upload_result_binds_backend_active_item_before_next_update_projection(self):
        current_month = dt.datetime.now().strftime("%Y-%m")
        first_text = (
            "【事件通告】状态：开始\n"
            "【标题】D楼直流屏系统总故障\n"
            f"【时间】{current_month}-24 10:00\n"
            "【机楼】D楼\n"
            "【来源】BMS\n"
            "【等级】I2"
        )
        update_text = (
            "【事件通告】状态：更新\n"
            "【标题】D楼直流屏系统总故障\n"
            f"【时间】{current_month}-24 10:00\n"
            "【机楼】D楼\n"
            "【来源】BMS\n"
            "【等级】I2\n"
            "【进展】处理中"
        )
        with tempfile.TemporaryDirectory() as tmp:
            original_store = PortalRuntime.state_store
            original_service = PortalRuntime.service
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            service = MaintenancePortalService()
            service._state_store = store
            PortalRuntime.state_store = store
            PortalRuntime.service = service
            try:
                store.upsert_qt_active_item(
                    {
                        "active_item_id": "event-active-1",
                        "record_id": "local_event_active_1",
                        "target_record_id": "",
                        "notice_type": "事件通告",
                        "work_type": "event",
                        "title": "D楼直流屏系统总故障",
                        "text": first_text,
                        "_is_placeholder_record": True,
                    },
                    section="event",
                    origin="clipboard",
                )
                service._jobs["qt-job-1"] = {
                    "job_id": "qt-job-1",
                    "phase": "uploading",
                    "prepared": {
                        "action": "start",
                        "active_item_id": "event-active-1",
                        "record_id": "local_event_active_1",
                        "notice_type": "事件通告",
                        "work_type": "event",
                        "title": "D楼直流屏系统总故障",
                        "text": first_text,
                    },
                }

                controller = FastAPIPortalController()
                controller.mark_job_upload_result(
                    "qt-job-1",
                    success=True,
                    message="rec-event-target",
                    record_id="rec-event-target",
                    active_item_id="event-active-1",
                )

                items = store.list_qt_active_items()
                self.assertEqual(len(items), 1)
                self.assertEqual(items[0]["record_id"], "rec-event-target")
                payload = items[0]["payload"]
                self.assertEqual(payload["record_id"], "rec-event-target")
                self.assertEqual(payload["target_record_id"], "rec-event-target")
                self.assertFalse(payload["_is_placeholder_record"])

                entry = FastAPIPortalController._clipboard_entry_from_content(update_text)
                self.assertIsNotNone(entry)
                result = FastAPIPortalController._project_clipboard_entry_to_active(entry or {})
                self.assertEqual(result["active_item_id"], "event-active-1")
                self.assertEqual(result["record_id"], "rec-event-target")
                payload = result["item"]["payload"]
                self.assertEqual(payload["target_record_id"], "rec-event-target")
                self.assertNotIn("local_event_active_1", payload["record_id"])
            finally:
                PortalRuntime.state_store = original_store
                PortalRuntime.service = original_service

    def test_local_qt_upload_remember_target_updates_backend_active_item(self):
        current_month = dt.datetime.now().strftime("%Y-%m")
        first_text = (
            "【事件通告】状态：开始\n"
            "【标题】D楼直流屏系统总故障\n"
            f"【时间】{current_month}-24 10:00\n"
            "【机楼】D楼\n"
            "【来源】BMS\n"
            "【等级】I2"
        )
        update_text = (
            "【事件通告】状态：更新\n"
            "【标题】D楼直流屏系统总故障\n"
            f"【时间】{current_month}-24 10:00\n"
            "【机楼】D楼\n"
            "【来源】BMS\n"
            "【等级】I2\n"
            "【进展】处理中"
        )
        with tempfile.TemporaryDirectory() as tmp:
            original_store = PortalRuntime.state_store
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            PortalRuntime.state_store = store
            try:
                store.upsert_qt_active_item(
                    {
                        "active_item_id": "event-active-1",
                        "record_id": "local_event_active_1",
                        "target_record_id": "",
                        "notice_type": "事件通告",
                        "work_type": "event",
                        "title": "D楼直流屏系统总故障",
                        "text": first_text,
                        "_is_placeholder_record": True,
                    },
                    section="event",
                    origin="clipboard",
                )

                with patch(
                    "lan_bitable_template_portal.server.query_record_by_id",
                    return_value=(True, {"record_version": "version-1"}),
                ):
                    PortalRuntime._remember_local_upload_target(
                        {
                            "active_item_id": "event-active-1",
                            "record_id": "local_event_active_1",
                            "notice_type": "事件通告",
                            "work_type": "event",
                            "title": "D楼直流屏系统总故障",
                            "text": first_text,
                            "_is_placeholder_record": True,
                        },
                        notice_type="事件通告",
                        target_record_id="rec-event-target",
                    )

                items = store.list_qt_active_items()
                self.assertEqual(len(items), 1)
                self.assertEqual(items[0]["record_id"], "rec-event-target")
                self.assertEqual(items[0]["payload"]["target_record_id"], "rec-event-target")

                entry = FastAPIPortalController._clipboard_entry_from_content(update_text)
                self.assertIsNotNone(entry)
                result = FastAPIPortalController._project_clipboard_entry_to_active(entry or {})
                self.assertEqual(result["active_item_id"], "event-active-1")
                self.assertEqual(result["record_id"], "rec-event-target")
            finally:
                PortalRuntime.state_store = original_store

    def test_event_upload_success_preserves_newer_clipboard_generation(self):
        uploaded_text = (
            "【事件通告】状态：更新\n"
            "【标题】EA118机房A楼I3级事件通报\n"
            "【来源】BMS系统\n"
            "【时间】2026-08-21 10:00\n"
            "【概述】本次正在上传的旧内容"
        )
        next_text = uploaded_text.replace("状态：更新", "状态：结束").replace(
            "本次正在上传的旧内容",
            "上传期间新复制的结束内容",
        )
        with tempfile.TemporaryDirectory() as tmp:
            original_store = PortalRuntime.state_store
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            PortalRuntime.state_store = store
            try:
                store.upsert_qt_active_item(
                    {
                        "active_item_id": "event-active-generation",
                        "record_id": "rec-event-generation",
                        "target_record_id": "rec-event-generation",
                        "notice_type": "事件通告",
                        "work_type": "event",
                        "status": "结束",
                        "action": "end",
                        "record_version": "version-before",
                        "_is_placeholder_record": False,
                        "_has_unuploaded_changes": True,
                        "text": next_text,
                    },
                    section="event",
                    origin="clipboard",
                )

                with patch(
                    "lan_bitable_template_portal.server.external_real_write_guard",
                    return_value={"mock_external": True},
                ):
                    PortalRuntime._remember_local_upload_target(
                        {
                            "active_item_id": "event-active-generation",
                            "record_id": "rec-event-generation",
                            "target_record_id": "rec-event-generation",
                            "notice_type": "事件通告",
                            "work_type": "event",
                            "status": "更新",
                            "action": "update",
                            "record_version": "version-before",
                            "_is_placeholder_record": False,
                            "text": uploaded_text,
                        },
                        notice_type="事件通告",
                        target_record_id="rec-event-generation",
                        action="update",
                    )

                rows = store.list_visible_qt_active_items()
                self.assertEqual(len(rows), 1)
                payload = rows[0]["payload"]
                self.assertEqual(payload["text"], next_text)
                self.assertEqual(payload["status"], "结束")
                self.assertEqual(payload["action"], "end")
                self.assertTrue(payload["_has_unuploaded_changes"])
                self.assertTrue(payload["_queued_after_upload"])
                self.assertEqual(payload["target_record_id"], "rec-event-generation")
            finally:
                PortalRuntime.state_store = original_store

    def test_event_upload_does_not_queue_whitespace_only_self_projection(self):
        uploaded_text = (
            "【事件通告】状态：新增\n"
            "【标题】EA118机房E楼I2级事件通报\n"
            "【来源】BMS\n【概述】冷机故障"
        )
        with tempfile.TemporaryDirectory() as tmp:
            original_store = PortalRuntime.state_store
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            PortalRuntime.state_store = store
            try:
                store.upsert_qt_active_item(
                    {
                        "active_item_id": "event-self-projection",
                        "record_id": "local_event_self_projection",
                        "notice_type": "事件通告",
                        "work_type": "event",
                        "_is_placeholder_record": True,
                        "_has_unuploaded_changes": True,
                        "text": uploaded_text.replace("\n", "\r\n "),
                    },
                    section="event",
                    origin="clipboard",
                )

                with patch(
                    "lan_bitable_template_portal.server.external_real_write_guard",
                    return_value={"mock_external": True},
                ):
                    PortalRuntime._remember_local_upload_target(
                        {
                            "active_item_id": "event-self-projection",
                            "record_id": "local_event_self_projection",
                            "notice_type": "事件通告",
                            "work_type": "event",
                            "_is_placeholder_record": True,
                            "text": uploaded_text,
                        },
                        notice_type="事件通告",
                        target_record_id="rec-event-self-projection",
                        action="start",
                    )

                payload = store.list_visible_qt_active_items()[0]["payload"]
                self.assertFalse(payload["_has_unuploaded_changes"])
                self.assertNotIn("_queued_after_upload", payload)
            finally:
                PortalRuntime.state_store = original_store

    def test_event_clipboard_projection_recovers_target_from_identity_map(self):
        current_month = dt.datetime.now().strftime("%Y-%m")
        first_text = (
            "【事件通告】状态：新增\n"
            "【标题】D楼直流屏系统总故障\n"
            f"【时间】{current_month}-24 10:00\n"
            "【机楼】D楼\n"
            "【来源】BMS\n"
            "【等级】I2"
        )
        update_text = (
            "【事件通告】状态：更新\n"
            "【标题】D楼直流屏系统总故障\n"
            f"【时间】{current_month}-24 10:00\n"
            "【机楼】D楼\n"
            "【来源】BMS\n"
            "【等级】I2\n"
            "【进展】处理中"
        )
        with tempfile.TemporaryDirectory() as tmp:
            original_store = PortalRuntime.state_store
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            PortalRuntime.state_store = store
            try:
                store.upsert_notice_identity(
                    {
                        "active_item_id": "event-active-1",
                        "record_id": "rec-event-target",
                        "target_record_id": "rec-event-target",
                        "notice_type": "事件通告",
                        "work_type": "event",
                        "title": "D楼直流屏系统总故障",
                        "text": first_text,
                    },
                    origin="qt_upload",
                )

                entry = FastAPIPortalController._clipboard_entry_from_content(update_text)
                self.assertIsNotNone(entry)
                result = FastAPIPortalController._project_clipboard_entry_to_active(entry or {})

                self.assertFalse(result.get("ignored"))
                self.assertEqual(result["record_id"], "rec-event-target")
                payload = result["item"]["payload"]
                self.assertEqual(payload["target_record_id"], "rec-event-target")
                self.assertFalse(payload["_is_placeholder_record"])
            finally:
                PortalRuntime.state_store = original_store

    def test_record_not_found_variants_are_treated_as_missing_remote_record(self):
        self.assertTrue(PortalRuntime._remote_record_not_found("1254043-RecordIdNotFound"))
        self.assertTrue(PortalRuntime._remote_record_not_found("1254043-RecordldNotFo"))

    def test_sparse_qt_active_payload_is_backfilled_from_notice_text(self):
        current_month = dt.datetime.now().strftime("%Y-%m")
        text = (
            "【变更通告】状态：开始\n"
            "【名称】EA118机房A楼冷源设备变更\n"
            f"【时间】{current_month}-18 09:00~{current_month}-18 18:00\n"
            "【位置】A-127冷站\n"
            "【内容】调整冷源设备\n"
            "【原因】运行优化\n"
            "【影响】无业务影响\n"
            "【进度】执行中"
        )
        with tempfile.TemporaryDirectory() as tmp:
            original_store = PortalRuntime.state_store
            PortalRuntime.state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            try:
                PortalRuntime.state_store.upsert_qt_active_item(
                    {
                        "active_item_id": "sparse-change-1",
                        "record_id": "local_sparse-change-1",
                        "notice_type": "变更通告",
                        "work_type": "change",
                        "title": "EA118机房A楼冷源设备变更",
                        "text": text,
                    },
                    origin="clipboard",
                )
                ongoing = FastAPIPortalController._get_ongoing("A")
                self.assertEqual(len(ongoing), 1)
                item = ongoing[0]
                self.assertEqual(item["notice_type"], "变更通告")
                self.assertEqual(item["location"], "A-127冷站")
                self.assertEqual(item["content"], "调整冷源设备")
                self.assertEqual(item["reason"], "运行优化")
                self.assertEqual(item["impact"], "无业务影响")
                self.assertEqual(item["progress"], "执行中")
            finally:
                PortalRuntime.state_store = original_store

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

    def test_manual_clipboard_submission_reports_qt_queue_rejection(self):
        class Harness(MainWindowRuntimeMixin):
            def __init__(self):
                self.lan_template_portal_controller = _Controller()

            def _enqueue_ui_mutation(self, _name, _callback):
                return False

        result = Harness()._submit_notice_text_to_backend_projection(
            "【事件通告】状态：新增\n"
            "【标题】EA118机房C楼I3级事件通报\n"
            "【来源】BMS发现\n"
            "【时间】2026-08-15 11:00\n"
            "【概述】C楼空调告警"
        )

        self.assertFalse(result["ok"])
        self.assertIn("队列已满", result["error"])

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

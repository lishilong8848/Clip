# -*- coding: utf-8 -*-
"""Isolated regression tests for PortalRuntime._undo_restore_fields.

Covers:
  * batch_get text-segment arrays (and a single {text:...} dict) flattened to a
    string on the genuine free-text fields of each notice type.
  * Non-text values (attachment lists, empty list, building multiselect, time
    ms, plain strings, None, bool, single select) preserved unchanged on their
    own real fields. A person-object list is covered only as a shape-boundary
    check (the current non-event notices have no User field) proving that
    non-{text:...} objects are not mis-converted.
  * Unknown (non-whitelisted) fields are dropped; source snapshots never mutate.
  * A remote write failure in execute_notice_undo keeps the undo available and
    skips local restore (single-table integration check).

Isolation: this module must NOT mutate the whole process at import time (no
os.environ writes, no temp-dir creation, no rmtree registration). Isolation is
delegated to the subprocess that runs this module: the runner sets
CLIPFLOW_DATA_DIR for the child env and keeps the real-external-write guard at
confirmed=0. The integration fixture itself still uses a temporary SQLite within
the test. No credentials, business data, or network access.
"""
import copy
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.portal_service import (  # noqa: E402
    NOTICE_TYPE_ADJUST,
    NOTICE_TYPE_CHANGE,
    NOTICE_TYPE_MAINTENANCE,
    NOTICE_TYPE_POLLING,
    NOTICE_TYPE_POWER_DOWN,
    NOTICE_TYPE_POWER_UP,
    PortalError,
    WORK_TYPE_MAINTENANCE,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402
from lan_bitable_template_portal.server import PortalRuntime  # noqa: E402
from upload_event_module.config import get_field_config  # noqa: E402

OVERHAUL_NOTICE_TYPE = "设备检修"

NOTICE_TYPES = {
    "维保": [NOTICE_TYPE_MAINTENANCE],
    "变更": [NOTICE_TYPE_CHANGE],
    "检修": [OVERHAUL_NOTICE_TYPE],
    "轮巡": [NOTICE_TYPE_POLLING],
    "设备调整": [NOTICE_TYPE_ADJUST],
    "上下电": [NOTICE_TYPE_POWER_UP, NOTICE_TYPE_POWER_DOWN],
}

# Logical keys (get_field_config) that genuinely carry free text.
# maintenance_cycle / repair_mode / discovery are single-select, not free text.
TEXT_KEYS = {
    "name", "title", "content", "progress", "location", "reason", "impact",
    "symptom", "solution", "repair_fault", "alarm_desc",
}

TEXT_SEGMENT_CASES = [
    ([{"text": "a", "type": "text"}, {"text": "\nb", "type": "text"}], "a\nb"),
    ([{"text": "a"}, {"text": "\n\nb"}, {"text": "\n c "}], "a\n\nb\n c "),
    ([{"text": "【名称】"}, {"text": " 维保 A 楼"}, {"text": "\n\n【状态】开始"}],
     "【名称】 维保 A 楼\n\n【状态】开始"),
]

# (label, logical-key candidates, value) — the first existing candidate field is
# used for the notice type so each value lands on its real field kind.
NON_TEXT_CASES = [
    # Plain strings / empty string / None go on a genuine free-text field.
    ("普通字符串", ("content", "progress", "location", "name", "title"), "旧值"),
    ("空字符串", ("content", "progress", "location", "name", "title"), ""),
    ("None", ("content", "progress", "location", "name", "title"), None),
    # Single select fields (executor is itself a 厂维/自维 single select, so it
    # is intentionally NOT treated as a person field).
    ("单选", ("status", "specialty"), "进行中"),
    # Multiselect building.
    ("多选楼栋", ("building",), ["A", "B"]),
    ("布尔True", ("involves_change",), True),
    ("布尔False", ("involves_change",), False),
    ("附件列表",
     ("notice_images", "site_images", "work_order_attachments", "operation_photos",
      "start_snapshot", "update_snapshot", "end_snapshot", "response_snapshot",
      "progress_snapshot", "recover_snapshot"),
     [{"file_token": "x", "name": "x.png"}]),
    # Person-object list on a free-text field: shape-only boundary check that a
    # non-{text:...} object list is preserved unchanged. The current non-event
    # notices have no User field, so this is NOT actual person-field validation.
    ("人员对象形状(非text对象不误转换)",
     ("content", "progress", "location", "name", "title"),
     [{"id": "ou_fixture", "name": "test"}]),
    ("空列表", ("content", "progress", "location", "name", "title"), []),
    ("时间毫秒",
     ("plan_start", "actual_start", "start_time", "occurrence_time",
      "recover_time", "end_time"),
     1750000000000),
]

_ILLEGAL_FIELD = "绝不是白名单字段"
_FAKE_UNDO_ID = "undo-remote-fail-0001"


def _field_name(config, *logical_keys):
    """First whitelisted Chinese field name matching a logical key, else None."""
    for k in logical_keys:
        if k in config:
            return config[k]
    return None


def _text_field_names(config):
    return [config[k] for k in TEXT_KEYS if k in config]


def _non_text_cases(config):
    cases = []
    for label, candidates, value in NON_TEXT_CASES:
        field = _field_name(config, *candidates)
        if field is not None:
            cases.append((label, field, value))
    return cases


class NoticeUndoRestoreFieldsTests(unittest.TestCase):
    """Direct unit regression of PortalRuntime._undo_restore_fields."""

    def _iter_types(self):
        for notice_types in NOTICE_TYPES.values():
            for notice_type in notice_types:
                yield notice_type

    def test_text_segments_flatten_only_on_free_text_fields(self):
        for notice_type in self._iter_types():
            text_fields = _text_field_names(get_field_config(notice_type))
            self.assertTrue(text_fields, f"{notice_type} 应存在自由文本字段")
            for field in text_fields:
                for segments, expected in TEXT_SEGMENT_CASES:
                    with self.subTest(notice_type=notice_type, field=field,
                                      segments=segments):
                        restored = PortalRuntime._undo_restore_fields(
                            notice_type, {field: copy.deepcopy(segments)}
                        )
                        self.assertEqual(restored, {field: expected})
                with self.subTest(notice_type=notice_type, field=field, kind="single-dict"):
                    restored = PortalRuntime._undo_restore_fields(
                        notice_type, {field: {"text": "单段", "type": "text"}}
                    )
                    self.assertEqual(restored, {field: "单段"})

    def test_non_text_values_preserved_on_their_own_fields(self):
        for notice_type in self._iter_types():
            config = get_field_config(notice_type)
            for label, field, value in _non_text_cases(config):
                with self.subTest(notice_type=notice_type, label=label, field=field):
                    restored = PortalRuntime._undo_restore_fields(
                        notice_type, {field: copy.deepcopy(value)}
                    )
                    self.assertEqual(restored, {field: value})

    def test_fields_outside_whitelist_are_not_written(self):
        for notice_type in self._iter_types():
            text_field = _field_name(
                get_field_config(notice_type), "content", "progress", "location", "name", "title"
            )
            snapshot = {
                text_field: [{"text": "合法", "type": "text"}, {"text": "\n内容"}],
                _ILLEGAL_FIELD: [{"text": "不应写入", "type": "text"}],
            }
            with self.subTest(notice_type=notice_type):
                restored = PortalRuntime._undo_restore_fields(notice_type, snapshot)
                self.assertEqual(restored[text_field], "合法\n内容")
                self.assertNotIn(_ILLEGAL_FIELD, restored)

    def test_source_snapshot_is_not_mutated(self):
        snapshot = {
            "名称": [{"text": "a", "type": "text"}, {"text": "\n\nb ", "type": "text"}],
            "内容": "旧文本",
            "进度": 1750000000000,
            _ILLEGAL_FIELD: {"text": "非法"},
        }
        before = copy.deepcopy(snapshot)
        result = PortalRuntime._undo_restore_fields(NOTICE_TYPE_MAINTENANCE, snapshot)
        self.assertEqual(result["名称"], "a\n\nb ")
        self.assertEqual(result["内容"], "旧文本")
        self.assertEqual(result["进度"], 1750000000000)
        self.assertNotIn(_ILLEGAL_FIELD, result)
        self.assertEqual(snapshot, before)


class NoticeUndoExecuteRemoteFailureTests(unittest.TestCase):
    """execute_notice_undo: a remote write failure must not mark undone or restore locally."""

    def _make_service_and_store(self):
        previous = (PortalRuntime.service, PortalRuntime.state_store)
        self.addCleanup(self._restore_runtime, previous)
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        store = LanPortalStateStore(Path(tmpdir.name) / "state.sqlite3")
        self.addCleanup(store.shutdown_write_worker, timeout=2)
        service = mock.MagicMock()
        service._state_store = store
        PortalRuntime.service = service
        PortalRuntime.state_store = store
        return service, store

    def _restore_runtime(self, previous):
        PortalRuntime.service, PortalRuntime.state_store = previous

    def _seed_undo(self, store):
        return store.create_notice_undo_action({
            "undo_id": _FAKE_UNDO_ID,
            "identity_key": f"maintenance:target:{_FAKE_UNDO_ID}",
            "action_type": "update",
            "scope": "A",
            "work_type": WORK_TYPE_MAINTENANCE,
            "notice_type": NOTICE_TYPE_MAINTENANCE,
            "target_record_id": "rec-remote-fail",
            "remote": {
                "missing": False,
                "fields": {
                    "名称": [{"text": "a", "type": "text"}, {"text": "\nb", "type": "text"}],
                },
            },
        })

    def test_remote_write_failure_keeps_available_and_skips_local_restore(self):
        service, store = self._make_service_and_store()
        undo_id = self._seed_undo(store)
        service.paired_notice_undo.return_value = (
            store.get_notice_undo_action(undo_id), None
        )
        with mock.patch(
            "lan_bitable_template_portal.server.external_real_write_guard",
            return_value={
                "mock_external": False,
                "require_confirm": True,
                "confirmed": True,
                "real_write_allowed": True,
                "reason": "",
            },
        ), mock.patch(
            "lan_bitable_template_portal.server.update_bitable_record_fields",
            return_value=(False, "远端写入失败"),
        ) as update_mock:
            with self.assertRaisesRegex(PortalError, "远端写入失败"):
                PortalRuntime.execute_notice_undo(undo_id)

        self.assertTrue(update_mock.called)
        self.assertEqual(update_mock.call_args[0][2], {"名称": "a\nb"})
        stored = store.get_notice_undo_action(undo_id)
        self.assertEqual(stored["status"], "available")
        self.assertNotEqual(stored["status"], "undone")
        service.restore_notice_undo_local.assert_not_called()
        service.restore_notice_undo_related_remote.assert_not_called()


if __name__ == "__main__":
    unittest.main()
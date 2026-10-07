# -*- coding: utf-8 -*-
"""Contract tests for ``lighthouse_api._cabinet_edit_frontend_fields``.

The helper is a bounded, PURE descriptor builder for the cabinet-power batch
row editor.  It mirrors the original ``CabinetPowerBatchPage.vue`` row form and
the native ``cabinet_power_batches.EDITABLE_FIELDS`` /
``POWER_ACTIONS_BY_STATE`` constants (lazily imported; no business calls, no
mutations, no network/cloud/production access).

Covered assertions:
- returned descriptor is ``{type: object, native_cabinet_row_editor: True}`` and
  every child carries ``literal_key=True``;
- the editable field set is exactly ``EDITABLE_FIELDS | {excluded}`` (no
  private/internal keys like row_id/operation_id/version/files/tokens/edits);
- the ``scope`` select is limited to the passed scopes (A–E), others dropped;
- ``expected``/``actual`` are ``datetime-local`` with ``step=1``;
- ``failure_reason`` is a maxlength-1000 textarea, required, shown only when
  ``result == 失败`` (via the ``when`` clause matching the original editor);
- ``rack_type`` blank/网络机柜/服务器机柜 and ``type_resolution`` blank/
  keep_current/sync_current with Chinese labels;
- action options: notice exposes every native action; image/pdf/text restrict to
  ``POWER_ACTIONS_BY_STATE[current_power_state]``; an old invalid action is kept
  as a disabled option labelled with the original value.
"""
import sys
import unittest
from pathlib import Path

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.cabinet_power_batches import (  # noqa: E402
    EDITABLE_FIELDS,
    POWER_ACTIONS_BY_STATE,
)
from lan_bitable_template_portal.lighthouse_api import (  # noqa: E402
    _cabinet_edit_frontend_fields,
)

ALL_ACTIONS = sorted({action for actions in POWER_ACTIONS_BY_STATE.values() for action in actions})
PRIVATE_KEYS = {
    "row_id", "operation_id", "version", "created_at", "updated_at", "edits",
    "edit_audit", "source", "file_name", "raw_text", "original", "inference",
    "file_token", "file_tokens", "attachment_token", "evidence_images",
    "evidence_time_review", "current_power_state", "current_rack_type",
    "application_ids", "notice_id", "source_system_id", "last_modified",
}


def _children(result: dict) -> list:
    return result.get("children") or []


def _child(result: dict, path: str) -> dict:
    for child in _children(result):
        if child.get("path") == path:
            return child
    raise AssertionError(f"child {path!r} not found in {[c.get('path') for c in _children(result)]}")


def _row(**overrides):
    base = {
        "scope": "E", "room": "E101", "rack": "RACK-E1",
        "supplier_rack": "SR-E1", "rack_type": "网络机柜", "type_detail": "标准",
        "action": "上正式电", "expected": "", "actual": "",
        "result": "", "failure_reason": "", "type_resolution": "",
        "current_power_state": "off",
    }
    base.update(overrides)
    return base


class CabinetEditFieldSetTest(unittest.TestCase):
    def test_field_set_is_editable_fields_plus_excluded(self):
        result = _cabinet_edit_frontend_fields(_row(), "notice", ["A", "E"])
        paths = {child["path"] for child in _children(result)}
        self.assertEqual(paths, EDITABLE_FIELDS | {"excluded"})

    def test_every_child_is_literal_key_and_type_object(self):
        result = _cabinet_edit_frontend_fields(_row(), "manual", ["A"])
        self.assertEqual(result["type"], "object")
        self.assertTrue(result.get("native_cabinet_row_editor"))
        for child in _children(result):
            self.assertTrue(child.get("literal_key"), f"child {child['path']} lacks literal_key")

    def test_no_private_or_internal_editable_keys(self):
        result = _cabinet_edit_frontend_fields(_row(), "notice", ["A", "E"])
        paths = {child["path"] for child in _children(result)}
        self.assertTrue(paths.isdisjoint(PRIVATE_KEYS))
        # No child is itself an object holding credentials/audit metadata.
        for child in _children(result):
            self.assertNotIn("children", child, f"unexpected nested child {child['path']}")


class CabinetEditScopeTest(unittest.TestCase):
    def test_scope_select_limited_to_allowed_scopes(self):
        result = _cabinet_edit_frontend_fields(_row(), "notice", ["A", "c", "E", "F"])
        scope = _child(result, "scope")
        self.assertEqual(scope["type"], "select")
        values = [option["value"] for option in scope["options"]]
        self.assertEqual(values, ["A", "E"])  # F dropped, lowercase c dropped
        self.assertTrue(all(option["label"].endswith("楼") for option in scope["options"]))

    def test_scope_select_empty_when_no_scopes(self):
        result = _cabinet_edit_frontend_fields(_row(), "notice", [])
        self.assertEqual(_child(result, "scope")["options"], [])

    def test_scope_exact_membership_and_dedupe(self):
        # Regression: substring matching previously admitted "", "AB", "ABCDE"
        # and failed to dedupe.  Exact membership against A–E plus deterministic
        # first-occurrence dedupe must yield A/E only.
        result = _cabinet_edit_frontend_fields(
            _row(), "notice", ["A", "AB", "", "ABCDE", "A", "E"]
        )
        scope = _child(result, "scope")
        values = [option["value"] for option in scope["options"]]
        self.assertEqual(values, ["A", "E"])


class CabinetEditDateAndConditionalTest(unittest.TestCase):
    def test_datetime_local_step_one(self):
        result = _cabinet_edit_frontend_fields(_row(), "notice", ["E"])
        for field in ("expected", "actual"):
            self.assertEqual(_child(result, field)["type"], "datetime-local")
            self.assertEqual(_child(result, field)["step"], 1)

    def test_failure_reason_conditional_and_required(self):
        result = _cabinet_edit_frontend_fields(_row(), "notice", ["E"])
        reason = _child(result, "failure_reason")
        self.assertEqual(reason["type"], "textarea")
        self.assertEqual(reason["maxlength"], 1000)
        self.assertTrue(reason.get("required"))
        self.assertEqual(reason.get("when"), {"path": "result", "equals": "失败"})

    def test_result_and_type_resolution_options(self):
        result = _cabinet_edit_frontend_fields(_row(), "notice", ["E"])
        result_field = _child(result, "result")
        self.assertEqual(
            [option["value"] for option in result_field["options"]],
            ["", "成功", "失败"],
        )
        resolution = _child(result, "type_resolution")
        self.assertEqual(
            [option["value"] for option in resolution["options"]],
            ["", "keep_current", "sync_current"],
        )
        self.assertEqual(resolution["options"][1]["label"], "沿用当前机柜类型")
        self.assertEqual(resolution["options"][2]["label"], "同步修正当前机柜类型")

    def test_rack_type_blank_server_network(self):
        result = _cabinet_edit_frontend_fields(_row(), "notice", ["E"])
        rack_type = _child(result, "rack_type")
        self.assertEqual(
            [option["value"] for option in rack_type["options"]],
            ["", "网络机柜", "服务器机柜"],
        )

    def test_excluded_is_checkbox(self):
        result = _cabinet_edit_frontend_fields(_row(), "notice", ["E"])
        self.assertEqual(_child(result, "excluded")["type"], "checkbox")


class CabinetEditSourceActionTest(unittest.TestCase):
    def test_notice_exposes_all_native_actions(self):
        result = _cabinet_edit_frontend_fields(_row(), "notice", ["E"])
        action = _child(result, "action")
        values = [option["value"] for option in action["options"] if not option.get("disabled")]
        self.assertEqual(values, ["", *ALL_ACTIONS])

    def test_manual_exposes_all_native_actions(self):
        result = _cabinet_edit_frontend_fields(_row(), "manual", ["E"])
        action = _child(result, "action")
        values = [option["value"] for option in action["options"] if not option.get("disabled")]
        self.assertEqual(values, ["", *ALL_ACTIONS])

    def test_image_restricted_to_state_actions(self):
        expect = sorted(POWER_ACTIONS_BY_STATE["off"])
        result = _cabinet_edit_frontend_fields(_row(current_power_state="off"), "image", ["E"])
        action = _child(result, "action")
        values = [option["value"] for option in action["options"] if not option.get("disabled")]
        self.assertEqual(values, ["", *expect])

    def test_pdf_text_restricted_like_image(self):
        for src in ("pdf", "text"):
            for state, expect in POWER_ACTIONS_BY_STATE.items():
                result = _cabinet_edit_frontend_fields(_row(current_power_state=state), src, ["E"])
                action = _child(result, "action")
                values = [option["value"] for option in action["options"] if not option.get("disabled")]
                self.assertEqual(values, ["", *sorted(expect)], f"{src}/{state}")

    def test_old_invalid_action_kept_disabled(self):
        row = _row(current_power_state="formal", action="下正式电")
        # "下正式电" is valid for formal so force an invalid value instead.
        row["action"] = "上测试电"
        result = _cabinet_edit_frontend_fields(row, "image", ["E"])
        action = _child(result, "action")
        valid = [option["value"] for option in action["options"] if not option.get("disabled")]
        self.assertNotIn("上测试电", valid)
        old = next((o for o in action["options"] if o.get("value") == "上测试电"), None)
        self.assertIsNotNone(old)
        self.assertTrue(old.get("disabled"))
        self.assertIn("原文件值", old.get("label", ""))


if __name__ == "__main__":
    unittest.main()
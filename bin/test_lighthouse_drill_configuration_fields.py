# -*- coding: utf-8 -*-
"""Isolated unit tests for ``lighthouse_api._drill_configuration_frontend_fields``.

The helper is a bounded PURE metadata builder: it turns an already-parsed drill
template ``definition`` into a LighthouseStructuredField-compatible descriptor
with ``_initial_form`` (editable whitelisted values) and ``_mapping_paths``
(UI leaf -> original configuration internal paths).  It performs no cloud/file
access, adds no dependency, does not copy the native ``_validate_configuration``
and never mutates its input.

Tests use a precise isolated fixture and, in one integration test, the real
``DrillManagementService`` over ``test_drill_management._fixture_xlsx``.
"""
import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.lighthouse_api import (  # noqa: E402
    _drill_configuration_frontend_fields,
)
from lan_bitable_template_portal.lighthouse_ai import safe_data  # noqa: E402
from lan_bitable_template_portal.drill_management import (  # noqa: E402
    DrillManagementService,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402
from test_drill_management import _fixture_xlsx  # noqa: E402


def _definition() -> dict:
    """A precise, fully-detected template configuration."""
    return {
        "drill_id": "abc123",
        "name": "模板演练",
        "sheets": [{"name": "本月记录"}, {"name": "评估表"}],
        "configuration": {
            "record_sheet": "本月记录",
            "assessment_sheet": "评估表",
            "mapping": {
                "machine_room": "B5:E5",
                "drill_date": "F5:I5",
                "drill_name": "C6:E6",
                "area": "C7:E7",
                "scenario": "C9:I9",
                "commander": "C8:E8",
                "participants": "C10:I10",
                "predicted_total": "D16:E16",
                "actual_total": "G16:I16",
                "participant_signatures": "C17:E17",
                "recorder_signature": "G17:I17",
                "reviewer_signature": "C18:E18",
                "review_time": "G18:I18",
                "steps": {
                    "start_row": 13,
                    "header_row": 12,
                    "end_row": 15,
                    "location_col": "C",
                    "content_col": "D",
                    "duration_col": "E",
                    "start_col": "F",
                    "end_col": "G",
                    "executor_col": "H",
                    "result_col": "I",
                },
                "assessment": {
                    "drill_name": "D5:E5",
                    "drill_date": "F5:K5",
                    "participants": "D6:E6",
                    "start_time": "F6:I6",
                    "end_time": "J6:K6",
                    "total_score": "H11:K11",
                    "evaluator_signature": "D12:F12",
                    "evaluation_time": "H12:K12",
                    "score_rows": [
                        {"row": 9, "value_cell": "H9", "score_cell": "I9", "score": 40},
                        {"row": 10, "value_cell": "H10", "score_cell": "I10", "score": 60},
                    ],
                },
            },
            "steps": [
                {"row": 13, "location": "ECC", "content": "第一步", "duration_text": "2分钟",
                 "duration_minutes": 2, "signature_slots": 2},
                {"row": 14, "location": "IT包间", "content": "第二步", "duration_text": "3分",
                 "duration_minutes": 3, "signature_slots": 3},
                {"row": 15, "location": "IT包间", "content": "第三步", "duration_text": "5分钟",
                 "duration_minutes": 5, "signature_slots": 2},
            ],
            "predicted_total_text": "0 时 10 分",
            "scenario_default_text": "模板模拟场景",
        },
    }


class DrillConfigurationFrontendFieldsTests(unittest.TestCase):
    def _child(self, parent: dict, path: str) -> dict:
        for child in parent.get("children", []):
            if child.get("path") == path:
                return child
        raise AssertionError(f"children 缺少 {path!r}: {[c.get('path') for c in parent.get('children', [])]}")

    def _leaf_paths(self, children: list[dict], prefix: str = "") -> list[str]:
        out: list[str] = []
        for child in children:
            here = f"{prefix}.{child['path']}" if prefix else child["path"]
            out.append(here)
            if child.get("type") == "object":
                out.extend(self._leaf_paths(child.get("children", []), here))
            elif child.get("type") == "array" and isinstance(child.get("item"), dict):
                item = child["item"]
                if item.get("type") == "object":
                    out.extend(self._leaf_paths(item.get("children", []), f"{here}.<i>"))
        return out

    def test_descriptor_shape_and_native_flag(self) -> None:
        desc = _drill_configuration_frontend_fields(_definition())
        self.assertEqual(desc["type"], "object")
        self.assertIs(desc["native_drill_configuration"], True)
        self.assertIn("children", desc)
        self.assertIn("_initial_form", desc)
        self.assertIn("_mapping_paths", desc)
        root_paths = [child["path"] for child in desc["children"]]
        self.assertEqual(root_paths, ["record_sheet", "assessment_sheet", "mapping",
                                      "step_layout", "assessment", "signers"])
        json.dumps(desc, ensure_ascii=False)  # descriptor must remain JSON-serializable

    def test_sheet_selects_options_come_from_definition_sheets(self) -> None:
        desc = _drill_configuration_frontend_fields(_definition())
        record = self._child(desc, "record_sheet")
        assessment = self._child(desc, "assessment_sheet")
        expected = [{"value": "本月记录", "label": "本月记录"}, {"value": "评估表", "label": "评估表"}]
        self.assertEqual(record["type"], "select")
        self.assertEqual(record["options"], expected)
        self.assertEqual(assessment["type"], "select")
        self.assertEqual(assessment["options"], expected)

    def test_record_mapping_group_and_signature_aliases(self) -> None:
        desc = _drill_configuration_frontend_fields(_definition())
        record = self._child(desc, "mapping")
        leaves = {child["path"]: child for child in record["children"]}
        for key in ("machine_room", "drill_date", "drill_name", "area", "scenario",
                    "commander", "participants", "predicted_total", "actual_total"):
            self.assertEqual(leaves[key]["type"], "text")
            self.assertEqual(leaves[key]["maxlength"], 32)
        # Signature ranges must be exposed under cell_N aliases, never "signature".
        self.assertIn("cell_0", leaves)
        self.assertIn("cell_1", leaves)
        self.assertIn("cell_2", leaves)
        self.assertIn("review_time", leaves)
        for path in ("participant_signatures", "recorder_signature", "reviewer_signature", "signature_slots"):
            self.assertNotIn(path, leaves)
        initial = desc["_initial_form"]["mapping"]
        self.assertEqual(initial["cell_0"], "C17:E17")
        self.assertEqual(initial["cell_1"], "G17:I17")
        self.assertEqual(initial["cell_2"], "C18:E18")
        self.assertEqual(initial["review_time"], "G18:I18")
        self.assertEqual(initial["machine_room"], "B5:E5")

    def test_step_layout_numbers_and_80_column_selects(self) -> None:
        desc = _drill_configuration_frontend_fields(_definition())
        step = self._child(desc, "step_layout")
        leaves = {child["path"]: child for child in step["children"]}
        for key in ("start_row", "header_row", "end_row"):
            ctrl = leaves[key]
            self.assertEqual(ctrl["type"], "number")
            self.assertEqual(ctrl["step"], 1)
            self.assertEqual(ctrl["min"], 1)
            self.assertEqual(ctrl["max"], 500)
        for key in ("location_col", "content_col", "duration_col", "start_col",
                    "end_col", "executor_col", "result_col"):
            ctrl = leaves[key]
            self.assertEqual(ctrl["type"], "select")
            options = ctrl["options"]
            self.assertEqual(len(options), 80)
            self.assertEqual(options[0]["value"], "A")
            self.assertEqual(options[-1]["value"], "CB")
            self.assertEqual([opt["value"] for opt in options], [opt["label"] for opt in options])
        initial = desc["_initial_form"]["step_layout"]
        self.assertEqual(initial["start_row"], 13)
        self.assertEqual(initial["header_row"], 12)
        self.assertEqual(initial["end_row"], 15)
        self.assertEqual(initial["location_col"], "C")
        self.assertEqual(initial["result_col"], "I")

    def test_assessment_mapping_and_fixed_score_rows(self) -> None:
        desc = _drill_configuration_frontend_fields(_definition())
        assessment = self._child(desc, "assessment")
        leaves = {child["path"]: child for child in assessment["children"]}
        for key in ("drill_name", "drill_date", "participants", "start_time", "end_time", "total_score"):
            self.assertEqual(leaves[key]["type"], "text")
            self.assertEqual(leaves[key]["maxlength"], 32)
        self.assertEqual(leaves["cell_0"]["label"], "评估人签名区")
        self.assertEqual(leaves["cell_0"]["type"], "text")
        score_rows = leaves["score_rows"]
        self.assertEqual(score_rows["type"], "array")
        self.assertEqual(score_rows["minItems"], 2)
        self.assertEqual(score_rows["maxItems"], 2)
        item_leaves = {child["path"]: child for child in score_rows["item"]["children"]}
        self.assertEqual(item_leaves["value_cell"]["type"], "text")
        self.assertEqual(item_leaves["value_cell"]["maxlength"], 32)
        self.assertEqual(item_leaves["score_cell"]["type"], "text")
        self.assertEqual(item_leaves["score_cell"]["maxlength"], 32)
        self.assertEqual(item_leaves["score"]["type"], "number")
        self.assertEqual(item_leaves["score"]["min"], 0)
        self.assertEqual(item_leaves["score"]["max"], 100)
        self.assertEqual(item_leaves["score"]["step"], 1)
        initial_rows = desc["_initial_form"]["assessment"]["score_rows"]
        self.assertEqual(
            initial_rows,
            [
                {"value_cell": "H9", "score_cell": "I9", "score": 40},
                {"value_cell": "H10", "score_cell": "I10", "score": 60},
            ],
        )
        self.assertEqual(desc["_initial_form"]["assessment"]["cell_0"], "D12:F12")
        self.assertEqual(desc["_initial_form"]["assessment"]["evaluation_time"], "H12:K12")

    def test_signers_paginated_object_labels_keep_original_content(self) -> None:
        desc = _drill_configuration_frontend_fields(_definition())
        signers = self._child(desc, "signers")
        self.assertIs(signers["paginated"], True)
        self.assertIs(signers["searchable"], True)
        children = {child["path"]: child for child in signers["children"]}
        self.assertEqual(sorted(children), ["step_0", "step_1", "step_2"])
        self.assertEqual(children["step_0"]["type"], "number")
        self.assertEqual(children["step_0"]["min"], 1)
        self.assertEqual(children["step_0"]["max"], 10)
        self.assertEqual(children["step_0"]["label"], "第 13 行 · ECC · 第一步")
        self.assertEqual(children["step_1"]["label"], "第 14 行 · IT包间 · 第二步")
        self.assertEqual(children["step_2"]["label"], "第 15 行 · IT包间 · 第三步")
        initial = desc["_initial_form"]["signers"]
        self.assertEqual(initial, {"step_0": 2, "step_1": 3, "step_2": 2})

    def test_mapping_paths_accuracy(self) -> None:
        definition = _definition()
        desc = _drill_configuration_frontend_fields(definition)
        paths = desc["_mapping_paths"]
        config = definition["configuration"]
        # Each mapping path is a segmented list that resolves segment-by-segment
        # through the original configuration to the value the UI shows.
        self.assertEqual(paths["record_sheet"], ["record_sheet"])
        self.assertEqual(_navigate(config, paths["record_sheet"]), "本月记录")
        self.assertEqual(paths["assessment_sheet"], ["assessment_sheet"])
        self.assertEqual(_navigate(config, paths["assessment_sheet"]), "评估表")
        self.assertEqual(paths["mapping.machine_room"], ["mapping", "machine_room"])
        self.assertEqual(_navigate(config, paths["mapping.machine_room"]), "B5:E5")
        self.assertEqual(paths["mapping.cell_0"], ["mapping", "participant_signatures"])
        self.assertEqual(_navigate(config, paths["mapping.cell_0"]), "C17:E17")
        self.assertEqual(paths["mapping.cell_1"], ["mapping", "recorder_signature"])
        self.assertEqual(_navigate(config, paths["mapping.cell_1"]), "G17:I17")
        self.assertEqual(paths["mapping.cell_2"], ["mapping", "reviewer_signature"])
        self.assertEqual(_navigate(config, paths["mapping.cell_2"]), "C18:E18")
        self.assertEqual(paths["mapping.review_time"], ["mapping", "review_time"])
        self.assertEqual(_navigate(config, paths["mapping.review_time"]), "G18:I18")
        self.assertEqual(paths["step_layout.start_row"], ["mapping", "steps", "start_row"])
        self.assertEqual(_navigate(config, paths["step_layout.start_row"]), 13)
        self.assertEqual(paths["step_layout.header_row"], ["mapping", "steps", "header_row"])
        self.assertEqual(_navigate(config, paths["step_layout.header_row"]), 12)
        self.assertEqual(paths["step_layout.end_row"], ["mapping", "steps", "end_row"])
        self.assertEqual(_navigate(config, paths["step_layout.end_row"]), 15)
        self.assertEqual(paths["step_layout.location_col"], ["mapping", "steps", "location_col"])
        self.assertEqual(_navigate(config, paths["step_layout.location_col"]), "C")
        self.assertEqual(paths["step_layout.result_col"], ["mapping", "steps", "result_col"])
        self.assertEqual(_navigate(config, paths["step_layout.result_col"]), "I")
        self.assertEqual(paths["assessment.drill_name"], ["mapping", "assessment", "drill_name"])
        self.assertEqual(_navigate(config, paths["assessment.drill_name"]), "D5:E5")
        self.assertEqual(paths["assessment.cell_0"], ["mapping", "assessment", "evaluator_signature"])
        self.assertEqual(_navigate(config, paths["assessment.cell_0"]), "D12:F12")
        self.assertEqual(paths["assessment.evaluation_time"], ["mapping", "assessment", "evaluation_time"])
        self.assertEqual(_navigate(config, paths["assessment.evaluation_time"]), "H12:K12")
        self.assertEqual(paths["assessment.score_rows.0.value_cell"], ["mapping", "assessment", "score_rows", "0", "value_cell"])
        self.assertEqual(_navigate(config, paths["assessment.score_rows.0.value_cell"]), "H9")
        self.assertEqual(paths["assessment.score_rows.0.score_cell"], ["mapping", "assessment", "score_rows", "0", "score_cell"])
        self.assertEqual(_navigate(config, paths["assessment.score_rows.0.score_cell"]), "I9")
        self.assertEqual(paths["assessment.score_rows.0.score"], ["mapping", "assessment", "score_rows", "0", "score"])
        self.assertEqual(_navigate(config, paths["assessment.score_rows.0.score"]), 40)
        self.assertEqual(paths["assessment.score_rows.1.score"], ["mapping", "assessment", "score_rows", "1", "score"])
        self.assertEqual(_navigate(config, paths["assessment.score_rows.1.score"]), 60)
        self.assertEqual(paths["signers.step_0"], ["steps", "0", "signature_slots"])
        self.assertEqual(_navigate(config, paths["signers.step_0"]), 2)
        self.assertEqual(paths["signers.step_1"], ["steps", "1", "signature_slots"])
        self.assertEqual(_navigate(config, paths["signers.step_1"]), 3)
        self.assertEqual(paths["signers.step_2"], ["steps", "2", "signature_slots"])
        self.assertEqual(_navigate(config, paths["signers.step_2"]), 2)

    def test_mapping_paths_keys_cover_all_ui_leaf_paths(self) -> None:
        desc = _drill_configuration_frontend_fields(_definition())
        expected = set(desc["_mapping_paths"])
        # Rebuild the exact full UI scalar leaf paths.  Array containers
        # (``assessment.score_rows``) have no scalar value of their own; only
        # their concrete per-item leaves are mapped.
        ui_leaves: set[str] = set()
        for child in desc["children"]:
            if child["type"] == "object":
                for leaf in child["children"]:
                    if leaf["type"] != "array":
                        ui_leaves.add(f"{child['path']}.{leaf['path']}")
            if child["path"] in ("record_sheet", "assessment_sheet"):
                ui_leaves.add(child["path"])
        for index in range(2):
            for item_key in ("value_cell", "score_cell", "score"):
                ui_leaves.add(f"assessment.score_rows.{index}.{item_key}")
        for index in range(3):
            ui_leaves.add(f"signers.step_{index}")
        # Every scalar leaf rendered for this fixture must have a mapping path.
        self.assertTrue(expected.issuperset(ui_leaves))

    def test_external_field_names_avoid_signature_privacy_filter(self) -> None:
        desc = _drill_configuration_frontend_fields(_definition())
        leaf_paths = self._leaf_paths(desc["children"])
        for path in leaf_paths:
            self.assertNotIn("signature", path.lower(), path)
            self.assertNotIn("签名", path, path)
        initial_keys = _walk_keys(desc["_initial_form"])
        for key in initial_keys:
            self.assertNotIn("signature", key.lower(), key)
            self.assertNotIn("签名", key, key)
        # safe_data must keep every control (paths/labels/types/constraints) and
        # every initial value.  `safe_data` caps lists at 40, so the 80-column
        # option lists are positionally truncated; the control itself survives.
        safe_children = safe_data(desc["children"])
        self.assertTrue(_same_controls(safe_children, desc["children"]))
        self.assertEqual(safe_data(desc["_initial_form"]), desc["_initial_form"])

    def test_no_signature_images_or_source_paths(self) -> None:
        desc = _drill_configuration_frontend_fields(_definition())
        blob = json.dumps(desc, ensure_ascii=False)
        for forbidden in ("base64,", "signature_png", "file_token", "upload_id",
                          "local_file_path", "source.xlsx", "/drills/", "D:\\", "\\\\"):
            self.assertNotIn(forbidden, blob)
        safe = safe_data(desc)
        safe_blob = json.dumps(safe, ensure_ascii=False)
        for forbidden in ("base64,", "signature_png", "file_token", "upload_id",
                          "local_file_path", "source.xlsx", "/drills/", "D:\\", "\\\\"):
            self.assertNotIn(forbidden, safe_blob)

    def test_does_not_mutate_input_definition(self) -> None:
        source = _definition()
        snapshot = copy.deepcopy(source)
        _drill_configuration_frontend_fields(source)
        self.assertEqual(source, snapshot)

    def test_unrecognized_empty_config_produces_refillable_initial_form(self) -> None:
        definition = {
            "sheets": [{"name": "本月记录"}, {"name": "评估表"}],
            "configuration": {
                "record_sheet": "",
                "assessment_sheet": "",
                "mapping": {},
                "steps": [],
            },
        }
        desc = _drill_configuration_frontend_fields(definition)
        initial = desc["_initial_form"]
        self.assertEqual(initial["record_sheet"], "")
        self.assertEqual(initial["assessment_sheet"], "")
        self.assertEqual(initial["mapping"], {key: "" for key in (
            "machine_room", "drill_date", "drill_name", "area", "scenario", "commander",
            "participants", "predicted_total", "actual_total", "cell_0", "cell_1",
            "cell_2", "review_time")})
        self.assertEqual(initial["step_layout"], {key: "" for key in (
            "start_row", "header_row", "end_row", "location_col", "content_col",
            "duration_col", "start_col", "end_col", "executor_col", "result_col")})
        self.assertEqual(initial["assessment"]["score_rows"], [])
        self.assertEqual(initial["assessment"]["cell_0"], "")
        self.assertEqual(initial["signers"], {})
        # Column and sheet selects still offer real options for re-filling.
        record_select = self._child(desc, "record_sheet")
        self.assertEqual(len(record_select["options"]), 2)
        step_layout = self._child(desc, "step_layout")
        location_col = self._child(step_layout, "location_col")
        self.assertEqual(len(location_col["options"]), 80)
        # score_rows array with an empty config stays an empty fixed array.
        score_array = self._child(self._child(desc, "assessment"), "score_rows")
        self.assertEqual(score_array["minItems"], 0)
        self.assertEqual(score_array["maxItems"], 0)

    def test_real_service_definition_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = LanPortalStateStore(Path(temporary) / "state.sqlite3")
            service = DrillManagementService(store, data_root=Path(temporary) / "drills")
            definition = service.create_definition(
                name="模板演练", year=2026, month=9, file_name="test.xlsx",
                source=_fixture_xlsx(),
            )
            desc = _drill_configuration_frontend_fields(definition)
            config = definition["configuration"]
            initial = desc["_initial_form"]
            self.assertEqual(initial["record_sheet"], config["record_sheet"])
            self.assertEqual(initial["assessment_sheet"], config["assessment_sheet"])
            real_score_len = len(config["mapping"]["assessment"]["score_rows"])
            self.assertEqual(real_score_len, 2)
            score_array = self._child(self._child(desc, "assessment"), "score_rows")
            self.assertEqual(score_array["minItems"], real_score_len)
            self.assertEqual(score_array["maxItems"], real_score_len)
            real_step_count = len(config["steps"])
            self.assertEqual(real_step_count, 3)
            signers = self._child(desc, "signers")
            self.assertEqual(len(signers["children"]), real_step_count)
            # safe_data keeps every control (list-capped) and every value.
            self.assertEqual(safe_data(desc["_initial_form"]), initial)
            self.assertTrue(_same_controls(safe_data(desc["children"]), desc["children"]))


def _navigate(node, segments):
    """Resolve a segmented configuration path through the original config."""
    current = node
    for segment in segments:
        if isinstance(current, list):
            segment = int(segment)
        current = current[segment]
    return current


def _walk_keys(node, prefix=""):
    keys = []
    if isinstance(node, dict):
        for key, value in node.items():
            here = f"{prefix}.{key}" if prefix else str(key)
            keys.append(here)
            if isinstance(value, (dict, list)):
                keys.extend(_walk_keys(value, here))
    elif isinstance(node, list):
        for index, value in enumerate(node):
            here = f"{prefix}.{index}"
            if isinstance(value, (dict, list)):
                keys.extend(_walk_keys(value, here))
    return keys


def _same_controls(safe_child, original_child):
    """Structural equality ignoring safe_data's 40-item list cap on options."""
    if isinstance(safe_child, list) and isinstance(original_child, list):
        return len(safe_child) == len(original_child) and all(
            _same_controls(a, b) for a, b in zip(safe_child, original_child)
        )
    if isinstance(safe_child, dict) and isinstance(original_child, dict):
        if set(safe_child) != set(original_child):
            return False
        for key, safe_value in safe_child.items():
            original_value = original_child[key]
            if key == "options":
                if not (isinstance(safe_value, list) and isinstance(original_value, list)):
                    return False
                if safe_value != original_value[: len(safe_value)]:
                    return False
            elif not _same_controls(safe_value, original_value):
                return False
        return True
    return safe_child == original_child


if __name__ == "__main__":
    unittest.main(verbosity=2)
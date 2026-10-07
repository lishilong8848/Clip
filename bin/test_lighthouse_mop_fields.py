# -*- coding: utf-8 -*-
"""Contract tests for ``lighthouse_api._mop_frontend_fields``.

The helper is a bounded PURE metadata builder: it turns an already-parsed MOP
preview (``preview['sheets']``) into a frontend form descriptor with a private
``_initial_form`` and ``_sheet_indexes`` mapping.  The tests exercise the real
``MaintenancePortalService._extract_mop_sheet_targets`` on synthetic rows so the
targets (maintenance_fields / checkbox_cells / is_cover) are produced by the
native portal parser instead of being invented dictionaries.  No network, cloud
service or credentials are touched.
"""
import json
import sys
import unittest
from pathlib import Path

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.lighthouse_api import (  # noqa: E402
    _mop_frontend_fields,
    _mop_parse_datetime_value,
)
from lan_bitable_template_portal.portal_service import MaintenancePortalService  # noqa: E402


def _column_labels(width: int) -> list[str]:
    return [chr(ord("A") + index) for index in range(width)]


def _make_sheet(name: str, rows: list[list[str]]) -> dict:
    """Build a preview sheet dict using the REAL native target extractor."""
    targets = MaintenancePortalService._extract_mop_sheet_targets(sheet_name=name, rows=rows)
    width = max((len(row) for row in rows), default=0)
    return {
        "name": name,
        "rows": rows,
        "row_count": len(rows),
        "column_count": width,
        "columns": _column_labels(width),
        "is_cover": bool(targets["is_cover"]),
        "checkbox_cells": targets["checkbox_cells"],
        "maintenance_fields": targets["maintenance_fields"],
    }


class MopFrontendFieldsTests(unittest.TestCase):
    def setUp(self) -> None:
        # A synthetic workbook: a cover sheet, a maintenance sheet with both
        # signer roles + datetime placeholders + a checkbox, and a second
        # non-cover sheet with a valid ISO datetime and a checked checkbox.
        cover = _make_sheet("封面", [["文件封面"]])
        sheet1_rows = [
            ["维护实施人：", "", "", "维护审核人：", "", ""],
            ["维护开始时间", "____年__月__日__时"],
            ["维护完成时间", "____年__月__日__时"],
            ["维护完成情况", "□正常 □异常"],
        ]
        sheet2_rows = [
            ["维护开始时间", "2026-10-02 10:30"],
            ["维护完成情况", "☑正常 □异常"],
        ]
        self.sheet1 = _make_sheet("Sheet1", sheet1_rows)
        self.sheet2 = _make_sheet("设备页", sheet2_rows)
        self.preview = {"sheets": [cover, self.sheet1, self.sheet2]}

    def _sheet_child(self, desc: dict, path: str) -> dict:
        for child in desc["children"]:
            if child.get("path") == path:
                return child
        raise AssertionError(f"children 中缺少 {path!r}: {[c.get('path') for c in desc['children']]}")

    def test_native_mop_descriptor_shape_and_sheet_indexes(self) -> None:
        desc = _mop_frontend_fields(self.preview, "Sheet1")
        self.assertEqual(desc["type"], "object")
        self.assertIs(desc["native_mop"], True)
        # Cover sheet skipped; only non-cover sheets are addressable.
        self.assertEqual(
            desc["_sheet_indexes"],
            {"sheet_0": 1, "sheet_1": 2},
        )
        self.assertIn("_initial_form", desc)

    def test_sheet_name_select_uses_real_noncover_names(self) -> None:
        desc = _mop_frontend_fields(self.preview, "")
        select = self._sheet_child(desc, "sheet_name")
        self.assertEqual(select["type"], "select")
        labels = [option["label"] for option in select["options"]]
        self.assertEqual(labels, ["Sheet1", "设备页"])
        self.assertNotIn("封面", labels)

    def test_invalid_requested_name_falls_back_to_first_noncover(self) -> None:
        desc = _mop_frontend_fields(self.preview, "不存在的表")
        self.assertEqual(desc["_initial_form"]["sheet_name"], "Sheet1")
        self.assertEqual(self._sheet_child(desc, "sheet_name")["options"][0]["value"], "Sheet1")

    def test_valid_requested_name_is_chosen(self) -> None:
        desc = _mop_frontend_fields(self.preview, "设备页")
        self.assertEqual(desc["_initial_form"]["sheet_name"], "设备页")

    def test_each_noncover_sheet_associated_by_when(self) -> None:
        desc = _mop_frontend_fields(self.preview, "Sheet1")
        sheet0 = self._sheet_child(desc, "sheet_0")
        sheet1 = self._sheet_child(desc, "sheet_1")
        self.assertEqual(sheet0["when"], {"path": "sheet_name", "equals": "Sheet1"})
        self.assertEqual(sheet1["when"], {"path": "sheet_name", "equals": "设备页"})
        # Cover sheet must never be a UI sheet child.
        with self.assertRaises(AssertionError):
            self._sheet_child(desc, "sheet_封面")

    def test_fields_exclude_signatures_and_use_native_types(self) -> None:
        desc = _mop_frontend_fields(self.preview, "Sheet1")
        sheet0 = self._sheet_child(desc, "sheet_0")
        fields_control = self._sheet_child(sheet0, "fields")
        field_descs = {child["path"]: child for child in fields_control["children"]}
        self.assertEqual(
            sorted(field_descs),
            ["field_0", "field_1", "field_2"],
        )
        self.assertEqual(field_descs["field_0"]["type"], "datetime-local")
        self.assertEqual(field_descs["field_0"]["label"], "维护开始时间 · B2")
        self.assertEqual(field_descs["field_0"]["step"], 1)
        self.assertEqual(field_descs["field_0"]["value_cell_ref"], "B2")
        self.assertEqual(field_descs["field_1"]["type"], "datetime-local")
        self.assertTrue(field_descs["field_1"]["label"].startswith("维护完成时间 · "))
        self.assertEqual(field_descs["field_2"]["type"], "textarea")
        self.assertTrue(field_descs["field_2"]["label"].startswith("维护完成情况 · "))
        # Signer labels are NOT editable value fields.
        self.assertTrue(all(desc["label"] not in {"维护实施人", "维护审核人"} for desc in field_descs.values()))

    def test_zero_based_targets_are_not_editable_coordinates(self) -> None:
        desc = _mop_frontend_fields(self.preview, "Sheet1")
        sheet0 = self._sheet_child(desc, "sheet_0")
        cell_edits = self._sheet_child(sheet0, "cell_edits")
        self.assertEqual(cell_edits["type"], "array")
        self.assertEqual(cell_edits["maxItems"], 200)
        item_children = {child["path"]: child for child in cell_edits["item"]["children"]}
        # cell_edits uses integer rows (step 1), A-style column labels; row and
        # column are required, value is optional free text so clearing is easy.
        self.assertEqual(item_children["row"]["type"], "integer")
        self.assertEqual(item_children["row"]["step"], 1)
        self.assertIs(item_children["row"]["required"], True)
        self.assertEqual(item_children["row"]["min"], 1)
        self.assertEqual(item_children["row"]["max"], self.sheet1["row_count"])
        self.assertIs(item_children["column"]["required"], True)
        self.assertEqual(
            [option["value"] for option in item_children["column"]["options"]],
            self.sheet1["columns"],
        )
        self.assertEqual(item_children["value"]["type"], "textarea")
        self.assertEqual(item_children["value"]["maxlength"], 5000)
        # No 0-based integer coordinate is exposed as an editable control.
        fields_control = self._sheet_child(sheet0, "fields")
        self.assertTrue(all(child["path"].startswith("field_") for child in fields_control["children"]))
        self.assertTrue(all(child.get("editable") is not True for child in fields_control["children"]))
        json.dumps(desc, ensure_ascii=False)  # descriptor must remain JSON-serializable

    def test_checkbox_options_use_native_labels_and_selected_state(self) -> None:
        desc = _mop_frontend_fields(self.preview, "设备页")
        sheet1 = self._sheet_child(desc, "sheet_1")
        checks_control = self._sheet_child(sheet1, "checkboxes")
        check = checks_control["children"][0]
        self.assertEqual(check["path"], "check_0")
        # label prefixes the real cell_ref followed by row context cells
        # excluding the checkbox cell itself.
        self.assertEqual(check["label"], "B2 · 维护完成情况")
        self.assertEqual(
            [option["value"] for option in check["options"]],
            ["正常", "异常"],
        )
        self.assertEqual(
            [option["label"] for option in check["options"]],
            ["正常", "异常"],
        )
        # Native checked option is the initial value.
        self.assertEqual(desc["_initial_form"]["sheet_1"]["checkboxes"]["check_0"], "正常")
        # Sheet1 checkbox has no checked native option -> empty initial.
        sheet0 = self._sheet_child(_mop_frontend_fields(self.preview, "Sheet1"), "sheet_0")
        self.assertEqual(
            self._sheet_child(sheet0, "checkboxes")["children"][0]["options"],
            [{"value": "正常", "label": "正常"}, {"value": "异常", "label": "异常"}],
        )
        self.assertEqual(
            _mop_frontend_fields(self.preview, "Sheet1")["_initial_form"]["sheet_0"]["checkboxes"]["check_0"],
            "",
        )

    def test_initial_form_datetime_parsing_only_iso_like(self) -> None:
        desc = _mop_frontend_fields(self.preview, "设备页")
        fields = desc["_initial_form"]["sheet_1"]["fields"]
        self.assertEqual(fields["field_0"], "2026-10-02T10:30:00")
        # Placeholder datetime is not guessed and stays empty.
        sheet1_initial = _mop_frontend_fields(self.preview, "Sheet1")["_initial_form"]["sheet_0"]["fields"]
        self.assertEqual(sheet1_initial["field_0"], "")
        self.assertEqual(sheet1_initial["field_1"], "")

    def test_timezone_datetime_converts_to_beijing_utc8(self) -> None:
        # Explicit UTC+8 conversion is independent of host local timezone.
        self.assertEqual(
            _mop_parse_datetime_value("2026-10-02T02:30:00Z"),
            "2026-10-02T10:30:00",
        )
        self.assertEqual(
            _mop_parse_datetime_value("2026-10-01T22:30:00+00:00"),
            "2026-10-02T06:30:00",
        )
        self.assertEqual(
            _mop_parse_datetime_value("2026-10-02T10:30:00+08:00"),
            "2026-10-02T10:30:00",
        )
        self.assertEqual(_mop_parse_datetime_value("____年__月__日__时"), "")

    def test_fields_and_checkboxes_are_paginated_and_searchable(self) -> None:
        desc = _mop_frontend_fields(self.preview, "Sheet1")
        sheet0 = self._sheet_child(desc, "sheet_0")
        fields_control = self._sheet_child(sheet0, "fields")
        checks_control = self._sheet_child(sheet0, "checkboxes")
        self.assertIs(fields_control["paginated"], True)
        self.assertIs(fields_control["searchable"], True)
        self.assertIs(checks_control["paginated"], True)
        self.assertIs(checks_control["searchable"], True)

    def test_no_native_coordinate_metadata_in_public_descriptor(self) -> None:
        desc = _mop_frontend_fields(self.preview, "Sheet1")
        payload = json.dumps(desc, ensure_ascii=False)
        for forbidden in ("_native_row", "_native_value_col", "_native_label_col"):
            self.assertNotIn(forbidden, payload)
        fields_control = self._sheet_child(self._sheet_child(desc, "sheet_0"), "fields")
        for child in fields_control["children"]:
            self.assertEqual(
                {key for key in child if key.startswith("_native_")},
                set(),
            )

    def test_signer_controls_only_for_roles_present(self) -> None:
        desc = _mop_frontend_fields(self.preview, "Sheet1")
        sheet0 = self._sheet_child(desc, "sheet_0")
        paths = {child["path"] for child in sheet0["children"]}
        self.assertIn("implementer", paths)
        self.assertIn("auditor", paths)
        impl = self._sheet_child(sheet0, "implementer")
        self.assertEqual(impl["type"], "multiselect")
        self.assertIs(impl["choice_group"], True)
        self.assertEqual(impl["maxItems"], 50)
        self.assertEqual(impl["options"], [])
        self.assertEqual(self._sheet_child(sheet0, "auditor")["options"], [])

        # 设备页 lacks signer rows -> neither control is emitted.
        sheet1 = self._sheet_child(_mop_frontend_fields(self.preview, "设备页"), "sheet_1")
        sheet1_paths = {child["path"] for child in sheet1["children"]}
        self.assertNotIn("implementer", sheet1_paths)
        self.assertNotIn("auditor", sheet1_paths)

    def test_no_paths_tokens_or_signature_images_in_descriptor_or_initial(self) -> None:
        desc = _mop_frontend_fields(self.preview, "Sheet1")
        payload = json.dumps(desc, ensure_ascii=False)
        for forbidden in ("local_file_path", "file_token", "upload_id", "mop_record_id", "signature_png", "base64,", "temp/", "signatures/"):
            self.assertNotIn(forbidden, payload)

        initial = json.dumps(desc["_initial_form"], ensure_ascii=False)
        for forbidden in ("local_file_path", "file_token", "upload_id", "mop_record_id", "signature_png", "base64,"):
            self.assertNotIn(forbidden, initial)

    def test_cell_edits_initial_is_empty_list(self) -> None:
        desc = _mop_frontend_fields(self.preview, "Sheet1")
        self.assertEqual(desc["_initial_form"]["sheet_0"]["cell_edits"], [])
        self.assertEqual(desc["_initial_form"]["sheet_1"]["cell_edits"], [])


if __name__ == "__main__":
    unittest.main(verbosity=2)

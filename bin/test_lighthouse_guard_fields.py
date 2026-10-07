# -*- coding: utf-8 -*-
"""Contract tests for ``lighthouse_api._guard_frontend_fields``.

The helper builds a structured control descriptor for a native critical-guard
sheet from ``critical_guard.sheet_definition`` / ``response_check_items``
(template layout is authoritative).  These tests are pure local metadata /
template parsing:  they read the local ``重保戒备检查表.xlsx`` template through
``critical_guard.critical_guard_catalog()`` (``sheet_definition``/``response_check_items``)
and never start a native service, call the cloud or touch credentials.

Covered assertions:
- all six real native sheet definitions produce ``{type:'object', native_guard:True}``
  and always carry the native ``check_date`` date child;
- check sheets expose ``checks`` + ``suggestions`` and never leak the frozen
  system/query fields (``machine_room``/``template_items``/``revision``/
  ``customized``/``source_file_id``) as editable children;
- custom check items with dotted keys stay literal (never split into nested
  objects), with labels built from category + content;
- status select options are normal/正常 + abnormal/异常; note maxlength=2000;
  suggestions maxlength=5000;
- ``weather`` uses the native ``weather_fields`` labels, text maxlength=500 and
  only exists on 灾害专项;
- file-mode sheets (materials/contacts) expose only ``check_date``;
- an unknown sheet kind raises the native ``CriticalGuardError``.
"""
import sys
import unittest
from pathlib import Path
from unittest import mock

BIN_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal import critical_guard  # noqa: E402
from lan_bitable_template_portal.critical_guard import CriticalGuardError  # noqa: E402
from lan_bitable_template_portal.lighthouse_api import _guard_frontend_fields  # noqa: E402


class GuardFrontendFieldsTests(unittest.TestCase):
    CHECK_SHEETS = ("设备安全", "环境安全", "客户重保", "灾害专项")
    FILE_SHEETS = ("物资检查清单", "重保联络清单")
    ALL_SHEETS = CHECK_SHEETS + FILE_SHEETS
    SYSTEM_FIELDS = {"machine_room", "template_items", "revision", "customized", "source_file_id"}

    @staticmethod
    def _child_by_path(desc: dict, path: str) -> dict:
        for child in desc["children"]:
            if child.get("path") == path:
                return child
        raise AssertionError(f"children 中缺少字段 {path!r}: {[c.get('path') for c in desc['children']]}")

    @staticmethod
    def _child_if_present(desc: dict, path: str):
        for child in desc["children"]:
            if child.get("path") == path:
                return child
        return None

    def test_all_six_native_sheets_produce_object_native_guard_with_check_date(self):
        for sheet in self.ALL_SHEETS:
            with self.subTest(sheet=sheet):
                desc = _guard_frontend_fields(sheet, {})
                self.assertEqual(desc["type"], "object")
                self.assertIs(desc["native_guard"], True)
                first = desc["children"][0]
                self.assertEqual(first["path"], "check_date")
                self.assertEqual(first["type"], "date")
                self.assertIs(first.get("literal_key"), True)

    def test_check_sheets_expose_checks_suggestions_and_no_system_fields(self):
        for sheet in self.CHECK_SHEETS:
            with self.subTest(sheet=sheet):
                desc = _guard_frontend_fields(sheet, {})
                paths = {child["path"] for child in desc["children"]}
                self.assertIn("checks", paths)
                self.assertIn("suggestions", paths)
                leaked = paths & self.SYSTEM_FIELDS
                self.assertFalse(leaked, f"{sheet} 暴露了只读系统字段: {sorted(leaked)}")

    def test_custom_check_items_with_dotted_keys_stay_literal_no_split(self):
        cells = {
            "template_items": [
                {"key": "A.1", "category": "设备类", "content": "检查A"},
                {"key": "env.cell.3", "category": "", "content": "带点键"},
            ]
        }
        desc = _guard_frontend_fields("设备安全", cells)
        checks = self._child_by_path(desc, "checks")
        keys = [child["path"] for child in checks["children"]]
        self.assertEqual(keys, ["A.1", "env.cell.3"])

        labels = {child["path"]: child["label"] for child in checks["children"]}
        self.assertIn("设备类", labels["A.1"])
        self.assertIn("检查A", labels["A.1"])
        self.assertEqual(labels["env.cell.3"], "带点键")

        for child in checks["children"]:
            self.assertIs(child.get("literal_key"), True)
            self.assertEqual(child["type"], "object")
            self.assertEqual(sorted(c["path"] for c in child["children"]), ["note", "status"])

    def test_check_status_options_and_note_suggestions_limits(self):
        cells = {"template_items": [{"key": "B.1", "category": "环境", "content": "渗漏"}]}
        desc = _guard_frontend_fields("环境安全", cells)
        checks = self._child_by_path(desc, "checks")
        entry = checks["children"][0]
        sub = {child["path"]: child for child in entry["children"]}

        self.assertEqual(sub["status"]["type"], "select")
        self.assertEqual(
            sub["status"]["options"],
            [{"value": "normal", "label": "正常"}, {"value": "abnormal", "label": "异常"}],
        )
        self.assertEqual(sub["note"]["type"], "textarea")
        self.assertEqual(sub["note"]["maxlength"], 2000)
        self.assertEqual(self._child_by_path(desc, "suggestions")["type"], "textarea")
        self.assertEqual(self._child_by_path(desc, "suggestions")["maxlength"], 5000)

    def test_weather_field_only_on_disaster_sheet_uses_native_labels_and_limits(self):
        desc = _guard_frontend_fields("灾害专项", {})
        weather = self._child_by_path(desc, "weather")
        weather_children = {child["path"]: child for child in weather["children"]}
        self.assertEqual(sorted(weather_children), ["current", "level1", "level2"])
        self.assertEqual(weather_children["level1"]["label"], "一级极端天气")
        self.assertEqual(weather_children["level2"]["label"], "二级极端天气")
        self.assertEqual(weather_children["current"]["label"], "当前检查极端天气")
        for child in weather["children"]:
            self.assertEqual(child["type"], "text")
            self.assertEqual(child["maxlength"], 500)
            self.assertIs(child.get("literal_key"), True)

        non_weather = self._child_if_present(_guard_frontend_fields("设备安全", {}), "weather")
        self.assertIsNone(non_weather)

    def test_file_mode_sheets_expose_only_check_date(self):
        for sheet in self.FILE_SHEETS:
            with self.subTest(sheet=sheet):
                desc = _guard_frontend_fields(sheet, {})
                paths = [child["path"] for child in desc["children"]]
                self.assertEqual(paths, ["check_date"])

    def test_unknown_kind_raises_critical_guard_error(self):
        fake = {"kind": "mystery", "name": "任意表"}
        with mock.patch.object(critical_guard, "sheet_definition", return_value=fake):
            with self.assertRaises(CriticalGuardError):
                _guard_frontend_fields("任意表", {})


if __name__ == "__main__":
    unittest.main(verbosity=2)
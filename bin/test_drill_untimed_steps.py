# -*- coding: utf-8 -*-
"""演练步骤 ⊗ 不计时符号的派生时间、不完整预览、总用时校验与实际文件发布形状回归。"""
from __future__ import annotations

import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from test_drill_management import (  # noqa: E402
    _fixture_xlsx,
    _replace_zip_part,
    _signature_png,
)

from lan_bitable_template_portal.drill_management import (  # noqa: E402
    DrillError,
    DrillManagementService,
    _cell_target,
    _derived_values,
    _parse_workbook,
    _validate_configuration,
    _validate_execution,
    build_time_chain,
    detect_drill_configuration,
    parse_duration_minutes,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402

UNTIMED_SYMBOL = "⊗"
_CONFIRMED = "符合【 √ 】 不符【   】"
_TEMPLATE_DURATIONS = {13: "2分钟", 14: "3分", 15: "5分钟"}
_TEMPLATE_PREDICTED = "0 时 10 分"


def _cell_xml(reference: str, text: str) -> bytes:
    return (
        f'<c r="{reference}" t="inlineStr"><is><t xml:space="preserve">'
        f"{text}</t></is></c>"
    ).encode("utf-8")


def _drill_fixture(*untimed_rows: int, predicted: str) -> bytes:
    """基于模板生成演练记录表，把指定行的预估时间单元格改写为 ⊗。"""
    untimed = set(untimed_rows)
    if not untimed:
        untimed = {14}

    def transform(content: bytes) -> bytes:
        for row, original in _TEMPLATE_DURATIONS.items():
            value = UNTIMED_SYMBOL if row in untimed else original
            content = content.replace(_cell_xml(f"E{row}", original), _cell_xml(f"E{row}", value), 1)
        content = content.replace(
            _cell_xml("D16", _TEMPLATE_PREDICTED), _cell_xml("D16", predicted), 1
        )
        return content

    return _replace_zip_part(_fixture_xlsx(), "xl/worksheets/sheet1.xml", transform)


def _make_people(count: int = 4) -> list[dict[str, str]]:
    return [
        {"record_id": f"person-{index}", "name": f"人员{index}"}
        for index in range(1, count + 1)
    ]


def _make_step_signers(people: list[dict[str, str]]) -> dict[str, list[str]]:
    ids = [item["record_id"] for item in people]
    return {
        "13": ids[:1],
        "14": ids[:3],
        "15": ids[:4],
    }


def _execution_for(configuration: dict) -> dict:
    people = _make_people()
    return {
        "scope": "A",
        "drill_date": "2026-10-17",
        "first_start_time": "09:00",
        "commander": people[0],
        "participants": people,
        "step_signers": _make_step_signers(people),
    }


class UntimedDrillStepRegressionTests(unittest.TestCase):
    def test_middle_untimed_step_full_workflow(self) -> None:
        """中间步骤 E14 改为 ⊗ 的全流程：配置、派生时间、生成文件一致。"""
        source = _drill_fixture(14, predicted="7分")
        with tempfile.TemporaryDirectory() as temporary:
            store = LanPortalStateStore(Path(temporary) / "state.sqlite3")
            service = DrillManagementService(store, data_root=Path(temporary) / "drills")
            definition = service.create_definition(
                name="中间步骤不计时",
                year=2026,
                month=10,
                file_name="untimed.xlsx",
                source=source,
            )
            steps = definition["configuration"]["steps"]
            self.assertEqual(
                [(s["row"], s["duration_text"], s["duration_minutes"]) for s in steps],
                [(13, "2分钟", 2), (14, UNTIMED_SYMBOL, 0), (15, "5分钟", 5)],
            )
            self.assertEqual(definition["configuration"]["predicted_total_text"], "7分")

            saved = service.save_configuration(
                definition["drill_id"],
                definition["configuration"],
                expected_version=definition["version"],
            )
            self.assertEqual(
                [(s["row"], s["duration_text"], s["duration_minutes"]) for s in saved["configuration"]["steps"]],
                [(13, "2分钟", 2), (14, UNTIMED_SYMBOL, 0), (15, "5分钟", 5)],
            )

            published = service.publish(saved["drill_id"], expected_version=saved["version"])
            self.assertEqual(published["status"], "published")

            people = _make_people()
            execution = service.save_execution(
                saved["drill_id"],
                "A",
                {
                    "drill_date": "2026-10-17",
                    "first_start_time": "09:00",
                    "commander": people[0],
                    "participants": people,
                    "step_signers": _make_step_signers(people),
                },
                expected_version=0,
            )
            self.assertEqual(execution["status"], "ready")

            configuration = saved["configuration"]
            derived = _derived_values(
                {"configuration": configuration}, execution, allow_incomplete=True
            )
            step_mapping = configuration["mapping"]["steps"]
            self.assertEqual(derived["record_values"].get(f"{step_mapping['start_col']}13"), "09:00")
            self.assertEqual(derived["record_values"].get(f"{step_mapping['end_col']}13"), "09:02")
            self.assertEqual(derived["record_values"].get(f"{step_mapping['start_col']}14"), "")
            self.assertEqual(derived["record_values"].get(f"{step_mapping['end_col']}14"), "")
            self.assertEqual(derived["record_values"].get(f"{step_mapping['start_col']}15"), "09:02")
            self.assertEqual(derived["record_values"].get(f"{step_mapping['end_col']}15"), "09:07")
            self.assertEqual(derived["record_values"].get(f"{step_mapping['result_col']}14"), _CONFIRMED)
            signature_ranges = {item["range"] for item in derived["signature_cells"]}
            self.assertTrue({"H13", "H14", "H15"}.issubset(signature_ranges))
            assessment = configuration["mapping"]["assessment"]
            self.assertEqual(
                derived["assessment_values"].get(_cell_target(assessment["start_time"])), "09:00"
            )
            self.assertEqual(
                derived["assessment_values"].get(_cell_target(assessment["end_time"])), "09:07"
            )

            total_cell = configuration["mapping"].get("actual_total")
            if total_cell:
                self.assertEqual(
                    derived["record_values"].get(_cell_target(total_cell)), "7分"
                )

            generated = service.generate(
                saved["drill_id"],
                "A",
                signatures={item["record_id"]: _signature_png() for item in people},
            )
            output_path = Path(generated["generated"]["path"])
            parsed = _parse_workbook(output_path)
            record = next(sheet for sheet in parsed["sheets"] if sheet["name"] == "本月记录")
            self.assertEqual(record["cells"].get("E14"), UNTIMED_SYMBOL)
            self.assertEqual(record["cells"].get("F13"), "09:00")
            self.assertEqual(record["cells"].get("G13"), "09:02")
            self.assertEqual(record["cells"].get("F14"), "")
            self.assertEqual(record["cells"].get("G14"), "")
            self.assertEqual(record["cells"].get("F15"), "09:02")
            self.assertEqual(record["cells"].get("G15"), "09:07")
            self.assertEqual(record["cells"].get("I14"), _CONFIRMED)
            with zipfile.ZipFile(output_path) as archive:
                self.assertTrue(
                    any("drill_signature_" in name for name in archive.namelist())
                )

    def test_first_last_and_multiple_untimed_steps(self) -> None:
        """首/尾/多步 ⊗ 时派生时间正确，评估起止取首尾计时步骤，签名位置保留。"""
        cases = [
            (
                {13}, "8分",
                [(13, UNTIMED_SYMBOL, 0), (14, "3分", 3), (15, "5分钟", 5)],
                {13: ("", ""), 14: ("09:00", "09:03"), 15: ("09:03", "09:08")},
                ("09:00", "09:08"),
            ),
            (
                {15}, "5分",
                [(13, "2分钟", 2), (14, "3分", 3), (15, UNTIMED_SYMBOL, 0)],
                {13: ("09:00", "09:02"), 14: ("09:02", "09:05"), 15: ("", "")},
                ("09:00", "09:05"),
            ),
            (
                {13, 15}, "3分",
                [(13, UNTIMED_SYMBOL, 0), (14, "3分", 3), (15, UNTIMED_SYMBOL, 0)],
                {13: ("", ""), 14: ("09:00", "09:03"), 15: ("", "")},
                ("09:00", "09:03"),
            ),
        ]
        for untimed_rows, predicted, expected, expected_times, assessment_range in cases:
            with self.subTest(untimed_rows=untimed_rows), tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary) / "reference.xlsx"
                path.write_bytes(_drill_fixture(*untimed_rows, predicted=predicted))
                workbook = _parse_workbook(path)
                configuration = detect_drill_configuration(workbook)
                validated = _validate_configuration(
                    workbook,
                    {
                        "record_sheet": "本月记录",
                        "assessment_sheet": "评估表",
                        "mapping": configuration["mapping"],
                    },
                )
                self.assertEqual(validated["predicted_total_text"], predicted)
                self.assertEqual(
                    [
                        (s["row"], s["duration_text"], s["duration_minutes"])
                        for s in validated["steps"]
                    ],
                    expected,
                )

                execution = _execution_for(validated)
                derived = _derived_values(
                    {"configuration": validated}, execution, allow_incomplete=True
                )
                step_mapping = validated["mapping"]["steps"]
                for row in (13, 14, 15):
                    start_value = derived["record_values"].get(f"{step_mapping['start_col']}{row}")
                    end_value = derived["record_values"].get(f"{step_mapping['end_col']}{row}")
                    self.assertEqual(
                        (start_value, end_value),
                        expected_times[row],
                        f"第 {row} 行派生时间不符",
                    )
                    self.assertEqual(
                        derived["record_values"].get(f"{step_mapping['result_col']}{row}"),
                        _CONFIRMED,
                        f"第 {row} 行确认结果缺失",
                    )

                assessment = validated["mapping"]["assessment"]
                self.assertEqual(
                    derived["assessment_values"].get(_cell_target(assessment["start_time"])),
                    assessment_range[0],
                )
                self.assertEqual(
                    derived["assessment_values"].get(_cell_target(assessment["end_time"])),
                    assessment_range[1],
                )

                signature_ranges = {item["range"] for item in derived["signature_cells"]}
                self.assertTrue({"H13", "H14", "H15"}.issubset(signature_ranges))
                step_by_row = {int(s["row"]): s for s in validated["steps"]}
                for row in (13, 14, 15):
                    step = step_by_row[row]
                    cells = [item for item in derived["signature_cells"] if item["range"] == f"H{row}"]
                    self.assertEqual(len(cells), 1, f"第 {row} 行 H 签名位缺失")
                    self.assertEqual(
                        cells[0]["signature_slots"], int(step["signature_slots"]),
                        f"第 {row} 行 ⊗/计时行执行人签名槽数被绕过",
                    )
                    self.assertTrue(
                        cells[0]["signers"],
                        f"第 {row} 行执行人签名人员被绕过",
                    )
                if 13 in untimed_rows:
                    invalid = {**execution, "step_signers": {**execution["step_signers"],
                        "13": [execution["participants"][1]["record_id"]]}}
                    self.assertIn("第 13 行 ECC 步骤需选择指挥人签名。",
                        _validate_execution({"configuration": validated}, invalid))

    def test_exact_symbol_with_whitespace_is_accepted_but_unknown_text_rejected(self) -> None:
        """精确符号 ⊗（剥离前后空白）计为不计时；其它未知文本仍严格拒绝。"""
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "spaced.xlsx"
            path.write_bytes(_drill_fixture(predicted="7分"))
            workbook = _parse_workbook(path)
            workbook["sheets"][0]["cells"]["E14"] = " ⊗ "
            configuration = detect_drill_configuration(workbook)
            self.assertEqual(configuration["steps"][1]["duration_text"].strip(), "⊗")
            self.assertEqual(configuration["steps"][1]["duration_minutes"], 0)
            validated = _validate_configuration(
                workbook,
                {
                    "record_sheet": "本月记录",
                    "assessment_sheet": "评估表",
                    "mapping": configuration["mapping"],
                },
            )
            self.assertEqual(validated["steps"][1]["duration_minutes"], 0)

        for bad in ("?", "abc", "⊗x", "1分⊗"):
            with self.subTest(bad=bad), tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary) / "bad.xlsx"
                path.write_bytes(_drill_fixture(predicted="7分"))
                workbook = _parse_workbook(path)
                mapping = detect_drill_configuration(workbook)["mapping"]
                workbook["sheets"][0]["cells"]["E14"] = bad
                with self.assertRaisesRegex(DrillError, "预估时间"):
                    _validate_configuration(
                        workbook,
                        {
                            "record_sheet": "本月记录",
                            "assessment_sheet": "评估表",
                            "mapping": mapping,
                        },
                    )

    def test_public_duration_guards_reject_zero_negative_symbol_and_unknown(self) -> None:
        """parse_duration_minutes 与 build_time_chain 对 0、负值、⊗ 符号及未知文本严格拒绝。"""
        for value in ("0", "0分", "-1", "-1分", "abc", "?", UNTIMED_SYMBOL):
            with self.subTest(value=value), self.assertRaises(DrillError):
                parse_duration_minutes(value)
        for durations in ([0], [-1], [-5, 2]):
            with self.subTest(durations=durations), self.assertRaisesRegex(
                DrillError, "大于 0"
            ):
                build_time_chain("2026-10-17", "09:00", durations)

    def test_untimed_step_total_validation_rejects_symbol_and_mismatch(self) -> None:
        """配置预计总用时为 ⊗ 或数值与计时步骤合计不一致时，校验必须拒绝。"""
        with self.subTest(case="predicted symbol"), tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "symbol.xlsx"
            path.write_bytes(_drill_fixture(predicted=UNTIMED_SYMBOL))
            workbook = _parse_workbook(path)
            mapping = detect_drill_configuration(workbook)["mapping"]
            with self.assertRaises(DrillError):
                _validate_configuration(
                    workbook,
                    {"record_sheet": "本月记录", "assessment_sheet": "评估表", "mapping": mapping},
                )
        with self.subTest(case="predicted mismatch"), tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "mismatch.xlsx"
            path.write_bytes(_drill_fixture(predicted="8分"))
            workbook = _parse_workbook(path)
            mapping = detect_drill_configuration(workbook)["mapping"]
            with self.assertRaisesRegex(DrillError, "不一致"):
                _validate_configuration(
                    workbook,
                    {"record_sheet": "本月记录", "assessment_sheet": "评估表", "mapping": mapping},
                )

    def test_incomplete_preview_produces_no_auto_timestamps(self) -> None:
        """first_start_time 缺失/错误时，allow_incomplete 不产生 F/G/I 自动值，严格模式抛错。"""
        for bad_start in ("", "abc", "25:99"):
            with self.subTest(bad_start=bad_start), tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary) / "incomplete.xlsx"
                path.write_bytes(_drill_fixture(predicted="7分"))
                workbook = _parse_workbook(path)
                configuration = detect_drill_configuration(workbook)
                validated = _validate_configuration(
                    workbook,
                    {
                        "record_sheet": "本月记录",
                        "assessment_sheet": "评估表",
                        "mapping": configuration["mapping"],
                    },
                )
                step_mapping = validated["mapping"]["steps"]
                execution = _execution_for(validated)
                execution["first_start_time"] = bad_start

                derived = _derived_values(
                    {"configuration": validated}, execution, allow_incomplete=True
                )
                auto_cells = [
                    f"{col}{row}"
                    for col in (
                        step_mapping["start_col"],
                        step_mapping["end_col"],
                        step_mapping["result_col"],
                    )
                    for row in (13, 14, 15)
                ]
                self.assertFalse(
                    any(cell in derived["record_values"] for cell in auto_cells),
                    "不完整预览不应产生任何 F/G/I 自动确认值",
                )
                with self.assertRaises(DrillError):
                    _derived_values(
                        {"configuration": validated}, execution, allow_incomplete=False
                    )

    @unittest.skipUnless(
        Path(r"D:/下载/21V-JSNTFOC-EOP-KT-308 机房局部高温故障应急处理流程V1.5.xlsx").is_file(),
        "reference drill workbook is not available",
    )
    def test_reference_untimed_workbook_regression(self) -> None:
        """用户实际文件回归：detect 与 _validate_configuration 发布形状一致且 E17 步骤保留。"""
        workbook = _parse_workbook(
            Path(r"D:/下载/21V-JSNTFOC-EOP-KT-308 机房局部高温故障应急处理流程V1.5.xlsx")
        )
        configuration = detect_drill_configuration(workbook)
        self.assertEqual(len(configuration["steps"]), 7)
        self.assertEqual(configuration["record_sheet"], "本月（机房负载突然增加导致单点高温)")
        self.assertEqual(configuration["assessment_sheet"], "评估表")
        by_row = {int(s["row"]): s for s in configuration["steps"]}
        self.assertEqual(by_row[17]["duration_text"], UNTIMED_SYMBOL)
        self.assertEqual(by_row[17]["duration_minutes"], 0)
        timed_total = sum(
            int(s["duration_minutes"]) for s in configuration["steps"] if s["row"] != 17
        )
        self.assertEqual(timed_total, 14)

        validated = _validate_configuration(
            workbook,
            {
                "record_sheet": configuration["record_sheet"],
                "assessment_sheet": configuration["assessment_sheet"],
                "mapping": configuration["mapping"],
            },
        )
        self.assertEqual(len(validated["steps"]), 7)
        validated_by_row = {int(s["row"]): s for s in validated["steps"]}
        self.assertIn(17, validated_by_row)
        self.assertEqual(validated_by_row[17]["duration_text"], UNTIMED_SYMBOL)
        self.assertEqual(validated_by_row[17]["duration_minutes"], 0)
        validated_timed_total = sum(
            int(s["duration_minutes"])
            for s in validated["steps"]
            if int(s["row"]) != 17
        )
        self.assertEqual(validated_timed_total, 14)
        assessment_rows = validated["mapping"]["assessment"]["score_rows"]
        self.assertEqual(sum(int(row["score"]) for row in assessment_rows), 100)
        next(step for step in validated["steps"] if step["row"] == 17)["signature_slots"] = 0
        zero_signatures = _validate_configuration(workbook, validated)
        self.assertEqual(len(zero_signatures["steps"]), 7)
        self.assertEqual(next(step["signature_slots"] for step in zero_signatures["steps"] if step["row"] == 17), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)

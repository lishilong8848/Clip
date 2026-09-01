# -*- coding: utf-8 -*-
from __future__ import annotations

import io
import sys
import tempfile
import unittest
import warnings
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

from PIL import Image, ImageDraw


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.drill_management import (  # noqa: E402
    DrillConflictError,
    DrillManagementService,
    _horizontal_signature_layout,
    _parse_workbook,
    build_time_chain,
    detect_drill_configuration,
    normalize_drill_signature_png,
    parse_duration_minutes,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402


def _inline_cell(reference: str, text: str) -> str:
    return (
        f'<c r="{reference}" t="inlineStr"><is><t xml:space="preserve">'
        f"{text}</t></is></c>"
    )


def _row(number: int, cells: list[tuple[str, str]], *, height: int = 30) -> str:
    return (
        f'<row r="{number}" ht="{height}" customHeight="1">'
        + "".join(_inline_cell(reference, value) for reference, value in cells)
        + "</row>"
    )


def _fixture_xlsx(
    *,
    recognized: bool = True,
    with_drawing: bool = True,
    with_table_parts: bool = False,
) -> bytes:
    record_title = "演练记录表" if recognized else "待配置记录"
    record_rows = [
        _row(2, [("C2", record_title)]),
        _row(5, [("B5", "机房名称"), ("F5", "演练时间")]),
        _row(6, [("B6", "演练名称")]),
        _row(7, [("B7", "涉及区域"), ("C7", "ECC、IT包间")]),
        _row(8, [("B8", "总指挥人")]),
        _row(10, [("B10", "参演人员")]),
        _row(
            12,
            [
                ("C12", "演练位置"),
                ("D12", "演练步骤"),
                ("E12", "预估时间"),
                ("F12", "开始时间"),
                ("G12", "结束时间"),
                ("H12", "步骤执行人"),
                ("I12", "确认结果"),
            ],
        ),
        _row(13, [("C13", "ECC"), ("D13", "第一步"), ("E13", "2分钟")], height=60),
        _row(14, [("C14", "IT包间"), ("D14", "第二步"), ("E14", "3分")], height=90),
        _row(15, [("C15", "IT包间"), ("D15", "第三步"), ("E15", "5分钟")], height=120),
        _row(16, [("C16", "预估总用时"), ("D16", "0 时 10 分"), ("F16", "演练总用时")]),
        _row(17, [("B17", "演练参演人（签字）"), ("F17", "演练记录人（签字）")], height=70),
    ]
    assessment_title = "演练评估表" if recognized else "待配置评估"
    assessment_rows = [
        _row(2, [("D2", assessment_title)]),
        _row(5, [("B5", "演练名称"), ("F5", "演练日期")]),
        _row(6, [("B6", "参演人员"), ("F6", "开始时间"), ("H6", "结束时间")]),
        _row(8, [("H8", "分值"), ("I8", "得分")]),
        _row(9, [("H9", "40")]),
        _row(10, [("H10", "60")]),
        _row(11, [("B11", "总分")]),
    ]
    worksheet = lambda rows, merges, drawing="": (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheetFormatPr defaultRowHeight="15"/><cols><col min="1" max="11" width="12" customWidth="1"/></cols>'
        f'<sheetData>{"".join(rows)}</sheetData><mergeCells count="{len(merges)}">'
        + "".join(f'<mergeCell ref="{item}"/>' for item in merges)
        + f"</mergeCells>{drawing}</worksheet>"
    )
    drawing_reference = '<drawing r:id="rId1"/>' if with_drawing else ""
    table_parts = '<tableParts count="0"/>' if with_table_parts else ""
    record_xml = worksheet(
        record_rows,
        ["C2:I4", "C5:E5", "G5:I5", "C6:E6", "C10:I10", "D16:E16", "G16:I16", "C17:E17", "G17:I17"],
        drawing_reference + table_parts,
    )
    record_xml = record_xml.replace('<c r="C2"', '<c r="C2" s="1"', 1)
    assessment_xml = worksheet(
        assessment_rows,
        ["D2:K4", "B5:C5", "D5:E5", "G5:K5", "B6:C6", "D6:E6", "H6:I6", "J6:K6", "B11:G11", "H11:K11"],
    )

    def connector(row_zero: int) -> str:
        return (
            '<xdr:twoCellAnchor><xdr:from><xdr:col>7</xdr:col><xdr:colOff>0</xdr:colOff>'
            f'<xdr:row>{row_zero}</xdr:row><xdr:rowOff>100</xdr:rowOff></xdr:from>'
            '<xdr:to><xdr:col>7</xdr:col><xdr:colOff>1000</xdr:colOff>'
            f'<xdr:row>{row_zero}</xdr:row><xdr:rowOff>100</xdr:rowOff></xdr:to>'
            '<xdr:cxnSp><xdr:nvCxnSpPr/><xdr:spPr/></xdr:cxnSp><xdr:clientData/></xdr:twoCellAnchor>'
        )

    logo_anchor = (
        '<xdr:twoCellAnchor><xdr:from><xdr:col>1</xdr:col><xdr:colOff>0</xdr:colOff>'
        '<xdr:row>1</xdr:row><xdr:rowOff>0</xdr:rowOff></xdr:from>'
        '<xdr:to><xdr:col>1</xdr:col><xdr:colOff>900000</xdr:colOff>'
        '<xdr:row>3</xdr:row><xdr:rowOff>0</xdr:rowOff></xdr:to>'
        '<xdr:pic><xdr:nvPicPr><xdr:cNvPr id="1" name="测试Logo"/><xdr:cNvPicPr/></xdr:nvPicPr>'
        '<xdr:blipFill><a:blip r:embed="rId1"/><a:stretch><a:fillRect/></a:stretch></xdr:blipFill>'
        '<xdr:spPr><a:prstGeom prst="rect"><a:avLst/></a:prstGeom></xdr:spPr></xdr:pic>'
        '<xdr:clientData/></xdr:twoCellAnchor>'
    )
    drawing_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<xdr:wsDr xmlns:xdr="http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing" '
        'xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        + logo_anchor
        + connector(13) * 2
        + connector(14) * 3
        + "</xdr:wsDr>"
    )
    content_types = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Default Extension="png" ContentType="image/png"/>'
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '<Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '<Override PartName="/xl/drawings/drawing1.xml" ContentType="application/vnd.openxmlformats-officedocument.drawing+xml"/>'
        '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        '</Types>'
    )
    workbook = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets><sheet name="本月记录" sheetId="1" r:id="rId1"/>'
        '<sheet name="评估表" sheetId="2" r:id="rId2"/></sheets></workbook>'
    )
    root_rels = (
        '<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
        '</Relationships>'
    )
    workbook_rels = (
        '<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
        '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/>'
        '<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
        '</Relationships>'
    )
    sheet_rels = (
        '<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/drawing" Target="../drawings/drawing1.xml"/>'
        '</Relationships>'
    )
    drawing_rels = (
        '<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image" Target="../media/logo.png"/>'
        '</Relationships>'
    )
    styles = (
        '<?xml version="1.0" encoding="UTF-8"?><styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="2"><font><sz val="10"/></font><font><b/><sz val="16"/><color rgb="FF000000"/></font></fonts>'
        '<fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="solid"><fgColor rgb="FFFFCC00"/></patternFill></fill></fills>'
        '<borders count="2"><border/><border><left style="thin"><color rgb="FF000000"/></left><right style="thin"><color rgb="FF000000"/></right><top style="thin"><color rgb="FF000000"/></top><bottom style="thin"><color rgb="FF000000"/></bottom></border></borders>'
        '<cellXfs count="2"><xf fontId="0" fillId="0" borderId="0"/><xf fontId="1" fillId="1" borderId="1"><alignment horizontal="center" vertical="center" wrapText="1"/></xf></cellXfs>'
        '</styleSheet>'
    )
    logo_output = io.BytesIO()
    Image.new("RGB", (24, 12), "#1678ff").save(logo_output, "PNG")
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        parts = {
            "[Content_Types].xml": content_types,
            "_rels/.rels": root_rels,
            "xl/workbook.xml": workbook,
            "xl/_rels/workbook.xml.rels": workbook_rels,
            "xl/styles.xml": styles,
            "xl/worksheets/sheet1.xml": record_xml,
            "xl/worksheets/sheet2.xml": assessment_xml,
            "docProps/custom.xml": b"preserve-this-byte-for-byte",
        }
        if with_drawing:
            parts.update(
                {
                    "xl/worksheets/_rels/sheet1.xml.rels": sheet_rels,
                    "xl/drawings/drawing1.xml": drawing_xml,
                    "xl/drawings/_rels/drawing1.xml.rels": drawing_rels,
                    "xl/media/logo.png": logo_output.getvalue(),
                }
            )
        for name, content in parts.items():
            archive.writestr(name, content)
    return output.getvalue()


def _signature_png() -> bytes:
    output = io.BytesIO()
    image = Image.new("RGBA", (160, 50), (255, 255, 255, 0))
    ImageDraw.Draw(image).line((5, 40, 155, 8), fill="black", width=4)
    image.save(output, "PNG")
    return output.getvalue()


def _replace_zip_part(source: bytes, name: str, transform) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(source)) as archive, zipfile.ZipFile(
        output, "w", zipfile.ZIP_DEFLATED
    ) as target:
        for info in archive.infolist():
            content = archive.read(info.filename)
            target.writestr(info, transform(content) if info.filename == name else content)
    return output.getvalue()


def _duplicate_workbook_part(source: bytes) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(source)) as archive, zipfile.ZipFile(
        output, "w", zipfile.ZIP_DEFLATED
    ) as target:
        for info in archive.infolist():
            target.writestr(info, archive.read(info.filename))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            target.writestr("xl/workbook.xml", archive.read("xl/workbook.xml"))
    return output.getvalue()


class DrillManagementTests(unittest.TestCase):
    @unittest.skipUnless(
        Path(r"D:\下载\21V-JSNTFOC-EOP-PD-306 机柜单路断电故障应急处理流程V1.3.xlsx").is_file(),
        "reference drill workbook is not available",
    )
    def test_reference_workbook_detects_expected_sheets_and_signature_slots(self) -> None:
        workbook = _parse_workbook(
            Path(r"D:\下载\21V-JSNTFOC-EOP-PD-306 机柜单路断电故障应急处理流程V1.3.xlsx")
        )
        configuration = detect_drill_configuration(workbook)
        self.assertEqual(configuration["record_sheet"], "本月（PDU故障）)")
        self.assertEqual(configuration["assessment_sheet"], "评估表")
        slots = {int(item["row"]): int(item["signature_slots"]) for item in configuration["steps"]}
        self.assertEqual(slots[14], 3)
        self.assertEqual(slots[15], 4)
        preview_workbook = _parse_workbook(
            Path(r"D:\下载\21V-JSNTFOC-EOP-PD-306 机柜单路断电故障应急处理流程V1.3.xlsx"),
            include_assets_for={
                configuration["record_sheet"],
                configuration["assessment_sheet"],
            },
        )
        record = next(
            item
            for item in preview_workbook["sheets"]
            if item["name"] == configuration["record_sheet"]
        )
        self.assertEqual(record["preview_images"][0]["range"], "B2:B4")
        self.assertTrue(record["preview_images"][0]["data_url"].startswith("data:image/png;base64,"))
        self.assertEqual(record["cell_styles"]["C2"]["font_size"], 16.0)
        self.assertTrue(record["cell_styles"]["C2"]["bold"])

    def test_duration_and_time_chain_support_spaces_and_midnight(self) -> None:
        self.assertEqual(parse_duration_minutes("1 小时 05 分钟"), 65)
        self.assertEqual(parse_duration_minutes("3分"), 3)
        timeline = build_time_chain("2026-08-27", "23:58", [3, 5])
        self.assertEqual(timeline[0]["end_at"], "2026-08-28T00:01")
        self.assertEqual(timeline[1]["start_time"], "00:01")

    def test_upload_rejects_sparse_rows_duplicate_parts_and_external_relationships(self) -> None:
        sparse = _replace_zip_part(
            _fixture_xlsx(),
            "xl/worksheets/sheet1.xml",
            lambda content: content.replace(b'C16"', b'C1048576"'),
        )
        external = _replace_zip_part(
            _fixture_xlsx(),
            "_rels/.rels",
            lambda content: content.replace(
                b'Target="xl/workbook.xml"',
                b'Target="https://example.invalid/book.xlsx" TargetMode="External"',
            ),
        )
        with tempfile.TemporaryDirectory() as temporary:
            service = DrillManagementService(
                LanPortalStateStore(Path(temporary) / "state.sqlite3"),
                data_root=Path(temporary) / "drills",
            )
            for source, message in (
                (sparse, "最多支持 500 行"),
                (_duplicate_workbook_part(_fixture_xlsx()), "重复的内部路径"),
                (external, "外部链接关系"),
            ):
                with self.subTest(message=message), self.assertRaisesRegex(
                    Exception, message
                ):
                    service.create_definition(
                        name="不安全模板",
                        year=2026,
                        month=8,
                        file_name="unsafe.xlsx",
                        source=source,
                    )

    def test_signature_image_limits_are_checked_before_rgba_conversion(self) -> None:
        oversized = io.BytesIO()
        Image.new("RGBA", (4097, 1), (255, 255, 255, 0)).save(
            oversized, "PNG"
        )
        with self.assertRaisesRegex(Exception, "尺寸过大"):
            normalize_drill_signature_png(oversized.getvalue())

    def test_horizontal_signatures_pack_left_and_shrink_to_fit(self) -> None:
        roomy = _horizontal_signature_layout([(160, 50)] * 3, 600, 40)
        self.assertLessEqual(roomy[0][0], 4)
        self.assertLessEqual(
            max(
                roomy[index + 1][0] - roomy[index][0] - roomy[index][2]
                for index in range(len(roomy) - 1)
            ),
            3.01,
        )
        self.assertLess(roomy[-1][0] + roomy[-1][2], 600)

        crowded = _horizontal_signature_layout([(160, 50)] * 10, 200, 40)
        self.assertLessEqual(crowded[-1][0] + crowded[-1][2], 200)
        self.assertTrue(all(width < 20 for _x, _y, width, _height in crowded))

    def test_new_drawing_is_inserted_before_later_worksheet_nodes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            service = DrillManagementService(
                LanPortalStateStore(Path(temporary) / "state.sqlite3"),
                data_root=Path(temporary) / "drills",
            )
            definition = service.create_definition(
                name="无绘图模板演练",
                year=2026,
                month=8,
                file_name="template.xlsx",
                source=_fixture_xlsx(with_drawing=False, with_table_parts=True),
            )
            definition = service.publish(
                definition["drill_id"], expected_version=definition["version"]
            )
            person = {"record_id": "person-1", "name": "人员1"}
            execution = service.save_execution(
                definition["drill_id"],
                "A",
                {
                    "drill_date": "2026-08-27",
                    "first_start_time": "09:00",
                    "commander": person,
                    "participants": [person],
                    "step_signers": {
                        str(step["row"]): ["person-1"]
                        for step in definition["configuration"]["steps"]
                    },
                },
                expected_version=0,
            )
            generated = service.generate(
                definition["drill_id"],
                "A",
                signatures={"person-1": _signature_png()},
                expected_execution_version=execution["execution_version"],
            )
            with zipfile.ZipFile(generated["generated"]["path"]) as archive:
                root = ET.fromstring(archive.read("xl/worksheets/sheet1.xml"))
            names = [item.tag.rsplit("}", 1)[-1] for item in list(root)]
            self.assertLess(names.index("drawing"), names.index("tableParts"))

    def test_unrecognized_template_is_saved_for_manual_configuration(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            service = DrillManagementService(
                LanPortalStateStore(Path(temporary) / "state.sqlite3"),
                data_root=Path(temporary) / "drills",
            )
            definition = service.create_definition(
                name="待配置演练",
                year=2026,
                month=8,
                file_name="template.xlsx",
                source=_fixture_xlsx(recognized=False),
            )
            self.assertTrue(definition["detection_error"])
            self.assertEqual(definition["configuration"]["record_sheet"], "")
            self.assertTrue(Path(definition["source"]["path"]).is_file())
            configured = service.save_configuration(
                definition["drill_id"],
                {
                    "record_sheet": "本月记录",
                    "assessment_sheet": "评估表",
                    "mapping": {},
                    "steps": [],
                },
                expected_version=definition["version"],
            )
            self.assertEqual(len(configured["configuration"]["steps"]), 3)
            changed = dict(configured["configuration"])
            changed["mapping"] = dict(changed["mapping"])
            changed["mapping"]["steps"] = dict(changed["mapping"]["steps"])
            changed["mapping"]["steps"]["start_col"] = "J"
            changed["mapping"]["steps"]["content_col"] = "C"
            configured = service.save_configuration(
                definition["drill_id"],
                changed,
                expected_version=configured["version"],
            )
            self.assertEqual(
                configured["configuration"]["mapping"]["steps"]["start_col"],
                "J",
            )
            self.assertEqual(
                configured["configuration"]["steps"][0]["content"], "ECC"
            )

    def test_queued_generation_survives_restart_and_is_version_pinned(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "state.sqlite3"
            data_root = Path(temporary) / "drills"
            store = LanPortalStateStore(database)
            service = DrillManagementService(store, data_root=data_root)
            definition = service.create_definition(
                name="排队恢复演练",
                year=2026,
                month=8,
                file_name="template.xlsx",
                source=_fixture_xlsx(),
            )
            definition = service.publish(
                definition["drill_id"], expected_version=definition["version"]
            )
            people = [
                {
                    "record_id": f"person-{index}",
                    "name": "@人员1" if index == 1 else f"人员{index}",
                }
                for index in range(1, 5)
            ]
            execution = service.save_execution(
                definition["drill_id"],
                "A",
                {
                    "drill_date": "2026-08-27",
                    "first_start_time": "09:00",
                    "commander": people[0],
                    "participants": people,
                    "step_signers": {
                        "13": ["person-1"],
                        "14": ["person-1", "person-2", "person-3"],
                        "15": ["person-1", "person-2", "person-3", "person-4"],
                    },
                },
                expected_version=0,
            )
            queued = service.queue_generation(
                definition["drill_id"],
                "A",
                expected_version=execution["version"],
            )
            expected_execution_version = queued["generation_queue"][
                "execution_version"
            ]
            restarted = DrillManagementService(store, data_root=data_root)
            pending = restarted.recover_pending()
            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0]["status"], "queued")
            with self.assertRaisesRegex(DrillConflictError, "正在生成或同步"):
                restarted.save_execution(
                    definition["drill_id"],
                    "A",
                    {
                        "drill_date": "2026-08-28",
                        "first_start_time": "09:00",
                        "commander": people[0],
                        "participants": people,
                        "step_signers": execution["step_signers"],
                    },
                    expected_version=queued["version"],
                )
            with self.assertRaisesRegex(DrillConflictError, "排队后已修改"):
                restarted.generate(
                    definition["drill_id"],
                    "A",
                    expected_execution_version=expected_execution_version + 1,
                )

    def test_full_workflow_preserves_workbook_and_separates_sheet_values(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = LanPortalStateStore(Path(temporary) / "state.sqlite3")
            service = DrillManagementService(store, data_root=Path(temporary) / "drills")
            definition = service.create_definition(
                name="机柜断电演练",
                year=2026,
                month=8,
                file_name="template.xlsx",
                source=_fixture_xlsx(),
                actor="admin",
            )
            self.assertEqual(
                [(item["row"], item["signature_slots"]) for item in definition["configuration"]["steps"]],
                [(13, 1), (14, 3), (15, 4)],
            )
            definition = service.publish(
                definition["drill_id"], expected_version=definition["version"]
            )
            preview = service.preview_model(definition["drill_id"], "E", "record")
            self.assertLessEqual(preview["column_count"], 9)
            self.assertFalse(preview["generated_current"])
            self.assertEqual(preview["images"][0]["range"], "B2:B4")
            self.assertTrue(preview["images"][0]["data_url"].startswith("data:image/png;base64,"))
            self.assertEqual(preview["cell_styles"]["C2"]["font_size"], 16.0)
            self.assertEqual(preview["cell_styles"]["C2"]["fill"], "#FFCC00")
            self.assertEqual(preview["cell_styles"]["C2"]["borders"]["bottom"]["style"], "thin")
            self.assertGreater(preview["sheet_width_px"], 0)
            self.assertGreater(preview["sheet_height_px"], 0)
            people = [
                {
                    "record_id": f"person-{index}",
                    "name": "@人员1" if index == 1 else f"人员{index}",
                }
                for index in range(1, 5)
            ]
            execution = service.save_execution(
                definition["drill_id"],
                "E",
                {
                    "drill_date": "2026-08-27",
                    "first_start_time": "09:00",
                    "commander": people[0],
                    "participants": people,
                    "step_signers": {
                        "13": ["person-1"],
                        "14": ["person-1", "person-2", "person-3"],
                        "15": ["person-1", "person-2", "person-3", "person-4"],
                    },
                },
                expected_version=0,
            )
            self.assertEqual(execution["status"], "ready")
            signature = _signature_png()
            with self.assertRaises(DrillConflictError):
                service.generate(definition["drill_id"], "E", signatures={})
            self.assertEqual(
                service.get_execution(definition["drill_id"], "E")["status"],
                "error",
            )
            generated = service.generate(
                definition["drill_id"],
                "E",
                signatures={item["record_id"]: signature for item in people},
            )
            self.assertEqual(
                generated["generated"]["name"],
                "E楼-2026年08月27日-机柜断电演练-演练记录.xlsx",
            )
            output_path = Path(generated["generated"]["path"])
            with zipfile.ZipFile(output_path) as archive:
                self.assertEqual(
                    archive.read("docProps/custom.xml"),
                    b"preserve-this-byte-for-byte",
                )
                drawing = archive.read("xl/drawings/drawing1.xml")
                self.assertEqual(drawing.count(b"cxnSp"), 10)
                self.assertIn(b"drill_signature_", b"\n".join(name.encode() for name in archive.namelist()))
            workbook = _parse_workbook(output_path)
            record = next(item for item in workbook["sheets"] if item["name"] == "本月记录")
            assessment = next(item for item in workbook["sheets"] if item["name"] == "评估表")
            self.assertEqual(record["cells"]["I13"], "符合【 √ 】 不符【   】")
            self.assertEqual(assessment["cells"]["I9"], "40")
            self.assertEqual(record["cells"]["C5"], "南通机房E楼")
            self.assertEqual(record["cells"]["C8"], "")
            self.assertEqual(record["cells"]["C10"], "")
            print_model = service.print_model(definition["drill_id"], "E", "record")
            self.assertTrue(print_model["generated_current"])
            self.assertTrue({"C8", "C10:I10"}.issubset({item["range"] for item in print_model["signature_cells"]}))
            syncing = service.begin_sync(
                definition["drill_id"],
                "E",
                generated_version=generated["execution"]["generated_version"],
            )
            self.assertEqual(syncing["status"], "syncing")
            self.assertEqual(service.recover_pending()[0]["status"], "syncing")
            with self.assertRaisesRegex(DrillConflictError, "正在生成或同步"):
                service.save_execution(
                    definition["drill_id"],
                    "E",
                    {
                        "drill_date": "2026-08-28",
                        "first_start_time": "09:00",
                        "commander": people[0],
                        "participants": people,
                        "step_signers": execution["step_signers"],
                    },
                    expected_version=syncing["version"],
                )
            synced = service.mark_sync_result(
                definition["drill_id"],
                "E",
                generated_version=generated["execution"]["generated_version"],
                record_id="record-1",
                file_token="file-1",
            )
            updated = service.save_execution(
                definition["drill_id"],
                "E",
                {
                    "drill_date": "2026-08-28",
                    "first_start_time": "09:00",
                    "commander": people[0],
                    "participants": people,
                    "step_signers": execution["step_signers"],
                },
                expected_version=synced["version"],
            )
            self.assertNotEqual(updated["generated_version"], updated["execution_version"])
            with self.assertRaises(DrillConflictError):
                service.print_model(definition["drill_id"], "E", "record")
            deleted = service.delete_definition(
                definition["drill_id"], expected_version=definition["version"]
            )
            self.assertTrue(deleted["archived"])
            with self.assertRaisesRegex(DrillConflictError, "已归档"):
                service.generate(
                    definition["drill_id"],
                    "E",
                    signatures={item["record_id"]: signature for item in people},
                )


if __name__ == "__main__":
    unittest.main()
